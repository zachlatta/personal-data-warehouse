"""Apple Photos upload runner.

Snapshot the library DB, resolve original-resource candidates, export each
selected resource through PhotoKit with iCloud network access enabled, and
upload its complete bytes through the app's shared photo endpoints (blob +
envelope). The local ``originals/`` cache is never used as a source of truth.
"""

from __future__ import annotations

import hashlib
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from personal_data_warehouse_photos.envelope import build_photo_metadata, provenance_dedup_sha256
from personal_data_warehouse_photos.exporter import ExportedPhotoFile, PhotoKitAssetExporter
from personal_data_warehouse_photos.scanner import (
    PhotoFileCandidate,
    scan_photo_file_candidates,
    snapshot_photos_store,
)
from personal_data_warehouse_photos.state import SOURCE_TYPE_ASSET_FILE, PhotosUploadState

PHOTO_SOURCE = "apple_photos"


@dataclass(frozen=True)
class PhotosUploadSummary:
    assets_seen: int
    files_seen: int
    files_selected: int
    files_skipped: int
    files_exported: int
    files_uploaded: int
    metadata_uploaded: int
    files_deferred_oversize: int
    bytes_exported: int = 0
    bytes_uploaded: int = 0


class PhotosUploadRunner:
    def __init__(
        self,
        *,
        account: str,
        library_path: Path | str,
        ingest_client,
        logger,
        now=None,
        limit: int | None = None,
        mode: str = "incremental",
        upload_state: PhotosUploadState | None = None,
        before_upload_check=None,
        max_upload_bytes: int | None = None,
        resource_exporter=None,
    ) -> None:
        if ingest_client is None:
            raise ValueError("ingest_client is required")
        if mode not in {"full", "incremental"}:
            raise ValueError("mode must be 'full' or 'incremental'")
        # Files larger than the chosen route's ceiling (the app cap, or
        # Cloudflare's 100 MiB edge limit on the public host) defer instead of
        # 413-ing forever — the voice-memos lesson. Mostly bites large videos.
        if max_upload_bytes is None:
            max_upload_bytes = getattr(ingest_client, "effective_max_upload_bytes", None)
        self._max_upload_bytes = max_upload_bytes if (max_upload_bytes and max_upload_bytes > 0) else None
        self._account = account
        self._library_path = Path(library_path).expanduser()
        self._ingest_client = ingest_client
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._limit = limit
        self._mode = mode
        self._upload_state = upload_state
        self._before_upload_check = before_upload_check
        self._resource_exporter = resource_exporter or PhotoKitAssetExporter()

    def sync(self) -> PhotosUploadSummary:
        self._logger.info("Scanning Apple Photos library at %s", self._library_path)
        with tempfile.TemporaryDirectory(prefix="pdw-photos-") as working_dir:
            snapshot = snapshot_photos_store(self._library_path, working_dir)
            candidates = scan_photo_file_candidates(snapshot)
            return self._sync_candidates(candidates, export_dir=Path(working_dir) / "exports")

    def _sync_candidates(
        self,
        candidates: list[PhotoFileCandidate],
        *,
        export_dir: Path,
    ) -> PhotosUploadSummary:
        export_dir.mkdir(parents=True, exist_ok=True)
        assets_seen = len({candidate.native_id for candidate in candidates})
        self._logger.info(
            "Apple Photos inventory: assets=%s original_resources=%s (full bytes exported via PhotoKit)",
            assets_seen,
            len(candidates),
        )
        # v1 uploads originals only; edited renditions (Photos' adjusted
        # output under resources/renders) are a known follow-up. Keep the
        # count visible so the gap never reads as complete coverage.
        edited_assets = len(
            {
                candidate.native_id
                for candidate in candidates
                if candidate.role == "original" and candidate.apple_record.get("adjustments_state")
            }
        )
        if edited_assets:
            self._logger.info(
                "%s asset(s) have Photos adjustments; edited renditions are not uploaded yet (originals only)",
                edited_assets,
            )

        selected: list[PhotoFileCandidate] = []
        state_skipped = 0
        oversize_deferred = 0
        for candidate in candidates:
            if self._mode == "incremental" and self._is_state_complete(candidate):
                state_skipped += 1
                continue
            selected.append(candidate)
        # The limit applies AFTER state selection (unlike voice memos) so a
        # capped run always makes forward progress through the backlog instead
        # of re-considering the same already-complete head of the list.
        if self._limit is not None:
            selected = selected[: self._limit]

        self._logger.info(
            "Incremental selection: selected=%s skipped=%s oversize_deferred=%s",
            len(selected),
            state_skipped,
            oversize_deferred,
        )
        if selected and self._before_upload_check is not None:
            skip_reason = self._before_upload_check()
            if skip_reason:
                self._logger.warning("Skipping photo upload: %s", skip_reason)
                return PhotosUploadSummary(
                    assets_seen=assets_seen,
                    files_seen=len(candidates),
                    files_selected=len(selected),
                    files_skipped=state_skipped,
                    files_exported=0,
                    files_uploaded=0,
                    metadata_uploaded=0,
                    files_deferred_oversize=oversize_deferred,
                )

        # Per-file failures are collected, not raised mid-batch (voice-memos
        # pattern): successes are recorded in upload_state so the next run
        # resumes past them, and the first failure re-raises at the end so the
        # run still exits non-zero for the status helper.
        failures: list[tuple[str, Exception]] = []
        uploaded = 0
        metadata_uploaded = 0
        exported_count = 0
        bytes_exported = 0
        bytes_uploaded = 0
        for index, candidate in enumerate(selected, start=1):
            exported: ExportedPhotoFile | None = None
            try:
                self._logger.info(
                    "[%s/%s] Exporting full original %s (%s) through PhotoKit",
                    index,
                    len(selected),
                    candidate.filename,
                    candidate.role,
                )
                exported = self._resource_exporter.export(candidate, export_dir)
                exported_count += 1
                bytes_exported += exported.size_bytes
                if (
                    self._max_upload_bytes is not None
                    and exported.size_bytes > self._max_upload_bytes
                ):
                    oversize_deferred += 1
                    self._logger.warning(
                        "Deferring %s (%s): exceeds the %s upload ceiling for the current route",
                        exported.filename,
                        format_bytes(exported.size_bytes),
                        format_bytes(self._max_upload_bytes),
                    )
                    continue
                self._upload_candidate(
                    index=index,
                    total=len(selected),
                    candidate=candidate,
                    exported=exported,
                )
            except Exception as exc:  # noqa: BLE001 - surfaced after the batch
                self._logger.warning("Failed to upload %s: %s", candidate.filename, exc)
                failures.append((candidate.filename, exc))
                if self._upload_state is not None:
                    self._upload_state.mark_failure(
                        source_type=SOURCE_TYPE_ASSET_FILE,
                        source_id=candidate.state_id,
                        fingerprint=candidate.fingerprint,
                        error=str(exc),
                        now=self._now(),
                    )
                continue
            finally:
                if exported is not None:
                    try:
                        exported.path.unlink(missing_ok=True)
                    except OSError as exc:
                        self._logger.warning(
                            "Could not remove temporary PhotoKit export %s: %s",
                            exported.path,
                            exc,
                        )
            uploaded += 1
            metadata_uploaded += 1
            bytes_uploaded += exported.size_bytes

        summary = PhotosUploadSummary(
            assets_seen=assets_seen,
            files_seen=len(candidates),
            files_selected=len(selected),
            files_skipped=state_skipped,
            files_exported=exported_count,
            files_uploaded=uploaded,
            metadata_uploaded=metadata_uploaded,
            files_deferred_oversize=oversize_deferred,
            bytes_exported=bytes_exported,
            bytes_uploaded=bytes_uploaded,
        )
        self._logger.info(
            "Photo upload summary: assets=%s original_resources=%s selected=%s exported=%s (%s) "
            "uploaded=%s (%s) skipped=%s oversize_deferred=%s",
            summary.assets_seen,
            summary.files_seen,
            summary.files_selected,
            summary.files_exported,
            format_bytes(summary.bytes_exported),
            summary.files_uploaded,
            format_bytes(summary.bytes_uploaded),
            summary.files_skipped,
            summary.files_deferred_oversize,
        )
        if failures:
            self._logger.warning(
                "Photo upload finished with %s failed file(s) after uploading %s; re-raising the "
                "first so the run is marked failed (successful uploads are recorded, so the next "
                "run resumes past them)",
                len(failures),
                summary.files_uploaded,
            )
            raise failures[0][1]
        return summary

    def _is_state_complete(self, candidate: PhotoFileCandidate) -> bool:
        if self._upload_state is None:
            return False
        return self._upload_state.is_complete(
            source_type=SOURCE_TYPE_ASSET_FILE,
            source_id=candidate.state_id,
            fingerprint=candidate.fingerprint,
        )

    def _upload_candidate(
        self,
        *,
        index: int,
        total: int,
        candidate: PhotoFileCandidate,
        exported: ExportedPhotoFile,
    ) -> None:
        content = exported.path.read_bytes()
        content_sha256 = hashlib.sha256(content).hexdigest()
        captured_at = candidate.captured_at or self._now().strftime("%Y-%m-%dT%H:%M:%S")
        self._logger.info(
            "[%s/%s] Uploading %s (%s, %s)",
            index,
            total,
            exported.filename,
            candidate.role,
            format_bytes(len(content)),
        )
        stored = self._ingest_client.upload_photo_file(
            content,
            captured_at=captured_at,
            extension=exported.extension,
            content_type=exported.mime_type,
        )
        envelope = build_photo_metadata(
            source=PHOTO_SOURCE,
            account=self._account,
            native_id=candidate.native_id,
            role=candidate.role,
            filename=exported.filename,
            mime_type=exported.mime_type,
            size_bytes=len(content),
            content_sha256=content_sha256,
            uploaded_at=self._now().isoformat(),
            width=candidate.width,
            height=candidate.height,
            captured_at=captured_at,
            capture_tz_offset=candidate.capture_tz_offset,
            camera_make=candidate.camera_make,
            camera_model=candidate.camera_model,
            record_key="apple_record",
            record=candidate.apple_record,
        )
        self._ingest_client.upload_photo_metadata(
            envelope,
            captured_at=captured_at,
            file_content_sha256=content_sha256,
            metadata_dedup_sha256=provenance_dedup_sha256(
                source=PHOTO_SOURCE,
                account=self._account,
                native_id=candidate.native_id,
                role=candidate.role,
                file_content_sha256=content_sha256,
            ),
        )
        if self._upload_state is not None:
            self._upload_state.mark_success(
                source_type=SOURCE_TYPE_ASSET_FILE,
                source_id=candidate.state_id,
                fingerprint=candidate.fingerprint,
                now=self._now(),
                content_sha256=content_sha256,
                storage_key=str(stored.get("storage_key", "")) if isinstance(stored, dict) else "",
            )


def format_bytes(count: float) -> str:
    size = float(count)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} TiB"
