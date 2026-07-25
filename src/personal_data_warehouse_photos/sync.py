"""Apple Photos upload runner.

Snapshot the library DB, resolve original-resource candidates, export each
selected resource through PhotoKit with iCloud network access enabled, and
upload its complete bytes through the app's shared photo endpoints (blob +
envelope). File blobs use resumable Drive sessions, so full originals are not
bounded by the app/proxy request ceiling. The local ``originals/`` cache is
never used as a source of truth.
"""

from __future__ import annotations

import hashlib
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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

# Some rows in Photos.sqlite can never be exported through PhotoKit (burst
# stack members, records whose iCloud original is gone, ...). Retrying them
# every 30 minutes forever burned the run's export budget and, because the
# first failure was re-raised, kept every scheduled run red — so a handful of
# dead assets hid the health of the ~12k that upload fine. Failing files are
# therefore backed off exponentially, and after MAX_FATAL_ATTEMPTS they stop
# failing the run: they are reported loudly in the summary instead.
MAX_FATAL_ATTEMPTS = 5
RETRY_BACKOFF_BASE = timedelta(minutes=30)
MAX_RETRY_BACKOFF = timedelta(days=7)
# 2 ** 12 * 30min is far past the cap; bound the shift so the delay math cannot
# overflow for a file that has been failing for years.
MAX_BACKOFF_DOUBLINGS = 12


def retry_delay(failure_count: int) -> timedelta:
    """Exponential backoff for a file that has failed ``failure_count`` times."""
    if failure_count <= 0:
        return timedelta(0)
    doublings = min(failure_count, MAX_BACKOFF_DOUBLINGS) - 1
    return min(RETRY_BACKOFF_BASE * (2**doublings), MAX_RETRY_BACKOFF)


@dataclass(frozen=True)
class PhotosUploadSummary:
    assets_seen: int
    files_seen: int
    files_selected: int
    files_skipped: int
    files_exported: int
    files_uploaded: int
    metadata_uploaded: int
    bytes_exported: int = 0
    bytes_uploaded: int = 0
    files_deferred: int = 0
    files_failed: int = 0


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
        resource_exporter=None,
    ) -> None:
        if ingest_client is None:
            raise ValueError("ingest_client is required")
        if mode not in {"full", "incremental"}:
            raise ValueError("mode must be 'full' or 'incremental'")
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
        deferred: list[tuple[PhotoFileCandidate, datetime]] = []
        for candidate in candidates:
            if self._mode == "incremental" and self._is_state_complete(candidate):
                state_skipped += 1
                continue
            retry_at = self._retry_not_before(candidate)
            if retry_at is not None:
                deferred.append((candidate, retry_at))
                continue
            selected.append(candidate)
        # The limit applies AFTER state selection (unlike voice memos) so a
        # capped run always makes forward progress through the backlog instead
        # of re-considering the same already-complete head of the list.
        # Backed-off failures are dropped before the limit too, so broken files
        # never consume slots that working ones need.
        if self._limit is not None:
            selected = selected[: self._limit]

        self._logger.info(
            "Incremental selection: selected=%s skipped=%s deferred=%s",
            len(selected),
            state_skipped,
            len(deferred),
        )
        if deferred:
            self._log_deferred(deferred)
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
                    files_deferred=len(deferred),
                )

        # Per-file failures are collected, not raised mid-batch (voice-memos
        # pattern): successes are recorded in upload_state so the next run
        # resumes past them, and a still-retryable failure re-raises at the end
        # so the run exits non-zero for the status helper.
        failures: list[tuple[PhotoFileCandidate, Exception]] = []
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
                self._upload_candidate(
                    index=index,
                    total=len(selected),
                    candidate=candidate,
                    exported=exported,
                )
            except Exception as exc:  # noqa: BLE001 - surfaced after the batch
                self._logger.warning("Failed to upload %s: %s", candidate.filename, exc)
                failures.append((candidate, exc))
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

        retryable = self._record_failures(failures)

        summary = PhotosUploadSummary(
            assets_seen=assets_seen,
            files_seen=len(candidates),
            files_selected=len(selected),
            files_skipped=state_skipped,
            files_exported=exported_count,
            files_uploaded=uploaded,
            metadata_uploaded=metadata_uploaded,
            bytes_exported=bytes_exported,
            bytes_uploaded=bytes_uploaded,
            files_deferred=len(deferred),
            files_failed=len(failures),
        )
        self._logger.info(
            "Photo upload summary: assets=%s original_resources=%s selected=%s exported=%s (%s) "
            "uploaded=%s (%s) skipped=%s deferred=%s failed=%s",
            summary.assets_seen,
            summary.files_seen,
            summary.files_selected,
            summary.files_exported,
            format_bytes(summary.bytes_exported),
            summary.files_uploaded,
            format_bytes(summary.bytes_uploaded),
            summary.files_skipped,
            summary.files_deferred,
            summary.files_failed,
        )
        if retryable:
            self._logger.warning(
                "Photo upload finished with %s failed file(s) after uploading %s; re-raising the "
                "first so the run is marked failed (successful uploads are recorded, so the next "
                "run resumes past them)",
                len(failures),
                summary.files_uploaded,
            )
            raise retryable[0]
        if failures:
            self._logger.warning(
                "%s file(s) have now failed %s+ times and no longer fail the run; they retry on "
                "backoff (up to %s days) and stay visible as failed= in this summary: %s",
                len(failures),
                MAX_FATAL_ATTEMPTS,
                MAX_RETRY_BACKOFF.days,
                ", ".join(f"{candidate.filename} ({exc})" for candidate, exc in failures[:5]),
            )
        return summary

    def _record_failures(
        self, failures: list[tuple[PhotoFileCandidate, Exception]]
    ) -> list[Exception]:
        """Persist each failed attempt; return the ones that must fail the run.

        A failure keeps failing the run until the same file has been attempted
        MAX_FATAL_ATTEMPTS times AND an upload has succeeded since that streak
        began. The second condition is what keeps a real outage (revoked Photos
        access, dead network) loudly red instead of quietly "green with
        failures": if nothing has succeeded since the failures started, every
        failure is still treated as fatal no matter how many attempts it has.
        """
        if self._upload_state is None:
            return [exc for _, exc in failures]
        now = self._now()
        latest_success = self._upload_state.latest_success_at()
        retryable: list[Exception] = []
        for candidate, exc in failures:
            entry = self._upload_state.mark_failure(
                source_type=SOURCE_TYPE_ASSET_FILE,
                source_id=candidate.state_id,
                fingerprint=candidate.fingerprint,
                error=str(exc),
                now=now,
            )
            first_failure = _parse_timestamp(entry.first_failure_at)
            proven = (
                latest_success is not None
                and first_failure is not None
                and latest_success >= first_failure
            )
            if entry.failure_count < MAX_FATAL_ATTEMPTS or not proven:
                retryable.append(exc)
        return retryable

    def _is_state_complete(self, candidate: PhotoFileCandidate) -> bool:
        if self._upload_state is None:
            return False
        return self._upload_state.is_complete(
            source_type=SOURCE_TYPE_ASSET_FILE,
            source_id=candidate.state_id,
            fingerprint=candidate.fingerprint,
        )

    def _retry_not_before(self, candidate: PhotoFileCandidate) -> datetime | None:
        """The moment a previously failed candidate may be attempted again."""
        if self._upload_state is None:
            return None
        entry = self._upload_state.entry_for(
            source_type=SOURCE_TYPE_ASSET_FILE, source_id=candidate.state_id
        )
        if entry is None or entry.failure_count <= 0:
            return None
        if entry.fingerprint != candidate.fingerprint:
            # The asset changed in Photos: a different file to upload, so the
            # old streak says nothing about it.
            return None
        last_failure = _parse_timestamp(entry.last_failure_at)
        if last_failure is None:
            return None
        retry_at = last_failure + retry_delay(entry.failure_count)
        return retry_at if self._now() < retry_at else None

    def _log_deferred(self, deferred: list[tuple[PhotoFileCandidate, datetime]]) -> None:
        earliest = min(retry_at for _, retry_at in deferred)
        self._logger.warning(
            "Deferred %s previously failed file(s) still in retry backoff (earliest retry %s): %s",
            len(deferred),
            earliest.isoformat(),
            ", ".join(candidate.filename for candidate, _ in deferred[:5]),
        )

    def _upload_candidate(
        self,
        *,
        index: int,
        total: int,
        candidate: PhotoFileCandidate,
        exported: ExportedPhotoFile,
    ) -> None:
        content_sha256 = sha256_file(exported.path)
        captured_at = candidate.captured_at or self._now().strftime("%Y-%m-%dT%H:%M:%S")
        self._logger.info(
            "[%s/%s] Uploading %s (%s, %s)",
            index,
            total,
            exported.filename,
            candidate.role,
            format_bytes(exported.size_bytes),
        )
        stored = self._ingest_client.upload_photo_file_path(
            exported.path,
            captured_at=captured_at,
            extension=exported.extension,
            content_type=exported.mime_type,
            content_sha256=content_sha256,
        )
        envelope = build_photo_metadata(
            source=PHOTO_SOURCE,
            account=self._account,
            native_id=candidate.native_id,
            role=candidate.role,
            filename=exported.filename,
            mime_type=exported.mime_type,
            size_bytes=exported.size_bytes,
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


def _parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def format_bytes(count: float) -> str:
    size = float(count)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} TiB"


def sha256_file(path: Path, *, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()
