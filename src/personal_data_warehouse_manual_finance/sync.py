"""Manual finance document upload runner.

Walks the given files/directories, uploads each not-yet-uploaded document
through the app's manual-finance endpoints (blob + envelope), and preserves
the folder organization: ``original_path`` is the file's path relative to the
upload root, and its top folder becomes the object key's account segment
(the uploader's folder-per-account layout, e.g. ``acme-checking-0001/``).

No format-specific handling happens here — every accepted file uploads as-is;
the server-side extraction agent figures out what each document is.
"""

from __future__ import annotations

import hashlib
import mimetypes
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from personal_data_warehouse_manual_finance.envelope import (
    build_document_metadata,
    provenance_dedup_sha256,
)
from personal_data_warehouse_manual_finance.state import ManualFinanceUploadState

DOCUMENT_EXTENSIONS = (
    ".pdf",
    ".png",
    ".jpg",
    ".jpeg",
    ".heic",
    ".csv",
    ".ofx",
    ".qfx",
    ".rtf",
)

_EXTENSION_CONTENT_TYPES = {
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".heic": "image/heic",
    ".csv": "text/csv",
    ".ofx": "application/x-ofx",
    ".qfx": "application/x-ofx",
    ".rtf": "text/rtf",
}


@dataclass(frozen=True)
class DocumentCandidate:
    path: Path
    original_path: str
    account_folder: str


@dataclass(frozen=True)
class ManualFinanceUploadSummary:
    files_seen: int
    files_ignored: int
    files_selected: int
    files_skipped: int
    files_uploaded: int
    metadata_uploaded: int
    bytes_uploaded: int = 0


def document_content_type(path: Path) -> str:
    known = _EXTENSION_CONTENT_TYPES.get(path.suffix.lower())
    if known:
        return known
    guessed, _ = mimetypes.guess_type(path.name)
    return guessed or "application/octet-stream"


def resolve_candidates(paths: list[Path], *, root: Path | None) -> tuple[list[DocumentCandidate], int]:
    """Expand files/directories into upload candidates.

    Returns (candidates, ignored_count). Directories are walked recursively;
    hidden files and unsupported extensions are ignored. ``original_path`` is
    relative to ``root`` (explicit, or each directory argument / a bare file's
    parent), and the account folder is its first directory component.
    """
    candidates: list[DocumentCandidate] = []
    ignored = 0
    seen: set[Path] = set()

    def add(file_path: Path, base: Path) -> None:
        nonlocal ignored
        resolved = file_path.resolve()
        if resolved in seen:
            return
        seen.add(resolved)
        if file_path.name.startswith("."):
            ignored += 1
            return
        if file_path.suffix.lower() not in DOCUMENT_EXTENSIONS:
            ignored += 1
            return
        try:
            relative = PurePosixPath(resolved.relative_to(base.resolve()).as_posix())
        except ValueError:
            relative = PurePosixPath(file_path.name)
        account_folder = relative.parts[0] if len(relative.parts) > 1 else ""
        candidates.append(
            DocumentCandidate(
                path=resolved,
                original_path=str(relative),
                account_folder=account_folder,
            )
        )

    for path in paths:
        expanded = path.expanduser()
        if expanded.is_dir():
            base = root.expanduser() if root else expanded
            for file_path in sorted(expanded.rglob("*")):
                if file_path.is_file():
                    add(file_path, base)
        elif expanded.is_file():
            base = root.expanduser() if root else expanded.parent
            add(expanded, base)
        else:
            raise FileNotFoundError(f"no such file or directory: {path}")

    candidates.sort(key=lambda candidate: candidate.original_path)
    return candidates, ignored


class ManualFinanceUploadRunner:
    def __init__(
        self,
        *,
        account: str,
        paths: list[Path],
        ingest_client,
        logger,
        root: Path | None = None,
        now=None,
        limit: int | None = None,
        mode: str = "incremental",
        upload_state: ManualFinanceUploadState | None = None,
    ) -> None:
        if ingest_client is None:
            raise ValueError("ingest_client is required")
        if mode not in {"full", "incremental"}:
            raise ValueError("mode must be 'full' or 'incremental'")
        if not paths:
            raise ValueError("at least one file or directory is required")
        self._account = account
        self._paths = paths
        self._root = root
        self._ingest_client = ingest_client
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._limit = limit
        self._mode = mode
        self._upload_state = upload_state

    def sync(self) -> ManualFinanceUploadSummary:
        candidates, ignored = resolve_candidates(self._paths, root=self._root)
        self._logger.info(
            "Manual finance scan: %s document(s) found, %s ignored (hidden/unsupported)",
            len(candidates),
            ignored,
        )

        selected: list[tuple[DocumentCandidate, bytes, str]] = []
        skipped = 0
        for candidate in candidates:
            content = candidate.path.read_bytes()
            content_sha256 = hashlib.sha256(content).hexdigest()
            if (
                self._mode == "incremental"
                and self._upload_state is not None
                and self._upload_state.is_complete(content_sha256=content_sha256)
            ):
                skipped += 1
                continue
            selected.append((candidate, content, content_sha256))
        # The limit applies AFTER state selection (photos pattern) so a capped
        # run always makes forward progress through the backlog.
        if self._limit is not None:
            selected = selected[: self._limit]

        self._logger.info(
            "Incremental selection: selected=%s skipped=%s", len(selected), skipped
        )

        # Per-file failures are collected, not raised mid-batch (photos
        # pattern): successes are recorded so the next run resumes past them,
        # and the first failure re-raises at the end.
        failures: list[tuple[str, Exception]] = []
        uploaded = 0
        bytes_uploaded = 0
        for index, (candidate, content, content_sha256) in enumerate(selected, start=1):
            try:
                self._upload_candidate(
                    index=index,
                    total=len(selected),
                    candidate=candidate,
                    content=content,
                    content_sha256=content_sha256,
                )
            except Exception as exc:  # noqa: BLE001 - surfaced after the batch
                self._logger.warning("Failed to upload %s: %s", candidate.original_path, exc)
                failures.append((candidate.original_path, exc))
                if self._upload_state is not None:
                    self._upload_state.mark_failure(
                        content_sha256=content_sha256,
                        original_path=candidate.original_path,
                        error=str(exc),
                        now=self._now(),
                    )
                continue
            uploaded += 1
            bytes_uploaded += len(content)

        summary = ManualFinanceUploadSummary(
            files_seen=len(candidates),
            files_ignored=ignored,
            files_selected=len(selected),
            files_skipped=skipped,
            files_uploaded=uploaded,
            metadata_uploaded=uploaded,
            bytes_uploaded=bytes_uploaded,
        )
        self._logger.info(
            "Manual finance upload summary: seen=%s ignored=%s selected=%s uploaded=%s skipped=%s",
            summary.files_seen,
            summary.files_ignored,
            summary.files_selected,
            summary.files_uploaded,
            summary.files_skipped,
        )
        if failures:
            self._logger.warning(
                "Manual finance upload finished with %s failed file(s) after uploading %s; "
                "re-raising the first so the run is marked failed",
                len(failures),
                summary.files_uploaded,
            )
            raise failures[0][1]
        return summary

    def _upload_candidate(
        self,
        *,
        index: int,
        total: int,
        candidate: DocumentCandidate,
        content: bytes,
        content_sha256: str,
    ) -> None:
        modified_at = datetime.fromtimestamp(candidate.path.stat().st_mtime, tz=UTC).isoformat()
        content_type = document_content_type(candidate.path)
        self._logger.info(
            "[%s/%s] Uploading %s (%s bytes)", index, total, candidate.original_path, len(content)
        )
        self._ingest_client.upload_manual_finance_document(
            content,
            modified_at=modified_at,
            account_folder=candidate.account_folder,
            extension=candidate.path.suffix.lower(),
            content_type=content_type,
        )
        envelope = build_document_metadata(
            account=self._account,
            filename=candidate.path.name,
            original_path=candidate.original_path,
            mime_type=content_type,
            size_bytes=len(content),
            content_sha256=content_sha256,
            uploaded_at=self._now().isoformat(),
            file_modified_at=modified_at,
        )
        self._ingest_client.upload_manual_finance_metadata(
            envelope,
            modified_at=modified_at,
            account_folder=candidate.account_folder,
            file_content_sha256=content_sha256,
            metadata_dedup_sha256=provenance_dedup_sha256(
                source=envelope["source"],
                account=self._account,
                native_id=content_sha256,
                file_content_sha256=content_sha256,
            ),
        )
        if self._upload_state is not None:
            self._upload_state.mark_success(
                content_sha256=content_sha256,
                original_path=candidate.original_path,
                now=self._now(),
            )
