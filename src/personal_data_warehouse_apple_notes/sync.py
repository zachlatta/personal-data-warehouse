from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import hashlib
from html import escape
import json
import threading
import tempfile

from personal_data_warehouse_voice_memos.storage import ObjectStore
from personal_data_warehouse_apple_notes.scanner import (
    AppleNote,
    AppleNoteAttachment,
    parse_datetime,
    scan_apple_notes_store,
    snapshot_apple_notes_store,
)
from personal_data_warehouse_apple_notes.state import AppleNotesUploadState

OBJECT_PREFIX = "apple-notes"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox"


@dataclass(frozen=True)
class AppleNotesUploadSummary:
    notes_seen: int
    notes_selected: int
    notes_skipped: int
    notes_deleted: int
    revisions_uploaded: int
    metadata_uploaded: int
    body_html_uploaded: int
    attachments_seen: int
    attachments_uploaded: int
    attachments_missing: int
    notes_deferred: int = 0


@dataclass(frozen=True)
class AppleNoteRevision:
    note: AppleNote
    revision_id: str
    fingerprint: str
    is_tombstone: bool = False


@dataclass(frozen=True)
class AppleNoteUploadResult:
    revision_uploaded: int
    metadata_uploaded: int
    html_uploaded: int
    attachments_uploaded: int
    attachments_missing: int


class AppleNotesUploadRunner:
    def __init__(
        self,
        *,
        account: str,
        store_path: Path | str,
        object_store: ObjectStore | None = None,
        object_store_factory: Callable[[], ObjectStore] | None = None,
        logger,
        upload_state: AppleNotesUploadState | None = None,
        now: Callable[[], datetime] | None = None,
        mode: str = "incremental",
        before_upload_check: Callable[[], str | None] | None = None,
        limit: int | None = None,
        workers: int = 1,
        state_save_callback: Callable[[], None] | None = None,
        check_remote_existing: bool = False,
    ) -> None:
        if object_store is None and object_store_factory is None:
            raise ValueError("object_store or object_store_factory must be provided")
        if object_store is not None and object_store_factory is not None:
            raise ValueError("pass only one of object_store or object_store_factory")
        if mode not in {"full", "incremental"}:
            raise ValueError("mode must be 'full' or 'incremental'")
        if limit is not None and limit < 0:
            raise ValueError("limit must be nonnegative")
        self._account = account
        self._store_path = Path(store_path).expanduser()
        self._object_store = object_store
        self._object_store_factory = object_store_factory
        self._thread_local = threading.local()
        self._state_lock = threading.Lock()
        self._logger = logger
        self._upload_state = upload_state
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._mode = mode
        self._before_upload_check = before_upload_check
        self._limit = limit
        self._workers = max(1, workers)
        self._state_save_callback = state_save_callback
        self._check_remote_existing = check_remote_existing

    def sync(self) -> AppleNotesUploadSummary:
        self._logger.info("Snapshotting Apple Notes store at %s", self._store_path)
        with tempfile.TemporaryDirectory(prefix="pdw-apple-notes-") as temp_dir:
            snapshot_path = snapshot_apple_notes_store(self._store_path, temp_dir)
            notes = scan_apple_notes_store(snapshot_path, attachments_root=self._store_path.parent)
        notes_by_id = {note.note_id: note for note in notes}
        revisions = [revision_from_note(note) for note in notes]
        tombstones = self._deleted_note_revisions(current_note_ids=set(notes_by_id))
        revisions.extend(tombstones)

        notes_seen = len(notes)
        attachments_seen = sum(len(note.attachments) for note in notes)
        attachments_missing = sum(1 for note in notes for attachment in note.attachments if attachment.is_missing)
        selected: list[AppleNoteRevision] = []
        skipped = 0
        for revision in revisions:
            entry = self._upload_state.entry_for(revision.note.note_id) if self._upload_state else None
            if (
                self._mode == "incremental"
                and entry is not None
                and entry.complete
                and entry.fingerprint == revision.fingerprint
                and entry.is_deleted == revision.note.is_deleted
            ):
                skipped += 1
                continue
            selected.append(revision)

        deferred = 0
        if self._limit is not None and len(selected) > self._limit:
            deferred = len(selected) - self._limit
            selected = selected[: self._limit]

        if selected and self._before_upload_check is not None:
            skip_reason = self._before_upload_check()
            if skip_reason:
                self._logger.warning("Skipping Apple Notes upload: %s", skip_reason)
                return AppleNotesUploadSummary(
                    notes_seen=notes_seen,
                    notes_selected=len(selected),
                    notes_skipped=skipped,
                    notes_deleted=len(tombstones),
                    revisions_uploaded=0,
                    metadata_uploaded=0,
                    body_html_uploaded=0,
                    attachments_seen=attachments_seen,
                    attachments_uploaded=0,
                    attachments_missing=attachments_missing,
                    notes_deferred=len(selected) + deferred,
                )

        self._logger.info("Uploading with %s worker(s)", self._workers)
        if self._workers == 1 or len(selected) <= 1:
            results = [
                self._sync_revision(index=index, total=len(selected), revision=revision)
                for index, revision in enumerate(selected, start=1)
            ]
        else:
            with ThreadPoolExecutor(max_workers=self._workers, thread_name_prefix="apple-notes") as executor:
                futures = [
                    executor.submit(self._sync_revision, index=index, total=len(selected), revision=revision)
                    for index, revision in enumerate(selected, start=1)
                ]
                results = [future.result() for future in as_completed(futures)]

        summary = AppleNotesUploadSummary(
            notes_seen=notes_seen,
            notes_selected=len(selected),
            notes_skipped=skipped,
            notes_deleted=len(tombstones),
            revisions_uploaded=sum(result.revision_uploaded for result in results),
            metadata_uploaded=sum(result.metadata_uploaded for result in results),
            body_html_uploaded=sum(result.html_uploaded for result in results),
            attachments_seen=attachments_seen,
            attachments_uploaded=sum(result.attachments_uploaded for result in results),
            attachments_missing=attachments_missing,
            notes_deferred=deferred,
        )
        self._logger.info(
            "Apple Notes upload summary: seen=%s selected=%s skipped=%s deferred=%s revisions=%s metadata=%s html=%s attachments=%s missing=%s deleted=%s",
            summary.notes_seen,
            summary.notes_selected,
            summary.notes_skipped,
            summary.notes_deferred,
            summary.revisions_uploaded,
            summary.metadata_uploaded,
            summary.body_html_uploaded,
            summary.attachments_uploaded,
            summary.attachments_missing,
            summary.notes_deleted,
        )
        return summary

    def _deleted_note_revisions(self, *, current_note_ids: set[str]) -> list[AppleNoteRevision]:
        if self._upload_state is None:
            return []
        revisions: list[AppleNoteRevision] = []
        deleted_at = self._now()
        for note_id, entry in sorted(self._upload_state.entries.items()):
            if note_id in current_note_ids or entry.is_deleted:
                continue
            note = AppleNote(
                note_id=note_id,
                title=entry.title,
                folder_id="",
                folder_path="",
                apple_account_id="",
                apple_account_name="",
                created_at=parse_datetime(entry.modified_at),
                modified_at=deleted_at,
                body_text="",
                body_html="",
                body_markdown="",
                attachments=(),
                is_deleted=True,
                raw={"tombstone_from_revision_id": entry.revision_id},
            )
            revisions.append(revision_from_note(note))
        return revisions

    def _sync_revision(self, *, index: int, total: int, revision: AppleNoteRevision) -> AppleNoteUploadResult:
        try:
            object_store = self._object_store_for_thread()
            if self._check_remote_existing and object_store.has_object(
                kind="apple_note_revision_metadata",
                key="revision_id",
                value=revision.revision_id,
            ):
                self._logger.info(
                    "[%s/%s] skip Apple Note %s revision %s: metadata already present in Drive",
                    index,
                    total,
                    revision.note.note_id,
                    short_sha256(revision.revision_id),
                )
                result = AppleNoteUploadResult(
                    revision_uploaded=0,
                    metadata_uploaded=0,
                    html_uploaded=0,
                    attachments_uploaded=0,
                    attachments_missing=sum(1 for attachment in revision.note.attachments if attachment.is_missing),
                )
            else:
                self._logger.info(
                    "[%s/%s] upload Apple Note %s revision %s",
                    index,
                    total,
                    revision.note.note_id,
                    short_sha256(revision.revision_id),
                )
                result = self._upload_revision(revision, object_store=object_store)
            self._mark_success(revision=revision, result=result)
            return result
        except Exception as exc:
            self._mark_failure(revision=revision, error=str(exc))
            raise

    def _mark_success(self, *, revision: AppleNoteRevision, result: AppleNoteUploadResult) -> None:
        if self._upload_state is None:
            return
        with self._state_lock:
            self._upload_state.mark_success(
                note_id=revision.note.note_id,
                fingerprint=revision.fingerprint,
                revision_id=revision.revision_id,
                title=revision.note.title,
                modified_at=revision.note.modified_at,
                is_deleted=revision.note.is_deleted,
                metadata_uploaded=True,
                html_uploaded=bool(revision.note.is_deleted or result.html_uploaded),
                attachments_uploaded=True,
                now=self._now(),
            )
            if self._state_save_callback is not None:
                self._state_save_callback()

    def _mark_failure(self, *, revision: AppleNoteRevision, error: str) -> None:
        if self._upload_state is None:
            return
        with self._state_lock:
            self._upload_state.mark_failure(
                note_id=revision.note.note_id,
                error=error,
                now=self._now(),
            )
            if self._state_save_callback is not None:
                self._state_save_callback()

    def _object_store_for_thread(self) -> ObjectStore:
        if self._object_store is not None:
            return self._object_store
        store = getattr(self._thread_local, "object_store", None)
        if store is None:
            if self._object_store_factory is None:
                raise RuntimeError("object_store_factory is not configured")
            store = self._object_store_factory()
            self._thread_local.object_store = store
        return store

    def _upload_revision(self, revision: AppleNoteRevision, *, object_store: ObjectStore) -> AppleNoteUploadResult:
        payload = metadata_payload(account=self._account, revision=revision, exported_at=self._now())
        base_key = revision_object_key_base(revision)
        html_uploaded = 0
        attachment_uploads = 0
        attachment_missing = sum(1 for attachment in revision.note.attachments if attachment.is_missing)

        if not revision.note.is_deleted:
            html = revision.note.body_html or html_document_for_note(revision.note)
            if html:
                html_sha = text_sha256(html)
                with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".html") as file:
                    file.write(html)
                    file.flush()
                    object_store.put_file(
                        path=Path(file.name),
                        object_key=f"{base_key}.html",
                        content_sha256=html_sha,
                        content_type="text/html",
                        skip_existing_check=True,
                        kind="apple_note_body_html",
                        app_properties={
                            "note_id": revision.note.note_id,
                            "revision_id": revision.revision_id,
                        },
                    )
                html_uploaded = 1

            for attachment in revision.note.attachments:
                if attachment.is_missing or attachment.path is None or not attachment.content_sha256:
                    continue
                object_store.put_file(
                    path=attachment.path,
                    object_key=attachment_object_key(revision, attachment),
                    content_sha256=attachment.content_sha256,
                    content_type=attachment.content_type,
                    kind="apple_note_attachment",
                    app_properties={
                        "note_id": revision.note.note_id,
                        "revision_id": revision.revision_id,
                        "attachment_id": attachment.attachment_id,
                    },
                )
                attachment_uploads += 1

        object_store.put_json(
            object_key=f"{base_key}.json",
            payload=payload,
            content_sha256=json_sha256(payload),
            skip_existing_check=True,
            kind="apple_note_revision_metadata",
            app_properties={
                "note_id": revision.note.note_id,
                "revision_id": revision.revision_id,
                "note_content_sha256": revision.fingerprint,
            },
        )
        return AppleNoteUploadResult(
            revision_uploaded=1,
            metadata_uploaded=1,
            html_uploaded=html_uploaded,
            attachments_uploaded=attachment_uploads,
            attachments_missing=attachment_missing,
        )


def revision_from_note(note: AppleNote) -> AppleNoteRevision:
    fingerprint_payload = note_payload(note)
    fingerprint = json_sha256(fingerprint_payload)
    return AppleNoteRevision(note=note, revision_id=fingerprint, fingerprint=fingerprint, is_tombstone=note.is_deleted)


def note_payload(note: AppleNote) -> dict[str, object]:
    return {
        "note_id": note.note_id,
        "title": note.title,
        "folder_id": note.folder_id,
        "folder_path": note.folder_path,
        "apple_account_id": note.apple_account_id,
        "apple_account_name": note.apple_account_name,
        "created_at": note.created_at.astimezone(UTC).isoformat(),
        "modified_at": note.modified_at.astimezone(UTC).isoformat(),
        "body_text": note.body_text,
        "body_html": note.body_html,
        "body_markdown": note.body_markdown,
        "attachments": [attachment_payload(attachment) for attachment in note.attachments],
        "is_deleted": note.is_deleted,
        "raw": note.raw or {},
    }


def attachment_payload(attachment: AppleNoteAttachment) -> dict[str, object]:
    return {
        "attachment_id": attachment.attachment_id,
        "note_id": attachment.note_id,
        "filename": attachment.filename,
        "content_type": attachment.content_type,
        "size_bytes": attachment.size_bytes,
        "content_sha256": attachment.content_sha256,
        "is_missing": attachment.is_missing,
        "error": attachment.error,
        "raw": attachment.raw or {},
    }


def metadata_payload(*, account: str, revision: AppleNoteRevision, exported_at: datetime) -> dict[str, object]:
    note = note_payload(revision.note)
    note["revision_id"] = revision.revision_id
    note["content_sha256"] = revision.fingerprint
    return {
        "schema_version": 1,
        "source": "apple_notes",
        "account": account,
        "exported_at": exported_at.astimezone(UTC).isoformat(),
        "note": note,
    }


def revision_object_key_base(revision: AppleNoteRevision, *, stage: str = "inbox") -> str:
    modified = revision.note.modified_at.astimezone(UTC)
    return (
        f"{OBJECT_PREFIX}/{stage}/{modified.year:04d}/{modified.month:02d}/"
        f"{safe_object_key_part(revision.note.note_id)}/{revision.revision_id}"
    )


def attachment_object_key(revision: AppleNoteRevision, attachment: AppleNoteAttachment, *, stage: str = "inbox") -> str:
    extension = Path(attachment.filename).suffix
    suffix = extension if extension else ".bin"
    return (
        f"{revision_object_key_base(revision, stage=stage)}/attachments/"
        f"{safe_object_key_part(attachment.attachment_id)}-{attachment.content_sha256}{suffix}"
    )


def html_document_for_note(note: AppleNote) -> str:
    title = escape(note.title or note.note_id)
    body = note.body_html or ("<pre>" + escape(note.body_text) + "</pre>")
    return f"<!doctype html><html><head><meta charset=\"utf-8\"><title>{title}</title></head><body>{body}</body></html>"


def safe_object_key_part(value: str) -> str:
    return re_sub_invalid(value)[:120] or "untitled"


def re_sub_invalid(value: str) -> str:
    import re

    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip(".-")


def json_sha256(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def short_sha256(value: str) -> str:
    return value[:12]
