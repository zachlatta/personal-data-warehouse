from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import gzip
import hashlib
import json
import re
import threading
import tempfile

from personal_data_warehouse.objectstore import ObjectStore, StoredObject
from personal_data_warehouse_apple_messages.body import decode_message_body
from personal_data_warehouse_apple_messages.scanner import (
    AppleMessage,
    AppleMessageAttachment,
    AppleMessageChat,
    AppleMessageChatHandle,
    AppleMessageChatMessage,
    AppleMessageDeletedMessage,
    AppleMessageHandle,
    AppleMessagesSnapshot,
    file_sha256,
    scan_apple_messages_store,
    snapshot_apple_messages_store,
)
from personal_data_warehouse_apple_messages.state import AppleMessagesUploadState

OBJECT_PREFIX = "apple-messages"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox"
BATCH_RECORD_KIND = "apple_message_export_batch"
ATTACHMENT_KIND = "apple_message_attachment"


@dataclass(frozen=True)
class AppleMessagesUploadSummary:
    handles_seen: int
    chats_seen: int
    messages_seen: int
    attachments_seen: int
    records_selected: int
    records_skipped: int
    batches_uploaded: int
    attachments_uploaded: int
    attachment_bytes_uploaded: int
    attachments_deferred: int


@dataclass(frozen=True)
class PendingStateMark:
    source_type: str
    source_id: str
    fingerprint: str
    content_sha256: str = ""
    storage_key: str = ""


@dataclass(frozen=True)
class PendingAttachmentUpload:
    attachment: AppleMessageAttachment
    content_sha256: str
    object_key: str
    size_bytes: int


@dataclass(frozen=True)
class AttachmentUploadResult:
    attachment: AppleMessageAttachment
    content_sha256: str
    stored: StoredObject
    size_bytes: int


class AppleMessagesUploadRunner:
    def __init__(
        self,
        *,
        account: str,
        store_path: Path | str,
        object_store: ObjectStore | None = None,
        object_store_factory: Callable[[], ObjectStore] | None = None,
        logger,
        upload_state: AppleMessagesUploadState | None = None,
        now: Callable[[], datetime] | None = None,
        mode: str = "incremental",
        limit: int | None = None,
        attachment_bytes_per_run: int = 512 * 1024 * 1024,
        attachment_count_per_run: int = 200,
        workers: int = 1,
        before_upload_check: Callable[[], str | None] | None = None,
    ) -> None:
        if object_store is None and object_store_factory is None:
            raise ValueError("object_store or object_store_factory must be provided")
        if object_store is not None and object_store_factory is not None:
            raise ValueError("pass only one of object_store or object_store_factory")
        if mode not in {"full", "incremental"}:
            raise ValueError("mode must be 'full' or 'incremental'")
        self._account = account
        self._store_path = Path(store_path).expanduser()
        self._object_store = object_store
        self._object_store_factory = object_store_factory
        self._thread_local = threading.local()
        self._logger = logger
        self._upload_state = upload_state
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._mode = mode
        self._limit = limit
        self._attachment_bytes_per_run = max(0, attachment_bytes_per_run)
        self._attachment_count_per_run = max(0, attachment_count_per_run)
        self._workers = max(1, workers)
        self._before_upload_check = before_upload_check

    def sync(self) -> AppleMessagesUploadSummary:
        self._logger.info("Snapshotting Apple Messages store at %s", self._store_path)
        with tempfile.TemporaryDirectory(prefix="pdw-apple-messages-") as temp_dir:
            snapshot_path = snapshot_apple_messages_store(self._store_path, temp_dir)
            snapshot = scan_apple_messages_store(snapshot_path, messages_root=self._store_path.parent)

        exported_at = self._now()
        records, marks, skipped = self._selected_manifest_records(snapshot=snapshot, exported_at=exported_at)
        attachment_uploads, attachment_deferred = self._selected_attachment_uploads(snapshot.attachments)

        if self._limit is not None and len(records) > self._limit:
            deferred_records = records[self._limit :]
            records = records[: self._limit]
            keep_ids = {(mark.source_type, mark.source_id) for mark in state_marks_for_records(records)}
            marks = [mark for mark in marks if (mark.source_type, mark.source_id) in keep_ids]
            skipped += len(deferred_records)

        if (records or attachment_uploads) and self._before_upload_check is not None:
            skip_reason = self._before_upload_check()
            if skip_reason:
                self._logger.warning("Skipping Apple Messages upload: %s", skip_reason)
                return AppleMessagesUploadSummary(
                    handles_seen=len(snapshot.handles),
                    chats_seen=len(snapshot.chats),
                    messages_seen=len(snapshot.messages),
                    attachments_seen=len(snapshot.attachments),
                    records_selected=len(records),
                    records_skipped=skipped,
                    batches_uploaded=0,
                    attachments_uploaded=0,
                    attachment_bytes_uploaded=0,
                    attachments_deferred=len(records) + len(attachment_uploads) + attachment_deferred,
                )

        attachment_records, attachment_marks, attachment_summary = self._upload_attachment_backfill(
            attachment_uploads,
            exported_at=exported_at,
        )
        records.extend(attachment_records)
        marks.extend(attachment_marks)

        batches_uploaded = 0
        if records:
            stored = self._upload_batch(records=records, exported_at=exported_at)
            batches_uploaded = 1
            for mark in marks:
                self._mark_success(mark, now=exported_at)
            self._logger.info(
                "Uploaded Apple Messages batch %s with %s records",
                stored["storage_key"],
                len(records),
            )

        self._logger.info(
            "Apple Messages upload summary: handles=%s chats=%s messages=%s attachments=%s selected=%s skipped=%s batches=%s attachment_uploads=%s",
            len(snapshot.handles),
            len(snapshot.chats),
            len(snapshot.messages),
            len(snapshot.attachments),
            len(records),
            skipped,
            batches_uploaded,
            attachment_summary.attachments_uploaded,
        )
        return AppleMessagesUploadSummary(
            handles_seen=len(snapshot.handles),
            chats_seen=len(snapshot.chats),
            messages_seen=len(snapshot.messages),
            attachments_seen=len(snapshot.attachments),
            records_selected=len(records),
            records_skipped=skipped,
            batches_uploaded=batches_uploaded,
            attachments_uploaded=attachment_summary.attachments_uploaded,
            attachment_bytes_uploaded=attachment_summary.attachment_bytes_uploaded,
            attachments_deferred=attachment_deferred,
        )

    def _selected_manifest_records(
        self,
        *,
        snapshot: AppleMessagesSnapshot,
        exported_at: datetime,
    ) -> tuple[list[dict[str, object]], list[PendingStateMark], int]:
        records: list[dict[str, object]] = []
        marks: list[PendingStateMark] = []
        skipped = 0

        for source_type, source_id, payload in iter_snapshot_payloads(snapshot):
            fingerprint = json_sha256(payload)
            if self._mode == "incremental" and self._is_complete(source_type=source_type, source_id=source_id, fingerprint=fingerprint):
                skipped += 1
                continue
            records.append(envelope(account=self._account, exported_at=exported_at, record_type=source_type, record=payload))
            marks.append(PendingStateMark(source_type=source_type, source_id=source_id, fingerprint=fingerprint))

        for message in snapshot.messages:
            source_type = "message"
            source_id = message.message_id
            fingerprint = json_sha256(message_fingerprint_payload(message))
            if self._mode == "incremental" and self._is_complete(source_type=source_type, source_id=source_id, fingerprint=fingerprint):
                skipped += 1
                continue
            payload = message_payload(message)
            records.append(envelope(account=self._account, exported_at=exported_at, record_type=source_type, record=payload))
            marks.append(PendingStateMark(source_type=source_type, source_id=source_id, fingerprint=fingerprint))

        return records, marks, skipped

    def _selected_attachment_uploads(
        self,
        attachments: Iterable[AppleMessageAttachment],
    ) -> tuple[list[PendingAttachmentUpload], int]:
        selected: list[PendingAttachmentUpload] = []
        selected_bytes = 0
        deferred = 0
        seen_attachment_ids: set[str] = set()
        for attachment in attachments:
            if attachment.resolved_path is None or attachment.is_missing:
                continue
            if self._attachment_blob_already_uploaded(attachment):
                continue
            if self._attachment_count_per_run and len(selected) >= self._attachment_count_per_run:
                deferred += 1
                continue
            size_bytes = attachment.resolved_path.stat().st_size
            if self._attachment_bytes_per_run and selected and selected_bytes + size_bytes > self._attachment_bytes_per_run:
                deferred += 1
                continue
            content_sha256 = file_sha256(attachment.resolved_path)
            blob_source_id = attachment.attachment_id
            blob_fingerprint = content_sha256
            if blob_source_id in seen_attachment_ids:
                continue
            seen_attachment_ids.add(blob_source_id)
            if self._mode == "incremental" and self._is_complete(
                source_type="attachment_blob",
                source_id=blob_source_id,
                fingerprint=blob_fingerprint,
            ):
                continue
            object_key = attachment_object_key(attachment, content_sha256=content_sha256)
            selected.append(
                PendingAttachmentUpload(
                    attachment=attachment,
                    content_sha256=content_sha256,
                    object_key=object_key,
                    size_bytes=size_bytes,
                )
            )
            selected_bytes += size_bytes
        return selected, deferred

    def _attachment_blob_already_uploaded(self, attachment: AppleMessageAttachment) -> bool:
        if self._mode != "incremental" or self._upload_state is None:
            return False
        entry = self._upload_state.entry_for(source_type="attachment_blob", source_id=attachment.attachment_id)
        return bool(entry and entry.complete and entry.content_sha256)

    def _upload_attachment_backfill(
        self,
        attachments: list[PendingAttachmentUpload],
        *,
        exported_at: datetime,
    ) -> tuple[list[dict[str, object]], list[PendingStateMark], _AttachmentUploadSummary]:
        records: list[dict[str, object]] = []
        marks: list[PendingStateMark] = []
        uploaded_bytes = 0
        if attachments:
            self._logger.info("Uploading %s Apple Messages attachment(s) with %s worker(s)", len(attachments), self._workers)
        if self._workers == 1 or len(attachments) <= 1:
            results = [self._upload_attachment(upload) for upload in attachments]
        else:
            with ThreadPoolExecutor(max_workers=self._workers, thread_name_prefix="apple-messages") as executor:
                futures = [executor.submit(self._upload_attachment, upload) for upload in attachments]
                results = [future.result() for future in as_completed(futures)]

        for result in results:
            attachment = result.attachment
            stored = result.stored
            content_sha256 = result.content_sha256
            payload = attachment_payload(attachment, content_sha256=content_sha256, stored=stored)
            storage_source_id = attachment_source_id(attachment)
            blob_source_id = attachment.attachment_id
            blob_fingerprint = content_sha256
            storage_fingerprint = json_sha256(
                {
                    "attachment_id": attachment.attachment_id,
                    "message_id": attachment.message_id,
                    "content_sha256": content_sha256,
                    "storage_key": stored["storage_key"],
                }
            )
            records.append(
                envelope(account=self._account, exported_at=exported_at, record_type="attachment", record=payload)
            )
            marks.append(
                PendingStateMark(
                    source_type="attachment_blob",
                    source_id=blob_source_id,
                    fingerprint=blob_fingerprint,
                    content_sha256=content_sha256,
                    storage_key=stored["storage_key"],
                )
            )
            marks.append(
                PendingStateMark(
                    source_type="attachment_storage",
                    source_id=storage_source_id,
                    fingerprint=storage_fingerprint,
                    content_sha256=content_sha256,
                    storage_key=stored["storage_key"],
                )
            )
            uploaded_bytes += result.size_bytes
        return records, marks, _AttachmentUploadSummary(len(results), uploaded_bytes, 0)

    def _upload_attachment(self, upload: PendingAttachmentUpload) -> AttachmentUploadResult:
        attachment = upload.attachment
        if attachment.resolved_path is None:
            raise RuntimeError(f"Attachment {attachment.attachment_id} has no local file path")
        stored = self._object_store_for_thread().put_file(
            path=attachment.resolved_path,
            object_key=upload.object_key,
            content_sha256=upload.content_sha256,
            content_type=attachment.content_type,
            kind=ATTACHMENT_KIND,
            app_properties={
                "attachment_guid": attachment.attachment_id,
                "message_guid": attachment.message_id,
            },
        )
        return AttachmentUploadResult(
            attachment=attachment,
            content_sha256=upload.content_sha256,
            stored=stored,
            size_bytes=upload.size_bytes,
        )

    def _upload_batch(self, *, records: list[dict[str, object]], exported_at: datetime) -> StoredObject:
        encoded = gzip_jsonl(records)
        batch_sha = hashlib.sha256(encoded).hexdigest()
        object_key = batch_object_key(exported_at=exported_at, batch_sha256=batch_sha)
        with tempfile.NamedTemporaryFile(suffix=".jsonl.gz") as file:
            file.write(encoded)
            file.flush()
            return self._object_store_for_thread().put_file(
                path=Path(file.name),
                object_key=object_key,
                content_sha256=batch_sha,
                content_type="application/gzip",
                kind=BATCH_RECORD_KIND,
                app_properties={"batch_sha256": batch_sha, "exported_at": exported_at.astimezone(UTC).isoformat()},
            )

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

    def _is_complete(self, *, source_type: str, source_id: str, fingerprint: str) -> bool:
        if self._upload_state is None:
            return False
        return self._upload_state.is_complete(source_type=source_type, source_id=source_id, fingerprint=fingerprint)

    def _mark_success(self, mark: PendingStateMark, *, now: datetime) -> None:
        if self._upload_state is None:
            return
        self._upload_state.mark_success(
            source_type=mark.source_type,
            source_id=mark.source_id,
            fingerprint=mark.fingerprint,
            content_sha256=mark.content_sha256,
            storage_key=mark.storage_key,
            now=now,
        )


@dataclass(frozen=True)
class _AttachmentUploadSummary:
    attachments_uploaded: int
    attachment_bytes_uploaded: int
    attachments_deferred: int


def iter_snapshot_payloads(snapshot: AppleMessagesSnapshot):
    for handle in snapshot.handles:
        yield "handle", handle.handle_id, handle_payload(handle)
    for chat in snapshot.chats:
        yield "chat", chat.chat_id, chat_payload(chat)
    for chat_handle in snapshot.chat_handles:
        yield "chat_handle", f"{chat_handle.chat_id}:{chat_handle.handle_id}", chat_handle_payload(chat_handle)
    for chat_message in snapshot.chat_messages:
        yield "chat_message", f"{chat_message.chat_id}:{chat_message.message_id}", chat_message_payload(chat_message)
    for attachment in snapshot.attachments:
        yield "attachment", attachment_source_id(attachment), attachment_payload(attachment)
    for deleted in snapshot.deleted_messages:
        yield "deleted_message", deleted.message_id, deleted_message_payload(deleted)


def state_marks_for_records(records: list[dict[str, object]]) -> list[PendingStateMark]:
    marks: list[PendingStateMark] = []
    for record in records:
        record_type = str(record.get("record_type", ""))
        payload = record.get("record")
        if not isinstance(payload, dict):
            continue
        source_id = record_source_id(record_type, payload)
        if source_id:
            marks.append(PendingStateMark(source_type=record_type, source_id=source_id, fingerprint=json_sha256(payload)))
    return marks


def record_source_id(record_type: str, payload: dict[str, object]) -> str:
    if record_type == "handle":
        return str(payload.get("handle_id", ""))
    if record_type == "chat":
        return str(payload.get("chat_id", ""))
    if record_type == "chat_handle":
        return f"{payload.get('chat_id', '')}:{payload.get('handle_id', '')}"
    if record_type == "chat_message":
        return f"{payload.get('chat_id', '')}:{payload.get('message_id', '')}"
    if record_type == "message":
        return str(payload.get("message_id", ""))
    if record_type == "attachment":
        return f"{payload.get('attachment_id', '')}:{payload.get('message_id', '')}"
    if record_type == "deleted_message":
        return str(payload.get("message_id", ""))
    return ""


def handle_payload(handle: AppleMessageHandle) -> dict[str, object]:
    return {
        "handle_id": handle.handle_id,
        "handle_rowid": handle.handle_rowid,
        "address": handle.address,
        "country": handle.country,
        "service": handle.service,
        "uncanonicalized_id": handle.uncanonicalized_id,
        "person_centric_id": handle.person_centric_id,
        "raw": handle.raw,
    }


def chat_payload(chat: AppleMessageChat) -> dict[str, object]:
    return {
        "chat_id": chat.chat_id,
        "chat_rowid": chat.chat_rowid,
        "guid": chat.guid,
        "chat_identifier": chat.chat_identifier,
        "service_name": chat.service_name,
        "display_name": chat.display_name,
        "room_name": chat.room_name,
        "account_login": chat.account_login,
        "style": chat.style,
        "state": chat.state,
        "is_archived": chat.is_archived,
        "is_filtered": chat.is_filtered,
        "is_recovered": chat.is_recovered,
        "is_pending_review": chat.is_pending_review,
        "last_read_message_at": chat.last_read_message_at.isoformat(),
        "raw": chat.raw,
    }


def chat_handle_payload(chat_handle: AppleMessageChatHandle) -> dict[str, object]:
    return {"chat_id": chat_handle.chat_id, "handle_id": chat_handle.handle_id, "raw": chat_handle.raw}


def chat_message_payload(chat_message: AppleMessageChatMessage) -> dict[str, object]:
    return {
        "chat_id": chat_message.chat_id,
        "message_id": chat_message.message_id,
        "message_date": chat_message.message_date.isoformat(),
        "message_date_ns": chat_message.message_date_ns,
        "raw": chat_message.raw,
    }


def message_fingerprint_payload(message: AppleMessage) -> dict[str, object]:
    return {
        "message_id": message.message_id,
        "message_rowid": message.message_rowid,
        "text": message.text,
        "attributed_body_sha256": message.attributed_body_sha256,
        "date_ns": message.date_ns,
        "date_edited": message.date_edited.isoformat(),
        "date_retracted": message.date_retracted.isoformat(),
        "raw": message.raw,
    }


def message_payload(message: AppleMessage) -> dict[str, object]:
    body = decode_message_body(text=message.text, attributed_body=message.attributed_body)
    return {
        "message_id": message.message_id,
        "message_rowid": message.message_rowid,
        "handle_id": message.handle_id,
        "service": message.service,
        "message_account": message.message_account,
        "body_text": body.text,
        "body_source": body.source,
        "body_decode_status": body.status,
        "body_decode_error": body.error,
        "attributed_body_sha256": body.attributed_body_sha256,
        "subject": message.subject,
        "country": message.country,
        "message_type": message.message_type,
        "message_item_type": message.item_type,
        "is_from_me": message.is_from_me,
        "is_read": message.is_read,
        "is_sent": message.is_sent,
        "is_delivered": message.is_delivered,
        "is_finished": message.is_finished,
        "is_system_message": message.is_system_message,
        "is_service_message": message.is_service_message,
        "is_forward": message.is_forward,
        "is_empty": message.is_empty,
        "is_audio_message": message.is_audio_message,
        "is_played": message.is_played,
        "cache_has_attachments": message.cache_has_attachments,
        "has_unseen_mention": message.has_unseen_mention,
        "is_spam": message.is_spam,
        "reply_to_guid": message.reply_to_guid,
        "associated_message_guid": message.associated_message_guid,
        "associated_message_type": message.associated_message_type,
        "associated_message_emoji": message.associated_message_emoji,
        "balloon_bundle_id": message.balloon_bundle_id,
        "group_title": message.group_title,
        "group_action_type": message.group_action_type,
        "message_action_type": message.message_action_type,
        "message_source": message.message_source,
        "expressive_send_style_id": message.expressive_send_style_id,
        "message_at": message.message_at.isoformat(),
        "date_ns": message.date_ns,
        "date_read": message.date_read.isoformat(),
        "date_delivered": message.date_delivered.isoformat(),
        "date_played": message.date_played.isoformat(),
        "date_edited": message.date_edited.isoformat(),
        "date_retracted": message.date_retracted.isoformat(),
        "date_recovered": message.date_recovered.isoformat(),
        "is_deleted": False,
        "raw": message.raw,
    }


def attachment_payload(
    attachment: AppleMessageAttachment,
    *,
    content_sha256: str = "",
    stored: StoredObject | None = None,
) -> dict[str, object]:
    payload = {
        "attachment_id": attachment.attachment_id,
        "attachment_rowid": attachment.attachment_rowid,
        "message_id": attachment.message_id,
        "guid": attachment.guid,
        "original_guid": attachment.original_guid,
        "filename": attachment.filename,
        "transfer_name": attachment.transfer_name,
        "content_type": attachment.content_type,
        "uti": attachment.uti,
        "mime_type": attachment.mime_type,
        "total_bytes": attachment.total_bytes,
        "size_bytes": attachment.size_bytes,
        "content_sha256": content_sha256,
        "is_missing": attachment.is_missing,
        "error": attachment.error,
        "is_outgoing": attachment.is_outgoing,
        "is_sticker": attachment.is_sticker,
        "hide_attachment": attachment.hide_attachment,
        "transfer_state": attachment.transfer_state,
        "created_at": attachment.created_at.isoformat(),
        "start_at": attachment.start_at.isoformat(),
        "raw": attachment.raw,
    }
    if stored is not None:
        payload["file"] = stored
    return payload


def deleted_message_payload(deleted: AppleMessageDeletedMessage) -> dict[str, object]:
    return {
        "message_id": deleted.message_id,
        "deleted_at": deleted.deleted_at.isoformat(),
        "is_deleted": True,
        "raw": deleted.raw,
    }


def attachment_source_id(attachment: AppleMessageAttachment) -> str:
    return f"{attachment.attachment_id}:{attachment.message_id}"


def envelope(*, account: str, exported_at: datetime, record_type: str, record: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": "apple_messages",
        "account": account,
        "exported_at": exported_at.astimezone(UTC).isoformat(),
        "record_type": record_type,
        "record": record,
    }


def gzip_jsonl(records: list[dict[str, object]]) -> bytes:
    lines = b"".join(json.dumps(record, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8") + b"\n" for record in records)
    with tempfile.NamedTemporaryFile() as file:
        with gzip.GzipFile(fileobj=file, mode="wb", mtime=0) as gzip_file:
            gzip_file.write(lines)
        file.flush()
        file.seek(0)
        return file.read()


def json_sha256(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def batch_object_key(*, exported_at: datetime, batch_sha256: str) -> str:
    exported = exported_at.astimezone(UTC)
    stamp = exported.strftime("%Y%m%dT%H%M%SZ")
    return f"{INBOX_PREFIX}/batches/{exported.year:04d}/{exported.month:02d}/{stamp}-{batch_sha256}.jsonl.gz"


def attachment_object_key(attachment: AppleMessageAttachment, *, content_sha256: str) -> str:
    created = attachment.created_at.astimezone(UTC)
    if created.year == 1970:
        created = datetime.now(tz=UTC)
    suffix = Path(attachment.filename).suffix or ".bin"
    return (
        f"{INBOX_PREFIX}/attachments/{created.year:04d}/{created.month:02d}/"
        f"{created.date().isoformat()}-{safe_object_key_part(attachment.attachment_id)}-{content_sha256}{suffix}"
    )


def safe_object_key_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip(".-")[:120] or "untitled"
