"""Buffer WhatsApp client records and flush them as Drive inbox batches.

Mirrors the Apple Messages upload format: JSONL.gz envelope batches under
whatsapp/inbox/batches/ and media blobs under whatsapp/inbox/media/, consumed
by the whatsapp_drive_ingest Dagster sensor.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import gzip
import hashlib
import json
import mimetypes
import re
import tempfile
import threading
from typing import Any

from personal_data_warehouse.objectstore import ObjectStore, StoredObject
from personal_data_warehouse_whatsapp.events import MediaRecord
from personal_data_warehouse_whatsapp.state import WhatsAppUploadState

OBJECT_PREFIX = "whatsapp"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox"
BATCH_RECORD_KIND = "whatsapp_export_batch"
MEDIA_KIND = "whatsapp_media_item"


@dataclass(frozen=True)
class FlushSummary:
    records_selected: int
    records_skipped: int
    batches_uploaded: int
    media_uploaded: int
    media_bytes_uploaded: int
    media_deferred: int


@dataclass
class _PendingMedia:
    media: MediaRecord
    downloader: Callable[[Any], bytes | None]


class WhatsAppBatcher:
    """Thread-safe record buffer with incremental-state dedupe.

    Event handlers (Go callback threads) enqueue payloads; a single flusher
    thread calls flush(), which owns all upload-state access.
    """

    def __init__(
        self,
        *,
        account: str,
        object_store: ObjectStore | None = None,
        object_store_factory: Callable[[], ObjectStore] | None = None,
        upload_state: WhatsAppUploadState | None,
        logger,
        now: Callable[[], datetime] | None = None,
        media_bytes_per_flush: int = 512 * 1024 * 1024,
        media_count_per_flush: int = 200,
    ) -> None:
        if object_store is None and object_store_factory is None:
            raise ValueError("object_store or object_store_factory must be provided")
        if object_store is not None and object_store_factory is not None:
            raise ValueError("pass only one of object_store or object_store_factory")
        self._account = account
        self._object_store = object_store
        self._object_store_factory = object_store_factory
        self._upload_state = upload_state
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._media_bytes_per_flush = max(0, media_bytes_per_flush)
        self._media_count_per_flush = max(0, media_count_per_flush)
        self._lock = threading.Lock()
        self._pending: list[tuple[str, str, dict[str, Any]]] = []
        self._pending_media: list[_PendingMedia] = []

    def add_chat(self, payload: dict[str, Any]) -> None:
        self._enqueue("chat", str(payload.get("chat_id", "")), payload)

    def add_contact(self, payload: dict[str, Any]) -> None:
        self._enqueue("contact", str(payload.get("jid", "")), payload)

    def add_message(self, payload: dict[str, Any]) -> None:
        source_id = f"{payload.get('chat_id', '')}:{payload.get('message_id', '')}"
        self._enqueue("message", source_id, payload)

    def add_media(self, media: MediaRecord, *, downloader: Callable[[Any], bytes | None] | None) -> None:
        source_id = f"{media.payload.get('chat_id', '')}:{media.payload.get('message_id', '')}"
        self._enqueue("media_item", source_id, dict(media.payload))
        if downloader is not None and media.e2e_message is not None:
            with self._lock:
                self._pending_media.append(_PendingMedia(media=media, downloader=downloader))

    def pending_counts(self) -> tuple[int, int]:
        with self._lock:
            return len(self._pending), len(self._pending_media)

    def _enqueue(self, source_type: str, source_id: str, payload: dict[str, Any]) -> None:
        if not source_id or source_id == ":":
            return
        with self._lock:
            self._pending.append((source_type, source_id, payload))

    def flush(self) -> FlushSummary:
        with self._lock:
            pending = self._pending
            pending_media = self._pending_media
            self._pending = []
            self._pending_media = []

        try:
            return self._flush_pending(pending=pending, pending_media=pending_media)
        except Exception:
            self._requeue(pending=pending, pending_media=pending_media)
            raise

    def _flush_pending(
        self,
        *,
        pending: list[tuple[str, str, dict[str, Any]]],
        pending_media: list[_PendingMedia],
    ) -> FlushSummary:

        exported_at = self._now()
        records: list[dict[str, Any]] = []
        marks: list[tuple[str, str, str, str, str]] = []  # type, id, fingerprint, sha, key
        skipped = 0
        seen_in_flush: set[tuple[str, str]] = set()
        for source_type, source_id, payload in pending:
            fingerprint = json_sha256(payload)
            if (source_type, source_id) in seen_in_flush:
                continue
            if self._is_complete(source_type=source_type, source_id=source_id, fingerprint=fingerprint):
                skipped += 1
                continue
            seen_in_flush.add((source_type, source_id))
            records.append(envelope(account=self._account, exported_at=exported_at, record_type=source_type, record=payload))
            marks.append((source_type, source_id, fingerprint, "", ""))

        media_records, media_marks, media_uploaded, media_bytes, media_deferred = self._upload_media(
            pending_media, exported_at=exported_at
        )
        records.extend(media_records)
        marks.extend(media_marks)

        batches_uploaded = 0
        if records:
            stored = self._upload_batch(records=records, exported_at=exported_at)
            batches_uploaded = 1
            for source_type, source_id, fingerprint, content_sha256, storage_key in marks:
                self._mark_success(
                    source_type=source_type,
                    source_id=source_id,
                    fingerprint=fingerprint,
                    content_sha256=content_sha256,
                    storage_key=storage_key,
                    now=exported_at,
                )
            self._logger.info(
                "Uploaded WhatsApp batch %s with %s records",
                stored["storage_key"],
                len(records),
            )
        return FlushSummary(
            records_selected=len(records),
            records_skipped=skipped,
            batches_uploaded=batches_uploaded,
            media_uploaded=media_uploaded,
            media_bytes_uploaded=media_bytes,
            media_deferred=media_deferred,
        )

    def _requeue(
        self,
        *,
        pending: list[tuple[str, str, dict[str, Any]]],
        pending_media: list[_PendingMedia],
    ) -> None:
        with self._lock:
            self._pending = pending + self._pending
            self._pending_media = pending_media + self._pending_media

    def _upload_media(
        self,
        pending_media: list[_PendingMedia],
        *,
        exported_at: datetime,
    ) -> tuple[list[dict[str, Any]], list[tuple[str, str, str, str, str]], int, int, int]:
        records: list[dict[str, Any]] = []
        marks: list[tuple[str, str, str, str, str]] = []
        uploaded = 0
        uploaded_bytes = 0
        deferred = 0
        for index, item in enumerate(pending_media):
            payload = dict(item.media.payload)
            source_id = f"{payload.get('chat_id', '')}:{payload.get('message_id', '')}"
            if self._media_count_per_flush and uploaded >= self._media_count_per_flush:
                deferred += 1
                self._requeue_media(pending_media[index:])
                break
            entry = (
                self._upload_state.entry_for(source_type="media_blob", source_id=source_id)
                if self._upload_state
                else None
            )
            if entry and entry.complete and entry.content_sha256:
                continue
            try:
                content = item.downloader(item.media.e2e_message)
            except Exception as exc:  # noqa: BLE001 - record the failure, keep going
                self._logger.warning("WhatsApp media download failed for %s: %s", source_id, exc)
                content = None
            if not content:
                continue
            if self._media_bytes_per_flush and uploaded_bytes and uploaded_bytes + len(content) > self._media_bytes_per_flush:
                deferred += 1
                self._requeue_media(pending_media[index:])
                break
            content_sha256 = hashlib.sha256(content).hexdigest()
            object_key = media_object_key(payload, content_sha256=content_sha256, fallback_now=exported_at)
            stored = self._put_media_bytes(
                content,
                object_key=object_key,
                content_sha256=content_sha256,
                content_type=str(payload.get("mime_type", "")) or "application/octet-stream",
                payload=payload,
            )
            payload.update(
                {
                    "size_bytes": len(content),
                    "content_sha256": content_sha256,
                    "is_missing": False,
                    "error": "",
                    "file": stored,
                }
            )
            records.append(envelope(account=self._account, exported_at=exported_at, record_type="media_item", record=payload))
            marks.append(("media_blob", source_id, content_sha256, content_sha256, stored["storage_key"]))
            marks.append(("media_item", source_id, json_sha256(payload), content_sha256, stored["storage_key"]))
            uploaded += 1
            uploaded_bytes += len(content)
        return records, marks, uploaded, uploaded_bytes, deferred

    def _requeue_media(self, items: list[_PendingMedia]) -> None:
        with self._lock:
            self._pending_media = list(items) + self._pending_media

    def _put_media_bytes(
        self,
        content: bytes,
        *,
        object_key: str,
        content_sha256: str,
        content_type: str,
        payload: dict[str, Any],
    ) -> StoredObject:
        with tempfile.NamedTemporaryFile() as file:
            file.write(content)
            file.flush()
            return self._store().put_file(
                path=Path(file.name),
                object_key=object_key,
                content_sha256=content_sha256,
                content_type=content_type,
                kind=MEDIA_KIND,
                app_properties={
                    "chat_id": str(payload.get("chat_id", "")),
                    "message_id": str(payload.get("message_id", "")),
                },
            )

    def _upload_batch(self, *, records: list[dict[str, Any]], exported_at: datetime) -> StoredObject:
        encoded = gzip_jsonl(records)
        batch_sha = hashlib.sha256(encoded).hexdigest()
        object_key = batch_object_key(exported_at=exported_at, batch_sha256=batch_sha)
        with tempfile.NamedTemporaryFile(suffix=".jsonl.gz") as file:
            file.write(encoded)
            file.flush()
            return self._store().put_file(
                path=Path(file.name),
                object_key=object_key,
                content_sha256=batch_sha,
                content_type="application/gzip",
                kind=BATCH_RECORD_KIND,
                app_properties={"batch_sha256": batch_sha, "exported_at": exported_at.astimezone(UTC).isoformat()},
            )

    def _store(self) -> ObjectStore:
        if self._object_store is None:
            assert self._object_store_factory is not None
            self._object_store = self._object_store_factory()
        return self._object_store

    def _is_complete(self, *, source_type: str, source_id: str, fingerprint: str) -> bool:
        if self._upload_state is None:
            return False
        return self._upload_state.is_complete(source_type=source_type, source_id=source_id, fingerprint=fingerprint)

    def _mark_success(
        self,
        *,
        source_type: str,
        source_id: str,
        fingerprint: str,
        content_sha256: str,
        storage_key: str,
        now: datetime,
    ) -> None:
        if self._upload_state is None:
            return
        self._upload_state.mark_success(
            source_type=source_type,
            source_id=source_id,
            fingerprint=fingerprint,
            content_sha256=content_sha256,
            storage_key=storage_key,
            now=now,
        )


def envelope(*, account: str, exported_at: datetime, record_type: str, record: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "source": "whatsapp",
        "account": account,
        "exported_at": exported_at.astimezone(UTC).isoformat(),
        "record_type": record_type,
        "record": record,
    }


def gzip_jsonl(records: list[dict[str, Any]]) -> bytes:
    lines = b"".join(json.dumps(record, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8") + b"\n" for record in records)
    with tempfile.NamedTemporaryFile() as file:
        with gzip.GzipFile(fileobj=file, mode="wb", mtime=0) as gzip_file:
            gzip_file.write(lines)
        file.flush()
        file.seek(0)
        return file.read()


def json_sha256(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def batch_object_key(*, exported_at: datetime, batch_sha256: str) -> str:
    exported = exported_at.astimezone(UTC)
    stamp = exported.strftime("%Y%m%dT%H%M%SZ")
    return f"{INBOX_PREFIX}/batches/{exported.year:04d}/{exported.month:02d}/{stamp}-{batch_sha256}.jsonl.gz"


def media_object_key(payload: dict[str, Any], *, content_sha256: str, fallback_now: datetime) -> str:
    occurred = parse_iso_datetime(str(payload.get("message_at", ""))) or fallback_now
    occurred = occurred.astimezone(UTC)
    if occurred.year <= 1970:
        occurred = fallback_now.astimezone(UTC)
    filename = str(payload.get("filename", ""))
    suffix = Path(filename).suffix if filename else ""
    if not suffix:
        suffix = mimetypes.guess_extension(str(payload.get("mime_type", "")) or "") or ".bin"
    part = safe_object_key_part(f"{payload.get('chat_id', '')}-{payload.get('message_id', '')}")
    return (
        f"{INBOX_PREFIX}/media/{occurred.year:04d}/{occurred.month:02d}/"
        f"{occurred.date().isoformat()}-{part}-{content_sha256}{suffix}"
    )


def parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def safe_object_key_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip(".-")[:120] or "untitled"
