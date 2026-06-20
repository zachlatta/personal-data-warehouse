"""Buffer WhatsApp client records and flush them through app ingest.

Mirrors the Apple Messages upload format: JSONL.gz envelope batches and media
blobs are posted to the app, which writes ``whatsapp/inbox/batches/`` and
``whatsapp/inbox/media/`` objects consumed by the whatsapp_drive_ingest Dagster
sensor.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import gzip
import hashlib
import json
import tempfile
import threading
from typing import Any

from personal_data_warehouse_whatsapp.events import MediaRecord
from personal_data_warehouse_whatsapp.state import WhatsAppUploadState

# Storage reference returned by the ingest client (mirrors the warehouse
# storage_* columns); the app owns the keys and tags behind it.
StoredObject = dict[str, str]


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
        ingest_client,
        upload_state: WhatsAppUploadState | None,
        logger,
        now: Callable[[], datetime] | None = None,
        media_bytes_per_flush: int = 512 * 1024 * 1024,
        media_count_per_flush: int = 200,
    ) -> None:
        # Uploads go through the app's ingest endpoints only.
        if ingest_client is None:
            raise ValueError("ingest_client is required")
        self._account = account
        self._ingest_client = ingest_client
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

    def add_chat_participant(self, payload: dict[str, Any]) -> None:
        source_id = f"{payload.get('chat_id', '')}:{payload.get('participant_jid', '')}"
        self._enqueue("chat_participant", source_id, payload)

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
            stored = self._put_media_bytes(
                content,
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
        content_type: str,
        payload: dict[str, Any],
    ) -> StoredObject:
        # The app owns the object key, kind, and pdw_* tags.
        return self._ingest_client.upload_whatsapp_media(
            content,
            chat_id=str(payload.get("chat_id", "")),
            message_id=str(payload.get("message_id", "")),
            content_type=content_type,
            message_at=str(payload.get("message_at", "")),
            filename=str(payload.get("filename", "")),
            mime_type=str(payload.get("mime_type", "")),
        )

    def _upload_batch(self, *, records: list[dict[str, Any]], exported_at: datetime) -> StoredObject:
        encoded = gzip_jsonl(records)
        return self._ingest_client.upload_whatsapp_batch(
            encoded, exported_at=exported_at.astimezone(UTC).isoformat()
        )

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
