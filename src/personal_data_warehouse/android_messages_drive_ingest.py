"""Promote Android message batches from the Drive inbox into the warehouse.

The phone's uploader (``pdw ingest android-messages``, run as root on the
Pixel) reads Google Messages' database and posts gzipped JSONL batches of
every SMS, MMS and RCS message since the switch from the iPhone to the app's
/ingest/android-messages/batch endpoint, which writes them into the Drive
inbox. Each batch is written and promoted to the library on its own. Rows
upsert by message key, so a message re-sent with a new status replaces its
row.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import islice
from typing import Any

from personal_data_warehouse.browser_history_drive_ingest import (
    batch_records,
    int_value,
    load_jsonl_gz_object,
    nested_mapping,
    parse_datetime,
)
from personal_data_warehouse.objectstore import ObjectStore

OBJECT_PREFIX = "android-messages"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox/"
LIBRARY_PREFIX = f"{OBJECT_PREFIX}/library/"
BATCH_KIND = "android_message_export_batch"
SOURCE = "android_messages"
BLOB_KIND = "android_message_blob"
RECORD_TYPE = "message"

# Google Messages' message_protocol values.
PROTOCOL_NAMES = {0: "sms", 1: "mms", 3: "rcs"}
# message_status: below 100 an outgoing message, 100-199 an incoming one, 200
# and up a conversation event (encryption turned on, someone joined) that
# carries no text.
_INCOMING_STATUS = 100
_EVENT_STATUS = 200


@dataclass(frozen=True)
class AndroidMessagesDriveIngestSummary:
    batches_seen: int
    messages_written: int
    files_promoted: int


class AndroidMessagesDriveIngestRunner:
    def __init__(
        self,
        *,
        warehouse,
        batch_source: Callable[[], Iterable[Mapping[str, Any]]],
        object_store: ObjectStore | None = None,
        logger,
        max_batches: int = 0,
    ) -> None:
        self._warehouse = warehouse
        self._batch_source = batch_source
        self._object_store = object_store
        self._logger = logger
        self._max_batches = max_batches

    def sync(self) -> AndroidMessagesDriveIngestSummary:
        self._warehouse.ensure_android_messages_tables()
        batches = 0
        written = 0
        promoted = 0
        source = iter(self._batch_source())
        if self._max_batches:
            source = islice(source, self._max_batches)
        for batch in source:
            batches += 1
            by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
            for envelope in batch_records(batch):
                if str(envelope.get("record_type", "")) != RECORD_TYPE:
                    continue
                row = record_to_message_row(envelope)
                if not row["message_key"]:
                    continue
                by_key[(row["account"], row["device"], row["message_key"])] = row
            rows = list(by_key.values())
            self._warehouse.insert_android_messages(rows)
            written += len(rows)
            if self._object_store is not None:
                batch_file = nested_mapping(batch, "batch_file")
                target_key = library_storage_key(str(batch_file.get("storage_key", "")))
                if batch_file.get("storage_file_id") and target_key:
                    self._object_store.move_object(batch_file, new_object_key=target_key)
                    promoted += 1
        self._logger.info("Ingested %s Android message batches and %s message rows", batches, written)
        return AndroidMessagesDriveIngestSummary(
            batches_seen=batches, messages_written=written, files_promoted=promoted
        )


def iter_batch_payloads(
    *, object_store: ObjectStore, stage: str = "inbox"
) -> Iterable[Mapping[str, Any]]:
    """Yield inbox batches oldest export first, loading one at a time."""
    listings = sorted(
        object_store.list_objects(kind=BATCH_KIND, stage=stage),
        key=lambda listing: (
            parse_datetime(listing.app_properties.get("exported_at")),
            listing.filename,
        ),
    )
    for listing in listings:
        exported_at = parse_datetime(listing.app_properties.get("exported_at"))
        storage_key = f"{OBJECT_PREFIX}/{stage}/batches/{exported_at:%Y/%m}/{listing.filename}"
        yield {
            "schema_version": 1,
            "source": SOURCE,
            "batch_file": {
                **listing.ref,
                "storage_key": storage_key,
                "content_sha256": str(listing.app_properties.get("content_sha256", "")),
            },
            "records": list(load_jsonl_gz_object(object_store, listing.ref)),
        }


def has_batch_payloads(*, object_store: ObjectStore, stage: str = "inbox") -> bool:
    return object_store.find_object(kind=BATCH_KIND, stage=stage) is not None


def record_to_message_row(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Map one uploader record (a row of Messages' database with its parts
    and participants) to a row; the whole record is kept as raw_json."""
    record = nested_mapping(envelope, "record")
    exported_at = parse_datetime(envelope.get("exported_at"))
    protocol = int_value(record.get("message_protocol"))
    status = int_value(record.get("message_status"))
    parts = [p for p in record.get("parts") or [] if isinstance(p, Mapping)]
    participants = [p for p in record.get("participants") or [] if isinstance(p, Mapping)]
    texts = [
        str(p.get("text") or "")
        for p in parts
        if str(p.get("content_type") or "").startswith("text/plain") and p.get("text")
    ]
    attachments = [p for p in parts if not str(p.get("content_type") or "").startswith("text/")]
    is_from_me = int(0 <= status < _INCOMING_STATUS)
    if status >= _EVENT_STATUS:
        kind = "system"
    elif attachments:
        kind = "attachment"
    elif texts:
        kind = "text"
    else:
        kind = "other"
    participant_count = int_value(record.get("participant_count")) or len(participants)
    return {
        "account": str(envelope.get("account", "")),
        "device": str(envelope.get("device", "")),
        "message_key": str(record.get("message_key", "") or ""),
        "bugle_id": int_value(record.get("bugle_id")),
        "chat_key": str(record.get("chat_key", "") or ""),
        "chat_name": str(record.get("conversation_name", "") or ""),
        "participants": [dict(p) for p in participants],
        "participant_count": participant_count,
        "is_group": int(participant_count > 1),
        "protocol": protocol,
        "protocol_name": PROTOCOL_NAMES.get(protocol, f"protocol_{protocol}"),
        "bugle_status": status,
        "is_from_me": is_from_me,
        "message_kind": kind,
        "sender_address": str(record.get("sender_address", "") or ""),
        "sender_name": str(record.get("sender_name", "") or ""),
        "self_address": str(record.get("self_address", "") or ""),
        "sim_name": str(record.get("sim_name", "") or ""),
        "message_at": epoch_ms(record.get("received_timestamp_ms")),
        "sent_at": epoch_ms(record.get("sent_timestamp_ms")),
        "subject": str(record.get("subject", "") or ""),
        "body_text": "\n".join(texts),
        "parts": [dict(p) for p in parts],
        "attachment_count": len(attachments),
        "is_read": int(bool(int_value(record.get("read")))),
        "is_seen": int(bool(int_value(record.get("seen")))),
        "is_hidden": int(bool(int_value(record.get("is_hidden")))),
        "telephony_uri": str(record.get("telephony_uri", "") or ""),
        "rcs_message_id": str(record.get("rcs_message_id", "") or ""),
        "raw_json": dict(record),
        "exported_at": exported_at,
        "synced_at": exported_at,
        "sync_version": int(exported_at.timestamp() * 1_000_000),
    }


def epoch_ms(value: object) -> datetime:
    millis = int_value(value)
    if millis <= 0:
        return datetime.fromtimestamp(0, tz=UTC)
    return datetime.fromtimestamp(millis / 1000, tz=UTC)


def library_storage_key(storage_key: str) -> str:
    if storage_key.startswith(INBOX_PREFIX):
        return LIBRARY_PREFIX + storage_key[len(INBOX_PREFIX) :]
    return storage_key
