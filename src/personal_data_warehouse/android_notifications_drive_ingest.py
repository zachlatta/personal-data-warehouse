"""Promote Android notification batches from the Drive inbox into the warehouse.

The phone's uploader (``pdw ingest android-notifications``, run as root on
the Pixel) posts gzipped JSONL batches of the PDW notification listener's
events to the app's /ingest/android-notifications/batch endpoint, which
writes them into the Drive inbox. Each batch is written and promoted to the
library on its own. Rows upsert by the listener's event uuid, so a batch the
phone re-sent after a crash lands once.
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

OBJECT_PREFIX = "android-notifications"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox/"
LIBRARY_PREFIX = f"{OBJECT_PREFIX}/library/"
BATCH_KIND = "android_notification_export_batch"
SOURCE = "android_notifications"
BLOB_KIND = "android_notification_blob"
RECORD_TYPE = "notification_event"

_STRING_FIELDS = (
    "event_type",
    "notification_key",
    "package_name",
    "app_label",
    "channel_id",
    "channel_name",
    "category",
    "group_key",
    "sort_key",
    "title",
    "text",
    "sub_text",
    "big_text",
    "info_text",
    "summary_text",
    "conversation_title",
    "removal_reason_name",
    "content_sha256",
)
_FLAG_FIELDS = (
    "is_group_summary",
    "is_ongoing",
    "is_clearable",
    "is_conversation",
    "is_group_conversation",
)
_INT_FIELDS = (
    "notification_id",
    "importance",
    "flags",
    "visibility",
    "removal_reason",
    "lifespan_ms",
    "freshness_ms",
    "exposure_ms",
    "action_index",
)
_LIST_FIELDS = ("text_lines", "messages", "people", "actions")


@dataclass(frozen=True)
class AndroidNotificationsDriveIngestSummary:
    batches_seen: int
    events_written: int
    files_promoted: int


class AndroidNotificationsDriveIngestRunner:
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

    def sync(self) -> AndroidNotificationsDriveIngestSummary:
        self._warehouse.ensure_android_notifications_tables()
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
                row = record_to_event_row(envelope)
                if not row["event_id"]:
                    continue
                by_key[(row["account"], row["device"], row["event_id"])] = row
            rows = list(by_key.values())
            self._warehouse.insert_android_notification_events(rows)
            written += len(rows)
            if self._object_store is not None:
                batch_file = nested_mapping(batch, "batch_file")
                target_key = library_storage_key(str(batch_file.get("storage_key", "")))
                if batch_file.get("storage_file_id") and target_key:
                    self._object_store.move_object(batch_file, new_object_key=target_key)
                    promoted += 1
        self._logger.info(
            "Ingested %s Android notification batches and %s event rows", batches, written
        )
        return AndroidNotificationsDriveIngestSummary(
            batches_seen=batches, events_written=written, files_promoted=promoted
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


def record_to_event_row(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Map one listener event to a row. The listener's names are the columns,
    except for its epoch-millisecond times and the few renamed below; the
    whole event is kept as raw_json."""
    record = nested_mapping(envelope, "record")
    exported_at = parse_datetime(envelope.get("exported_at"))
    row: dict[str, Any] = {
        "account": str(envelope.get("account", "")),
        "device": str(envelope.get("device", "")),
        "event_id": str(record.get("event_id", "")),
        "event_at": epoch_ms(record.get("event_at_ms")),
        "posted_at": epoch_ms(record.get("post_time_ms")),
        "notification_when_at": epoch_ms(record.get("when_ms")),
        "notification_tag": str(record.get("tag", "") or ""),
        "user_id": "" if record.get("user_id") is None else str(int_value(record.get("user_id"))),
        "raw_json": dict(record),
        "exported_at": exported_at,
        "synced_at": exported_at,
        "sync_version": int(exported_at.timestamp() * 1_000_000),
    }
    for field in _STRING_FIELDS:
        value = record.get(field)
        row[field] = "" if value is None else str(value)
    for field in _FLAG_FIELDS:
        row[field] = int(bool(record.get(field)))
    for field in _INT_FIELDS:
        row[field] = int_value(record.get(field))
    # The listener's events predate the field: they are the listener's.
    row["event_source"] = str(record.get("event_source") or "listener")
    row["notification_rank"] = int_value(record.get("rank"))
    for field in _LIST_FIELDS:
        value = record.get(field)
        row[field] = list(value) if isinstance(value, list) else []
    # An event the listener saw before Android stamped a post time (or an
    # app that set none) still sorts by when it happened.
    if row["posted_at"].timestamp() <= 0:
        row["posted_at"] = row["event_at"]
    return row


def epoch_ms(value: object) -> datetime:
    millis = int_value(value)
    if millis <= 0:
        return datetime.fromtimestamp(0, tz=UTC)
    return datetime.fromtimestamp(millis / 1000, tz=UTC)


def library_storage_key(storage_key: str) -> str:
    if storage_key.startswith(INBOX_PREFIX):
        return LIBRARY_PREFIX + storage_key[len(INBOX_PREFIX) :]
    return storage_key
