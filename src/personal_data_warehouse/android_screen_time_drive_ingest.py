"""Promote Android screen time batches from the Drive inbox into the warehouse.

Each Android device's uploader (``pdw ingest android-screen-time``, run as
root on the device) posts gzipped JSONL batches of Android's usage events and
daily usage totals, read through the PDW usage exporter app, to the app's
/ingest/android-screen-time/batch endpoint, which writes them into the Drive
inbox. Each batch is written and promoted to the library on its own. Events
upsert by (account, device, event_key) and daily buckets by (account, device,
usage_key), so the uploader's overlapping re-reads and today's growing
bucket land once, latest value winning.
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

OBJECT_PREFIX = "android-screen-time"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox/"
LIBRARY_PREFIX = f"{OBJECT_PREFIX}/library/"
BATCH_KIND = "android_screen_time_export_batch"
SOURCE = "android_screen_time"
BLOB_KIND = "android_screen_time_blob"
EVENT_RECORD_TYPE = "usage_event"
DAILY_RECORD_TYPES = {"daily_app_usage": "app", "daily_device_usage": "device"}


@dataclass(frozen=True)
class AndroidScreenTimeDriveIngestSummary:
    batches_seen: int
    events_written: int
    daily_written: int
    files_promoted: int


class AndroidScreenTimeDriveIngestRunner:
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

    def sync(self) -> AndroidScreenTimeDriveIngestSummary:
        self._warehouse.ensure_android_screen_time_tables()
        batches = 0
        events_written = 0
        daily_written = 0
        promoted = 0
        source = iter(self._batch_source())
        if self._max_batches:
            source = islice(source, self._max_batches)
        for batch in source:
            batches += 1
            events: dict[tuple[str, str, str], dict[str, Any]] = {}
            daily: dict[tuple[str, str, str], dict[str, Any]] = {}
            for envelope in batch_records(batch):
                record_type = str(envelope.get("record_type", ""))
                if record_type == EVENT_RECORD_TYPE:
                    row = record_to_event_row(envelope)
                    if row["event_key"]:
                        events[(row["account"], row["device"], row["event_key"])] = row
                elif record_type in DAILY_RECORD_TYPES:
                    row = record_to_daily_row(envelope)
                    if row["usage_key"]:
                        daily[(row["account"], row["device"], row["usage_key"])] = row
            self._warehouse.insert_android_screen_time_events(list(events.values()))
            self._warehouse.insert_android_screen_time_daily_usage(list(daily.values()))
            events_written += len(events)
            daily_written += len(daily)
            if self._object_store is not None:
                batch_file = nested_mapping(batch, "batch_file")
                target_key = library_storage_key(str(batch_file.get("storage_key", "")))
                if batch_file.get("storage_file_id") and target_key:
                    self._object_store.move_object(batch_file, new_object_key=target_key)
                    promoted += 1
        self._logger.info(
            "Ingested %s Android screen time batches: %s events, %s daily rows",
            batches,
            events_written,
            daily_written,
        )
        return AndroidScreenTimeDriveIngestSummary(
            batches_seen=batches,
            events_written=events_written,
            daily_written=daily_written,
            files_promoted=promoted,
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


def _common(envelope: Mapping[str, Any]) -> tuple[Mapping[str, Any], dict[str, Any]]:
    record = nested_mapping(envelope, "record")
    exported_at = parse_datetime(envelope.get("exported_at"))
    return record, {
        "account": str(envelope.get("account", "")),
        "device": str(envelope.get("device", "")),
        "raw_json": dict(record),
        "exported_at": exported_at,
        "synced_at": exported_at,
        "sync_version": int(exported_at.timestamp() * 1_000_000),
    }


def record_to_event_row(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Map one usage event to a row; the whole record is kept as raw_json."""
    record, row = _common(envelope)
    row.update(
        {
            "event_key": str(record.get("event_key", "") or ""),
            "event_at": epoch_ms(record.get("event_at_ms")),
            "usage_event_type": int_value(record.get("event_type")),
            "usage_event_type_name": str(record.get("event_type_name", "") or ""),
            "package_name": str(record.get("package_name", "") or ""),
            "class_name": str(record.get("class_name", "") or ""),
            "shortcut_id": str(record.get("shortcut_id", "") or ""),
            "standby_bucket": int_value(record.get("standby_bucket")),
            "configuration": str(record.get("configuration", "") or ""),
        }
    )
    return row


def record_to_daily_row(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Map one daily bucket (an app's totals or a device-level event type's)."""
    record, row = _common(envelope)
    row.update(
        {
            "usage_key": str(record.get("usage_key", "") or ""),
            "usage_kind": DAILY_RECORD_TYPES.get(str(envelope.get("record_type", "")), ""),
            "bucket_start_at": epoch_ms(record.get("bucket_start_ms")),
            "bucket_end_at": epoch_ms(record.get("bucket_end_ms")),
            "package_name": str(record.get("package_name", "") or ""),
            "app_label": str(record.get("app_label", "") or ""),
            "total_time_in_foreground_ms": int_value(record.get("total_time_in_foreground_ms")),
            "total_time_visible_ms": int_value(record.get("total_time_visible_ms")),
            "total_time_foreground_service_ms": int_value(record.get("total_time_foreground_service_ms")),
            "last_time_used_at": epoch_ms(record.get("last_time_used_ms")),
            "last_time_visible_at": epoch_ms(record.get("last_time_visible_ms")),
            "usage_event_type": int_value(record.get("event_type")),
            "usage_event_type_name": str(record.get("event_type_name", "") or ""),
            "event_count": int_value(record.get("count")),
            "total_time_ms": int_value(record.get("total_time_ms")),
            "last_event_at": epoch_ms(record.get("last_event_ms")),
            "time_zone": str(record.get("time_zone", "") or ""),
        }
    )
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
