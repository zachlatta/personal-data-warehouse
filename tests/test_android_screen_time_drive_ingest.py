from __future__ import annotations

from datetime import UTC, datetime
import gzip
import json
import os

from dagster import DagsterInstance, SkipReason, build_sensor_context
from dotenv import load_dotenv
import pytest

from personal_data_warehouse.android_screen_time_drive_ingest import (
    AndroidScreenTimeDriveIngestRunner,
    iter_batch_payloads,
    record_to_daily_row,
    record_to_event_row,
)
from personal_data_warehouse.defs import android_screen_time_drive_ingest as defs_module
from personal_data_warehouse.objectstore import ObjectListing
from personal_data_warehouse.postgres import PostgresWarehouse
from tests.conftest import cleanup_test_warehouse, make_test_schema


@pytest.fixture()
def warehouse():
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    wh = PostgresWarehouse(url, schema=make_test_schema())
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


class FakeLogger:
    def info(self, *_args, **_kwargs) -> None:
        pass


class FakeWarehouse:
    def __init__(self) -> None:
        self.ensure_called = False
        self.events: list[dict[str, object]] = []
        self.daily: list[dict[str, object]] = []

    def ensure_android_screen_time_tables(self) -> None:
        self.ensure_called = True

    def insert_android_screen_time_events(self, rows) -> None:
        self.events.extend(rows)

    def insert_android_screen_time_daily_usage(self, rows) -> None:
        self.daily.extend(rows)


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, *, listings=None, payloads=None) -> None:
        self.listings = list(listings or [])
        self.payloads = dict(payloads or {})
        self.moved: list[tuple[str, str]] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        return list(self.listings) if kind == "android_screen_time_export_batch" else []

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == "android_screen_time_export_batch" and self.listings:
            return self.listings[0]
        return None

    def get_object(self, ref):
        return self.payloads[str(ref["storage_file_id"])]

    def move_object(self, ref, *, new_object_key, app_properties=None):
        self.moved.append((str(ref["storage_file_id"]), new_object_key))
        return {**ref, "storage_key": new_object_key}


def usage_event(key: str = "k-1", **overrides):
    """A usage event as the uploader sends it (exporter v1 plus event_key)."""
    event = {
        "record_type": "usage_event",
        "event_at_ms": 1792878753735,
        "event_type": 1,
        "event_type_name": "ACTIVITY_RESUMED",
        "package_name": "com.example.reader",
        "class_name": "com.example.reader.MainActivity",
        "exporter_version": 1,
        "event_key": key,
    }
    event.update(overrides)
    return event


def app_day(key: str = "d-1", foreground_ms: int = 2419867):
    return {
        "record_type": "daily_app_usage",
        "package_name": "com.example.reader",
        "app_label": "Reader",
        "bucket_start_ms": 1792780014651,
        "bucket_end_ms": 1792866414650,
        "total_time_in_foreground_ms": foreground_ms,
        "last_time_used_ms": 1792817113971,
        "total_time_visible_ms": 2506229,
        "last_time_visible_ms": 1792817134478,
        "total_time_foreground_service_ms": 0,
        "last_time_foreground_service_ms": 0,
        "time_zone": "America/New_York",
        "exporter_version": 1,
        "usage_key": key,
    }


def device_day(key: str = "d-2"):
    return {
        "record_type": "daily_device_usage",
        "event_type": 18,
        "event_type_name": "KEYGUARD_HIDDEN",
        "bucket_start_ms": 1792780014651,
        "bucket_end_ms": 1792866414650,
        "count": 139,
        "total_time_ms": 25066005,
        "last_event_ms": 0,
        "time_zone": "America/New_York",
        "exporter_version": 1,
        "usage_key": key,
    }


def envelope(record, *, device: str = "testphone", exported_at: str = "2026-10-24T22:40:00+00:00"):
    return {
        "schema_version": 1,
        "source": "android_screen_time",
        "account": "owner@example.test",
        "device": device,
        "exported_at": exported_at,
        "record_type": record["record_type"],
        "record": record,
    }


def listing(file_id: str, exported_at: str) -> ObjectListing:
    return ObjectListing(
        ref={"storage_backend": "google_drive", "storage_key": "", "storage_file_id": file_id, "storage_url": ""},
        app_properties={"content_sha256": file_id, "exported_at": exported_at},
        filename=f"{file_id}.jsonl.gz",
    )


def gz(*records) -> bytes:
    return gzip.compress("\n".join(json.dumps(record) for record in records).encode())


def test_record_to_event_row_maps_a_usage_event() -> None:
    row = record_to_event_row(envelope(usage_event()))
    assert (row["account"], row["device"], row["event_key"]) == ("owner@example.test", "testphone", "k-1")
    assert row["event_at"] == datetime.fromtimestamp(1792878753.735, tz=UTC)
    assert (row["usage_event_type"], row["usage_event_type_name"]) == (1, "ACTIVITY_RESUMED")
    assert row["package_name"] == "com.example.reader"
    assert row["standby_bucket"] == 0 and row["shortcut_id"] == ""
    assert row["raw_json"]["exporter_version"] == 1


def test_record_to_daily_row_maps_app_and_device_totals() -> None:
    app = record_to_daily_row(envelope(app_day()))
    assert app["usage_kind"] == "app"
    assert app["app_label"] == "Reader"
    assert app["total_time_in_foreground_ms"] == 2419867
    assert app["bucket_start_at"] == datetime.fromtimestamp(1792780014.651, tz=UTC)
    assert app["last_time_used_at"] == datetime.fromtimestamp(1792817113.971, tz=UTC)
    device = record_to_daily_row(envelope(device_day()))
    assert device["usage_kind"] == "device"
    assert (device["usage_event_type_name"], device["event_count"], device["total_time_ms"]) == (
        "KEYGUARD_HIDDEN",
        139,
        25066005,
    )
    # Android leaves last_event_ms 0 on a closed bucket: the epoch sentinel.
    assert device["last_event_at"] == datetime.fromtimestamp(0, tz=UTC)


def test_drive_ingest_splits_records_dedupes_and_promotes() -> None:
    store = FakeObjectStore(
        listings=[listing("b", "2026-10-24T22:45:00+00:00"), listing("a", "2026-10-24T22:40:00+00:00")],
        payloads={
            # The uploader's overlap re-sends an event: one row.
            "a": gz(envelope(usage_event("k-1")), envelope(usage_event("k-1")), envelope(app_day())),
            # A second device with the same keys is its own rows.
            "b": gz(
                envelope(usage_event("k-1"), device="tablet"),
                envelope(device_day()),
                {"record_type": "something_else"},
            ),
        },
    )
    warehouse = FakeWarehouse()
    summary = AndroidScreenTimeDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: iter_batch_payloads(object_store=store),
        object_store=store,
        logger=FakeLogger(),
    ).sync()
    assert warehouse.ensure_called
    assert (summary.batches_seen, summary.events_written, summary.daily_written, summary.files_promoted) == (
        2,
        2,
        2,
        2,
    )
    assert [(e["device"], e["event_key"]) for e in warehouse.events] == [("testphone", "k-1"), ("tablet", "k-1")]
    assert store.moved[0] == ("a", "android-screen-time/library/batches/2026/10/a.jsonl.gz")


def test_rows_round_trip_through_the_warehouse_and_today_upserts(warehouse) -> None:
    def run(*envelopes):
        AndroidScreenTimeDriveIngestRunner(
            warehouse=warehouse,
            batch_source=lambda: [{"records": list(envelopes)}],
            logger=FakeLogger(),
        ).sync()

    run(envelope(usage_event()), envelope(app_day(foreground_ms=1000)), envelope(device_day()))
    run(
        envelope(usage_event(), exported_at="2026-10-24T22:45:00+00:00"),
        envelope(app_day(foreground_ms=5000), exported_at="2026-10-24T22:45:00+00:00"),
    )
    events = warehouse._query_dicts(
        "SELECT usage_event_type, usage_event_type_name, event_at, synced_at FROM @android_screen_time_events"
    )
    assert len(events) == 1
    assert events[0]["usage_event_type"] == 1
    assert events[0]["synced_at"] == datetime(2026, 10, 24, 22, 45, tzinfo=UTC)
    daily = warehouse._query_dicts(
        "SELECT usage_kind, total_time_in_foreground_ms, event_count, raw_json "
        "FROM @android_screen_time_daily_usage ORDER BY usage_kind"
    )
    assert [(d["usage_kind"], d["total_time_in_foreground_ms"], d["event_count"]) for d in daily] == [
        ("app", 5000, 0),
        ("device", 0, 139),
    ]
    assert daily[0]["raw_json"]["app_label"] == "Reader"


class FakeSettings:
    android_screen_time = type(
        "AndroidScreenTime", (), {"google_drive_folder_id": "folder", "google_drive_account": "owner@example.test"}
    )()


def test_sensor_skips_an_empty_inbox(monkeypatch) -> None:
    assert defs_module.android_screen_time_drive_inbox_sensor.minimum_interval_seconds == 60
    monkeypatch.setattr(defs_module, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(defs_module, "_android_screen_time_object_store", lambda _settings: object())
    monkeypatch.setattr(defs_module, "has_batch_payloads", lambda **_kwargs: False)
    with DagsterInstance.ephemeral() as instance:
        result = defs_module.android_screen_time_drive_inbox_sensor(build_sensor_context(instance=instance))
    assert isinstance(result, SkipReason)
