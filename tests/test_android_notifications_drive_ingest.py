from __future__ import annotations

from datetime import UTC, datetime
import gzip
import json
import os

from dagster import DagsterInstance, SkipReason, build_sensor_context
from dotenv import load_dotenv
import pytest

from personal_data_warehouse.android_notifications_drive_ingest import (
    AndroidNotificationsDriveIngestRunner,
    iter_batch_payloads,
    record_to_event_row,
)
from personal_data_warehouse.defs import android_notifications_drive_ingest as defs_module
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

    def ensure_android_notifications_tables(self) -> None:
        self.ensure_called = True

    def insert_android_notification_events(self, rows) -> None:
        self.events.extend(rows)


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, *, listings=None, payloads=None) -> None:
        self.listings = list(listings or [])
        self.payloads = dict(payloads or {})
        self.moved: list[tuple[str, str]] = []
        self.fetched: list[str] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        return list(self.listings) if kind == "android_notification_export_batch" else []

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == "android_notification_export_batch" and self.listings:
            return self.listings[0]
        return None

    def get_object(self, ref):
        self.fetched.append(str(ref["storage_file_id"]))
        return self.payloads[str(ref["storage_file_id"])]

    def move_object(self, ref, *, new_object_key, app_properties=None):
        self.moved.append((str(ref["storage_file_id"]), new_object_key))
        return {**ref, "storage_key": new_object_key}


def listener_event(event_id: str = "e-1", **overrides):
    """An event as the PDW notification listener writes it (listener v1)."""
    event = {
        "event_id": event_id,
        "event_type": "posted",
        "event_at_ms": 1792863236787,
        "listener_version": 1,
        "notification_key": "0|com.google.android.apps.messaging|12|null|10213",
        "package_name": "com.google.android.apps.messaging",
        "app_label": "Messages",
        "notification_id": 12,
        "tag": None,
        "user_id": 0,
        "post_time_ms": 1792863230000,
        "when_ms": 1792863229000,
        "channel_id": "conversation_channel",
        "category": "msg",
        "group_key": "0|com.google.android.apps.messaging|g:group",
        "group": "group",
        "sort_key": "",
        "is_group_summary": False,
        "is_ongoing": False,
        "is_clearable": True,
        "flags": 8,
        "visibility": 0,
        "number": 0,
        "ticker": "",
        "template": "android.app.Notification$MessagingStyle",
        "title": "Alex Example",
        "title_big": "",
        "text": "lunch tomorrow?",
        "sub_text": "",
        "big_text": "",
        "info_text": "",
        "summary_text": "",
        "conversation_title": "",
        "is_group_conversation": False,
        "text_lines": [],
        "messages": [{"text": "lunch tomorrow?", "time_ms": 1792863229000, "sender": "Alex Example"}],
        "people": ["Alex Example"],
        "actions": ["Reply", "Mark as read"],
        "importance": 4,
        "channel_name": "Conversations",
        "is_ambient": False,
        "is_conversation": True,
        "extras": {"android.title": "Alex Example", "android.largeIcon": "<Icon>"},
        "content_sha256": "a" * 64,
    }
    event.update(overrides)
    return event


def envelope(event=None, *, exported_at: str = "2026-10-24T17:40:00+00:00"):
    return {
        "schema_version": 1,
        "source": "android_notifications",
        "account": "owner@example.test",
        "device": "testphone",
        "exported_at": exported_at,
        "record_type": "notification_event",
        "record": event if event is not None else listener_event(),
    }


def listing(file_id: str, exported_at: str) -> ObjectListing:
    return ObjectListing(
        ref={"storage_backend": "google_drive", "storage_key": "", "storage_file_id": file_id, "storage_url": ""},
        app_properties={"content_sha256": file_id, "exported_at": exported_at},
        filename=f"{file_id}.jsonl.gz",
    )


def gz(*records) -> bytes:
    return gzip.compress("\n".join(json.dumps(record) for record in records).encode())


def test_record_to_event_row_maps_the_listener_event() -> None:
    row = record_to_event_row(envelope())
    assert (row["account"], row["device"], row["event_id"]) == ("owner@example.test", "testphone", "e-1")
    assert row["event_type"] == "posted"
    assert row["event_at"] == datetime.fromtimestamp(1792863236.787, tz=UTC)
    assert row["posted_at"] == datetime.fromtimestamp(1792863230, tz=UTC)
    assert row["notification_when_at"] == datetime.fromtimestamp(1792863229, tz=UTC)
    assert row["notification_tag"] == ""
    assert row["user_id"] == "0"
    assert row["is_ongoing"] == 0 and row["is_clearable"] == 1 and row["is_conversation"] == 1
    assert row["importance"] == 4 and row["flags"] == 8 and row["removal_reason"] == 0
    assert row["messages"][0]["sender"] == "Alex Example"
    assert row["actions"] == ["Reply", "Mark as read"]
    assert row["raw_json"]["extras"]["android.largeIcon"] == "<Icon>"
    assert row["sync_version"] == int(datetime(2026, 10, 24, 17, 40, tzinfo=UTC).timestamp() * 1_000_000)


def test_a_removal_carries_its_reason_and_a_missing_post_time_falls_back() -> None:
    row = record_to_event_row(
        envelope(
            listener_event(
                "e-2", event_type="removed", removal_reason=1, removal_reason_name="click", post_time_ms=0
            )
        )
    )
    assert (row["removal_reason"], row["removal_reason_name"]) == (1, "click")
    assert row["posted_at"] == row["event_at"]


def test_a_system_log_event_keeps_its_timings_and_a_listener_event_is_the_listener() -> None:
    assert record_to_event_row(envelope())["event_source"] == "listener"
    row = record_to_event_row(
        envelope(
            {
                "event_id": "syslog-abc",
                "event_type": "action_clicked",
                "event_source": "system_log",
                "event_at_ms": 1792866657101,
                "notification_key": "0|com.example.chat|7|null|10123",
                "package_name": "com.example.chat",
                "notification_id": 7,
                "user_id": 0,
                "action_index": 1,
                "lifespan_ms": 5000,
                "freshness_ms": 4000,
                "exposure_ms": 3000,
                "rank": 2,
                "count": 4,
            }
        )
    )
    assert row["event_source"] == "system_log"
    assert (row["lifespan_ms"], row["freshness_ms"], row["exposure_ms"]) == (5000, 4000, 3000)
    assert (row["notification_rank"], row["action_index"]) == (2, 1)
    assert row["posted_at"] == row["event_at"]
    assert row["raw_json"]["count"] == 4


def test_drive_ingest_writes_and_promotes_each_batch_oldest_first_up_to_the_cap() -> None:
    store = FakeObjectStore(
        listings=[
            listing("newer", "2026-10-24T17:41:00+00:00"),
            listing("older", "2026-10-24T17:40:00+00:00"),
            listing("newest", "2026-10-24T17:42:00+00:00"),
        ],
        payloads={
            # A batch the phone re-sent holds the same event twice: one row.
            "older": gz(envelope(listener_event("e-1")), envelope(listener_event("e-1"))),
            "newer": gz(envelope(listener_event("e-2")), {"record_type": "something_else"}),
            "newest": gz(envelope(listener_event("e-3"))),
        },
    )
    warehouse = FakeWarehouse()
    summary = AndroidNotificationsDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: iter_batch_payloads(object_store=store),
        object_store=store,
        logger=FakeLogger(),
        max_batches=2,
    ).sync()

    assert warehouse.ensure_called
    assert (summary.batches_seen, summary.events_written, summary.files_promoted) == (2, 2, 2)
    assert [e["event_id"] for e in warehouse.events] == ["e-1", "e-2"]
    assert store.moved == [
        ("older", "android-notifications/library/batches/2026/10/older.jsonl.gz"),
        ("newer", "android-notifications/library/batches/2026/10/newer.jsonl.gz"),
    ]
    assert store.fetched == ["older", "newer"]


def test_events_round_trip_through_the_warehouse_and_upsert(warehouse) -> None:
    def run(*envelopes):
        AndroidNotificationsDriveIngestRunner(
            warehouse=warehouse,
            batch_source=lambda: [{"records": list(envelopes)}],
            logger=FakeLogger(),
        ).sync()

    run(envelope())
    run(envelope(exported_at="2026-10-24T17:45:00+00:00"))
    rows = warehouse._query_dicts(
        "SELECT event_type, title, posted_at, is_ongoing, importance, messages, actions, raw_json, synced_at "
        "FROM @android_notification_events"
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["title"] == "Alex Example"
    assert row["posted_at"] == datetime.fromtimestamp(1792863230, tz=UTC)
    assert row["importance"] == 4
    assert row["messages"][0]["text"] == "lunch tomorrow?"
    assert row["actions"] == ["Reply", "Mark as read"]
    assert row["synced_at"] == datetime(2026, 10, 24, 17, 45, tzinfo=UTC)


class FakeSettings:
    android_notifications = type(
        "AndroidNotifications", (), {"google_drive_folder_id": "folder", "google_drive_account": "owner@example.test"}
    )()


def test_sensor_skips_an_empty_inbox(monkeypatch) -> None:
    assert defs_module.android_notifications_drive_inbox_sensor.minimum_interval_seconds == 60
    monkeypatch.setattr(defs_module, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(defs_module, "_android_notifications_object_store", lambda _settings: object())
    monkeypatch.setattr(defs_module, "has_batch_payloads", lambda **_kwargs: False)
    with DagsterInstance.ephemeral() as instance:
        result = defs_module.android_notifications_drive_inbox_sensor(build_sensor_context(instance=instance))
    assert isinstance(result, SkipReason)
