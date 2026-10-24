from __future__ import annotations

from datetime import UTC, datetime
import gzip
import json
import os

from dagster import DagsterInstance, SkipReason, build_sensor_context
from dotenv import load_dotenv
import pytest

from personal_data_warehouse.android_messages_drive_ingest import (
    AndroidMessagesDriveIngestRunner,
    iter_batch_payloads,
    record_to_message_row,
)
from personal_data_warehouse.defs import android_messages_drive_ingest as defs_module
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
        self.messages: list[dict[str, object]] = []

    def ensure_android_messages_tables(self) -> None:
        self.ensure_called = True

    def insert_android_messages(self, rows) -> None:
        self.messages.extend(rows)


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, *, listings=None, payloads=None) -> None:
        self.listings = list(listings or [])
        self.payloads = dict(payloads or {})
        self.moved: list[tuple[str, str]] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        return list(self.listings) if kind == "android_message_export_batch" else []

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == "android_message_export_batch" and self.listings:
            return self.listings[0]
        return None

    def get_object(self, ref):
        return self.payloads[str(ref["storage_file_id"])]

    def move_object(self, ref, *, new_object_key, app_properties=None):
        self.moved.append((str(ref["storage_file_id"]), new_object_key))
        return {**ref, "storage_key": new_object_key}


def uploader_record(key: str = "content://sms/1", **overrides):
    """A message as `pdw ingest android-messages` reads it from bugle_db."""
    record = {
        "bugle_id": 324818,
        "conversation_id": 2911,
        "sender_id": 13,
        "self_id": 2,
        "sent_timestamp_ms": 1792873100000,
        "received_timestamp_ms": 1792873161000,
        "message_protocol": 0,
        "message_status": 100,
        "seen": 1,
        "read": 0,
        "telephony_uri": key,
        "subject": "",
        "rcs_message_id": "",
        "is_hidden": 0,
        "sms_error_code": 0,
        "conversation_name": "Alex Example",
        "participant_count": 0,
        "conversation_type": 0,
        "rcs_group_id": "",
        "sender_address": "+15550000002",
        "sender_name": "Alex Example",
        "self_address": "+15550000001",
        "sim_name": "Example Carrier",
        "parts": [
            {
                "part_id": 1,
                "content_type": "text/plain",
                "text": "lunch tomorrow?",
                "file_name": "",
                "width": 0,
                "height": 0,
                "duration_ms": 0,
                "file_size_bytes": 0,
            }
        ],
        "participants": [{"address": "+15550000002", "name": "Alex Example"}],
        "chat_key": "+15550000002",
        "message_key": key,
    }
    record.update(overrides)
    return record


def envelope(record=None, *, exported_at: str = "2026-10-24T20:40:00+00:00"):
    return {
        "schema_version": 1,
        "source": "android_messages",
        "account": "owner@example.test",
        "device": "testphone",
        "exported_at": exported_at,
        "record_type": "message",
        "record": record if record is not None else uploader_record(),
    }


def listing(file_id: str, exported_at: str) -> ObjectListing:
    return ObjectListing(
        ref={"storage_backend": "google_drive", "storage_key": "", "storage_file_id": file_id, "storage_url": ""},
        app_properties={"content_sha256": file_id, "exported_at": exported_at},
        filename=f"{file_id}.jsonl.gz",
    )


def gz(*records) -> bytes:
    return gzip.compress("\n".join(json.dumps(record) for record in records).encode())


def test_record_to_message_row_maps_an_incoming_sms() -> None:
    row = record_to_message_row(envelope())
    assert (row["account"], row["device"], row["message_key"]) == (
        "owner@example.test",
        "testphone",
        "content://sms/1",
    )
    assert (row["protocol"], row["protocol_name"], row["bugle_status"]) == (0, "sms", 100)
    assert row["is_from_me"] == 0 and row["message_kind"] == "text"
    assert row["body_text"] == "lunch tomorrow?"
    assert row["message_at"] == datetime.fromtimestamp(1792873161, tz=UTC)
    assert row["sent_at"] == datetime.fromtimestamp(1792873100, tz=UTC)
    # A one-to-one chat: Messages leaves participant_count 0, the roster has one.
    assert (row["participant_count"], row["is_group"]) == (1, 0)
    assert row["chat_name"] == "Alex Example" and row["sim_name"] == "Example Carrier"
    assert row["raw_json"]["bugle_id"] == 324818


def test_sent_attachments_and_conversation_events_are_told_apart() -> None:
    photo = {"part_id": 2, "content_type": "image/jpeg", "text": "", "file_name": "IMG_1.jpg"}
    sent = record_to_message_row(
        envelope(uploader_record("content://mms/40", message_protocol=1, message_status=2, parts=[photo]))
    )
    assert (sent["is_from_me"], sent["message_kind"], sent["attachment_count"]) == (1, "attachment", 1)
    assert sent["protocol_name"] == "mms"
    event = record_to_message_row(
        envelope(uploader_record("content://mms/41", message_protocol=3, message_status=233, parts=[]))
    )
    assert (event["is_from_me"], event["message_kind"], event["protocol_name"]) == (0, "system", "rcs")
    group = record_to_message_row(
        envelope(
            uploader_record(
                "rcs:abc",
                participant_count=3,
                participants=[{"address": "+15550000002"}, {"address": "+15550000003"}, {"address": "+15550000004"}],
            )
        )
    )
    assert group["is_group"] == 1


def test_drive_ingest_writes_and_promotes_each_batch_oldest_first() -> None:
    store = FakeObjectStore(
        listings=[listing("newer", "2026-10-24T20:41:00+00:00"), listing("older", "2026-10-24T20:40:00+00:00")],
        payloads={
            # A message re-sent with a new status in one batch: one row, the last.
            "older": gz(envelope(uploader_record(read=0)), envelope(uploader_record(read=1))),
            "newer": gz(envelope(uploader_record("content://sms/2")), {"record_type": "something_else"}),
        },
    )
    warehouse = FakeWarehouse()
    summary = AndroidMessagesDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: iter_batch_payloads(object_store=store),
        object_store=store,
        logger=FakeLogger(),
    ).sync()
    assert warehouse.ensure_called
    assert (summary.batches_seen, summary.messages_written, summary.files_promoted) == (2, 2, 2)
    assert [m["message_key"] for m in warehouse.messages] == ["content://sms/1", "content://sms/2"]
    assert warehouse.messages[0]["is_read"] == 1
    assert store.moved[0] == ("older", "android-messages/library/batches/2026/10/older.jsonl.gz")


def test_messages_round_trip_through_the_warehouse_and_upsert(warehouse) -> None:
    def run(*envelopes):
        AndroidMessagesDriveIngestRunner(
            warehouse=warehouse,
            batch_source=lambda: [{"records": list(envelopes)}],
            logger=FakeLogger(),
        ).sync()

    run(envelope())
    run(envelope(uploader_record(read=1), exported_at="2026-10-24T20:45:00+00:00"))
    rows = warehouse._query_dicts(
        "SELECT message_key, body_text, is_read, protocol, parts, participants, message_at, synced_at "
        "FROM @android_messages"
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["body_text"] == "lunch tomorrow?" and row["is_read"] == 1 and row["protocol"] == 0
    assert row["parts"][0]["content_type"] == "text/plain"
    assert row["participants"][0]["address"] == "+15550000002"
    assert row["message_at"] == datetime.fromtimestamp(1792873161, tz=UTC)
    assert row["synced_at"] == datetime(2026, 10, 24, 20, 45, tzinfo=UTC)


def test_the_messages_mart_carries_the_android_texts(warehouse) -> None:
    warehouse.ensure_apple_messages_tables()
    warehouse.ensure_whatsapp_tables()
    AndroidMessagesDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [{"records": [envelope()]}],
        logger=FakeLogger(),
    ).sync()
    rows = warehouse._query_dicts(
        "SELECT source, chat_id, chat_kind, sender_name, is_from_me, body_text, message_kind, service "
        "FROM @marts_messages_messages WHERE source = 'android_messages'"
    )
    assert rows == [
        {
            "source": "android_messages",
            "chat_id": "+15550000002",
            "chat_kind": "direct",
            "sender_name": "Alex Example",
            "is_from_me": 0,
            "body_text": "lunch tomorrow?",
            "message_kind": "text",
            "service": "SMS",
        }
    ]


class FakeSettings:
    android_messages = type(
        "AndroidMessages", (), {"google_drive_folder_id": "folder", "google_drive_account": "owner@example.test"}
    )()


def test_sensor_skips_an_empty_inbox(monkeypatch) -> None:
    assert defs_module.android_messages_drive_inbox_sensor.minimum_interval_seconds == 60
    monkeypatch.setattr(defs_module, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(defs_module, "_android_messages_object_store", lambda _settings: object())
    monkeypatch.setattr(defs_module, "has_batch_payloads", lambda **_kwargs: False)
    with DagsterInstance.ephemeral() as instance:
        result = defs_module.android_messages_drive_inbox_sensor(build_sensor_context(instance=instance))
    assert isinstance(result, SkipReason)
