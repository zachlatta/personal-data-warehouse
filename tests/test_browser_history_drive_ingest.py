from __future__ import annotations

from datetime import UTC, datetime
import gzip
import json
import os

from dagster import DagsterInstance, SkipReason, build_sensor_context

from personal_data_warehouse.browser_history_drive_ingest import (
    BrowserHistoryDriveIngestRunner,
    iter_batch_payloads,
    record_to_visit_row,
)
from personal_data_warehouse.defs import browser_history_drive_ingest as defs_module
from personal_data_warehouse.objectstore import ObjectListing
from personal_data_warehouse.postgres import PostgresWarehouse
from dotenv import load_dotenv
import pytest

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
        self.visits: list[dict[str, object]] = []

    def ensure_browser_history_tables(self) -> None:
        self.ensure_called = True

    def insert_browser_history_visits(self, rows) -> None:
        self.visits.extend(rows)


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, *, listings=None, payloads=None) -> None:
        self.listings = list(listings or [])
        self.payloads = dict(payloads or {})
        self.moved: list[tuple[str, str]] = []
        self.fetched: list[str] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        return list(self.listings) if kind == "browser_history_export_batch" else []

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == "browser_history_export_batch" and self.listings:
            return self.listings[0]
        return None

    def get_object(self, ref):
        self.fetched.append(str(ref["storage_file_id"]))
        return self.payloads[str(ref["storage_file_id"])]

    def move_object(self, ref, *, new_object_key, app_properties=None):
        self.moved.append((str(ref["storage_file_id"]), new_object_key))
        return {**ref, "storage_key": new_object_key}


def envelope(visit_id: str = "7", *, title: str = "Example page", exported_at: str = "2026-10-14T12:00:00+00:00"):
    return {
        "schema_version": 1,
        "source": "browser_history",
        "account": "owner@example.test",
        "exported_at": exported_at,
        "record_type": "visit",
        "record": {
            "device": "testmac",
            "browser": "chrome",
            "profile": "Default",
            "profile_name": "Personal",
            "visit_id": visit_id,
            "url_id": "3",
            "url": "https://example.test/page",
            "host": "example.test",
            "title": title,
            "visited_at": "2026-10-14T11:59:00+00:00",
            "transition": "typed",
            # The Go uploader's JSON numbers arrive as floats in Python only
            # when they are large; accept both.
            "transition_raw": 805306369.0,
            "is_redirect": False,
            "is_synced": True,
            "from_visit_id": "0",
            "visit_duration_ms": 2500,
            "search_term": "",
            "raw": {"visit_source": None},
        },
    }


def listing(file_id: str, exported_at: str) -> ObjectListing:
    return ObjectListing(
        ref={"storage_backend": "google_drive", "storage_key": "", "storage_file_id": file_id, "storage_url": ""},
        app_properties={"content_sha256": file_id, "exported_at": exported_at},
        filename=f"{file_id}.jsonl.gz",
    )


def gz(*records) -> bytes:
    return gzip.compress("\n".join(json.dumps(record) for record in records).encode())


def test_record_to_visit_row_maps_every_column() -> None:
    row = record_to_visit_row(envelope())
    assert row["account"] == "owner@example.test"
    assert (row["device"], row["browser"], row["profile"], row["visit_id"]) == ("testmac", "chrome", "Default", "7")
    assert row["visited_at"] == datetime(2026, 10, 14, 11, 59, tzinfo=UTC)
    assert row["transition_raw"] == 805306369
    assert row["is_redirect"] == 0
    assert row["is_synced"] == 1
    assert row["visit_duration_ms"] == 2500
    assert row["sync_version"] == int(datetime(2026, 10, 14, 12, tzinfo=UTC).timestamp() * 1_000_000)


def test_drive_ingest_writes_and_promotes_each_batch_oldest_first_up_to_the_cap() -> None:
    store = FakeObjectStore(
        listings=[
            listing("newer", "2026-10-14T13:00:00+00:00"),
            listing("older", "2026-10-14T12:00:00+00:00"),
            listing("newest", "2026-10-14T14:00:00+00:00"),
        ],
        payloads={
            "older": gz(envelope("1"), envelope("1", title="later read of the same visit")),
            "newer": gz(envelope("2")),
            "newest": gz(envelope("3")),
        },
    )
    warehouse = FakeWarehouse()
    summary = BrowserHistoryDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: iter_batch_payloads(object_store=store),
        object_store=store,
        logger=FakeLogger(),
        max_batches=2,
    ).sync()

    assert warehouse.ensure_called
    assert summary.batches_seen == 2
    assert summary.visits_written == 2
    assert [v["visit_id"] for v in warehouse.visits] == ["1", "2"]
    assert warehouse.visits[0]["title"] == "later read of the same visit"
    assert store.moved == [
        ("older", "browser-history/library/batches/2026/10/older.jsonl.gz"),
        ("newer", "browser-history/library/batches/2026/10/newer.jsonl.gz"),
    ]
    # The cap never downloads a batch it will not write.
    assert store.fetched == ["older", "newer"]


def test_visits_round_trip_through_the_warehouse_and_upsert(warehouse) -> None:
    runner = BrowserHistoryDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [{"records": [envelope()]}],
        logger=FakeLogger(),
    )
    runner.sync()
    BrowserHistoryDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [
            {"records": [envelope(title="Renamed page", exported_at="2026-10-14T12:15:00+00:00")]}
        ],
        logger=FakeLogger(),
    ).sync()
    rows = warehouse._query_dicts(
        "SELECT title, visited_at, is_synced, transition_raw, raw_json FROM @browser_history_visits"
    )
    assert len(rows) == 1
    assert rows[0]["title"] == "Renamed page"
    assert rows[0]["visited_at"] == datetime(2026, 10, 14, 11, 59, tzinfo=UTC)
    assert rows[0]["is_synced"] == 1
    assert rows[0]["transition_raw"] == 805306369


class FakeSettings:
    browser_history = type(
        "BrowserHistory", (), {"google_drive_folder_id": "folder", "google_drive_account": "owner@example.test"}
    )()


def test_sensor_skips_an_empty_inbox(monkeypatch) -> None:
    assert defs_module.browser_history_drive_inbox_sensor.minimum_interval_seconds == 60
    monkeypatch.setattr(defs_module, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(defs_module, "_browser_history_object_store", lambda _settings: object())
    monkeypatch.setattr(defs_module, "has_batch_payloads", lambda **_kwargs: False)
    with DagsterInstance.ephemeral() as instance:
        result = defs_module.browser_history_drive_inbox_sensor(build_sensor_context(instance=instance))
    assert isinstance(result, SkipReason)
