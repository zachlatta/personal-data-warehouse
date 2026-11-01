from __future__ import annotations

from datetime import UTC, datetime
import gzip
import json
import os

from dagster import DagsterInstance, SkipReason, build_sensor_context
from dotenv import load_dotenv
import pytest

from personal_data_warehouse.defs import scans_drive_ingest as defs_module
from personal_data_warehouse.objectstore import ObjectListing
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.scans_drive_ingest import (
    ScansDriveIngestRunner,
    iter_batch_payloads,
    record_to_row,
)
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
        self.rows: list[dict[str, object]] = []

    def ensure_scans_tables(self) -> None:
        self.ensure_called = True

    def insert_scan_documents(self, rows) -> None:
        self.rows.extend(rows)


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, *, listings=None, payloads=None) -> None:
        self.listings = list(listings or [])
        self.payloads = dict(payloads or {})
        self.moved: list[tuple[str, str]] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        return list(self.listings) if kind == "scan_export_batch" else []

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == "scan_export_batch" and self.listings:
            return self.listings[0]
        return None

    def get_object(self, ref):
        return self.payloads[str(ref["storage_file_id"])]

    def move_object(self, ref, *, new_object_key, app_properties=None):
        self.moved.append((str(ref["storage_file_id"]), new_object_key))
        return {**ref, "storage_key": new_object_key}


def scan(scan_id: str = "scan-1", **overrides):
    """A scan record as `pdw ingest scanner` sends it."""
    record = {
        "record_type": "scan",
        "scan_id": scan_id,
        "scanned_at": "2026-11-01T09:30:00.123456789-04:00",
        "completed_at": "2026-11-01T09:34:10-04:00",
        "profile": "pdw-duplex-color",
        "profile_name": "PDW Duplex Color",
        "color_mode": "color",
        "resolution_dpi": 300,
        "page_size": "letter",
        "input_source": "ADF,ADF",
        "is_duplex": True,
        "duplex_status": "paired",
        "page_count": 6,
        "front_page_count": 3,
        "back_page_count": 3,
        "scanner_name": "Test MFC Scanner",
        "filename": "Scan 2026-11-01 09-30.pdf",
        "content_type": "application/pdf",
        "content_sha256": "a" * 64,
        "size_bytes": 812345,
        "time_zone": "Local",
        "storage_backend": "google_drive",
        "storage_key": "scans/library/documents/2026/11/2026-11-01-" + "a" * 64 + ".pdf",
        "storage_file_id": "drive-file-1",
        "storage_url": "",
    }
    record.update(overrides)
    return record


def envelope(record, *, exported_at: str = "2026-11-01T13:34:11+00:00"):
    return {
        "schema_version": 1,
        "source": "scans",
        "account": "owner@example.test",
        "device": "scanbox",
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


def test_record_to_row_maps_a_scan() -> None:
    landed = datetime(2026, 11, 1, 13, 35, tzinfo=UTC)
    row = record_to_row(envelope(scan()), synced_at=landed)
    assert (row["account"], row["device"], row["scan_id"]) == ("owner@example.test", "scanbox", "scan-1")
    assert row["scanned_at"] == datetime(2026, 11, 1, 13, 30, 0, 123456, tzinfo=UTC)
    assert (row["scan_profile"], row["color_mode"], row["resolution_dpi"]) == ("pdw-duplex-color", "color", 300)
    assert (row["is_duplex"], row["duplex_status"], row["page_count"]) == (1, "paired", 6)
    assert (row["front_page_count"], row["back_page_count"]) == (3, 3)
    assert (row["mime_type"], row["storage_file_id"]) == ("application/pdf", "drive-file-1")
    # synced_at is when the row lands, not when the uploader exported it: a
    # scan spooled through an outage must still be read by the timeline.
    assert row["synced_at"] == landed
    assert row["exported_at"] == datetime(2026, 11, 1, 13, 34, 11, tzinfo=UTC)
    assert row["raw_json"]["profile_name"] == "PDW Duplex Color"
    single = record_to_row(envelope(scan(is_duplex=False, duplex_status="")))
    assert (single["is_duplex"], single["duplex_status"]) == (0, "")


def test_drive_ingest_dedupes_and_promotes() -> None:
    store = FakeObjectStore(
        listings=[listing("b", "2026-11-01T13:40:00+00:00"), listing("a", "2026-11-01T13:34:11+00:00")],
        payloads={
            # The uploader re-sends a record after a lost response: one row.
            "a": gz(envelope(scan("scan-1")), envelope(scan("scan-1"))),
            "b": gz(envelope(scan("scan-2")), {"record_type": "something_else"}),
        },
    )
    warehouse = FakeWarehouse()
    summary = ScansDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: iter_batch_payloads(object_store=store),
        object_store=store,
        logger=FakeLogger(),
    ).sync()
    assert warehouse.ensure_called
    assert (summary.batches_seen, summary.scans_written, summary.files_promoted) == (2, 2, 2)
    assert [row["scan_id"] for row in warehouse.rows] == ["scan-1", "scan-2"]
    assert store.moved[0] == ("a", "scans/library/batches/2026/11/a.jsonl.gz")


def test_a_scan_round_trips_and_reaches_the_files_mart(warehouse) -> None:
    def run(*envelopes):
        ScansDriveIngestRunner(
            warehouse=warehouse,
            batch_source=lambda: [{"records": list(envelopes)}],
            logger=FakeLogger(),
        ).sync()

    run(envelope(scan()))
    run(envelope(scan(page_count=7)))
    rows = warehouse._query_dicts(
        "SELECT scan_profile, page_count, is_duplex, scanned_at, raw_json FROM @scan_documents"
    )
    assert len(rows) == 1
    assert (rows[0]["scan_profile"], rows[0]["page_count"], rows[0]["is_duplex"]) == ("pdw-duplex-color", 7, 1)
    assert rows[0]["raw_json"]["scanner_name"] == "Test MFC Scanner"
    # The vision pass reads scans through the conformed files mart (C5).
    files = warehouse._query_dicts(
        "SELECT parent_id, attachment_id, mime_type, is_stored, occurred_at, storage_file_id "
        "FROM @marts_files_attachments WHERE source = 'scans'"
    )
    assert files == [
        {
            "parent_id": "scan-1",
            "attachment_id": "scan-1",
            "mime_type": "application/pdf",
            "is_stored": 1,
            "occurred_at": datetime(2026, 11, 1, 13, 30, 0, 123456, tzinfo=UTC),
            "storage_file_id": "drive-file-1",
        }
    ]


def test_the_files_mart_reports_an_unknown_scan_time_as_null(warehouse) -> None:
    ScansDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [{"records": [envelope(scan(scanned_at=""))]}],
        logger=FakeLogger(),
    ).sync()
    assert warehouse._query("SELECT occurred_at FROM @marts_files_attachments WHERE source = 'scans'") == [(None,)]


class FakeSettings:
    scans = type("Scans", (), {"google_drive_folder_id": "folder", "google_drive_account": "owner@example.test"})()


def test_sensor_skips_an_empty_inbox(monkeypatch) -> None:
    assert defs_module.scans_drive_inbox_sensor.minimum_interval_seconds == 30
    monkeypatch.setattr(defs_module, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(defs_module, "scans_object_store", lambda _settings: object())
    monkeypatch.setattr(defs_module, "has_batch_payloads", lambda **_kwargs: False)
    with DagsterInstance.ephemeral() as instance:
        result = defs_module.scans_drive_inbox_sensor(build_sensor_context(instance=instance))
    assert isinstance(result, SkipReason)
