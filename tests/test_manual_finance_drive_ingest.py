"""Manual finance Drive-inbox ingest: row mapping, promotion, library fallback."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from personal_data_warehouse.manual_finance_drive_ingest import (
    ManualFinanceDriveIngestRunner,
    attach_storage_context,
    find_file_listing,
    has_metadata_payloads,
    iter_metadata_payloads,
    library_storage_key,
    metadata_to_row,
)
from personal_data_warehouse.objectstore import ObjectListing


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeWarehouse:
    def __init__(self) -> None:
        self.ensured = False
        self.rows: list[dict[str, object]] = []

    def ensure_manual_finance_tables(self) -> None:
        self.ensured = True

    def insert_manual_finance_documents(self, rows) -> None:
        self.rows.extend(rows)


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, *, metadata_listings=None, files_by_stage=None, json_by_id=None) -> None:
        self.metadata_listings = list(metadata_listings or [])
        self.files_by_stage = dict(files_by_stage or {})
        self.json_by_id = dict(json_by_id or {})
        self.moved: list[tuple[str, str]] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        return list(self.metadata_listings) if kind == "manual_finance_metadata" else []

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == "manual_finance_metadata":
            return self.metadata_listings[0] if self.metadata_listings else None
        if kind == "manual_finance_document":
            sha = str((properties or {}).get("content_sha256", ""))
            return self.files_by_stage.get((stage, sha))
        return None

    def get_object(self, ref):
        return json.dumps(self.json_by_id[str(ref["storage_file_id"])]).encode("utf-8")

    def move_object(self, ref, *, new_object_key, app_properties=None):
        self.moved.append((str(ref.get("storage_file_id", "")), new_object_key))
        return {
            "storage_backend": self.backend,
            "storage_key": new_object_key,
            "storage_file_id": str(ref.get("storage_file_id", "")),
            "storage_url": "",
        }


def envelope() -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": "manual",
        "account": "z@x.test",
        "uploaded_at": "2026-07-13T12:00:00+00:00",
        "file": {
            "native_id": "filesha",
            "filename": "acme-checking-2026-06.pdf",
            "original_path": "acme-checking-0001/acme-checking-2026-06.pdf",
            "mime_type": "application/pdf",
            "size_bytes": 11,
            "content_sha256": "filesha",
            "file_modified_at": "2026-06-30T10:00:00+00:00",
        },
    }


def _listing(storage_key: str, file_id: str, *, props: dict[str, str] | None = None) -> ObjectListing:
    return ObjectListing(
        ref={
            "storage_backend": "google_drive",
            "storage_key": storage_key,
            "storage_file_id": file_id,
            "storage_url": "",
        },
        app_properties=props or {},
        filename=storage_key.rsplit("/", 1)[-1],
    )


def _metadata_listing() -> ObjectListing:
    return _listing(
        "manual-finance/inbox/acme-checking-0001/2026-06-30-dedupsha.json",
        "meta-1",
        props={"content_sha256": "metasha", "file_content_sha256": "filesha"},
    )


def _file_listing(stage: str = "inbox") -> ObjectListing:
    return _listing(f"manual-finance/{stage}/acme-checking-0001/2026-06-30-filesha.pdf", "file-1")


def test_library_storage_key_preserves_account_segment():
    assert (
        library_storage_key("manual-finance/inbox/acme-checking-0001/2026-06-30-sha.pdf")
        == "manual-finance/library/acme-checking-0001/2026-06-30-sha.pdf"
    )
    assert library_storage_key("manual-finance/library/x.pdf") == "manual-finance/library/x.pdf"


def test_metadata_to_row_maps_envelope_and_storage_context():
    payload = attach_storage_context(
        envelope(), metadata_listing=_metadata_listing(), file_listing=_file_listing()
    )
    row = metadata_to_row(payload, ingested_at=datetime(2026, 7, 13, tzinfo=UTC))
    assert row["source"] == "manual"
    assert row["source_native_id"] == "filesha"
    assert row["original_path"] == "acme-checking-0001/acme-checking-2026-06.pdf"
    assert row["content_sha256"] == "filesha"
    assert row["file_modified_at"] == datetime(2026, 6, 30, 10, 0, tzinfo=UTC)
    # Rows record LIBRARY keys (their post-promotion home).
    assert row["storage_key"] == "manual-finance/library/acme-checking-0001/2026-06-30-filesha.pdf"
    assert row["metadata_storage_key"] == "manual-finance/library/acme-checking-0001/2026-06-30-dedupsha.json"
    # The envelope survives losslessly minus the transient storage context.
    assert row["raw_metadata_json"]["file"]["original_path"] == "acme-checking-0001/acme-checking-2026-06.pdf"
    assert "file_object" not in row["raw_metadata_json"]


def test_runner_ingests_and_promotes():
    store = FakeObjectStore(
        metadata_listings=[_metadata_listing()],
        files_by_stage={("inbox", "filesha"): _file_listing()},
        json_by_id={"meta-1": envelope()},
    )
    warehouse = FakeWarehouse()
    summary = ManualFinanceDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: iter_metadata_payloads(object_store=store),
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 7, 13, tzinfo=UTC),
    ).sync()
    assert warehouse.ensured
    assert summary.rows_written == 1
    assert summary.objects_promoted == 2
    assert sorted(key for _, key in store.moved) == [
        "manual-finance/library/acme-checking-0001/2026-06-30-dedupsha.json",
        "manual-finance/library/acme-checking-0001/2026-06-30-filesha.pdf",
    ]


def test_find_file_listing_falls_back_to_library():
    store = FakeObjectStore(files_by_stage={("library", "filesha"): _file_listing("library")})
    listing = find_file_listing(object_store=store, payload=envelope(), stage="inbox")
    assert listing.ref["storage_key"].startswith("manual-finance/library/")
    with pytest.raises(ValueError, match="Could not find manual finance document"):
        find_file_listing(object_store=FakeObjectStore(), payload=envelope(), stage="inbox")


def test_has_metadata_payloads():
    assert has_metadata_payloads(object_store=FakeObjectStore(metadata_listings=[_metadata_listing()]))
    assert not has_metadata_payloads(object_store=FakeObjectStore())
