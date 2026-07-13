"""Photos Drive-inbox ingest: per-source routing, promotion, library fallback."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from personal_data_warehouse.objectstore import ObjectListing
from personal_data_warehouse.photos_drive_ingest import (
    PhotosDriveIngestRunner,
    attach_storage_context,
    clean_metadata_payload,
    find_file_listing,
    has_metadata_payloads,
    iter_metadata_payloads,
    library_storage_key,
    metadata_to_row,
    parse_captured_at,
    photo_source_table,
)


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeWarehouse:
    def __init__(self) -> None:
        self.ensured = False
        self.rows_by_table: dict[str, list[dict[str, object]]] = {}

    def ensure_photos_tables(self) -> None:
        self.ensured = True

    def insert_photo_source_files(self, table, rows) -> None:
        self.rows_by_table.setdefault(table, []).extend(rows)


class FakeObjectStore:
    """Minimal, stage-aware ObjectStore for the ingest tests."""

    backend = "google_drive"

    def __init__(self, *, metadata_listings=None, files_by_stage=None, json_by_id=None) -> None:
        self.metadata_listings = list(metadata_listings or [])
        # {(stage, content_sha256): ObjectListing}
        self.files_by_stage = dict(files_by_stage or {})
        self.json_by_id = dict(json_by_id or {})
        self.moved: list[tuple[str, str]] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        return list(self.metadata_listings) if kind == "photo_metadata" else []

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == "photo_metadata":
            return self.metadata_listings[0] if self.metadata_listings else None
        if kind == "photo_file":
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
            "storage_url": "https://drive/promoted",
        }


def envelope() -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": "apple_photos",
        "account": "zach@example.com",
        "uploaded_at": "2026-06-02T12:00:00+00:00",
        "file": {
            "native_id": "UUID-1",
            "role": "original",
            "filename": "IMG_0001.HEIC",
            "mime_type": "image/heic",
            "size_bytes": 11,
            "content_sha256": "filesha",
            "width": 4284,
            "height": 5712,
            "captured_at": "2026-06-01T14:30:00",
            "capture_tz_offset": "-07:00",
            "camera_make": "Apple",
            "camera_model": "iPhone 16 Pro",
            "file_modified_at": "",
        },
        "apple_record": {"uuid": "UUID-1", "kind": 0},
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
        "photos/inbox/2026/06/2026-06-01-dedupsha.json",
        "meta-1",
        props={"content_sha256": "metasha", "file_content_sha256": "filesha"},
    )


def _file_listing(stage: str = "inbox") -> ObjectListing:
    return _listing(f"photos/{stage}/2026/06/2026-06-01-filesha.heic", "file-1")


def test_photo_source_table_routes_by_registry_and_fails_loud() -> None:
    assert photo_source_table(envelope()) == "apple_photos_files"
    with pytest.raises(ValueError, match="Unknown photo source 'google_photos'"):
        photo_source_table({**envelope(), "source": "google_photos"})
    with pytest.raises(ValueError, match="Unknown photo source ''"):
        photo_source_table({})


def test_metadata_to_row_maps_envelope_and_rewrites_library_keys() -> None:
    payload = attach_storage_context(
        envelope(), metadata_listing=_metadata_listing(), file_listing=_file_listing()
    )
    row = metadata_to_row(payload, ingested_at=datetime(2026, 6, 2, 13, 0, tzinfo=UTC))
    assert row["source"] == "apple_photos"
    assert row["source_native_id"] == "UUID-1"
    assert row["role"] == "original"
    assert row["content_sha256"] == "filesha"
    # 14:30 at -07:00 is 21:30Z.
    assert row["captured_at"] == datetime(2026, 6, 1, 21, 30, tzinfo=UTC)
    assert row["capture_tz_offset"] == "-07:00"
    # Rows carry LIBRARY keys (objects are promoted in the same run).
    assert row["storage_key"] == "photos/library/2026/06/2026-06-01-filesha.heic"
    assert row["metadata_storage_key"] == "photos/library/2026/06/2026-06-01-dedupsha.json"
    assert row["metadata_content_sha256"] == "metasha"
    # The raw envelope is stored losslessly minus the transient storage context.
    assert row["raw_metadata_json"]["apple_record"] == {"uuid": "UUID-1", "kind": 0}
    assert "file_object" not in row["raw_metadata_json"]


def test_find_file_listing_falls_back_to_library() -> None:
    # The blob was already promoted by an earlier envelope (e.g. a second
    # source holding identical bytes): the lookup must NOT raise.
    store = FakeObjectStore(files_by_stage={("library", "filesha"): _file_listing("library")})
    listing = find_file_listing(object_store=store, payload=envelope(), stage="inbox")
    assert listing.ref["storage_key"].startswith("photos/library/")


def test_find_file_listing_raises_when_blob_is_nowhere() -> None:
    with pytest.raises(ValueError, match="Could not find photo file object"):
        find_file_listing(object_store=FakeObjectStore(), payload=envelope(), stage="inbox")


def test_iter_metadata_payloads_reads_through_object_store() -> None:
    store = FakeObjectStore(
        metadata_listings=[_metadata_listing()],
        files_by_stage={("inbox", "filesha"): _file_listing()},
        json_by_id={"meta-1": envelope()},
    )
    payloads = list(iter_metadata_payloads(object_store=store))
    assert len(payloads) == 1
    assert payloads[0]["file_object"]["storage_file_id"] == "file-1"
    assert payloads[0]["metadata_object"]["content_sha256"] == "metasha"


def test_has_metadata_payloads_uses_find_object() -> None:
    assert has_metadata_payloads(object_store=FakeObjectStore(metadata_listings=[_metadata_listing()]))
    assert not has_metadata_payloads(object_store=FakeObjectStore())


def test_runner_writes_rows_and_promotes_inbox_objects() -> None:
    store = FakeObjectStore(
        metadata_listings=[_metadata_listing()],
        files_by_stage={("inbox", "filesha"): _file_listing()},
        json_by_id={"meta-1": envelope()},
    )
    warehouse = FakeWarehouse()
    summary = PhotosDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: iter_metadata_payloads(object_store=store),
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 6, 2, 13, 0, tzinfo=UTC),
    ).sync()
    assert warehouse.ensured
    assert summary.rows_written == 1
    assert list(warehouse.rows_by_table) == ["apple_photos_files"]
    assert summary.objects_promoted == 2
    assert store.moved == [
        ("file-1", "photos/library/2026/06/2026-06-01-filesha.heic"),
        ("meta-1", "photos/library/2026/06/2026-06-01-dedupsha.json"),
    ]


def test_runner_promotes_only_metadata_when_blob_already_in_library() -> None:
    store = FakeObjectStore(
        metadata_listings=[_metadata_listing()],
        files_by_stage={("library", "filesha"): _file_listing("library")},
        json_by_id={"meta-1": envelope()},
    )
    warehouse = FakeWarehouse()
    summary = PhotosDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: iter_metadata_payloads(object_store=store),
        object_store=store,
        logger=FakeLogger(),
    ).sync()
    assert summary.objects_promoted == 1
    assert store.moved == [("meta-1", "photos/library/2026/06/2026-06-01-dedupsha.json")]


def test_runner_is_idempotent_across_reruns() -> None:
    store = FakeObjectStore(
        metadata_listings=[_metadata_listing()],
        files_by_stage={("inbox", "filesha"): _file_listing()},
        json_by_id={"meta-1": envelope()},
    )
    warehouse = FakeWarehouse()
    run = lambda now: PhotosDriveIngestRunner(  # noqa: E731
        warehouse=warehouse,
        metadata_source=lambda: iter_metadata_payloads(object_store=store),
        object_store=None,  # no promotion: same payload seen twice
        logger=FakeLogger(),
        now=lambda: now,
    ).sync()
    run(datetime(2026, 6, 2, 13, 0, tzinfo=UTC))
    run(datetime(2026, 6, 2, 13, 5, tzinfo=UTC))
    rows = warehouse.rows_by_table["apple_photos_files"]
    assert len(rows) == 2
    first, second = rows
    # Same provenance primary key with a monotonically increasing
    # sync_version: the warehouse upsert collapses them to one row.
    key = ("source", "account", "source_native_id", "content_sha256")
    assert tuple(first[k] for k in key) == tuple(second[k] for k in key)
    assert second["sync_version"] > first["sync_version"]


def test_parse_captured_at_handles_missing_and_bad_values() -> None:
    assert parse_captured_at("", "") == datetime(1970, 1, 1, tzinfo=UTC)
    assert parse_captured_at("garbage", "-07:00") == datetime(1970, 1, 1, tzinfo=UTC)
    assert parse_captured_at("2026-06-01T14:30:00", "") == datetime(2026, 6, 1, 14, 30, tzinfo=UTC)


def test_library_storage_key_only_rewrites_inbox_prefix() -> None:
    assert library_storage_key("photos/inbox/2026/06/x.heic") == "photos/library/2026/06/x.heic"
    assert library_storage_key("photos/library/2026/06/x.heic") == "photos/library/2026/06/x.heic"


def test_clean_metadata_payload_strips_storage_context_only() -> None:
    payload = attach_storage_context(
        envelope(), metadata_listing=_metadata_listing(), file_listing=_file_listing()
    )
    clean = clean_metadata_payload(payload)
    assert "file_object" not in clean and "metadata_object" not in clean
    assert clean["file"]["native_id"] == "UUID-1"
