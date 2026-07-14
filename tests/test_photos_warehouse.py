"""Warehouse schema + marts contract for the photos source family.

Raw photo files land per-source (apple_photos.files now; future sources add
their own <source>.files via PHOTO_SOURCE_RELATIONS), identity lives in the
derived photos.* tables, and cross-source querying goes through the
marts.photo_files / marts.photos / marts.photo_canonical_renditions views.
The timeline adapter contract for photos is covered by tests/test_timeline.py.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.postgres import POSTGRES_TABLES, PostgresWarehouse
from personal_data_warehouse.relations import (
    DERIVED_SCHEMAS,
    PHOTO_SOURCE_RELATIONS,
    SOURCE_RAW_SCHEMAS,
    relation,
)
from personal_data_warehouse.schema import PHOTO_SOURCE_FILE_COLUMNS


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    schema = make_test_schema()
    wh = PostgresWarehouse(_postgres_url(), schema=schema)
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


_TS = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


def _photo_file_row(**overrides) -> dict:
    row = {
        "source": "apple_photos",
        "account": "z@x.test",
        "source_native_id": "UUID-1",
        "role": "original",
        "filename": "IMG_0001.HEIC",
        "mime_type": "image/heic",
        "size_bytes": 123456,
        "width": 4284,
        "height": 5712,
        "content_sha256": "sha-still",
        "captured_at": _TS,
        "capture_tz_offset": "-07:00",
        "camera_make": "Apple",
        "camera_model": "iPhone 16 Pro",
        "raw_metadata_json": {"apple_record": {"uuid": "UUID-1"}},
        "storage_backend": "google_drive",
        "storage_key": "photos/library/2026/06/x.heic",
        "storage_file_id": "drive-1",
        "storage_url": "",
        "metadata_storage_key": "photos/library/2026/06/x.json",
        "metadata_storage_file_id": "drive-2",
        "metadata_storage_url": "",
        "metadata_content_sha256": "sha-meta",
        "is_deleted": 0,
        "ingested_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _photo_asset_row(**overrides) -> dict:
    row = {
        "photo_id": "ph1",
        "account": "z@x.test",
        "kind": "image",
        "capture_ts": _TS,
        "capture_tz_offset": "-07:00",
        "latitude": 45.5,
        "longitude": -122.6,
        "camera_make": "Apple",
        "camera_model": "iPhone 16 Pro",
        "width": 4284,
        "height": 5712,
        "best_file_sha256": "sha-still",
        "best_file_mime_type": "image/heic",
        "best_file_filename": "IMG_0001.HEIC",
        "best_file_size_bytes": 123456,
        "thumbnail_content_sha256": "sha-thumb",
        "thumbnail_content_type": "image/jpeg",
        "thumbnail_size_bytes": 45000,
        "thumbnail_storage_backend": "google_drive",
        "thumbnail_storage_key": "photos/derived/thumbnails/sha-thumb.jpg",
        "thumbnail_storage_file_id": "drive-th1",
        "thumbnail_storage_url": "",
        "created_at": _TS,
        "updated_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


def _link_row(**overrides) -> dict:
    row = {
        "source": "apple_photos",
        "account": "z@x.test",
        "source_native_id": "UUID-1",
        "role": "original",
        "content_sha256": "sha-still",
        "photo_id": "ph1",
        "match_method": "new",
        "match_score": 0.0,
        "created_at": _TS,
        "sync_version": 1,
    }
    row.update(overrides)
    return row


# --- pure registry contracts ---------------------------------------------------


def test_photo_source_registry_shape():
    # The registry is the single extension point: every registered source table
    # must be a real warehouse table sharing the raw photo file shape and the
    # provenance primary key.
    assert PHOTO_SOURCE_RELATIONS == {"apple_photos": "apple_photos_files"}
    for source, table in PHOTO_SOURCE_RELATIONS.items():
        spec = POSTGRES_TABLES[table]
        assert spec.columns == PHOTO_SOURCE_FILE_COLUMNS, table
        assert spec.primary_key == ("source", "account", "source_native_id", "content_sha256"), table
        assert relation(table).schema == source


def test_photo_relations_are_registered():
    assert "apple_photos" in SOURCE_RAW_SCHEMAS
    assert "photos" in DERIVED_SCHEMAS
    assert (relation("apple_photos_files").schema, relation("apple_photos_files").name) == ("apple_photos", "files")
    assert (relation("photo_assets").schema, relation("photo_assets").name) == ("photos", "assets")
    assert (relation("photo_asset_files").schema, relation("photo_asset_files").name) == ("photos", "asset_files")
    assert (relation("media_fingerprints").schema, relation("media_fingerprints").name) == (
        "enrichment",
        "media_fingerprints",
    )
    assert (relation("photo_files").schema, relation("photo_files").name) == ("marts", "photo_files")
    assert (relation("clean_photos").schema, relation("clean_photos").name) == ("marts", "photos")
    assert (relation("photo_canonical_renditions").schema, relation("photo_canonical_renditions").name) == (
        "marts",
        "photo_canonical_renditions",
    )


def test_insert_photo_source_files_rejects_unregistered_table():
    wh = PostgresWarehouse.__new__(PostgresWarehouse)  # no connection needed
    with pytest.raises(ValueError, match="unknown photo source table"):
        wh.insert_photo_source_files("whatsapp_media_items", [])


# --- live schema (Postgres) ------------------------------------------------------


def test_ensure_photos_tables_is_idempotent_and_creates_marts_views(warehouse):
    warehouse.ensure_photos_tables()
    warehouse.ensure_photos_tables()
    rows = warehouse._query(
        """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = ANY(%s)
        """,
        (warehouse.physical_schema_names(include_private=True),),
    )
    relations = {(schema, table): type_ for schema, table, type_ in rows}

    def phys(schema: str) -> str:
        return warehouse.physical_schema_name(schema)

    assert relations[(phys("apple_photos"), "files")] == "BASE TABLE"
    assert relations[(phys("photos"), "assets")] == "BASE TABLE"
    assert relations[(phys("photos"), "asset_files")] == "BASE TABLE"
    assert relations[(phys("enrichment"), "media_fingerprints")] == "BASE TABLE"
    assert relations[(phys("marts"), "photo_files")] == "VIEW"
    assert relations[(phys("marts"), "photos")] == "VIEW"
    assert relations[(phys("marts"), "photo_canonical_renditions")] == "VIEW"


def test_insert_photo_source_files_upserts_by_provenance_key(warehouse):
    warehouse.ensure_photos_tables()
    warehouse.insert_photo_source_files("apple_photos_files", [_photo_file_row()])
    # Same provenance key, higher sync_version: the row updates.
    warehouse.insert_photo_source_files(
        "apple_photos_files",
        [_photo_file_row(filename="IMG_0001 (edited).HEIC", sync_version=2)],
    )
    rows = warehouse._query("SELECT filename, sync_version FROM apple_photos_files")
    assert rows == [("IMG_0001 (edited).HEIC", 2)]
    # Lower sync_version: stale write is ignored.
    warehouse.insert_photo_source_files(
        "apple_photos_files",
        [_photo_file_row(filename="stale.HEIC", sync_version=1)],
    )
    rows = warehouse._query("SELECT filename, sync_version FROM apple_photos_files")
    assert rows == [("IMG_0001 (edited).HEIC", 2)]


def test_photo_files_mart_unions_sources_and_joins_identity(warehouse):
    warehouse.ensure_photos_tables()
    warehouse.insert_photo_source_files(
        "apple_photos_files",
        [
            _photo_file_row(),
            _photo_file_row(source_native_id="UUID-2", content_sha256="sha-2", filename="IMG_0002.HEIC"),
        ],
    )
    warehouse.insert_photo_asset_files([_link_row()])
    rows = warehouse._query(
        "SELECT source, source_native_id, photo_id, match_method FROM photo_files ORDER BY source_native_id"
    )
    # Linked row carries its identity; unresolved row shows empty photo_id.
    assert rows == [
        ("apple_photos", "UUID-1", "ph1", "new"),
        ("apple_photos", "UUID-2", "", ""),
    ]


def test_canonical_renditions_are_stills_with_thumbnails_only(warehouse):
    warehouse.ensure_photos_tables()
    warehouse.insert_photo_assets(
        [
            _photo_asset_row(),
            # Video asset: excluded even though it has a thumbnail.
            _photo_asset_row(photo_id="ph-video", kind="video", thumbnail_content_sha256="sha-vthumb"),
            # Still without a generated thumbnail yet: excluded until identity
            # produces one.
            _photo_asset_row(photo_id="ph-nothumb", thumbnail_content_sha256=""),
        ]
    )
    rows = warehouse._query(
        "SELECT photo_id, content_sha256, mime_type, filename, storage_file_id FROM photo_canonical_renditions"
    )
    assert rows == [("ph1", "sha-thumb", "image/jpeg", "IMG_0001.HEIC", "drive-th1")]


def test_photo_canonical_renditions_feed_the_enrichment_candidate_query(warehouse):
    from personal_data_warehouse.file_attachment_enrichment import (
        PHOTOS_SOURCE,
        load_file_enrichment_candidates,
    )

    warehouse.ensure_photos_tables()
    warehouse.insert_photo_assets([_photo_asset_row()])

    def candidates():
        return load_file_enrichment_candidates(
            warehouse,
            source=PHOTOS_SOURCE,
            provider="agent_codex",
            prompt_version=PHOTOS_SOURCE.prompt_version,
            limit=10,
        )

    found = candidates()
    assert [c["content_sha256"] for c in found] == ["sha-thumb"]
    assert found[0]["mime_type"] == "image/jpeg"
    assert found[0]["storage_file_id"] == "drive-th1"

    # A completed enrichment for the thumbnail sha removes the candidate: one
    # vision pass per logical photo.
    warehouse._command(
        """
        INSERT INTO file_attachment_enrichments (content_sha256, ai_provider, ai_model,
                                                 ai_prompt_version, text, text_extraction_status, updated_at)
        VALUES ('sha-thumb', 'agent_codex', 'm', %s, 'caption', 'agent_ok', %s)
        """,
        (PHOTOS_SOURCE.prompt_version, _TS),
    )
    assert candidates() == []


def test_clean_photos_carries_caption_and_counts(warehouse):
    warehouse.ensure_photos_tables()
    warehouse.insert_photo_assets([_photo_asset_row()])
    warehouse.insert_photo_asset_files(
        [
            _link_row(),
            _link_row(source_native_id="takeout/IMG_0001.jpg", source="apple_photos", content_sha256="sha-rendition"),
        ]
    )
    warehouse._command(
        """
        INSERT INTO file_attachment_enrichments (content_sha256, ai_provider, ai_model,
                                                 ai_prompt_version, text, updated_at)
        VALUES ('sha-thumb', 'agent_codex', 'm', 'photo-agent-v1', 'A red bicycle against a wall', %s)
        """,
        (_TS,),
    )
    rows = warehouse._query(
        "SELECT photo_id, kind, camera_model, rendition_count, caption FROM clean_photos"
    )
    assert rows == [("ph1", "image", "iPhone 16 Pro", 2, "A red bicycle against a wall")]
