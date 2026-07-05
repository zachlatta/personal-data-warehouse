from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest
from dotenv import load_dotenv

from tests.conftest import make_test_schema

from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.schema import (
    GOOGLE_DRIVE_FILE_COLUMNS,
    GOOGLE_DRIVE_FILE_TEXT_COLUMNS,
    GOOGLE_DRIVE_SYNC_STATE_COLUMNS,
)


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
        wh._command(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def _ensure_all_table_groups(wh: PostgresWarehouse) -> None:
    # search_text() is only (re)created once every source table it reads exists,
    # so building the full set is required to test the function.
    wh.ensure_tables()
    wh.ensure_calendar_tables()
    wh.ensure_contacts_tables()
    wh.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    wh.ensure_apple_notes_tables()
    wh.ensure_apple_messages_tables()
    wh.ensure_whatsapp_tables()
    wh.ensure_agent_sessions_tables()
    wh.ensure_slack_tables()
    wh.ensure_upstream_mutation_tables()
    wh.ensure_google_drive_source_tables()


def _require_pg_textsearch(warehouse: PostgresWarehouse) -> None:
    rows = warehouse._query(
        "SELECT 1 FROM pg_available_extensions WHERE name = 'pg_textsearch'"
        " AND current_setting('shared_preload_libraries') LIKE '%pg_textsearch%'"
    )
    if not rows:
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")
    warehouse._command(f'SET search_path TO "{warehouse._schema}", public')


def _file_row(**overrides):
    row = {column: "" for column in GOOGLE_DRIVE_FILE_COLUMNS}
    row.update(
        {
            "account": "zach@hackclub.com",
            "file_id": "f1",
            "name": "Quarterly Plan",
            "mime_type": "application/vnd.google-apps.document",
            "is_google_native": 1,
            "parents_json": ["folderA"],
            "folder_path": "/My Drive/Docs",
            "parent_folder_id": "folderA",
            "size_bytes": 0,
            "owners_json": [{"emailAddress": "zach@hackclub.com"}],
            "last_modifying_user": "Zach",
            "created_time": datetime(2026, 1, 1, tzinfo=UTC),
            "modified_time": datetime(2026, 6, 1, tzinfo=UTC),
            "viewed_by_me_time": datetime(2026, 6, 2, tzinfo=UTC),
            "starred": 0,
            "shared": 0,
            "trashed": 0,
            "is_excluded": 0,
            "storage_backend": "google_drive_source",
            "storage_file_id": "f1",
            "storage_status": "available",
            "raw_metadata_json": {"id": "f1"},
            "ingested_at": datetime(2026, 6, 2, tzinfo=UTC),
            "sync_version": 1,
        }
    )
    row.update(overrides)
    return row


def _text_row(**overrides):
    row = {column: "" for column in GOOGLE_DRIVE_FILE_TEXT_COLUMNS}
    row.update(
        {
            "account": "zach@hackclub.com",
            "file_id": "f1",
            "extractor": "drive_export",
            "extractor_version": "1",
            "text": "the secret pineapple roadmap",
            "text_extraction_status": "ok",
            "char_count": 28,
            "truncated": 0,
            "source_modified_time": datetime(2026, 6, 1, tzinfo=UTC),
            "extracted_at": datetime(2026, 6, 2, tzinfo=UTC),
            "sync_version": 1,
        }
    )
    row.update(overrides)
    return row


def test_ensure_creates_tables(warehouse):
    warehouse.ensure_google_drive_source_tables()
    # All three tables exist.
    for table in ("google_drive_files", "google_drive_file_texts", "google_drive_sync_state"):
        assert warehouse._relation_exists(table)


def test_files_and_text_surface_in_search_text(warehouse):
    _require_pg_textsearch(warehouse)
    _ensure_all_table_groups(warehouse)
    warehouse.insert_google_drive_files([_file_row()])
    warehouse.insert_google_drive_file_texts([_text_row()])

    name_hits = warehouse._query(
        "SELECT context, who, ref FROM search_text('quarterly', 20, ARRAY['google_drive']) "
        "WHERE score < 0 AND subsource = 'filename'"
    )
    assert name_hits == [("/My Drive/Docs", "Zach", "zach@hackclub.com:f1")]

    content_hits = warehouse._query(
        "SELECT ref FROM search_text('pineapple', 20, ARRAY['google_drive']) "
        "WHERE score < 0 AND subsource = 'content'"
    )
    assert content_hits == [("zach@hackclub.com:f1",)]


def test_excluded_and_trashed_files_hidden_from_search(warehouse):
    _require_pg_textsearch(warehouse)
    _ensure_all_table_groups(warehouse)
    warehouse.insert_google_drive_files(
        [
            _file_row(file_id="ex1", name="excluded doc", is_excluded=1, exclude_reason="pdw_transport_folder"),
            _file_row(file_id="tr1", name="trashed doc", trashed=1),
        ]
    )
    warehouse.insert_google_drive_file_texts(
        [
            _text_row(file_id="ex1", text="hidden excluded text"),
            _text_row(file_id="tr1", text="hidden trashed text"),
        ]
    )
    hits = warehouse._query(
        "SELECT text FROM search_text('hidden', 20, ARRAY['google_drive']) WHERE score < 0"
    )
    assert hits == []


def test_sync_state_roundtrip_and_upsert(warehouse):
    warehouse.ensure_google_drive_source_tables()
    warehouse.upsert_google_drive_sync_state(
        {
            "account": "zach@hackclub.com",
            "start_page_token": "100",
            "last_page_token": "100",
            "drive_id": "",
            "last_sync_type": "full_crawl",
            "status": "ok",
            "error": "",
            "full_crawled_at": datetime(2026, 6, 2, tzinfo=UTC),
            "files_seen": 42,
            "updated_at": datetime(2026, 6, 2, tzinfo=UTC),
            "sync_version": 1,
        }
    )
    warehouse.upsert_google_drive_sync_state(
        {
            "account": "zach@hackclub.com",
            "start_page_token": "205",
            "last_page_token": "205",
            "drive_id": "",
            "last_sync_type": "incremental",
            "status": "ok",
            "error": "",
            "full_crawled_at": datetime(2026, 6, 2, tzinfo=UTC),
            "files_seen": 7,
            "updated_at": datetime(2026, 6, 2, 1, tzinfo=UTC),
            "sync_version": 2,
        }
    )
    state = warehouse.load_google_drive_sync_state()
    assert set(state) == {"zach@hackclub.com"}
    assert state["zach@hackclub.com"].start_page_token == "205"
    assert state["zach@hackclub.com"].last_sync_type == "incremental"
    assert state["zach@hackclub.com"].files_seen == 7
