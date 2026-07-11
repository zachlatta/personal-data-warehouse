from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest
from dotenv import load_dotenv

from tests.conftest import make_test_schema

from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.timeline import BACKFILL_CURSOR_START, TIMELINE_ADAPTERS


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
        for schema_name in wh.physical_schema_names(include_private=True) + [schema]:
            wh._raw_command(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        wh.close()


def test_whoop_timeline_adapter_queries_execute_against_real_schema(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_whoop_tables()
    adapters = [adapter for adapter in TIMELINE_ADAPTERS if adapter.source == "whoop"]

    assert {adapter.name for adapter in adapters} == {
        "whoop_cycle",
        "whoop_recovery",
        "whoop_sleep",
        "whoop_workout",
    }
    for adapter in adapters:
        assert warehouse._query(
            adapter.backfill_sql,
            {"cursor_ts": BACKFILL_CURSOR_START, "cursor_id": "", "limit": 5},
        ) == []
        assert warehouse._query(
            adapter.incremental_sql,
            {"watermark_ts": datetime(1970, 1, 1, tzinfo=UTC), "watermark_id": "", "limit": 5},
        ) == []


def test_whoop_tables_upsert_rows_and_state(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_whoop_tables()
    synced_at = datetime(2026, 7, 9, 12, tzinfo=UTC)

    warehouse.insert_whoop_cycles(
        [
            {
                "account": "zach@example.com",
                "cycle_id": "cycle-1",
                "whoop_user_id": 101,
                "created_at": synced_at,
                "updated_at": synced_at,
                "start_at": synced_at,
                "end_at": synced_at,
                "timezone_offset": "Z",
                "score_state": "SCORED",
                "strain": 5.0,
                "kilojoule": 100.0,
                "average_heart_rate": 60,
                "max_heart_rate": 120,
                "score_json": {"strain": 5.0},
                "raw_json": {"id": "cycle-1", "score_state": "SCORED"},
                "synced_at": synced_at,
                "sync_version": 1,
            },
            {
                "account": "zach@example.com",
                "cycle_id": "cycle-1",
                "whoop_user_id": 101,
                "created_at": synced_at,
                "updated_at": synced_at,
                "start_at": synced_at,
                "end_at": synced_at,
                "timezone_offset": "Z",
                "score_state": "SCORED",
                "strain": 6.0,
                "kilojoule": 110.0,
                "average_heart_rate": 61,
                "max_heart_rate": 121,
                "score_json": {"strain": 6.0},
                "raw_json": {"id": "cycle-1", "score_state": "SCORED"},
                "synced_at": synced_at,
                "sync_version": 2,
            },
        ]
    )
    warehouse.upsert_whoop_oauth_token(
        account="zach@example.com",
        token_json='{"access_token":"private-token"}',
        updated_at=synced_at,
    )
    warehouse.insert_whoop_sync_state(
        account="zach@example.com",
        collection="cycles",
        watermark_updated_at=synced_at,
        last_sync_type="full",
        status="ok",
        error="",
        updated_at=synced_at,
    )

    rows = warehouse._query("SELECT cycle_id, strain FROM whoop_cycles WHERE account = %s", ("zach@example.com",))
    assert rows == [("cycle-1", 6.0)]
    state = warehouse.load_whoop_sync_state()
    assert state[("zach@example.com", "cycles")]["status"] == "ok"
    assert warehouse.load_whoop_oauth_token(account="zach@example.com") == '{"access_token":"private-token"}'
    assert state[("zach@example.com", "cycles")]["watermark_updated_at"] == synced_at
