from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Event

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

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
        cleanup_test_warehouse(wh)


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


def test_whoop_schema_upgrade_adds_credential_fingerprint_to_existing_state_table(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse._command(
        """
        CREATE TABLE @whoop_sync_state (
            account text NOT NULL,
            collection text NOT NULL,
            watermark_updated_at timestamptz NOT NULL,
            last_sync_type text NOT NULL DEFAULT '',
            status text NOT NULL DEFAULT 'ok',
            error text NOT NULL DEFAULT '',
            updated_at timestamptz NOT NULL,
            sync_version bigint NOT NULL DEFAULT 1,
            PRIMARY KEY (account, collection)
        )
        """
    )

    warehouse.ensure_whoop_tables()

    columns = warehouse._query_dicts(
        """
        SELECT column_name, column_default, is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = 'credential_sha256'
        """,
        (
            warehouse.physical_schema_name("ops"),
            "whoop_sync_state",
        ),
    )
    assert columns == [
        {
            "column_name": "credential_sha256",
            "column_default": "''::text",
            "is_nullable": "NO",
        }
    ]


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
    warehouse.replace_whoop_oauth_token(
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

    rows = warehouse._query("SELECT cycle_id, strain FROM @whoop_cycles WHERE account = %s", ("zach@example.com",))
    assert rows == [("cycle-1", 6.0)]
    state = warehouse.load_whoop_sync_state()
    assert state[("zach@example.com", "cycles")]["status"] == "ok"
    assert warehouse.load_whoop_oauth_token(account="zach@example.com") == '{"access_token":"private-token"}'
    assert state[("zach@example.com", "cycles")]["watermark_updated_at"] == synced_at


def test_whoop_token_rotation_preserves_the_current_token_when_refresh_fails(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_whoop_tables()
    account = "zach@example.com"
    stored = '{"access_token":"current","refresh_token":"single-use"}'
    warehouse.replace_whoop_oauth_token(
        account=account,
        token_json=stored,
        updated_at=datetime(2026, 8, 11, 12, tzinfo=UTC),
    )

    def fail_refresh(_token_json: str) -> str:
        raise RuntimeError("provider response was incomplete")

    with pytest.raises(RuntimeError, match="incomplete"):
        warehouse.rotate_whoop_oauth_token(
            account=account,
            expected_token_json=stored,
            rotate=fail_refresh,
            updated_at=datetime(2026, 8, 11, 12, 1, tzinfo=UTC),
        )

    assert warehouse.load_whoop_oauth_token(account=account) == stored


def test_whoop_token_rotation_serializes_racers_and_refreshes_only_once(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_whoop_tables()
    account = "zach@example.com"
    original = '{"access_token":"old","refresh_token":"single-use"}'
    winner = '{"access_token":"winner","refresh_token":"rotated"}'
    warehouse.replace_whoop_oauth_token(
        account=account,
        token_json=original,
        updated_at=datetime(2026, 8, 11, 12, tzinfo=UTC),
    )
    first_has_lock = Event()
    allow_first_to_finish = Event()
    losing_refresh_calls: list[str] = []
    second = PostgresWarehouse(
        warehouse._database_url,
        schema=warehouse.schema_namespace,
    )

    def first_refresh(_token_json: str) -> str:
        first_has_lock.set()
        assert allow_first_to_finish.wait(timeout=5)
        return winner

    def losing_refresh(token_json: str) -> str:
        losing_refresh_calls.append(token_json)
        return '{"access_token":"loser","refresh_token":"invalid"}'

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(
                warehouse.rotate_whoop_oauth_token,
                account=account,
                expected_token_json=original,
                rotate=first_refresh,
                updated_at=datetime(2026, 8, 11, 12, 1, tzinfo=UTC),
            )
            assert first_has_lock.wait(timeout=5)
            racer = pool.submit(
                second.rotate_whoop_oauth_token,
                account=account,
                expected_token_json=original,
                rotate=losing_refresh,
                updated_at=datetime(2026, 8, 11, 12, 1, tzinfo=UTC),
            )
            allow_first_to_finish.set()

            assert first.result(timeout=5) == winner
            assert racer.result(timeout=5) == winner
    finally:
        second.close()

    assert losing_refresh_calls == []
    assert warehouse.load_whoop_oauth_token(account=account) == winner


def test_whoop_reauthorization_and_rotation_share_one_critical_section(
    warehouse: PostgresWarehouse,
) -> None:
    """An explicit reauthorization cannot race a scheduled refresh."""

    warehouse.ensure_whoop_tables()
    account = "zach@example.com"
    original = '{"access_token":"old","refresh_token":"single-use"}'
    rotated = '{"access_token":"rotated","refresh_token":"next"}'
    reauthorized = '{"access_token":"reauthorized","refresh_token":"fresh"}'
    warehouse.replace_whoop_oauth_token(
        account=account,
        token_json=original,
        updated_at=datetime(2026, 8, 12, 5, tzinfo=UTC),
    )
    refresh_has_lock = Event()
    allow_refresh_to_finish = Event()
    install_started = Event()
    second = PostgresWarehouse(
        warehouse._database_url,
        schema=warehouse.schema_namespace,
    )

    def provider_refresh(_token_json: str) -> str:
        refresh_has_lock.set()
        assert allow_refresh_to_finish.wait(timeout=5)
        return rotated

    def install_reauthorization() -> None:
        install_started.set()
        second.replace_whoop_oauth_token(
            account=account,
            token_json=reauthorized,
            updated_at=datetime(2026, 8, 12, 6, 1, tzinfo=UTC),
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            refresh = pool.submit(
                warehouse.rotate_whoop_oauth_token,
                account=account,
                expected_token_json=original,
                rotate=provider_refresh,
                updated_at=datetime(2026, 8, 12, 6, tzinfo=UTC),
            )
            assert refresh_has_lock.wait(timeout=5)
            install = pool.submit(
                install_reauthorization,
            )
            assert install_started.wait(timeout=5)
            assert not install.done()
            allow_refresh_to_finish.set()

            assert refresh.result(timeout=5) == rotated
            install.result(timeout=5)
    finally:
        second.close()

    assert warehouse.load_whoop_oauth_token(account=account) == reauthorized


def test_competing_first_bootstraps_install_exactly_one_authority(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_whoop_tables()
    account = "zach@example.com"
    first = '{"access_token":"first","refresh_token":"first-refresh"}'
    second_token = '{"access_token":"second","refresh_token":"second-refresh"}'
    second = PostgresWarehouse(
        warehouse._database_url,
        schema=warehouse.schema_namespace,
    )

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = [
                pool.submit(
                    wh.load_or_bootstrap_whoop_oauth_token,
                    account=account,
                    bootstrap_token_json=token,
                    updated_at=datetime(2026, 8, 12, 6, tzinfo=UTC),
                )
                for wh, token in ((warehouse, first), (second, second_token))
            ]
            installed = [result.result(timeout=5) for result in results]
    finally:
        second.close()

    assert installed[0] == installed[1]
    assert warehouse.load_whoop_oauth_token(account=account) == installed[0]
