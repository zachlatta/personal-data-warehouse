from __future__ import annotations

from datetime import UTC, datetime

from personal_data_warehouse.clickhouse import ClickHouseWarehouse, SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS


def test_gmail_schema_drops_thread_view_and_does_not_recreate_it() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [(0,)]

    warehouse.ensure_tables()

    assert "DROP VIEW IF EXISTS gmail_thread_messages" in commands
    assert all("CREATE OR REPLACE VIEW gmail_thread_messages" not in command for command in commands)


def test_gmail_schema_creates_account_state_view() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [(0,)]

    warehouse.ensure_tables()

    assert any("CREATE OR REPLACE VIEW gmail_account_state_items" in command for command in commands)


def test_gmail_attachment_backfill_schema_tracks_ai_version() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [(0,)]

    warehouse.ensure_tables()

    assert any("ai_provider LowCardinality(String)" in command for command in commands)
    assert any("ADD COLUMN IF NOT EXISTS ai_model" in command for command in commands)
    assert any("ADD COLUMN IF NOT EXISTS ai_prompt_version" in command for command in commands)


def test_gmail_attachment_backfill_candidates_are_scoped_to_ai_version() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    queries: list[str] = []

    def fake_query(sql: str):
        queries.append(sql)
        return []

    warehouse._query = fake_query

    warehouse.load_attachment_backfill_candidate_messages(
        account="zrl@example.com",
        limit=5,
        ai_provider="ollama",
        ai_model="qwen3-vl:8b",
        ai_prompt_version="gmail-attachment-ai-v13",
    )

    assert "AND ai_provider = 'ollama'" in queries[0]
    assert "AND ai_model = 'qwen3-vl:8b'" in queries[0]
    assert "AND ai_prompt_version = 'gmail-attachment-ai-v13'" in queries[0]


def test_slack_schema_creates_identity_table_and_account_state_view() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [(0,)]

    warehouse.ensure_slack_tables()

    assert any("CREATE TABLE IF NOT EXISTS slack_account_identities" in command for command in commands)
    assert any("CREATE OR REPLACE VIEW slack_account_state_items" in command for command in commands)


def test_combined_account_state_view_created_when_source_views_exist() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [(1,)]

    warehouse.ensure_tables()

    assert any("CREATE OR REPLACE VIEW account_state_items" in command for command in commands)


def test_slack_read_state_candidates_include_direct_messages() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    queries: list[str] = []

    def fake_query(sql: str):
        queries.append(sql)
        return []

    warehouse._query = fake_query

    warehouse.load_slack_read_state_candidate_payloads(account="zrl", team_id="T1", limit=10)

    assert "(c.is_member = 1 OR c.is_im = 1 OR c.is_mpim = 1)" in queries[0]
    assert "last_read" in queries[0]


def test_slack_account_state_refresh_inserts_tombstones_before_snapshot() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []
    inserts: list[tuple[str, list[tuple[object, ...]], tuple[str, ...]]] = []
    synced_at = datetime(2026, 4, 27, 13, 30, tzinfo=UTC)
    active_row = tuple(
        0 if column in {"priority_rank", "unread_count", "is_deleted", "sync_version"} else synced_at
        if column in {"latest_activity_at", "synced_at"}
        else "value"
        for column in SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS
    )

    warehouse._query = lambda _sql: [active_row]
    warehouse._command = commands.append
    warehouse._insert = lambda table, rows, columns: inserts.append((table, rows, columns))

    warehouse.refresh_slack_account_state_items(account="zrl", team_id="T1", synced_at=synced_at)

    assert inserts[0][0] == "slack_account_state_item_rows"
    tombstone = inserts[0][1][0]
    assert tombstone[SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS.index("is_deleted")] == 1
    assert tombstone[SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS.index("sync_version")] == int(synced_at.timestamp() * 1_000_000)
    assert "INSERT INTO slack_account_state_item_rows" in commands[0]


def test_slack_account_state_sql_uses_precise_and_thread_read_state() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    sql = warehouse._slack_account_state_items_select_sql(
        account="zrl",
        team_id="T1",
        synced_at=datetime(2026, 4, 27, 13, 30, tzinfo=UTC),
        sync_version=1,
    )

    assert "toDecimal64OrZero(JSONExtractString(raw_json, 'last_read'), 6)" in sql
    assert "toDecimal64OrZero(m.message_ts, 6)" in sql
    assert "thread_last_read_ts" in sql
    assert "JSONExtractBool(raw_json, 'subscribed')" in sql
