from __future__ import annotations

from datetime import UTC, datetime
import json

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
        ai_model="qwen3-vl:2b",
        ai_prompt_version="gmail-attachment-ai-v13",
    )

    assert "AND ai_provider = 'ollama'" in queries[0]
    assert "AND ai_model = 'qwen3-vl:2b'" in queries[0]
    assert "AND ai_prompt_version = 'gmail-attachment-ai-v13'" in queries[0]


def test_slack_schema_creates_identity_table_and_account_state_view() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [(0,)]

    warehouse.ensure_slack_tables()

    assert any("CREATE TABLE IF NOT EXISTS slack_account_identities" in command for command in commands)
    assert any("CREATE OR REPLACE VIEW slack_account_state_items" in command for command in commands)


def test_voice_memos_schema_creates_file_table() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append

    warehouse.ensure_voice_memos_tables()

    assert any("CREATE TABLE IF NOT EXISTS voice_memo_files" in command for command in commands)
    assert any("storage_backend LowCardinality(String)" in command for command in commands)
    assert any("metadata_storage_key String" in command for command in commands)
    assert any("ORDER BY (account, recording_id)" in command for command in commands)
    assert not any("ALTER TABLE IF EXISTS voice_memo_files" in command for command in commands)
    assert any("CREATE TABLE IF NOT EXISTS voice_memo_transcription_runs" in command for command in commands)
    assert any("CREATE TABLE IF NOT EXISTS voice_memo_transcript_segments" in command for command in commands)
    assert any("CREATE TABLE IF NOT EXISTS voice_memo_enrichments" in command for command in commands)
    assert any("CREATE TABLE IF NOT EXISTS agent_runs" in command for command in commands)
    assert any("CREATE TABLE IF NOT EXISTS agent_run_events" in command for command in commands)
    assert any("CREATE TABLE IF NOT EXISTS agent_run_tool_calls" in command for command in commands)
    assert any("corrected_transcript String" in command for command in commands)
    assert any("meeting_notes String" in command for command in commands)
    assert any("ADD COLUMN IF NOT EXISTS corrected_transcript" in command for command in commands)
    assert any("ADD COLUMN IF NOT EXISTS meeting_notes" in command for command in commands)


def test_voice_memos_schema_recreates_old_content_hash_ordering() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [("CREATE TABLE voice_memo_files (...) ORDER BY (account, content_sha256)",)]

    warehouse.ensure_voice_memos_tables()

    assert commands[0] == "DROP TABLE IF EXISTS voice_memo_files"
    assert any("ORDER BY (account, recording_id)" in command for command in commands)


def test_voice_memos_untranscribed_query_orders_recent_recordings() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    queries: list[str] = []

    def fake_query(sql: str):
        queries.append(sql)
        return []

    warehouse._query = fake_query

    assert warehouse.load_untranscribed_voice_memo_files(provider="assemblyai", limit=3) == []

    assert "FROM voice_memo_files AS f" in queries[0]
    assert "voice_memo_transcription_runs" in queries[0]
    assert "provider = 'assemblyai'" in queries[0]
    assert "status = 'completed'" in queries[0]
    assert "ORDER BY f.recorded_at DESC" in queries[0]
    assert "LIMIT 3" in queries[0]


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
    assert "m.latest_message_at IS NOT NULL" in queries[0]
    assert "last_read" in queries[0]


def test_slack_account_state_refresh_inserts_snapshot_before_tombstones() -> None:
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

    assert "INSERT INTO slack_account_state_item_rows" in commands[0]
    assert inserts[0][0] == "slack_account_state_item_rows"
    tombstone = inserts[0][1][0]
    assert tombstone[SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS.index("is_deleted")] == 1
    assert tombstone[SLACK_ACCOUNT_STATE_ITEM_ROW_COLUMNS.index("sync_version")] == int(synced_at.timestamp() * 1_000_000)


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


def test_slack_conversation_insert_preserves_existing_read_state() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    inserted: list[dict[str, object]] = []
    incoming = {
        "account": "zrl",
        "team_id": "T1",
        "conversation_id": "D1",
        "raw_json": json.dumps({"id": "D1", "is_im": True, "unread_count": 0, "is_open": False}),
    }

    warehouse._query = lambda _sql: [
        (
            "D1",
            json.dumps(
                {
                    "id": "D1",
                    "is_im": True,
                    "last_read": "1777302000.000000",
                    "unread_count": 2,
                    "unread_count_display": 2,
                    "is_open": True,
                }
            ),
        )
    ]
    warehouse._insert_rows = lambda _table, rows, _columns: inserted.extend(rows)

    warehouse.insert_slack_conversations([incoming])

    payload = json.loads(str(inserted[0]["raw_json"]))
    assert payload["last_read"] == "1777302000.000000"
    assert payload["unread_count"] == 0
    assert payload["unread_count_display"] == 2
    assert payload["is_open"] is False
