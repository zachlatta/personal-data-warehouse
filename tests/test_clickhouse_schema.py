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


def test_gmail_schema_creates_clean_inbox_view() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [(0,)]

    warehouse.ensure_tables()

    assert any("DROP VIEW IF EXISTS account_state_items" in command for command in commands)
    assert any("DROP VIEW IF EXISTS gmail_account_state_items" in command for command in commands)
    assert any("CREATE OR REPLACE VIEW clean_gmail_inbox" in command for command in commands)
    view_sql = next(command for command in commands if "CREATE OR REPLACE VIEW clean_gmail_inbox" in command)
    assert "thread_id AS thread_id" in view_sql
    assert "latest_message_id AS message_id" in view_sql
    assert "latest_activity_at AS latest_at" in view_sql
    assert "latest_from_address AS from_address" in view_sql
    assert "source_table" not in view_sql
    assert "drilldown_hint" not in view_sql
    assert all("CREATE OR REPLACE VIEW account_state_items" not in command for command in commands)


def test_gmail_attachment_backfill_schema_tracks_ai_version() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [(0,)]

    warehouse.ensure_tables()

    assert any("ai_provider LowCardinality(String)" in command for command in commands)
    assert any("ADD COLUMN IF NOT EXISTS ai_model" in command for command in commands)
    assert any("ADD COLUMN IF NOT EXISTS ai_prompt_version" in command for command in commands)
    assert any("CREATE TABLE IF NOT EXISTS gmail_attachment_enrichments" in command for command in commands)
    assert any("ORDER BY (content_sha256, ai_provider, ai_model, ai_prompt_version)" in command for command in commands)


def test_gmail_attachment_schema_keeps_enrichment_payloads_off_attachment_rows() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [(0,)]

    warehouse.ensure_tables()

    attachment_sql = next(command for command in commands if "CREATE TABLE IF NOT EXISTS gmail_attachments" in command)
    enrichment_sql = next(
        command for command in commands if "CREATE TABLE IF NOT EXISTS gmail_attachment_enrichments" in command
    )
    assert "text String" not in attachment_sql
    assert "text_extraction_status" not in attachment_sql
    assert "ai_prompt_version" not in attachment_sql
    assert "text String" in enrichment_sql
    assert "text_extraction_status LowCardinality(String)" in enrichment_sql


def test_gmail_attachment_schema_migrates_legacy_enrichment_columns_before_drop() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []
    legacy_columns = {
        "text",
        "text_extraction_status",
        "text_extraction_error",
        "ai_provider",
        "ai_model",
        "ai_base_url",
        "ai_prompt_version",
        "ai_prompt_sha256",
        "ai_prompt",
        "ai_source_status",
        "ai_elapsed_ms",
        "ai_processed_at",
    }

    def fake_query(sql: str):
        if sql == "DESCRIBE TABLE gmail_attachments":
            return [(column,) for column in sorted(legacy_columns | {"content_sha256", "synced_at", "sync_version"})]
        return [(0,)]

    warehouse._command = commands.append
    warehouse._query = fake_query

    warehouse.ensure_tables()

    migrate_index = next(
        index for index, command in enumerate(commands) if "INSERT INTO gmail_attachment_enrichments" in command
    )
    drop_text_index = next(
        index for index, command in enumerate(commands) if "DROP COLUMN IF EXISTS text" in command
    )
    migrate_sql = commands[migrate_index]
    assert migrate_index < drop_text_index
    assert "FROM gmail_attachments FINAL" in migrate_sql
    assert "argMax(text, sync_version) AS migrated_text" in migrate_sql
    assert "GROUP BY" in migrate_sql
    assert any("ALTER TABLE gmail_attachments DROP COLUMN IF EXISTS ai_provider" in command for command in commands)
    assert any("ALTER TABLE gmail_attachments DROP COLUMN IF EXISTS ai_processed_at" in command for command in commands)


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


def test_gmail_attachment_enrichments_are_scoped_to_hash_and_ai_version() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    queries: list[str] = []

    def fake_query(sql: str):
        queries.append(sql)
        return [
            (
                "hash-1",
                "cached text",
                "ai_ok",
                "{}",
                "ollama",
                "qwen3-vl:2b",
                "http://127.0.0.1:11435",
                "gmail-attachment-ai-v20",
                "prompt-hash",
                "prompt",
                "unsupported",
                123,
                datetime(2026, 4, 30, tzinfo=UTC),
            )
        ]

    warehouse._query = fake_query

    rows = warehouse.load_attachment_enrichments(
        content_sha256s=["hash-1", "hash-2", "hash-1"],
        ai_provider="ollama",
        ai_model="qwen3-vl:2b",
        ai_prompt_version="gmail-attachment-ai-v20",
    )

    assert sorted(rows) == ["hash-1"]
    assert rows["hash-1"]["text"] == "cached text"
    assert "content_sha256 IN ('hash-1', 'hash-2')" in queries[0]
    assert "AND ai_provider = 'ollama'" in queries[0]
    assert "AND ai_model = 'qwen3-vl:2b'" in queries[0]
    assert "AND ai_prompt_version = 'gmail-attachment-ai-v20'" in queries[0]


def test_slack_schema_creates_identity_table_and_account_state_view() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [(0,)]

    warehouse.ensure_slack_tables()

    assert any("CREATE TABLE IF NOT EXISTS slack_account_identities" in command for command in commands)
    assert any("DROP TABLE IF EXISTS slack_account_state_items" in command for command in commands)
    assert any("CREATE OR REPLACE VIEW clean_slack_inbox" in command for command in commands)
    view_sql = next(command for command in commands if "CREATE OR REPLACE VIEW clean_slack_inbox" in command)
    assert "scope_id AS team_id" in view_sql
    assert "item_type AS kind" in view_sql
    assert "item_state AS state" in view_sql
    assert "latest_activity_at AS latest_at" in view_sql
    assert "container_id AS conversation_id" in view_sql
    assert "message_id AS message_ts" in view_sql
    assert "source_table" not in view_sql
    assert "drilldown_hint" not in view_sql


def test_apple_voice_memos_schema_creates_file_table() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append

    warehouse.ensure_apple_voice_memos_tables()

    assert any("CREATE TABLE IF NOT EXISTS apple_voice_memos_files" in command for command in commands)
    assert any("storage_backend LowCardinality(String)" in command for command in commands)
    assert any("metadata_storage_key String" in command for command in commands)
    assert any("ORDER BY (account, recording_id)" in command for command in commands)
    assert not any("ALTER TABLE IF EXISTS apple_voice_memos_files" in command for command in commands)
    assert any("CREATE TABLE IF NOT EXISTS apple_voice_memos_transcription_runs" in command for command in commands)
    assert any("CREATE TABLE IF NOT EXISTS apple_voice_memos_transcript_segments" in command for command in commands)
    assert any("CREATE TABLE IF NOT EXISTS apple_voice_memos_enrichments" in command for command in commands)
    assert any("CREATE TABLE IF NOT EXISTS agent_runs" in command for command in commands)
    assert any("prompt_version String" in command for command in commands)
    assert any("ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS prompt_version" in command for command in commands)
    assert any("CREATE TABLE IF NOT EXISTS agent_run_events" in command for command in commands)
    assert any("CREATE TABLE IF NOT EXISTS agent_run_tool_calls" in command for command in commands)
    assert any("transcript String" in command for command in commands)
    assert any("participants_json String" in command for command in commands)
    assert any("ADD COLUMN IF NOT EXISTS transcript" in command for command in commands)
    assert any("DROP COLUMN IF EXISTS cleaned_transcript" in command for command in commands)


def test_apple_voice_memos_schema_renames_legacy_tables() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []
    legacy_tables = {
        "voice_memo_files",
        "voice_memo_transcription_runs",
        "voice_memo_transcript_segments",
        "voice_memo_enrichments",
    }

    def fake_query(sql: str):
        if sql.startswith("EXISTS TABLE "):
            table = sql.removeprefix("EXISTS TABLE ")
            return [(1 if table in legacy_tables else 0,)]
        return []

    warehouse._command = commands.append
    warehouse._query = fake_query

    warehouse.ensure_apple_voice_memos_tables()

    assert "RENAME TABLE voice_memo_files TO apple_voice_memos_files" in commands
    assert "RENAME TABLE voice_memo_transcription_runs TO apple_voice_memos_transcription_runs" in commands
    assert "RENAME TABLE voice_memo_transcript_segments TO apple_voice_memos_transcript_segments" in commands
    assert "RENAME TABLE voice_memo_enrichments TO apple_voice_memos_enrichments" in commands


def test_apple_voice_memos_enrichment_schema_backfills_renamed_columns_before_drop() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [
        ("meeting_title",),
        ("meeting_start_at",),
        ("meeting_end_at",),
        ("attendees_json",),
        ("corrected_transcript",),
    ]

    warehouse.ensure_apple_voice_memos_tables()

    update_index = next(index for index, command in enumerate(commands) if "UPDATE" in command and "meeting_title" in command)
    drop_index = next(index for index, command in enumerate(commands) if "DROP COLUMN IF EXISTS meeting_title" in command)
    assert update_index < drop_index
    assert "participants_json = if(participants_json = '', attendees_json, participants_json)" in commands[update_index]


def test_apple_voice_memos_schema_recreates_old_content_hash_ordering() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [("CREATE TABLE apple_voice_memos_files (...) ORDER BY (account, content_sha256)",)]

    warehouse.ensure_apple_voice_memos_tables()

    assert commands[0] == "DROP TABLE IF EXISTS apple_voice_memos_files"
    assert any("ORDER BY (account, recording_id)" in command for command in commands)


def test_apple_voice_memos_untranscribed_query_orders_recent_recordings() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    queries: list[str] = []

    def fake_query(sql: str):
        queries.append(sql)
        return []

    warehouse._query = fake_query

    assert warehouse.load_untranscribed_apple_voice_memos_files(provider="assemblyai", limit=3) == []

    assert "FROM apple_voice_memos_files AS f" in queries[0]
    assert "apple_voice_memos_transcription_runs" in queries[0]
    assert "provider = 'assemblyai'" in queries[0]
    assert "status = 'completed'" in queries[0]
    assert "ORDER BY f.recorded_at DESC" in queries[0]
    assert "LIMIT 3" in queries[0]


def test_combined_account_state_view_is_not_created() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [(1,)]

    warehouse.ensure_tables()

    assert any("DROP VIEW IF EXISTS account_state_items" in command for command in commands)
    assert all("CREATE OR REPLACE VIEW account_state_items" not in command for command in commands)


def test_calendar_schema_creates_clean_transcript_views_when_sources_exist() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [(1,)]

    warehouse.ensure_calendar_tables()

    assert any("CREATE OR REPLACE VIEW clean_calendar_with_transcripts" in command for command in commands)
    assert any("CREATE OR REPLACE VIEW clean_transcripts_no_calendar_match" in command for command in commands)
    calendar_view_sql = next(command for command in commands if "CREATE OR REPLACE VIEW clean_calendar_with_transcripts" in command)
    no_match_view_sql = next(
        command for command in commands if "CREATE OR REPLACE VIEW clean_transcripts_no_calendar_match" in command
    )
    assert "c.calendar_account AS calendar_account" in calendar_view_sql
    assert "e.account AS recording_account" in calendar_view_sql
    assert "c.event_id AS event_id" in calendar_view_sql
    assert "if(e.title != '', e.title, c.summary) AS title" in calendar_view_sql
    assert "ON c.event_id = e.calendar_event_id" in calendar_view_sql
    assert "c.account = e.account" not in calendar_view_sql
    assert "e.summary AS summary" in calendar_view_sql
    assert "calendar_event_id AS calendar_event_id" not in calendar_view_sql
    assert "transcript_summary" not in calendar_view_sql
    assert "recording_id AS recording_id" in no_match_view_sql
    assert "if(e.title != '', e.title, f.title) AS title" in no_match_view_sql
    assert "e.calendar_event_id AS attempted_calendar_event_id" in no_match_view_sql
    assert "calendar_event_not_found" in no_match_view_sql
    assert "OR c.event_id = ''" in no_match_view_sql
    assert "e.summary AS summary" in no_match_view_sql
    assert "transcript_summary" not in no_match_view_sql


def test_calendar_schema_skips_clean_transcript_views_until_sources_exist() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [(0,)]

    warehouse.ensure_calendar_tables()

    assert all("CREATE OR REPLACE VIEW clean_calendar_with_transcripts" not in command for command in commands)
    assert all("CREATE OR REPLACE VIEW clean_transcripts_no_calendar_match" not in command for command in commands)


def test_apple_voice_memos_schema_creates_clean_transcript_views_when_calendar_exists() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands: list[str] = []

    warehouse._command = commands.append
    warehouse._query = lambda _sql: [(1,)]

    warehouse.ensure_apple_voice_memos_tables()

    assert any("CREATE OR REPLACE VIEW clean_calendar_with_transcripts" in command for command in commands)
    assert any("CREATE OR REPLACE VIEW clean_transcripts_no_calendar_match" in command for command in commands)


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
