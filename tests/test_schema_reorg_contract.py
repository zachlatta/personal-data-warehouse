from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from dotenv import load_dotenv

from tests.conftest import make_test_schema

from personal_data_warehouse.postgres import POSTGRES_TABLES, PostgresWarehouse, _default_sql, _identifier, _postgres_type
from personal_data_warehouse.schema import AGENT_SESSION_EVENT_COLUMNS
from personal_data_warehouse_alice_voice_recordings.sync import SOURCE as ALICE_VOICE_RECORDINGS_SOURCE
from personal_data_warehouse.relations import (
    CANONICAL_RELATIONS,
    QUERYABLE_SCHEMAS,
    SOURCE_RAW_SCHEMAS,
    physical_schema_name,
    qualify_sql_relations,
    relation,
)


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    namespace = make_test_schema("schema_reorg")
    wh = PostgresWarehouse(_postgres_url(), schema=namespace)
    try:
        yield wh
    finally:
        for schema in wh.physical_schema_names(include_private=True) + [wh.schema_namespace]:
            wh._raw_command(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        wh.close()


def test_alice_archive_source_uses_source_owned_name() -> None:
    assert ALICE_VOICE_RECORDINGS_SOURCE == "alice_voice_recordings"


def test_agent_documentation_uses_new_ai_query_surfaces() -> None:
    docs = Path("AGENTS.md").read_text()
    assert "agent_session_events" not in docs
    assert "clean_agent_sessions" not in docs
    assert "searchable_text" not in docs
    assert "marts.ai_conversation_events" in docs
    assert "marts.ai_conversation_sessions" in docs
    assert "search.search_text()" in docs


def test_relation_qualification_distinguishes_search_function_from_column() -> None:
    sql = """
        CREATE TABLE timeline_events ("search_text" text NOT NULL);
        CREATE INDEX idx ON timeline_events (search_text);
        SELECT * FROM search_text('needle', 10);
    """

    qualified = qualify_sql_relations(sql, namespace="pdw_test")

    assert '"search_text" text NOT NULL' in qualified
    assert "(search_text)" in qualified
    assert 'FROM "pdw_test_search"."search_text"(' in qualified
    assert qualified.count('"pdw_test_timeline"."events"') == 2


def test_relation_registry_encodes_source_owned_raw_schemas() -> None:
    assert SOURCE_RAW_SCHEMAS == (
        "gmail",
        "google_calendar",
        "google_contacts",
        "google_drive",
        "slack",
        "apple_notes",
        "apple_messages",
        "apple_voice_memos",
        "alice_voice_recordings",
        "whatsapp",
        "chatgpt",
        "claude_desktop",
        "claude_code",
        "codex",
        "openclaw",
        "pi",
    )

    expected = {
        "gmail_messages": ("gmail", "messages"),
        "gmail_attachments": ("gmail", "attachments"),
        "calendar_events": ("google_calendar", "events"),
        "contact_cards": ("google_contacts", "cards"),
        "google_drive_files": ("google_drive", "files"),
        "slack_messages": ("slack", "messages"),
        "apple_notes": ("apple_notes", "notes"),
        "apple_messages": ("apple_messages", "messages"),
        "apple_voice_memos_files": ("apple_voice_memos", "files"),
        "whatsapp_messages": ("whatsapp", "messages"),
        "chatgpt_events": ("chatgpt", "events"),
        "claude_desktop_events": ("claude_desktop", "events"),
        "claude_code_events": ("claude_code", "events"),
        "codex_events": ("codex", "events"),
        "openclaw_events": ("openclaw", "events"),
        "pi_events": ("pi", "events"),
        "file_attachment_enrichments": ("enrichment", "file_attachment_enrichments"),
        "agent_runs": ("ai_processing", "agent_runs"),
        "upstream_mutations": ("upstream_mutations", "operations"),
        "timeline_events": ("timeline", "events"),
        "search_schema_state": ("search", "schema_state"),
        "chatgpt_sessions": ("private", "chatgpt_sessions"),
        "claude_desktop_credentials": ("private", "claude_desktop_credentials"),
        "whatsapp_client_sessions": ("private", "whatsapp_client_sessions"),
    }
    for logical_name, (schema, table) in expected.items():
        rel = relation(logical_name)
        assert (rel.schema, rel.name) == (schema, table)

    assert "agent_session_events" not in CANONICAL_RELATIONS, (
        "mixed AI events must be split into source-owned raw event tables and re-unified in marts"
    )
    assert "private" not in QUERYABLE_SCHEMAS


def test_fresh_warehouse_creates_source_owned_and_derived_schemas(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_tables()
    warehouse.ensure_calendar_tables()
    warehouse.ensure_contacts_tables()
    warehouse.ensure_google_drive_source_tables()
    warehouse.ensure_slack_tables()
    warehouse.ensure_apple_notes_tables()
    warehouse.ensure_apple_messages_tables()
    warehouse.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    warehouse.ensure_whatsapp_tables()
    warehouse.ensure_agent_sessions_tables()
    warehouse.ensure_timeline_tables()
    warehouse.ensure_upstream_mutation_tables()

    rows = warehouse._query(
        """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = ANY(%s)
        ORDER BY table_schema, table_name
        """,
        (warehouse.physical_schema_names(include_private=True),),
    )
    relations = {(schema, table) for schema, table, _type in rows}

    expected_relations = {
        (physical_schema_name("gmail", namespace=warehouse.schema_namespace), "messages"),
        (physical_schema_name("gmail", namespace=warehouse.schema_namespace), "attachments"),
        (physical_schema_name("google_calendar", namespace=warehouse.schema_namespace), "events"),
        (physical_schema_name("google_contacts", namespace=warehouse.schema_namespace), "cards"),
        (physical_schema_name("google_drive", namespace=warehouse.schema_namespace), "files"),
        (physical_schema_name("slack", namespace=warehouse.schema_namespace), "messages"),
        (physical_schema_name("apple_notes", namespace=warehouse.schema_namespace), "notes"),
        (physical_schema_name("apple_messages", namespace=warehouse.schema_namespace), "messages"),
        (physical_schema_name("apple_voice_memos", namespace=warehouse.schema_namespace), "files"),
        (physical_schema_name("whatsapp", namespace=warehouse.schema_namespace), "messages"),
        (physical_schema_name("chatgpt", namespace=warehouse.schema_namespace), "events"),
        (physical_schema_name("claude_desktop", namespace=warehouse.schema_namespace), "events"),
        (physical_schema_name("claude_code", namespace=warehouse.schema_namespace), "events"),
        (physical_schema_name("codex", namespace=warehouse.schema_namespace), "events"),
        (physical_schema_name("openclaw", namespace=warehouse.schema_namespace), "events"),
        (physical_schema_name("pi", namespace=warehouse.schema_namespace), "events"),
        (physical_schema_name("marts", namespace=warehouse.schema_namespace), "ai_conversation_events"),
        (physical_schema_name("marts", namespace=warehouse.schema_namespace), "ai_conversation_sessions"),
        (physical_schema_name("timeline", namespace=warehouse.schema_namespace), "events"),
        (physical_schema_name("search", namespace=warehouse.schema_namespace), "schema_state"),
        (physical_schema_name("enrichment", namespace=warehouse.schema_namespace), "file_attachment_enrichments"),
        (physical_schema_name("ai_processing", namespace=warehouse.schema_namespace), "agent_runs"),
        (physical_schema_name("upstream_mutations", namespace=warehouse.schema_namespace), "operations"),
        (physical_schema_name("private", namespace=warehouse.schema_namespace), "chatgpt_sessions"),
        (physical_schema_name("private", namespace=warehouse.schema_namespace), "claude_desktop_credentials"),
        (physical_schema_name("private", namespace=warehouse.schema_namespace), "whatsapp_client_sessions"),
    }
    assert expected_relations <= relations

    search_schema = physical_schema_name("search", namespace=warehouse.schema_namespace)
    search_type_rows = warehouse._query(
        """
        SELECT n.nspname, t.typname
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = %s AND t.typname = 'text_hit'
        """,
        (search_schema,),
    )
    assert search_type_rows == [(search_schema, "text_hit")]
    search_function_rows = warehouse._query(
        """
        SELECT n.nspname, p.proname
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = %s AND p.proname IN ('search_text', 'search_text_sources')
        ORDER BY p.proname
        """,
        (search_schema,),
    )
    assert search_function_rows == [(search_schema, "search_text"), (search_schema, "search_text_sources")]
    util_schema = physical_schema_name("util", namespace=warehouse.schema_namespace)
    util_function_rows = warehouse._query(
        """
        SELECT n.nspname, p.proname
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = %s AND p.proname = 'utf8_byte_prefix'
        """,
        (util_schema,),
    )
    assert util_function_rows == [(util_schema, "utf8_byte_prefix")]

    legacy_names = sorted(set(CANONICAL_RELATIONS) | {"agent_session_events"})
    old_public_rows = warehouse._query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = ANY(%s)
        UNION ALL
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = %s AND table_name = ANY(%s)
        """,
        (warehouse.schema_namespace, legacy_names, warehouse.schema_namespace, legacy_names),
    )
    assert old_public_rows == []


def _agent_event_row(*, source: str, session_id: str, event_uuid: str, seq: int) -> dict[str, object]:
    now = datetime(2026, 7, 9, 7, tzinfo=UTC)
    row = {column: "" for column in AGENT_SESSION_EVENT_COLUMNS}
    row.update(
        {
            "source": source,
            "session_id": session_id,
            "event_uuid": event_uuid,
            "account": "zach@example.test",
            "device": "test-device",
            "seq": seq,
            "occurred_at": now,
            "role": "user",
            "event_type": "message",
            "session_title": f"{source} session",
            "text": f"hello from {source}",
            "raw_json": "{}",
            "ingested_at": now,
            "sync_version": 1,
        }
    )
    for column in (
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_creation_tokens",
        "is_sidechain",
    ):
        row[column] = 0
    return row


def test_ai_events_split_by_source_and_reunified_in_marts(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_agent_sessions_tables()
    rows = [
        _agent_event_row(source="chatgpt", session_id="chatgpt-1", event_uuid="event-chatgpt", seq=1),
        _agent_event_row(source="claude_desktop", session_id="desktop-1", event_uuid="event-desktop", seq=1),
        _agent_event_row(source="claude_code", session_id="claude-code-1", event_uuid="event-claude-code", seq=1),
        _agent_event_row(source="codex", session_id="codex-1", event_uuid="event-codex", seq=1),
        _agent_event_row(source="openclaw", session_id="openclaw-1", event_uuid="event-openclaw", seq=1),
        _agent_event_row(source="pi", session_id="pi-1", event_uuid="event-pi", seq=1),
    ]
    warehouse.insert_agent_session_events(rows)
    warehouse.insert_agent_session_events(rows)  # idempotent upsert into per-source tables.

    for source in ("chatgpt", "claude_desktop", "claude_code", "codex", "openclaw", "pi"):
        source_rows = warehouse._query(
            f"SELECT source, session_id, text FROM {warehouse.sql_relation(source + '_events')}"
        )
        assert source_rows == [(source, rows[[row["source"] for row in rows].index(source)]["session_id"], f"hello from {source}")]

    unified = warehouse._query(
        f"""
        SELECT source, session_id, event_uuid
        FROM {warehouse.sql_relation('ai_conversation_events')}
        ORDER BY source
        """
    )
    assert len(unified) == 6
    assert {row[0] for row in unified} == {"chatgpt", "claude_desktop", "claude_code", "codex", "openclaw", "pi"}

    sessions = warehouse._query(
        f"""
        SELECT source, session_id, title, first_prompt, event_count
        FROM {warehouse.sql_relation('clean_agent_sessions')}
        ORDER BY source
        """
    )
    assert len(sessions) == 5
    assert {row[4] for row in sessions} == {1}

    old_mixed_rows = warehouse._query(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = 'agent_session_events'
        """,
        (warehouse.schema_namespace,),
    )
    assert old_mixed_rows == []


def _create_legacy_agent_session_events_table(warehouse: PostgresWarehouse) -> None:
    spec = POSTGRES_TABLES["agent_session_events"]
    columns = ", ".join(
        f"{_identifier(column)} {_postgres_type(column, table='agent_session_events')} NOT NULL DEFAULT {_default_sql(column, table='agent_session_events')}"
        for column in spec.columns
    )
    primary_key = ", ".join(_identifier(column) for column in spec.primary_key)
    warehouse._raw_command(
        f"""
        CREATE TABLE "{warehouse.schema_namespace}".agent_session_events (
            {columns},
            PRIMARY KEY ({primary_key})
        )
        """
    )


def test_old_layout_mixed_ai_events_migrate_to_source_tables_without_legacy_view(warehouse: PostgresWarehouse) -> None:
    _create_legacy_agent_session_events_table(warehouse)
    warehouse._raw_command(
        f"""
        CREATE VIEW "{warehouse.schema_namespace}".clean_agent_sessions AS
        SELECT source, session_id, count(*) AS event_count
        FROM "{warehouse.schema_namespace}".agent_session_events
        GROUP BY source, session_id
        """
    )
    legacy_row = _agent_event_row(source="claude_code", session_id="legacy-session", event_uuid="legacy-event", seq=1)
    columns = AGENT_SESSION_EVENT_COLUMNS
    placeholders = ", ".join(["%s"] * len(columns))
    column_sql = ", ".join(_identifier(column) for column in columns)
    warehouse._raw_command(
        f"""
        INSERT INTO "{warehouse.schema_namespace}".agent_session_events ({column_sql})
        VALUES ({placeholders})
        """,
        tuple(legacy_row[column] for column in columns),
    )

    warehouse.ensure_agent_sessions_tables()

    migrated = warehouse._query(
        f"""
        SELECT source, session_id, event_uuid, text
        FROM {warehouse.sql_relation('claude_code_events')}
        """
    )
    assert migrated == [("claude_code", "legacy-session", "legacy-event", "hello from claude_code")]
    unified = warehouse._query(
        f"""
        SELECT source, session_id, event_uuid
        FROM {warehouse.sql_relation('ai_conversation_events')}
        """
    )
    assert unified == [("claude_code", "legacy-session", "legacy-event")]
    legacy_rows = warehouse._query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name IN ('agent_session_events', 'clean_agent_sessions')
        UNION ALL
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = %s AND table_name IN ('agent_session_events', 'clean_agent_sessions')
        """,
        (warehouse.schema_namespace, warehouse.schema_namespace),
    )
    assert legacy_rows == []


def test_old_layout_raw_control_tables_migrate_without_public_leftovers(warehouse: PostgresWarehouse) -> None:
    warehouse._raw_command(
        f"""
        CREATE TABLE "{warehouse.schema_namespace}".chatgpt_sessions (
            account text NOT NULL,
            session_key text NOT NULL DEFAULT 'default',
            session_token text NOT NULL DEFAULT '',
            source_browser text NOT NULL DEFAULT '',
            token_sha256 text NOT NULL DEFAULT '',
            published_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
            updated_at timestamptz NOT NULL DEFAULT now(),
            sync_version bigint NOT NULL DEFAULT 1,
            PRIMARY KEY (account, session_key)
        )
        """
    )
    warehouse._raw_command(
        f"""
        INSERT INTO "{warehouse.schema_namespace}".chatgpt_sessions (account, session_key, session_token, token_sha256)
        VALUES ('zach@example.test', 'default', 'token-redacted', 'sha')
        """
    )
    warehouse._raw_command(
        f"""
        CREATE TABLE "{warehouse.schema_namespace}".pdw_search_schema_state (
            id smallint PRIMARY KEY DEFAULT 1,
            signature text NOT NULL,
            CONSTRAINT pdw_search_schema_state_single_row CHECK (id = 1)
        )
        """
    )
    warehouse._raw_command(
        f"INSERT INTO \"{warehouse.schema_namespace}\".pdw_search_schema_state (id, signature) VALUES (1, 'legacy-signature')"
    )

    warehouse.ensure_chatgpt_session_table()
    assert warehouse._stored_search_schema_signature() == "legacy-signature"

    chatgpt_rows = warehouse._query(
        f"SELECT account, session_key, session_token FROM {warehouse.sql_relation('chatgpt_sessions')}"
    )
    assert chatgpt_rows == [("zach@example.test", "default", "token-redacted")]
    legacy_rows = warehouse._query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name IN ('chatgpt_sessions', 'pdw_search_schema_state')
        ORDER BY table_name
        """,
        (warehouse.schema_namespace,),
    )
    assert legacy_rows == []


def test_old_layout_sync_state_migrates_without_legacy_public_view(warehouse: PostgresWarehouse) -> None:
    warehouse._raw_command(
        f"""
        CREATE TABLE "{warehouse.schema_namespace}".gmail_sync_state (
            account text PRIMARY KEY,
            last_history_id bigint NOT NULL DEFAULT 0,
            last_sync_type text NOT NULL DEFAULT '',
            status text NOT NULL DEFAULT '',
            error text NOT NULL DEFAULT '',
            updated_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz
        )
        """
    )
    updated_at = datetime(2026, 7, 9, 6, tzinfo=UTC)
    warehouse._raw_command(
        f"""
        INSERT INTO "{warehouse.schema_namespace}".gmail_sync_state
            (account, last_history_id, last_sync_type, status, error, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        ("zach@example.test", 123, "partial", "ok", "", updated_at),
    )

    warehouse.ensure_tables()

    migrated = warehouse._query(
        f"""
        SELECT account, last_history_id, last_sync_type, status, updated_at
        FROM {warehouse.sql_relation('gmail_sync_state')}
        """
    )
    assert migrated == [("zach@example.test", 123, "partial", "ok", updated_at)]

    legacy_rows = warehouse._query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = 'gmail_sync_state'
        UNION ALL
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = %s AND table_name = 'gmail_sync_state'
        """,
        (warehouse.schema_namespace, warehouse.schema_namespace),
    )
    assert legacy_rows == []
