from __future__ import annotations

import os
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.postgres import POSTGRES_INDEXES, POSTGRES_TABLES, PostgresWarehouse
from personal_data_warehouse.relations import AI_EVENT_SOURCE_RELATIONS, CANONICAL_RELATIONS, physical_schema_names
from personal_data_warehouse.timeline import (
    RAW_DDL_TABLES,
    TIMELINE_ADAPTERS,
    TIMELINE_NORMALIZED_COLUMNS,
    TIMELINE_TABLE_COVERAGE,
    BACKFILL_CURSOR_START,
    TimelineSyncEngine,
    TimelineSyncError,
    adapter_by_name,
    timeline_upsert_sql,
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
        cleanup_test_warehouse(wh)


def _ensure_all_source_tables(wh: PostgresWarehouse) -> None:
    """Run every ensure_* path so the schema contains every warehouse table."""
    wh.ensure_tables()
    wh.ensure_calendar_tables()
    wh.ensure_contacts_tables()
    wh.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    wh.ensure_alice_voice_recordings_tables()
    wh.ensure_apple_notes_tables()
    wh.ensure_apple_messages_tables()
    wh.ensure_whatsapp_tables()
    wh.ensure_photos_tables()
    wh.ensure_agent_sessions_tables()
    wh.ensure_claude_desktop_tables()
    wh.ensure_agent_tables()
    wh.ensure_slack_tables()
    wh.ensure_upstream_mutation_tables()
    wh.ensure_google_drive_source_tables()
    wh.ensure_whoop_tables()
    wh.ensure_plaid_tables()
    wh.ensure_finance_tables()
    wh.ensure_manual_finance_tables()
    wh.ensure_timeline_tables()


# --- coverage registry (pure) -------------------------------------------------


def test_every_registered_table_is_classified():
    registered = set(POSTGRES_TABLES) | set(RAW_DDL_TABLES)
    classified = set(TIMELINE_TABLE_COVERAGE)
    assert registered - classified == set(), "warehouse tables missing timeline classification"
    assert classified - registered == set(), "timeline classifications for unknown tables"


def test_adapter_source_tables_are_classified_as_events():
    adapter_tables: set[str] = set()
    for adapter in TIMELINE_ADAPTERS:
        if adapter.source_table == "agent_session_events":
            adapter_tables.update(AI_EVENT_SOURCE_RELATIONS.values())
        else:
            adapter_tables.add(adapter.source_table)
    event_tables = {
        table for table, coverage in TIMELINE_TABLE_COVERAGE.items() if coverage.role == "events"
    }
    assert adapter_tables == event_tables


def test_adapter_names_are_unique_and_resolvable():
    names = [adapter.name for adapter in TIMELINE_ADAPTERS]
    assert len(names) == len(set(names))
    for name in names:
        assert adapter_by_name(name).name == name
    with pytest.raises(KeyError):
        adapter_by_name("nope")


def test_timeline_includes_alice_and_finance_activity_adapters():
    requested = {
        "alice_voice_recording": ("alice_voice_recordings", "voice_recording"),
        "finance_transaction": ("finance", "transaction"),
        "finance_observation": ("finance", "balance_observation"),
        "manual_finance_document": ("finance", "document"),
    }

    for adapter_name, expected_source_kind in requested.items():
        adapter = adapter_by_name(adapter_name)
        assert (adapter.source, adapter.kind) == expected_source_kind


def test_detail_coverage_points_at_covered_tables():
    for table, coverage in TIMELINE_TABLE_COVERAGE.items():
        if coverage.role != "detail":
            continue
        parent = TIMELINE_TABLE_COVERAGE.get(coverage.parent)
        assert parent is not None, f"{table} detail parent {coverage.parent!r} is unclassified"
        assert parent.role in ("events", "detail"), (
            f"{table} detail parent {coverage.parent!r} must chain to an events table"
        )

        seen = {table}
        cursor = coverage
        while cursor.role == "detail":
            assert cursor.parent not in seen, f"detail coverage cycle: {seen} -> {cursor.parent}"
            seen.add(cursor.parent)
            cursor = TIMELINE_TABLE_COVERAGE[cursor.parent]
        assert cursor.role == "events", f"{table} does not ultimately belong to a timeline event"


def test_go_warming_filter_catalog_matches_runtime_event_sources():
    go_source = (
        Path(__file__).resolve().parents[1] / "app" / "internal" / "server" / "timeline.go"
    ).read_text()
    catalog_block = go_source.split("var timelineFilterCatalog", 1)[1].split(
        "var timelinePriorityCatalog", 1
    )[0]
    actual = set(re.findall(r'\{source: "([^"]+)", kind: "([^"]+)"\}', catalog_block))
    expected = {
        (adapter.source, adapter.kind)
        for adapter in TIMELINE_ADAPTERS
        if adapter.name != "agent_session"
    }
    expected.update((source, "agent_session") for source in AI_EVENT_SOURCE_RELATIONS)
    assert actual == expected


def test_coverage_roles_are_valid():
    for table, coverage in TIMELINE_TABLE_COVERAGE.items():
        assert coverage.role in ("events", "detail", "entity", "state"), table


def test_adapter_sql_carries_the_pagination_contract():
    for adapter in TIMELINE_ADAPTERS:
        for param in ("%(cursor_ts)s", "%(cursor_id)s", "%(limit)s"):
            assert param in adapter.backfill_sql, (adapter.name, param)
        for param in ("%(watermark_ts)s", "%(watermark_id)s", "%(limit)s"):
            assert param in adapter.incremental_sql, (adapter.name, param)
        assert ";" not in adapter.backfill_sql
        assert ";" not in adapter.incremental_sql
        assert adapter.max_ingest_sql.lstrip().upper().startswith("SELECT")


def test_heavy_adapters_bound_incremental_scans_to_changed_candidates():
    # The incremental predicate is a computed ingest_ts (GREATEST over the
    # attachment/enrichment LATERAL), which no index can serve: without a
    # candidate pre-filter every tick re-evaluates the full multi-join for
    # every source row (measured at ~3 minutes per 5-minute tick for
    # apple_message in production — the single largest recurring load on the
    # database). The attachment-carrying adapters must instead join a
    # watermark-driven candidate set covering every input of ingest_ts:
    # message ingestion, attachment ingestion, and enrichment updates.
    for name in ("apple_message", "gmail_email", "whatsapp_message"):
        adapter = adapter_by_name(name)
        assert "pdw_changed" in adapter.incremental_sql, name
        assert "e.updated_at >= %(watermark_ts)s" in adapter.incremental_sql, (
            f"{name}: enrichment updates must re-emit their parent message"
        )
        # The candidate join is an incremental-only optimization; backfill and
        # first-contact max-ingest stay full-range.
        assert "pdw_changed" not in adapter.backfill_sql, name
        assert "pdw_changed" not in adapter.max_ingest_sql, name


def test_apple_message_contact_changes_invalidate_message_history():
    adapter = adapter_by_name("apple_message")

    assert "contact_cards" in adapter.incremental_sql
    assert "apple_contact_cards" in adapter.incremental_sql
    assert "contact_sync.latest_synced_at" in adapter.incremental_sql
    assert "identity_sync.latest_synced_at" in adapter.incremental_sql
    assert "m.is_from_me = 0" in adapter.incremental_sql
    assert adapter.incremental_sql.count("LIMIT %(limit)s") == 2
    assert adapter.incremental_sql.index("pdw_changed") < adapter.incremental_sql.index(
        "apple_message_handles h"
    )
    assert adapter.batch_size == 2_000
    assert adapter.max_incremental_batches_per_run == 1
    assert adapter.refresh_hours == 168


def test_upsert_sql_bumps_seq_only_on_content_change():
    sql = timeline_upsert_sql()
    assert "ON CONFLICT (adapter, event_id) DO UPDATE" in sql
    assert "seq = nextval('timeline_events_seq')" in sql
    assert "IS DISTINCT FROM" in sql
    # A re-sync that only refreshes the source's ingestion timestamp must not
    # count as a content change, or every re-synced row looks new to
    # arrival-order consumers.
    guard = sql.split("WHERE", 1)[1]
    assert "timeline_events.ingest_ts" not in guard


def test_timeline_indexes_are_registered_for_timeline_tables():
    names = {index.name for index in POSTGRES_INDEXES if index.table == "timeline_events"}
    assert {
        "timeline_events_time_idx",
        "timeline_events_source_time_idx",
        "timeline_events_priority_time_idx",
        "timeline_events_search_text_bm25_idx",
        "timeline_events_search_text_trgm_idx",
    } <= names
    # Retired after production usage counters showed zero lifetime scans: the
    # kind filter rides the time/priority indexes and nothing pages by bare seq.
    assert "timeline_events_kind_time_idx" not in names
    assert "timeline_events_seq_idx" not in names


# --- live schema coverage (Postgres) -------------------------------------------


def test_live_schema_has_no_unclassified_tables(warehouse):
    _ensure_all_source_tables(warehouse)
    rows = warehouse._query(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = ANY(%s) AND table_type = 'BASE TABLE'
        """,
        (warehouse.physical_schema_names(include_private=True),),
    )
    physical_to_logical = {
        (rel.with_namespace(warehouse.schema_namespace).schema, rel.name): logical
        for logical, rel in CANONICAL_RELATIONS.items()
        if logical in TIMELINE_TABLE_COVERAGE
    }
    live_tables = {physical_to_logical.get((schema, table), f"{schema}.{table}") for schema, table in rows}
    unclassified = live_tables - set(TIMELINE_TABLE_COVERAGE)
    assert unclassified == set(), (
        "tables exist in canonical warehouse schemas without a timeline classification; "
        "add them to TIMELINE_TABLE_COVERAGE (and an adapter if they hold activity): "
        f"{sorted(unclassified)}"
    )
    # And the classification list should not reference canonical tables that no longer exist.
    expected_physical = {
        (rel.with_namespace(warehouse.schema_namespace).schema, rel.name): logical
        for logical, rel in CANONICAL_RELATIONS.items()
        if logical in (set(POSTGRES_TABLES) | set(RAW_DDL_TABLES))
    }
    live_physical = set(rows)
    stale = {
        logical
        for physical, logical in expected_physical.items()
        if physical not in live_physical and logical != "agent_session_events"
    }
    assert stale == set(), f"classified tables missing from the live schema: {sorted(stale)}"


def test_ensure_timeline_tables_is_idempotent_and_indexed(warehouse):
    warehouse.ensure_timeline_tables()
    warehouse.ensure_timeline_tables()
    rows = warehouse._query(
        "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND tablename = 'events'",
        (warehouse.physical_schema_name("timeline"),),
    )
    names = {row[0] for row in rows}
    assert "timeline_events_time_idx" in names
    assert "timeline_events_source_time_idx" in names
    # seq must be sequence-backed so upserts can bump it.
    warehouse._command(
        "INSERT INTO timeline_events (adapter, event_id, source, kind, event_ts, source_table) "
        "VALUES ('t', 'e1', 's', 'k', now(), 'x'), ('t', 'e2', 's', 'k', now(), 'x')"
    )
    seqs = [row[0] for row in warehouse._query("SELECT seq FROM timeline_events ORDER BY event_id")]
    assert seqs[0] != seqs[1]


def test_adapter_queries_run_against_the_real_schema(warehouse):
    """Every adapter's generated SQL must execute against the ensured schema.

    This is the drift guard: renaming or dropping a source column breaks the
    corresponding adapter here, not in production.
    """
    _ensure_all_source_tables(warehouse)
    engine = _engine(warehouse)
    try:
        engine._connect()
        for adapter in TIMELINE_ADAPTERS:
            backfill = engine._fetch(
                adapter.backfill_sql,
                {"cursor_ts": BACKFILL_CURSOR_START, "cursor_id": "", "limit": 5},
            )
            incremental = engine._fetch(
                adapter.incremental_sql,
                {
                    "watermark_ts": datetime(1970, 1, 1, tzinfo=UTC),
                    "watermark_id": "",
                    "limit": 5,
                },
            )
            assert backfill == []
            assert incremental == []
            with engine._source_conn.cursor() as cursor:
                cursor.execute(
                    engine._source_sql(adapter.backfill_sql),
                    {"cursor_ts": BACKFILL_CURSOR_START, "cursor_id": "", "limit": 5},
                )
                columns = [d[0] for d in cursor.description]
            assert tuple(columns) == TIMELINE_NORMALIZED_COLUMNS, adapter.name
    finally:
        engine.close()


# --- sync engine (Postgres) -----------------------------------------------------


def _engine(warehouse, dest_schema: str | None = None, **kwargs) -> TimelineSyncEngine:
    return TimelineSyncEngine(
        source_url=_postgres_url(),
        source_schema=warehouse._schema,
        dest_schema=dest_schema or warehouse._schema,
        **kwargs,
    )


_NOW = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


def _seed_sources(wh: PostgresWarehouse) -> None:
    """A little bit of everything, with distinct event times."""
    wh._command(
        """
        INSERT INTO gmail_messages (account, message_id, thread_id, internal_date, subject,
                                    from_address, to_addresses, snippet, synced_at)
        VALUES ('z@x.test', 'm1', 'th1', %s, 'Hello world', 'alice@example.test',
                %s, 'hi there', %s)
        """,
        (_NOW - timedelta(hours=1), ["Zach <z@x.test>"], _NOW),
    )
    wh._command(
        """
        INSERT INTO slack_users (account, team_id, user_id, display_name)
        VALUES ('z', 'T1', 'U1', 'alice')
        """
    )
    wh._command(
        """
        INSERT INTO slack_account_identities (account, team_id, user_id)
        VALUES ('z', 'T1', 'UME')
        """
    )
    wh._command(
        """
        INSERT INTO slack_conversations (account, team_id, conversation_id, name, is_member)
        VALUES ('z', 'T1', 'C1', 'general', 1)
        """
    )
    wh._command(
        """
        INSERT INTO slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, text, synced_at)
        VALUES ('z', 'T1', 'C1', '1000.1', %s, 'U1', 'slack says hi', %s)
        """,
        (_NOW - timedelta(hours=2), _NOW),
    )
    wh._command(
        """
        INSERT INTO slack_files (account, team_id, file_id, conversation_id, message_ts,
                                 user_id, created_at, name, title, mimetype, synced_at)
        VALUES ('z', 'T1', 'F1', 'C1', '1000.1', 'U1', %s, 'notes.pdf', '', 'application/pdf', %s)
        """,
        (_NOW - timedelta(hours=3), _NOW),
    )
    wh._command(
        """
        INSERT INTO apple_message_handles (account, handle_id, address)
        VALUES ('z@x.test', 'h1', '+15551234567')
        """
    )
    wh._command(
        """
        INSERT INTO apple_message_chats (account, chat_id, display_name)
        VALUES ('z@x.test', 'c1', 'Family')
        """
    )
    wh._command(
        """
        INSERT INTO apple_message_chat_messages (account, chat_id, message_id, message_date, ingested_at)
        VALUES ('z@x.test', 'c1', 'am1', %s, %s)
        """,
        (_NOW - timedelta(hours=4), _NOW),
    )
    wh._command(
        """
        INSERT INTO apple_messages (account, message_id, handle_id, body_text, message_at,
                                    is_from_me, ingested_at)
        VALUES ('z@x.test', 'am1', 'h1', 'imessage body', %s, 0, %s)
        """,
        (_NOW - timedelta(hours=4), _NOW),
    )
    # Zach has replied in this chat, so it reads as a conversation rather
    # than a one-way broadcast.
    wh._command(
        """
        INSERT INTO apple_message_chat_messages (account, chat_id, message_id, message_date, ingested_at)
        VALUES ('z@x.test', 'c1', 'am0', %s, %s)
        """,
        (_NOW - timedelta(hours=3), _NOW),
    )
    wh._command(
        """
        INSERT INTO apple_messages (account, message_id, handle_id, body_text, message_at,
                                    is_from_me, ingested_at)
        VALUES ('z@x.test', 'am0', '', 'sounds good!', %s, 1, %s)
        """,
        (_NOW - timedelta(hours=3), _NOW),
    )
    wh._command(
        """
        INSERT INTO whatsapp_chats (account, chat_id, name)
        VALUES ('z@x.test', 'chat@g.us', 'The Group')
        """
    )
    wh._command(
        """
        INSERT INTO whatsapp_messages (account, chat_id, message_id, sender_jid, push_name,
                                       body_text, message_at, is_from_me, ingested_at)
        VALUES ('z@x.test', 'chat@g.us', 'wm1', 'p@s.whatsapp.net', 'bob',
                'whatsapp body', %s, 0, %s)
        """,
        (_NOW - timedelta(hours=5), _NOW),
    )
    for seq, (role, text) in enumerate([("user", "fix the bug"), ("assistant", "done")]):
        wh._command(
            """
            INSERT INTO agent_session_events (source, session_id, event_uuid, seq, occurred_at,
                                              role, text, session_title, cwd, device, ingested_at)
            VALUES ('claude_code', 'sess1', %s, %s, %s, %s, %s, 'Fix the bug', '/repo', 'porygon', %s)
            """,
            (f"u{seq}", seq, _NOW - timedelta(hours=6) + timedelta(minutes=seq), role, text, _NOW),
        )
    wh._command(
        """
        INSERT INTO apple_note_revisions (account, note_id, revision_id, title, body_text,
                                          folder_path, modified_at, ingested_at)
        VALUES ('z@x.test', 'n1', 'r1', 'Groceries', 'milk, eggs', 'Notes', %s, %s)
        """,
        (_NOW - timedelta(hours=7), _NOW),
    )
    wh._command(
        """
        INSERT INTO apple_voice_memos_files (account, recording_id, title, filename, recorded_at, ingested_at)
        VALUES ('z@x.test', 'rec1', 'Standup', 'standup.m4a', %s, %s)
        """,
        (_NOW - timedelta(hours=8), _NOW),
    )
    wh._command(
        """
        INSERT INTO apple_voice_memos_enrichments (account, recording_id, provider, model,
                                                   prompt_version, title, summary, created_at)
        VALUES ('z@x.test', 'rec1', 'p', 'm', 'v1', 'Standup notes', 'we discussed things', %s)
        """,
        (_NOW,),
    )
    wh._command(
        """
        INSERT INTO alice_voice_recordings (account, recording_id, title, filename,
                                            content_type, recorded_at, duration_seconds, ingested_at)
        VALUES ('z@x.test', 'alice-rec1', 'Alice walk', 'alice-walk.m4a',
                'audio/mp4', %s, 321, %s)
        """,
        (_NOW - timedelta(hours=8, minutes=30), _NOW),
    )
    wh._command(
        """
        INSERT INTO calendar_events (account, calendar_id, event_id, summary, description,
                                     organizer_email, start_at, end_at, updated_at, synced_at)
        VALUES ('z@x.test', 'cal1', 'ev1', 'Team sync', 'weekly', 'z@x.test', %s, %s, %s, %s)
        """,
        (_NOW - timedelta(hours=9), _NOW - timedelta(hours=8, minutes=30), _NOW, _NOW),
    )
    wh._command(
        """
        INSERT INTO google_drive_files (account, file_id, name, mime_type, folder_path,
                                        last_modifying_user, modified_time, ingested_at)
        VALUES ('z@x.test', 'f1', 'Design doc', 'application/vnd.google-apps.document',
                'My Drive', 'zach', %s, %s)
        """,
        (_NOW - timedelta(hours=10), _NOW),
    )
    wh._command(
        """
        INSERT INTO contact_cards (source, account, source_kind, address_book_id, card_id,
                                   display_name, organization, source_updated_at, synced_at)
        VALUES ('google', 'z@x.test', 'personal', 'ab1', 'card1', 'Ada Example', 'Example Engines',
                %s, %s)
        """,
        (_NOW - timedelta(hours=11), _NOW),
    )
    wh._command(
        """
        INSERT INTO photo_assets (photo_id, account, kind, capture_ts, camera_make, camera_model,
                                  width, height, best_file_sha256, best_file_mime_type,
                                  best_file_filename, thumbnail_content_sha256,
                                  thumbnail_content_type, thumbnail_storage_file_id,
                                  created_at, updated_at)
        VALUES ('ph1', 'z@x.test', 'image', %s, 'Apple', 'iPhone 16 Pro', 4284, 5712,
                'stillsha', 'image/heic', 'IMG_0001.HEIC', 'thumbsha', 'image/jpeg', 'drive-th1',
                %s, %s)
        """,
        (_NOW - timedelta(hours=10, minutes=30), _NOW, _NOW),
    )
    wh._command(
        """
        INSERT INTO photo_asset_files (source, account, source_native_id, role, content_sha256,
                                       photo_id, match_method, created_at)
        VALUES ('apple_photos', 'z@x.test', 'UUID-1', 'original', 'stillsha', 'ph1', 'new', %s)
        """,
        (_NOW,),
    )
    wh._command(
        """
        INSERT INTO file_attachment_enrichments (content_sha256, ai_provider, ai_model,
                                                 ai_prompt_version, text, updated_at)
        VALUES ('thumbsha', 'agent_codex', 'm', 'photo-agent-v1',
                'A golden retriever on a beach at sunset', %s)
        """,
        (_NOW,),
    )
    wh._command(
        """
        INSERT INTO upstream_mutations (id, provider, operation, status, title, reason,
                                        requested_by, executed_at, created_at, updated_at)
        VALUES ('mut1', 'slack', 'chat.postMessage', 'executed', 'Send standup reminder',
                'weekly reminder', 'assistant', %s, %s, %s)
        """,
        (_NOW - timedelta(hours=12), _NOW - timedelta(hours=13), _NOW),
    )
    wh._command(
        """
        INSERT INTO upstream_mutation_requests (id, status, title, reason, requested_by,
                                                created_at, updated_at)
        VALUES ('req1', 'approved', 'Standup reminders', 'requested by zach', 'assistant', %s, %s)
        """,
        (_NOW - timedelta(hours=13), _NOW),
    )
    wh._command(
        """
        INSERT INTO agent_runs (run_id, provider, model, task_type, subject_id, status,
                                started_at, completed_at)
        VALUES ('run1', 'codex', 'gpt-5', 'attachment_enrichment', 'sha1', 'ok', %s, %s)
        """,
        (_NOW - timedelta(hours=14), _NOW - timedelta(hours=13, minutes=50)),
    )
    sync_version = int(_NOW.timestamp() * 1_000_000)
    wh._command(
        """
        INSERT INTO finance_accounts (account_id, account, name, kind, side, currency,
                                      institution, created_at, updated_at, sync_version)
        VALUES ('fa1', 'z@x.test', 'Checking', 'checking', 'asset', 'USD',
                'Example Bank', %s, %s, %s)
        """,
        (_NOW, _NOW, sync_version),
    )
    wh._command(
        """
        INSERT INTO finance_transactions (transaction_id, account_id, posted_at, amount,
                                          currency, description, merchant, pending, source,
                                          created_at, sync_version)
        VALUES ('ft1', 'fa1', %s, -12.34, 'USD', 'Lunch', 'Cafe', 0, 'plaid', %s, %s)
        """,
        (_NOW - timedelta(hours=15), _NOW, sync_version),
    )
    wh._command(
        """
        INSERT INTO finance_observations (account_id, as_of, kind, value, currency,
                                          source, observed_at, sync_version)
        VALUES ('fa1', '2026-05-31', 'balance', 1234.56, 'USD', 'plaid', %s, %s)
        """,
        (_NOW, sync_version),
    )
    wh._command(
        """
        INSERT INTO manual_finance_documents (source, account, source_native_id, filename,
                                              original_path, mime_type, content_sha256,
                                              file_modified_at, ingested_at, sync_version)
        VALUES ('manual', 'z@x.test', 'docsha', 'statement.pdf', 'Bank/Checking',
                'application/pdf', 'docsha', %s, %s, %s)
        """,
        (_NOW - timedelta(hours=16), _NOW, sync_version),
    )
    wh._command(
        """
        INSERT INTO manual_finance_extractions (content_sha256, ai_provider, ai_model,
                                                ai_prompt_version, status, institution,
                                                period_end, summary, created_at, sync_version)
        VALUES ('docsha', 'agent_codex', 'm', 'v1', 'completed', 'Example Bank',
                '2026-06-01', 'Monthly checking statement', %s, %s)
        """,
        (_NOW, sync_version),
    )


# The seeded fixture rows exercise one classification branch per adapter:
# gmail addressed directly to the account (2), a member-channel slack message
# from someone else (3), a 1:1 iMessage in a chat Zach replies in (2, plus
# his own reply at 1), an unknown-roster whatsapp group (3), a session Zach
# prompted (1), his own notes/memos (1), a calendar event he organizes (1),
# an unstarred drive file (3), contact churn (5: sync machinery), and the
# warehouse's own machinery (5).
EXPECTED_SEEDED_PRIORITIES = {
    "gmail_email": 2,
    "slack_message": 3,
    "slack_file": 3,
    "apple_message": 2,
    "whatsapp_message": 3,
    "agent_session": 1,
    "apple_note_revision": 1,
    "voice_memo": 1,
    "alice_voice_recording": 1,
    "calendar_event": 1,
    "drive_file": 3,
    "photo": 1,
    "contact_update": 5,
    "finance_transaction": 1,
    "finance_observation": 1,
    "manual_finance_document": 1,
    "mutation": 5,
    "mutation_request": 5,
    "enrichment_run": 5,
}

EXPECTED_SEEDED_EVENTS = {
    "gmail_email": 1,
    "slack_message": 1,
    "slack_file": 1,
    "apple_message": 2,
    "whatsapp_message": 1,
    "agent_session": 1,
    "apple_note_revision": 1,
    "voice_memo": 1,
    "alice_voice_recording": 1,
    "calendar_event": 1,
    "drive_file": 1,
    "photo": 1,
    "contact_update": 1,
    "finance_transaction": 1,
    "finance_observation": 1,
    "manual_finance_document": 1,
    "mutation": 1,
    "mutation_request": 1,
    "enrichment_run": 1,
}


def test_backfill_normalizes_every_source(warehouse):
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    engine = _engine(warehouse)
    try:
        stats = engine.run()
    finally:
        engine.close()
    by_adapter = {s.adapter: s for s in stats}
    for adapter, expected in EXPECTED_SEEDED_EVENTS.items():
        assert by_adapter[adapter].backfill_rows == expected, adapter
        assert by_adapter[adapter].backfill_done, adapter

    rows = warehouse._query_dicts(
        "SELECT * FROM timeline_events ORDER BY event_ts DESC"
    )
    assert len(rows) == sum(EXPECTED_SEEDED_EVENTS.values())
    # Newest first: gmail (NOW-1h); finance observations include older days.
    assert rows[0]["adapter"] == "gmail_email"
    assert rows[-1]["adapter"] == "finance_observation"

    gmail = rows[0]
    assert gmail["source"] == "gmail"
    assert gmail["kind"] == "email"
    assert gmail["actor"] == "alice@example.test"
    assert gmail["title"] == "Hello world"
    assert gmail["source_table"] == "gmail_messages"
    assert gmail["source_pk"] == {"account": "z@x.test", "message_id": "m1"}
    assert gmail["metadata"]["thread_id"] == "th1"
    assert "Hello world" in gmail["search_text"]
    assert "hi there" in gmail["search_text"]

    slack = next(r for r in rows if r["adapter"] == "slack_message")
    assert slack["actor"] == "alice"
    assert slack["context"] == "#general"
    assert slack["snippet"] == "slack says hi"
    assert "slack says hi" in slack["search_text"]

    imsg = next(
        r for r in rows
        if r["adapter"] == "apple_message" and not r["metadata"]["from_me"]
    )
    assert imsg["actor"] == "+15551234567"
    assert imsg["context"] == "Family"

    session = next(r for r in rows if r["adapter"] == "agent_session")
    assert session["source"] == "claude_code"
    assert session["title"] == "Fix the bug"
    assert session["context"] == "/repo"
    assert session["metadata"]["events"] == 2
    assert session["end_ts"] > session["event_ts"]
    assert "fix the bug" in session["search_text"]
    assert "done" in session["search_text"]

    memo = next(r for r in rows if r["adapter"] == "voice_memo")
    assert memo["title"] == "Standup notes"
    assert memo["snippet"] == "we discussed things"

    alice = next(r for r in rows if r["adapter"] == "alice_voice_recording")
    assert alice["title"] == "Alice walk"
    assert alice["snippet"] == "321 seconds"

    transaction = next(r for r in rows if r["adapter"] == "finance_transaction")
    assert transaction["title"] == "Cafe"
    assert transaction["snippet"] == "-12.34 USD"
    assert transaction["context"] == "Example Bank · Checking"

    observation = next(r for r in rows if r["adapter"] == "finance_observation")
    assert observation["title"] == "Checking balance"
    assert observation["snippet"] == "1234.56 USD"

    document = next(r for r in rows if r["adapter"] == "manual_finance_document")
    assert document["title"] == "statement.pdf"
    assert document["snippet"] == "Monthly checking statement"

    cal = next(r for r in rows if r["adapter"] == "calendar_event")
    assert cal["end_ts"] > cal["event_ts"]

    photo = next(r for r in rows if r["adapter"] == "photo")
    assert photo["source"] == "photos"
    assert photo["kind"] == "photo"
    assert photo["actor"] == "me"
    assert photo["title"] == "IMG_0001.HEIC"
    assert photo["source_table"] == "photo_assets"
    assert photo["source_pk"] == {"photo_id": "ph1"}
    assert photo["metadata"]["thumbnail_file_id"] == "drive-th1"
    assert photo["metadata"]["camera_model"] == "iPhone 16 Pro"
    # The AI caption (keyed by the thumbnail sha) is the snippet and searchable.
    assert "golden retriever" in photo["snippet"]
    assert "golden retriever" in photo["search_text"]

    priorities = {row["adapter"]: row["priority"] for row in rows}
    assert priorities == EXPECTED_SEEDED_PRIORITIES

    # Second run is a no-op: nothing new, no seq churn.
    seqs_before = {row["event_id"]: row["seq"] for row in rows}
    engine2 = _engine(warehouse)
    try:
        stats2 = engine2.run()
    finally:
        engine2.close()
    assert all(s.backfill_rows == 0 and s.incremental_rows == 0 for s in stats2)
    rows_after = warehouse._query_dicts("SELECT event_id, seq FROM timeline_events")
    assert {r["event_id"]: r["seq"] for r in rows_after} == seqs_before


def test_priority_classifies_self_direct_mention_bulk_and_cron(warehouse):
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    # My own slack message -> self.
    warehouse._command(
        """
        INSERT INTO slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, text, synced_at)
        VALUES ('z', 'T1', 'C1', '3000.1', %s, 'UME', 'shipping it', %s)
        """,
        (_NOW, _NOW),
    )
    # A mention of me in a member channel -> direct.
    warehouse._command(
        """
        INSERT INTO slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, text, synced_at)
        VALUES ('z', 'T1', 'C1', '3000.2', %s, 'U1', 'hey <@UME> take a look', %s)
        """,
        (_NOW, _NOW),
    )
    # A DM from a real person -> direct.
    warehouse._command(
        """
        INSERT INTO slack_conversations (account, team_id, conversation_id, is_im)
        VALUES ('z', 'T1', 'D1', 1)
        """
    )
    warehouse._command(
        """
        INSERT INTO slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, text, synced_at)
        VALUES ('z', 'T1', 'D1', '3000.3', %s, 'U1', 'lunch?', %s)
        """,
        (_NOW, _NOW),
    )
    # A bot post in the member channel -> noise.
    warehouse._command(
        """
        INSERT INTO slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, bot_id, text, synced_at)
        VALUES ('z', 'T1', 'C1', '3000.4', %s, '', 'B1', 'deploy finished', %s)
        """,
        (_NOW, _NOW),
    )
    # A promo email addressed directly to me is still bulk -> noise.
    warehouse._command(
        """
        INSERT INTO gmail_messages (account, message_id, internal_date, subject, from_address,
                                    to_addresses, label_ids, synced_at)
        VALUES ('z@x.test', 'm-promo', %s, 'SALE', 'deals@shop.example',
                %s, %s, %s)
        """,
        (_NOW, ["z@x.test"], ["CATEGORY_PROMOTIONS", "INBOX"], _NOW),
    )
    # A reply by someone else in a thread I participated in -> direct.
    warehouse._command(
        """
        INSERT INTO slack_messages (account, team_id, conversation_id, message_ts, thread_ts,
                                    message_datetime, user_id, text, synced_at)
        VALUES ('z', 'T1', 'C1', '4000.1', '4000.1', %s, 'UME', 'starting a thread', %s),
               ('z', 'T1', 'C1', '4000.2', '4000.1', %s, 'U1', 'replying to zach', %s)
        """,
        (_NOW, _NOW, _NOW, _NOW),
    )
    # A drive file I own and last modified myself -> self; my file edited by
    # someone else -> direct.
    warehouse._command(
        """
        INSERT INTO google_drive_files (account, file_id, name, owners_json,
                                        last_modifying_user, modified_time, ingested_at)
        VALUES ('z@x.test', 'f-mine', 'journal.txt',
                '[{"displayName": "Zach Latta", "emailAddress": "z@x.test"}]'::jsonb,
                'Zach Latta', %s, %s),
               ('z@x.test', 'f-shared', 'proposal.doc',
                '[{"displayName": "Zach Latta", "emailAddress": "z@x.test"}]'::jsonb,
                'Someone Else', %s, %s)
        """,
        (_NOW, _NOW, _NOW, _NOW),
    )
    # Uncategorized automation: broadcast senders -> noise; transactional
    # senders -> cc tier; both even when addressed straight to me.
    warehouse._command(
        """
        INSERT INTO gmail_messages (account, message_id, internal_date, subject, from_address,
                                    to_addresses, synced_at)
        VALUES ('z@x.test', 'm-noreply', %s, 'Weekly digest', 'noreply@service.example', %s, %s),
               ('z@x.test', 'm-notify', %s, 'Receipt attached', 'receipts@service.example', %s, %s)
        """,
        (_NOW, ["z@x.test"], _NOW, _NOW, ["z@x.test"], _NOW),
    )
    warehouse._command(
        """
        INSERT INTO gmail_messages (account, message_id, internal_date, subject, from_address,
                                    to_addresses, label_ids, synced_at)
        VALUES ('z@x.test', 'm-starred', %s, 'Contract', 'lawyer@firm.example', %s, %s, %s)
        """,
        (_NOW, ["z@x.test"], ["STARRED", "INBOX"], _NOW),
    )
    # A business/RCS sender (not a phone number, not an email) -> noise.
    warehouse._command(
        """
        INSERT INTO apple_message_handles (account, handle_id, address)
        VALUES ('z@x.test', 'h-biz', 'some_airline_dsqx1')
        """
    )
    warehouse._command(
        """
        INSERT INTO apple_messages (account, message_id, handle_id, body_text, message_at,
                                    is_from_me, ingested_at)
        VALUES ('z@x.test', 'am-biz', 'h-biz', 'Your flight changed', %s, 0, %s)
        """,
        (_NOW, _NOW),
    )
    # The warehouse's own excluded Drive storage blobs -> background.
    warehouse._command(
        """
        INSERT INTO google_drive_files (account, file_id, name, is_excluded, modified_time, ingested_at)
        VALUES ('z@x.test', 'f-excluded', 'blob-shard.bin', 1, %s, %s)
        """,
        (_NOW, _NOW),
    )
    # An openclaw cron heartbeat session -> background.
    for seq, (role, text) in enumerate([("user", "[cron:abc123 Monitor things] Run checks"), ("assistant", "ok")]):
        warehouse._command(
            """
            INSERT INTO agent_session_events (source, session_id, event_uuid, seq, occurred_at,
                                              role, text, ingested_at)
            VALUES ('openclaw', 'cron-sess', %s, %s, %s, %s, %s, %s)
            """,
            (f"c{seq}", seq, _NOW, role, text, _NOW),
        )

    engine = _engine(warehouse)
    try:
        engine.run()
    finally:
        engine.close()

    def priority_of(event_id: str) -> int:
        return warehouse._query(
            "SELECT priority FROM timeline_events WHERE event_id = %s", (event_id,)
        )[0][0]

    assert priority_of("z|T1|C1|3000.1") == 1, "my own message is self-priority"
    assert priority_of("z|T1|C1|3000.2") == 2, "a mention of me is direct"
    assert priority_of("z|T1|D1|3000.3") == 2, "a DM is direct"
    assert priority_of("z|T1|C1|3000.4") == 4, "bot posts are noise"
    assert priority_of("z|T1|C1|4000.2") == 2, "a reply in my thread is direct"
    assert priority_of("z@x.test|f-mine") == 1, "my own drive edits are self"
    assert priority_of("z@x.test|f-shared") == 2, "someone editing my file is direct"
    assert priority_of("z@x.test|am-biz") == 4, "business/RCS senders are noise"
    assert priority_of("z@x.test|f-excluded") == 5, "warehouse-excluded drive blobs are background"
    assert priority_of("z@x.test|m-promo") == 4, "promos are noise even when addressed to me"
    assert priority_of("z@x.test|m-noreply") == 4, "broadcast senders are noise"
    assert priority_of("z@x.test|m-notify") == 4, "pure machine mail is noise"
    assert priority_of("z@x.test|m-starred") == 2, "starred email is direct"
    assert priority_of("openclaw|cron-sess") == 5, "cron heartbeat sessions are background"


def test_priority_separates_conversations_automation_and_machinery(warehouse):
    """The benchmark-tuned heuristics (sampling/, 2026-07): active-window and
    thread-root promotion in chats, mail-merge and correspondent rules in
    gmail, interactive-vs-programmatic agent sessions, calendar and drive
    pipeline demotions."""
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)

    # --- slack: engagement windows, name mentions, group DMs ----------------
    warehouse._command(
        """
        INSERT INTO slack_conversations (account, team_id, conversation_id, name, is_member,
                                         num_members, is_mpim, is_private)
        VALUES ('z', 'T1', 'CBIG', 'lounge', 1, 40000, 0, 0),
               ('z', 'T1', 'G1', 'mpdm-group', 1, 7, 1, 0),
               ('z', 'T1', 'G2', 'mpdm-small', 1, 4, 1, 0)
        """
    )
    # Zach posts twice in #general around _NOW -> surrounding messages are a
    # conversation he is in; a single drive-by post in #lounge is not.
    warehouse._command(
        """
        INSERT INTO slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, text, synced_at)
        VALUES ('z', 'T1', 'C1', '5000.1', %s, 'UME', 'working on it', %s),
               ('z', 'T1', 'C1', '5000.2', %s, 'UME', 'done', %s),
               ('z', 'T1', 'C1', '5000.3', %s, 'U1', 'nice work everyone', %s),
               ('z', 'T1', 'CBIG', '5000.4', %s, 'UME', 'hello lounge', %s),
               ('z', 'T1', 'CBIG', '5000.5', %s, 'U1', 'ambient chatter', %s),
               ('z', 'T1', 'CBIG', '5000.6', %s, 'U1', 'they should ask zach latta', %s),
               ('z', 'T1', 'G1', '5000.7', %s, 'U1', 'big group dm chatter', %s),
               ('z', 'T1', 'G2', '5000.8', %s, 'U1', 'small group dm', %s)
        """,
        (
            _NOW - timedelta(hours=1), _NOW,
            _NOW + timedelta(hours=1), _NOW,
            _NOW, _NOW,
            _NOW, _NOW,
            _NOW + timedelta(hours=2), _NOW,
            _NOW + timedelta(hours=3), _NOW,
            _NOW, _NOW,
            _NOW, _NOW,
        ),
    )
    # A legacy integration posting with a username and no user account.
    warehouse._command(
        """
        INSERT INTO slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, username, text, synced_at)
        VALUES ('z', 'T1', 'C1', '5000.9', %s, '', 'streambot', 'streaming activity', %s)
        """,
        (_NOW, _NOW),
    )

    # --- gmail: merge blasts, replies, relays, OTP, RSVP --------------------
    merge_rows = []
    for i in range(31):
        merge_rows.append(
            (
                "z@x.test", f"merge-{i}", f"mth-{i}",
                _NOW - timedelta(minutes=i), "join the program?",
                "Zach <z@x.test>", [f"school{i}@example.test"], _NOW,
            )
        )
    for row in merge_rows:
        warehouse._command(
            """
            INSERT INTO gmail_messages (account, message_id, thread_id, internal_date,
                                        subject, from_address, to_addresses, synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            row,
        )
    # A personal reply in a thread someone else wrote in first stays his.
    warehouse._command(
        """
        INSERT INTO gmail_messages (account, message_id, thread_id, internal_date,
                                    subject, from_address, to_addresses, synced_at)
        VALUES ('z@x.test', 'inbound-1', 'mth-5', %s, 'Re: join the program?',
                'school5@example.test', %s, %s),
               ('z@x.test', 'my-reply', 'mth-5', %s, 'join the program?',
                'Zach <z@x.test>', %s, %s)
        """,
        (
            _NOW - timedelta(minutes=30), ["z@x.test"], _NOW,
            _NOW - timedelta(minutes=2), ["school5@example.test"], _NOW,
        ),
    )
    # Human mail he answered within 48h -> attention, even unaddressed.
    warehouse._command(
        """
        INSERT INTO gmail_messages (account, message_id, thread_id, internal_date,
                                    subject, from_address, to_addresses, synced_at)
        VALUES ('z@x.test', 'm-replied', 'th-conv', %s, 'quick question',
                'friend@example.test', %s, %s),
               ('z@x.test', 'm-my-answer', 'th-conv', %s, 'Re: quick question',
                'Zach <z@x.test>', %s, %s)
        """,
        (
            _NOW - timedelta(hours=3), ["z@x.test"], _NOW,
            _NOW - timedelta(hours=2), ["friend@example.test"], _NOW,
        ),
    )
    # Relay notifications: a mention copy is direct; a bot payload is noise;
    # a plain relayed comment is cc.
    warehouse._command(
        """
        INSERT INTO gmail_messages (account, message_id, internal_date, subject, from_address,
                                    to_addresses, cc_addresses, snippet, synced_at)
        VALUES ('z@x.test', 'gh-mention', %s, 'Re: [org/repo] fix (PR #1)',
                'notifications@github.com', %s, %s, 'someone: @zach take a look', %s),
               ('z@x.test', 'gh-bot', %s, 'Re: [org/repo] bump deps (PR #2)',
                'notifications@github.com', %s, %s,
                'vercel[bot] left a comment (org/repo#2)', %s),
               ('z@x.test', 'gh-plain', %s, 'Re: [org/repo] discussion (Issue #3)',
                'notifications@github.com', %s, %s, 'a human wrote words here', %s)
        """,
        (
            _NOW, ["z@x.test"], ["mention@noreply.github.com"], _NOW,
            _NOW, ["z@x.test"], ["push@noreply.github.com"], _NOW,
            _NOW, ["z@x.test"], ["subscribed@noreply.github.com"], _NOW,
        ),
    )
    warehouse._command(
        """
        INSERT INTO gmail_messages (account, message_id, internal_date, subject, from_address,
                                    to_addresses, synced_at)
        VALUES ('z@x.test', 'm-otp', %s, 'Your login code: 123-456', 'human.sounding@bank.example', %s, %s),
               ('z@x.test', 'm-confirm-code', %s, '123456 is your confirmation code', 'human.sounding@example.test', %s, %s),
               ('z@x.test', 'm-rsvp', %s, 'Accepted: 1:1 @ Fri (owner)', 'colleague@example.test', %s, %s),
               ('z@x.test', 'm-shipment-confirmation', %s, 'Shipment Confirmation', 'dinobox@example.test', %s, %s)
        """,
        (
            _NOW, ["z@x.test"], _NOW,
            _NOW, ["z@x.test"], _NOW,
            _NOW, ["z@x.test"], _NOW,
            _NOW, ["z+recipient@x.test"], _NOW,
        ),
    )
    # A known correspondent whose mail Gmail mis-categorized as bulk.
    warehouse._command(
        """
        INSERT INTO gmail_messages (account, message_id, internal_date, subject, from_address,
                                    to_addresses, label_ids, synced_at)
        VALUES ('z@x.test', 'm-corr', %s, 'travel receipts', 'friend@example.test', %s,
                %s, %s)
        """,
        (_NOW, ["z@x.test"], ["CATEGORY_UPDATES", "INBOX"], _NOW),
    )
    warehouse._command(
        """
        INSERT INTO timeline_gmail_correspondents (addr, n_sent_to, last_sent_at, refreshed_at)
        VALUES ('friend@example.test', 12, %s, now())
        """,
        (_NOW,),
    )
    # Gmail's Forums bucket mixes real list discussion with broadcast digests
    # and newsletters. Individual human posts to work aliases remain cc, but
    # digest/newsletter/list-announcement shapes are noise.
    warehouse._command(
        """
        INSERT INTO gmail_messages (account, message_id, internal_date, subject, from_address,
                                    to_addresses, label_ids, snippet, synced_at)
        VALUES ('z@x.test', 'm-forum-human', %s, 'WFH today', 'teammate@example.test', %s,
                %s, 'Hi, I will be working from home today. -- You received this message because you are subscribed.', %s),
               ('z@x.test', 'm-forum-digest', %s, 'Digest for ops@example.test - 8 updates in 6 topics',
                'ops@example.test', %s, %s,
                'ops@example.test Google Groups Logo Google Groups Topic digest View all topics', %s),
               ('z@x.test', 'm-forum-newsletter', %s, 'Recommendations from your newsletters',
                'news@example.test', %s, %s,
                'View in browser ͏ ͏ ͏ weekly reading recommendations', %s),
               ('z@x.test', 'm-forum-announcement', %s, '[publiclist] Funding opportunities',
                'person@example.test', %s, %s,
                'Here are several announcements for subscribers.', %s),
               ('z@x.test', 'm-forum-new-comment', %s, 'New comment on Budget',
                'notify@example.test', %s, %s,
                'A person left a new comment on the document.', %s),
               ('z@x.test', 'm-figma-upgrade', %s, 'Upgrade request from teammate',
                'no-reply@email.figma.com', %s, %s,
                'teammate is requesting a Full seat.', %s),
               ('z@x.test', 'm-airtable-access', %s, 'teammate (teammate@example.test) requested access to Ops - Airtable',
                'noreply@airtable.com', %s, %s,
                'teammate would like to access the Ops base. Grant Access', %s),
               ('z@x.test', 'm-sign-request', %s, 'Example Agreement: Signature Request from Example Org',
                'mail@signnow.com', %s, %s,
                'You were invited to review and sign a document Example Org invited you to sign', %s),
               ('z@x.test', 'm-drive-share', %s, 'Document shared with you: "Plan"',
                'drive-shares-dm-noreply@google.com', %s, %s,
                'teammate shared a document teammate has invited you to edit the following document', %s),
               ('z@x.test', 'm-vercel-access', %s, '[Access Request] teammate requested access to app.example.dev',
                'notifications@vercel.com', %s, %s,
                'Access request Hello, teammate@example.test wants access to a URL', %s)
        """,
        (
            _NOW, ["timeoff@example.test"], ["CATEGORY_FORUMS", "INBOX"], _NOW,
            _NOW, ["ops@example.test"], ["CATEGORY_FORUMS", "INBOX"], _NOW,
            _NOW, ["news@example.test"], ["CATEGORY_FORUMS", "INBOX"], _NOW,
            _NOW, ["publiclist@googlegroups.com"], ["CATEGORY_FORUMS", "INBOX"], _NOW,
            _NOW, ["z@x.test"], ["CATEGORY_FORUMS", "INBOX"], _NOW,
            _NOW, ["z@x.test"], ["CATEGORY_UPDATES", "INBOX"], _NOW,
            _NOW, ["z@x.test"], ["CATEGORY_UPDATES", "INBOX"], _NOW,
            _NOW, ["z@x.test"], ["CATEGORY_UPDATES", "INBOX"], _NOW,
            _NOW, ["z@x.test"], ["CATEGORY_UPDATES", "INBOX"], _NOW,
            _NOW, ["z@x.test"], ["CATEGORY_UPDATES", "INBOX"], _NOW,
        ),
    )

    # --- apple: one-way broadcasts, toll-free, shortcode groups, windows ----
    warehouse._command(
        """
        INSERT INTO apple_message_handles (account, handle_id, address)
        VALUES ('z@x.test', 'h-oneway', '+15559990000'),
               ('z@x.test', 'h-tollfree', '+18335551234'),
               ('z@x.test', 'h-group', '+15558887777')
        """
    )
    warehouse._command(
        """
        INSERT INTO apple_message_chats (account, chat_id, display_name, style)
        VALUES ('z@x.test', 'c-oneway', '', 45),
               ('z@x.test', 'c-shortcode', '56789', 43),
               ('z@x.test', 'c-biggroup', 'Trip Crew', 43)
        """
    )
    for chat_id, handle, mid, offset in (
        ("c-oneway", "h-oneway", "am-oneway", 0),
        ("c-shortcode", "h-group", "am-shortcode", 0),
        ("c-biggroup", "h-group", "am-group-active", 0),
        ("c-biggroup", "h-group", "am-group-idle", 90),
    ):
        warehouse._command(
            """
            INSERT INTO apple_message_chat_messages (account, chat_id, message_id, message_date, ingested_at)
            VALUES ('z@x.test', %s, %s, %s, %s)
            """,
            (chat_id, mid, _NOW - timedelta(days=offset), _NOW),
        )
        warehouse._command(
            """
            INSERT INTO apple_messages (account, message_id, handle_id, body_text, message_at,
                                        is_from_me, ingested_at)
            VALUES ('z@x.test', %s, %s, 'hello', %s, 0, %s)
            """,
            (mid, handle, _NOW - timedelta(days=offset), _NOW),
        )
    # Eleven other participants make c-biggroup a big group (the attention
    # threshold is nine distinct addresses); Zach posted in it near _NOW
    # (active window) but not near the idle message.
    for i in range(11):
        warehouse._command(
            """
            INSERT INTO apple_message_chat_handles (account, chat_id, handle_id)
            VALUES ('z@x.test', 'c-biggroup', %s)
            """,
            (f"h-g{i}",),
        )
    warehouse._command(
        """
        INSERT INTO apple_message_chat_messages (account, chat_id, message_id, message_date, ingested_at)
        VALUES ('z@x.test', 'c-biggroup', 'am-group-mine', %s, %s)
        """,
        (_NOW - timedelta(hours=2), _NOW),
    )
    warehouse._command(
        """
        INSERT INTO apple_messages (account, message_id, handle_id, body_text, message_at,
                                    is_from_me, ingested_at)
        VALUES ('z@x.test', 'am-group-mine', '', 'on my way', %s, 1, %s)
        """,
        (_NOW - timedelta(hours=2), _NOW),
    )

    # --- whatsapp: business senders and E2E stubs ----------------------------
    warehouse._command(
        """
        INSERT INTO whatsapp_contacts (account, jid, push_name, business_name)
        VALUES ('z@x.test', 'agent@lid', 'Agent', 'Agent Service')
        """
    )
    warehouse._command(
        """
        INSERT INTO whatsapp_messages (account, chat_id, message_id, sender_jid, push_name,
                                       body_text, message_at, is_from_me, ingested_at)
        VALUES ('z@x.test', 'agent@lid', 'wm-agent', 'agent@lid', 'Agent',
                'task finished', %s, 0, %s),
               ('z@x.test', 'chat@g.us', 'wm-stub', 'chat@g.us', '', '', %s, 0, %s)
        """,
        (_NOW, _NOW, _NOW, _NOW),
    )

    # --- agent sessions: programmatic entrypoints and empty transcripts -----
    for sess, entrypoint, rows in (
        ("sdk-sess", "sdk-cli", [("user", "Reply with ONLY minified JSON")]),
        ("empty-sess", "", []),
        ("desktop-conv", "", []),
    ):
        source = "claude_desktop" if sess == "desktop-conv" else "claude_code"
        if not rows:
            warehouse._command(
                """
                INSERT INTO agent_session_events (source, session_id, event_uuid, seq, occurred_at,
                                                  role, event_type, session_title, entrypoint, ingested_at)
                VALUES (%s, %s, 'meta0', 0, %s, 'meta', 'conversation', 'A titled conversation', %s, %s)
                """,
                (source, sess, _NOW, entrypoint, _NOW),
            )
        for seq, (role, text) in enumerate(rows):
            warehouse._command(
                """
                INSERT INTO agent_session_events (source, session_id, event_uuid, seq, occurred_at,
                                                  role, text, entrypoint, ingested_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (source, sess, f"e{seq}", seq, _NOW, role, text, entrypoint, _NOW),
            )
    # A sidechain-only subagent transcript is machinery even with user rows.
    for seq, (role, text) in enumerate(
        [("user", "You are a code-review finder"), ("assistant", "findings: [] ")]
    ):
        warehouse._command(
            """
            INSERT INTO agent_session_events (source, session_id, event_uuid, seq, occurred_at,
                                              role, text, is_sidechain, ingested_at)
            VALUES ('claude_code', 'side-sess', %s, %s, %s, %s, %s, 1, %s)
            """,
            (f"s{seq}", seq, _NOW, role, text, _NOW),
        )
    # The same long opening prompt recurring across sessions = a scheduled
    # routine, not a human typing it four days in a row.
    monitor_prompt = "Monitor and debug the example service in production, checking dashboards"
    for day in range(4):
        warehouse._command(
            """
            INSERT INTO agent_session_events (source, session_id, event_uuid, seq, occurred_at,
                                              role, text, entrypoint, ingested_at)
            VALUES ('claude_code', %s, 'r0', 0, %s, 'user', %s, 'cli', %s)
            """,
            (f"routine-{day}", _NOW - timedelta(days=day), monitor_prompt, _NOW),
        )

    # --- calendar: feeds, promo invites, flighty ------------------------------
    warehouse._command(
        """
        INSERT INTO calendar_events (account, calendar_id, event_id, summary, description,
                                     organizer_email, start_at, updated_at, synced_at)
        VALUES ('z@x.test', 'cal1', 'ev-feed', 'Vinyasa Flow', '',
                'studio_x1@group.calendar.google.com', %s, %s, %s),
               ('z@x.test', 'cal1', 'ev-promo', 'Free Ticket!', 'come along ͏ ­͏ ­',
                'random@gmail.example', %s, %s, %s),
               ('z@x.test', 'cal1', 'ev-flight', '✈ BTV→IAD • UA 4178', '',
                'z@x.test', %s, %s, %s),
               ('z@x.test', 'cal1', 'ev-invite', 'Coffee', '',
                'human@example.test', %s, %s, %s)
        """,
        (_NOW, _NOW, _NOW) * 4,
    )

    # --- drive: form pipelines and shortcuts ---------------------------------
    warehouse._command(
        """
        INSERT INTO google_drive_files (account, file_id, name, mime_type, folder_path,
                                        last_modifying_user, modified_time, ingested_at)
        VALUES ('z@x.test', 'f-form', 'logo - applicant.png', 'image/png',
                '/apps form/Application (File responses)/Upload Logo (File responses)',
                'applicant', %s, %s),
               ('z@x.test', 'f-shortcut', 'Old Report', 'application/vnd.google-apps.shortcut',
                '/My Drive', 'someone', %s, %s)
        """,
        (_NOW, _NOW, _NOW, _NOW),
    )

    engine = _engine(warehouse)
    try:
        engine.run()
    finally:
        engine.close()

    def priority_of(event_id: str) -> int:
        return warehouse._query(
            "SELECT priority FROM timeline_events WHERE event_id = %s", (event_id,)
        )[0][0]

    # slack
    assert priority_of("z|T1|C1|5000.3") == 2, "channel msg inside his two-post window is a conversation"
    assert priority_of("z|T1|CBIG|5000.5") == 3, "one drive-by post does not promote a 40k channel"
    assert priority_of("z|T1|CBIG|5000.6") == 2, "naming him promotes anywhere"
    assert priority_of("z|T1|G1|5000.7") == 3, "a big group DM he is not engaged in is peripheral"
    assert priority_of("z|T1|G2|5000.8") == 2, "small group DMs are attention"
    assert priority_of("z|T1|C1|5000.9") == 4, "username-only legacy integrations are bots"
    # gmail
    assert priority_of("z@x.test|merge-20") == 4, "mail-merge blast sends are not his actions"
    assert priority_of("z@x.test|my-reply") == 1, "a personal reply after inbound mail stays his"
    assert priority_of("z@x.test|m-replied") == 2, "mail he answered within 48h has his attention"
    assert priority_of("z@x.test|gh-mention") == 2, "github mention copies are direct"
    assert priority_of("z@x.test|gh-bot") == 4, "relayed bot payloads are noise"
    assert priority_of("z@x.test|gh-plain") == 3, "relayed human comments are cc"
    assert priority_of("z@x.test|m-otp") == 4, "login codes are noise"
    assert priority_of("z@x.test|m-confirm-code") == 4, "confirmation codes are noise"
    assert priority_of("z@x.test|m-rsvp") == 3, "auto-RSVP notices are cc"
    assert priority_of("z@x.test|m-shipment-confirmation") == 4, "shipment automations are noise"
    assert priority_of("z@x.test|m-corr") == 2, "known correspondents beat gmail's bulk category"
    assert priority_of("z@x.test|m-forum-human") == 3, "human work-list posts are cc"
    assert priority_of("z@x.test|m-forum-digest") == 4, "forum digests are noise"
    assert priority_of("z@x.test|m-forum-newsletter") == 4, "forum newsletters are noise"
    assert priority_of("z@x.test|m-forum-announcement") == 4, "public-list announcements are noise"
    assert priority_of("z@x.test|m-forum-new-comment") == 3, "automated human comment relays remain cc"
    assert priority_of("z@x.test|m-figma-upgrade") == 3, "seat requests are human-action relays"
    assert priority_of("z@x.test|m-airtable-access") == 3, "access requests are human-action relays"
    assert priority_of("z@x.test|m-sign-request") == 3, "signature requests are human-action relays"
    assert priority_of("z@x.test|m-drive-share") == 3, "drive shares are human-action relays"
    assert priority_of("z@x.test|m-vercel-access") == 3, "app access requests are human-action relays"
    # apple
    assert priority_of("z@x.test|am-oneway") == 4, "a 1:1 chat he never answers is a broadcast"
    assert priority_of("z@x.test|am-shortcode") == 4, "shortcode-named group blasts are noise"
    assert priority_of("z@x.test|am-group-active") == 2, "big group during his active window"
    assert priority_of("z@x.test|am-group-idle") == 3, "big group outside his window is peripheral"
    # whatsapp
    assert priority_of("z@x.test|agent@lid|wm-agent") == 4, "business/bot accounts are automated"
    assert priority_of("z@x.test|chat@g.us|wm-stub") == 4, "contentless E2E stubs are noise"
    # agent sessions
    assert priority_of("claude_code|sdk-sess") == 5, "sdk-cli runs are machinery"
    assert priority_of("claude_code|empty-sess") == 5, "zero-user-turn transcripts are machinery"
    assert priority_of("claude_desktop|desktop-conv") == 1, "desktop conversations are his even header-only"
    assert priority_of("claude_code|side-sess") == 5, "sidechain-only subagent transcripts are machinery"
    assert priority_of("claude_code|routine-0") == 5, "recurring template prompts are scheduled routines"
    # calendar
    assert priority_of("z@x.test|cal1|ev-feed") == 4, "subscribed calendar feeds are noise"
    assert priority_of("z@x.test|cal1|ev-promo") == 4, "promo-invite blasts are noise"
    assert priority_of("z@x.test|cal1|ev-flight") == 4, "flighty auto-events are not his actions"
    assert priority_of("z@x.test|cal1|ev-invite") == 2, "human invites are attention"
    # drive
    assert priority_of("z@x.test|f-form") == 3, "form-response uploads are pipeline traffic"
    assert priority_of("z@x.test|f-shortcut") == 3, "shortcut churn is ambient"


def test_quality_regressions_for_recent_self_timeline_samples(warehouse):
    _ensure_all_source_tables(warehouse)
    now = datetime.now(tz=UTC)

    # Slack DMs should identify the other participant; attachment-only
    # message shells and inaccessible file stubs should not surface as self
    # activity, and stale file stubs should use the Slack message timestamp
    # rather than the sync timestamp.
    warehouse._command(
        """
        INSERT INTO slack_users (account, team_id, user_id, display_name)
        VALUES ('z', 'T1', 'UME', 'self'),
               ('z', 'T1', 'U1', 'Teammate One'),
               ('z', 'T1', 'U2', 'Teammate Two')
        """
    )
    warehouse._command(
        """
        INSERT INTO slack_account_identities (account, team_id, user_id)
        VALUES ('z', 'T1', 'UME')
        """
    )
    warehouse._command(
        """
        INSERT INTO slack_conversations (account, team_id, conversation_id, name, is_im)
        VALUES ('z', 'T1', 'D1', '', 1),
               ('z', 'T1', 'D2', 'U2', 1)
        """
    )
    warehouse._command(
        """
        INSERT INTO slack_conversation_members (account, team_id, conversation_id, user_id)
        VALUES ('z', 'T1', 'D1', 'UME'),
               ('z', 'T1', 'D1', 'U1')
        """
    )
    stale_message_ts = f"{(now - timedelta(hours=1)).timestamp():.6f}"
    warehouse._command(
        """
        INSERT INTO slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, text, raw_json, synced_at)
        VALUES ('z', 'T1', 'D1', 'dm-1', %s, 'UME', 'hello there', '{}', %s),
               ('z', 'T1', 'D1', 'dm-file-shell', %s, 'UME', '',
                '{"files":[{"id":"FREAL"}]}', %s),
               ('z', 'T1', 'D2', 'dm-name-fallback', %s, 'UME', 'named fallback', '{}', %s)
        """,
        (now, now, now, now, now, now),
    )
    warehouse._command(
        """
        INSERT INTO slack_files (account, team_id, file_id, conversation_id, message_ts,
                                 user_id, created_at, name, title, filetype, size, raw_json, synced_at)
        VALUES ('z', 'T1', 'FSTUB', 'D1', %s, 'UME', '1970-01-01', '', '',
                'jpg', 0, '{"file_access":"file_not_found"}', %s)
        """,
        (stale_message_ts, now),
    )

    # WhatsApp 1:1 chats should use contact names for context, and voice rows
    # with stored media should have a readable placeholder instead of a blank
    # snippet.
    warehouse._command(
        """
        INSERT INTO whatsapp_contacts (account, jid, push_name)
        VALUES ('z@x.test', 'friend@lid', 'Saved Contact')
        """
    )
    warehouse._command(
        """
        INSERT INTO whatsapp_messages (account, chat_id, message_id, sender_jid, body_text,
                                       message_kind, media_type, message_at, is_from_me, ingested_at)
        VALUES ('z@x.test', 'friend@lid', 'voice-1', '', '', 'voice', 'voice', %s, 1, %s)
        """,
        (now, now),
    )

    # Gmail should decode common HTML entities in timeline display fields.
    warehouse._command(
        """
        INSERT INTO gmail_messages (account, message_id, internal_date, subject, from_address,
                                    snippet, synced_at)
        VALUES ('z@x.test', 'html-1', %s, 'Re: &lt;Plan&gt;', 'Zach <z@x.test>',
                'I&#39;m ready &amp; excited &lt;3', %s)
        """,
        (now, now),
    )

    # iMessage attachment-only rows should show attachment labels instead of
    # the object-replacement placeholder character.
    warehouse._command(
        """
        INSERT INTO apple_message_chat_messages (account, chat_id, message_id, message_date, ingested_at)
        VALUES ('z@x.test', 'chat-attach', 'im-attach', %s, %s)
        """,
        (now, now),
    )
    warehouse._command(
        """
        INSERT INTO apple_messages (account, message_id, body_text, message_at,
                                    is_from_me, cache_has_attachments, ingested_at)
        VALUES ('z@x.test', 'im-attach', '￼', %s, 1, 1, %s)
        """,
        (now, now),
    )
    warehouse._command(
        """
        INSERT INTO apple_message_attachments (account, attachment_id, message_id, filename,
                                               mime_type, is_missing, ingested_at)
        VALUES ('z@x.test', 'att-1', 'im-attach', '~/Library/Messages/Attachments/x/photo.jpg',
                'image/jpeg', 0, %s)
        """,
        (now,),
    )

    # Cancelled/deleted calendar events and future own-calendar entries should
    # not classify as self activity in a recent-self review.
    warehouse._command(
        """
        INSERT INTO calendar_events (account, calendar_id, event_id, summary, organizer_email,
                                     start_at, status, is_deleted, updated_at, synced_at)
        VALUES ('z@x.test', 'primary', 'cancelled', 'Cancelled haircut', 'z@x.test',
                %s, 'cancelled', 1, %s, %s),
               ('z@x.test', 'primary', 'future', 'Future office', 'z@x.test',
                %s, 'confirmed', 0, %s, %s)
        """,
        (now - timedelta(hours=1), now, now, now + timedelta(days=1), now, now),
    )

    # OpenClaw subagent/cron monitor sessions are background machinery even
    # when they contain a user row.
    warehouse._command(
        """
        INSERT INTO agent_session_events (source, session_id, event_uuid, seq, occurred_at,
                                          role, text, device, ingested_at)
        VALUES ('openclaw', 'subagent-cron', 'u0', 0, %s, 'user',
                '[Subagent Context] You are running as a subagent.\n\n[Subagent Task]\nCron monitor subtask.',
                'openclaw', %s)
        """,
        (now, now),
    )

    adapters = [
        adapter_by_name(name)
        for name in (
            "slack_message",
            "slack_file",
            "whatsapp_message",
            "gmail_email",
            "apple_message",
            "calendar_event",
            "agent_session",
        )
    ]
    engine = _engine(warehouse, adapters=adapters)
    try:
        engine.run()
    finally:
        engine.close()

    by_event_id = {
        row["event_id"]: row
        for row in warehouse._query_dicts("SELECT * FROM timeline_events")
    }

    assert by_event_id["z|T1|D1|dm-1"]["context"] == "DM with Teammate One"
    assert by_event_id["z|T1|D2|dm-name-fallback"]["context"] == "DM with Teammate Two"
    assert by_event_id["z|T1|D1|dm-file-shell"]["priority"] == 5
    stale_file = by_event_id[f"z|T1|FSTUB|D1|{stale_message_ts}"]
    assert stale_file["priority"] == 5
    assert stale_file["event_ts"] < now - timedelta(minutes=30)

    whatsapp = by_event_id["z@x.test|friend@lid|voice-1"]
    assert whatsapp["context"] == "Saved Contact"
    assert whatsapp["snippet"] == "[voice message]"

    gmail = by_event_id["z@x.test|html-1"]
    assert gmail["title"] == "Re: <Plan>"
    assert gmail["snippet"] == "I'm ready & excited <3"

    imessage = by_event_id["z@x.test|im-attach"]
    assert imessage["snippet"] == "[attachment: photo.jpg]"

    assert by_event_id["z@x.test|primary|cancelled"]["priority"] != 1
    assert by_event_id["z@x.test|primary|future"]["priority"] != 1
    assert by_event_id["openclaw|subagent-cron"]["priority"] == 5


def test_voice_memo_timeline_refreshes_when_enrichment_arrives_later(warehouse):
    _ensure_all_source_tables(warehouse)
    adapter = adapter_by_name("voice_memo")
    warehouse._command(
        """
        INSERT INTO apple_voice_memos_files (account, recording_id, title, filename,
                                             recorded_at, ingested_at)
        VALUES ('z@x.test', 'rec-late', '20260709 raw title', 'raw.m4a', %s, %s)
        """,
        (_NOW, _NOW),
    )

    engine = _engine(warehouse, adapters=[adapter])
    try:
        engine.run()
    finally:
        engine.close()
    before = warehouse._query_dicts(
        "SELECT title, snippet FROM timeline_events WHERE event_id = 'z@x.test|rec-late'"
    )[0]
    assert before["title"] == "20260709 raw title"
    assert before["snippet"] == ""

    warehouse._command(
        """
        INSERT INTO apple_voice_memos_enrichments (account, recording_id, provider, model,
                                                   prompt_version, status, title, summary, created_at)
        VALUES ('z@x.test', 'rec-late', 'p', 'm', 'v1', 'completed',
                'Readable memo title', 'Readable memo summary', %s)
        """,
        (_NOW + timedelta(minutes=5),),
    )

    engine = _engine(warehouse, adapters=[adapter])
    try:
        stats = engine.run()
    finally:
        engine.close()
    assert stats[0].incremental_rows == 1
    after = warehouse._query_dicts(
        "SELECT title, snippet FROM timeline_events WHERE event_id = 'z@x.test|rec-late'"
    )[0]
    assert after["title"] == "Readable memo title"
    assert after["snippet"] == "Readable memo summary"


def test_refresh_window_converges_late_signals(warehouse):
    """A chat message classified before Zach replied is upgraded once the
    refresh window re-walks it (his reply promotes the surrounding window)."""
    _ensure_all_source_tables(warehouse)
    now = datetime.now(tz=UTC)
    warehouse._command(
        "INSERT INTO slack_account_identities (account, team_id, user_id) VALUES ('z', 'T1', 'UME')"
    )
    warehouse._command(
        """
        INSERT INTO slack_conversations (account, team_id, conversation_id, name, is_member, num_members)
        VALUES ('z', 'T1', 'C9', 'work', 1, 30)
        """
    )
    warehouse._command(
        """
        INSERT INTO slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, text, synced_at)
        VALUES ('z', 'T1', 'C9', '9000.1', %s, 'U1', 'question for the room', %s)
        """,
        (now - timedelta(hours=2), now - timedelta(hours=2)),
    )
    adapter = adapter_by_name("slack_message")
    engine = _engine(warehouse, adapters=[adapter])
    try:
        engine.run()
    finally:
        engine.close()
    row = warehouse._query(
        "SELECT priority FROM timeline_events WHERE event_id = 'z|T1|C9|9000.1'"
    )
    assert row[0][0] == 3, "no engagement yet: ambient member channel"

    # Zach replies twice; the original message predates the watermark so only
    # the refresh re-walk can reclassify it.
    warehouse._command(
        """
        INSERT INTO slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, text, synced_at)
        VALUES ('z', 'T1', 'C9', '9000.2', %s, 'UME', 'on it', %s),
               ('z', 'T1', 'C9', '9000.3', %s, 'UME', 'fixed', %s)
        """,
        (now - timedelta(hours=1), now, now - timedelta(minutes=30), now),
    )
    engine = _engine(warehouse, adapters=[adapter])
    try:
        stats = engine.run()
    finally:
        engine.close()
    assert stats[0].refreshed_rows > 0
    row = warehouse._query(
        "SELECT priority FROM timeline_events WHERE event_id = 'z|T1|C9|9000.1'"
    )
    assert row[0][0] == 2, "his replies retroactively promote the conversation window"


def test_incremental_picks_up_new_and_changed_rows(warehouse):
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    engine = _engine(warehouse)
    try:
        engine.run()

        later = _NOW + timedelta(minutes=10)
        # A brand-new message and an edit to an existing one, both with a
        # fresher ingestion timestamp than the stored watermark.
        warehouse._command(
            """
            INSERT INTO slack_messages (account, team_id, conversation_id, message_ts,
                                        message_datetime, user_id, text, synced_at)
            VALUES ('z', 'T1', 'C1', '2000.1', %s, 'U1', 'newer message', %s)
            """,
            (later, later),
        )
        warehouse._command(
            "UPDATE slack_messages SET text = 'slack says hi (edited)', synced_at = %s "
            "WHERE message_ts = '1000.1'",
            (later,),
        )
        old_seqs = {
            row["event_id"]: row["seq"]
            for row in warehouse._query_dicts("SELECT event_id, seq FROM timeline_events")
        }
        stats = engine.run()
    finally:
        engine.close()

    by_adapter = {s.adapter: s for s in stats}
    assert by_adapter["slack_message"].incremental_rows == 2
    rows = warehouse._query_dicts(
        "SELECT event_id, snippet, seq FROM timeline_events WHERE adapter = 'slack_message'"
    )
    by_id = {row["event_id"]: row for row in rows}
    assert len(by_id) == 2
    edited = by_id["z|T1|C1|1000.1"]
    assert edited["snippet"] == "slack says hi (edited)"
    assert edited["seq"] > old_seqs["z|T1|C1|1000.1"], "content change must bump seq"
    # Untouched rows keep their seq.
    gmail_id = "z@x.test|m1"
    gmail_seq = warehouse._query(
        "SELECT seq FROM timeline_events WHERE event_id = %s", (gmail_id,)
    )[0][0]
    assert gmail_seq == old_seqs[gmail_id]


def test_apple_message_incremental_picks_up_late_attachment_enrichment(warehouse):
    # The candidate-join incremental must re-emit a message when only its
    # attachment's enrichment changed (message row untouched) — the case that
    # forced the old unindexable GREATEST-over-LATERAL watermark predicate.
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    engine = _engine(warehouse)
    try:
        engine.run()

        later = _NOW + timedelta(minutes=10)
        warehouse._command(
            """
            INSERT INTO apple_message_attachments (account, attachment_id, message_id,
                                                   filename, content_sha256, ingested_at)
            VALUES ('z@x.test', 'att1', 'am1', 'marina.heic', 'sha-att1', %s)
            """,
            (later,),
        )
        warehouse._command(
            """
            INSERT INTO file_attachment_enrichments (content_sha256, ai_provider, ai_model,
                                                     ai_prompt_version, text, updated_at)
            VALUES ('sha-att1', 'p', 'm', 'v1', 'a photo of the marina at sunset', %s)
            """,
            (later,),
        )
        engine.run()
    finally:
        engine.close()

    rows = warehouse._query(
        "SELECT search_text FROM timeline_events WHERE event_id = 'z@x.test|am1'"
    )
    assert rows and "marina at sunset" in rows[0][0]


def test_backfill_pages_newest_first(warehouse):
    _ensure_all_source_tables(warehouse)
    for i in range(7):
        warehouse._command(
            """
            INSERT INTO gmail_messages (account, message_id, internal_date, subject, synced_at)
            VALUES ('z@x.test', %s, %s, %s, %s)
            """,
            (f"m{i}", _NOW - timedelta(hours=i), f"mail {i}", _NOW),
        )
    engine = _engine(warehouse, adapters=[adapter_by_name("gmail_email")], batch_size=3)
    try:
        # A tiny budget still lands the newest batch first.
        engine.run(max_seconds=0.000001)
        titles = [
            row[0]
            for row in warehouse._query("SELECT title FROM timeline_events ORDER BY event_ts DESC")
        ]
        assert titles == ["mail 0", "mail 1", "mail 2"]
        state = warehouse._query_dicts("SELECT * FROM timeline_sync_state")[0]
        assert state["backfill_done"] == 0
        # Finish the job with no budget cap.
        engine.run()
    finally:
        engine.close()
    count = warehouse._query("SELECT count(*) FROM timeline_events")[0][0]
    assert count == 7
    state = warehouse._query_dicts("SELECT * FROM timeline_sync_state")[0]
    assert state["backfill_done"] == 1
    assert state["backfill_rows"] == 7


def test_engine_pumps_into_a_separate_destination_schema(warehouse):
    """The dev mode: source stays untouched, timeline lands elsewhere."""
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    dest_schema = make_test_schema("dest")
    engine = _engine(warehouse, dest_schema=dest_schema)
    try:
        engine.run()
        with engine._dest_conn.cursor() as cursor:
            cursor.execute(engine._dest_sql("SELECT count(*) FROM timeline_events"))
            count = cursor.fetchone()[0]
        assert count == sum(EXPECTED_SEEDED_EVENTS.values())
        # Nothing was written into the source schema.
        assert not warehouse._query(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = 'events' LIMIT 1",
            (warehouse.physical_schema_name("timeline"),),
        ) or warehouse._query("SELECT count(*) FROM timeline_events")[0][0] == 0
    finally:
        with engine._dest_conn.cursor() as cursor:
            for schema_name in physical_schema_names(namespace=dest_schema, include_private=True) + [dest_schema]:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        engine.close()


def test_engine_reports_failures_loudly_but_keeps_going(warehouse):
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    broken = adapter_by_name("gmail_email").__class__(
        name="broken",
        source_table="gmail_messages",
        source="gmail",
        kind="email",
        backfill_sql="SELECT nonsense FROM missing_table WHERE x < %(cursor_ts)s AND y = %(cursor_id)s LIMIT %(limit)s",
        incremental_sql="SELECT nonsense FROM missing_table WHERE x > %(watermark_ts)s AND y = %(watermark_id)s LIMIT %(limit)s",
        max_ingest_sql="SELECT max(synced_at) FROM gmail_messages",
    )
    engine = _engine(warehouse, adapters=[broken, adapter_by_name("gmail_email")])
    try:
        with pytest.raises(TimelineSyncError) as excinfo:
            engine.run()
    finally:
        engine.close()
    stats = {s.adapter: s for s in excinfo.value.stats}
    assert stats["broken"].error
    assert stats["gmail_email"].backfill_rows == 1
    assert not stats["gmail_email"].error
