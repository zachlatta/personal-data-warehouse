from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from dotenv import load_dotenv

from personal_data_warehouse.postgres import POSTGRES_INDEXES, POSTGRES_TABLES, PostgresWarehouse
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
    schema = "pdw_test_" + uuid.uuid4().hex
    wh = PostgresWarehouse(_postgres_url(), schema=schema)
    try:
        yield wh
    finally:
        wh._command(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        wh.close()


def _ensure_all_source_tables(wh: PostgresWarehouse) -> None:
    """Run every ensure_* path so the schema contains every warehouse table."""
    wh.ensure_tables()
    wh.ensure_calendar_tables()
    wh.ensure_contacts_tables()
    wh.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    wh.ensure_apple_notes_tables()
    wh.ensure_apple_messages_tables()
    wh.ensure_whatsapp_tables()
    wh.ensure_agent_sessions_tables()
    wh.ensure_claude_desktop_tables()
    wh.ensure_agent_tables()
    wh.ensure_slack_tables()
    wh.ensure_upstream_mutation_tables()
    wh.ensure_google_drive_source_tables()
    wh.ensure_timeline_tables()


# --- coverage registry (pure) -------------------------------------------------


def test_every_registered_table_is_classified():
    registered = set(POSTGRES_TABLES) | set(RAW_DDL_TABLES)
    classified = set(TIMELINE_TABLE_COVERAGE)
    assert registered - classified == set(), "warehouse tables missing timeline classification"
    assert classified - registered == set(), "timeline classifications for unknown tables"


def test_adapter_source_tables_are_classified_as_events():
    adapter_tables = {adapter.source_table for adapter in TIMELINE_ADAPTERS}
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


def test_detail_coverage_points_at_covered_tables():
    for table, coverage in TIMELINE_TABLE_COVERAGE.items():
        if coverage.role != "detail":
            continue
        parent = TIMELINE_TABLE_COVERAGE.get(coverage.parent)
        assert parent is not None, f"{table} detail parent {coverage.parent!r} is unclassified"
        assert parent.role in ("events", "detail"), (
            f"{table} detail parent {coverage.parent!r} must chain to an events table"
        )


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
        "timeline_events_kind_time_idx",
        "timeline_events_seq_idx",
    } <= names


# --- live schema coverage (Postgres) -------------------------------------------


def test_live_schema_has_no_unclassified_tables(warehouse):
    _ensure_all_source_tables(warehouse)
    rows = warehouse._query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        """,
        (warehouse._schema,),
    )
    live_tables = {row[0] for row in rows}
    unclassified = live_tables - set(TIMELINE_TABLE_COVERAGE)
    assert unclassified == set(), (
        "tables exist in the warehouse schema without a timeline classification; "
        "add them to TIMELINE_TABLE_COVERAGE (and an adapter if they hold activity): "
        f"{sorted(unclassified)}"
    )
    # And the classification list should not reference tables that no longer exist.
    stale = set(TIMELINE_TABLE_COVERAGE) - live_tables
    assert stale == set(), f"classified tables missing from the live schema: {sorted(stale)}"


def test_ensure_timeline_tables_is_idempotent_and_indexed(warehouse):
    warehouse.ensure_timeline_tables()
    warehouse.ensure_timeline_tables()
    rows = warehouse._query(
        "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND tablename = 'timeline_events'",
        (warehouse._schema,),
    )
    names = {row[0] for row in rows}
    assert "timeline_events_time_idx" in names
    assert "timeline_events_seq_idx" in names
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
                    adapter.backfill_sql,
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


# The seeded fixture rows exercise one classification branch per adapter:
# gmail addressed directly to the account (2), a member-channel slack message
# from someone else (3), a 1:1-ish iMessage (2), a big whatsapp group (3),
# a session Zach prompted (1), his own notes/memos (1), a calendar event he
# organizes (1), an unstarred drive file (3), contact churn (4), and the
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
    "calendar_event": 1,
    "drive_file": 3,
    "contact_update": 4,
    "mutation": 5,
    "mutation_request": 5,
    "enrichment_run": 5,
}

EXPECTED_SEEDED_EVENTS = {
    "gmail_email": 1,
    "slack_message": 1,
    "slack_file": 1,
    "apple_message": 1,
    "whatsapp_message": 1,
    "agent_session": 1,
    "apple_note_revision": 1,
    "voice_memo": 1,
    "calendar_event": 1,
    "drive_file": 1,
    "contact_update": 1,
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
    # Newest first: gmail (NOW-1h) ... enrichment run (NOW-14h).
    assert rows[0]["adapter"] == "gmail_email"
    assert rows[-1]["adapter"] == "enrichment_run"

    gmail = rows[0]
    assert gmail["source"] == "gmail"
    assert gmail["kind"] == "email"
    assert gmail["actor"] == "alice@example.test"
    assert gmail["title"] == "Hello world"
    assert gmail["source_table"] == "gmail_messages"
    assert gmail["source_pk"] == {"account": "z@x.test", "message_id": "m1"}
    assert gmail["metadata"]["thread_id"] == "th1"

    slack = next(r for r in rows if r["adapter"] == "slack_message")
    assert slack["actor"] == "alice"
    assert slack["context"] == "#general"
    assert slack["snippet"] == "slack says hi"

    imsg = next(r for r in rows if r["adapter"] == "apple_message")
    assert imsg["actor"] == "+15551234567"
    assert imsg["context"] == "Family"

    session = next(r for r in rows if r["adapter"] == "agent_session")
    assert session["source"] == "claude_code"
    assert session["title"] == "Fix the bug"
    assert session["context"] == "/repo"
    assert session["metadata"]["events"] == 2
    assert session["end_ts"] > session["event_ts"]

    memo = next(r for r in rows if r["adapter"] == "voice_memo")
    assert memo["title"] == "Standup notes"
    assert memo["snippet"] == "we discussed things"

    cal = next(r for r in rows if r["adapter"] == "calendar_event")
    assert cal["end_ts"] > cal["event_ts"]

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
    assert priority_of("z@x.test|m-notify") == 3, "transactional senders cap at the cc tier"
    assert priority_of("z@x.test|m-starred") == 2, "starred email is direct"
    assert priority_of("openclaw|cron-sess") == 5, "cron heartbeat sessions are background"


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
    dest_schema = "pdw_test_dest_" + uuid.uuid4().hex
    engine = _engine(warehouse, dest_schema=dest_schema)
    try:
        engine.run()
        with engine._dest_conn.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM timeline_events")
            count = cursor.fetchone()[0]
        assert count == sum(EXPECTED_SEEDED_EVENTS.values())
        # Nothing was written into the source schema.
        assert not warehouse._query(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = 'timeline_events' LIMIT 1",
            (warehouse._schema,),
        ) or warehouse._query("SELECT count(*) FROM timeline_events")[0][0] == 0
    finally:
        with engine._dest_conn.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{dest_schema}" CASCADE')
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
