from __future__ import annotations

import hashlib
import os
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.postgres import POSTGRES_INDEXES, POSTGRES_TABLES, PostgresWarehouse
from personal_data_warehouse.relations import (
    AI_EVENT_SOURCE_RELATIONS,
    CANONICAL_RELATIONS,
    VOICE_EVENT_SOURCE_RELATIONS,
    physical_schema_names,
    relation,
)
from personal_data_warehouse.timeline import (
    RAW_DDL_TABLES,
    RETIRED_TIMELINE_ADAPTERS,
    TIMELINE_ADAPTERS,
    TIMELINE_NORMALIZED_COLUMNS,
    TIMELINE_PRIORITY_DEFINITIONS,
    TIMELINE_PRIORITY_LABELS,
    TIMELINE_PRIORITY_UNCLASSIFIED,
    TIMELINE_TABLE_COVERAGE,
    BACKFILL_CURSOR_START,
    TimelineSyncEngine,
    TimelineSyncError,
    _simple_adapter,
    adapter_definition_signature,
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
    wh.ensure_whoop_private_tables()
    wh.ensure_plaid_tables()
    wh.ensure_finance_tables()
    wh.ensure_manual_finance_tables()
    wh.ensure_receipt_tables()
    wh.ensure_search_index_tables()
    wh.ensure_timeline_tables()
    wh.ensure_pipeline_health_tables()


# --- coverage registry (pure) -------------------------------------------------


def test_every_registered_table_is_classified():
    registered = set(POSTGRES_TABLES) | set(RAW_DDL_TABLES)
    classified = set(TIMELINE_TABLE_COVERAGE)
    assert registered - classified == set(), "warehouse tables missing timeline classification"
    assert classified - registered == set(), "timeline classifications for unknown tables"


def test_adapter_source_tables_are_classified_as_events():
    adapter_tables: set[str] = set()
    for adapter in TIMELINE_ADAPTERS:
        # An adapter over a conforming mart covers every raw table that mart
        # unions, and those raw tables are what the coverage registry knows
        # about. The registry is the only thing that can say so.
        if adapter.source_table == "ai_conversation_events":
            adapter_tables.update(AI_EVENT_SOURCE_RELATIONS.values())
        elif adapter.source_table == "marts_voice_memos_recordings":
            adapter_tables.update(VOICE_EVENT_SOURCE_RELATIONS.values())
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


def test_one_voice_adapter_covers_every_voice_source():
    """Voice reaches the timeline through the mart, not once per source.

    There were two adapters, one per raw table, and only the Apple one had
    ever been taught about transcripts and summaries -- so the second source's
    rows carried a filename and nothing else. One adapter over
    marts_voice_memos.recordings is what makes a third source free.
    """
    voice = [a for a in TIMELINE_ADAPTERS if a.source_table in set(VOICE_EVENT_SOURCE_RELATIONS.values())]
    assert voice == [], "a voice adapter still reads a raw source table"
    adapter = adapter_by_name("voice_memo")
    assert adapter.source_table == "marts_voice_memos_recordings"
    assert "alice_voice_recording" in RETIRED_TIMELINE_ADAPTERS
    with pytest.raises(KeyError):
        adapter_by_name("alice_voice_recording")


def test_timeline_includes_finance_activity_adapters():
    requested = {
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
        if adapter.name not in ("agent_session", "agent_session_turn")
    }
    # The agent-session adapters emit per-provider `source` values at runtime.
    expected.update((source, "agent_session") for source in AI_EVENT_SOURCE_RELATIONS)
    expected.update((source, "agent_turn") for source in AI_EVENT_SOURCE_RELATIONS)
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


def _priority_literals(sql: str) -> set[str]:
    """Every enum-label literal the adapter's SQL could emit as a priority.

    The priority expression is always built from quoted label literals (the
    TIMELINE_PRIORITY_* constants or an inline CASE arm), so scanning for
    quoted occurrences of the six enum labels is exact enough to catch both a
    typo'd tier and the unclassified sentinel.
    """
    labels = (*TIMELINE_PRIORITY_LABELS, TIMELINE_PRIORITY_UNCLASSIFIED.strip("'"))
    return {label for label in labels if f"'{label}'" in sql}


def test_every_adapter_emits_a_real_priority_tier():
    """The tiers are a contract, not free text.

    Agents are told to filter on these five labels; an adapter that emitted a
    sixth (or nothing) would silently drop its whole source out of every
    documented review. The engine has no fallback: a missing or NULL tier
    stops that adapter and is recorded on the health surface.
    """
    for adapter in TIMELINE_ADAPTERS:
        for sql in (adapter.backfill_sql, adapter.incremental_sql):
            emitted = _priority_literals(sql)
            assert emitted, f"{adapter.name} emits no priority tier literal"
            assert emitted <= set(TIMELINE_PRIORITY_LABELS), (adapter.name, emitted)


def test_adapter_registration_requires_an_intentional_priority_expression():
    with pytest.raises(TypeError):
        _simple_adapter(
            name="missing_priority",
            source_table="gmail_messages",
            source="gmail",
            kind="email",
            from_sql="@gmail_messages m",
            event_id="m.id",
            event_ts="m.internal_date",
            ingest_ts="m.synced_at",
            source_pk="m.id",
        )

    assert all(adapter.priority_expression.strip() for adapter in TIMELINE_ADAPTERS)


def test_generated_adapter_sql_has_no_silent_cc_fallback_and_keeps_rollout_signature():
    adapter = adapter_by_name("gmail_email")
    fallback = "COALESCE((" + adapter.priority_expression + "), 'cc') AS priority"
    assert fallback not in adapter.backfill_sql
    assert fallback in adapter.signature_backfill_sql

    # Only the removed engine wrapper differs. The compatibility SQL exactly
    # reproduces the pre-rollout fingerprint, avoiding a 48M-row rewalk.
    legacy_payload = "\n".join(
        [
            adapter.name,
            adapter.source_table,
            adapter.source,
            adapter.kind,
            adapter.signature_backfill_sql,
            adapter.signature_incremental_sql,
            adapter.max_ingest_sql,
        ]
    )
    assert adapter_definition_signature(adapter) == hashlib.sha256(
        legacy_payload.encode("utf-8")
    ).hexdigest()


def test_no_adapter_can_emit_the_unclassified_sentinel():
    """'unclassified' is the column DEFAULT and a bug marker, never a tier.

    The label cannot be dropped (rewriting a 60 GB enum column), so the
    guarantee that it only ever marks a row the sync engine never wrote has to
    be enforced here instead.
    """
    assert TIMELINE_PRIORITY_UNCLASSIFIED.strip("'") not in TIMELINE_PRIORITY_LABELS
    for adapter in TIMELINE_ADAPTERS:
        for sql in (adapter.backfill_sql, adapter.incremental_sql):
            assert TIMELINE_PRIORITY_UNCLASSIFIED not in sql, adapter.name


def test_expected_fixtures_cover_every_adapter():
    """Pin the fixtures to the registry, not the other way round.

    test_backfill_normalizes_every_source iterates EXPECTED_SEEDED_EVENTS, so
    for as long as that dict was the source of truth an adapter could be added
    with no row count and no tier assertion anywhere — which is exactly how the
    four whoop adapters and apple_contact_update went unasserted.
    """
    registered = {adapter.name for adapter in TIMELINE_ADAPTERS}
    assert set(EXPECTED_SEEDED_EVENTS) == registered
    assert set(EXPECTED_SEEDED_PRIORITIES) == registered


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
    sql = timeline_upsert_sql(table_ref='"timeline"."events"', sequence_ref='"timeline"."events_seq"')
    assert "ON CONFLICT (adapter, event_id) DO UPDATE" in sql
    assert "seq = nextval('\"timeline\".\"events_seq\"')" in sql
    assert "IS DISTINCT FROM" in sql
    # A re-sync that only refreshes the source's ingestion timestamp must not
    # count as a content change, or every re-synced row looks new to
    # arrival-order consumers.
    guard = sql.split("WHERE", 1)[1]
    assert "ingest_ts = EXCLUDED.ingest_ts" not in guard


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
    # kind filter rides the time/priority indexes. (timeline_events_seq_idx was
    # retired for the same reason, then revived: the search-chunk builder pages
    # timeline changes by bare seq as its incremental cursor.)
    assert "timeline_events_kind_time_idx" not in names
    assert "timeline_events_seq_idx" in names


# --- live schema coverage (Postgres) -------------------------------------------


def test_live_schema_has_no_unclassified_tables(warehouse):
    _ensure_all_source_tables(warehouse)
    rows = warehouse._query(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = ANY(%s) AND table_type = 'BASE TABLE'
        """,
        (warehouse.physical_schema_names(include_hidden=True),),
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
        if physical not in live_physical
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
    assert "timeline_events_priority_time_idx" in names
    # A fresh install builds priority as the self-describing enum, defaulting to
    # 'unclassified'; the value IS the label and only valid labels are accepted.
    col = warehouse._query(
        """
        SELECT data_type, udt_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = 'events' AND column_name = 'priority'
        """,
        (warehouse.physical_schema_name("timeline"),),
    )[0]
    assert col == ("USER-DEFINED", "timeline_priority")
    warehouse._command(
        "INSERT INTO @timeline_events (adapter, event_id, source, kind, event_ts, source_table, priority) "
        "VALUES ('t', 'ep', 's', 'k', now(), 'x', 'self')"
    )
    assert warehouse._query(
        "SELECT priority FROM @timeline_events WHERE event_id = 'ep'"
    )[0][0] == "self"
    with pytest.raises(Exception):
        warehouse._command(
            "INSERT INTO @timeline_events (adapter, event_id, source, kind, event_ts, source_table, priority) "
            "VALUES ('t', 'ebad', 's', 'k', now(), 'x', 'not-a-tier')"
        )
    # The tiers and the columns must be self-documenting in Postgres itself:
    # agents are told to filter on `priority` and many of them read the schema
    # directly, where col_description() used to return NULL for all nineteen
    # columns and the tier labels were undefined anywhere but a Python comment.
    type_comment = warehouse._query(
        "SELECT obj_description(%s::regtype, 'pg_type')",
        (warehouse.sql_relation("timeline_priority"),),
    )[0][0]
    for label, meaning in TIMELINE_PRIORITY_DEFINITIONS:
        assert f"{label} = {meaning}" in type_comment, label
    column_comments = dict(
        warehouse._query(
            """
            SELECT a.attname, col_description(a.attrelid, a.attnum)
            FROM pg_attribute a
            WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped
            """,
            (warehouse.sql_relation("timeline_events"),),
        )
    )
    assert all(comment for comment in column_comments.values()), column_comments
    assert "timeline_priority" in column_comments["priority"]

    # seq must be sequence-backed so upserts can bump it.
    warehouse._command(
        "INSERT INTO @timeline_events (adapter, event_id, source, kind, event_ts, source_table) "
        "VALUES ('t', 'e1', 's', 'k', now(), 'x'), ('t', 'e2', 's', 'k', now(), 'x')"
    )
    seqs = [row[0] for row in warehouse._query("SELECT seq FROM @timeline_events ORDER BY event_id")]
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
_EPOCH_TS = datetime(1970, 1, 1, tzinfo=UTC)


def _seed_sources(wh: PostgresWarehouse) -> None:
    """A little bit of everything, with distinct event times."""
    wh._command(
        """
        INSERT INTO @gmail_messages (account, message_id, thread_id, internal_date, subject,
                                    from_address, to_addresses, snippet, synced_at)
        VALUES ('z@x.test', 'm1', 'th1', %s, 'Hello world', 'alice@example.test',
                %s, 'hi there', %s)
        """,
        (_NOW - timedelta(hours=1), ["Zach <z@x.test>"], _NOW),
    )
    wh._command(
        """
        INSERT INTO @slack_users (account, team_id, user_id, display_name)
        VALUES ('z', 'T1', 'U1', 'alice')
        """
    )
    wh._command(
        """
        INSERT INTO @slack_account_identities (account, team_id, user_id)
        VALUES ('z', 'T1', 'UME')
        """
    )
    wh._command(
        """
        INSERT INTO @slack_conversations (account, team_id, conversation_id, name, is_member)
        VALUES ('z', 'T1', 'C1', 'general', 1)
        """
    )
    wh._command(
        """
        INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, text, synced_at)
        VALUES ('z', 'T1', 'C1', '1000.1', %s, 'U1', 'slack says hi', %s)
        """,
        (_NOW - timedelta(hours=2), _NOW),
    )
    wh._command(
        """
        INSERT INTO @slack_files (account, team_id, file_id, conversation_id, message_ts,
                                 user_id, created_at, name, title, mimetype, synced_at)
        VALUES ('z', 'T1', 'F1', 'C1', '1000.1', 'U1', %s, 'notes.pdf', '', 'application/pdf', %s)
        """,
        (_NOW - timedelta(hours=3), _NOW),
    )
    wh._command(
        """
        INSERT INTO @apple_message_handles (account, handle_id, address)
        VALUES ('z@x.test', 'h1', '+15551234567')
        """
    )
    wh._command(
        """
        INSERT INTO @apple_message_chats (account, chat_id, display_name)
        VALUES ('z@x.test', 'c1', 'Family')
        """
    )
    wh._command(
        """
        INSERT INTO @apple_message_chat_messages (account, chat_id, message_id, message_date, ingested_at)
        VALUES ('z@x.test', 'c1', 'am1', %s, %s)
        """,
        (_NOW - timedelta(hours=4), _NOW),
    )
    wh._command(
        """
        INSERT INTO @apple_messages (account, message_id, handle_id, body_text, message_at,
                                    is_from_me, ingested_at)
        VALUES ('z@x.test', 'am1', 'h1', 'imessage body', %s, 0, %s)
        """,
        (_NOW - timedelta(hours=4), _NOW),
    )
    # Zach has replied in this chat, so it reads as a conversation rather
    # than a one-way broadcast.
    wh._command(
        """
        INSERT INTO @apple_message_chat_messages (account, chat_id, message_id, message_date, ingested_at)
        VALUES ('z@x.test', 'c1', 'am0', %s, %s)
        """,
        (_NOW - timedelta(hours=3), _NOW),
    )
    wh._command(
        """
        INSERT INTO @apple_messages (account, message_id, handle_id, body_text, message_at,
                                    is_from_me, ingested_at)
        VALUES ('z@x.test', 'am0', '', 'sounds good!', %s, 1, %s)
        """,
        (_NOW - timedelta(hours=3), _NOW),
    )
    wh._command(
        """
        INSERT INTO @whatsapp_chats (account, chat_id, name)
        VALUES ('z@x.test', 'chat@g.us', 'The Group')
        """
    )
    wh._command(
        """
        INSERT INTO @whatsapp_messages (account, chat_id, message_id, sender_jid, push_name,
                                       body_text, message_at, is_from_me, ingested_at)
        VALUES ('z@x.test', 'chat@g.us', 'wm1', 'p@s.whatsapp.net', 'bob',
                'whatsapp body', %s, 0, %s)
        """,
        (_NOW - timedelta(hours=5), _NOW),
    )
    for seq, (role, text) in enumerate([("user", "fix the bug"), ("assistant", "done")]):
        wh._command(
            """
            INSERT INTO @claude_code_events (source, session_id, event_uuid, seq, occurred_at,
                                              role, text, session_title, cwd, device, ingested_at)
            VALUES ('claude_code', 'sess1', %s, %s, %s, %s, %s, 'Fix the bug', '/repo', 'porygon', %s)
            """,
            (f"u{seq}", seq, _NOW - timedelta(hours=6) + timedelta(minutes=seq), role, text, _NOW),
        )
    wh._command(
        """
        INSERT INTO @apple_note_revisions (account, note_id, revision_id, title, body_text,
                                          folder_path, modified_at, ingested_at)
        VALUES ('z@x.test', 'n1', 'r1', 'Groceries', 'milk, eggs', 'Notes', %s, %s)
        """,
        (_NOW - timedelta(hours=7), _NOW),
    )
    wh._command(
        """
        INSERT INTO @apple_note_attachments (account, note_id, revision_id, attachment_id, filename,
                                            content_type, size_bytes, content_sha256, is_missing,
                                            storage_file_id, ingested_at)
        VALUES ('z@x.test', 'n1', 'r1', 'note-audio-1', 'Call Recording.m4a',
                'audio/mp4a-latm', 4096, 'note-audio-sha', 0, 'drive-note-audio', %s)
        """,
        (_NOW,),
    )
    wh._command(
        """
        INSERT INTO @apple_voice_memos_files (account, recording_id, title, filename, recorded_at, ingested_at)
        VALUES ('z@x.test', 'rec1', 'Standup', 'standup.m4a', %s, %s)
        """,
        (_NOW - timedelta(hours=8), _NOW),
    )
    wh._command(
        """
        INSERT INTO @apple_voice_memos_enrichments (source, account, recording_id, provider, model,
                                                   prompt_version, status, title, summary, created_at)
        VALUES ('apple_voice_memos', 'z@x.test', 'rec1', 'p', 'm', 'v1', 'completed',
                'Standup notes', 'we discussed things', %s)
        """,
        (_NOW,),
    )
    wh._command(
        """
        INSERT INTO @alice_voice_recordings (account, recording_id, title, filename,
                                            content_type, recorded_at, duration_seconds, ingested_at)
        VALUES ('z@x.test', 'alice-rec1', 'Alice walk', 'alice-walk.m4a',
                'audio/mp4', %s, 321, %s)
        """,
        (_NOW - timedelta(hours=8, minutes=30), _NOW),
    )
    wh._command(
        """
        INSERT INTO @calendar_events (account, calendar_id, event_id, summary, description,
                                     organizer_email, start_at, end_at, updated_at, synced_at)
        VALUES ('z@x.test', 'cal1', 'ev1', 'Team sync', 'weekly', 'z@x.test', %s, %s, %s, %s)
        """,
        (_NOW - timedelta(hours=9), _NOW - timedelta(hours=8, minutes=30), _NOW, _NOW),
    )
    wh._command(
        """
        INSERT INTO @google_drive_files (account, file_id, name, mime_type, folder_path,
                                        last_modifying_user, modified_time, ingested_at)
        VALUES ('z@x.test', 'f1', 'Design doc', 'application/vnd.google-apps.document',
                'My Drive', 'zach', %s, %s)
        """,
        (_NOW - timedelta(hours=10), _NOW),
    )
    wh._command(
        """
        INSERT INTO @contact_cards (source, account, source_kind, address_book_id, card_id,
                                   display_name, organization, source_updated_at, synced_at)
        VALUES ('google', 'z@x.test', 'personal', 'ab1', 'card1', 'Ada Example', 'Example Engines',
                %s, %s)
        """,
        (_NOW - timedelta(hours=11), _NOW),
    )
    wh._command(
        """
        INSERT INTO @photo_assets (photo_id, account, kind, capture_ts, camera_make, camera_model,
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
        INSERT INTO @photo_asset_files (source, account, source_native_id, role, content_sha256,
                                       photo_id, match_method, created_at)
        VALUES ('apple_photos', 'z@x.test', 'UUID-1', 'original', 'stillsha', 'ph1', 'new', %s)
        """,
        (_NOW,),
    )
    wh._command(
        """
        INSERT INTO @file_attachment_enrichments (content_sha256, ai_provider, ai_model,
                                                 ai_prompt_version, text, updated_at)
        VALUES ('thumbsha', 'agent_codex', 'm', 'photo-agent-v1',
                'A golden retriever on a beach at sunset', %s)
        """,
        (_NOW,),
    )
    wh._command(
        """
        INSERT INTO @upstream_mutations (id, provider, operation, status, title, reason,
                                        requested_by, executed_at, created_at, updated_at)
        VALUES ('mut1', 'slack', 'chat.postMessage', 'executed', 'Send standup reminder',
                'weekly reminder', 'assistant', %s, %s, %s)
        """,
        (_NOW - timedelta(hours=12), _NOW - timedelta(hours=13), _NOW),
    )
    wh._command(
        """
        INSERT INTO @upstream_mutation_requests (id, status, title, reason, requested_by,
                                                created_at, updated_at)
        VALUES ('req1', 'approved', 'Standup reminders', 'requested by zach', 'assistant', %s, %s)
        """,
        (_NOW - timedelta(hours=13), _NOW),
    )
    wh._command(
        """
        INSERT INTO @agent_runs (run_id, provider, model, task_type, subject_id, status,
                                started_at, completed_at)
        VALUES ('run1', 'codex', 'gpt-5', 'attachment_enrichment', 'sha1', 'ok', %s, %s)
        """,
        (_NOW - timedelta(hours=14), _NOW - timedelta(hours=13, minutes=50)),
    )
    sync_version = int(_NOW.timestamp() * 1_000_000)
    wh._command(
        """
        INSERT INTO @finance_accounts (account_id, account, name, kind, side, currency,
                                      institution, created_at, updated_at, sync_version)
        VALUES ('fa1', 'z@x.test', 'Checking', 'checking', 'asset', 'USD',
                'Example Bank', %s, %s, %s)
        """,
        (_NOW, _NOW, sync_version),
    )
    wh._command(
        """
        INSERT INTO @finance_transactions (transaction_id, account_id, posted_at, amount,
                                          currency, description, merchant, pending, source,
                                          created_at, sync_version)
        VALUES ('ft1', 'fa1', %s, -12.34, 'USD', 'Lunch', 'Cafe', 0, 'plaid', %s, %s)
        """,
        (_NOW - timedelta(hours=15), _NOW, sync_version),
    )
    wh._command(
        """
        INSERT INTO @finance_observations (account_id, as_of, kind, value, currency,
                                          source, observed_at, sync_version)
        VALUES ('fa1', '2026-05-31', 'balance', 1234.56, 'USD', 'plaid', %s, %s)
        """,
        (_NOW, sync_version),
    )
    wh._command(
        """
        INSERT INTO @manual_finance_documents (source, account, source_native_id, filename,
                                              original_path, mime_type, content_sha256,
                                              file_modified_at, ingested_at, sync_version)
        VALUES ('manual', 'z@x.test', 'docsha', 'statement.pdf', 'Bank/Checking',
                'application/pdf', 'docsha', %s, %s, %s)
        """,
        (_NOW - timedelta(hours=16), _NOW, sync_version),
    )
    wh._command(
        """
        INSERT INTO @manual_finance_extractions (content_sha256, ai_provider, ai_model,
                                                ai_prompt_version, status, institution,
                                                period_end, summary, created_at, sync_version)
        VALUES ('docsha', 'agent_codex', 'm', 'v1', 'completed', 'Example Bank',
                '2026-06-01', 'Monthly checking statement', %s, %s)
        """,
        (_NOW, sync_version),
    )
    wh._command(
        """
        INSERT INTO @apple_contact_cards (source, account, source_kind, address_book_id, card_id,
                                         display_name, organization, source_updated_at, synced_at)
        VALUES ('apple', 'z@x.test', 'local', 'ab-apple', 'apple-card1', 'Bo Example',
                'Example Engines', %s, %s)
        """,
        (_NOW - timedelta(hours=17), _NOW),
    )
    wh._command(
        """
        INSERT INTO @whoop_cycles (account, cycle_id, start_at, end_at, score_state, strain,
                                  average_heart_rate, max_heart_rate, created_at, synced_at)
        VALUES ('z@x.test', 'cyc1', %s, %s, 'SCORED', 12.5, 62, 141, %s, %s)
        """,
        (_NOW - timedelta(hours=18), _NOW - timedelta(hours=17), _NOW, _NOW),
    )
    wh._command(
        """
        INSERT INTO @whoop_recoveries (account, cycle_id, sleep_id, score_state, recovery_score,
                                      resting_heart_rate, hrv_rmssd_milli, created_at,
                                      updated_at, synced_at)
        VALUES ('z@x.test', 'cyc1', 'slp1', 'SCORED', 71, 54, 84.2, %s, %s, %s)
        """,
        (_NOW, _NOW, _NOW),
    )
    wh._command(
        """
        INSERT INTO @whoop_sleeps (account, sleep_id, cycle_id, start_at, end_at, nap,
                                  score_state, respiratory_rate,
                                  sleep_performance_percentage, sleep_efficiency_percentage,
                                  created_at, synced_at)
        VALUES ('z@x.test', 'slp1', 'cyc1', %s, %s, 0, 'SCORED', 14.1, 92, 95, %s, %s)
        """,
        (_NOW - timedelta(hours=19), _NOW - timedelta(hours=18), _NOW, _NOW),
    )
    wh._command(
        """
        INSERT INTO @whoop_workouts (account, workout_id, start_at, end_at, sport_name,
                                    score_state, strain, average_heart_rate, max_heart_rate,
                                    distance_meter, created_at, synced_at)
        VALUES ('z@x.test', 'wk1', %s, %s, 'Running', 'SCORED', 9.4, 148, 172, 5000, %s, %s)
        """,
        (_NOW - timedelta(hours=20), _NOW - timedelta(hours=19), _NOW, _NOW),
    )
    # The one whoop_private table with an adapter. Everything else that source
    # syncs is `detail` of the public base_whoop row it duplicates, so seeding
    # it would be seeding rows no adapter reads.
    wh._command(
        """
        INSERT INTO @whoop_private_journal_entries (account, day, question_id, question_text,
                                                   answer, behavior_id, synced_at, sync_version)
        VALUES ('z@x.test', %s, '62', 'Did you drink any alcohol?', 'false', '7', %s, %s)
        """,
        # `day` is a DATE, so this lands at midnight UTC of the seeded day —
        # deliberately not older than the finance observation the ordering
        # assertions treat as the oldest seeded row.
        (_NOW.date(), _NOW, sync_version),
    )


# The seeded fixture rows exercise one classification branch per adapter:
# gmail addressed directly to the account (2), a member-channel slack message
# from someone else (3), a 1:1 iMessage in a chat Zach replies in (2, plus
# his own reply at 1), an unknown-roster whatsapp group (3), a session Zach
# prompted (1), his own notes/memos (1), a calendar event he organizes (1),
# an unstarred drive file (3), contact churn (5: sync machinery), device
# telemetry vs a workout he did and a journal answer he typed (4 vs 1), and
# the warehouse's own machinery (5).
# Every adapter appears: test_expected_fixtures_cover_every_adapter pins the
# keys to the registry, so a new adapter cannot ship without a tier assertion.
EXPECTED_SEEDED_PRIORITIES = {
    "gmail_email": "direct",
    "slack_message": "noise",
    "slack_file": "noise",
    "apple_message": "direct",
    "whatsapp_message": "cc",
    "agent_session": "self",
    "agent_session_turn": "background",
    "apple_note_revision": "self",
    "voice_memo": "self",
    "calendar_event": "self",
    "drive_file": "background",
    "photo": "self",
    "contact_update": "background",
    "apple_contact_update": "background",
    "whoop_cycle": "noise",
    "whoop_recovery": "noise",
    "whoop_sleep": "noise",
    "whoop_workout": "self",
    "whoop_private_journal": "self",
    "finance_transaction": "self",
    "finance_observation": "background",
    "manual_finance_document": "self",
    "mutation": "background",
    "mutation_request": "background",
    "enrichment_run": "background",
}

EXPECTED_SEEDED_EVENTS = {
    "gmail_email": 1,
    "slack_message": 1,
    "slack_file": 1,
    "apple_message": 2,
    "whatsapp_message": 1,
    "agent_session": 1,
    "agent_session_turn": 2,
    "apple_note_revision": 1,
    # One adapter, two seeded sources: the Apple memo and the Alice recording.
    "voice_memo": 3,
    "calendar_event": 1,
    "drive_file": 1,
    "photo": 1,
    "contact_update": 1,
    "apple_contact_update": 1,
    "whoop_cycle": 1,
    "whoop_recovery": 1,
    "whoop_sleep": 1,
    "whoop_workout": 1,
    "whoop_private_journal": 1,
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
        "SELECT * FROM @timeline_events ORDER BY event_ts DESC"
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
    # Transcript content is indexed per turn, not on the session row: one
    # session-sized document diluted BM25 relevance and hid the matched turn
    # outside the search preview.
    assert "done" not in session["search_text"]

    drive = next(r for r in rows if r["adapter"] == "drive_file")
    # The file id is searchable: an agent holding a Drive id (from a URL or
    # an email) must be able to reach the file itself, not only the mail
    # that mentions it.
    assert "f1" in drive["search_text"].split()
    assert "Design doc" in drive["search_text"]

    turns = [r for r in rows if r["adapter"] == "agent_session_turn"]
    assert len(turns) == 2
    assert {t["kind"] for t in turns} == {"agent_turn"}
    assert {t["source"] for t in turns} == {"claude_code"}
    assert {t["context"] for t in turns} == {"claude_code|sess1"}
    assert all(t["priority"] == "background" for t in turns)
    reply = next(t for t in turns if t["metadata"]["role"] == "assistant")
    assert reply["actor"] == "assistant"
    assert "done" in reply["search_text"]
    assert reply["source_pk"]["session_id"] == "sess1"
    assert reply["metadata"]["seq"] == 1

    memos = [r for r in rows if r["adapter"] == "voice_memo"]
    assert {r["metadata"]["voice_source"] for r in memos} == {
        "apple_voice_memos",
        "alice_voice_recordings",
        "apple_notes",
    }
    note_audio = next(r for r in memos if r["metadata"]["voice_source"] == "apple_notes")
    assert note_audio["source_pk"]["recording_id"] == "note-audio-1"
    assert note_audio["title"] == "Groceries"  # the note's title until an enrichment names it
    memo = next(r for r in memos if r["metadata"]["voice_source"] == "apple_voice_memos")
    assert memo["title"] == "Standup notes"
    assert memo["snippet"] == "we discussed things"

    alice = next(r for r in memos if r["metadata"]["voice_source"] == "alice_voice_recordings")
    assert alice["title"] == "Alice walk"
    # The second source is on the SAME adapter, so it inherits every field the
    # first one has -- including the summary a transcript will later fill in.
    assert alice["source_pk"]["recording_id"] == "alice-rec1"

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

    # Second run is a no-op: nothing new, no seq churn. Keyed on the real
    # primary key: event_id alone is only unique within an adapter, and the
    # whoop cycle/recovery adapters legitimately share one (account, cycle_id).
    seqs_before = {(row["adapter"], row["event_id"]): row["seq"] for row in rows}
    engine2 = _engine(warehouse)
    try:
        stats2 = engine2.run()
    finally:
        engine2.close()
    assert all(s.backfill_rows == 0 and s.incremental_rows == 0 for s in stats2)
    rows_after = warehouse._query_dicts("SELECT adapter, event_id, seq FROM @timeline_events")
    assert {(r["adapter"], r["event_id"]): r["seq"] for r in rows_after} == seqs_before


def test_priority_classifies_self_direct_mention_bulk_and_cron(warehouse):
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    # My own slack message -> self.
    warehouse._command(
        """
        INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, text, synced_at)
        VALUES ('z', 'T1', 'C1', '3000.1', %s, 'UME', 'shipping it', %s)
        """,
        (_NOW, _NOW),
    )
    # A mention of me in a member channel -> direct.
    warehouse._command(
        """
        INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, text, synced_at)
        VALUES ('z', 'T1', 'C1', '3000.2', %s, 'U1', 'hey <@UME> take a look', %s)
        """,
        (_NOW, _NOW),
    )
    # A DM from a real person -> direct.
    warehouse._command(
        """
        INSERT INTO @slack_conversations (account, team_id, conversation_id, is_im)
        VALUES ('z', 'T1', 'D1', 1)
        """
    )
    warehouse._command(
        """
        INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, text, synced_at)
        VALUES ('z', 'T1', 'D1', '3000.3', %s, 'U1', 'lunch?', %s)
        """,
        (_NOW, _NOW),
    )
    # A bot post in the member channel -> noise.
    warehouse._command(
        """
        INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, bot_id, text, synced_at)
        VALUES ('z', 'T1', 'C1', '3000.4', %s, '', 'B1', 'deploy finished', %s)
        """,
        (_NOW, _NOW),
    )
    # A promo email addressed directly to me is still bulk -> noise.
    warehouse._command(
        """
        INSERT INTO @gmail_messages (account, message_id, internal_date, subject, from_address,
                                    to_addresses, label_ids, synced_at)
        VALUES ('z@x.test', 'm-promo', %s, 'SALE', 'deals@shop.example',
                %s, %s, %s)
        """,
        (_NOW, ["z@x.test"], ["CATEGORY_PROMOTIONS", "INBOX"], _NOW),
    )
    # A reply by someone else in a thread I participated in -> direct.
    warehouse._command(
        """
        INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts, thread_ts,
                                    message_datetime, user_id, text, synced_at)
        VALUES ('z', 'T1', 'C1', '4000.1', '4000.1', %s, 'UME', 'starting a thread', %s),
               ('z', 'T1', 'C1', '4000.2', '4000.1', %s, 'U1', 'replying to zach', %s)
        """,
        (_NOW, _NOW, _NOW, _NOW),
    )
    # Slack narrating that I was added to a channel is not my action -> noise.
    warehouse._command(
        """
        INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
                                    message_datetime, user_id, subtype, text, synced_at)
        VALUES ('z', 'T1', 'C1', '3000.5', %s, 'UME', 'channel_join',
                '<@UME> has joined the channel', %s)
        """,
        (_NOW, _NOW),
    )
    # A drive file I own and last modified myself -> self; my file edited by
    # someone else keeps me in the loop -> cc.
    warehouse._command(
        """
        INSERT INTO @google_drive_files (account, file_id, name, owners_json,
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
        INSERT INTO @gmail_messages (account, message_id, internal_date, subject, from_address,
                                    to_addresses, synced_at)
        VALUES ('z@x.test', 'm-noreply', %s, 'Weekly digest', 'noreply@service.example', %s, %s),
               ('z@x.test', 'm-notify', %s, 'Receipt attached', 'receipts@service.example', %s, %s)
        """,
        (_NOW, ["z@x.test"], _NOW, _NOW, ["z@x.test"], _NOW),
    )
    warehouse._command(
        """
        INSERT INTO @gmail_messages (account, message_id, internal_date, subject, from_address,
                                    to_addresses, label_ids, synced_at)
        VALUES ('z@x.test', 'm-starred', %s, 'Contract', 'lawyer@firm.example', %s, %s, %s)
        """,
        (_NOW, ["z@x.test"], ["STARRED", "INBOX"], _NOW),
    )
    # A business/RCS sender (not a phone number, not an email) -> noise.
    warehouse._command(
        """
        INSERT INTO @apple_message_handles (account, handle_id, address)
        VALUES ('z@x.test', 'h-biz', 'some_airline_dsqx1')
        """
    )
    warehouse._command(
        """
        INSERT INTO @apple_messages (account, message_id, handle_id, body_text, message_at,
                                    is_from_me, ingested_at)
        VALUES ('z@x.test', 'am-biz', 'h-biz', 'Your flight changed', %s, 0, %s)
        """,
        (_NOW, _NOW),
    )
    # The warehouse's own excluded Drive storage blobs -> background.
    warehouse._command(
        """
        INSERT INTO @google_drive_files (account, file_id, name, is_excluded, modified_time, ingested_at)
        VALUES ('z@x.test', 'f-excluded', 'blob-shard.bin', 1, %s, %s)
        """,
        (_NOW, _NOW),
    )
    # An openclaw cron heartbeat session -> background.
    for seq, (role, text) in enumerate([("user", "[cron:abc123 Monitor things] Run checks"), ("assistant", "ok")]):
        warehouse._command(
            """
            INSERT INTO @openclaw_events (source, session_id, event_uuid, seq, occurred_at,
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

    def priority_of(event_id: str) -> str:
        return warehouse._query(
            "SELECT priority FROM @timeline_events WHERE event_id = %s", (event_id,)
        )[0][0]

    assert priority_of("z|T1|C1|3000.1") == "self", "my own message is self-priority"
    assert priority_of("z|T1|C1|3000.2") == "direct", "a mention of me is direct"
    assert priority_of("z|T1|D1|3000.3") == "direct", "a DM is direct"
    assert priority_of("z|T1|C1|3000.4") == "noise", "bot posts are noise"
    assert priority_of("z|T1|C1|4000.2") == "direct", "a reply in my thread is direct"
    assert priority_of("z@x.test|f-mine") == "self", "my own drive edits are self"
    assert priority_of("z@x.test|f-shared") == "cc", "someone editing my file keeps me in the loop"
    assert priority_of("z|T1|C1|3000.5") == "noise", "a channel_join narrated by Slack is not his action"
    assert priority_of("z@x.test|am-biz") == "noise", "business/RCS senders are noise"
    assert priority_of("z@x.test|f-excluded") == "background", "warehouse-excluded drive blobs are background"
    assert priority_of("z@x.test|m-promo") == "noise", "promos are noise even when addressed to me"
    assert priority_of("z@x.test|m-noreply") == "noise", "broadcast senders are noise"
    assert priority_of("z@x.test|m-notify") == "noise", "pure machine mail is noise"
    assert priority_of("z@x.test|m-starred") == "direct", "starred email is direct"
    assert priority_of("openclaw|cron-sess") == "background", "cron heartbeat sessions are background"


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
        INSERT INTO @slack_conversations (account, team_id, conversation_id, name, is_member,
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
        INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
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
            _NOW + timedelta(hours=8), _NOW,
            _NOW, _NOW,
            _NOW, _NOW,
        ),
    )
    # A real <@id> ping in the 40k channel is aimed at him wherever it lands;
    # a message in a member channel days from any post of his is the public
    # firehose; and replies under his <!channel> announcement are a crowd
    # reacting to a broadcast, not people addressing him -- unless one of them
    # says his name.
    warehouse._command(
        """
        INSERT INTO @slack_conversations (account, team_id, conversation_id, name, is_member,
                                         num_members, is_mpim, is_private)
        VALUES ('z', 'T1', 'CANN', 'announcements', 1, 40000, 0, 0)
        """
    )
    warehouse._command(
        """
        INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts, thread_ts,
                                    message_datetime, user_id, text, reply_count, synced_at)
        VALUES ('z', 'T1', 'CBIG', '5000.10', '', %s, 'U1', 'hey <@UME> can you look at this', 0, %s),
               ('z', 'T1', 'C1', '5000.11', '', %s, 'U1', 'ambient member-channel chatter', 0, %s),
               ('z', 'T1', 'CANN', '6000.1', '6000.1', %s, 'UME', '<!channel> watch this video', 40, %s),
               ('z', 'T1', 'CANN', '6000.2', '6000.1', %s, 'U1', 'First', 0, %s),
               ('z', 'T1', 'CANN', '6000.3', '6000.1', %s, 'U1', 'zach can i still sign up?', 0, %s)
        """,
        (
            _NOW + timedelta(hours=8), _NOW,
            _NOW - timedelta(days=4), _NOW,
            _NOW - timedelta(hours=2), _NOW,
            _NOW - timedelta(hours=1), _NOW,
            _NOW - timedelta(hours=1), _NOW,
        ),
    )
    # A legacy integration posting with a username and no user account.
    warehouse._command(
        """
        INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
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
            INSERT INTO @gmail_messages (account, message_id, thread_id, internal_date,
                                        subject, from_address, to_addresses, synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            row,
        )
    # A personal reply in a thread someone else wrote in first stays his.
    warehouse._command(
        """
        INSERT INTO @gmail_messages (account, message_id, thread_id, internal_date,
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
        INSERT INTO @gmail_messages (account, message_id, thread_id, internal_date,
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
        INSERT INTO @gmail_messages (account, message_id, internal_date, subject, from_address,
                                    to_addresses, cc_addresses, snippet, synced_at)
        VALUES ('z@x.test', 'gh-mention', %s, 'Re: [org/repo] fix (PR #1)',
                'notifications@github.com', %s, %s, 'someone: @zach take a look', %s),
               ('z@x.test', 'gh-bot', %s, 'Re: [org/repo] bump deps (PR #2)',
                'notifications@github.com', %s, %s,
                'vercel[bot] left a comment (org/repo#2)', %s),
               ('z@x.test', 'gh-plain', %s, 'Re: [org/repo] discussion (Issue #3)',
                'notifications@github.com', %s, %s, 'a human wrote words here', %s),
               ('z@x.test', 'gh-bot-author', %s, 'Re: [org/repo] my feature (PR #4)',
                'notifications@github.com', %s, %s,
                '@coderabbitai[bot] commented on this pull request. In src/x.go:', %s),
               ('z@x.test', 'gh-state-author', %s, 'Re: [org/repo] my feature (PR #4)',
                'notifications@github.com', %s, %s, 'Merged #4 into main.', %s),
               ('z@x.test', 'gh-push', %s, 'Re: [org/repo] their feature (PR #5)',
                'notifications@github.com', %s, %s, '@someone pushed 1 commit. abc123 tidy', %s)
        """,
        (
            _NOW, ["z@x.test"], ["mention@noreply.github.com"], _NOW,
            _NOW, ["z@x.test"], ["push@noreply.github.com"], _NOW,
            _NOW, ["z@x.test"], ["subscribed@noreply.github.com"], _NOW,
            _NOW, ["z@x.test"], ["author@noreply.github.com"], _NOW,
            _NOW, ["z@x.test"], ["author@noreply.github.com"], _NOW,
            _NOW, ["z@x.test"], ["push@noreply.github.com"], _NOW,
        ),
    )
    warehouse._command(
        """
        INSERT INTO @gmail_messages (account, message_id, internal_date, subject, from_address,
                                    to_addresses, synced_at)
        VALUES ('z@x.test', 'm-otp', %s, 'Your login code: 123-456', 'human.sounding@bank.example', %s, %s),
               ('z@x.test', 'm-confirm-code', %s, '123456 is your confirmation code', 'human.sounding@example.test', %s, %s),
               ('z@x.test', 'm-rsvp', %s, 'Accepted: 1:1 @ Fri (owner)', 'colleague@example.test', %s, %s),
               ('z@x.test', 'm-outlook-cancel', %s, 'Canceled: Partner sync', 'partner@example.test', %s, %s),
               ('z@x.test', 'm-shipment-confirmation', %s, 'Shipment Confirmation', 'dinobox@example.test', %s, %s)
        """,
        (
            _NOW, ["z@x.test"], _NOW,
            _NOW, ["z@x.test"], _NOW,
            _NOW, ["z@x.test"], _NOW,
            _NOW, ["z@x.test"], _NOW,
            _NOW, ["z+recipient@x.test"], _NOW,
        ),
    )
    # A known correspondent whose mail Gmail mis-categorized as bulk.
    warehouse._command(
        """
        INSERT INTO @gmail_messages (account, message_id, internal_date, subject, from_address,
                                    to_addresses, label_ids, synced_at)
        VALUES ('z@x.test', 'm-corr', %s, 'travel receipts', 'friend@example.test', %s,
                %s, %s)
        """,
        (_NOW, ["z@x.test"], ["CATEGORY_UPDATES", "INBOX"], _NOW),
    )
    warehouse._command(
        """
        INSERT INTO @timeline_gmail_correspondents (addr, n_sent_to, last_sent_at, refreshed_at)
        VALUES ('friend@example.test', 12, %s, now())
        """,
        (_NOW,),
    )
    # Gmail's Forums bucket mixes real list discussion with broadcast digests
    # and newsletters. Individual human posts to work aliases remain cc, but
    # digest/newsletter/list-announcement shapes are noise.
    warehouse._command(
        """
        INSERT INTO @gmail_messages (account, message_id, internal_date, subject, from_address,
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
        INSERT INTO @apple_message_handles (account, handle_id, address)
        VALUES ('z@x.test', 'h-oneway', '+15559990000'),
               ('z@x.test', 'h-tollfree', '+18335551234'),
               ('z@x.test', 'h-group', '+15558887777')
        """
    )
    warehouse._command(
        """
        INSERT INTO @apple_message_chats (account, chat_id, display_name, style)
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
            INSERT INTO @apple_message_chat_messages (account, chat_id, message_id, message_date, ingested_at)
            VALUES ('z@x.test', %s, %s, %s, %s)
            """,
            (chat_id, mid, _NOW - timedelta(days=offset), _NOW),
        )
        warehouse._command(
            """
            INSERT INTO @apple_messages (account, message_id, handle_id, body_text, message_at,
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
            INSERT INTO @apple_message_chat_handles (account, chat_id, handle_id)
            VALUES ('z@x.test', 'c-biggroup', %s)
            """,
            (f"h-g{i}",),
        )
    warehouse._command(
        """
        INSERT INTO @apple_message_chat_messages (account, chat_id, message_id, message_date, ingested_at)
        VALUES ('z@x.test', 'c-biggroup', 'am-group-mine', %s, %s)
        """,
        (_NOW - timedelta(hours=2), _NOW),
    )
    warehouse._command(
        """
        INSERT INTO @apple_messages (account, message_id, handle_id, body_text, message_at,
                                    is_from_me, ingested_at)
        VALUES ('z@x.test', 'am-group-mine', '', 'on my way', %s, 1, %s)
        """,
        (_NOW - timedelta(hours=2), _NOW),
    )

    # --- whatsapp: business senders and E2E stubs ----------------------------
    warehouse._command(
        """
        INSERT INTO @whatsapp_contacts (account, jid, push_name, business_name)
        VALUES ('z@x.test', 'agent@lid', 'Agent', 'Agent Service')
        """
    )
    warehouse._command(
        """
        INSERT INTO @whatsapp_messages (account, chat_id, message_id, sender_jid, push_name,
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
        ("sdk-typed", "sdk-cli", [("user", "how did the freight break down for this?")]),
        ("cli-brief", "cli", [("user", "You are auditing the PDW repo at /tmp/x. Report findings.")]),
        ("empty-sess", "", []),
        ("desktop-conv", "", []),
    ):
        source = "claude_desktop" if sess == "desktop-conv" else "claude_code"
        # Writes go to the source-owned raw table; the unified view is read-only.
        events_table = f"@{source}_events"
        if not rows:
            warehouse._command(
                f"""
                INSERT INTO {events_table} (source, session_id, event_uuid, seq, occurred_at,
                                                  role, event_type, session_title, entrypoint, ingested_at)
                VALUES (%s, %s, 'meta0', 0, %s, 'meta', 'conversation', 'A titled conversation', %s, %s)
                """,
                (source, sess, _NOW, entrypoint, _NOW),
            )
        for seq, (role, text) in enumerate(rows):
            warehouse._command(
                f"""
                INSERT INTO {events_table} (source, session_id, event_uuid, seq, occurred_at,
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
            INSERT INTO @claude_code_events (source, session_id, event_uuid, seq, occurred_at,
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
            INSERT INTO @claude_code_events (source, session_id, event_uuid, seq, occurred_at,
                                              role, text, entrypoint, ingested_at)
            VALUES ('claude_code', %s, 'r0', 0, %s, 'user', %s, 'cli', %s)
            """,
            (f"routine-{day}", _NOW - timedelta(days=day), monitor_prompt, _NOW),
        )

    # --- calendar: feeds, promo invites, flighty ------------------------------
    warehouse._command(
        """
        INSERT INTO @calendar_events (account, calendar_id, event_id, summary, description,
                                     organizer_email, start_at, updated_at, synced_at)
        VALUES ('z@x.test', 'cal1', 'ev-feed', 'Vinyasa Flow', '',
                'studio_x1@group.calendar.google.com', %s, %s, %s),
               ('z@x.test', 'cal1', 'ev-promo', 'Free Ticket!', 'come along ͏ ­͏ ­',
                'random@gmail.example', %s, %s, %s),
               ('z@x.test', 'cal1', 'ev-flight', '✈ BTV→IAD • UA 4178', '',
                'z@x.test', %s, %s, %s),
               ('z@x.test', 'cal1', 'ev-invite', 'Coffee', '',
                'human@example.test', %s, %s, %s),
               ('z@x.test', 'cal1', 'ev-luma', 'AMA with a founder', '',
                'calendar-invite@lu.ma', %s, %s, %s)
        """,
        (_NOW, _NOW, _NOW) * 5,
    )

    # Google marks the owner's own attendee entry "self" and the organizer's
    # "organizer"; that pair identifies an event Zach set up even when
    # organizer_email is an alias. A meeting he declined is not attention owed,
    # and an invite alongside a crowd is activity he is peripheral to.
    warehouse._command(
        """
        INSERT INTO @calendar_events (account, calendar_id, event_id, summary, organizer_email,
                                     attendees_json, start_at, updated_at, synced_at)
        VALUES ('z@x.test', 'cal1', 'ev-alias', 'Board prep', 'alias@x.test',
                '[{"email": "alias@x.test", "self": true, "organizer": true}]', %s, %s, %s),
               ('z@x.test', 'cal1', 'ev-declined', 'Optional sync', 'human@example.test',
                '[{"email": "human@example.test", "organizer": true},
                  {"email": "z@x.test", "self": true, "responseStatus": "declined"}]',
                %s, %s, %s),
               ('z@x.test', 'cal1', 'ev-allhands', 'All hands', 'human@example.test',
                '[{"email": "human@example.test", "organizer": true},
                  {"email": "a@example.test"}, {"email": "b@example.test"},
                  {"email": "c@example.test"}, {"email": "d@example.test"},
                  {"email": "e@example.test"}, {"email": "f@example.test"},
                  {"email": "g@example.test"}, {"email": "h@example.test"},
                  {"email": "z@x.test", "self": true}]', %s, %s, %s),
               ('z@x.test', 'cal1', 'ev-room', 'Design review', 'human@example.test',
                '[{"email": "human@example.test", "organizer": true},
                  {"email": "room@resource.calendar.google.com", "resource": true},
                  {"email": "z@x.test", "self": true}]', %s, %s, %s)
        """,
        (_NOW, _NOW, _NOW) * 4,
    )

    # --- finance: his own spend vs money a machine moved ----------------------
    sync_version = int(_NOW.timestamp() * 1_000_000)
    warehouse._command(
        """
        INSERT INTO @finance_transactions (transaction_id, account_id, posted_at, amount,
                                          currency, description, merchant, pending, source,
                                          created_at, sync_version)
        VALUES ('ft-coffee', 'fa1', %s, -6.42, 'USD', 'VILLAGE WINE AND COFFEE', '', 0,
                'plaid', %s, %s),
               ('ft-sweep', 'fa1', %s, -3582.92, 'USD',
                'PURCHASE INTO CORE ACCOUNT FDIC INSURED DEPOSIT', '', 0, 'plaid', %s, %s),
               ('ft-interest', 'fa1', %s, 0.55, 'USD', 'Interest Earned', '', 0, 'plaid', %s, %s),
               ('ft-autopay', 'fa1', %s, -240.00, 'USD', 'CAPITAL ONE AUTOPAY PYMT', '', 0,
                'plaid', %s, %s)
        """,
        (_NOW, _NOW, sync_version) * 4,
    )

    # --- codex: the harness-injected opening turn is not a human prompt -------
    for session in ("codex-a", "codex-b", "codex-c", "codex-d"):
        warehouse._command(
            """
            INSERT INTO @codex_events (source, session_id, event_uuid, seq, occurred_at,
                                       role, text, entrypoint, ingested_at)
            VALUES ('codex', %s, 'p0', 0, %s, 'user', %s, 'codex_cli_rs', %s),
                   ('codex', %s, 'u1', 1, %s, 'user', %s, 'codex_cli_rs', %s)
            """,
            (
                session,
                _NOW,
                "<recommended_plugins>\nHere is a list of plugins that are available "
                "but not installed. Ask before installing any of them.",
                _NOW,
                session,
                _NOW,
                f"pull latest from main and keep going on {session}",
                _NOW,
            ),
        )

    # --- drive: form pipelines and shortcuts ---------------------------------
    warehouse._command(
        """
        INSERT INTO @google_drive_files (account, file_id, name, mime_type, folder_path,
                                        last_modifying_user, modified_time, ingested_at)
        VALUES ('z@x.test', 'f-form', 'logo - applicant.png', 'image/png',
                '/apps form/Application (File responses)/Upload Logo (File responses)',
                'applicant', %s, %s),
               ('z@x.test', 'f-shortcut', 'Old Report', 'application/vnd.google-apps.shortcut',
                '/My Drive', 'someone', %s, %s)
        """,
        (_NOW, _NOW, _NOW, _NOW),
    )
    # The three branches of drive_file's eight-way CASE that nothing exercised.
    # A multi-branch classifier with partial coverage is how a tier silently
    # moves: the untested branches are exactly the ones a refactor can drop.
    warehouse._command(
        """
        INSERT INTO @google_drive_files (account, file_id, name, mime_type, folder_path,
                                        owners_json, last_modifying_user, trashed, starred,
                                        modified_time, ingested_at)
        VALUES ('z@x.test', 'f-trashed', 'Deleted Plan', 'application/pdf', '/My Drive',
                '[{"emailAddress": "z@x.test", "displayName": "Zach"}]', 'Zach', 1, 0, %s, %s),
               ('z@x.test', 'f-starred', 'Someone Else Doc', 'application/pdf', '/My Drive',
                '[{"emailAddress": "other@x.test", "displayName": "Other"}]', 'Other', 0, 1, %s, %s),
               ('z@x.test', 'f-other', 'Their Doc', 'application/pdf', '/My Drive',
                '[{"emailAddress": "other@x.test", "displayName": "Other"}]', 'Other', 0, 0, %s, %s)
        """,
        (_NOW, _NOW, _NOW, _NOW, _NOW, _NOW),
    )

    engine = _engine(warehouse)
    try:
        engine.run()
    finally:
        engine.close()

    def priority_of(event_id: str) -> str:
        return warehouse._query(
            "SELECT priority FROM @timeline_events WHERE event_id = %s", (event_id,)
        )[0][0]

    # slack
    assert priority_of("z|T1|C1|5000.3") == "direct", "channel msg inside his two-post window is a conversation"
    assert priority_of("z|T1|CBIG|5000.5") == "cc", "one drive-by post does not promote a 40k channel"
    assert priority_of("z|T1|CBIG|5000.6") == "cc", "his name in public, outside his window, is people talking about him"
    assert priority_of("z|T1|CBIG|5000.10") == "direct", "a real <@id> ping is aimed at him anywhere"
    assert priority_of("z|T1|C1|5000.11") == "noise", "member-channel chatter he is nowhere near is the public firehose"
    assert priority_of("z|T1|CANN|6000.2") == "cc", "a reply under his <!channel> broadcast is a crowd reacting"
    assert priority_of("z|T1|CANN|6000.3") == "direct", "a reply under his broadcast that names him is aimed at him"
    assert priority_of("z|T1|G1|5000.7") == "cc", "a big group DM he is not engaged in is peripheral"
    assert priority_of("z|T1|G2|5000.8") == "direct", "small group DMs are attention"
    assert priority_of("z|T1|C1|5000.9") == "noise", "username-only legacy integrations are bots"
    # gmail
    assert priority_of("z@x.test|merge-20") == "noise", "mail-merge blast sends are not his actions"
    assert priority_of("z@x.test|my-reply") == "self", "a personal reply after inbound mail stays his"
    assert priority_of("z@x.test|m-replied") == "direct", "mail he answered within 48h has his attention"
    assert priority_of("z@x.test|gh-mention") == "direct", "github mention copies are direct"
    assert priority_of("z@x.test|gh-bot") == "noise", "relayed bot payloads are noise"
    assert priority_of("z@x.test|gh-plain") == "cc", "relayed human comments are cc"
    assert priority_of("z@x.test|gh-bot-author") == "noise", "a review bot on his own PR is still a machine"
    assert priority_of("z@x.test|gh-state-author") == "cc", "a merge of his PR is the platform reporting, not a person writing"
    assert priority_of("z@x.test|gh-push") == "noise", "push notifications on a watched repo are machinery"
    assert priority_of("z@x.test|m-outlook-cancel") == "cc", "Outlook-style Canceled: stubs are auto-RSVP notices"
    assert priority_of("z@x.test|m-otp") == "noise", "login codes are noise"
    assert priority_of("z@x.test|m-confirm-code") == "noise", "confirmation codes are noise"
    assert priority_of("z@x.test|m-rsvp") == "cc", "auto-RSVP notices are cc"
    assert priority_of("z@x.test|m-shipment-confirmation") == "noise", "shipment automations are noise"
    assert priority_of("z@x.test|m-corr") == "direct", "known correspondents beat gmail's bulk category"
    assert priority_of("z@x.test|m-forum-human") == "cc", "human work-list posts are cc"
    assert priority_of("z@x.test|m-forum-digest") == "noise", "forum digests are noise"
    assert priority_of("z@x.test|m-forum-newsletter") == "noise", "forum newsletters are noise"
    assert priority_of("z@x.test|m-forum-announcement") == "noise", "public-list announcements are noise"
    assert priority_of("z@x.test|m-forum-new-comment") == "cc", "automated human comment relays remain cc"
    assert priority_of("z@x.test|m-figma-upgrade") == "cc", "seat requests are human-action relays"
    assert priority_of("z@x.test|m-airtable-access") == "cc", "access requests are human-action relays"
    assert priority_of("z@x.test|m-sign-request") == "cc", "signature requests are human-action relays"
    assert priority_of("z@x.test|m-drive-share") == "cc", "drive shares are human-action relays"
    assert priority_of("z@x.test|m-vercel-access") == "cc", "app access requests are human-action relays"
    # apple
    assert priority_of("z@x.test|am-oneway") == "noise", "a 1:1 chat he never answers is a broadcast"
    assert priority_of("z@x.test|am-shortcode") == "noise", "shortcode-named group blasts are noise"
    assert priority_of("z@x.test|am-group-active") == "direct", "big group during his active window"
    assert priority_of("z@x.test|am-group-idle") == "cc", "big group outside his window is peripheral"
    # whatsapp
    assert priority_of("z@x.test|agent@lid|wm-agent") == "noise", "business/bot accounts are automated"
    assert priority_of("z@x.test|chat@g.us|wm-stub") == "noise", "contentless E2E stubs are noise"
    # agent sessions
    assert priority_of("claude_code|sdk-sess") == "background", "an output-format brief is a program talking"
    assert priority_of("claude_code|sdk-typed") == "self", "sdk-cli is also how paseo launches the sessions he types into"
    assert priority_of("claude_code|cli-brief") == "background", "a 'You are ...' role brief is orchestrator-written whatever the entrypoint"
    assert priority_of("claude_code|empty-sess") == "background", "zero-user-turn transcripts are machinery"
    assert priority_of("claude_desktop|desktop-conv") == "self", "desktop conversations are his even header-only"
    assert priority_of("claude_code|side-sess") == "background", "sidechain-only subagent transcripts are machinery"
    assert priority_of("claude_code|routine-0") == "background", "recurring template prompts are scheduled routines"
    # calendar
    assert priority_of("z@x.test|cal1|ev-feed") == "noise", "subscribed calendar feeds are noise"
    assert priority_of("z@x.test|cal1|ev-promo") == "noise", "promo-invite blasts are noise"
    assert priority_of("z@x.test|cal1|ev-flight") == "noise", "flighty auto-events are not his actions"
    assert priority_of("z@x.test|cal1|ev-invite") == "direct", "human invites are attention"
    assert priority_of("z@x.test|cal1|ev-luma") == "noise", "event-platform robots are not a person inviting him"
    assert priority_of("z@x.test|cal1|ev-alias") == "self", "the self+organizer attendee flags are him"
    assert priority_of("z@x.test|cal1|ev-declined") == "noise", "a meeting he declined is not attention"
    assert priority_of("z@x.test|cal1|ev-allhands") == "cc", "an invite alongside a crowd is peripheral"
    assert priority_of("z@x.test|cal1|ev-room") == "direct", "a booked room is not an extra attendee"
    # finance
    assert priority_of("ft-coffee") == "self", "a card purchase is money he chose to move"
    assert priority_of("ft-sweep") == "noise", "brokerage cash sweeps are automated movement"
    assert priority_of("ft-interest") == "noise", "interest is credited by the institution"
    assert priority_of("ft-autopay") == "noise", "autopay runs without him"
    # codex
    assert priority_of("codex|codex-a") == "self", (
        "the identical <recommended_plugins> preamble every codex session opens with "
        "must not read as a recurring scheduled prompt"
    )
    # drive
    assert priority_of("z@x.test|f-form") == "background", "form-response uploads are pipeline traffic"
    assert priority_of("z@x.test|f-shortcut") == "background", "shortcut churn is machinery"
    assert priority_of("z@x.test|f-trashed") == "noise", (
        "a trashed file is noise even though he owns and last touched it -- the "
        "trashed branch must win over the ownership branches below it"
    )
    assert priority_of("z@x.test|f-starred") == "cc", (
        "starring someone else's file is Zach asking to be kept in the loop"
    )
    assert priority_of("z@x.test|f-other") == "background", (
        "a file he neither owns nor starred changing is other people's work"
    )


def test_quality_regressions_for_recent_self_timeline_samples(warehouse):
    _ensure_all_source_tables(warehouse)
    now = datetime.now(tz=UTC)

    # Slack DMs should identify the other participant; attachment-only
    # message shells and inaccessible file stubs should not surface as self
    # activity, and stale file stubs should use the Slack message timestamp
    # rather than the sync timestamp.
    warehouse._command(
        """
        INSERT INTO @slack_users (account, team_id, user_id, display_name)
        VALUES ('z', 'T1', 'UME', 'self'),
               ('z', 'T1', 'U1', 'Teammate One'),
               ('z', 'T1', 'U2', 'Teammate Two')
        """
    )
    warehouse._command(
        """
        INSERT INTO @slack_account_identities (account, team_id, user_id)
        VALUES ('z', 'T1', 'UME')
        """
    )
    warehouse._command(
        """
        INSERT INTO @slack_conversations (account, team_id, conversation_id, name, is_im)
        VALUES ('z', 'T1', 'D1', '', 1),
               ('z', 'T1', 'D2', 'U2', 1)
        """
    )
    warehouse._command(
        """
        INSERT INTO @slack_conversation_members (account, team_id, conversation_id, user_id)
        VALUES ('z', 'T1', 'D1', 'UME'),
               ('z', 'T1', 'D1', 'U1')
        """
    )
    stale_message_ts = f"{(now - timedelta(hours=1)).timestamp():.6f}"
    warehouse._command(
        """
        INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
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
        INSERT INTO @slack_files (account, team_id, file_id, conversation_id, message_ts,
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
        INSERT INTO @whatsapp_contacts (account, jid, push_name)
        VALUES ('z@x.test', 'friend@lid', 'Saved Contact')
        """
    )
    warehouse._command(
        """
        INSERT INTO @whatsapp_messages (account, chat_id, message_id, sender_jid, body_text,
                                       message_kind, media_type, message_at, is_from_me, ingested_at)
        VALUES ('z@x.test', 'friend@lid', 'voice-1', '', '', 'voice', 'voice', %s, 1, %s)
        """,
        (now, now),
    )

    # Gmail should decode common HTML entities in timeline display fields.
    warehouse._command(
        """
        INSERT INTO @gmail_messages (account, message_id, internal_date, subject, from_address,
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
        INSERT INTO @apple_message_chat_messages (account, chat_id, message_id, message_date, ingested_at)
        VALUES ('z@x.test', 'chat-attach', 'im-attach', %s, %s)
        """,
        (now, now),
    )
    warehouse._command(
        """
        INSERT INTO @apple_messages (account, message_id, body_text, message_at,
                                    is_from_me, cache_has_attachments, ingested_at)
        VALUES ('z@x.test', 'im-attach', '￼', %s, 1, 1, %s)
        """,
        (now, now),
    )
    warehouse._command(
        """
        INSERT INTO @apple_message_attachments (account, attachment_id, message_id, filename,
                                               mime_type, is_missing, ingested_at)
        VALUES ('z@x.test', 'att-1', 'im-attach', '~/Library/Messages/Attachments/x/photo.jpg',
                'image/jpeg', 0, %s)
        """,
        (now,),
    )

    # Cancelled/deleted calendar events are noise. A meeting Zach set up is his
    # own action whether or not it has started yet: the tier describes who
    # organized it, and a past-window review bounds on event_ts, so a future
    # event cannot leak into one. Classifying not-yet-started events 'cc' froze
    # them there forever (no refresh window), which is why prod held 0 'self'
    # and 0 'direct' calendar rows in the future.
    warehouse._command(
        """
        INSERT INTO @calendar_events (account, calendar_id, event_id, summary, organizer_email,
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
        INSERT INTO @openclaw_events (source, session_id, event_uuid, seq, occurred_at,
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
        for row in warehouse._query_dicts("SELECT * FROM @timeline_events")
    }

    assert by_event_id["z|T1|D1|dm-1"]["context"] == "DM with Teammate One"
    assert by_event_id["z|T1|D2|dm-name-fallback"]["context"] == "DM with Teammate Two"
    assert by_event_id["z|T1|D1|dm-file-shell"]["priority"] == "background"
    stale_file = by_event_id[f"z|T1|FSTUB|D1|{stale_message_ts}"]
    assert stale_file["priority"] == "background"
    assert stale_file["event_ts"] < now - timedelta(minutes=30)

    whatsapp = by_event_id["z@x.test|friend@lid|voice-1"]
    assert whatsapp["context"] == "Saved Contact"
    assert whatsapp["snippet"] == "[voice message]"

    gmail = by_event_id["z@x.test|html-1"]
    assert gmail["title"] == "Re: <Plan>"
    assert gmail["snippet"] == "I'm ready & excited <3"

    imessage = by_event_id["z@x.test|im-attach"]
    assert imessage["snippet"] == "[attachment: photo.jpg]"

    assert by_event_id["z@x.test|primary|cancelled"]["priority"] == "noise"
    assert by_event_id["z@x.test|primary|future"]["priority"] == "self"
    assert by_event_id["openclaw|subagent-cron"]["priority"] == "background"


def test_calendar_collapses_duplicate_source_rows_to_the_newest(warehouse):
    """Prod's calendar heap holds rows its own primary key says cannot exist.

    Measured 2026-08-23: base_google_calendar.events has 17,141 rows but only
    16,996 distinct (account, calendar_id, event_id) byte-triples, despite a
    valid unique index on exactly those columns. Repairing the source table is
    a REINDEX, not an adapter change — and no event_id derivation can help,
    since concatenating or hashing three byte-identical columns yields the same
    16,996 keys. What the adapter owes is a deterministic choice: 30 of the 145
    duplicate pairs disagree on content, so with no tiebreak the timeline row
    for them flips with batch order and the content guard bumps seq — and
    re-chunks and re-embeds the event — on every sync. It keeps the newest
    copy.

    The constraint has to be dropped to reproduce this, which is only safe
    because the fixture runs in a throwaway schema.
    """
    _ensure_all_source_tables(warehouse)
    table_ref = warehouse.sql_relation("calendar_events")
    constraint = warehouse._query(
        "SELECT conname FROM pg_constraint WHERE conrelid = %s::regclass AND contype = 'p'",
        (table_ref,),
    )[0][0]
    warehouse._command(f'ALTER TABLE @calendar_events DROP CONSTRAINT "{constraint}"')
    warehouse._command(
        """
        INSERT INTO @calendar_events (account, calendar_id, event_id, summary, organizer_email,
                                     start_at, updated_at, synced_at, sync_version)
        VALUES ('z@x.test', 'primary', 'dup', 'Stale copy', 'z@x.test', %s, %s, %s, 1),
               ('z@x.test', 'primary', 'dup', 'Current copy', 'z@x.test', %s, %s, %s, 2)
        """,
        (_NOW, _NOW, _NOW, _NOW, _NOW, _NOW),
    )

    engine = _engine(warehouse, adapters=[adapter_by_name("calendar_event")])
    try:
        engine.run()
    finally:
        engine.close()

    rows = warehouse._query_dicts(
        "SELECT title FROM @timeline_events WHERE event_id = 'z@x.test|primary|dup'"
    )
    assert [row["title"] for row in rows] == ["Current copy"]


def test_voice_memo_timeline_refreshes_when_enrichment_arrives_later(warehouse):
    _ensure_all_source_tables(warehouse)
    adapter = adapter_by_name("voice_memo")
    warehouse._command(
        """
        INSERT INTO @apple_voice_memos_files (account, recording_id, title, filename,
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
        "SELECT title, snippet FROM @timeline_events "
        "WHERE event_id = 'apple_voice_memos|z@x.test|rec-late'"
    )[0]
    assert before["title"] == "20260709 raw title"
    assert before["snippet"] == ""

    warehouse._command(
        """
        INSERT INTO @apple_voice_memos_enrichments (source, account, recording_id, provider, model,
                                                   prompt_version, status, title, summary, created_at)
        VALUES ('apple_voice_memos', 'z@x.test', 'rec-late', 'p', 'm', 'v1', 'completed',
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
        "SELECT title, snippet FROM @timeline_events "
        "WHERE event_id = 'apple_voice_memos|z@x.test|rec-late'"
    )[0]
    assert after["title"] == "Readable memo title"
    assert after["snippet"] == "Readable memo summary"


def test_refresh_window_converges_late_signals(warehouse):
    """A chat message classified before Zach replied is upgraded once the
    refresh window re-walks it (his reply promotes the surrounding window)."""
    _ensure_all_source_tables(warehouse)
    now = datetime.now(tz=UTC)
    warehouse._command(
        "INSERT INTO @slack_account_identities (account, team_id, user_id) VALUES ('z', 'T1', 'UME')"
    )
    warehouse._command(
        """
        INSERT INTO @slack_conversations (account, team_id, conversation_id, name, is_member, num_members)
        VALUES ('z', 'T1', 'C9', 'work', 1, 30)
        """
    )
    warehouse._command(
        """
        INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
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
        "SELECT priority FROM @timeline_events WHERE event_id = 'z|T1|C9|9000.1'"
    )
    assert row[0][0] == "noise", "no engagement yet: public member-channel chatter is the firehose"

    # Zach replies twice; the original message predates the watermark so only
    # the refresh re-walk can reclassify it.
    warehouse._command(
        """
        INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
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
        "SELECT priority FROM @timeline_events WHERE event_id = 'z|T1|C9|9000.1'"
    )
    assert row[0][0] == "direct", "his replies retroactively promote the conversation window"


def test_coverage_reconcile_picks_the_stalest_adapter_not_the_first(warehouse):
    """The sweep must rotate by state, never by position in the adapter list.

    Its cost is the ingest window, not the gap count -- 24s for slack_message
    whether it repairs 62,891 rows or none -- so within one run's deadline only
    some adapters get swept. Walking the fixed adapter order would hand the
    budget to the same few every time and the tail would never reconcile, which
    is exactly how Slack's coverage rotation forfeited a stage's turn on every
    lock-skipped run and hid a three-month discovery outage. Order by
    last_reconcile_at so the one that has waited longest goes first.
    """

    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    engine = _engine(warehouse)
    try:
        engine.run()
        # Level the field first: an adapter whose backfill did not finish in
        # run 1 never reconciled, so it still sits at the epoch and would be
        # the stalest for reasons that have nothing to do with rotation.
        warehouse._command("UPDATE @timeline_sync_state SET last_reconcile_at = now()")
        stalest = engine._adapters[-1].name
        warehouse._command(
            "UPDATE @timeline_sync_state SET last_reconcile_at = %s WHERE adapter = %s",
            (datetime(1971, 1, 1, tzinfo=UTC), stalest),
        )
        swept: list[str] = []
        original = engine._run_coverage_reconcile

        def record(adapter, state, deadline):
            swept.append(adapter.name)
            return original(adapter, state, deadline)

        engine._run_coverage_reconcile = record  # type: ignore[method-assign]
        engine.run()
    finally:
        engine.close()

    assert swept, "no adapter was reconciled at all"
    assert swept[0] == stalest, (
        f"reconcile swept {swept[0]!r} first but {stalest!r} had waited longest; "
        "the rotation is positional, so the tail of the registry can starve"
    )


def test_no_adapter_declares_a_placeholder_priority_expression():
    """`priority_expression` must name a rule, not gesture at one.

    C2 requires every adapter to declare its classification deliberately, and
    the registration test only checks the field is non-empty. `agent_session`
    therefore shipped the literal string "CASE ... 'self' ... 'background' ...
    END" -- it mentions two real tiers, so a keyword check passes, while telling
    a reader nothing about when either applies. Ellipses are the tell.
    """

    from personal_data_warehouse.timeline import TIMELINE_ADAPTERS

    for adapter in TIMELINE_ADAPTERS:
        assert "..." not in adapter.priority_expression, (
            f"{adapter.name}: priority_expression is a placeholder, not a rule: "
            f"{adapter.priority_expression!r}"
        )


def test_every_adapter_run_stamps_a_heartbeat(warehouse):
    """A pass that legitimately had nothing to do must still say it ran.

    `_save_state` was reached only when rows were written, so `last_run_at` --
    and the `run_age_seconds` built on it in marts_ops.timeline_adapter_health --
    actually meant "last WROTE". A converged adapter and a stopped one were
    indistinguishable.
    """

    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    engine = _engine(warehouse)
    try:
        engine.run()
        warehouse._command("UPDATE @timeline_sync_state SET last_run_at = %s", (_EPOCH_TS,))
        # A second pass has no new rows for any adapter.
        engine.run()
    finally:
        engine.close()

    stale = warehouse._query_dicts(
        "SELECT adapter FROM @timeline_sync_state WHERE last_run_at = %s",
        (_EPOCH_TS,),
    )
    assert not stale, f"adapters ran but never stamped last_run_at: {[r['adapter'] for r in stale]}"


def test_coverage_reconcile_is_not_part_of_the_adapter_signature():
    """Adding the gap repair must not re-walk 48M rows to install itself.

    `adapter_signature` resets an adapter's backfill when it changes, and Slack
    alone owns 46.8M timeline rows -- a past re-walk grew `timeline.events` to
    93 GB. `reconcile_sql` changes no row's normalized content, so it must stay
    out of the signature payload, and so must the two tuning knobs beside it.
    """

    from dataclasses import replace

    from personal_data_warehouse.timeline import (
        TIMELINE_ADAPTERS,
        adapter_definition_signature,
    )

    for adapter in TIMELINE_ADAPTERS:
        baseline = adapter_definition_signature(adapter)
        mutated = replace(
            adapter,
            reconcile_sql="SELECT 'obviously different'",
            reconcile_hours=adapter.reconcile_hours + 24,
            incremental_lag_hours=adapter.incremental_lag_hours + 1,
        )
        assert adapter_definition_signature(mutated) == baseline, (
            f"{adapter.name}: reconcile/lag settings leaked into adapter_signature, "
            "which would reset every production backfill"
        )


def test_coverage_reconcile_repairs_a_gap_whatever_caused_it(warehouse):
    """Delete a synced row behind the engine's back; the next pass restores it.

    The reconcile pass exists so C1 does not depend on every other pass being
    correct. It asks the source which of its rows the timeline is missing, so
    it repairs a gap regardless of cause -- a lost watermark race, a crashed
    pass, a hand-run DELETE. Nothing else in the engine asks that question:
    `_run_refresh` re-walks a window to reconverge CONTENT and never notices an
    absent row.
    """

    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    engine = _engine(warehouse)
    try:
        engine.run()
        before = warehouse._query_dicts(
            "SELECT count(*) AS n FROM @timeline_events WHERE adapter = 'slack_message'"
        )[0]["n"]
        assert before > 0
        warehouse._command(
            "DELETE FROM @timeline_events WHERE adapter = 'slack_message'"
        )
        # Drive the reconcile pass directly. Through a full run() the
        # incremental lag window would restore these rows first, which proves
        # the belt works but says nothing about the braces.
        slack = next(a for a in engine._adapters if a.name == "slack_message")
        slack_state = engine._load_state(slack)
        # The cadence gate would otherwise skip this sweep: the run above just
        # stamped last_reconcile_at.
        slack_state.last_reconcile_at = datetime(1970, 1, 1, tzinfo=UTC)
        repaired = engine._run_coverage_reconcile(slack, slack_state, None)
    finally:
        engine.close()

    after = warehouse._query_dicts(
        "SELECT count(*) AS n FROM @timeline_events WHERE adapter = 'slack_message'"
    )[0]["n"]
    assert after == before, "reconcile did not restore rows missing from the timeline"
    assert repaired == before


def test_incremental_recovers_a_row_written_behind_the_watermark(warehouse):
    """A source row whose ingest_ts predates the stored watermark must still land.

    This is the loss class C1 was quietly failing, measured in production on
    2026-08-26: `base_slack.messages` held 26,217 rows in a settled one-day
    window (8->7 days ago) against 25,419 on the timeline -- 798 missing, 3.0%,
    across only 12 conversations. Every missing row carried a `synced_at`
    between 08-25 04:37 and 08-26 06:07 while `ops.timeline_sync_state` had
    `slack_message.watermark_ingest_ts = 2026-08-26 12:23:14`, i.e. hours to a
    day AHEAD of them.

    The engine walks `(ingest_ts, event_id)` strictly ascending, so a row that
    commits after the pass has read -- but carries an ingest stamp from before
    the watermark moved -- is skipped forever. `_run_refresh` cannot recover it
    either: that pass re-walks by EVENT time (slack_message keeps 12h), and the
    lost rows were 7-8 days old by event time. Permanent, silent loss.
    """

    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    engine = _engine(warehouse)
    try:
        engine.run()

        # The watermark now sits at _NOW. Write a row stamped BEHIND it, exactly
        # as a late-committing Slack backfill of a newly discovered channel does.
        behind = _NOW - timedelta(hours=2)
        warehouse._command(
            """
            INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
                                        message_datetime, user_id, text, synced_at)
            VALUES ('z', 'T1', 'C1', '3000.1', %s, 'U1', 'written behind the watermark', %s)
            """,
            (behind, behind),
        )
        engine.run()
    finally:
        engine.close()

    landed = warehouse._query_dicts(
        "SELECT event_id, snippet FROM @timeline_events "
        "WHERE adapter = 'slack_message' AND event_id = 'z|T1|C1|3000.1'"
    )
    assert landed, (
        "a source row stamped behind the watermark never reached the timeline; "
        "this is the permanent-loss class that cost 3.0% of a settled Slack window"
    )
    assert landed[0]["snippet"] == "written behind the watermark"


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
            INSERT INTO @slack_messages (account, team_id, conversation_id, message_ts,
                                        message_datetime, user_id, text, synced_at)
            VALUES ('z', 'T1', 'C1', '2000.1', %s, 'U1', 'newer message', %s)
            """,
            (later, later),
        )
        warehouse._command(
            "UPDATE @slack_messages SET text = 'slack says hi (edited)', synced_at = %s "
            "WHERE message_ts = '1000.1'",
            (later,),
        )
        old_seqs = {
            row["event_id"]: row["seq"]
            for row in warehouse._query_dicts("SELECT event_id, seq FROM @timeline_events")
        }
        stats = engine.run()
    finally:
        engine.close()

    by_adapter = {s.adapter: s for s in stats}
    assert by_adapter["slack_message"].incremental_rows == 2
    rows = warehouse._query_dicts(
        "SELECT event_id, snippet, seq FROM @timeline_events WHERE adapter = 'slack_message'"
    )
    by_id = {row["event_id"]: row for row in rows}
    assert len(by_id) == 2
    edited = by_id["z|T1|C1|1000.1"]
    assert edited["snippet"] == "slack says hi (edited)"
    assert edited["seq"] > old_seqs["z|T1|C1|1000.1"], "content change must bump seq"
    # Untouched rows keep their seq.
    gmail_id = "z@x.test|m1"
    gmail_seq = warehouse._query(
        "SELECT seq FROM @timeline_events WHERE event_id = %s", (gmail_id,)
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
            INSERT INTO @apple_message_attachments (account, attachment_id, message_id,
                                                   filename, content_sha256, ingested_at)
            VALUES ('z@x.test', 'att1', 'am1', 'marina.heic', 'sha-att1', %s)
            """,
            (later,),
        )
        warehouse._command(
            """
            INSERT INTO @file_attachment_enrichments (content_sha256, ai_provider, ai_model,
                                                     ai_prompt_version, text, updated_at)
            VALUES ('sha-att1', 'p', 'm', 'v1', 'a photo of the marina at sunset', %s)
            """,
            (later,),
        )
        engine.run()
    finally:
        engine.close()

    rows = warehouse._query(
        "SELECT search_text FROM @timeline_events WHERE event_id = 'z@x.test|am1'"
    )
    assert rows and "marina at sunset" in rows[0][0]


def test_backfill_pages_newest_first(warehouse):
    _ensure_all_source_tables(warehouse)
    for i in range(7):
        warehouse._command(
            """
            INSERT INTO @gmail_messages (account, message_id, internal_date, subject, synced_at)
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
            for row in warehouse._query("SELECT title FROM @timeline_events ORDER BY event_ts DESC")
        ]
        assert titles == ["mail 0", "mail 1", "mail 2"]
        state = warehouse._query_dicts("SELECT * FROM @timeline_sync_state")[0]
        assert state["backfill_done"] == 0
        # Finish the job with no budget cap.
        engine.run()
    finally:
        engine.close()
    count = warehouse._query("SELECT count(*) FROM @timeline_events")[0][0]
    assert count == 7
    state = warehouse._query_dicts("SELECT * FROM @timeline_sync_state")[0]
    assert state["backfill_done"] == 1
    assert state["backfill_rows"] == 7


def test_backfill_budget_throttles_the_rewalk_but_never_incremental_sync(warehouse):
    """`backfill_max_seconds` bounds only the history re-walk.

    A changed adapter re-walks every row it owns (slack alone is 46.8M), and
    the run budget let that spend 240 of every 300 seconds writing WAL faster
    than the archiver could ship it. The throttle is a separate, smaller
    budget for the backfill loop; incremental sync -- the part that keeps the
    timeline current -- is never throttled by it.
    """
    _ensure_all_source_tables(warehouse)
    # Ingested well before the incremental lag window, so only the backfill
    # re-walk can deliver these seven.
    for i in range(7):
        warehouse._command(
            """
            INSERT INTO @gmail_messages (account, message_id, internal_date, subject, synced_at)
            VALUES ('z@x.test', %s, %s, %s, %s)
            """,
            (f"m{i}", _NOW - timedelta(hours=i), f"mail {i}", _NOW - timedelta(days=10 + i)),
        )
    engine = _engine(warehouse, adapters=[adapter_by_name("gmail_email")], batch_size=3)
    try:
        engine.run(backfill_max_seconds=0.000001)
        landed = warehouse._query("SELECT count(*) FROM @timeline_events")[0][0]
        assert landed < 7

        # A zero backfill budget: the re-walk does not advance at all...
        warehouse._command(
            """
            INSERT INTO @gmail_messages (account, message_id, internal_date, subject, synced_at)
            VALUES ('z@x.test', 'm-new', %s, 'mail new', %s)
            """,
            (_NOW + timedelta(hours=1), _NOW + timedelta(hours=1)),
        )
        stats = engine.run(backfill_max_seconds=0)
        # ...but the new message still lands through incremental sync.
        assert stats[0].backfill_rows == 0
        assert stats[0].incremental_rows == 1
        assert warehouse._query("SELECT count(*) FROM @timeline_events")[0][0] == landed + 1
        state = warehouse._query_dicts("SELECT * FROM @timeline_sync_state")[0]
        assert state["backfill_done"] == 0

        engine.run()
    finally:
        engine.close()
    assert warehouse._query("SELECT count(*) FROM @timeline_events")[0][0] == 8


def test_engine_pumps_into_a_separate_destination_schema(warehouse):
    """The dev mode: source stays untouched, timeline lands elsewhere."""
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    dest_schema = make_test_schema("dest")
    engine = _engine(warehouse, dest_schema=dest_schema)
    try:
        engine.run()
        with engine._dest_conn.cursor() as cursor:
            cursor.execute(engine._dest_sql("SELECT count(*) FROM @timeline_events"))
            count = cursor.fetchone()[0]
        assert count == sum(EXPECTED_SEEDED_EVENTS.values())
        # Nothing was written into the source schema.
        assert not warehouse._query(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = 'events' LIMIT 1",
            (warehouse.physical_schema_name("timeline"),),
        ) or warehouse._query("SELECT count(*) FROM @timeline_events")[0][0] == 0
    finally:
        with engine._dest_conn.cursor() as cursor:
            for schema_name in physical_schema_names(namespace=dest_schema, include_hidden=True) + [dest_schema]:
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
        max_ingest_sql="SELECT max(synced_at) FROM @gmail_messages",
        priority_expression="'direct'",
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


def test_null_priority_fails_the_adapter_and_is_visible_in_health(warehouse):
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    select = """
        SELECT
            'null-priority' AS event_id, 'gmail' AS source, 'email' AS kind,
            m.internal_date AS event_ts, '1970-01-01'::timestamptz AS end_ts,
            '' AS actor, '' AS title, '' AS snippet, '' AS context,
            m.message_id::text AS source_pk, '{}'::jsonb::text AS metadata,
            '' AS search_text, m.synced_at AS ingest_ts, NULL::text AS priority
        FROM @gmail_messages m
        WHERE m.message_id = 'm1'
    """
    broken = adapter_by_name("gmail_email").__class__(
        name="null_priority",
        source_table="gmail_messages",
        source="gmail",
        kind="email",
        backfill_sql=select
        + " AND m.internal_date <= %(cursor_ts)s"
        + " AND (m.internal_date, 'null-priority') < (%(cursor_ts)s, %(cursor_id)s)"
        + " ORDER BY 4 DESC, 1 DESC LIMIT %(limit)s",
        incremental_sql=select
        + " AND m.synced_at >= %(watermark_ts)s"
        + " AND (m.synced_at, 'null-priority') > (%(watermark_ts)s, %(watermark_id)s)"
        + " ORDER BY 13, 1 LIMIT %(limit)s",
        max_ingest_sql="SELECT max(synced_at) FROM @gmail_messages",
        priority_expression="NULL::text",
    )
    engine = _engine(warehouse, adapters=[broken])
    try:
        with pytest.raises(TimelineSyncError) as excinfo:
            engine.run()
    finally:
        engine.close()
    assert "invalid priority None" in excinfo.value.stats[0].error
    assert warehouse._query("SELECT count(*) FROM @timeline_events")[0][0] == 0

    warehouse.ensure_pipeline_health_tables()
    health = warehouse._query_dicts(
        "SELECT status, last_error FROM @marts_timeline_adapter_health "
        "WHERE adapter = 'null_priority'"
    )[0]
    assert health["status"] == "failing"
    assert "invalid priority" in health["last_error"]


def test_prune_removes_orphans_but_never_touches_an_append_only_adapter(warehouse):
    """A reconciled source re-keys its rows; the timeline must follow it down.

    derived_finance.transactions is rebuilt and re-deduplicated every run, so a
    transaction_id it stops issuing leaves a timeline row nothing upstream can
    correct. Measured in production 2026-08-23: 19,316 finance_transaction rows
    against 14,372 live transactions, 4,944 orphans (25.6%) that search
    returned alongside their live replacements.

    The same pass must be inert for append-only adapters, whose bounded
    incremental queries legitimately do not re-return rows they already synced.
    """
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    # The prune refuses to delete a large share of an adapter, so a two-row
    # fixture can only ever exercise the guard. Seed a realistic population so
    # a single orphan is the small minority the cap is designed to allow.
    for i in range(200):
        warehouse._command(
            """
            INSERT INTO @finance_transactions (transaction_id, account_id, posted_at, amount,
                                              currency, description, merchant, pending, source,
                                              created_at, sync_version)
            VALUES (%s, 'fa1', %s, -1.00, 'USD', 'Bulk', 'Bulk', 0, 'plaid', %s, %s)
            """,
            (f"ft-bulk-{i}", _NOW - timedelta(hours=20), _NOW, 1),
        )
    engine = _engine(warehouse)
    engine.run()

    def _timeline_ids(adapter: str) -> set[str]:
        rows = warehouse._query(
            "SELECT event_id FROM @timeline_events WHERE adapter = %s", (adapter,)
        )
        return {row[0] for row in rows}

    assert "ft1" in _timeline_ids("finance_transaction")
    slack_before = _timeline_ids("slack_message")
    assert slack_before, "fixture must produce slack rows for the append-only half"

    # Re-key the transaction the way a re-dedup does: the old id is simply
    # gone, and nothing upstream will ever mention it again.
    warehouse._command(
        "UPDATE @finance_transactions SET transaction_id = 'ft1-rekeyed' "
        "WHERE transaction_id = 'ft1'"
    )
    _engine(warehouse).run()

    finance_after = _timeline_ids("finance_transaction")
    assert "ft1" not in finance_after, "the orphaned row survived the prune"
    assert "ft1-rekeyed" in finance_after, "the live replacement was not synced"
    assert _timeline_ids("slack_message") == slack_before, (
        "an append-only adapter must never lose rows to the prune pass"
    )


def test_prune_refuses_rather_than_empty_an_adapter(warehouse):
    """A runaway prune is worse than the orphans it removes; there is no undo.

    An authoritative query that returns nothing looks identical to one that is
    broken, mid-rebuild, or joined wrong. Refuse loudly instead of deleting.
    """
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    engine = _engine(warehouse)
    engine.run()
    before = warehouse._query(
        "SELECT count(*) FROM @timeline_events WHERE adapter = 'finance_transaction'"
    )[0][0]
    assert before > 0

    warehouse._command("DELETE FROM @finance_transactions")
    _engine(warehouse).run()

    after = warehouse._query(
        "SELECT count(*) FROM @timeline_events WHERE adapter = 'finance_transaction'"
    )[0][0]
    assert after == before, (
        "an empty authoritative set must refuse the prune, not delete every row"
    )


def test_prune_clamps_a_large_backlog_instead_of_declining_it(warehouse):
    """A real backlog is legitimately large; refusing it outright never converges.

    Production carried 4,944 orphans against 19,316 finance_transaction rows
    (25.6%) when the prune shipped. A hard cap would have declined that exact
    cleanup forever. Deleting at most a tenth per run converges it in a few
    passes while keeping any single mistake bounded.
    """
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    for i in range(200):
        warehouse._command(
            """
            INSERT INTO @finance_transactions (transaction_id, account_id, posted_at, amount,
                                              currency, description, merchant, pending, source,
                                              created_at, sync_version)
            VALUES (%s, 'fa1', %s, -1.00, 'USD', 'Bulk', 'Bulk', 0, 'plaid', %s, %s)
            """,
            (f"ft-mass-{i}", _NOW - timedelta(hours=20), _NOW, 1),
        )
    _engine(warehouse).run()

    def _count() -> int:
        return warehouse._query(
            "SELECT count(*) FROM @timeline_events WHERE adapter = 'finance_transaction'"
        )[0][0]

    total = _count()
    # Orphan a third of them: far past the per-run cap, but a legitimate backlog.
    warehouse._command(
        "UPDATE @finance_transactions SET transaction_id = 'gone-' || transaction_id "
        "WHERE transaction_id LIKE 'ft-mass-%' "
        "AND replace(transaction_id, 'ft-mass-', '')::int < 60"
    )

    first = _count()
    _engine(warehouse).run()
    after_one = _count()
    assert after_one < first, "a clamped prune must still make progress"

    for _ in range(30):
        _engine(warehouse).run()
    settled = _count()

    # The contract is that no timeline row outlives its source row. Comparing
    # raw counts would be wrong here: the re-keyed rows are still live under
    # new ids and the incremental watermark has not seen them yet (the rename
    # did not bump sync_version), so the timeline legitimately lags them.
    orphans = warehouse._query(
        """
        SELECT count(*) FROM @timeline_events e
        WHERE e.adapter = 'finance_transaction'
          AND NOT EXISTS (
              SELECT 1 FROM @finance_transactions t WHERE t.transaction_id = e.event_id
          )
        """
    )[0][0]
    assert orphans == 0, f"repeated runs must drain the backlog; {orphans} orphans remain"
    assert settled < total, "the orphans were never removed"


def test_a_retired_adapters_rows_are_removed_not_stranded(warehouse):
    """Removing an adapter must remove its rows, or they answer forever.

    timeline.events is keyed (adapter, event_id), and `prune_sql` only ever
    reconciles an adapter that still runs. So dropping `alice_voice_recording`
    from the registry when it was folded into `voice_memo` would have left its
    53 rows in the read surface for every agent, duplicating the rows that
    superseded them, with nothing in the engine able to notice.
    """
    _ensure_all_source_tables(warehouse)
    warehouse._command(
        """
        INSERT INTO @timeline_events
            (adapter, event_id, source, kind, priority, event_ts, ingest_ts, title,
             search_text, source_table, source_pk, metadata, seq)
        VALUES ('alice_voice_recording', 'z@x.test|gone', 'alice_voice_recordings',
                'voice_recording', 'self', %s, %s, 'stranded', 'stranded',
                'alice_voice_recordings', '{}'::jsonb, '{}'::jsonb, 1)
        """,
        (_NOW, _NOW),
    )
    warehouse._command(
        """
        INSERT INTO @timeline_sync_state (adapter) VALUES ('alice_voice_recording')
        """
    )

    engine = _engine(warehouse, adapters=[adapter_by_name("voice_memo")])
    try:
        engine.run()
    finally:
        engine.close()

    assert warehouse._query(
        "SELECT count(*) FROM @timeline_events WHERE adapter = 'alice_voice_recording'"
    ) == [(0,)]
    assert warehouse._query(
        "SELECT count(*) FROM @timeline_sync_state WHERE adapter = 'alice_voice_recording'"
    ) == [(0,)]


def test_a_live_adapter_is_never_retired_by_the_cleanup(warehouse):
    """The registry only ever names adapters that are actually gone.

    A live adapter listed there would have its whole history deleted on the
    next run, so the engine refuses on the registry's own contents rather than
    trusting whoever edited it.
    """
    live = {adapter.name for adapter in TIMELINE_ADAPTERS}
    assert set(RETIRED_TIMELINE_ADAPTERS).isdisjoint(live)
