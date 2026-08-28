"""Pipeline freshness: registry coverage, probe policy, and the live snapshot.

The coverage tests here are the twin of ``tests/test_timeline.py``: the timeline
guarantees every warehouse table is *represented*, and these guarantee every
warehouse table is *monitored*. A new source that skips either registry fails the
suite instead of shipping invisible.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.pipeline_health import (
    COLLECTOR_STALE_SECONDS,
    DATA_BASIS_REQUIRED_ABOVE,
    EXPENSIVE_MART_VIEWS,
    INHERIT_DATA_INTERVAL,
    LATE_MULTIPLIER,
    MART_PROBE_STATEMENT_TIMEOUT_MS,
    PIPELINE_KINDS,
    PIPELINES,
    PROBE_EMPTY,
    PROBE_MAX_UNINDEXED_BYTES,
    PROBE_MISSING,
    PROBE_NO_TIMESTAMP,
    PROBE_OK,
    PROBE_SKIPPED_EXPENSIVE,
    PROBE_SKIPPED_UNINDEXED,
    PROBE_STATEMENT_TIMEOUT_MS,
    STALE_MULTIPLIER,
    TABLE_PIPELINES,
    TABLE_ROLES,
    PipelineHealthCollector,
    mart_view_ids,
    pipeline,
    pipeline_tables,
)
from personal_data_warehouse.postgres import POSTGRES_INDEXES, POSTGRES_TABLES, PostgresWarehouse
from personal_data_warehouse.relations import CANONICAL_RELATIONS, CATALOG, relation
from personal_data_warehouse.schema import voice_memo_transcription_failure_status
from personal_data_warehouse.timeline import RAW_DDL_TABLES, TIMELINE_TABLE_COVERAGE


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


# --- coverage registry (pure) -------------------------------------------------


def test_every_registered_table_has_a_pipeline():
    registered = set(POSTGRES_TABLES) | set(RAW_DDL_TABLES)
    classified = set(TABLE_PIPELINES)
    assert registered - classified == set(), (
        "warehouse tables with no pipeline classification; add them to "
        "TABLE_PIPELINES so the freshness dashboard covers them"
    )
    assert classified - registered == set(), "pipeline classifications for unknown tables"


def test_pipeline_and_timeline_registries_cover_the_same_tables():
    """The two registries must not drift apart.

    Adding a table to one and not the other is the exact failure mode both
    guards exist to prevent, so they check each other as well as the schema.
    """
    assert set(TABLE_PIPELINES) == set(TIMELINE_TABLE_COVERAGE)


def test_table_pipelines_reference_declared_pipelines():
    declared = {entry.id for entry in PIPELINES}
    named = {coverage.pipeline for coverage in TABLE_PIPELINES.values()}
    assert named <= declared, f"tables name undeclared pipelines: {sorted(named - declared)}"
    assert declared == named, f"declared pipelines with no tables: {sorted(declared - named)}"


def test_pipeline_ids_are_unique_and_resolvable():
    ids = [entry.id for entry in PIPELINES]
    assert len(ids) == len(set(ids))
    for entry_id in ids:
        assert pipeline(entry_id).id == entry_id
    with pytest.raises(KeyError):
        pipeline("nope")


def test_every_pipeline_has_a_payload_table():
    """Data freshness is measured from ``data`` tables only.

    A pipeline whose tables are all dimensions or cursors could never report
    "nothing has arrived", which is the one thing the dashboard must be able to
    say.
    """
    for entry in PIPELINES:
        assert pipeline_tables(entry.id, role="data"), f"{entry.id} has no data table"


def test_pipeline_metadata_is_complete():
    for entry in PIPELINES:
        assert entry.kind in PIPELINE_KINDS, entry.id
        assert entry.label.strip(), entry.id
        # Cadence and transport are what turn a red row into an action: they name
        # where the data comes from and how often to expect it.
        assert entry.cadence.strip(), entry.id
        assert entry.transport.strip(), entry.id
        for interval in (entry.expected_data_interval, entry.expected_run_interval):
            assert interval is None or interval > timedelta(0), entry.id
        # A run interval without a heartbeat table would be permanently 'no_data'.
        if entry.expected_run_interval is not None:
            assert entry.state is not None, entry.id
        if entry.state is not None:
            # A scoped StateSource reads a SHARED heartbeat table that belongs
            # to another pipeline (ops.uploader_heartbeats holds every remote
            # uploader's runs); an unscoped one must own a state table.
            if entry.state.scope_column:
                assert entry.state.table in TABLE_PIPELINES, entry.id
            else:
                # Either a dedicated state table, or one of the pipeline's own
                # tables doubling as its failure record (transcription runs).
                owned = {table for table, cov in TABLE_PIPELINES.items() if cov.pipeline == entry.id}
                assert entry.state.table in owned, entry.id


def test_table_coverage_roles_and_columns_are_valid():
    for table, coverage in TABLE_PIPELINES.items():
        assert coverage.role in TABLE_ROLES, table
        spec = POSTGRES_TABLES.get(table)
        if spec is None:
            # Raw-DDL tables are checked against the live schema below.
            continue
        for column in (coverage.written_at, coverage.event_at):
            if column is None:
                continue
            assert column in spec.columns, f"{table}.{column} is not a column of that table"


def test_tables_without_a_timestamp_explain_themselves():
    for table, coverage in TABLE_PIPELINES.items():
        if coverage.written_at is None and coverage.event_at is None:
            assert coverage.note, f"{table} declares no timestamp column without saying why"


def test_state_sources_name_cataloged_tables_with_their_columns():
    for entry in PIPELINES:
        source = entry.state
        if source is None:
            continue
        assert source.table in CANONICAL_RELATIONS, entry.id
        spec = POSTGRES_TABLES.get(source.table)
        if spec is None:
            continue
        for column in (source.updated_column, source.status_column, source.error_column):
            if column:
                assert column in spec.columns, f"{entry.id}: {source.table}.{column}"


def test_chatgpt_pipeline_surfaces_expired_credentials_as_action_required():
    chatgpt = next(entry for entry in PIPELINES if entry.id == "chatgpt")
    assert chatgpt.state is not None
    assert chatgpt.state.table == "chatgpt_sessions"
    assert chatgpt.state.updated_column == "updated_at"
    assert chatgpt.state.status_column == "status"
    assert chatgpt.state.error_column == "error"
    assert "action_required" in chatgpt.state.attention_statuses


def test_pipeline_tables_are_catalog_ids():
    for table in TABLE_PIPELINES:
        assert table in CANONICAL_RELATIONS, table


def test_the_pipelines_data_tables_are_cheaply_probeable():
    """Each pipeline needs at least one payload table an index can answer.

    The collector refuses to run ``max()`` over a large unindexed heap, so a
    pipeline whose only data table is both huge and unindexed would report no
    freshness at all. The timeline is the deliberate exception: 43M rows with no
    updated_at index, monitored through its per-adapter sync state instead.
    """
    leading_indexed: set[tuple[str, str]] = set()
    for index in POSTGRES_INDEXES:
        # "(column)" or "(column DESC" / "(column," — the leading key column.
        opening = index.sql.find("(", index.sql.find(" ON "))
        if opening == -1:
            continue
        leading = index.sql[opening + 1 :].split(",")[0].split(")")[0].split()[0]
        leading_indexed.add((index.table, leading))

    exempt = {"timeline"}
    for entry in PIPELINES:
        if entry.id in exempt:
            continue
        indexed_data_tables = [
            table
            for table in pipeline_tables(entry.id, role="data")
            if (table, TABLE_PIPELINES[table].written_at or "") in leading_indexed
            or table not in _LARGE_TABLES
        ]
        assert indexed_data_tables, (
            f"{entry.id} has no cheaply probeable data table; index its written_at "
            "column or the dashboard cannot measure it"
        )


# Tables measured in production at over PROBE_MAX_UNINDEXED_BYTES. Kept as an
# explicit list so the guard above stays a pure test; the collector itself reads
# real sizes from pg_class at runtime.
_LARGE_TABLES = {
    "slack_messages",
    "slack_message_reactions",
    "gmail_messages",
    "gmail_attachments",
    "timeline_events",
    "file_attachment_enrichments",
    "apple_messages",
    "apple_message_chat_messages",
    "google_drive_files",
    "codex_events",
    "claude_code_events",
    "slack_sync_state",
}


def test_state_status_vocabulary_covers_both_failure_dialects():
    """Every sync writer records a hard failure as 'error' or 'failed'.

    The writers never agreed on one word — slack_sync writes ``error`` while
    whoop_sync, calendar_sync, contacts_sync, and google_drive_source_sync
    write ``failed`` — so every StateSource with a status column must classify
    *both*. A dialect the collector does not classify is a pipeline that can
    die invisibly: on 2026-07-30 WHOOP reported 'ok' on the dashboard through
    26 hours of hard failure because its ``failed`` rows matched nothing.
    """
    for entry in PIPELINES:
        state = entry.state
        if state is None or not state.status_column:
            continue
        assert "error" in state.error_statuses, (
            f"{entry.id}: 'error' missing from error_statuses"
        )
        assert "failed" in state.error_statuses, (
            f"{entry.id}: 'failed' missing from error_statuses"
        )


def test_benign_statuses_are_never_classified_as_failures():
    """'ok', slack's 'gone' tombstones, and plaid's 'unsupported' products are
    working-as-designed states, not failures, and must never count toward the
    dashboard's error or attention totals."""
    for entry in PIPELINES:
        state = entry.state
        if state is None:
            continue
        for benign in ("ok", "gone", "unsupported"):
            assert benign not in state.error_statuses, (entry.id, benign)
            assert benign not in state.attention_statuses, (entry.id, benign)


def test_plaid_action_required_is_surfaced_as_attention():
    """An expired Plaid Item login stays green at the run level by design; the
    dashboard's attention state is the only place it becomes visible."""
    state = pipeline("plaid").state
    assert state is not None
    assert "action_required" in state.attention_statuses


def test_whoop_action_required_remains_an_operator_visible_attention_state():
    """Skipped retry ticks must not make a rejected WHOOP credential look healthy."""
    state = pipeline("whoop").state
    assert state is not None
    assert "action_required" in state.attention_statuses
    assert "re-author" in pipeline("whoop").note


def test_thresholds_are_ordered_and_bounded():
    assert 1 < LATE_MULTIPLIER < STALE_MULTIPLIER
    # A missed collection must not turn the whole dashboard 'unknown': the asset
    # runs every ten minutes.
    assert COLLECTOR_STALE_SECONDS >= 1800
    assert 0 < PROBE_STATEMENT_TIMEOUT_MS <= 30_000
    assert PROBE_MAX_UNINDEXED_BYTES >= 64 * 1024 * 1024


def test_probe_reason_is_recorded_for_skipped_tables():
    collector = PipelineHealthCollector(_FakeWarehouse())
    probeable, reason = collector._probeable(
        "base_slack", "message_reactions", "synced_at", 2 * PROBE_MAX_UNINDEXED_BYTES, set()
    )
    assert probeable is False
    assert "no index" in reason and "MiB" in reason
    assert collector._probeable(
        "base_slack", "message_reactions", "synced_at", 2 * PROBE_MAX_UNINDEXED_BYTES,
        {("base_slack", "message_reactions", "synced_at")},
    ) == (True, "")
    assert collector._probeable("base_x", "y", "synced_at", 1024, set()) == (True, "")


class _FakeWarehouse:
    """Enough of PostgresWarehouse for the pure probe-policy tests."""

    schema_namespace = "public"

    def physical_schema_names(self, *, include_hidden: bool = False) -> list[str]:
        return []

    def _query(self, sql, params=None):
        return []

    def _query_dicts(self, sql, params=None):
        return []

    def _raw_command(self, sql, params=None):
        return None


# --- live schema + collector (Postgres) ---------------------------------------


def _provision_every_table(wh: PostgresWarehouse) -> None:
    wh.ensure_tables()
    wh.ensure_calendar_tables()
    wh.ensure_contacts_tables()
    wh.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    wh.ensure_alice_voice_recordings_tables()
    wh.ensure_apple_notes_tables()
    wh.ensure_apple_messages_tables()
    wh.ensure_whatsapp_tables()
    wh.ensure_whatsapp_client_session_table()
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


def test_declared_columns_exist_in_the_live_schema(warehouse):
    """Covers the raw-DDL tables the pure column test cannot check."""
    _provision_every_table(warehouse)
    live: dict[tuple[str, str], set[str]] = {}
    for schema, table, column in warehouse._query(
        """
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = ANY(%s)
        """,
        (warehouse.physical_schema_names(include_hidden=True),),
    ):
        live.setdefault((schema, table), set()).add(column)

    missing: list[str] = []
    for table_id, coverage in TABLE_PIPELINES.items():
        rel = relation(table_id).with_namespace(warehouse.schema_namespace)
        columns = live.get((rel.schema, rel.name))
        assert columns is not None, f"{table_id} was not provisioned ({rel.schema}.{rel.name})"
        for column in (coverage.written_at, coverage.event_at):
            if column and column not in columns:
                missing.append(f"{table_id}.{column}")
    assert missing == [], f"declared freshness columns that do not exist: {missing}"


def test_live_schema_has_no_unmonitored_tables(warehouse):
    _provision_every_table(warehouse)
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
        if logical in TABLE_PIPELINES
    }
    live = {physical_to_logical.get((schema, table), f"{schema}.{table}") for schema, table in rows}
    unmonitored = live - set(TABLE_PIPELINES)
    assert unmonitored == set(), (
        "tables exist in canonical warehouse schemas without a pipeline classification; "
        "add them to TABLE_PIPELINES (and a Pipeline if they belong to a new one): "
        f"{sorted(unmonitored)}"
    )


def test_ensure_pipeline_health_tables_is_idempotent(warehouse):
    warehouse.ensure_pipeline_health_tables()
    warehouse.ensure_pipeline_health_tables()
    for logical in ("pipeline_health", "pipeline_table_freshness"):
        rel = relation(logical).with_namespace(warehouse.schema_namespace)
        assert warehouse._physical_table_exists(schema=rel.schema, table=rel.name)
    assert warehouse._query("SELECT count(*) FROM @marts_pipeline_health")[0][0] == 0
    assert warehouse._query("SELECT count(*) FROM @marts_pipeline_table_freshness")[0][0] == 0


def test_collector_writes_a_row_for_every_pipeline_and_table(warehouse):
    _provision_every_table(warehouse)
    pipelines, tables = PipelineHealthCollector(warehouse).run()
    assert {entry.pipeline for entry in pipelines} == {entry.id for entry in PIPELINES}
    assert {entry.table_id for entry in tables} == set(TABLE_PIPELINES)

    rows = {
        row["pipeline"]: row
        for row in warehouse._query_dicts(
            "SELECT pipeline, label, kind, cadence, transport, status, data_status, run_status,"
            " table_count, tables_probed, collected_at, expected_data_interval_seconds"
            " FROM @marts_pipeline_health"
        )
    }
    assert set(rows) == {entry.id for entry in PIPELINES}
    gmail = rows["gmail"]
    assert gmail["label"] == "Gmail"
    assert gmail["kind"] == "source"
    assert gmail["table_count"] == len(pipeline_tables("gmail"))
    assert gmail["collected_at"] is not None
    # Every table is empty in a fresh schema, so nothing can be 'ok' yet.
    assert gmail["status"] == "no_data"

    table_rows = warehouse._query_dicts(
        "SELECT table_id, pipeline, role, probe_status, table_schema, table_name"
        " FROM @marts_pipeline_table_freshness"
    )
    assert len(table_rows) == len(TABLE_PIPELINES)
    by_id = {row["table_id"]: row for row in table_rows}
    assert by_id["gmail_messages"]["probe_status"] == PROBE_EMPTY
    assert by_id["gmail_messages"]["table_schema"] == relation("gmail_messages").with_namespace(
        warehouse.schema_namespace
    ).schema
    # The one table with no timestamp column says so instead of looking stale.
    assert by_id["search_schema_state"]["probe_status"] == PROBE_NO_TIMESTAMP
    assert PROBE_MISSING not in {row["probe_status"] for row in table_rows}


def test_collector_measures_real_writes_and_derives_status(warehouse):
    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    warehouse._command(
        """
        INSERT INTO @gmail_messages (account, message_id, synced_at, internal_date)
        VALUES ('z@x.test', 'm1', %s, %s)
        """,
        (now - timedelta(minutes=3), now - timedelta(minutes=9)),
    )
    warehouse._command(
        """
        INSERT INTO @gmail_sync_state (account, status, error, updated_at)
        VALUES ('z@x.test', 'ok', '', %s)
        """,
        (now - timedelta(minutes=2),),
    )
    PipelineHealthCollector(warehouse).run()

    row = warehouse._query_dicts(
        "SELECT status, data_status, run_status, last_write_at, newest_event_at, last_run_at,"
        " data_age_seconds, state_rows, state_error_rows, last_error, row_estimate"
        " FROM @marts_pipeline_health WHERE pipeline = 'gmail'"
    )[0]
    assert row["status"] == "ok"
    assert row["data_status"] == "ok"
    assert row["run_status"] == "ok"
    assert row["last_write_at"] is not None
    assert row["newest_event_at"] is not None
    assert row["last_run_at"] is not None
    assert 0 <= row["data_age_seconds"] < 600
    assert row["state_rows"] == 1
    assert row["state_error_rows"] == 0
    assert row["last_error"] is None

    table_row = warehouse._query_dicts(
        "SELECT probe_status, last_write_at, newest_event_at, written_at_column, event_at_column"
        " FROM @marts_pipeline_table_freshness WHERE table_id = 'gmail_messages'"
    )[0]
    assert table_row["probe_status"] == PROBE_OK
    assert table_row["written_at_column"] == "synced_at"
    assert table_row["event_at_column"] == "internal_date"
    assert table_row["last_write_at"] is not None


def test_status_ladder_reports_lateness_staleness_and_failure(warehouse):
    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    expected = pipeline("gmail").expected_data_interval
    assert expected is not None

    def collect_status(write_age: timedelta, *, error: str = "", status: str = "ok") -> dict:
        warehouse._command("DELETE FROM @gmail_messages")
        warehouse._command("DELETE FROM @gmail_sync_state")
        warehouse._command(
            """
            INSERT INTO @gmail_messages (account, message_id, synced_at, internal_date)
            VALUES ('z@x.test', 'm1', %s, %s)
            """,
            (now - write_age, now - write_age),
        )
        warehouse._command(
            """
            INSERT INTO @gmail_sync_state (account, status, error, updated_at)
            VALUES ('z@x.test', %s, %s, %s)
            """,
            (status, error, now - timedelta(minutes=1)),
        )
        PipelineHealthCollector(warehouse).run()
        return warehouse._query_dicts(
            "SELECT status, data_status, last_error, state_error_rows"
            " FROM @marts_pipeline_health WHERE pipeline = 'gmail'"
        )[0]

    late = collect_status(expected * (LATE_MULTIPLIER + 1))
    assert (late["status"], late["data_status"]) == ("late", "late")

    stale = collect_status(expected * (STALE_MULTIPLIER + 1))
    assert (stale["status"], stale["data_status"]) == ("stale", "stale")

    # A recorded sync error outranks freshness: the data may look current while
    # one account is locked out.
    failing = collect_status(timedelta(minutes=1), error="invalid_grant", status="error")
    assert failing["status"] == "failing"
    assert failing["last_error"] == "invalid_grant"
    assert failing["state_error_rows"] == 1


def test_terminal_gone_state_rows_do_not_surface_as_failures(warehouse):
    # Slack records deleted/archived channels and deleted thread parents with
    # the terminal status 'gone', keeping the failure text as the reason. Those
    # rows are expected, closed-out facts: they must not count as failing and
    # their error text must not surface as the pipeline's current last_error.
    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    warehouse._command(
        """
        INSERT INTO @slack_sync_state
            (account, team_id, object_type, object_id, cursor_ts, last_sync_type,
             status, error, updated_at, sync_version)
        VALUES
            ('zrl', 'T1', 'thread', 'C1:1770000000.000001', '', 'thread_replies',
             'gone', 'conversations.replies failed: channel_not_found', %s, 1),
            ('zrl', 'T1', 'conversation', 'C2', '', 'partial',
             'ok', '', %s, 1)
        """,
        (now, now - timedelta(minutes=5)),
    )
    PipelineHealthCollector(warehouse).run()

    row = warehouse._query_dicts(
        "SELECT state_rows, state_error_rows, last_error, last_error_at"
        " FROM @marts_pipeline_health WHERE pipeline = 'slack'"
    )[0]
    assert row["state_rows"] == 2
    assert row["state_error_rows"] == 0
    assert row["last_error"] is None
    assert row["last_error_at"] is None


def test_uploader_heartbeats_give_each_uploader_pipeline_its_own_run_status(warehouse):
    """A Mac uploader that fires and fails must read `failing`, not `late`.

    Until ops.uploader_heartbeats the remote-device uploaders had no run
    heartbeat at all, so apple_voice_memos sat `late` for fifteen days with no
    way to say whether the LaunchAgent was healthy. The table holds every
    uploader's runs keyed by (pipeline, device), and the StateSource scope
    filter is what keeps one uploader's success from greening another.
    """
    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    warehouse._command(
        """
        INSERT INTO @uploader_heartbeats
            (pipeline, device, ran_at, status, error, exit_code, duration_seconds, updated_at, sync_version)
        VALUES
            ('apple_notes', 'porygon', %s, 'error', 'PermissionError: Operation not permitted', 1, 3, %s, 2),
            ('apple_messages', 'porygon', %s, 'ok', '', 0, 5, %s, 2),
            ('claude_code', 'porygon', %s, 'ok', '', 0, 5, %s, 2)
        """,
        (now, now, now, now, now - timedelta(hours=5), now - timedelta(hours=5)),
    )
    PipelineHealthCollector(warehouse).run()

    rows = {
        row["pipeline"]: row
        for row in warehouse._query_dicts(
            "SELECT pipeline, status, run_status, state_rows, state_error_rows, last_error, last_run_at"
            " FROM @marts_pipeline_health"
            " WHERE pipeline IN ('apple_notes', 'apple_messages', 'claude_code', 'codex', 'uploader_heartbeats')"
        )
    }
    assert rows["apple_notes"]["status"] == "failing"
    assert rows["apple_notes"]["state_rows"] == 1
    assert rows["apple_notes"]["last_error"] == "PermissionError: Operation not permitted"
    # Only its own row: the notes failure does not leak into messages.
    assert rows["apple_messages"]["state_error_rows"] == 0
    assert rows["apple_messages"]["run_status"] == "ok"
    # A heartbeat five hours old on a thirty-minute cadence is past stale (6x).
    assert rows["claude_code"]["run_status"] == "stale"
    # No row at all is not a failure, it is the absence of a heartbeat.
    assert rows["codex"]["state_rows"] == 0
    assert rows["codex"]["run_status"] in ("no_data", "unmonitored")
    assert rows["uploader_heartbeats"]["status"] == "ok"


def test_every_remote_device_uploader_declares_a_run_heartbeat():
    """Every pipeline fed by a Mac/VM uploader reads ops.uploader_heartbeats."""
    for pipeline_id in (
        "apple_notes", "apple_messages", "apple_contacts", "apple_voice_memos",
        "apple_photos", "claude_code", "codex", "openclaw", "pi",
    ):
        entry = pipeline(pipeline_id)
        assert entry.state is not None, pipeline_id
        assert entry.state.table == "uploader_heartbeats", pipeline_id
        assert entry.state.scope_value == pipeline_id, pipeline_id
        assert entry.expected_run_interval is not None, pipeline_id


def test_priority_mix_counts_each_sources_tiers_and_flags_unclassified(warehouse):
    """Contract C2 on a surface: the tier mix per source, with the sentinel red.

    `unclassified` is a fail-loud sentinel, not a sixth tier -- a row carrying
    it means an adapter's classification did not run and every attention
    question answered from that source is wrong. It must read `failing` here,
    where the adapter row (which only knows about SQL errors) reads `ok`.
    """
    _provision_every_table(warehouse)
    warehouse.ensure_timeline_tables()
    for event_id, source, priority, hours_ago in (
        ("s1", "slack", "noise", 1),
        ("s2", "slack", "noise", 30),
        ("s3", "slack", "direct", 2),
        ("g1", "gmail", "unclassified", 3),
        ("old", "gmail", "self", 24 * 9),
    ):
        warehouse._command(
            "INSERT INTO @timeline_events (adapter, event_id, source, kind, event_ts, source_table, priority) "
            "VALUES ('t', %s, %s, 'k', now() - make_interval(hours => %s), 'x', %s::timeline.timeline_priority)"
            .replace("timeline.timeline_priority", warehouse.physical_schema_name("timeline") + ".timeline_priority"),
            (event_id, source, hours_ago, priority),
        )
    PipelineHealthCollector(warehouse).run()

    rows = {
        (row["source"], row["priority"]): row
        for row in warehouse._query_dicts("SELECT * FROM @marts_timeline_priority_mix")
    }
    assert set(rows) == {("slack", "noise"), ("slack", "direct"), ("gmail", "unclassified")}
    assert rows[("slack", "noise")]["events_7d"] == 2
    assert rows[("slack", "noise")]["events_1d"] == 1
    assert float(rows[("slack", "noise")]["share_7d"]) == pytest.approx(0.6667, abs=1e-4)
    assert rows[("slack", "direct")]["status"] == "ok"
    assert rows[("gmail", "unclassified")]["status"] == "failing"
    assert rows[("gmail", "unclassified")]["newest_event_at"] is not None


def test_a_rejected_transcription_provider_call_reads_failing(warehouse):
    """AssemblyAI returned 400 "account balance is negative" on every call for
    hours on 2026-08-27 while the transcription pipeline read green: the runs
    table recorded each rejection and nothing read it. The error row is the
    pipeline's state, and a later success overwrites it."""
    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    warehouse._command(
        """
        INSERT INTO @apple_voice_memos_transcription_runs
            (source, account, recording_id, content_sha256, provider, status, error, requested_at, sync_version)
        VALUES ('apple_notes', 'z', 'rec-1', 'sha', 'assemblyai', 'error',
                '400 Client Error: account balance is negative', %s, 1)
        """,
        (now,),
    )
    PipelineHealthCollector(warehouse).run()
    row = warehouse._query_dicts(
        "SELECT status, state_error_rows, last_error FROM @marts_pipeline_health WHERE pipeline = 'voice_memo_transcription'"
    )[0]
    assert row["status"] == "failing"
    assert row["state_error_rows"] == 1
    assert "balance is negative" in row["last_error"]


def test_an_impossible_recording_does_not_pin_transcription_to_failing(warehouse):
    """A memo the provider will NEVER accept must not read as a pipeline failure.

    The error count is over the whole runs table with no time bound, so before
    'rejected' existed one silent voice memo turned voice_memo_transcription red
    permanently. Production carried eleven of them going back to 2026-05-01 --
    "no spoken audio", "audio duration is too short", "does not appear to
    contain audio" -- which meant the row was ALREADY failing when the
    AssemblyAI balance outage arrived, and the StateSource that was added to
    catch that outage could not have caught it.
    """
    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    for index, error in enumerate(
        (
            "language_detection cannot be performed on files with no spoken audio.",
            "Audio duration is too short.",
            "Transcoding failed. File does not appear to contain audio.",
        )
    ):
        warehouse._command(
            """
            INSERT INTO @apple_voice_memos_transcription_runs
                (source, account, recording_id, content_sha256, provider, status, error, requested_at, sync_version)
            VALUES ('apple_voice_memos', 'z', %s, 'sha', 'assemblyai', %s, %s, %s, 1)
            """,
            (
                f"rec-impossible-{index}",
                voice_memo_transcription_failure_status(error),
                error,
                now,
            ),
        )

    PipelineHealthCollector(warehouse).run()
    row = warehouse._query_dicts(
        "SELECT status, state_error_rows FROM @marts_pipeline_health"
        " WHERE pipeline = 'voice_memo_transcription'"
    )[0]
    assert row["state_error_rows"] == 0
    assert row["status"] != "failing"


def test_an_impossible_recording_is_still_terminal_and_never_retried(warehouse):
    """'rejected' must stay terminal, or the fix trades a red row for a retry loop."""
    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    warehouse._command(
        """
        INSERT INTO @apple_voice_memos_transcription_runs
            (source, account, recording_id, content_sha256, provider, status, error, requested_at, sync_version)
        VALUES ('apple_voice_memos', 'z', 'rec-1', 'sha-1', 'assemblyai', 'rejected',
                'Audio duration is too short.', %s, 1)
        """,
        (now,),
    )
    candidates = warehouse.load_untranscribed_voice_recordings(provider="assemblyai", limit=50)
    assert not [row for row in candidates if row["recording_id"] == "rec-1"]


def test_a_legacy_error_row_for_an_impossible_recording_is_reclassified(warehouse):
    """Rows written before 'rejected' existed must migrate, or the row stays red."""
    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    warehouse._command(
        """
        INSERT INTO @apple_voice_memos_transcription_runs
            (source, account, recording_id, content_sha256, provider, status, error, requested_at, sync_version)
        VALUES ('apple_voice_memos', 'z', 'legacy-1', 'sha', 'assemblyai', 'error',
                'language_detection cannot be performed on files with no spoken audio.', %s, 1),
               ('apple_voice_memos', 'z', 'legacy-2', 'sha', 'assemblyai', 'error',
                '400 Client Error: account balance is negative', %s, 1)
        """,
        (now, now),
    )
    # Deliberately the ensure path the RUNNERS call, not the transcription-only
    # alias: that alias has no callers at all, so a migration living there would
    # never run in production while a test calling it directly still passed.
    warehouse.ensure_apple_voice_memos_tables()

    statuses = {
        row["recording_id"]: row["status"]
        for row in warehouse._query_dicts(
            "SELECT recording_id, status FROM @apple_voice_memos_transcription_runs"
            " WHERE recording_id LIKE 'legacy-%'"
        )
    }
    # The impossible input becomes terminal; the provider outage stays an error
    # so a genuine outage still reads failing.
    assert statuses == {"legacy-1": "rejected", "legacy-2": "error"}


def test_the_rejection_migration_runs_from_a_path_something_actually_calls() -> None:
    """A migration is only real if a live code path reaches it.

    ensure_voice_memo_transcription_tables() has no callers anywhere in the
    repo, so a reclassification placed there would never execute against
    production -- and a test that called it directly would still be green.
    Pin it to ensure_apple_voice_memos_tables(), which the transcription
    runner, the enrichment runner and the Drive ingest all call.
    """
    import inspect

    from personal_data_warehouse.postgres import PostgresWarehouse

    live = inspect.getsource(PostgresWarehouse.ensure_apple_voice_memos_tables)
    assert "_ensure_transcription_runs_rejections_reclassified" in live

    callers = [
        name
        for name, member in inspect.getmembers(PostgresWarehouse, inspect.isfunction)
        if name != "ensure_voice_memo_transcription_tables"
        and "ensure_voice_memo_transcription_tables(" in inspect.getsource(member)
    ]
    assert not callers or "_ensure_transcription_runs_rejections_reclassified" in live


def test_a_dead_retryable_failure_ages_out_while_a_live_one_stays_red(warehouse):
    """The last way this row could be pinned red forever.

    'Upload failed, please try again' is a RETRYABLE message, so it is
    correctly not 'rejected'. But production had two such rows from 2026-05-02
    on recordings of size_bytes = 0, which the candidate query excludes -- so
    nothing would ever retry them and clear the row, and voice_memo_transcription
    read failing on a four-month-old ghost. A live outage re-stamps
    requested_at every run, so it stays inside the window; an error that has
    stopped being re-stamped is history, not state.
    """
    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    warehouse._command(
        """
        INSERT INTO @apple_voice_memos_transcription_runs
            (source, account, recording_id, content_sha256, provider, status, error, requested_at, sync_version)
        VALUES ('apple_voice_memos', 'z', 'ancient', 'sha', 'assemblyai', 'error',
                '422 Client Error: Upload failed, please try again', %s, 1)
        """,
        (now - timedelta(days=120),),
    )
    PipelineHealthCollector(warehouse).run()
    row = warehouse._query_dicts(
        "SELECT status, state_error_rows, last_error FROM @marts_pipeline_health"
        " WHERE pipeline = 'voice_memo_transcription'"
    )[0]
    assert row["state_error_rows"] == 0
    assert row["status"] != "failing"
    # The banner must not quote a failure that no longer colours the row.
    assert "Upload failed" not in (row["last_error"] or "")

    # The same message, today, is a live outage and must read failing.
    warehouse._command(
        """
        INSERT INTO @apple_voice_memos_transcription_runs
            (source, account, recording_id, content_sha256, provider, status, error, requested_at, sync_version)
        VALUES ('apple_voice_memos', 'z', 'today', 'sha', 'assemblyai', 'error',
                '400 Client Error: account balance is negative', %s, 1)
        """,
        (now,),
    )
    PipelineHealthCollector(warehouse).run()
    row = warehouse._query_dicts(
        "SELECT status, state_error_rows, last_error FROM @marts_pipeline_health"
        " WHERE pipeline = 'voice_memo_transcription'"
    )[0]
    assert row["state_error_rows"] == 1
    assert row["status"] == "failing"
    assert "balance is negative" in row["last_error"]


def test_only_a_history_state_source_ages_its_failures_out(warehouse):
    """Every ops.*_sync_state row IS current state; ageing one out hides an outage."""
    from personal_data_warehouse.pipeline_health import PIPELINES

    windowed = {p.id for p in PIPELINES if p.state and p.state.error_window is not None}
    for entry in PIPELINES:
        if entry.state is None or entry.id in windowed:
            continue
        assert entry.state.error_window is None
    # WHOOP went 26 hours hard-down reading 'ok' once; its failure must never age out.
    whoop = next(p for p in PIPELINES if p.id == "whoop")
    assert whoop.state is not None and whoop.state.error_window is None


def test_a_stale_snapshot_reports_unknown_instead_of_stale_facts(warehouse):
    """The dashboard must distrust itself when the collector stops running."""
    _provision_every_table(warehouse)
    collector = PipelineHealthCollector(warehouse)
    old = datetime.now(tz=UTC) - timedelta(seconds=COLLECTOR_STALE_SECONDS * 2)
    pipelines, tables = collector.collect()
    warehouse.write_pipeline_health(pipelines, tables, collected_at=old)

    statuses = {
        row["status"]
        for row in warehouse._query_dicts("SELECT status FROM @marts_pipeline_health")
    }
    assert statuses == {"unknown"}


def test_collector_prunes_rows_for_retired_pipelines(warehouse):
    _provision_every_table(warehouse)
    collector = PipelineHealthCollector(warehouse)
    collector.run()
    warehouse._command(
        """
        INSERT INTO @pipeline_health (pipeline, label, kind, collected_at)
        VALUES ('retired_source', 'Retired', 'source', now())
        """
    )
    warehouse._command(
        """
        INSERT INTO @pipeline_table_freshness (table_id, pipeline, role, collected_at)
        VALUES ('retired_table', 'retired_source', 'data', now())
        """
    )
    collector.run()
    remaining = {
        row["pipeline"]
        for row in warehouse._query_dicts("SELECT pipeline FROM @marts_pipeline_health")
    }
    assert "retired_source" not in remaining
    tables = {
        row["table_id"]
        for row in warehouse._query_dicts("SELECT table_id FROM @marts_pipeline_table_freshness")
    }
    assert "retired_table" not in tables


def test_collector_skips_probes_it_cannot_afford(warehouse, monkeypatch):
    """The cost guard has to be observable, not silent.

    A skipped probe records why, so "no timestamp" on the dashboard is never
    mistaken for "no data".
    """
    _provision_every_table(warehouse)
    monkeypatch.setattr(
        "personal_data_warehouse.pipeline_health.PROBE_MAX_UNINDEXED_BYTES", -1
    )
    PipelineHealthCollector(warehouse).run()
    rows = warehouse._query_dicts(
        "SELECT table_id, probe_status, probe_detail FROM @marts_pipeline_table_freshness"
        " WHERE probe_status = %s",
        (PROBE_SKIPPED_UNINDEXED,),
    )
    assert rows, "expected the cost guard to skip probes when nothing is affordable"
    assert all(row["probe_detail"] for row in rows)


def test_read_only_query_role_can_read_the_marts_views(warehouse):
    """Zach and the MCP agent must be able to ask "when was gmail last updated?".

    The snapshot lives in ops, which the query role cannot read directly; the
    marts_ops views are the sanctioned surface.
    """
    _provision_every_table(warehouse)
    PipelineHealthCollector(warehouse).run()
    connection = warehouse.read_only_connection()
    try:
        with connection.cursor() as cursor:
            rel = relation("marts_pipeline_health").with_namespace(warehouse.schema_namespace)
            cursor.execute(f'SELECT count(*) FROM "{rel.schema}"."{rel.name}"')
            assert cursor.fetchone()[0] == len(PIPELINES)
            rel = relation("marts_pipeline_table_freshness").with_namespace(
                warehouse.schema_namespace
            )
            cursor.execute(f'SELECT count(*) FROM "{rel.schema}"."{rel.name}"')
            assert cursor.fetchone()[0] == len(TABLE_PIPELINES)
    finally:
        connection.close()


def test_timeline_adapter_health_exposes_every_adapter_to_the_query_role(warehouse):
    """Level 3 of the health contract: is THIS kind of data current on the timeline?

    The single ``timeline`` row in ``marts_ops.pipeline_health`` cannot answer
    it. Its run heartbeat is a ``max()`` over every adapter, so one wedged
    adapter is arithmetically invisible behind the healthy ones -- measured in
    production on 2026-08-23, six adapters had not run in roughly sixty hours
    against a thirty-minute cadence while the pipeline reported ``ok``.
    """
    from personal_data_warehouse.timeline import TIMELINE_ADAPTERS, TimelineSyncEngine

    _provision_every_table(warehouse)
    PipelineHealthCollector(warehouse).run()
    TimelineSyncEngine(
        source_url=_postgres_url(),
        source_schema=warehouse._schema,
        dest_schema=warehouse._schema,
    ).run()

    connection = warehouse.read_only_connection()
    try:
        with connection.cursor() as cursor:
            rel = relation("marts_timeline_adapter_health").with_namespace(
                warehouse.schema_namespace
            )
            cursor.execute(f'SELECT adapter, status FROM "{rel.schema}"."{rel.name}"')
            rows = cursor.fetchall()
    finally:
        connection.close()

    seen = {adapter for adapter, _ in rows}
    expected = {adapter.name for adapter in TIMELINE_ADAPTERS}
    assert seen == expected, (
        "every timeline adapter must be visible in the health surface; missing "
        f"{sorted(expected - seen)}, unexpected {sorted(seen - expected)}"
    )
    # A freshly synced warehouse has no errors and no unfinished backfill, so
    # anything other than 'ok' here means the status expression is wrong rather
    # than that the fixture is unhealthy.
    assert {status for _, status in rows} == {"ok"}


# --- level 2: the marts layer's own health (pure) -----------------------------


def test_every_marts_view_is_covered_by_the_mart_health_registry():
    """The marts layer had ZERO health coverage until 2026-08-23.

    Every ``derived_*`` TABLE was covered by ``TABLE_PIPELINES``; not one
    ``marts_*`` VIEW was, so ``SELECT layer, count(*) FROM
    marts_ops.table_freshness GROUP BY 1`` returned base/derived/ops/private/
    timeline and no marts row at all -- for the exact relations every agent is
    told to start from.

    The list is derived from the catalog rather than hand-maintained, so adding
    a mart stays one catalog edit; this pins that the derivation covers it.
    """
    catalogued = {
        obj.id for obj in CATALOG.objects if obj.layer == "marts" and obj.kind == "view"
    }
    assert set(mart_view_ids()) == catalogued
    assert catalogued, "the warehouse always has marts views"


def test_a_mart_view_cannot_be_measured_the_way_a_table_is():
    """State the limitation instead of pretending the table probe generalizes.

    ``TABLE_PIPELINES`` measures ``max(<written_at>)`` over a heap. A view has
    no stamped column to take a max of and no ``relpages`` for the cheapness
    guard to consult, so pointing the existing probe at one would mean either
    inventing a timestamp column or running an unbounded aggregate over a union
    of six source tables every ten minutes. No mart may appear in the table
    registry.
    """
    marts = set(mart_view_ids())
    assert marts.isdisjoint(TABLE_PIPELINES)
    assert marts.isdisjoint(POSTGRES_TABLES)


def test_expensive_mart_probes_are_declared_not_discovered():
    """A view too expensive to probe says so up front, and is still measured.

    Same contract as ``skipped_unindexed``: record the skip and why. Discovering
    the cost at runtime instead means the collector's window absorbs it every
    ten minutes until someone notices.
    """
    assert EXPENSIVE_MART_VIEWS <= set(mart_view_ids())
    assert 0 < MART_PROBE_STATEMENT_TIMEOUT_MS <= 30_000


# --- the SLA registry itself (pure) -------------------------------------------


def test_a_long_data_sla_says_where_its_number_came_from():
    """A week-plus SLA has to justify itself.

    Seven pipelines carried ``expected_data_interval = 30 days`` -- ``pi``,
    whose uploader runs every five minutes, could therefore not reach 'late'
    until SIXTY days, and did sit quiet for five weeks under a green dot. The
    blunt month was not carelessness: these sources really are bursty, and the
    cadence is not the answer either. The fix is to require the number to be
    traceable, so the next reader re-measures it instead of re-guessing it.
    """
    for entry in PIPELINES:
        interval = entry.expected_data_interval
        if interval is None or interval < DATA_BASIS_REQUIRED_ABOVE:
            continue
        assert entry.data_basis.strip(), (
            f"{entry.id} declares a {interval.days}-day data SLA with no basis; say how "
            "that number was derived (see the measurement query in pipeline_health.py)"
        )


def test_the_bursty_sources_have_an_sla_that_can_catch_a_forty_four_day_silence():
    """The seven blunt-30-day pipelines, judged against what they must catch.

    ``alice_voice_recordings`` sat 44.5 days without a write under a green dot.
    Any pipeline whose data SLA is set from a measured gap distribution must
    reach at least 'late' inside that window; the exceptions are the ones where
    measurement says the source genuinely is quieter than that, and they have to
    earn the exception by keeping a run heartbeat instead.
    """
    forty_four_days = timedelta(days=44)
    previously_blunt = {
        "alice_voice_recordings",
        "pi",
        "voice_memo_transcription",
        "apple_voice_memos",
        "voice_memo_enrichment",
        "apple_contacts",
        "google_contacts",
    }
    for entry_id in sorted(previously_blunt):
        entry = pipeline(entry_id)
        interval = entry.expected_data_interval
        assert interval is not None, entry_id
        # No pipeline may keep the old 30-day number by accident.
        assert entry.data_basis.strip(), entry_id
        if interval * LATE_MULTIPLIER <= forty_four_days:
            continue
        # The only way to be looser than that is to have a heartbeat that
        # catches breakage instead -- run freshness, not data freshness.
        assert entry.expected_run_interval is not None and entry.state is not None, (
            f"{entry_id} cannot reach 'late' within 44 days and has no run heartbeat "
            "to catch it breaking; either tighten the data SLA or explain the heartbeat"
        )
        assert "heartbeat" in entry.data_basis or "heartbeat" in entry.note, entry_id


def test_alice_is_judged_by_its_poller_running_not_by_zach_recording():
    """A daily poller against a source used a few times a year.

    Measured 2026-08-27: 34 days of use across 17 months (2024-12-07 to
    2026-04-27), 33 gaps between them, p90 17 days and a longest of **223**.
    Any data SLA tight enough to notice the poller dying is therefore far
    tighter than the source's own longest legitimate silence, and would fire on
    Zach simply not picking the device up -- which it did, holding four
    marts_voice_memos / marts_calendar views 'stale' while the poller was
    running daily and succeeding.

    The two facts have to be separated: the RUN heartbeat says the poller is
    alive, and only it may be tight. Data freshness here is nearly mute by
    construction, and the basis has to admit that rather than imply the 240 days
    is a real expectation about recordings.
    """
    entry = pipeline("alice_voice_recordings")
    longest_observed_gap = timedelta(days=223)
    assert entry.expected_data_interval is not None
    assert entry.expected_data_interval >= longest_observed_gap, (
        "alice goes 223 days between recordings; a tighter SLA alarms on Zach "
        "not using the device, which is not a pipeline fact"
    )
    assert entry.event_interval is not None
    assert entry.event_interval >= longest_observed_gap, (
        "event lateness escalates the pipeline exactly like write lateness, so "
        "loosening only the data side leaves it red"
    )
    assert entry.state is not None, "the poller needs a heartbeat to be judged by"
    assert entry.expected_run_interval is not None
    assert entry.expected_run_interval <= timedelta(days=2), (
        "the heartbeat is the only tight signal left; keep it tight"
    )
    assert "heartbeat" in entry.data_basis or "heartbeat" in entry.note


def test_alices_heartbeat_table_is_in_both_registries_and_the_catalog():
    """The state table has to be a real, cataloged, monitored warehouse table."""
    table = "alice_voice_recordings_sync_state"
    assert table in TABLE_PIPELINES, "the heartbeat table needs a pipeline row"
    assert TABLE_PIPELINES[table].role == "state"
    assert TABLE_PIPELINES[table].pipeline == "alice_voice_recordings"
    assert pipeline("alice_voice_recordings").state is not None
    assert pipeline("alice_voice_recordings").state.table == table


def test_pi_absorbs_its_longest_observed_gap_between_uses():
    """The old 3-day number measured chattiness, not cadence.

    ``pi``'s basis read "168 gaps, p95 0.06d, max 2.86d", which is the gap
    between consecutive *events* -- and an agent session emits events seconds
    apart, so that distribution describes how talkative a session is, never how
    often Zach opens the tool. Measured properly on 2026-08-27, over distinct
    days on which pi was used at all, the source has 8 such days in its whole
    life (2026-05-19 to 2026-07-16) and 7 gaps between them, the longest **40
    days** -- inside its active period, not counting the silence since. A 3-day
    SLA against a source that legitimately goes 40 days between uses is a
    guaranteed false positive, and it duly fired.

    So the data interval has to clear the longest gap the source has actually
    shown, and the run heartbeat -- which only began working on 2026-08-27, see
    ``pdw_export_app_credentials`` -- is what catches the uploader dying.
    """
    longest_observed_gap = timedelta(days=40)
    entry = pipeline("pi")
    assert entry.expected_data_interval is not None
    assert entry.expected_data_interval >= longest_observed_gap, (
        "pi goes 40 days between uses; an SLA below that alarms on Zach not "
        "using the tool, which is not a pipeline fact"
    )
    assert entry.state is not None and entry.expected_run_interval is not None, (
        "pi is only allowed a loose data SLA because its uploader heartbeat is "
        "the real detector"
    )
    basis = entry.data_basis.lower()
    assert "usage" in basis or "between uses" in basis, (
        "say that the number is a gap between USES, so the next reader does not "
        "re-measure event gaps and get 3 days again"
    )


def test_run_cadence_and_data_arrival_are_separate_numbers():
    """The distinction the blunt 30 days collapsed.

    ``pi``'s uploader runs every five minutes and its data arrives in bursts;
    those are different facts and only one of them is an SLA. A data interval
    tighter than the run interval is incoherent -- it demands output faster than
    the pipeline is even scheduled -- and every real-world source is looser
    still. ``timeline`` is the deliberate equal case: it runs every five minutes
    and, because it is fed by every other pipeline at once, genuinely must write
    within its 30-minute run window.
    """
    equal_by_design = {"timeline"}
    for entry in PIPELINES:
        if entry.expected_run_interval is None or entry.expected_data_interval is None:
            continue
        assert entry.expected_data_interval >= entry.expected_run_interval, entry.id
        if entry.id in equal_by_design:
            continue
        assert entry.expected_data_interval > entry.expected_run_interval, entry.id


# --- newest_event_at is judged ------------------------------------------------


def test_event_time_is_judged_by_default_so_a_new_source_cannot_forget():
    """``newest_event_at`` was collected, stored, shipped and rendered -- never judged.

    The status ladder branched only on ``last_write_at``/``last_run_at``, so
    ``alice_voice_recordings`` showed a green dot beside a newest event 118 days
    old. Inheriting the data interval by default means a new source is judged on
    event time without doing anything; opting out is explicit and carries a
    reason.
    """
    inherited = [entry for entry in PIPELINES if entry.event_interval_is_inherited]
    assert inherited, "inheriting must be the default"
    for entry in inherited:
        assert entry.event_interval == entry.expected_data_interval, entry.id
    for entry in PIPELINES:
        if entry.event_interval_is_inherited:
            continue
        assert entry.note or entry.data_basis, (
            f"{entry.id} overrides the event interval without explaining why"
        )
    assert INHERIT_DATA_INTERVAL not in {
        entry.event_interval for entry in PIPELINES
    }, "the sentinel must never leak out as a real interval"


def test_the_finance_ledger_event_interval_prevents_a_measured_false_positive():
    """The one override, and the reason it exists.

    A naive event check at the data interval flips exactly two pipelines on the
    production corpus: ``alice_voice_recordings``, a true positive, and
    ``finance_ledger``, a false one. ``derived_finance.observations.as_of`` is a
    DATE, so an observation written at 15:40 is dated 00:00 that day and event
    time trails write time by up to a day *while working perfectly*. Measured
    2026-08-23: newest_event_at was 15.8h old against a 6h 'late' threshold.
    """
    ledger = pipeline("finance_ledger")
    assert not ledger.event_interval_is_inherited
    assert ledger.expected_data_interval == timedelta(hours=3)
    assert ledger.event_interval is not None
    assert ledger.event_interval > ledger.expected_data_interval * LATE_MULTIPLIER
    # And it must still be able to see a genuinely frozen ledger.
    assert ledger.event_interval <= timedelta(days=3)

    # alice USED to need the same override, for the opposite reason: its ingest
    # interval was set from the poll cadence (7d) while its event interval came
    # from Zach's bursty recording habit (30d). Both numbers were guesses
    # standing in for a heartbeat the pipeline did not have. Now it has one, the
    # two sides collapse into a single measured fact -- the poller writes when
    # there IS a recording -- so the override is gone and the interval simply
    # inherits. Splitting them again would only re-create a tight event
    # threshold that fires on Zach not recording.
    alice = pipeline("alice_voice_recordings")
    assert alice.event_interval_is_inherited
    assert alice.event_interval == alice.expected_data_interval
    assert alice.state is not None, (
        "the override is only safe to drop because a run heartbeat replaced it"
    )


# --- level 2 live behaviour (Postgres) ----------------------------------------


def test_collector_writes_a_row_for_every_marts_view(warehouse):
    _provision_every_table(warehouse)
    _, _, marts = PipelineHealthCollector(warehouse).run_all()
    assert {view.view_id for view in marts} == set(mart_view_ids())

    rows = {
        row["view_id"]: row
        for row in warehouse._query_dicts(
            "SELECT view_id, domain, view_schema, view_name, status, input_status,"
            " probe_status, input_count, input_tables, definition_sha256,"
            " first_seen_at, collected_at FROM @marts_mart_view_health"
        )
    }
    assert set(rows) == set(mart_view_ids())
    for row in rows.values():
        assert row["definition_sha256"], row["view_id"]
        assert row["first_seen_at"] is not None
        assert row["collected_at"] is not None

    # The declared-expensive view is skipped with a reason rather than timing
    # the collection window out.
    for view_id in EXPENSIVE_MART_VIEWS:
        assert rows[view_id]["probe_status"] == PROBE_SKIPPED_EXPENSIVE


def test_mart_inputs_are_resolved_from_pg_depend_not_a_hardcoded_map(warehouse):
    """A view's inputs must come from the catalog, or the map rots on redefinition.

    ``marts_ai_conversations.events`` unions the six agent-source event tables;
    the resolver has to find all of them, including through intermediate views.
    """
    _provision_every_table(warehouse)
    _, _, marts = PipelineHealthCollector(warehouse).run_all()
    by_id = {view.view_id: view for view in marts}

    conversations = by_id["ai_conversation_events"]
    assert {
        "claude_code_events",
        "codex_events",
        "openclaw_events",
        "pi_events",
        "claude_desktop_events",
        "chatgpt_events",
    } <= set(conversations.input_tables)

    # A view over a view resolves to base tables, never to the intermediate.
    for view in marts:
        assert all(table in TABLE_PIPELINES for table in view.input_tables), view.view_id


def test_a_stale_input_makes_the_mart_that_reads_it_stale(warehouse):
    """Input-freshness roll-up: this is what surfaces `pi` through the mart.

    ``pi`` went 38 days without a write against a 3-day SLA, and every consumer
    of ``marts_ai_conversations.events`` was reading a source that had stopped
    -- with nothing anywhere saying so.
    """
    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    stale_by = pipeline("pi").expected_data_interval
    assert stale_by is not None
    warehouse._command(
        """
        INSERT INTO @pi_events (account, source, session_id, event_uuid, ingested_at, occurred_at)
        VALUES ('z@x.test', 'pi', 's1', 'e1', %s, %s)
        """,
        (now - stale_by * (STALE_MULTIPLIER + 1), now - stale_by * (STALE_MULTIPLIER + 1)),
    )
    PipelineHealthCollector(warehouse).run_all()

    row = warehouse._query_dicts(
        "SELECT status, input_status, stalest_pipeline, stalest_pipeline_expected_seconds"
        " FROM @marts_mart_view_health WHERE view_id = 'ai_conversation_events'"
    )[0]
    assert row["stalest_pipeline"] == "pi"
    assert row["input_status"] == "stale"
    assert row["status"] == "stale"
    assert row["stalest_pipeline_expected_seconds"] == int(stale_by.total_seconds())


def test_each_mart_input_is_judged_against_its_own_pipelines_sla(warehouse):
    """Not simply the oldest input.

    ``marts_ai_conversations.events`` reads six sources whose expectations
    differ by an order of magnitude (``codex`` at 7 days, ``pi`` at 45, because
    pi is used in bursts and leans on its run heartbeat instead). Ranking by raw
    age would permanently nominate whichever source is legitimately the quietest
    -- pi -- and never notice the one actually misbehaving.
    """
    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    codex_interval = pipeline("codex").expected_data_interval
    pi_interval = pipeline("pi").expected_data_interval
    assert codex_interval is not None and pi_interval is not None
    assert pi_interval > codex_interval, "the fixture needs two different SLAs"
    # pi is OLDER in absolute terms but still inside its own loose SLA; codex is
    # younger yet past its tighter one. Raw-age ranking picks pi; the
    # SLA-relative ranking that matters picks codex.
    pi_age = pi_interval * LATE_MULTIPLIER - timedelta(hours=1)
    codex_age = codex_interval * LATE_MULTIPLIER + timedelta(hours=1)
    assert pi_age > codex_age
    warehouse._command(
        """
        INSERT INTO @codex_events (account, source, session_id, event_uuid, ingested_at, occurred_at)
        VALUES ('z@x.test', 'codex', 's1', 'e1', %s, %s)
        """,
        (now - codex_age, now - codex_age),
    )
    warehouse._command(
        """
        INSERT INTO @pi_events (account, source, session_id, event_uuid, ingested_at, occurred_at)
        VALUES ('z@x.test', 'pi', 's1', 'e1', %s, %s)
        """,
        (now - pi_age, now - pi_age),
    )
    PipelineHealthCollector(warehouse).run_all()
    row = warehouse._query_dicts(
        "SELECT stalest_pipeline, input_status FROM @marts_mart_view_health"
        " WHERE view_id = 'ai_conversation_events'"
    )[0]
    assert row["stalest_pipeline"] == "codex", (
        "the input past its own SLA must win, not the one with the older timestamp"
    )
    assert row["input_status"] == "late"


def test_a_bounded_non_empty_probe_reports_empty_rather_than_hanging(warehouse):
    _provision_every_table(warehouse)
    _, _, marts = PipelineHealthCollector(warehouse).run_all()
    probed = [
        view
        for view in marts
        if view.probe_status not in {PROBE_SKIPPED_EXPENSIVE, PROBE_MISSING}
    ]
    assert probed
    # Nothing is seeded, so every probeable view is empty -- and says so.
    assert {view.probe_status for view in probed} <= {PROBE_EMPTY, PROBE_OK}
    for view in probed:
        assert view.probe_ms < MART_PROBE_STATEMENT_TIMEOUT_MS

    row = warehouse._query_dicts(
        "SELECT status, probe_status, has_rows FROM @marts_mart_view_health"
        " WHERE view_id = %s",
        (probed[0].view_id,),
    )[0]
    assert row["probe_status"] in {PROBE_EMPTY, PROBE_OK}
    assert row["has_rows"] in (0, 1)


def test_a_redefined_view_is_visible_as_definition_drift(warehouse):
    """A redefinition that silently drops a source table changes no rows.

    So the definition itself is what is watched: the hash changes and
    ``first_seen_at`` resets, which is the only signal such a change produces.
    """
    _provision_every_table(warehouse)
    collector = PipelineHealthCollector(warehouse)
    _, _, first = collector.run_all()
    before = {view.view_id: (view.definition_sha256, view.first_seen_at) for view in first}

    # A stable second collection must NOT report drift.
    _, _, second = collector.run_all()
    for view in second:
        assert view.definition_sha256 == before[view.view_id][0], view.view_id
        assert view.first_seen_at == before[view.view_id][1], view.view_id

    # A leaf mart, redefined to read nothing -- the shape of a redefinition that
    # silently drops a source table.
    view_id = "slack_image_fingerprints"
    assert before[view_id][0]
    target = relation(view_id).with_namespace(warehouse.schema_namespace)
    warehouse._command(f'DROP VIEW "{target.schema}"."{target.name}"')
    warehouse._command(
        f'CREATE VIEW "{target.schema}"."{target.name}" AS SELECT 1::bigint AS placeholder'
    )
    _, _, third = collector.run_all()
    changed = {view.view_id: view for view in third}[view_id]
    assert changed.definition_sha256 != before[view_id][0]
    assert changed.first_seen_at != before[view_id][1]
    # The inputs it no longer reads are gone from the recorded list, which is
    # the drop this check exists to make visible.
    assert changed.input_tables == []
    assert before[view_id][0] and view_id in before


def test_a_stale_mart_snapshot_reports_unknown(warehouse):
    _provision_every_table(warehouse)
    collector = PipelineHealthCollector(warehouse)
    pipelines, tables = collector.collect()
    marts = collector.collect_marts(pipelines, tables)
    old = datetime.now(tz=UTC) - timedelta(seconds=COLLECTOR_STALE_SECONDS * 2)
    warehouse.write_mart_view_health(marts, collected_at=old)
    statuses = {
        row["status"]
        for row in warehouse._query_dicts("SELECT status FROM @marts_mart_view_health")
    }
    assert statuses == {"unknown"}


def test_mart_health_is_readable_by_the_query_role(warehouse):
    _provision_every_table(warehouse)
    PipelineHealthCollector(warehouse).run_all()
    connection = warehouse.read_only_connection()
    try:
        with connection.cursor() as cursor:
            rel = relation("marts_mart_view_health").with_namespace(
                warehouse.schema_namespace
            )
            cursor.execute(f'SELECT count(*) FROM "{rel.schema}"."{rel.name}"')
            assert cursor.fetchone()[0] == len(mart_view_ids())
    finally:
        connection.close()


def test_event_lateness_escalates_but_a_measurement_gap_never_does(warehouse):
    """Judging ``newest_event_at`` must not turn "we did not look" into red."""
    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    event_interval = pipeline("gmail").event_interval
    assert event_interval is not None

    def collect(event_age: timedelta) -> dict:
        warehouse._command("DELETE FROM @gmail_messages")
        warehouse._command(
            """
            INSERT INTO @gmail_messages (account, message_id, synced_at, internal_date)
            VALUES ('z@x.test', 'm1', %s, %s)
            """,
            (now, now - event_age),
        )
        PipelineHealthCollector(warehouse).run_all()
        return warehouse._query_dicts(
            "SELECT status, data_status, event_status, event_age_seconds,"
            " expected_event_interval_seconds, event_tables_probed"
            " FROM @marts_pipeline_health WHERE pipeline = 'gmail'"
        )[0]

    fresh = collect(timedelta(minutes=1))
    assert fresh["event_status"] == "ok"
    assert fresh["status"] == "ok"
    assert fresh["expected_event_interval_seconds"] == int(event_interval.total_seconds())

    late = collect(event_interval * (LATE_MULTIPLIER + 1))
    # The write is current; only the event time is behind. Before this shipped
    # that combination was reported 'ok'.
    assert late["data_status"] == "ok"
    assert late["event_status"] == "late"
    assert late["status"] == "late"

    stale = collect(event_interval * (STALE_MULTIPLIER + 1))
    assert stale["event_status"] == "stale"
    assert stale["status"] == "stale"

    # A pipeline whose data tables declare no event column at all is
    # 'unmonitored', never 'no_data'.
    unmonitored = warehouse._query_dicts(
        "SELECT event_status, expected_event_interval_seconds"
        " FROM @marts_pipeline_health WHERE pipeline = 'pipeline_health'"
    )[0]
    assert unmonitored["expected_event_interval_seconds"] == 0
    assert unmonitored["event_status"] == "unmonitored"


def test_an_unmeasured_event_column_is_not_reported_as_no_data(warehouse):
    """``google_drive`` and ``attachment_enrichment`` DO declare an event column.

    It sits on a 376 MiB / 561 MiB heap with no index leading with it, so the
    collector skips the ``max()`` by design. That is 'unmeasured' -- a different
    and much quieter claim than "nothing has ever arrived", and it must never
    colour the pipeline late or stale.
    """
    _provision_every_table(warehouse)
    collector = PipelineHealthCollector(warehouse)
    pipelines, tables = collector.collect()
    drive = next(entry for entry in pipelines if entry.pipeline == "google_drive")
    # Force the "declared but unmeasured" shape the production heap produces.
    drive.newest_event_at = None
    drive.event_tables_probed = 0
    warehouse.write_pipeline_health(pipelines, tables, collected_at=datetime.now(tz=UTC))
    row = warehouse._query_dicts(
        "SELECT status, event_status FROM @marts_pipeline_health WHERE pipeline = 'google_drive'"
    )[0]
    assert row["event_status"] == "unmeasured"
    assert row["status"] not in {"late", "stale"}


def test_a_mart_is_never_more_broken_than_the_pipelines_feeding_it(warehouse):
    """The measured false positive this rule exists to prevent.

    The registry declares an SLA per PIPELINE, and a pipeline's own freshness is
    a ``max()`` over its data tables — deliberately, because a pipeline is not
    broken just because one of its tables is quiet. Judging an individual input
    TABLE against its pipeline's interval breaks that symmetry: measured against
    production 2026-08-23 it reported four marts 'stale'
    (``marts_finance.transactions``, ``marts_finance.account_freshness``,
    ``marts_finance.position_coverage``, ``marts_receipts.transaction_receipts``)
    because ``derived_finance.transactions`` was 1.1 days old against
    ``finance_ledger``'s three-hour interval — while the ledger was writing
    balance observations every half hour exactly as designed.

    So the invariant is: a mart's input_status can never be worse than the
    worst data_status of the pipelines feeding it. The per-table detail lives in
    marts_ops.table_freshness, which is where a quiet table inside a healthy
    pipeline belongs.
    """
    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    ledger = pipeline("finance_ledger")
    assert ledger.expected_data_interval is not None
    # The ledger's transactions table is far past the pipeline's interval while
    # its observations table -- the one the pipeline's freshness actually comes
    # from -- was written seconds ago. That is the healthy production shape.
    warehouse._command(
        """
        INSERT INTO @finance_transactions
            (transaction_id, account_id, posted_at, created_at)
        VALUES ('t1', 'fa_1', %s, %s)
        """,
        (now - timedelta(days=2), now - timedelta(days=2)),
    )
    warehouse._command(
        """
        INSERT INTO @finance_observations
            (account_id, as_of, kind, source, observed_at)
        VALUES ('fa_1', %s, 'balance', 'plaid', %s)
        """,
        (now.date(), now),
    )
    PipelineHealthCollector(warehouse).run_all()

    ledger_status = warehouse._query_dicts(
        "SELECT data_status FROM @marts_pipeline_health WHERE pipeline = 'finance_ledger'"
    )[0]["data_status"]
    assert ledger_status == "ok", "fixture must model a HEALTHY ledger with one quiet table"

    rows = warehouse._query_dicts(
        "SELECT view_id, input_status, stalest_pipeline FROM @marts_mart_view_health"
        " WHERE 'finance_ledger' = ANY(input_pipelines)"
    )
    assert rows, "expected marts fed by the finance ledger"
    for row in rows:
        assert row["input_status"] in {"ok", "unmeasured"}, (
            f"{row['view_id']} reported {row['input_status']} off a healthy ledger; "
            "input freshness must be judged per pipeline, not per table"
        )


def test_mart_input_pipelines_are_recorded_alongside_the_tables(warehouse):
    """Both facts are stored: the tables are the pg_depend evidence and the
    drift signal, the pipelines are what gets judged."""
    _provision_every_table(warehouse)
    _, _, marts = PipelineHealthCollector(warehouse).run_all()
    by_id = {view.view_id: view for view in marts}
    conversations = by_id["ai_conversation_events"]
    assert "pi" in conversations.input_pipelines
    assert "codex" in conversations.input_pipelines
    for view in marts:
        assert set(view.input_pipelines) <= {entry.id for entry in PIPELINES}, view.view_id
        # Every named pipeline must be reachable from a recorded input table --
        # the two lists describe the same dependency, at two grains.
        derived = {
            TABLE_PIPELINES[table].pipeline
            for table in view.input_tables
            if TABLE_PIPELINES[table].role != "state"
        }
        assert set(view.input_pipelines) == derived, view.view_id


def test_ensure_widens_a_pipeline_health_table_provisioned_before_the_new_columns(warehouse):
    """A warehouse older than a column must be widened, not left behind.

    _ensure_table_group only CREATEs; it does not ALTER an existing table. So a
    column added to PIPELINE_HEALTH_COLUMNS reaches a fresh database (and every
    test, and CI) while a long-lived warehouse keeps the old shape -- and the
    marts views that reference the column then fail on every collection. That
    is not hypothetical: it happened in production on 2026-08-23, where the
    collector raised every ten minutes while the whole suite was green,
    precisely because no test provisions an OLD table and re-ensures over it.
    """
    warehouse.ensure_pipeline_health_tables()
    rel = relation("pipeline_health").with_namespace(warehouse.schema_namespace)
    added = ("data_basis", "expected_event_interval_seconds", "event_tables_probed")
    for column in added:
        warehouse._command(
            f'ALTER TABLE "{rel.schema}"."{rel.name}" DROP COLUMN IF EXISTS {column} CASCADE'
        )

    # Re-ensuring must both re-add the columns and rebuild the views over them.
    warehouse.ensure_pipeline_health_tables()

    present = {
        row[0]
        for row in warehouse._query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s",
            (rel.schema, rel.name),
        )
    }
    assert set(added) <= present, f"ensure did not widen the table; missing {sorted(set(added) - present)}"

    # The view is the thing that actually broke, so prove it reads.
    marts = relation("marts_pipeline_health").with_namespace(warehouse.schema_namespace)
    warehouse._query(f'SELECT count(*) FROM "{marts.schema}"."{marts.name}"')


# --- backups (C10) ------------------------------------------------------------


def _backup_row(warehouse, **overrides):
    """Insert one ops.pgbackrest_health row and read its judged status back."""

    columns = {
        "stanza": "pdw",
        "repo_status": "ok",
        "repo_message": "",
        "last_full_at": "now() - interval '2 hours'",
        "last_diff_at": "now() - interval '2 hours'",
        "last_incr_at": "now() - interval '2 hours'",
        "last_backup_label": "20260826-120000F",
        "last_backup_type": "full",
        "backup_count": "3",
        "repo_bytes": "1",
        "wal_min": "000000010000000000000001",
        "wal_max": "000000010000000000000009",
        "archived_count": "100",
        "failed_count": "0",
        "last_archived_at": "now() - interval '2 minutes'",
        "last_attempt_at": "now() - interval '2 hours'",
        "last_attempt_type": "full",
        "last_attempt_ok": "1",
        "last_error": "",
        "collected_at": "now()",
    }
    columns.update(overrides)
    text_cols = {
        "stanza", "repo_status", "repo_message", "last_backup_label",
        "last_backup_type", "wal_min", "wal_max", "last_attempt_type", "last_error",
    }
    values = ", ".join(
        (f"'{v}'" if k in text_cols else str(v)) for k, v in columns.items()
    )
    warehouse._command("DELETE FROM @pgbackrest_health")
    warehouse._command(
        f"INSERT INTO @pgbackrest_health ({', '.join(columns)}) VALUES ({values})"
    )
    return warehouse._query_dicts("SELECT * FROM @marts_pgbackrest_health")[0]


def test_no_valid_backup_is_the_loudest_state_there_is(warehouse):
    """The exact production state of 2026-08-26 must not read green.

    `pgbackrest info` reported `status: error (no valid backups)` and had for a
    day. WAL archiving was perfect the whole time -- 2,696 segments, zero
    pending -- every pipeline read `ok`, and the backup loop logged "backup
    failed" every six hours to a stdout nothing escalates. The outage was
    visible only by running pgbackrest by hand.

    So this is the case the view exists for, and the WAL columns are exactly
    what must NOT be allowed to redeem it: you cannot restore from WAL alone.
    """

    warehouse.ensure_pipeline_health_tables()
    row = _backup_row(
        warehouse,
        backup_count=0,
        last_full_at="'1970-01-01 00:00:00+00'::timestamptz",
        last_diff_at="'1970-01-01 00:00:00+00'::timestamptz",
        last_incr_at="'1970-01-01 00:00:00+00'::timestamptz",
        repo_status="error",
        repo_message="no valid backups",
        # Archiving healthy, exactly as it was.
        last_archived_at="now() - interval '1 minute'",
        archived_count=2696,
    )
    assert row["status"] == "failing", (
        "a repository with no valid backup read %r; healthy WAL archiving must "
        "never redeem a missing base backup" % row["status"]
    )
    assert row["last_full_at"] is None, "the epoch sentinel must be translated to NULL"


def test_a_healthy_repository_reads_ok(warehouse):
    """Healthy means backups exist, WAL ships, AND a restore has been verified."""

    warehouse.ensure_pipeline_health_tables()
    _backup_row(warehouse)
    warehouse.record_pgbackrest_restore_drill(stanza="pdw", label="20260826-120000F", rows=1, note="")
    assert warehouse._query_dicts("SELECT * FROM @marts_pgbackrest_health")[0]["status"] == "ok"


def test_a_failing_loop_with_an_older_good_backup_is_attention_not_ok(warehouse):
    """Distinct from "no backup": the clock is running but the floor still holds."""

    warehouse.ensure_pipeline_health_tables()
    row = _backup_row(warehouse, last_attempt_ok=0, last_error="incr backup failed")
    assert row["status"] == "attention"


def test_stalled_wal_archiving_is_reported_even_with_a_recent_full(warehouse):
    """A backup is a floor, not a recovery point, once shipping stops."""

    warehouse.ensure_pipeline_health_tables()
    row = _backup_row(warehouse, last_archived_at="now() - interval '6 hours'")
    assert row["status"] == "attention"


def test_a_stale_snapshot_reports_unknown_rather_than_stale_facts(warehouse):
    """Store facts, derive status -- the rule the rest of marts_ops follows.

    A row nobody has refreshed cannot testify that backups are fine.
    """

    warehouse.ensure_pipeline_health_tables()
    row = _backup_row(warehouse, collected_at="now() - interval '30 days'")
    assert row["status"] == "unknown"


def test_a_fresh_daily_collation_snapshot_reads_ok_not_unknown(warehouse):
    """Level 4 was dark by construction, not clean.

    `marts_ops.collation_health` judged its snapshot against
    COLLECTOR_STALE_SECONDS (1 hour), but the collation_health asset runs DAILY
    at 03:41 -- it costs a bounded sequential scan of every unique index's heap,
    which is a daily amount of work. So all 252 of its rows read `unknown` for
    ~96% of every day. Measured on production 2026-08-26: 252 of 252 `unknown`.

    That is worse than a missing check. The one level that exists because
    Postgres CANNOT warn about collation drift on this database was reporting a
    permanent "no opinion", and a genuine finding would have looked identical to
    it.
    """

    warehouse.ensure_pipeline_health_tables()
    warehouse._command("DELETE FROM @collation_health")
    warehouse._command(
        """
        INSERT INTO @collation_health
            (object_id, scope, object_name, provider, recorded_version, actual_version,
             dependent_indexes, finding, detail, collected_at)
        VALUES ('db:test', 'database', 'pdw', 'libc', '2.36', '2.36', 1, 'ok', '',
                now() - interval '6 hours')
        """
    )
    row = warehouse._query_dicts("SELECT status FROM @marts_collation_health")[0]
    assert row["status"] == "ok", (
        "a snapshot six hours old from a DAILY collector read %r; judged against the "
        "ten-minutely collector's window, this level can only be non-unknown for one "
        "hour a day" % row["status"]
    )


def test_a_genuinely_abandoned_collation_snapshot_still_reads_unknown(warehouse):
    """Loosening the window must not remove the self-distrust that motivates it."""

    warehouse.ensure_pipeline_health_tables()
    warehouse._command("DELETE FROM @collation_health")
    warehouse._command(
        """
        INSERT INTO @collation_health
            (object_id, scope, object_name, provider, recorded_version, actual_version,
             dependent_indexes, finding, detail, collected_at)
        VALUES ('db:test', 'database', 'pdw', 'libc', '2.36', '2.36', 1, 'ok', '',
                now() - interval '9 days')
        """
    )
    row = warehouse._query_dicts("SELECT status FROM @marts_collation_health")[0]
    assert row["status"] == "unknown"


def test_a_backup_nobody_has_restored_reads_attention(warehouse):
    """A backup you have not restored is a hypothesis (C10).

    Until 2026-08-28 the only record that a restore had ever been performed
    was a commit message, and the view could not distinguish "restores fine"
    from "never tried". A healthy repository with no drill on record reads
    attention -- never failing, because the facts about the backups
    themselves are all good and this is the row saying "unverified".
    """

    warehouse.ensure_pipeline_health_tables()
    row = _backup_row(warehouse)
    assert row["status"] == "attention"
    assert row["restore_status"] == "never"
    assert row["last_restore_verified_at"] is None


def test_a_recorded_restore_drill_makes_the_row_ok(warehouse):
    warehouse.ensure_pipeline_health_tables()
    _backup_row(warehouse)
    warehouse.record_pgbackrest_restore_drill(
        stanza="pdw",
        label="20260827-032703F_20260827-050637D",
        rows=49_131_629,
        note="restored into a fresh volume, promoted, counted",
    )
    row = warehouse._query_dicts("SELECT * FROM @marts_pgbackrest_health")[0]
    assert row["status"] == "ok"
    assert row["restore_status"] == "ok"
    assert row["last_restore_label"] == "20260827-032703F_20260827-050637D"
    assert row["last_restore_rows"] == 49_131_629
    assert row["restore_age_seconds"] < 60
    # The loop's own facts survive the drill record untouched.
    assert row["backup_count"] == 3


def test_a_stale_restore_drill_reads_attention_again(warehouse):
    """Weekly fulls and a monthly drill: 45 days is one missed month plus margin."""

    warehouse.ensure_pipeline_health_tables()
    _backup_row(warehouse)
    warehouse.record_pgbackrest_restore_drill(
        stanza="pdw",
        label="20260701-000000F",
        rows=1,
        note="old",
        verified_at=datetime.now(UTC) - timedelta(days=46),
    )
    row = warehouse._query_dicts("SELECT * FROM @marts_pgbackrest_health")[0]
    assert row["status"] == "attention"
    assert row["restore_status"] == "stale"


def test_a_restore_drill_never_stands_in_for_a_backup_report(warehouse):
    """A drill recorded before the loop has reported must not read ok."""

    warehouse.ensure_pipeline_health_tables()
    warehouse._command("DELETE FROM @pgbackrest_health")
    warehouse.record_pgbackrest_restore_drill(stanza="pdw", label="x", rows=1, note="")
    row = warehouse._query_dicts("SELECT * FROM @marts_pgbackrest_health")[0]
    assert row["status"] == "unknown"


def test_a_restore_drill_must_carry_a_label_and_a_count(warehouse):
    warehouse.ensure_pipeline_health_tables()
    with pytest.raises(ValueError):
        warehouse.record_pgbackrest_restore_drill(stanza="pdw", label=" ", rows=1, note="")
    with pytest.raises(ValueError):
        warehouse.record_pgbackrest_restore_drill(stanza="pdw", label="x", rows=0, note="")


AGENT_BACKED_ENRICHMENT_PIPELINES = (
    "voice_memo_transcription",
    "voice_memo_enrichment",
    "receipt_enrichment",
)


def test_every_agent_backed_enrichment_pass_declares_a_heartbeat():
    """A pass that calls a provider per row must be able to read `failing`.

    Audit 2026-08-28: `voice_memo_enrichment` and `receipt_enrichment` read
    `run_status = unmonitored` -- no StateSource -- so a pass whose every
    agent call errored was indistinguishable from one with nothing to do.
    Each of these already writes a status/error row per attempt somewhere
    (the enrichments table, ops.ai_processing_agent_runs); the declaration is
    what makes the dashboard read it. They are history tables, one row per
    attempt, so each needs an error_window or a single dead row pins it red.
    """
    for pipeline_id in AGENT_BACKED_ENRICHMENT_PIPELINES:
        entry = pipeline(pipeline_id)
        assert entry.state is not None, f"{pipeline_id} has no heartbeat"
        assert entry.state.status_column, pipeline_id
        assert entry.state.error_column, pipeline_id
        assert entry.state.error_window is not None, pipeline_id


def test_receipt_heartbeat_reads_only_the_receipt_agents_runs():
    """ops.ai_processing_agent_runs is shared by every agent-backed pass, so
    the receipt StateSource must scope to its own task_type -- and to the one
    the runner actually stamps, or the row is permanently no_data."""
    from personal_data_warehouse.receipt_enrichment import RECEIPT_AGENT_TASK_TYPE

    entry = pipeline("receipt_enrichment")
    assert entry.state is not None
    assert entry.state.table == "agent_runs"
    assert entry.state.scope_column == "task_type"
    assert entry.state.scope_value == RECEIPT_AGENT_TASK_TYPE


def test_a_failed_voice_memo_enrichment_reads_failing(warehouse):
    """The enrichment row IS the pass's state: a failed agent run lands as
    status='error' with the exception text, and a later success overwrites it."""
    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    warehouse._command(
        """
        INSERT INTO @apple_voice_memos_enrichments
            (source, account, recording_id, provider, model, prompt_version, status, error, created_at, sync_version)
        VALUES ('apple_voice_memos', 'z', 'rec-1', 'agent_codex', 'm', 'v1', 'error',
                'agent run failed: container exited 1', %s, 1)
        """,
        (now,),
    )
    PipelineHealthCollector(warehouse).run()
    row = warehouse._query_dicts(
        "SELECT status, run_status, state_error_rows, last_error FROM @marts_pipeline_health"
        " WHERE pipeline = 'voice_memo_enrichment'"
    )[0]
    assert row["run_status"] != "unmonitored"
    assert row["status"] == "failing"
    assert row["state_error_rows"] == 1
    assert "container exited 1" in row["last_error"]


def test_a_failed_receipt_agent_run_reads_failing_and_other_agents_do_not(warehouse):
    """The receipt runner writes no receipt row on an agent failure (so the
    transaction is retried next run), which is exactly why its heartbeat has
    to come from the agent-runs table instead. Another pass's failure in that
    shared table must not colour the receipt row."""
    from personal_data_warehouse.receipt_enrichment import RECEIPT_AGENT_TASK_TYPE

    _provision_every_table(warehouse)
    now = datetime.now(tz=UTC)
    for run_id, task_type, status, error in (
        ("run-other", "gmail_attachment_enrichment", "error", "vision model timed out"),
        ("run-receipt-ok", RECEIPT_AGENT_TASK_TYPE, "completed", ""),
    ):
        warehouse._command(
            """
            INSERT INTO @agent_runs
                (run_id, provider, model, task_type, subject_id, prompt_version, status, error, started_at, completed_at, sync_version)
            VALUES (%s, 'codex', 'm', %s, 'ft_1', 'v', %s, %s, %s, %s, 1)
            """,
            (run_id, task_type, status, error, now, now),
        )
    PipelineHealthCollector(warehouse).run()
    row = warehouse._query_dicts(
        "SELECT status, run_status, state_error_rows, last_run_at FROM @marts_pipeline_health"
        " WHERE pipeline = 'receipt_enrichment'"
    )[0]
    assert row["run_status"] != "unmonitored"
    assert row["state_error_rows"] == 0
    assert row["status"] != "failing"
    assert row["last_run_at"] is not None

    warehouse._command(
        """
        INSERT INTO @agent_runs
            (run_id, provider, model, task_type, subject_id, prompt_version, status, error, started_at, completed_at, sync_version)
        VALUES ('run-receipt-bad', 'codex', 'm', %s, 'ft_2', 'v', 'error', 'container exited 1', %s, %s, 1)
        """,
        (RECEIPT_AGENT_TASK_TYPE, now, now),
    )
    PipelineHealthCollector(warehouse).run()
    row = warehouse._query_dicts(
        "SELECT status, state_error_rows, last_error FROM @marts_pipeline_health"
        " WHERE pipeline = 'receipt_enrichment'"
    )[0]
    assert row["status"] == "failing"
    assert row["state_error_rows"] == 1
    assert "container exited 1" in row["last_error"]
