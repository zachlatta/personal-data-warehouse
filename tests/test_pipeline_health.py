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
    LATE_MULTIPLIER,
    PIPELINE_KINDS,
    PIPELINES,
    PROBE_EMPTY,
    PROBE_MAX_UNINDEXED_BYTES,
    PROBE_MISSING,
    PROBE_NO_TIMESTAMP,
    PROBE_OK,
    PROBE_SKIPPED_UNINDEXED,
    PROBE_STATEMENT_TIMEOUT_MS,
    STALE_MULTIPLIER,
    TABLE_PIPELINES,
    TABLE_ROLES,
    PipelineHealthCollector,
    pipeline,
    pipeline_tables,
)
from personal_data_warehouse.postgres import POSTGRES_INDEXES, POSTGRES_TABLES, PostgresWarehouse
from personal_data_warehouse.relations import CANONICAL_RELATIONS, relation
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
            assert pipeline_tables(entry.id, role="state"), entry.id


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
    wh.ensure_plaid_tables()
    wh.ensure_finance_tables()
    wh.ensure_manual_finance_tables()
    wh.ensure_receipt_tables()
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
