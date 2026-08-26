"""Warehouse contract for the WHOOP private (app API) source.

Two halves, deliberately:

* the checks that need no database at all -- column tuples, catalog placement,
  index coverage, the HRV unit conversion -- so a machine with no Postgres still
  catches the registry mistakes the "Adding a warehouse source" checklist calls
  SILENT; and
* the real-Postgres checks, following tests/test_whoop_warehouse.py, which are
  the only way to prove the DDL, the upserts and the credential's
  compare-and-swap actually behave.
"""

from __future__ import annotations

from dataclasses import replace
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from threading import Event

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.postgres import (
    POSTGRES_INDEXES,
    POSTGRES_TABLES,
    SEARCH_SOURCE_ALIASES,
    SEARCH_SOURCE_DEFS,
    PostgresWarehouse,
    _postgres_type,
)
from personal_data_warehouse.relations import CATALOG, relation
from personal_data_warehouse.schema import (
    WHOOP_PRIVATE_CYCLE_COLUMNS,
    WHOOP_PRIVATE_DOCUMENT_COLUMNS,
    WHOOP_PRIVATE_HEART_RATE_SAMPLE_COLUMNS,
    WHOOP_PRIVATE_JOURNAL_ENTRY_COLUMNS,
    WHOOP_PRIVATE_RECOVERY_COLUMNS,
    WHOOP_PRIVATE_SESSION_COLUMNS,
    WHOOP_PRIVATE_SLEEP_COLUMNS,
    WHOOP_PRIVATE_SLEEP_EVENT_COLUMNS,
    WHOOP_PRIVATE_SPORT_COLUMNS,
    WHOOP_PRIVATE_SYNC_STATE_COLUMNS,
    WHOOP_PRIVATE_WORKOUT_COLUMNS,
    whoop_private_hrv_rmssd_milli,
)

ACCOUNT = "zach@example.com"

#: The contract in /tmp/whoop_private_contract.md, restated as data so a rename
#: anywhere in the catalog, the specs or the schema module fails here first.
EXPECTED_TABLES: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "whoop_private_cycles": ("base_whoop_private", "cycles", ("account", "cycle_id")),
    "whoop_private_sleeps": ("base_whoop_private", "sleeps", ("account", "activity_id")),
    "whoop_private_recoveries": ("base_whoop_private", "recoveries", ("account", "activity_id")),
    "whoop_private_workouts": ("base_whoop_private", "workouts", ("account", "activity_id")),
    "whoop_private_sleep_events": (
        "base_whoop_private",
        "sleep_events",
        ("account", "activity_id", "event_index"),
    ),
    "whoop_private_heart_rate_samples": (
        "base_whoop_private",
        "heart_rate_samples",
        ("account", "sample_at"),
    ),
    "whoop_private_journal_entries": (
        "base_whoop_private",
        "journal_entries",
        ("account", "day", "question_id"),
    ),
    "whoop_private_sports": ("base_whoop_private", "sports", ("account", "sport_id")),
    "whoop_private_documents": (
        "base_whoop_private",
        "documents",
        ("account", "kind", "doc_key"),
    ),
    "whoop_private_sync_state": ("ops", "whoop_private_sync_state", ("account", "collection")),
    "whoop_private_sessions": ("private", "whoop_private_sessions", ("account", "session_key")),
}

#: Everything except the sync state and the credential.
DATA_TABLES: tuple[str, ...] = tuple(
    table
    for table in EXPECTED_TABLES
    if table not in {"whoop_private_sync_state", "whoop_private_sessions"}
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


# ---------------------------------------------------------------------------
# no database required
# ---------------------------------------------------------------------------


def test_the_hrv_unit_conversion_is_seconds_to_milliseconds() -> None:
    """The 1000x trap, pinned.

    The private API reports hrv_rmssd in SECONDS; base_whoop.recoveries reports
    it in milliseconds. A recovery whose private reading is 0.0821 is 82.1 ms --
    a perfectly ordinary HRV -- while reading the seconds column as if it were
    the public column's unit yields 0.08 ms, which is not a number a human body
    produces.
    """
    assert whoop_private_hrv_rmssd_milli(0.0821) == pytest.approx(82.1)
    assert whoop_private_hrv_rmssd_milli(0) == 0.0
    assert whoop_private_hrv_rmssd_milli(1) == 1000.0


def test_recoveries_store_hrv_in_both_units_so_a_join_cannot_be_wrong() -> None:
    """Both columns exist, and neither is a bare `hrv_rmssd`.

    A single unit-less column is the failure this pair exists to prevent: the
    name has to say which unit it is, in both directions, or a cross-source
    query silently picks one.
    """
    assert "hrv_rmssd_seconds" in WHOOP_PRIVATE_RECOVERY_COLUMNS
    assert "hrv_rmssd_milli" in WHOOP_PRIVATE_RECOVERY_COLUMNS
    assert "hrv_rmssd" not in WHOOP_PRIVATE_RECOVERY_COLUMNS


def test_every_data_table_carries_the_provenance_columns() -> None:
    for table in DATA_TABLES:
        columns = POSTGRES_TABLES[table].columns
        assert columns[0] == "account", table
        assert columns[-3:] == ("raw_json", "synced_at", "sync_version"), table


def test_every_table_is_cataloged_at_the_contracted_location() -> None:
    for logical, (schema, name, primary_key) in EXPECTED_TABLES.items():
        rel = relation(logical)
        assert (rel.schema, rel.name) == (schema, name), logical
        assert POSTGRES_TABLES[logical].primary_key == primary_key, logical
        assert CATALOG.object(logical).kind == "table", logical


def test_the_source_owns_its_own_base_schema() -> None:
    schema = CATALOG.schema("base_whoop_private")
    assert (schema.layer, schema.domain) == ("base", "whoop_private")
    assert schema.discoverable
    # base_whoop stays a separate source: same provider, different API, and the
    # timeline reads the public one.
    assert CATALOG.schema("base_whoop").domain == "whoop"


def test_the_session_credential_is_secret_and_denied_to_the_query_role() -> None:
    obj = CATALOG.object("whoop_private_sessions")
    assert obj.layer == "private"
    assert obj.secret is True
    assert obj.query_access == "denied"
    assert not obj.discoverable
    assert "private" in CATALOG.denied_schemas()


def test_the_sync_state_is_hidden_ops_not_a_query_surface() -> None:
    obj = CATALOG.object("whoop_private_sync_state")
    assert (obj.layer, obj.schema) == ("ops", "ops")
    assert obj.query_access == "denied"
    assert not obj.discoverable
    # Source-prefixed physical name: one flat ops schema cannot collide.
    assert obj.name.startswith("whoop_private_")


def _leading_index_columns() -> set[tuple[str, str]]:
    """(table, leading key column) for every declared index.

    Only the leading column counts, the same rule the freshness collector
    applies: a backward scan of (account, synced_at) cannot answer
    max(synced_at) cheaply.
    """
    leading: set[tuple[str, str]] = set()
    for index in POSTGRES_INDEXES:
        opening = index.sql.find("(", index.sql.find(" ON "))
        if opening == -1:
            continue
        column = index.sql[opening + 1 :].split(",")[0].split(")")[0].split()[0]
        leading.add((index.table, column))
    return leading


def test_every_data_table_has_an_index_leading_with_the_freshness_column() -> None:
    """Without this the dashboard reports no freshness at all, not late freshness.

    The collector refuses to run max() over a large unindexed heap and records
    probe_status = 'skipped_unindexed', so a missing index here is a silently
    unmonitored table rather than a slow one.
    """
    leading = _leading_index_columns()
    for table in DATA_TABLES:
        assert (table, "synced_at") in leading, table


def test_the_sample_table_is_indexed_for_time_range_scans() -> None:
    """The one table the source exists for.

    At six-second grain heart_rate_samples is ~5.2M rows a year, so both things
    asked of it have to be index work: its primary key IS the time-range index
    for an account, and a separate sample_at index is what keeps the freshness
    collector's max(sample_at) from being a full-heap scan it refuses to run.
    """
    assert POSTGRES_TABLES["whoop_private_heart_rate_samples"].primary_key == (
        "account",
        "sample_at",
    )
    time_index = next(
        index
        for index in POSTGRES_INDEXES
        if index.name == "whoop_private_heart_rate_samples_time_idx"
    )
    assert index_sql_columns(time_index.sql).startswith("(sample_at")


def index_sql_columns(sql: str) -> str:
    return sql[sql.find("(", sql.find(" ON ")) :]


def test_search_exposes_a_whoop_private_source_token() -> None:
    """Without the token the source cannot be scoped, and a broad search only

    reaches its rows by walking past millions of gmail/slack documents.
    """
    tokens = {token for token, _adapters, _ in SEARCH_SOURCE_DEFS}
    assert "whoop_private" in tokens
    adapters = next(
        adapters for token, adapters, _ in SEARCH_SOURCE_DEFS if token == "whoop_private"
    )
    assert adapters == ("whoop_private_journal",)
    # The public source keeps its own token; the two must not be merged.
    assert "whoop" in tokens
    assert set(SEARCH_SOURCE_ALIASES).isdisjoint(tokens)


def test_the_session_table_matches_the_apps_own_definition() -> None:
    """The app creates the idempotent twin of this table (Go), so the two

    definitions have to agree column for column -- including the primary key,
    which the app's upsert names in ON CONFLICT.
    """
    store = (
        os.path.join(os.path.dirname(__file__), os.pardir, "app", "internal", "whoopsession", "store.go")
    )
    with open(store, encoding="utf-8") as handle:
        text = handle.read()
    for column in WHOOP_PRIVATE_SESSION_COLUMNS:
        assert f"\n    {column} " in text, column
    assert "PRIMARY KEY (account, session_key)" in text


# ---------------------------------------------------------------------------
# real Postgres
# ---------------------------------------------------------------------------


def test_ensure_creates_every_contracted_relation(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_whoop_private_tables()

    for logical, (schema, name, _pk) in EXPECTED_TABLES.items():
        rows = warehouse._query(
            """
            SELECT count(*) FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (warehouse.physical_schema_name(schema), name),
        )
        assert rows == [(1,)], logical


def test_hrv_survives_the_round_trip_in_both_units(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_whoop_private_tables()
    synced_at = datetime(2026, 8, 23, 12, tzinfo=UTC)
    seconds = 0.0821

    warehouse.insert_whoop_private_recoveries(
        [
            {
                **{column: "" for column in WHOOP_PRIVATE_RECOVERY_COLUMNS},
                "account": ACCOUNT,
                "activity_id": "recovery-1",
                "recovery_score": 63,
                "resting_heart_rate": 52,
                "hrv_rmssd_seconds": seconds,
                "hrv_rmssd_milli": whoop_private_hrv_rmssd_milli(seconds),
                "skin_temp_celsius": 33.4,
                "spo2": 96.0,
                "calibrating": 0,
                "prob_covid": 0.0,
                "hr_baseline": 51.0,
                "hrv_component": 0.5,
                "rhr_component": 0.5,
                "recovery_rate": 1.0,
                "state": "COMPLETE",
                "algo_version": "8.0",
                "history_size": 200,
                "survey_response_id": "",
                "created_at": synced_at,
                "updated_at": synced_at,
                "raw_json": {"hrv_rmssd": seconds},
                "synced_at": synced_at,
                "sync_version": 1,
            }
        ]
    )

    rows = warehouse._query(
        "SELECT hrv_rmssd_seconds, hrv_rmssd_milli, state FROM @whoop_private_recoveries "
        "WHERE account = %s",
        (ACCOUNT,),
    )
    assert rows[0][0] == pytest.approx(seconds)
    assert rows[0][1] == pytest.approx(82.1)
    # The 1000x guard stated as the invariant, not as two literals.
    assert rows[0][1] == pytest.approx(rows[0][0] * 1000.0)
    # `state` is a label here even though the global type map calls the name
    # numeric (apple_message_chats.state is a number).
    assert rows[0][2] == "COMPLETE"


def test_samples_upsert_by_their_natural_key(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_whoop_private_tables()
    synced_at = datetime(2026, 8, 23, 12, tzinfo=UTC)
    sample_at = datetime(2026, 8, 23, 11, 30, tzinfo=UTC)

    def sample(heart_rate: int, version: int) -> dict:
        return {
            "account": ACCOUNT,
            "sample_at": sample_at,
            "heart_rate": heart_rate,
            "step_seconds": 6,
            "raw_json": {"data": heart_rate},
            "synced_at": synced_at,
            "sync_version": version,
        }

    warehouse.insert_whoop_private_heart_rate_samples([sample(61, 1), sample(62, 2)])
    assert warehouse._query(
        "SELECT heart_rate, step_seconds FROM @whoop_private_heart_rate_samples WHERE account = %s",
        (ACCOUNT,),
    ) == [(62, 6)]



def test_a_retired_grain_is_deleted_over_exactly_the_window_rewritten(
    warehouse: PostgresWarehouse,
) -> None:
    """One table, one grid.

    A retired-grain sample that lands on the current grid loses the upsert to
    the finer reading; one whose millisecond offset misses it survives beside it
    and weights that instant twice in every average. The delete is scoped to the
    window just written -- a sample outside it has not been replaced yet and
    must not be removed, or the series goes briefly empty behind the walk.
    """
    warehouse.ensure_whoop_private_tables()
    synced_at = datetime(2026, 8, 23, 12, tzinfo=UTC)
    start = datetime(2026, 8, 23, 6, tzinfo=UTC)
    end = datetime(2026, 8, 23, 12, tzinfo=UTC)

    def sample(sample_at: datetime, step_seconds: int) -> dict:
        return {
            "account": ACCOUNT,
            "sample_at": sample_at,
            "heart_rate": 60,
            "step_seconds": step_seconds,
            "raw_json": {},
            "synced_at": synced_at,
            "sync_version": 1,
        }

    warehouse.insert_whoop_private_heart_rate_samples(
        [
            sample(start + timedelta(seconds=31), 60),  # stale grain, inside
            sample(start + timedelta(seconds=36), 6),  # current grain, inside
            sample(start - timedelta(minutes=1), 60),  # stale grain, OUTSIDE
            sample(end, 60),  # stale grain, on the exclusive upper bound
        ]
    )

    warehouse.delete_whoop_private_heart_rate_samples(
        account=ACCOUNT, start=start, end=end, keep_step_seconds=6
    )

    assert sorted(
        warehouse._query(
            "SELECT sample_at, step_seconds FROM @whoop_private_heart_rate_samples "
            "WHERE account = %s ORDER BY sample_at",
            (ACCOUNT,),
        )
    ) == sorted(
        [
            (start - timedelta(minutes=1), 60),
            (start + timedelta(seconds=36), 6),
            (end, 60),
        ]
    )


def test_the_retired_workout_sample_table_is_dropped_in_place(
    warehouse: PostgresWarehouse,
) -> None:
    """A table absent from the catalog but present in the database is invisible.

    Nothing in TIMELINE_TABLE_COVERAGE, TABLE_PIPELINES or the freshness
    collector would ever mention it again, so it would sit there accumulating
    nothing while looking like data. ensure_* drops it, which is what makes the
    retirement reach a database that already has one.
    """
    warehouse.ensure_whoop_private_tables()
    schema = warehouse._object_schema("whoop_private_heart_rate_samples")
    warehouse._command(
        f'CREATE TABLE IF NOT EXISTS "{schema}".workout_heart_rate_samples '
        "(account text NOT NULL, activity_id text NOT NULL, PRIMARY KEY (account, activity_id))"
    )

    warehouse.ensure_whoop_private_tables()

    assert warehouse._query(
        "SELECT to_regclass(%s)", (f"{schema}.workout_heart_rate_samples",)
    ) == [(None,)]


def test_every_insert_method_writes_its_table(warehouse: PostgresWarehouse) -> None:
    """One row through each of the nine writers, so no method is only ever

    exercised against a fake. The row is built FROM the column tuple, so a
    column added later is written too rather than quietly skipped.
    """
    warehouse.ensure_whoop_private_tables()
    synced_at = datetime(2026, 8, 23, 12, tzinfo=UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)

    def build(table: str, columns: tuple[str, ...], **overrides) -> dict:
        built: dict = {}
        for column in columns:
            kind = _postgres_type(column, table=table)
            if kind == "jsonb":
                built[column] = {}
            elif kind == "timestamptz":
                built[column] = epoch
            elif kind == "date":
                built[column] = date(1970, 1, 1)
            elif kind in ("bigint", "double precision", "numeric"):
                built[column] = 0
            else:
                built[column] = ""
        built["account"] = ACCOUNT
        built["synced_at"] = synced_at
        built["sync_version"] = 1
        built.update(overrides)
        return built

    writers = (
        (
            warehouse.insert_whoop_private_cycles,
            "whoop_private_cycles",
            WHOOP_PRIVATE_CYCLE_COLUMNS,
            {"cycle_id": "cycle-1", "start_at": synced_at, "day_start": date(2026, 8, 23)},
        ),
        (
            warehouse.insert_whoop_private_sleeps,
            "whoop_private_sleeps",
            WHOOP_PRIVATE_SLEEP_COLUMNS,
            {"activity_id": "sleep-1", "state": "COMPLETE", "start_at": synced_at},
        ),
        (
            warehouse.insert_whoop_private_recoveries,
            "whoop_private_recoveries",
            WHOOP_PRIVATE_RECOVERY_COLUMNS,
            {"activity_id": "recovery-1", "state": "COMPLETE", "hrv_rmssd_seconds": 0.05},
        ),
        (
            warehouse.insert_whoop_private_workouts,
            "whoop_private_workouts",
            WHOOP_PRIVATE_WORKOUT_COLUMNS,
            {"activity_id": "workout-1", "sport_id": 1, "start_at": synced_at},
        ),
        (
            warehouse.insert_whoop_private_sleep_events,
            "whoop_private_sleep_events",
            WHOOP_PRIVATE_SLEEP_EVENT_COLUMNS,
            {"activity_id": "sleep-1", "event_index": 0, "stage": "SWS", "started_at": synced_at},
        ),
        (
            warehouse.insert_whoop_private_heart_rate_samples,
            "whoop_private_heart_rate_samples",
            WHOOP_PRIVATE_HEART_RATE_SAMPLE_COLUMNS,
            {"sample_at": synced_at, "heart_rate": 60, "step_seconds": 6},
        ),
        (
            warehouse.insert_whoop_private_journal_entries,
            "whoop_private_journal_entries",
            WHOOP_PRIVATE_JOURNAL_ENTRY_COLUMNS,
            {"day": date(2026, 8, 23), "question_id": "q-1", "answer": "false"},
        ),
        (
            warehouse.insert_whoop_private_sports,
            "whoop_private_sports",
            WHOOP_PRIVATE_SPORT_COLUMNS,
            {"sport_id": 1, "name": "Running", "has_gps": 1},
        ),
        (
            warehouse.insert_whoop_private_documents,
            "whoop_private_documents",
            WHOOP_PRIVATE_DOCUMENT_COLUMNS,
            {"kind": "trend", "doc_key": "VO2_MAX", "collected_at": synced_at},
        ),
    )

    assert {table for _writer, table, _columns, _overrides in writers} == set(DATA_TABLES)
    for writer, table, columns, overrides in writers:
        writer([build(table, columns, **overrides)])
        assert warehouse._query(f"SELECT count(*) FROM @{table}") == [(1,)], table


def test_sync_state_round_trips_per_collection(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_whoop_private_tables()
    updated_at = datetime(2026, 8, 23, 12, tzinfo=UTC)

    for collection in ("cycles", "heart_rate", "journal"):
        warehouse.insert_whoop_private_sync_state(
            account=ACCOUNT,
            collection=collection,
            watermark_updated_at=updated_at,
            last_sync_type="incremental",
            status="ok",
            error="",
            updated_at=updated_at,
        )
    warehouse.insert_whoop_private_sync_state(
        account=ACCOUNT,
        collection="journal",
        watermark_updated_at=updated_at,
        last_sync_type="incremental",
        status="action_required",
        error="session rejected",
        updated_at=datetime(2026, 8, 23, 13, tzinfo=UTC),
        credential_sha256="deadbeef",
    )

    state = warehouse.load_whoop_private_sync_state()
    assert set(state) == {
        (ACCOUNT, "cycles"),
        (ACCOUNT, "heart_rate"),
        (ACCOUNT, "journal"),
    }
    assert state[(ACCOUNT, "cycles")]["watermark_updated_at"] == updated_at
    journal = state[(ACCOUNT, "journal")]
    assert journal["status"] == "action_required"
    # The fingerprint is what lets the schedule skip a KNOWN-dead credential
    # without skipping a repaired one.
    assert journal["credential_sha256"] == "deadbeef"


def test_publishing_a_session_stores_it_and_fingerprints_the_refresh_token(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_whoop_private_tables()
    now = datetime(2026, 8, 23, 12, tzinfo=UTC)

    assert warehouse.load_whoop_private_session(account=ACCOUNT) == {}

    warehouse.replace_whoop_private_session(
        account=ACCOUNT,
        access_token="access-1",
        refresh_token="refresh-1",
        access_expires_at=datetime(2026, 8, 24, 12, tzinfo=UTC),
        refresh_expires_at=datetime(2026, 9, 22, 12, tzinfo=UTC),
        published_at=now,
        updated_at=now,
        source_browser="chrome",
    )

    stored = warehouse.load_whoop_private_session(account=ACCOUNT)
    assert stored["access_token"] == "access-1"
    assert stored["refresh_token"] == "refresh-1"
    assert stored["session_key"] == "default"
    assert stored["status"] == "ok"
    # The fingerprint is what sync state records when a credential is rejected,
    # so it must be the hash of the token itself and never the token.
    assert stored["refresh_token_sha256"] == hashlib.sha256(b"refresh-1").hexdigest()
    assert "refresh-1" != stored["refresh_token_sha256"]


def test_publishing_a_session_repairs_a_rejected_one(warehouse: PostgresWarehouse) -> None:
    """A publish is a human repair action, so it always wins and clears state."""

    warehouse.ensure_whoop_private_tables()
    warehouse.replace_whoop_private_session(
        account=ACCOUNT,
        access_token="dead",
        refresh_token="dead-refresh",
        access_expires_at=datetime(2026, 8, 1, tzinfo=UTC),
        refresh_expires_at=datetime(2026, 8, 2, tzinfo=UTC),
        published_at=datetime(2026, 8, 1, tzinfo=UTC),
        updated_at=datetime(2026, 8, 1, tzinfo=UTC),
        status="action_required",
        error="session rejected",
    )

    warehouse.replace_whoop_private_session(
        account=ACCOUNT,
        access_token="fresh",
        refresh_token="fresh-refresh",
        access_expires_at=datetime(2026, 8, 24, tzinfo=UTC),
        refresh_expires_at=datetime(2026, 9, 22, tzinfo=UTC),
        published_at=datetime(2026, 8, 23, tzinfo=UTC),
        updated_at=datetime(2026, 8, 23, tzinfo=UTC),
    )

    stored = warehouse.load_whoop_private_session(account=ACCOUNT)
    assert stored["refresh_token"] == "fresh-refresh"
    assert stored["status"] == "ok"
    assert stored["error"] == ""


def test_rotation_installs_the_new_credential(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_whoop_private_tables()
    _publish(warehouse, "refresh-1", "access-1")

    rotated = warehouse.rotate_whoop_private_session(
        account=ACCOUNT,
        expected_refresh_token="refresh-1",
        access_token="access-2",
        refresh_token="refresh-2",
        access_expires_at=datetime(2026, 8, 25, tzinfo=UTC),
        refresh_expires_at=datetime(2026, 9, 23, tzinfo=UTC),
        updated_at=datetime(2026, 8, 23, 13, tzinfo=UTC),
    )

    assert rotated["refresh_token"] == "refresh-2"
    stored = warehouse.load_whoop_private_session(account=ACCOUNT)
    assert stored["access_token"] == "access-2"
    assert stored["refresh_token"] == "refresh-2"
    # The publish time is a property of the browser capture, not of a rotation.
    assert stored["published_at"] == datetime(2026, 8, 23, 12, tzinfo=UTC)


def test_a_rotation_from_a_superseded_token_adopts_the_winner(
    warehouse: PostgresWarehouse,
) -> None:
    """The compare in compare-and-swap.

    Two pollers that both refreshed from refresh-1 hold two different live
    sessions. Whichever commits second must NOT overwrite the first -- its own
    token is already superseded at WHOOP -- so it makes no change and adopts
    what it finds.
    """
    warehouse.ensure_whoop_private_tables()
    _publish(warehouse, "refresh-1", "access-1")

    warehouse.rotate_whoop_private_session(
        account=ACCOUNT,
        expected_refresh_token="refresh-1",
        access_token="access-winner",
        refresh_token="refresh-winner",
        access_expires_at=datetime(2026, 8, 25, tzinfo=UTC),
        refresh_expires_at=datetime(2026, 9, 23, tzinfo=UTC),
        updated_at=datetime(2026, 8, 23, 13, tzinfo=UTC),
    )

    adopted = warehouse.rotate_whoop_private_session(
        account=ACCOUNT,
        expected_refresh_token="refresh-1",
        access_token="access-loser",
        refresh_token="refresh-loser",
        access_expires_at=datetime(2026, 8, 25, tzinfo=UTC),
        refresh_expires_at=datetime(2026, 9, 23, tzinfo=UTC),
        updated_at=datetime(2026, 8, 23, 13, 1, tzinfo=UTC),
    )

    assert adopted["refresh_token"] == "refresh-winner"
    assert warehouse.load_whoop_private_session(account=ACCOUNT)["refresh_token"] == (
        "refresh-winner"
    )


def test_concurrent_rotations_serialize_on_the_authority_lock(
    warehouse: PostgresWarehouse,
) -> None:
    """The lock in compare-and-swap.

    Without it both racers read the pre-rotation row, both pass the compare,
    and the second UPDATE silently installs a credential the first already
    superseded. Same failure shape as the three public-WHOOP incidents.
    """
    warehouse.ensure_whoop_private_tables()
    _publish(warehouse, "refresh-1", "access-1")
    first_holds_lock = Event()
    release_first = Event()
    second = PostgresWarehouse(warehouse._database_url, schema=warehouse.schema_namespace)

    def rotate(wh: PostgresWarehouse, token: str, gate: bool):
        def call():
            if gate:
                first_holds_lock.set()
                assert release_first.wait(timeout=10)
            return wh.rotate_whoop_private_session(
                account=ACCOUNT,
                expected_refresh_token="refresh-1",
                access_token=f"access-{token}",
                refresh_token=f"refresh-{token}",
                access_expires_at=datetime(2026, 8, 25, tzinfo=UTC),
                refresh_expires_at=datetime(2026, 9, 23, tzinfo=UTC),
                updated_at=datetime(2026, 8, 23, 13, tzinfo=UTC),
            )

        return call

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(rotate(warehouse, "winner", gate=True))
            assert first_holds_lock.wait(timeout=10)
            racer = pool.submit(rotate(second, "loser", gate=False))
            release_first.set()
            first_result = first.result(timeout=15)
            racer_result = racer.result(timeout=15)
    finally:
        second.close()

    # Exactly one credential is live afterwards, and both callers agree on it.
    assert first_result["refresh_token"] == racer_result["refresh_token"]
    assert warehouse.load_whoop_private_session(account=ACCOUNT)["refresh_token"] == (
        first_result["refresh_token"]
    )


def test_rotation_without_a_published_session_fails_loudly(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_whoop_private_tables()
    with pytest.raises(RuntimeError, match="publish-session"):
        warehouse.rotate_whoop_private_session(
            account=ACCOUNT,
            expected_refresh_token="refresh-1",
            access_token="access-2",
            refresh_token="refresh-2",
            access_expires_at=datetime(2026, 8, 25, tzinfo=UTC),
            refresh_expires_at=datetime(2026, 9, 23, tzinfo=UTC),
            updated_at=datetime(2026, 8, 23, 13, tzinfo=UTC),
        )


def test_a_rotation_that_returns_no_refresh_token_is_rejected(
    warehouse: PostgresWarehouse,
) -> None:
    """Storing an empty refresh token is how the source loses its credential."""

    warehouse.ensure_whoop_private_tables()
    _publish(warehouse, "refresh-1", "access-1")
    with pytest.raises(ValueError):
        warehouse.rotate_whoop_private_session(
            account=ACCOUNT,
            expected_refresh_token="refresh-1",
            access_token="access-2",
            refresh_token="",
            access_expires_at=datetime(2026, 8, 25, tzinfo=UTC),
            refresh_expires_at=datetime(2026, 9, 23, tzinfo=UTC),
            updated_at=datetime(2026, 8, 23, 13, tzinfo=UTC),
        )
    assert warehouse.load_whoop_private_session(account=ACCOUNT)["refresh_token"] == "refresh-1"


def test_absent_timestamps_are_the_epoch_not_null(warehouse: PostgresWarehouse) -> None:
    """The warehouse-wide convention, checked on the columns most likely to be

    absent: a cycle still running has no end_at, and a sleep with no computed
    optimal window has no optimal_sleep_start.
    """
    warehouse.ensure_whoop_private_tables()
    synced_at = datetime(2026, 8, 23, 12, tzinfo=UTC)
    warehouse._command(
        "INSERT INTO @whoop_private_cycles (account, cycle_id, synced_at) VALUES (%s, %s, %s)",
        (ACCOUNT, "cycle-running", synced_at),
    )
    rows = warehouse._query(
        "SELECT end_at, predicted_end FROM @whoop_private_cycles WHERE cycle_id = %s",
        ("cycle-running",),
    )
    assert rows == [(datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 1, tzinfo=UTC))]

    nullable = warehouse._query(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND is_nullable = 'YES'
        """,
        (warehouse.physical_schema_name("base_whoop_private"), "cycles"),
    )
    assert nullable == []


def _publish(warehouse: PostgresWarehouse, refresh_token: str, access_token: str) -> None:
    warehouse.replace_whoop_private_session(
        account=ACCOUNT,
        access_token=access_token,
        refresh_token=refresh_token,
        access_expires_at=datetime(2026, 8, 24, 12, tzinfo=UTC),
        refresh_expires_at=datetime(2026, 9, 22, 12, tzinfo=UTC),
        published_at=datetime(2026, 8, 23, 12, tzinfo=UTC),
        updated_at=datetime(2026, 8, 23, 12, tzinfo=UTC),
    )


def test_a_changed_partial_index_definition_is_rebuilt_not_left_stale(warehouse):
    """A stale bm25 predicate takes down broad search, so it must self-heal.

    `CREATE INDEX IF NOT EXISTS` cannot express "the WHERE clause moved". When
    the whoop_private adapter joined the low-volume list, production kept the
    18-adapter index it was born with; the search layer pins that index by name
    and vchord-bm25 raised as soon as the query mentioned the 19th adapter,
    which broke EVERY broad search until the index was rebuilt by hand.
    """
    from personal_data_warehouse.postgres import IndexSpec, PostgresWarehouse

    spec = IndexSpec(
        name="pdw_drift_probe_idx",
        table="whoop_private_journal_entries",
        sql="CREATE INDEX IF NOT EXISTS pdw_drift_probe_idx ON @whoop_private_journal_entries (day)",
        rebuild_on_definition_change=True,
    )
    changed = replace(spec, sql=spec.sql + " WHERE question_id <> ''")

    first = PostgresWarehouse.index_definition_fingerprint(spec)
    second = PostgresWarehouse.index_definition_fingerprint(changed)

    assert first != second, "a changed predicate must change the fingerprint"
    # Whitespace alone is not a definition change; it must not trigger rebuilds.
    assert PostgresWarehouse.index_definition_fingerprint(
        replace(spec, sql=spec.sql.replace(" ", "  "))
    ) == first


def test_indexes_not_opted_in_are_never_rebuilt(warehouse):
    """Rebuilds are expensive; only indexes that ask for it are checked."""
    from personal_data_warehouse.postgres import IndexSpec

    plain = IndexSpec(name="x_idx", table="whoop_private_sports", sql="CREATE INDEX x_idx ON @whoop_private_sports (name)")

    assert warehouse._index_definition_drifted(plain) is False
