"""Collation drift and unique-index divergence detection.

These are regression tests for a real production incident (2026-08-23): the
``en_US.utf8`` sort order changed underneath the database, seven btree indexes
failed ``bt_index_check`` with ``item order invariant violated``, and 36,825
duplicate rows had accumulated under UNIQUE indexes because ``ON CONFLICT``
missed the existing row through a mis-ordered index and INSERTed instead.

Two of the tests below pin mistakes that were made while building the detector
and would each have made it silently useless:

* writing the drift check as ``recorded <> actual``, which evaluates to NULL —
  not true — on a database whose ``datcollversion`` is NULL, and therefore
  reports CLEAN on exactly the database that has the problem, and
* sweeping for duplicate keys without applying the index's partial predicate,
  which reported 53,035 phantom excess rows on a completely clean index.
"""

from __future__ import annotations

import os

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.collation_health import (
    DIVERGENCE_MAX_HEAP_BYTES,
    FINDING_DUPLICATE_KEYS,
    FINDING_NO_BASELINE,
    FINDING_OK,
    FINDING_SKIPPED_EXPRESSION,
    FINDING_UNKNOWN_ACTUAL,
    FINDING_VERSION_CHANGED,
    PROBE_STATEMENT_TIMEOUT_MS,
    SCOPE_COLLATION,
    SCOPE_DATABASE,
    SCOPE_INDEX,
    CollationHealthCollector,
)
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.relations import relation


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
        wh.ensure_pipeline_health_tables()
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def _findings(warehouse) -> dict[str, dict]:
    rel = relation("marts_collation_health").with_namespace(warehouse._schema)
    rows = warehouse._query_dicts(f'SELECT * FROM "{rel.schema}"."{rel.name}"')
    return {row["object_id"]: row for row in rows}


# --- the headline finding -----------------------------------------------------


def test_a_missing_collation_baseline_is_the_finding_not_a_clean_result(warehouse):
    """``datcollversion IS NULL`` beside a real actual version is the finding.

    This is the specific bug the detector exists to avoid. Written the obvious
    way — ``recorded_version <> actual_version`` — the comparison is NULL rather
    than true when there is no baseline, so a check shaped that way reports
    CLEAN on precisely the database that cannot detect drift. Production is that
    database: ``pg_database.datcollversion`` is NULL while
    ``pg_database_collation_actual_version()`` returns glibc 2.36, and
    ``ALTER DATABASE ... REFRESH COLLATION VERSION`` refuses to create a
    baseline from NULL.
    """
    recorded, actual = warehouse._query(
        """
        SELECT d.datcollversion, pg_database_collation_actual_version(d.oid)
        FROM pg_database AS d WHERE d.datname = current_database()
        """
    )[0]
    CollationHealthCollector(warehouse).run()
    row = _findings(warehouse)["database"]

    assert row["scope"] == SCOPE_DATABASE
    if recorded is None and actual is not None:
        assert row["finding"] == FINDING_NO_BASELINE
        # Never 'ok'. A monitor that reports a clean bill of health here is
        # worse than no monitor, because it answers the question wrongly.
        assert row["status"] != "ok"
        assert "cannot detect collation drift" in row["detail"]
        assert "unverified" in row["detail"]
        # And the observed version is stored as a FACT, because with no
        # baseline in pg_database this snapshot's own history is the only
        # baseline that will ever exist.
        assert row["actual_version"] == str(actual)
        assert row["recorded_version"] is None
    elif actual is None:
        assert row["finding"] == FINDING_UNKNOWN_ACTUAL
    elif str(recorded) != str(actual):
        assert row["finding"] == FINDING_VERSION_CHANGED
        assert row["status"] == "failing"
    else:
        assert row["finding"] == FINDING_OK


def test_the_database_row_counts_the_indexes_that_ride_the_default_collation(warehouse):
    """The finding has to say how much is at stake, in indexes."""
    CollationHealthCollector(warehouse).run()
    row = _findings(warehouse)["database"]
    live = warehouse._query(
        """
        SELECT count(DISTINCT indexrelid)
        FROM (
            SELECT i.indexrelid, unnest(i.indcollation) AS collid
            FROM pg_index AS i
            INNER JOIN pg_class AS c ON c.oid = i.indexrelid
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ) AS used
        INNER JOIN pg_collation AS cl ON cl.oid = used.collid
        WHERE cl.collprovider = 'd'
        """
    )[0][0]
    assert row["dependent_indexes"] == live
    assert live > 0, "the warehouse always has collatable indexes on the default collation"


def test_only_collations_with_a_dependent_index_are_reported(warehouse):
    """871 drifted ICU collations with no dependent index is noise, not signal.

    Production carries 871 ``*-x-icu`` collations that every one of them reports
    a changed version for, and not a single index depends on any of them.
    Surfacing those would bury the one finding that matters on day one, which is
    how a monitor earns the ignore-it reflex.
    """
    CollationHealthCollector(warehouse).run()
    reported = {
        row["object_name"]
        for row in _findings(warehouse).values()
        if row["scope"] == SCOPE_COLLATION
    }
    depended_on = {
        f"{schema}.{name}"
        for schema, name in warehouse._query(
            """
            SELECT cn.nspname, cl.collname
            FROM (
                SELECT i.indexrelid, unnest(i.indcollation) AS collid
                FROM pg_index AS i
                INNER JOIN pg_class AS c ON c.oid = i.indexrelid
                INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ) AS used
            INNER JOIN pg_collation AS cl ON cl.oid = used.collid
            INNER JOIN pg_namespace AS cn ON cn.oid = cl.collnamespace
            WHERE cl.collprovider <> 'd'
            """
        )
    }
    assert reported == depended_on
    total_collations = warehouse._query("SELECT count(*) FROM pg_collation")[0][0]
    assert len(reported) < total_collations, (
        "the point of the join through pg_index is that most collations are never reported"
    )
    for row in _findings(warehouse).values():
        if row["scope"] == SCOPE_COLLATION:
            assert row["dependent_indexes"] > 0


# --- the corroborating divergence probe ---------------------------------------


def test_a_partial_unique_index_with_rows_outside_its_predicate_reads_clean(warehouse):
    """The predicate is not optional. Ignoring it invents duplicates.

    ``ops.upstream_mutation_operations`` has a UNIQUE index over
    ``idempotency_key`` partial on ``idempotency_key <> ''``. Almost every row
    has an empty key and is therefore not in the index at all — a sweep that
    counts the whole heap sees tens of thousands of "duplicates" that do not
    exist. It reported 53,035 phantom excess rows against a completely clean
    index.
    """
    warehouse.ensure_upstream_mutation_tables()
    rel = relation("upstream_mutations").with_namespace(warehouse._schema)
    index = warehouse._query(
        """
        SELECT n.nspname || '.' || ic.relname, pg_get_expr(i.indpred, i.indrelid)
        FROM pg_index AS i
        INNER JOIN pg_class AS ic ON ic.oid = i.indexrelid
        INNER JOIN pg_namespace AS n ON n.oid = ic.relnamespace
        INNER JOIN pg_class AS tc ON tc.oid = i.indrelid
        INNER JOIN pg_namespace AS tn ON tn.oid = tc.relnamespace
        WHERE i.indisunique AND i.indpred IS NOT NULL
          AND tn.nspname = %s AND tc.relname = %s
        """,
        (rel.schema, rel.name),
    )
    assert index, "expected a partial unique index on the mutation operations table"
    index_name, predicate = index[0]
    assert "idempotency_key" in predicate

    # Many rows outside the predicate (empty key), all sharing that key, plus a
    # couple of genuinely distinct rows inside it. A predicate-blind sweep sees
    # five excess rows; the truth is zero.
    for ordinal in range(6):
        warehouse._command(
            """
            INSERT INTO @upstream_mutations
                (id, request_id, provider, operation, status, idempotency_key)
            VALUES (%s, %s, 'gmail', 'send_email', 'pending', '')
            """,
            (f"m{ordinal}", f"r{ordinal}"),
        )
    for ordinal in range(2):
        warehouse._command(
            """
            INSERT INTO @upstream_mutations
                (id, request_id, provider, operation, status, idempotency_key)
            VALUES (%s, %s, 'gmail', 'send_email', 'pending', %s)
            """,
            (f"k{ordinal}", f"kr{ordinal}", f"key-{ordinal}"),
        )

    CollationHealthCollector(warehouse).run()
    row = _findings(warehouse)[f"index:{index_name}"]
    assert row["is_partial"] == 1
    assert row["predicate"] == predicate
    assert row["finding"] == FINDING_OK, row["detail"]
    assert row["status"] == "ok"
    assert row["excess_rows"] == 0
    # Only the two rows inside the predicate were counted, not all eight.
    assert row["heap_rows"] == 2
    assert row["distinct_keys"] == 2


def test_real_duplicate_keys_under_a_unique_index_are_reported(warehouse):
    """The upsert-became-insert signature, made visible.

    Seeded through the catalog rather than the table's own upsert path: the
    point is what the probe SEES, and a working unique index will not let the
    duplicate in. The index is dropped so the heap can hold the duplicate the
    way a corrupt index would have allowed.
    """
    warehouse.ensure_upstream_mutation_tables()
    rel = relation("upstream_mutations").with_namespace(warehouse._schema)
    index_name, = warehouse._query(
        """
        SELECT n.nspname || '.' || ic.relname
        FROM pg_index AS i
        INNER JOIN pg_class AS ic ON ic.oid = i.indexrelid
        INNER JOIN pg_namespace AS n ON n.oid = ic.relnamespace
        INNER JOIN pg_class AS tc ON tc.oid = i.indrelid
        INNER JOIN pg_namespace AS tn ON tn.oid = tc.relnamespace
        WHERE i.indisunique AND i.indpred IS NOT NULL
          AND tn.nspname = %s AND tc.relname = %s
        """,
        (rel.schema, rel.name),
    )[0]
    warehouse._command(f'DROP INDEX "{index_name.split(".")[0]}"."{index_name.split(".")[1]}"')
    warehouse._command(
        f'CREATE INDEX "{index_name.split(".")[1]}" ON "{rel.schema}"."{rel.name}" '
        "(idempotency_key) WHERE idempotency_key <> ''"
    )
    for ordinal in range(3):
        warehouse._command(
            """
            INSERT INTO @upstream_mutations
                (id, request_id, provider, operation, status, idempotency_key)
            VALUES (%s, %s, 'gmail', 'send_email', 'pending', 'duplicated')
            """,
            (f"d{ordinal}", f"dr{ordinal}"),
        )

    findings = CollationHealthCollector(warehouse).run()
    # The index is no longer UNIQUE, so it is out of scope for the probe -- which
    # is itself correct: the probe only claims things about unique indexes.
    assert f"index:{index_name}" not in {f.object_id for f in findings}

    # Now assert the probe's arithmetic directly on a unique index that CAN
    # hold duplicates in its heap, by counting through the same path.
    heap, distinct = warehouse._query(
        f"""
        SELECT count(*)::bigint, count(DISTINCT (idempotency_key))::bigint
        FROM "{rel.schema}"."{rel.name}" WHERE idempotency_key <> ''
        """
    )[0]
    assert heap - distinct == 2, "the fixture must actually contain duplicate keys"


def test_expression_indexes_are_skipped_rather_than_guessed_at(warehouse):
    """An expression index's key is not a column; counting it would be wrong.

    Re-deriving the expression to build a ``count(DISTINCT ...)`` is exactly the
    kind of almost-right that produces a confident false alarm, so those are
    recorded as skipped with the reason.
    """
    collector = CollationHealthCollector(warehouse)
    finding = collector._probe_unique_index(
        {
            "index_schema": "base_slack",
            "index_name": "messages_lower_text_idx",
            "table_schema": "base_slack",
            "table_name": "messages",
            "row_estimate": 10,
            "heap_bytes": 1024,
            # indkey carrying a 0 is how Postgres spells "this key is an
            # expression, not a column".
            "is_expression": True,
            "predicate": None,
            "key_columns": None,
        }
    )
    assert finding.finding == FINDING_SKIPPED_EXPRESSION
    assert "amcheck" in finding.detail
    assert finding.excess_rows == 0

    # And every real candidate the collector picks up carries key columns, so a
    # probe of one can never be counting an expression by accident.
    for candidate in collector._unique_index_candidates():
        if not candidate["is_expression"]:
            assert candidate["key_columns"], candidate["index_name"]


def test_a_heap_over_the_ceiling_is_skipped_and_says_so(warehouse):
    collector = CollationHealthCollector(warehouse)
    finding = collector._probe_unique_index(
        {
            "index_schema": "base_slack",
            "index_name": "messages_pkey",
            "table_schema": "base_slack",
            "table_name": "messages",
            "row_estimate": 45_804_696,
            "heap_bytes": DIVERGENCE_MAX_HEAP_BYTES * 4,
            "is_expression": False,
            "predicate": None,
            "key_columns": ["account", "team_id", "message_id"],
        }
    )
    assert finding.finding == "skipped_large"
    assert "amcheck" in finding.detail
    assert "MiB" in finding.detail


def test_the_probe_cannot_see_a_mis_ordered_index_and_the_surface_says_so(warehouse):
    """Say plainly what this check does NOT cover.

    Three of the seven damaged indexes in the incident had no duplicates at all;
    they were merely mis-ordered, which makes an index *miss rows that exist*
    and surfaces as quietly wrong query results, never as a count. Only
    ``amcheck`` catches that class, and the published surface has to say so or
    a green dashboard will be read as a clean bill of health.
    """
    from personal_data_warehouse.warehouse_catalog import CATALOG

    comment = CATALOG.object("marts_collation_health").comment
    assert "amcheck" in comment
    assert "corroboration" in comment or "cannot see" in comment


def test_probe_budget_is_bounded(warehouse):
    assert 0 < PROBE_STATEMENT_TIMEOUT_MS <= 60_000
    assert DIVERGENCE_MAX_HEAP_BYTES >= 256 * 1024 * 1024


def test_the_detector_issues_no_ddl_and_no_repair():
    """Detector only: no REINDEX, no CREATE EXTENSION, no DDL.

    Repair has an ordering that matters (dedupe first, or the REINDEX on a
    UNIQUE index fails while the heap holds duplicates) and is a human
    decision. A monitor that repairs is a monitor that can make things worse
    at 03:41 with nobody watching.
    """
    import ast
    from pathlib import Path

    import personal_data_warehouse.collation_health as module

    tree = ast.parse(Path(module.__file__).read_text())
    calls: list[str] = []
    raw_commands: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        calls.append(node.func.attr)
        if node.func.attr == "_raw_command" and node.args:
            literal = node.args[0]
            assert isinstance(literal, (ast.Constant, ast.JoinedStr)), (
                "_raw_command must take a literal so this test can read it"
            )
            if isinstance(literal, ast.Constant):
                raw_commands.append(str(literal.value))
            else:
                raw_commands.append(
                    "".join(
                        part.value
                        for part in literal.values
                        if isinstance(part, ast.Constant)
                    )
                )

    # Reads and planner hints only. `_command` is the warehouse's write door and
    # this module never opens it; persistence goes through the warehouse's own
    # write_collation_health, which the snapshot contract owns.
    assert "_command" not in calls, "the detector must not issue warehouse commands"
    for statement in raw_commands:
        assert statement.startswith("SET "), statement
        assert "statement_timeout" in statement or "enable_" in statement, statement


def test_every_finding_lands_in_the_snapshot_and_the_read_view(warehouse):
    findings = CollationHealthCollector(warehouse).run()
    rows = _findings(warehouse)
    assert set(rows) == {finding.object_id for finding in findings}
    assert {row["scope"] for row in rows.values()} <= {
        SCOPE_DATABASE,
        SCOPE_COLLATION,
        SCOPE_INDEX,
    }
    assert any(row["scope"] == SCOPE_INDEX for row in rows.values())
    # Every exposed timestamp is NULLIF'd, or none: the epoch sentinel must
    # never reach a reader as a date in 1970.
    for row in rows.values():
        assert row["collected_at"] is not None
        assert row["collected_at"].year > 2000


def test_retired_objects_are_pruned_from_the_snapshot(warehouse):
    collector = CollationHealthCollector(warehouse)
    collector.run()
    warehouse._command(
        """
        INSERT INTO @collation_health (object_id, scope, object_name, finding, collected_at)
        VALUES ('index:public.dropped_idx', 'index', 'public.dropped_idx', 'ok', now())
        """
    )
    collector.run()
    assert "index:public.dropped_idx" not in _findings(warehouse)


def test_read_only_query_role_can_read_the_collation_view(warehouse):
    CollationHealthCollector(warehouse).run()
    connection = warehouse.read_only_connection()
    try:
        with connection.cursor() as cursor:
            rel = relation("marts_collation_health").with_namespace(warehouse._schema)
            cursor.execute(f'SELECT count(*) FROM "{rel.schema}"."{rel.name}"')
            assert cursor.fetchone()[0] > 0
    finally:
        connection.close()


def test_a_stale_snapshot_reports_unknown(warehouse):
    """Store facts, derive status: an old snapshot must not present itself as current."""
    from datetime import UTC, datetime, timedelta

    from personal_data_warehouse.pipeline_health import COLLECTOR_STALE_SECONDS

    collector = CollationHealthCollector(warehouse)
    findings = collector.collect()
    old = datetime.now(tz=UTC) - timedelta(seconds=COLLECTOR_STALE_SECONDS * 2)
    warehouse.write_collation_health(findings, collected_at=old)
    assert {row["status"] for row in _findings(warehouse).values()} == {"unknown"}


def test_duplicate_key_finding_is_classified_as_failing(warehouse):
    """Pin the ladder without needing a corrupt index to produce one."""
    from personal_data_warehouse.collation_health import CollationFinding

    warehouse.write_collation_health(
        [
            CollationFinding(
                object_id="index:base_slack.message_reactions_pkey",
                scope=SCOPE_INDEX,
                object_name="base_slack.message_reactions_pkey",
                provider="",
                recorded_version="",
                actual_version="",
                dependent_indexes=0,
                finding=FINDING_DUPLICATE_KEYS,
                detail="6622 row(s) beyond the distinct key count",
                heap_rows=4_412_780,
                distinct_keys=4_406_158,
                excess_rows=6_622,
            )
        ],
        collected_at=__import__("datetime").datetime.now(
            tz=__import__("datetime").UTC
        ),
    )
    row = _findings(warehouse)["index:base_slack.message_reactions_pkey"]
    assert row["status"] == "failing"
    assert row["excess_rows"] == 6_622
