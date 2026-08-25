"""Collation drift and index-integrity detection.

**This database cannot warn you about collation changes, and one has already
happened.** ``pg_database.datcollversion`` is NULL while
``pg_database_collation_actual_version()`` reports glibc 2.36. Postgres raises
its "collation version mismatch" warning only when it has a recorded baseline to
compare against, and ``ALTER DATABASE ... REFRESH COLLATION VERSION`` refuses to
create one from NULL (``ERROR: invalid collation version change``). So the
``en_US.utf8`` sort order changed underneath the data silently, and the next
change will be silent too.

What that did, found and repaired 2026-08-23: seven btree indexes failed
``bt_index_check`` with ``item order invariant violated``, and four UNIQUE
indexes had been admitting duplicates — an ``ON CONFLICT`` lookup missing the
existing row through a mis-ordered index and INSERTing instead of upserting.
36,825 duplicate rows had accumulated.

This module is the cover Postgres will not provide. It is a **detector only**:
it reads catalogs and runs bounded read-only counts. It never REINDEXes, never
creates an extension, and never issues DDL.

Four things it gets right, each learned the hard way:

* **A NULL recorded version is the finding, not a neutral state.** Written the
  obvious way — ``recorded_version <> actual_version`` — the check evaluates to
  NULL on this database and reports CLEAN, which is the exact bug that lets the
  next drift through. The NULL case is therefore its own finding
  (:data:`FINDING_NO_BASELINE`) and it is *not* ``ok``.
* **Only collations something actually uses.** All 188 collatable indexes here
  ride the database default; **zero** use an ICU collation, and yet **871** ICU
  collations report drifted versions. Reporting those buries the signal on day
  one, so a collation is only surfaced when an index depends on it.
* **The observed actual version is stored as a fact.** With no baseline in
  ``pg_database``, the snapshot's own history is the only baseline that will
  ever exist: the next glibc change becomes visible as a change to
  ``actual_version`` against the previously stored row.
* **The corroborating duplicate probe must apply the index's partial
  predicate.** A sweep that ignores ``pg_index.indpred`` reported 53,035
  phantom excess rows on ``ops.upstream_mutation_operations``'s partial unique
  index, which is completely clean.

**The duplicate count is not the integrity check.** Three of the seven damaged
indexes had no duplicates at all; they were merely mis-ordered, which makes an
index *miss rows that exist*. The scheduled collector therefore runs amcheck's
``bt_index_check`` for every valid btree index,
including large and expression indexes skipped by the corroborating count. It
never creates the extension or repairs an index; unavailable/error/timeout are
published explicitly instead of being mistaken for a pass.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import psycopg2

logger = logging.getLogger(__name__)

__all__ = [
    "CollationFinding",
    "CollationHealthCollector",
    "DIVERGENCE_MAX_HEAP_BYTES",
    "FINDING_DUPLICATE_KEYS",
    "FINDING_ERROR",
    "FINDING_NO_BASELINE",
    "FINDING_OK",
    "FINDING_SKIPPED_EXPRESSION",
    "FINDING_SKIPPED_LARGE",
    "FINDING_TIMEOUT",
    "FINDING_UNKNOWN_ACTUAL",
    "FINDING_VERSION_CHANGED",
    "PROBE_STATEMENT_TIMEOUT_MS",
    "AMCHECK_STATEMENT_TIMEOUT_MS",
    "SCOPE_COLLATION",
    "SCOPE_DATABASE",
    "SCOPE_INDEX",
]

#: One row per checked object. The three scopes answer three different
#: questions, and they are kept in one relation because they are one finding:
#: "did the sort order move under us, and did anything break as a result?"
SCOPE_DATABASE = "database"
SCOPE_COLLATION = "collation"
SCOPE_INDEX = "index"

FINDING_OK = "ok"
#: Postgres has no recorded baseline to compare against, so it can never warn.
#: The single most important value in this module.
FINDING_NO_BASELINE = "no_baseline"
#: A recorded baseline exists and the library no longer matches it.
FINDING_VERSION_CHANGED = "version_changed"
#: The provider cannot report a version at all (libc collations on some
#: platforms), so neither Postgres nor this detector can compare anything.
FINDING_UNKNOWN_ACTUAL = "unknown_actual"
#: A unique index's key columns hold more rows than distinct keys.
FINDING_DUPLICATE_KEYS = "duplicate_keys"
FINDING_SKIPPED_EXPRESSION = "skipped_expression"
FINDING_SKIPPED_LARGE = "skipped_large"
FINDING_TIMEOUT = "timeout"
FINDING_ERROR = "error"

#: Heap-size ceiling for the corroborating divergence probe. The probe is a
#: ``count(*)`` plus a ``count(DISTINCT key)`` with index plans disabled, so it
#: costs a sequential scan of the heap; the ceiling is what keeps that a
#: bounded amount of work rather than an unbounded one.
#:
#: 2 GiB is chosen from the production shape, not picked round: it covers 104
#: of the 108 unique btree indexes and, critically, both of the big tables that
#: actually accumulated duplicates in the 2026-08-23 incident
#: (``base_slack.message_reactions`` at 1,131 MiB with 6,622 duplicates and
#: ``base_apple_messages.chat_messages`` at 216 MiB with 30,043). The four it
#: excludes are ``base_slack.messages`` (47 GB), ``timeline.events`` (27 GB) and
#: the two ``derived_search`` tables (8 GB, 7 GB) — all of which were swept
#: clean by ``amcheck``, which is the right tool at that size anyway. They
#: record ``skipped_large`` and say so.
DIVERGENCE_MAX_HEAP_BYTES = 2 * 1024 * 1024 * 1024

#: Per-probe statement budget. Wider than the freshness collector's five
#: seconds because this asset runs once a day rather than every ten minutes, and
#: because the number is measured rather than guessed: against production
#: 2026-08-23 the slowest probe under the size ceiling,
#: ``base_slack.message_reactions_pkey`` (1,131 MiB, 4.4M rows), needed more
#: than 15s and was recorded as a ``timeout`` — a permanent daily amber on an
#: index that is in fact clean, and one of the two that actually accumulated
#: duplicates in the incident, so skipping it instead would have been worse.
PROBE_STATEMENT_TIMEOUT_MS = 60_000
# Structural checks are the reason this daily job exists.  Give large indexes
# a real maintenance-window budget rather than turning them into permanent
# ``unknown`` rows after sixty seconds.
AMCHECK_STATEMENT_TIMEOUT_MS = 15 * 60_000


@dataclass
class CollationFinding:
    """One row of ``ops.collation_health``. Facts; the verdict is read-time."""

    object_id: str
    scope: str
    object_name: str
    provider: str
    recorded_version: str
    actual_version: str
    dependent_indexes: int
    finding: str
    detail: str
    table_name: str = ""
    is_unique: int = 0
    is_partial: int = 0
    predicate: str = ""
    heap_rows: int = 0
    distinct_keys: int = 0
    excess_rows: int = 0
    probe_ms: int = 0
    key_columns: list[str] = field(default_factory=list)
    amcheck_status: str = "unavailable"
    amcheck_detail: str = "amcheck extension/function is not installed"
    amcheck_ms: int = 0


#: Postgres spells providers as single characters. Rendering them as words is
#: the difference between a dashboard someone reads and one they squint at.
_PROVIDERS = {
    "c": "libc",
    "d": "database default",
    "i": "icu",
    "b": "builtin",
}


class CollationHealthCollector:
    """Reads collation baselines and probes unique indexes for divergence.

    One collection is: one catalog read for the database's own collation
    versions, one for the collations any index depends on, one for the unique
    indexes worth probing, then a bounded pair of counts per probed index.
    Nothing here writes to a source relation or issues DDL.
    """

    def __init__(self, warehouse, *, now: Any = None, run_amcheck: bool | None = None) -> None:
        self._warehouse = warehouse
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._run_structural_checks = (
            warehouse.schema_namespace == "public" if run_amcheck is None else run_amcheck
        )

    # -- collection --------------------------------------------------------

    def collect(self) -> list[CollationFinding]:
        findings = [self._database_finding()]
        findings.extend(self._collation_findings())
        findings.extend(self._index_findings())
        return findings

    def run(self) -> list[CollationFinding]:
        findings = self.collect()
        self._warehouse.write_collation_health(findings, collected_at=self._now())
        return findings

    # -- the database's own collation -------------------------------------

    def _database_finding(self) -> CollationFinding:
        """The headline row: can this database detect collation drift at all?

        Written deliberately as an explicit NULL test rather than an inequality.
        ``recorded <> actual`` is NULL when ``datcollversion`` is NULL, so an
        inequality-shaped check reports this database CLEAN — which is how a
        drift that had already corrupted seven indexes went unnoticed.
        """
        rows = self._warehouse._query_dicts(
            """
            SELECT
                d.datname AS name,
                d.datcollate AS collate,
                d.datctype AS ctype,
                d.datcollversion AS recorded,
                pg_database_collation_actual_version(d.oid) AS actual
            FROM pg_database AS d
            WHERE d.datname = current_database()
            """
        )
        row = rows[0] if rows else {}
        recorded = row.get("recorded")
        actual = row.get("actual")
        dependents = self._default_collation_index_count()
        finding = CollationFinding(
            object_id="database",
            scope=SCOPE_DATABASE,
            object_name=str(row.get("name") or ""),
            provider="database default",
            recorded_version=str(recorded or ""),
            actual_version=str(actual or ""),
            dependent_indexes=dependents,
            finding=FINDING_OK,
            detail="",
        )
        collate = str(row.get("collate") or "")
        if actual is None:
            finding.finding = FINDING_UNKNOWN_ACTUAL
            finding.detail = (
                f"the provider behind {collate} reports no version, so neither "
                "Postgres nor this check can compare sort orders"
            )
        elif recorded is None:
            finding.finding = FINDING_NO_BASELINE
            finding.detail = (
                "this database cannot detect collation drift; text index ordering "
                f"is unverified. pg_database.datcollversion is NULL while the live "
                f"{collate} library reports {actual}, and ALTER DATABASE ... REFRESH "
                "COLLATION VERSION refuses to create a baseline from NULL "
                "('invalid collation version change'), so Postgres will never raise "
                f"its own mismatch warning. {dependents} collatable index(es) ride "
                "this collation. Verify ordering with amcheck's bt_index_check; a "
                "future library change is visible here as a change to actual_version."
            )
        elif str(recorded) != str(actual):
            finding.finding = FINDING_VERSION_CHANGED
            finding.detail = (
                f"the {collate} library moved from {recorded} to {actual} under this "
                f"database; every text index built before the change ({dependents} "
                "collatable index(es)) may be mis-ordered. Verify with amcheck, then "
                "REINDEX and REFRESH COLLATION VERSION."
            )
        return finding

    def _default_collation_index_count(self) -> int:
        rows = self._warehouse._query(
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
        )
        return int(rows[0][0]) if rows else 0

    # -- named collations, but only ones an index depends on ---------------

    def _collation_findings(self) -> list[CollationFinding]:
        """Only collations with a dependent index.

        Production carries 871 ICU collations that all report drifted versions
        and not one of them has a dependent index. Surfacing them would bury the
        one finding that matters under 871 that do not, which is how a monitor
        teaches people to ignore it.
        """
        rows = self._warehouse._query_dicts(
            """
            SELECT
                cn.nspname AS schema,
                cl.collname AS name,
                cl.collprovider AS provider,
                cl.collversion AS recorded,
                pg_collation_actual_version(cl.oid) AS actual,
                count(DISTINCT used.indexrelid) AS dependent_indexes
            FROM (
                SELECT i.indexrelid, unnest(i.indcollation) AS collid
                FROM pg_index AS i
                INNER JOIN pg_class AS c ON c.oid = i.indexrelid
                INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ) AS used
            INNER JOIN pg_collation AS cl ON cl.oid = used.collid
            INNER JOIN pg_namespace AS cn ON cn.oid = cl.collnamespace
            -- The database-default pseudo-collation is reported by its own row
            -- above, where the baseline actually lives (pg_database, not
            -- pg_collation): a 'd' row here would always read NULL/NULL and
            -- look reassuringly clean.
            WHERE cl.collprovider <> 'd'
            GROUP BY 1, 2, 3, 4, 5
            ORDER BY 1, 2
            """
        )
        findings: list[CollationFinding] = []
        for row in rows:
            recorded = row.get("recorded")
            actual = row.get("actual")
            name = f"{row['schema']}.{row['name']}"
            finding = CollationFinding(
                object_id=f"collation:{name}",
                scope=SCOPE_COLLATION,
                object_name=name,
                provider=_PROVIDERS.get(str(row.get("provider") or ""), str(row.get("provider") or "")),
                recorded_version=str(recorded or ""),
                actual_version=str(actual or ""),
                dependent_indexes=int(row.get("dependent_indexes") or 0),
                finding=FINDING_OK,
                detail="",
            )
            if actual is None:
                finding.finding = FINDING_UNKNOWN_ACTUAL
                finding.detail = "the provider reports no version for this collation"
            elif recorded is None:
                finding.finding = FINDING_NO_BASELINE
                finding.detail = (
                    "no recorded baseline, so a change in this collation's sort "
                    f"order cannot be detected by Postgres; live version {actual}"
                )
            elif str(recorded) != str(actual):
                finding.finding = FINDING_VERSION_CHANGED
                finding.detail = f"recorded {recorded}, live {actual}; indexes on this collation may be mis-ordered"
            findings.append(finding)
        return findings

    # -- corroborating divergence probe ------------------------------------

    def _index_findings(self) -> list[CollationFinding]:
        candidates = self._unique_index_candidates()
        findings: list[CollationFinding] = []
        amcheck = self._amcheck_function() if self._run_structural_checks else ""
        self._warehouse._raw_command(f"SET statement_timeout = {PROBE_STATEMENT_TIMEOUT_MS}")
        try:
            findings = [self._probe_unique_index(row) for row in candidates]
            by_name = {finding.object_name: finding for finding in findings}
            # Structural integrity applies to every valid btree, not only
            # UNIQUE indexes. The latter merely get the additional duplicate
            # corroboration above.
            for row in self._amcheck_candidates():
                name = f"{row['index_schema']}.{row['index_name']}"
                finding = by_name.get(name)
                if finding is None:
                    finding = CollationFinding(
                        object_id=f"index:{name}",
                        scope=SCOPE_INDEX,
                        object_name=name,
                        provider="",
                        recorded_version="",
                        actual_version="",
                        dependent_indexes=0,
                        finding=FINDING_OK,
                        detail="non-unique btree; duplicate-key corroboration is not applicable",
                        table_name=f"{row['table_schema']}.{row['table_name']}",
                    )
                    findings.append(finding)
                self._run_amcheck(row, finding, amcheck)
        finally:
            self._warehouse._raw_command("SET statement_timeout = DEFAULT")
        return findings

    def _amcheck_candidates(self) -> list[dict[str, Any]]:
        return self._warehouse._query_dicts(
            """
            SELECT n.nspname AS index_schema, ic.relname AS index_name,
                   tn.nspname AS table_schema, tc.relname AS table_name
            FROM pg_index i
            JOIN pg_class ic ON ic.oid = i.indexrelid
            JOIN pg_namespace n ON n.oid = ic.relnamespace
            JOIN pg_class tc ON tc.oid = i.indrelid
            JOIN pg_namespace tn ON tn.oid = tc.relnamespace
            JOIN pg_am am ON am.oid = ic.relam
            WHERE i.indisvalid AND i.indisready AND am.amname = 'btree'
              AND n.nspname = ANY(%s)
            ORDER BY 1, 2
            """,
            (self._warehouse.physical_schema_names(include_hidden=True),),
        )

    def _amcheck_function(self) -> str:
        """Return the installed function's qualified schema, never CREATE it."""
        rows = self._warehouse._query(
            """
            SELECT n.nspname
            FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            JOIN pg_extension e ON e.extnamespace = n.oid
            WHERE e.extname = 'amcheck' AND p.proname = 'bt_index_check'
            ORDER BY p.pronargs DESC LIMIT 1
            """
        )
        return str(rows[0][0]) if rows else ""

    def _run_amcheck(self, row: dict[str, Any], finding: CollationFinding, function_schema: str) -> None:
        if not function_schema:
            return
        started = time.monotonic()
        self._warehouse._raw_command(f"SET statement_timeout = {AMCHECK_STATEMENT_TIMEOUT_MS}")
        try:
            qualified_index = f"{_ident(row['index_schema'])}.{_ident(row['index_name'])}"
            self._warehouse._query(
                f"SELECT {_ident(function_schema)}.bt_index_check(%s::regclass, false)",
                (qualified_index,),
            )
            finding.amcheck_status = "ok"
            finding.amcheck_detail = "bt_index_check structural verification passed"
        except psycopg2.errors.QueryCanceled as error:
            finding.amcheck_status = "timeout"
            finding.amcheck_detail = _one_line(str(error))[:500]
        except psycopg2.Error as error:
            # amcheck reports corruption as an ERROR; unlike an infrastructure
            # error, the invariant wording is a definitive failing result.
            detail = _one_line(str(error))[:500]
            finding.amcheck_status = (
                "failed" if "invariant" in detail.lower() or "corrupt" in detail.lower() else "error"
            )
            finding.amcheck_detail = detail
        finally:
            finding.amcheck_ms = int((time.monotonic() - started) * 1000)
            self._warehouse._raw_command(f"SET statement_timeout = {PROBE_STATEMENT_TIMEOUT_MS}")

    def _unique_index_candidates(self) -> list[dict[str, Any]]:
        """Unique btree indexes over plain columns, with their partial predicate.

        ``indkey`` containing 0 marks an expression index: the key is not a
        column, so there is nothing to ``count(DISTINCT ...)`` without
        re-deriving the expression, and getting that subtly wrong produces a
        confident false alarm. Those are skipped explicitly rather than
        silently.
        """
        return self._warehouse._query_dicts(
            """
            SELECT
                n.nspname AS index_schema,
                ic.relname AS index_name,
                tn.nspname AS table_schema,
                tc.relname AS table_name,
                tc.reltuples AS row_estimate,
                pg_relation_size(tc.oid) AS heap_bytes,
                (0 = ANY(i.indkey::int[])) AS is_expression,
                pg_get_expr(i.indpred, i.indrelid) AS predicate,
                (
                    SELECT array_agg(a.attname ORDER BY k.ord)
                    FROM unnest(i.indkey::int[]) WITH ORDINALITY AS k(attnum, ord)
                    INNER JOIN pg_attribute AS a
                      ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                    WHERE k.ord <= i.indnkeyatts
                ) AS key_columns
            FROM pg_index AS i
            INNER JOIN pg_class AS ic ON ic.oid = i.indexrelid
            INNER JOIN pg_namespace AS n ON n.oid = ic.relnamespace
            INNER JOIN pg_class AS tc ON tc.oid = i.indrelid
            INNER JOIN pg_namespace AS tn ON tn.oid = tc.relnamespace
            INNER JOIN pg_am AS am ON am.oid = ic.relam
            WHERE i.indisunique
              AND i.indisvalid
              AND am.amname = 'btree'
              AND n.nspname = ANY(%s)
            ORDER BY 1, 2
            """,
            (self._warehouse.physical_schema_names(include_hidden=True),),
        )

    def _probe_unique_index(self, row: dict[str, Any]) -> CollationFinding:
        index_name = f"{row['index_schema']}.{row['index_name']}"
        table_name = f"{row['table_schema']}.{row['table_name']}"
        predicate = row.get("predicate") or ""
        key_columns = list(row.get("key_columns") or [])
        row_estimate = max(0, int(row.get("row_estimate") or 0))
        finding = CollationFinding(
            object_id=f"index:{index_name}",
            scope=SCOPE_INDEX,
            object_name=index_name,
            provider="",
            recorded_version="",
            actual_version="",
            dependent_indexes=0,
            finding=FINDING_OK,
            detail="",
            table_name=table_name,
            is_unique=1,
            is_partial=1 if predicate else 0,
            predicate=predicate,
            key_columns=key_columns,
        )
        if row.get("is_expression") or not key_columns:
            finding.finding = FINDING_SKIPPED_EXPRESSION
            finding.detail = (
                "expression index: its key is not a column, so a duplicate-key "
                "count would have to re-derive the expression and would be wrong "
                "in a confident-looking way. Check it with amcheck."
            )
            return finding
        heap_bytes = max(0, int(row.get("heap_bytes") or 0))
        if heap_bytes > DIVERGENCE_MAX_HEAP_BYTES:
            finding.finding = FINDING_SKIPPED_LARGE
            finding.detail = (
                f"{heap_bytes // (1024 * 1024)} MiB heap ({row_estimate} estimated rows) "
                f"exceeds the {DIVERGENCE_MAX_HEAP_BYTES // (1024 * 1024)} MiB probe "
                "ceiling; amcheck's bt_index_check is the right tool at this size"
            )
            return finding

        keys = ", ".join(_ident(column) for column in key_columns)
        where = f" WHERE {predicate}" if predicate else ""
        sql = (
            f"SELECT count(*)::bigint, count(DISTINCT ({keys}))::bigint "
            f"FROM {_ident(row['table_schema'])}.{_ident(row['table_name'])}{where}"
        )
        started = time.monotonic()
        try:
            # A corrupt unique index reports exactly what it believes, and both
            # count(*) and count(DISTINCT ...) can be answered from an index
            # depending on plan shape — on this warehouse two such plans
            # disagreed by 145 rows. Forcing the heap is the whole point of the
            # probe: read the rows that exist, not the index's opinion of them.
            self._warehouse._raw_command("SET enable_indexscan = off")
            self._warehouse._raw_command("SET enable_indexonlyscan = off")
            self._warehouse._raw_command("SET enable_bitmapscan = off")
            rows = self._warehouse._query(sql)
        except psycopg2.errors.QueryCanceled as error:
            finding.finding = FINDING_TIMEOUT
            finding.detail = _one_line(str(error))[:500]
            return finding
        except psycopg2.Error as error:
            finding.finding = FINDING_ERROR
            finding.detail = _one_line(str(error))[:500]
            return finding
        finally:
            self._warehouse._raw_command("SET enable_indexscan = DEFAULT")
            self._warehouse._raw_command("SET enable_indexonlyscan = DEFAULT")
            self._warehouse._raw_command("SET enable_bitmapscan = DEFAULT")
            finding.probe_ms = int((time.monotonic() - started) * 1000)

        heap, distinct = (int(rows[0][0]), int(rows[0][1])) if rows else (0, 0)
        finding.heap_rows = heap
        finding.distinct_keys = distinct
        finding.excess_rows = max(0, heap - distinct)
        if finding.excess_rows:
            finding.finding = FINDING_DUPLICATE_KEYS
            finding.detail = (
                f"{finding.excess_rows} row(s) beyond the distinct key count on a "
                f"UNIQUE index over ({', '.join(key_columns)}). A working ON CONFLICT "
                "cannot produce this; the upsert became an insert because the index "
                "did not find the existing row. Dedupe keeping the highest "
                "sync_version per key, then REINDEX INDEX CONCURRENTLY, then "
                "re-verify with amcheck."
            )
        return finding


def _one_line(text: str) -> str:
    return " ".join(text.split())


def _ident(value: str) -> str:
    if not value.replace("_", "a").isalnum() or value[0].isdigit():
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return '"' + value + '"'
