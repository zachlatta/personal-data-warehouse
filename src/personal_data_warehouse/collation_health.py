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
index *miss rows that exist*. The scheduled collector therefore rotates
amcheck's ``bt_index_check`` across every valid btree index, including large
and expression indexes skipped by the corroborating count. Each run is bounded
by count and wall time; never-checked, old, large, and previously failing
indexes go first, while unvisited indexes retain their last result. It
never creates the extension or repairs an index; unavailable/error/timeout are
published explicitly instead of being mistaken for a pass.

**Page checksums cannot see damage done before the checksum was computed.** On
2026-10-05 one bit flipped in shared-buffers RAM (the host's memory, not the
disk) moved a line pointer of ``base_gmail.messages`` from offset 6984 to 6986.
A hint-bit write then persisted that page with a fresh, valid checksum, so
``pg_stat_database.checksum_failures`` never moved while every read of
``marts_inbox.gmail_threads`` failed. The same collector therefore also rotates
amcheck's ``verify_heapam`` (with TOAST pointer checks) across every heap table,
under the same bounded-rotation rules as the btree checks.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg2

logger = logging.getLogger(__name__)

__all__ = [
    "CHECKSUM_FAILURE_WINDOW",
    "CollationFinding",
    "CollationHealthCollector",
    "DIVERGENCE_MAX_HEAP_BYTES",
    "FINDING_CHECKSUM_FAILURES",
    "FINDING_CHECKSUMS_DISABLED",
    "FINDING_DUPLICATE_KEYS",
    "FINDING_ERROR",
    "FINDING_NO_BASELINE",
    "FINDING_OK",
    "FINDING_SKIPPED_EXPRESSION",
    "FINDING_SKIPPED_LARGE",
    "FINDING_TIMEOUT",
    "FINDING_UNKNOWN_ACTUAL",
    "FINDING_VERSION_CHANGED",
    "HEAPCHECK_MAX_PER_RUN",
    "HEAPCHECK_RUN_BUDGET_SECONDS",
    "HEAPCHECK_STATEMENT_TIMEOUT_MS",
    "PROBE_STATEMENT_TIMEOUT_MS",
    "AMCHECK_STATEMENT_TIMEOUT_MS",
    "AMCHECK_MAX_PER_RUN",
    "AMCHECK_RUN_BUDGET_SECONDS",
    "OBJECT_ID_DATA_CHECKSUMS",
    "SCOPE_COLLATION",
    "SCOPE_DATABASE",
    "SCOPE_HEAP",
    "SCOPE_INDEX",
    "checksum_finding",
]

#: One row per checked object. The three scopes answer three different
#: questions, and they are kept in one relation because they are one finding:
#: "did the sort order move under us, and did anything break as a result?"
SCOPE_DATABASE = "database"
SCOPE_COLLATION = "collation"
SCOPE_INDEX = "index"
#: A heap table's structural verdict from amcheck's ``verify_heapam``: line
#: pointers, tuple headers, xid bounds and TOAST pointers. The finding a page
#: checksum cannot make when the page was damaged in memory before it was
#: written (2026-10-05).
SCOPE_HEAP = "heap"

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

#: The database's own page-integrity verdict, one row beside the collation
#: baseline. Postgres verifies a page's checksum only when it reads it, and
#: counts every failure in ``pg_stat_database.checksum_failures`` -- a counter
#: no surface here read until 2026-10-03, when production had logged 9,125 of
#: them over three days (the HNSW embeddings index, the timeline BM25 index and
#: the TOAST heap of base_muse.events) behind an all-green /pipelines.
OBJECT_ID_DATA_CHECKSUMS = "data_checksums"
#: A checksum failure inside :data:`CHECKSUM_FAILURE_WINDOW`: some page the
#: database read recently is corrupt, and every backup since copies it.
FINDING_CHECKSUM_FAILURES = "checksum_failures"
#: Data checksums are off, so a corrupt page is invisible until it breaks a
#: query -- the database cannot answer the integrity question at all.
FINDING_CHECKSUMS_DISABLED = "checksums_disabled"
#: The counter never resets, so the verdict keys off the LAST failure. A
#: repaired database stops failing reads and ages out of this window instead
#: of reading red forever; a week is long enough that a corrupt page read only
#: by a daily or weekly job still keeps the row failing.
CHECKSUM_FAILURE_WINDOW = timedelta(days=7)

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
# Rotation bounds.  A 1,200-index database must make steady progress without a
# daily job turning into an unbounded I/O sweep -- but the rotation must also
# lap the fleet comfortably inside AMCHECK_STALE_SECONDS, because the read view
# turns a verdict older than fourteen days into `attention`.  At 25 a day
# production's 252 indexes took ~10 days per lap, and up to ~12.6 once the
# failure-retry cap ate its five slots: any skipped run (lock, budget) tipped
# clean indexes amber.  Measured 2026-08-26/27 the whole daily slice cost 74s
# and 452s of the 45-minute budget, so 50 a day (~5-6 days per lap) is cheap.
AMCHECK_MAX_PER_RUN = 50
AMCHECK_RUN_BUDGET_SECONDS = 45 * 60
AMCHECK_STALE_SECONDS = 14 * 24 * 60 * 60
AMCHECK_FAILURE_RETRY_CAP = 5
# The heap rotation's own bounds, separate from the btree one so neither can
# starve the other. Measured 2026-10-05, pg_amcheck verified all 144 production
# heaps (130 GB, the largest 63 GB) plus every btree in ~13 minutes on six
# workers, so 45 heaps a day (~3.2 days per lap) in a 45-minute budget is cheap
# and leaves the 14-day staleness window a wide margin. A single 63 GB heap
# with TOAST checks needs more than the btree statement budget.
HEAPCHECK_MAX_PER_RUN = 50
HEAPCHECK_RUN_BUDGET_SECONDS = 45 * 60
HEAPCHECK_STATEMENT_TIMEOUT_MS = 30 * 60_000
#: How many verify_heapam reports one finding quotes. The first few name the
#: block and tuple to inspect; the rest is the same damage restated.
HEAPCHECK_REPORT_LIMIT = 5


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
    amcheck_at: datetime | None = None


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
        findings = [self._database_finding(), self._checksum_finding()]
        findings.extend(self._collation_findings())
        findings.extend(self._index_findings())
        findings.extend(self._heap_findings())
        return findings

    def run(self) -> list[CollationFinding]:
        findings = self.collect()
        self._warehouse.write_collation_health(findings, collected_at=self._now())
        return findings

    def refresh_database_integrity(self) -> CollationFinding:
        """Re-measure only the checksum row, for the ten-minute collector.

        The collation and amcheck rows cost a daily amount of work; the
        checksum counter is one catalog read, and corruption should colour
        /pipelines within minutes, not by tomorrow's 03:41 run. Upserts the one
        row and prunes nothing.
        """
        finding = self._checksum_finding()
        self._warehouse.upsert_collation_health([finding], collected_at=self._now())
        return finding

    def _checksum_finding(self) -> CollationFinding:
        rows = self._warehouse._query_dicts(
            """
            SELECT
                current_database() AS name,
                current_setting('data_checksums') AS data_checksums,
                s.checksum_failures AS failures,
                s.checksum_last_failure AS last_failure
            FROM pg_stat_database AS s
            WHERE s.datname = current_database()
            """
        )
        row = rows[0] if rows else {}
        return checksum_finding(
            database=str(row.get("name") or ""),
            data_checksums=str(row.get("data_checksums") or ""),
            failures=row.get("failures"),
            last_failure=row.get("last_failure"),
            now=self._now(),
        )

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
            all_amcheck = self._amcheck_candidates() if self._run_structural_checks else []
            prior = self._previous_amcheck_results() if all_amcheck else {}
            selected = {
                self._candidate_name(row)
                for row in self._select_amcheck_candidates(
                    all_amcheck, prior, limit=AMCHECK_MAX_PER_RUN
                )
            }
            deadline = time.monotonic() + AMCHECK_RUN_BUDGET_SECONDS
            # Structural integrity applies to every valid btree, but only a
            # bounded rotation is checked on one day. Every unvisited index is
            # still emitted with its previous rigorous result intact.
            for row in all_amcheck:
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
                previous = prior.get(name)
                # A previous "unavailable" only says the extension was missing
                # on THAT day. Once amcheck is installed the index is simply
                # unchecked and queued for the rotation; carrying "unavailable"
                # forward kept 114 production indexes reading `attention` for
                # weeks after the extension existed -- a measurement gap
                # presented as a finding, which buries the real ones.
                if previous and not (amcheck and str(previous.get("amcheck_status") or "") == "unavailable"):
                    self._restore_amcheck(finding, previous)
                elif not amcheck:
                    finding.amcheck_status = "unavailable"
                    finding.amcheck_detail = "amcheck extension/function is not installed"
                else:
                    finding.amcheck_status = "never_checked"
                    finding.amcheck_detail = "pending the bounded daily amcheck rotation"
                if name in selected and amcheck and time.monotonic() < deadline:
                    remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
                    self._run_amcheck(
                        row,
                        finding,
                        amcheck,
                        timeout_ms=min(AMCHECK_STATEMENT_TIMEOUT_MS, remaining_ms),
                    )
                elif name in selected and amcheck:
                    finding.amcheck_status = "pending"
                    finding.amcheck_detail = "daily amcheck wall-time budget exhausted"
        finally:
            self._warehouse._raw_command("SET statement_timeout = DEFAULT")
        return findings

    def _amcheck_candidates(self) -> list[dict[str, Any]]:
        return self._warehouse._query_dicts(
            """
            SELECT n.nspname AS index_schema, ic.relname AS index_name,
                   tn.nspname AS table_schema, tc.relname AS table_name,
                   pg_relation_size(ic.oid) AS index_bytes,
                   pg_relation_size(tc.oid) AS heap_bytes
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

    @staticmethod
    def _candidate_name(row: dict[str, Any]) -> str:
        schema = str(row.get("index_schema") or "")
        name = str(row["index_name"])
        return f"{schema}.{name}" if schema else name

    def _previous_amcheck_results(self) -> dict[str, dict[str, Any]]:
        rows = self._warehouse._query_dicts(
            """
            SELECT object_name, amcheck_status, amcheck_detail, amcheck_ms,
                   NULLIF(amcheck_at, '1970-01-01 00:00:00+00'::timestamptz) AS amcheck_at
            FROM @collation_health WHERE scope IN ('index', 'heap')
            """
        )
        return {str(row["object_name"]): row for row in rows}

    def _select_amcheck_candidates(
        self,
        candidates: list[dict[str, Any]],
        prior: dict[str, dict[str, Any]],
        *,
        limit: int,
        name_of=None,
        size_key: str = "index_bytes",
        max_per_run: int = AMCHECK_MAX_PER_RUN,
    ) -> list[dict[str, Any]]:
        now = self._now()
        name_of = name_of or self._candidate_name

        def priority(row: dict[str, Any]) -> tuple[int, float, int, str]:
            name = name_of(row)
            old = prior.get(name)
            status = str((old or {}).get("amcheck_status") or "")
            at = (old or {}).get("amcheck_at")
            if status in {"failed", "error", "timeout"}:
                tier = 0
            elif not at or status in {"", "never_checked", "pending", "unavailable"}:
                tier = 1
            elif (now - at).total_seconds() >= AMCHECK_STALE_SECONDS:
                tier = 2
            else:
                tier = 3
            age = (now - at).total_seconds() if at else float("inf")
            return (tier, -age, -int(row.get(size_key) or 0), name)

        ordered = sorted(candidates, key=priority)
        failed = [row for row in ordered if priority(row)[0] == 0]
        rest = [row for row in ordered if priority(row)[0] != 0]
        # A fleet of permanently timing-out indexes must not starve the
        # never-checked tail forever. Retried failures still lead every run,
        # but consume only a bounded share of the rotation.
        chosen = failed[:AMCHECK_FAILURE_RETRY_CAP]
        chosen.extend(rest[: max(0, min(limit, max_per_run) - len(chosen))])
        return chosen

    @staticmethod
    def _restore_amcheck(finding: CollationFinding, previous: dict[str, Any]) -> None:
        finding.amcheck_status = str(previous.get("amcheck_status") or "never_checked")
        finding.amcheck_detail = str(previous.get("amcheck_detail") or "")
        finding.amcheck_ms = int(previous.get("amcheck_ms") or 0)
        finding.amcheck_at = previous.get("amcheck_at")

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

    def _run_amcheck(
        self,
        row: dict[str, Any],
        finding: CollationFinding,
        function_schema: str,
        *,
        timeout_ms: int = AMCHECK_STATEMENT_TIMEOUT_MS,
    ) -> None:
        if not function_schema:
            return
        started = time.monotonic()
        self._warehouse._raw_command(f"SET statement_timeout = {timeout_ms}")
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
            finding.amcheck_at = self._now()
            self._warehouse._raw_command(f"SET statement_timeout = {PROBE_STATEMENT_TIMEOUT_MS}")

    # -- heap structure ----------------------------------------------------

    def _heap_findings(self) -> list[CollationFinding]:
        """One row per heap table, a bounded rotation of them verified today.

        Mirrors the btree rotation: never-checked, stale and previously failing
        heaps go first, every unvisited heap keeps its last rigorous verdict,
        and a missing extension is published as ``unavailable`` rather than as
        a pass.
        """
        if not self._run_structural_checks:
            return []
        candidates = self._heapcheck_candidates()
        if not candidates:
            return []
        function_schema = self._amcheck_heap_function()
        prior = self._previous_amcheck_results()
        selected = {
            _table_name(row)
            for row in self._select_amcheck_candidates(
                candidates,
                prior,
                limit=HEAPCHECK_MAX_PER_RUN,
                name_of=_table_name,
                size_key="heap_bytes",
                max_per_run=HEAPCHECK_MAX_PER_RUN,
            )
        }
        deadline = time.monotonic() + HEAPCHECK_RUN_BUDGET_SECONDS
        findings: list[CollationFinding] = []
        for row in candidates:
            name = _table_name(row)
            finding = CollationFinding(
                object_id=f"heap:{name}",
                scope=SCOPE_HEAP,
                object_name=name,
                provider="",
                recorded_version="",
                actual_version="",
                dependent_indexes=0,
                finding=FINDING_OK,
                detail="heap table; structure verified by the verify_heapam rotation",
                table_name=name,
            )
            findings.append(finding)
            previous = prior.get(name)
            if previous and not (
                function_schema and str(previous.get("amcheck_status") or "") == "unavailable"
            ):
                self._restore_amcheck(finding, previous)
            elif not function_schema:
                finding.amcheck_status = "unavailable"
                finding.amcheck_detail = "amcheck extension/verify_heapam is not installed"
            else:
                finding.amcheck_status = "never_checked"
                finding.amcheck_detail = "pending the bounded daily verify_heapam rotation"
            if name in selected and function_schema and time.monotonic() < deadline:
                remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
                self._run_heapcheck(
                    row,
                    finding,
                    function_schema,
                    timeout_ms=min(HEAPCHECK_STATEMENT_TIMEOUT_MS, remaining_ms),
                )
            elif name in selected and function_schema:
                finding.amcheck_status = "pending"
                finding.amcheck_detail = "daily verify_heapam wall-time budget exhausted"
        return findings

    def _heapcheck_candidates(self) -> list[dict[str, Any]]:
        return self._warehouse._query_dicts(
            """
            SELECT n.nspname AS table_schema, c.relname AS table_name,
                   pg_relation_size(c.oid) AS heap_bytes
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_am am ON am.oid = c.relam
            WHERE c.relkind IN ('r', 'm') AND am.amname = 'heap'
              AND n.nspname = ANY(%s)
            ORDER BY 1, 2
            """,
            (self._warehouse.physical_schema_names(include_hidden=True),),
        )

    def _amcheck_heap_function(self) -> str:
        """The schema holding ``verify_heapam``, never CREATE it."""
        rows = self._warehouse._query(
            """
            SELECT n.nspname
            FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            JOIN pg_extension e ON e.extnamespace = n.oid
            WHERE e.extname = 'amcheck' AND p.proname = 'verify_heapam'
            LIMIT 1
            """
        )
        return str(rows[0][0]) if rows else ""

    def _run_heapcheck(
        self,
        row: dict[str, Any],
        finding: CollationFinding,
        function_schema: str,
        *,
        timeout_ms: int = HEAPCHECK_STATEMENT_TIMEOUT_MS,
    ) -> None:
        if not function_schema:
            return
        started = time.monotonic()
        self._warehouse._raw_command(f"SET statement_timeout = {timeout_ms}")
        try:
            qualified = f"{_ident(row['table_schema'])}.{_ident(row['table_name'])}"
            # verify_heapam reports corruption as ROWS, not as an ERROR: an
            # empty result is the pass, and any row is a definitive failure.
            reports = self._warehouse._query(
                f"SELECT blkno, offnum, attnum, msg FROM {_ident(function_schema)}.verify_heapam("
                "%s::regclass, on_error_stop => false, check_toast => true)",
                (qualified,),
            )
            if reports:
                quoted = "; ".join(
                    f"block {blkno} offset {offnum}"
                    + (f" attribute {attnum}" if attnum is not None else "")
                    + f": {msg}"
                    for blkno, offnum, attnum, msg in reports[:HEAPCHECK_REPORT_LIMIT]
                )
                finding.amcheck_status = "failed"
                finding.amcheck_detail = (
                    f"verify_heapam reported {len(reports)} corruption(s): {quoted}. "
                    "Check the host's memory before repairing; inspect the block with "
                    "pageinspect, recover the tuple, then heap_force_kill the damaged item."
                )[:1000]
            else:
                finding.amcheck_status = "ok"
                finding.amcheck_detail = "verify_heapam heap and TOAST-pointer verification passed"
        except psycopg2.errors.QueryCanceled as error:
            finding.amcheck_status = "timeout"
            finding.amcheck_detail = _one_line(str(error))[:500]
        except psycopg2.Error as error:
            detail = _one_line(str(error))[:500]
            finding.amcheck_status = "failed" if "corrupt" in detail.lower() else "error"
            finding.amcheck_detail = detail
        finally:
            finding.amcheck_ms = int((time.monotonic() - started) * 1000)
            finding.amcheck_at = self._now()
            self._warehouse._raw_command("SET statement_timeout = DEFAULT")

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


def _table_name(row: dict[str, Any]) -> str:
    return f"{row['table_schema']}.{row['table_name']}"


def _one_line(text: str) -> str:
    return " ".join(text.split())


def _ident(value: str) -> str:
    if not value.replace("_", "a").isalnum() or value[0].isdigit():
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return '"' + value + '"'


def checksum_finding(
    *,
    database: str,
    data_checksums: str,
    failures: int | None,
    last_failure: datetime | None,
    now: datetime,
) -> CollationFinding:
    """Judge the database's checksum counter into one finding.

    A pure function of the catalog facts so the verdict is testable without a
    corrupt page to produce one.
    """
    finding = CollationFinding(
        object_id=OBJECT_ID_DATA_CHECKSUMS,
        scope=SCOPE_DATABASE,
        object_name=database,
        provider="",
        recorded_version="",
        actual_version="",
        dependent_indexes=0,
        finding=FINDING_OK,
        detail="",
    )
    if data_checksums.lower() != "on":
        finding.finding = FINDING_CHECKSUMS_DISABLED
        finding.detail = (
            "data checksums are off: a corrupt page is invisible until it breaks a query, "
            "and pgBackRest cannot flag one in a backup"
        )
        return finding
    count = int(failures or 0)
    if count == 0 or last_failure is None:
        finding.detail = "data checksums on; no page has ever failed verification"
        return finding
    last = last_failure.isoformat()
    if now - last_failure <= CHECKSUM_FAILURE_WINDOW:
        finding.finding = FINDING_CHECKSUM_FAILURES
        finding.detail = (
            f"{count} page checksum failure(s), the last at {last}: a page read in the last "
            f"{CHECKSUM_FAILURE_WINDOW.days} days is corrupt and every backup since copies it. "
            "Find the relations in the server log ('invalid page in block N of relation "
            "base/<db>/<filenode>', map with pg_class.relfilenode); check the host's memory and "
            "disk before repairing, then REINDEX an index or restore a heap from the last "
            "backup pgbackrest info lists without 'error(s) detected'."
        )
        return finding
    finding.detail = (
        f"{count} page checksum failure(s) recorded since the stats reset, none since {last}"
    )
    return finding
