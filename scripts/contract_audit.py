"""Score the warehouse's contracts green / yellow / red from live evidence.

The 2026-08-26 audit was a day of agents reading health views, timing searches
and grepping the repo. This is that audit as a command, so "are we still
green" is a question with a cheap, repeatable answer instead of a re-audit.
Every verdict cites the numbers it was made from; a contract whose evidence is
unavailable reads yellow with the reason, never green by default.

Run from a machine with `pdw` logged in:

    uv run python scripts/contract_audit.py            # table
    uv run python scripts/contract_audit.py --json     # machine-readable

Repo-side checks (registry exemptions, test names) read this checkout; live
checks go through `pdw sql --output json` against the read-only query role, so
nothing here can write.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

from personal_data_warehouse.search_benchmark import (
    ATTENTION_PRIORITIES,
    run_search,
)
from personal_data_warehouse.search_benchmark_runner import (
    LATENCY_P50_TARGET_MS,
    MRR_FLOOR,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

GREEN, YELLOW, RED = "green", "yellow", "red"

#: The search latency the goal set for the tool, end to end through the CLI.
SEARCH_P50_TARGET_SECONDS = LATENCY_P50_TARGET_MS / 1000
SEARCH_P50_YELLOW_SECONDS = 5.0
SEARCH_ATTENTION_PRIORITIES = tuple(ATTENTION_PRIORITIES)
SEARCH_PROBE_QUERIES = (
    "runway burn rate months cash remaining",
    "trip planning flights hotel booking",
    "invoice payment received thanks",
)


@dataclass
class Verdict:
    contract: str
    title: str
    status: str
    evidence: str


def pdw_sql(intent: str, sql: str, *, timeout: float = 75.0) -> list[dict] | None:
    try:
        completed = subprocess.run(
            ["pdw", "sql", "--output", "json", "-q", intent, sql],
            capture_output=True, text=True, timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    try:
        payload = json.loads(completed.stdout)
    except ValueError:
        return None
    if isinstance(payload, dict) and "rows" in payload:
        payload = payload["rows"]
    return payload if isinstance(payload, list) else None


def worst(statuses: list[str]) -> str:
    order = {RED: 2, YELLOW: 1, GREEN: 0}
    return max(statuses, key=lambda s: order[s]) if statuses else YELLOW


def _unavailable(contract: str, title: str, what: str) -> Verdict:
    return Verdict(contract, title, YELLOW, f"could not read {what}; verdict withheld")


def _latency_status(elapsed_seconds: float | None) -> str:
    if elapsed_seconds is None:
        return YELLOW
    if elapsed_seconds < SEARCH_P50_TARGET_SECONDS:
        return GREEN
    if elapsed_seconds < SEARCH_P50_YELLOW_SECONDS:
        return YELLOW
    return RED


def _validated_search_probe(
    query: str, *, priorities: Sequence[str]
) -> tuple[float | None, str]:
    """Run one canonical search and return only valid hybrid latency.

    ``run_search`` owns subprocess, JSON, payload-error, and priority-scope
    validation. The audit adds the two properties specific to a hybrid latency
    claim: the effective mode must still be hybrid and no keyword fallback may
    have occurred. Error details are deliberately collapsed to categories so
    audit evidence cannot expose returned rows, credentials, or private query
    text.
    """

    result = run_search(
        query,
        "hybrid",
        20,
        priorities=tuple(priorities),
        timeout=90.0,
    )
    if result.error:
        issue = "scope_error" if "scope mismatch" in result.error else "search_error"
        return None, issue
    if result.mode != "hybrid":
        return None, "mode_mismatch"
    if result.fallback_reason:
        return None, "fallback"
    return result.elapsed_seconds, ""


# --- contracts ----------------------------------------------------------------

def c1_timeline_coverage() -> Verdict:
    title = "everything synced lands on timeline.events"
    rows = pdw_sql("adapter health", "SELECT adapter, status FROM marts_ops.timeline_adapter_health")
    if rows is None:
        return _unavailable("C1", title, "marts_ops.timeline_adapter_health")
    failing = sorted(r["adapter"] for r in rows if r["status"] == "failing")
    backfilling = sorted(r["adapter"] for r in rows if r["status"] == "backfilling")
    late = sorted(r["adapter"] for r in rows if r["status"] == "late")
    status = RED if failing else (YELLOW if backfilling or late else GREEN)
    return Verdict("C1", title, status,
                   f"{len(rows)} adapters; failing={failing or 'none'}; "
                   f"late={late or 'none'}; backfilling={backfilling or 'none'}")


def c2_priority_tiers() -> Verdict:
    title = "five tiers, everything classified"
    rows = pdw_sql("priority mix", "SELECT source, priority, status, events_7d FROM marts_ops.timeline_priority_mix")
    if rows is None:
        return _unavailable("C2", title, "marts_ops.timeline_priority_mix")
    unclassified = [r for r in rows if r["priority"] == "unclassified" and int(r["events_7d"] or 0) > 0]
    unknown = any(r["status"] == "unknown" for r in rows)
    tiers = sorted({r["priority"] for r in rows})
    if unclassified:
        return Verdict("C2", title, RED, f"unclassified rows in the last 7 days: {[(r['source'], r['events_7d']) for r in unclassified]}")
    if unknown or not rows:
        return Verdict("C2", title, YELLOW, "priority-mix snapshot is stale or empty")
    return Verdict("C2", title, GREEN, f"{len(rows)} (source, tier) cells over 7 days; tiers seen {tiers}")


def c3_agents_start_at_timeline() -> Verdict:
    title = "agents start at the timeline and filter by priority"
    rows = pdw_sql("agent usage", "SELECT source, status, search_first_rate, priority_filter_rate, sql_base_only_rate, sql_error_session_rate, pdw_sessions FROM marts_ops.agent_usage WHERE source = 'all'")
    if not rows:
        return _unavailable("C3", title, "marts_ops.agent_usage (has the daily asset run?)")
    r = rows[0]
    ev = (f"search-first {r['search_first_rate']}, priority filter {r['priority_filter_rate']}, "
          f"base-only SQL {r['sql_base_only_rate']}, SQL-error sessions {r['sql_error_session_rate']} "
          f"over {r['pdw_sessions']} PDW sessions")
    status = {"ok": GREEN, "attention": YELLOW}.get(str(r["status"]), YELLOW)
    return Verdict("C3", title, status, ev)


def c4_raw_data_queryable() -> Verdict:
    title = "raw source data queryable via SQL"
    rows = pdw_sql("base schemas", "SELECT count(DISTINCT table_schema) AS n FROM information_schema.tables WHERE table_schema LIKE 'base\\_%'")
    if not rows:
        return _unavailable("C4", title, "information_schema through the query role")
    n = int(rows[0]["n"])
    return Verdict("C4", title, GREEN if n > 0 else RED, f"{n} base_* schemas readable by the query role")


def c5_layering() -> Verdict:
    title = "base -> derived/marts -> timeline; enrichment reads the intermediate layer"
    text = (REPO_ROOT / "tests" / "test_repo_contracts.py").read_text(encoding="utf-8")
    attachment_exempt = re.search(r"ALLOWED_RAW_ATTACHMENT_SOURCES: dict\[str, str\] = \{(.*?)\}", text, re.S)
    exempt_entries = [line for line in (attachment_exempt.group(1) if attachment_exempt else "").splitlines()
                      if line.strip().startswith('"')]
    pending = len(re.findall(r'"pending marts_', text))
    status = GREEN if not exempt_entries and pending == 0 else YELLOW
    return Verdict("C5", title, status,
                   f"attachment raw-read exemptions={len(exempt_entries)}, 'pending marts_*' debts={pending}")


def c6_performance() -> Verdict:
    title = "responds fast (search p50 < 2s)"
    scopes: dict[str, tuple[str, ...]] = {
        "all tiers": (),
        "attention (self,direct,cc)": SEARCH_ATTENTION_PRIORITIES,
    }
    timings: dict[str, list[float]] = {name: [] for name in scopes}
    errors = {name: 0 for name in scopes}

    # Paired and serial: concurrent probes are a load test, not single-user
    # latency. Alternate the first scope so cache warmth cannot consistently
    # favour either path.
    for index, query in enumerate(SEARCH_PROBE_QUERIES):
        order = list(scopes.items())
        if index % 2:
            order.reverse()
        for scope_name, priorities in order:
            elapsed, issue = _validated_search_probe(query, priorities=priorities)
            if issue:
                errors[scope_name] += 1
            else:
                assert elapsed is not None
                timings[scope_name].append(elapsed)

    p50s = {
        name: statistics.median(values) if values else None
        for name, values in timings.items()
    }
    scope_statuses = []
    for name in scopes:
        # An invalid response is a search failure, not a very fast latency
        # sample. Keep it in the grade even when other probes succeeded.
        scope_statuses.append(RED if errors[name] else _latency_status(p50s[name]))
    status = worst(scope_statuses)

    # This row sampled host pressure while ITS older persisted probes ran. It
    # is useful context, but cannot establish what the host was doing during
    # the just-completed audit probes.
    bench = pdw_sql(
        "search benchmark saturation",
        "SELECT saturation, io_pressure_full_avg10, cpu_pressure_some_avg10, load_1m, cpu_count, latency_p50_ms, collected_at"
        " FROM marts_ops.search_benchmark ORDER BY collected_at DESC NULLS LAST LIMIT 1",
    )
    b = (bench or [None])[0]
    if b:
        host = (
            f"historical benchmark context at {str(b['collected_at'])[:16]}: "
            f"host {b['saturation']} (io full {b['io_pressure_full_avg10']}%, "
            f"cpu some {b['cpu_pressure_some_avg10']}%, load "
            f"{b['load_1m']}/{b['cpu_count']}, p50 {b['latency_p50_ms']}ms)"
        )
    else:
        host = "no historical benchmark host-pressure context available"

    current_slow_or_invalid = any(
        errors[name]
        or p50s[name] is None
        or p50s[name] >= SEARCH_P50_TARGET_SECONDS
        for name in scopes
    )
    if current_slow_or_invalid:
        pressure = (
            "no contemporaneous host-pressure measurement for the current "
            "slow/invalid probes; bottleneck saturation is not established"
        )
    else:
        pressure = (
            "current probes have no contemporaneous host-pressure measurement; "
            "fast responses do not require a saturation claim"
        )

    scope_evidence = []
    for name in scopes:
        p50 = p50s[name]
        p50_text = f"{p50:.2f}s" if p50 is not None else "unmeasured"
        scope_evidence.append(
            f"{name}: {len(timings[name])}/{len(SEARCH_PROBE_QUERIES)} valid, "
            f"p50 {p50_text}, errors={errors[name]}"
        )
    return Verdict(
        "C6",
        title,
        status,
        f"paired serial hybrid probes; {'; '.join(scope_evidence)}; {pressure}; {host}",
    )


def c7_pipeline_health() -> Verdict:
    title = "pipeline health inspectable via SQL and web"
    rows = pdw_sql("pipeline health", "SELECT pipeline, status FROM marts_ops.pipeline_health")
    marts = pdw_sql("mart health", "SELECT view_name, status FROM marts_ops.mart_view_health WHERE status NOT IN ('ok')")
    if rows is None or marts is None:
        return _unavailable("C7", title, "marts_ops.pipeline_health / mart_view_health")
    bad = {r["pipeline"]: r["status"] for r in rows if r["status"] in ("failing", "stale", "attention", "unknown")}
    late = [r["pipeline"] for r in rows if r["status"] == "late"]
    status = RED if any(s in ("failing", "stale") for s in bad.values()) else (YELLOW if bad or late else GREEN)
    return Verdict("C7", title, status, f"{len(rows)} pipelines; not ok: {bad or 'none'}; late: {late or 'none'}; non-ok marts: {len(marts)}")


def c8_search_quality() -> Verdict:
    title = "one hybrid search, embeddings current, quality measured"
    rows = pdw_sql("search health", "SELECT component, status, seq_lag FROM marts_ops.search_health")
    if rows is None:
        return _unavailable("C8", title, "marts_ops.search_health")
    statuses = {r["component"]: r["status"] for r in rows}
    bench = pdw_sql(
        "search benchmark",
        "SELECT mode, status, probe_queries, latency_p50_ms, labeled_cases, mrr, errors, "
        "attention_priorities_json, attention_probe_queries, "
        "attention_latency_p50_ms, attention_labeled_cases, "
        "attention_comparable_cases, attention_found, attention_mrr, "
        "attention_errors, attention_recall_lost, attention_recall_gained, "
        "attention_recall_retained, all_relevant_lower_tier, collected_at "
        "FROM marts_ops.search_benchmark WHERE mode = 'hybrid'",
    )
    b = (bench or [None])[0]
    grades: list[str] = [GREEN]
    if not statuses:
        grades.append(YELLOW)
    elif any(s in ("failing", "unknown") for s in statuses.values()):
        grades.append(RED)
    elif any(
        s in ("attention", "late", "backfilling") for s in statuses.values()
    ):
        grades.append(YELLOW)

    if not b:
        grades.append(YELLOW)
        return Verdict(
            "C8",
            title,
            worst(grades),
            f"search_health {statuses}; no hybrid benchmark row yet",
        )

    def integer(name: str) -> int | None:
        value = b.get(name)
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def number(name: str) -> float | None:
        value = b.get(name)
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    benchmark_status = str(b.get("status") or "unknown")
    if benchmark_status == "failing":
        grades.append(RED)
    elif benchmark_status in ("attention", "unknown", "no_data"):
        grades.append(YELLOW)

    all_probes = integer("probe_queries")
    all_p50_ms = number("latency_p50_ms")
    all_labels = integer("labeled_cases")
    all_mrr = number("mrr")
    all_errors = integer("errors")
    attention_probes = integer("attention_probe_queries")
    attention_p50_ms = number("attention_latency_p50_ms")
    attention_labels = integer("attention_labeled_cases")
    attention_comparable = integer("attention_comparable_cases")
    attention_mrr = number("attention_mrr")
    attention_errors = integer("attention_errors")

    for probes, p50_ms, errors in (
        (all_probes, all_p50_ms, all_errors),
        (attention_probes, attention_p50_ms, attention_errors),
    ):
        if probes is None or probes <= 0 or p50_ms is None or errors is None:
            grades.append(YELLOW)
        else:
            grades.append(_latency_status(p50_ms / 1000))
        if errors is not None and errors > 0:
            grades.append(RED)

    if all_labels is None or all_labels <= 0 or all_mrr is None:
        grades.append(YELLOW)
    elif all_mrr < MRR_FLOOR:
        grades.append(YELLOW)

    # The paired attention score describes what happened, but the all-tier
    # label set deliberately includes relevant noise/background rows that an
    # attention filter must exclude. Do not apply the all-tier MRR floor to it.
    if (
        attention_labels is None
        or attention_labels <= 0
        or attention_comparable is None
        or attention_comparable <= 0
        or attention_mrr is None
    ):
        grades.append(YELLOW)

    try:
        measured_priorities = tuple(json.loads(str(b.get("attention_priorities_json"))))
    except (TypeError, ValueError):
        measured_priorities = ()
    if measured_priorities != SEARCH_ATTENTION_PRIORITIES:
        grades.append(YELLOW)

    all_p50_text = f"{all_p50_ms:g}ms" if all_p50_ms is not None else "unmeasured"
    all_mrr_text = f"{all_mrr:g}" if all_mrr is not None else "unmeasured"
    attention_p50_text = (
        f"{attention_p50_ms:g}ms" if attention_p50_ms is not None else "unmeasured"
    )
    attention_mrr_text = (
        f"{attention_mrr:g}" if attention_mrr is not None else "unmeasured"
    )
    lower_tier = integer("all_relevant_lower_tier")
    if lower_tier is None:
        eligibility = "attention scoped relevance eligibility is not established by this schema"
    elif lower_tier > 0:
        eligibility = (
            f"{lower_tier} all-tier relevant answers were lower-tier; exclusions are expected"
        )
    else:
        eligibility = (
            "no lower-tier answer was identified, but eligible-only relevance is not "
            "independently labeled"
        )
    all_labels_text = str(all_labels) if all_labels is not None else "unmeasured"
    all_errors_text = str(all_errors) if all_errors is not None else "unmeasured"
    attention_probes_text = (
        str(attention_probes) if attention_probes is not None else "unmeasured"
    )
    attention_errors_text = (
        str(attention_errors) if attention_errors is not None else "unmeasured"
    )
    attention_labels_text = (
        str(attention_labels) if attention_labels is not None else "unmeasured"
    )
    attention_comparable_text = (
        str(attention_comparable)
        if attention_comparable is not None
        else "unmeasured"
    )
    evidence = (
        f"search_health {statuses}; benchmark {benchmark_status} at "
        f"{str(b.get('collected_at'))[:16]}: all-tier MRR {all_mrr_text} over "
        f"{all_labels_text} labels, p50 {all_p50_text}, errors={all_errors_text}; "
        f"attention p50 {attention_p50_text} over {attention_probes_text} probes, "
        f"errors={attention_errors_text}; attention MRR {attention_mrr_text} is "
        f"diagnostic, not graded against the all-tier floor; attention labels "
        f"{attention_labels_text}, comparable {attention_comparable_text}; {eligibility}"
    )
    return Verdict("C8", title, worst(grades), evidence)


def c9_one_way() -> Verdict:
    title = "one obvious way per surface"
    try:
        out = subprocess.run(["pdw", "list"], capture_output=True, text=True, timeout=30).stdout
    except (OSError, subprocess.TimeoutExpired):
        return _unavailable("C9", title, "pdw list")
    tools = set(re.findall(r"^\s*([a-z_]+)\b", out, re.M))
    expected = {"search", "sql", "schema_overview", "describe_table"}
    forbidden = {"query", "search_hybrid", "grep_rows"}
    ok = expected <= tools and not (forbidden & tools)
    return Verdict("C9", title, GREEN if ok else RED, f"CLI tools {sorted(tools & (expected | forbidden))}")


def c10_backups() -> Verdict:
    title = "backed up, restore performed"
    rows = pdw_sql("backup posture", "SELECT stanza, status, backup_count, last_full_at, full_age_seconds, last_archived_at, restore_status, last_restore_label, restore_age_seconds FROM marts_ops.pgbackrest_health")
    if not rows:
        return Verdict("C10", title, RED, "marts_ops.pgbackrest_health has no row: backup existence is unobservable")
    r = rows[0]
    count = int(r["backup_count"] or 0)
    age_days = (float(r["full_age_seconds"]) / 86400) if r["full_age_seconds"] is not None else None
    restore_days = (float(r["restore_age_seconds"]) / 86400) if r.get("restore_age_seconds") is not None else None
    status = RED if count == 0 or r["status"] in ("failing",) else (YELLOW if r["status"] in ("late", "stale", "unknown", "attention") else GREEN)
    return Verdict("C10", title, status, f"{count} backups, status {r['status']}, last full {age_days and f'{age_days:.1f}d'} ago; restore {r.get('restore_status')} ({r.get('last_restore_label') or 'none'}, {restore_days and f'{restore_days:.1f}d'} ago)")


def c11_source_slas() -> Verdict:
    title = "a source's own SLA is stated and detected"
    slack = pdw_sql("slack health", "SELECT conversation_type, status FROM marts_ops.slack_conversation_health")
    plaid = pdw_sql("plaid health", "SELECT institution_name, status FROM marts_ops.plaid_item_health")
    if slack is None or plaid is None:
        return _unavailable("C11", title, "slack_conversation_health / plaid_item_health")
    bad = [f"slack:{r['conversation_type']}={r['status']}" for r in slack if r["status"] != "ok"]
    bad += [f"plaid:{r['institution_name']}={r['status']}" for r in plaid if r["status"] not in ("ok",)]
    return Verdict("C11", title, YELLOW if bad else GREEN, f"slack types {len(slack)}, plaid items {len(plaid)}; not ok: {bad or 'none'}")


def s1_slack() -> Verdict:
    title = "Slack: everything synced, DMs current"
    rows = pdw_sql("slack recency", "SELECT conversation_type, status, refreshed_fraction, message_age_seconds FROM marts_ops.slack_conversation_health")
    if not rows:
        return _unavailable("S1", title, "marts_ops.slack_conversation_health")
    bad = [r["conversation_type"] for r in rows if r["status"] != "ok"]
    dm = next((r for r in rows if r["conversation_type"] == "im"), None)
    dm_age_h = (float(dm["message_age_seconds"]) / 3600) if dm and dm["message_age_seconds"] is not None else None
    status = RED if bad else (YELLOW if dm_age_h is not None and dm_age_h > 24 else GREEN)
    return Verdict("S1", title, status, f"not ok types: {bad or 'none'}; newest DM {dm_age_h and f'{dm_age_h:.1f}h'} ago")


def s2_voice() -> Verdict:
    title = "voice memos: every source transcribed, enriched, calendar-matched"
    # A recording the provider will never accept (rejected: no spoken audio,
    # too short) and an empty upload (size_bytes = 0, which the candidate
    # query excludes) are not a backlog; counting them read S2 red forever
    # on eighteen recordings nothing could ever transcribe.
    rows = pdw_sql("voice coverage", """
        WITH r AS (
            -- Untranscribable: an empty upload, a provider rejection (no spoken
            -- audio, too short), or a COMPLETED run whose transcript is empty
            -- because the audio is silent -- the mart shows NULL for all three.
            -- Joined on (source, recording_id): the id is unique within a source.
            SELECT r.*, (r.size_bytes = 0 OR EXISTS (
                        SELECT 1 FROM derived_voice_memos.transcription_runs run
                        WHERE run.source = r.source AND run.recording_id = r.recording_id
                          AND run.status IN ('rejected', 'completed'))) AS untranscribable
            FROM marts_voice_memos.recordings r WHERE r.is_deleted = 0)
        SELECT source, count(*) AS recordings,
               count(*) FILTER (WHERE transcript IS NULL AND NOT untranscribable
                                  AND recorded_at < now() - interval '2 days') AS untranscribed,
               count(*) FILTER (WHERE transcript IS NULL AND untranscribable) AS untranscribable,
               count(*) FILTER (WHERE summary IS NULL AND transcript IS NOT NULL
                                  AND recorded_at < now() - interval '2 days') AS unenriched,
               count(*) FILTER (WHERE calendar_event_id IS NOT NULL) AS matched
        FROM r GROUP BY source ORDER BY source""")
    if rows is None:
        return _unavailable("S2", title, "marts_voice_memos.recordings")
    backlog = sum(int(r["untranscribed"] or 0) + int(r["unenriched"] or 0) for r in rows)
    sources = {r["source"] for r in rows}
    expected = {"apple_voice_memos", "alice_voice_recordings", "apple_notes"}
    status = GREEN if backlog == 0 and expected <= sources else (YELLOW if backlog < 10 else RED)
    return Verdict("S2", title, status, "; ".join(f"{r['source']}: {r['recordings']} rec, {r['untranscribed']} untranscribed ({r['untranscribable']} untranscribable), {r['unenriched']} unenriched, {r['matched']} matched" for r in rows))


def s3_finance() -> Verdict:
    title = "finances: every source, mortgage, liabilities, PE; receipts linked"
    stale = pdw_sql("valuation staleness", "SELECT kind, staleness, age_days FROM marts_finance.net_worth WHERE staleness <> 'ok'")
    if stale is None:
        # Pre-2026-08-27 deployments have no staleness column: judge by the
        # same per-kind refresh the view uses, computed here.
        stale = pdw_sql("valuation age", """
            SELECT kind, CASE WHEN CURRENT_DATE - as_of > 3 * expected THEN 'stale'
                              WHEN CURRENT_DATE - as_of > expected THEN 'late' ELSE 'ok' END AS staleness,
                   CURRENT_DATE - as_of AS age_days
            FROM (SELECT kind, as_of, CASE kind WHEN 'mortgage' THEN 35 WHEN 'property' THEN 120 WHEN 'vehicle' THEN 120
                       WHEN 'private_fund' THEN 120 WHEN 'receivable' THEN 120 WHEN 'other' THEN 120 ELSE 3 END AS expected
                  FROM marts_finance.net_worth) x
            WHERE CURRENT_DATE - as_of > expected""")
    kinds = pdw_sql("account kinds", "SELECT DISTINCT kind FROM marts_finance.net_worth")
    receipts = pdw_sql("receipt coverage", "SELECT decision, count(*) AS n FROM marts_finance.transaction_receipts GROUP BY 1") \
        or pdw_sql("receipt coverage", "SELECT decision, count(*) AS n FROM derived_receipts.transaction_receipts GROUP BY 1")
    if stale is None or kinds is None:
        return _unavailable("S3", title, "marts_finance.net_worth")
    have = {r["kind"] for r in kinds}
    missing = {"mortgage", "private_fund", "brokerage", "credit"} - have
    status = RED if any(r["staleness"] == "stale" for r in stale) or missing else (YELLOW if stale else GREEN)
    return Verdict("S3", title, status, f"kinds {sorted(have)}; missing {sorted(missing) or 'none'}; stale/late: {[(r['kind'], r['staleness'], r['age_days']) for r in stale] or 'none'}; receipts {receipts and {r['decision']: r['n'] for r in receipts}}")


CHECKS = [c1_timeline_coverage, c2_priority_tiers, c3_agents_start_at_timeline, c4_raw_data_queryable,
          c5_layering, c6_performance, c7_pipeline_health, c8_search_quality, c9_one_way, c10_backups,
          c11_source_slas, s1_slack, s2_voice, s3_finance]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--skip-latency", action="store_true", help="skip the live search timing probe")
    args = parser.parse_args(argv)
    verdicts: list[Verdict] = []
    for check in CHECKS:
        if args.skip_latency and check is c6_performance:
            verdicts.append(Verdict("C6", "responds fast (search p50 < 2s)", YELLOW, "latency probe skipped"))
            continue
        try:
            verdicts.append(check())
        except Exception as error:  # noqa: BLE001 - one broken check must not hide the rest
            verdicts.append(Verdict(check.__name__[:3].upper().rstrip("_"), check.__name__, YELLOW, f"check crashed: {error}"))
    if args.json:
        print(json.dumps([asdict(v) for v in verdicts], indent=2))
    else:
        for v in verdicts:
            print(f"{v.status.upper():6} {v.contract:4} {v.title}\n       {v.evidence}")
        print(f"\noverall: {worst([v.status for v in verdicts]).upper()}")
    return 0 if worst([v.status for v in verdicts]) == GREEN else 1


if __name__ == "__main__":
    sys.exit(main())
