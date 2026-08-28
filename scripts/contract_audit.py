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
import time
from dataclasses import asdict, dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

GREEN, YELLOW, RED = "green", "yellow", "red"

#: The search latency the goal set for the tool, end to end through the CLI.
SEARCH_P50_TARGET_SECONDS = 2.0
SEARCH_P50_YELLOW_SECONDS = 5.0
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


# --- contracts ----------------------------------------------------------------

def c1_timeline_coverage() -> Verdict:
    title = "everything synced lands on timeline.events"
    rows = pdw_sql("adapter health", "SELECT adapter, status FROM marts_ops.timeline_adapter_health")
    if rows is None:
        return _unavailable("C1", title, "marts_ops.timeline_adapter_health")
    failing = sorted(r["adapter"] for r in rows if r["status"] == "failing")
    backfilling = sorted(r["adapter"] for r in rows if r["status"] == "backfilling")
    status = RED if failing else (YELLOW if backfilling else GREEN)
    return Verdict("C1", title, status,
                   f"{len(rows)} adapters; failing={failing or 'none'}; backfilling={backfilling or 'none'}")


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
    timings: list[float] = []
    for query in SEARCH_PROBE_QUERIES:
        started = time.time()
        try:
            subprocess.run(["pdw", "search", "--output", "json", "-n", "20", "--", query],
                           capture_output=True, text=True, timeout=90)
        except (OSError, subprocess.TimeoutExpired):
            timings.append(90.0)
            continue
        timings.append(time.time() - started)
    p50 = statistics.median(timings)
    status = GREEN if p50 < SEARCH_P50_TARGET_SECONDS else (YELLOW if p50 < SEARCH_P50_YELLOW_SECONDS else RED)
    return Verdict("C6", title, status, f"hybrid search p50 {p50:.2f}s over {len(timings)} novel queries ({', '.join(f'{t:.1f}' for t in timings)})")


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
    bench = pdw_sql("search benchmark", "SELECT mode, status, labeled_cases, mrr, latency_p50_ms, collected_at FROM marts_ops.search_benchmark")
    b = (bench or [None])[0]
    labels = bool(b and int(b.get("labeled_cases") or 0) > 0)
    status = GREEN
    if any(s in ("failing", "unknown") for s in statuses.values()):
        status = RED
    elif any(s in ("late", "backfilling") for s in statuses.values()) or not labels or (b and b["status"] in ("attention", "unknown")):
        status = YELLOW
    bench_ev = f"benchmark {b['status']}: MRR {b['mrr']} over {b['labeled_cases']} cases, p50 {b['latency_p50_ms']}ms ({str(b['collected_at'])[:16]})" if b else "no benchmark row yet"
    return Verdict("C8", title, status, f"search_health {statuses}; {bench_ev}")


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
    rows = pdw_sql("voice coverage", """
        SELECT source, count(*) AS recordings,
               count(*) FILTER (WHERE transcript IS NULL AND recorded_at < now() - interval '2 days') AS untranscribed,
               count(*) FILTER (WHERE summary IS NULL AND recorded_at < now() - interval '2 days') AS unenriched,
               count(*) FILTER (WHERE calendar_event_id IS NOT NULL) AS matched
        FROM marts_voice_memos.recordings WHERE is_deleted = 0 GROUP BY source ORDER BY source""")
    if rows is None:
        return _unavailable("S2", title, "marts_voice_memos.recordings")
    backlog = sum(int(r["untranscribed"] or 0) for r in rows)
    sources = {r["source"] for r in rows}
    status = GREEN if backlog == 0 and "apple_notes" in sources else (YELLOW if backlog < 10 else RED)
    return Verdict("S2", title, status, "; ".join(f"{r['source']}: {r['recordings']} rec, {r['untranscribed']} untranscribed, {r['unenriched']} unenriched, {r['matched']} matched" for r in rows))


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
