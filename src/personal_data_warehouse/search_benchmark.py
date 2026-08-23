"""Labeled retrieval benchmark for timeline search.

This measures *where a known-correct answer actually ranks*, against labels a
human (or an agent forbidden from using the ranker) produced independently.
That is the difference between this and ``search_retrieval_eval``: the replay
evaluator judges a retriever against the results the previous retriever
returned, so it can only detect regressions, never an improvement.  A label set
built without the ranker's help can measure both.

Privacy: the labels are Zach's private queries and timeline references, so the
label file, every report, and every log stay under the gitignored
``.search-eval/`` directory.  Only this harness belongs in source control, and
``assert_private_path`` enforces that at runtime.

Latency: search is slow enough (tens of seconds per hybrid call) that a serial
run is impractical, so scored calls are fanned out across a thread pool.
Timings collected under concurrency are *not* comparable to single-user latency
and are reported separately from the optional serial latency sample.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence
import argparse
import json
import re
import statistics
import subprocess
import time


DEFAULT_LABELS = Path(".search-eval/ground_truth.json")
DEFAULT_REPORT = Path(".search-eval/benchmark_report.json")
PRIVATE_DIR_NAME = ".search-eval"

DEFAULT_MODES = ("hybrid", "keyword", "exact")
DEFAULT_DEPTH = 50
DEFAULT_WORKERS = 8
SOFT_MATCH_SECONDS = 3600

HIT_THRESHOLDS = (1, 5, 10)


class PrivateOutputError(RuntimeError):
    """Raised when a benchmark artifact would be written outside the private dir."""


def assert_private_path(path: Path) -> Path:
    """Refuse to write labels, reports or logs anywhere git could pick them up."""

    if PRIVATE_DIR_NAME not in Path(path).parts:
        raise PrivateOutputError(
            f"{path} is outside {PRIVATE_DIR_NAME}/; benchmark artifacts contain "
            "private queries and timeline references and must never be committed"
        )
    return path


def parse_timestamp(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class ResultRow:
    """One row returned by the ``search`` tool."""

    ref: str
    source: str = ""
    context: str = ""
    event_ts: str = ""
    title: str = ""
    text: str = ""

    @property
    def timestamp(self) -> datetime | None:
        return parse_timestamp(self.event_ts)

    @property
    def haystack(self) -> str:
        return f"{self.title}\n{self.text}"


@dataclass(frozen=True)
class SearchResult:
    mode: str = ""
    rows: tuple[ResultRow, ...] = ()
    fallback_reason: str = ""
    error: str = ""
    elapsed_seconds: float = 0.0


@dataclass(frozen=True)
class BenchmarkCase:
    query: str
    stratum: str
    verdict: str
    truth_refs: tuple[str, ...]
    truth_predicate: dict[str, Any] | None
    ambiguous: bool
    note: str
    sources: tuple[str, ...] = ()
    since: str = ""

    @property
    def relevance_basis(self) -> str:
        """What a result may match on.  Both bases can apply to one case."""

        if self.truth_refs and self.truth_predicate:
            return "refs+predicate"
        return "refs" if self.truth_refs else "predicate"


@dataclass
class ContaminationFilter:
    """Drops results produced by the benchmark's own investigation.

    Agent sessions are themselves indexed into the timeline, so a session that
    discusses a benchmark query quotes that query verbatim and then ranks for
    it.  Left in, those rows both fake hits and displace real answers.
    """

    session_ids: tuple[str, ...] = ()
    cutoff: datetime | None = None

    def is_contaminated(self, row: ResultRow) -> bool:
        if any(session_id and session_id in row.ref for session_id in self.session_ids):
            return True
        if self.cutoff is not None and row.source == "agent_session":
            stamp = row.timestamp
            if stamp is not None and stamp >= self.cutoff:
                return True
        return False

    def apply(self, rows: Sequence[ResultRow]) -> tuple[list[ResultRow], int]:
        kept = [row for row in rows if not self.is_contaminated(row)]
        return kept, len(rows) - len(kept)


def matches_predicate(row: ResultRow, predicate: dict[str, Any] | None) -> bool:
    """Whether a result satisfies a predicate label.

    Predicate labels exist for queries with hundreds of equally-correct answers
    (`any out-of-office notice from 2026`), where enumerating refs would
    understate recall by sampling a handful of a large relevant set.  An empty
    predicate matches nothing: it is a labeling mistake, not a wildcard.
    """

    if not predicate:
        return False
    sources = predicate.get("sources")
    if sources and row.source not in set(sources):
        return False
    stamp = row.timestamp
    since = parse_timestamp(predicate.get("since"))
    if since is not None:
        if stamp is None or stamp < _as_utc(since):
            return False
    until = parse_timestamp(predicate.get("until"))
    if until is not None:
        if stamp is None or stamp >= _as_utc(until):
            return False
    pattern = predicate.get("text_regex")
    if pattern and not re.search(str(pattern), row.haystack, re.IGNORECASE):
        return False
    return True


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _soft_match(row: ResultRow, truth_meta: Sequence[tuple[str, str, datetime]]) -> bool:
    """Accept a neighbouring event from the same chunk window.

    The semantic branch chunks chat sources into (context, hour) windows, so it
    can legitimately return a different message from the window that holds the
    labeled answer.  Scoring that as a miss would penalise the retriever for the
    indexer's granularity.
    """

    stamp = row.timestamp
    if stamp is None:
        return False
    for source, context, truth_stamp in truth_meta:
        if row.source == source and row.context == context:
            if abs((stamp - truth_stamp).total_seconds()) <= SOFT_MATCH_SECONDS:
                return True
    return False


def first_relevant_rank(
    rows: Sequence[ResultRow],
    truth_refs: set[str],
    truth_meta: Sequence[tuple[str, str, datetime]],
    predicate: dict[str, Any] | None,
) -> int | None:
    """1-based rank of the first relevant result, or None if absent."""

    for index, row in enumerate(rows, start=1):
        if row.ref in truth_refs:
            return index
        if predicate and matches_predicate(row, predicate):
            return index
        if truth_meta and _soft_match(row, truth_meta):
            return index
    return None


def parse_search_payload(raw: str) -> SearchResult:
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as error:
        return SearchResult(error=f"invalid JSON response: {error}")
    rows = []
    for item in payload.get("rows") or []:
        if not isinstance(item, dict):
            continue
        rows.append(
            ResultRow(
                ref=str(item.get("ref") or ""),
                source=str(item.get("source") or ""),
                context=str(item.get("context") or ""),
                event_ts=str(item.get("event_ts") or item.get("occurred_at") or ""),
                title=str(item.get("title") or ""),
                text=str(item.get("text") or ""),
            )
        )
    return SearchResult(
        mode=str(payload.get("mode") or ""),
        rows=tuple(rows),
        fallback_reason=str(payload.get("fallback_reason") or ""),
    )


def load_cases(path: Path, *, include_not_in_corpus: bool = False) -> list[BenchmarkCase]:
    """Read labels, rejecting cases that carry no relevance judgment."""

    cases: list[BenchmarkCase] = []
    for raw in json.loads(Path(path).read_text(encoding="utf-8")):
        verdict = str(raw.get("verdict") or "FOUND")
        if verdict == "NOT_IN_CORPUS" and not include_not_in_corpus:
            continue
        refs = tuple(raw.get("truth_refs") or ())
        predicate = raw.get("truth_predicate") or None
        if not refs and not predicate:
            raise ValueError(
                f"case {raw.get('query')!r} has neither truth_refs nor truth_predicate; "
                "an unlabeled case cannot be scored"
            )
        cases.append(
            BenchmarkCase(
                query=str(raw["query"]),
                stratum=str(raw.get("stratum") or "unclassified"),
                verdict=verdict,
                truth_refs=refs,
                truth_predicate=predicate,
                ambiguous=bool(raw.get("ambiguous")),
                note=str(raw.get("note") or ""),
                sources=tuple(raw.get("sources") or ()),
                since=str(raw.get("since") or ""),
            )
        )
    return cases


def _metrics(ranks: Sequence[int | None], depth: int) -> dict[str, Any]:
    total = len(ranks)
    found = [r for r in ranks if r]
    out: dict[str, Any] = {"queries": total}
    for threshold in HIT_THRESHOLDS:
        out[f"hit_at_{threshold}"] = sum(1 for r in found if r <= threshold)
    out["found"] = len(found)
    out["depth"] = depth
    out["mrr"] = (sum(1.0 / r for r in found) / total) if total else 0.0
    out["median_rank_when_found"] = statistics.median(found) if found else None
    return out


def summarize(rows: Sequence[dict[str, Any]], *, depth: int) -> dict[str, Any]:
    modes = [m for m in DEFAULT_MODES if rows and m in rows[0]]
    overall = {m: _metrics([r[m]["rank"] for r in rows], depth) for m in modes}
    strata = sorted({str(r.get("stratum") or "unclassified") for r in rows})
    by_stratum = {
        stratum: {
            m: _metrics(
                [r[m]["rank"] for r in rows if (r.get("stratum") or "unclassified") == stratum],
                depth,
            )
            for m in modes
        }
        for stratum in strata
    }
    return {"overall": overall, "by_stratum": by_stratum}


# --------------------------------------------------------------------------
# Impure layer: talking to the deployment under test.
# --------------------------------------------------------------------------


def trim_to_json(stdout: str) -> str:
    """Drop any human-readable preamble the CLI printed before the payload.

    ``pdw call`` returns an object and ``pdw sql --output json`` returns an
    array, so anchoring on ``{`` alone lands *inside* an array and decodes only
    its first element before failing on the rest.
    """

    candidates = [pos for pos in (stdout.find("{"), stdout.find("[")) if pos != -1]
    return stdout[min(candidates) :] if candidates else stdout


def _pdw_json(args: list[str], *, timeout: float) -> Any:
    completed = subprocess.run(
        ["pdw", *args], check=True, capture_output=True, text=True, timeout=timeout
    )
    return json.loads(trim_to_json(completed.stdout))


def run_search(
    query: str, mode: str, depth: int, *, sources: Sequence[str] = (), since: str = "",
    timeout: float = 420.0,
) -> SearchResult:
    request: dict[str, Any] = {"query": query, "mode": mode, "max_results": depth}
    if sources:
        request["sources"] = list(sources)
    if since:
        request["since"] = since
    started = time.time()
    try:
        payload = _pdw_json(["call", "search", "--data", json.dumps(request)], timeout=timeout)
        result = parse_search_payload(json.dumps(payload))
    except (subprocess.SubprocessError, OSError, ValueError) as error:
        result = SearchResult(mode=mode, error=str(error)[:200])
    return SearchResult(
        mode=result.mode or mode,
        rows=result.rows,
        fallback_reason=result.fallback_reason,
        error=result.error,
        elapsed_seconds=time.time() - started,
    )


def resolve_truth_metadata(refs: Sequence[str]) -> dict[str, tuple[str, str, datetime]]:
    """Look up (source, context, event_ts) for each labeled ref.

    Also proves the labels still point at live rows: an unresolved ref is a
    stale label, not a retrieval failure, and must not be scored as one.
    """

    pairs = [ref.split(":", 1) for ref in refs if ":" in ref]
    if not pairs:
        return {}
    values = ",".join(
        "(" + ",".join("'" + part.replace("'", "''") + "'" for part in pair) + ")"
        for pair in pairs
    )
    sql = (
        f"WITH want(adapter, event_id) AS (VALUES {values}) "
        "SELECT e.adapter||':'||e.event_id AS ref, e.source, coalesce(e.context,'') AS context, "
        "e.event_ts FROM timeline.events e "
        "JOIN want w ON w.adapter = e.adapter AND w.event_id = e.event_id"
    )
    rows = _pdw_json(
        ["sql", "--output", "json", "-q",
         "resolve labeled ground-truth refs for the retrieval benchmark", sql],
        timeout=180,
    )
    out: dict[str, tuple[str, str, datetime]] = {}
    for row in rows if isinstance(rows, list) else []:
        stamp = parse_timestamp(row.get("event_ts"))
        if stamp is not None:
            out[str(row["ref"])] = (str(row["source"]), str(row["context"] or ""), stamp)
    return out


def capture_environment() -> dict[str, Any]:
    """Stamp what was under test, so two reports can be compared honestly."""

    env: dict[str, Any] = {}
    try:
        env["git_sha"] = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, timeout=15
        ).stdout.strip()
    except (subprocess.SubprocessError, OSError):
        env["git_sha"] = ""
    try:
        env["pdw_version"] = subprocess.run(
            ["pdw", "version"], capture_output=True, text=True, timeout=30
        ).stdout.strip()
    except (subprocess.SubprocessError, OSError):
        env["pdw_version"] = ""
    corpus_sql = (
        "SELECT (SELECT count(*) FROM derived_search.chunks) AS chunks, "
        "(SELECT count(*) FROM derived_search.chunk_embeddings) AS embeddings, "
        "(SELECT max(seq) FROM timeline.events) AS timeline_max_seq"
    )
    try:
        rows = _pdw_json(
            ["sql", "--no-timeout", "--output", "json", "-q",
             "stamp corpus frontiers for the retrieval benchmark", corpus_sql],
            timeout=180,
        )
        env["corpus"] = rows[0] if isinstance(rows, list) and rows else {}
    except (subprocess.SubprocessError, OSError, ValueError, KeyError, IndexError) as error:
        env["corpus"] = {"error": str(error)[:160]}
    env["captured_at"] = datetime.now(timezone.utc).isoformat()
    return env


def run_benchmark(
    cases: Sequence[BenchmarkCase],
    *,
    modes: Sequence[str] = DEFAULT_MODES,
    depth: int = DEFAULT_DEPTH,
    workers: int = DEFAULT_WORKERS,
    contamination: ContaminationFilter | None = None,
    progress: bool = True,
) -> dict[str, Any]:
    contamination = contamination or ContaminationFilter()
    truth_meta = resolve_truth_metadata(sorted({r for c in cases for r in c.truth_refs}))
    unresolved = sorted({r for c in cases for r in c.truth_refs} - set(truth_meta))

    # Identical (query, mode) pairs across cases are executed once.  Searches
    # cost tens of seconds, so the fan-out is what makes this runnable at all.
    tasks = sorted({(c.query, m, tuple(c.sources), c.since) for c in cases for m in modes})
    done = {"n": 0}

    def execute(task: tuple[str, str, tuple[str, ...], str]) -> tuple[tuple, SearchResult]:
        query, mode, sources, since = task
        result = run_search(query, mode, depth, sources=sources, since=since)
        done["n"] += 1
        if progress:
            print(
                f"  [{done['n']:3d}/{len(tasks)}] {mode:8s} {result.elapsed_seconds:6.1f}s "
                f"n={len(result.rows):3d}"
                f"{' FALLBACK' if result.fallback_reason else ''}"
                f"{' ERR' if result.error else ''}  {query[:44]}",
                flush=True,
            )
        return task, result

    started = time.time()
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        results = dict(pool.map(execute, tasks))
    wall_seconds = time.time() - started

    rows: list[dict[str, Any]] = []
    for case in cases:
        row: dict[str, Any] = {
            "query": case.query,
            "stratum": case.stratum,
            "verdict": case.verdict,
            "ambiguous": case.ambiguous,
            "relevance_basis": case.relevance_basis,
            "note": case.note,
        }
        meta = [truth_meta[r] for r in case.truth_refs if r in truth_meta]
        for mode in modes:
            result = results[(case.query, mode, tuple(case.sources), case.since)]
            kept, dropped = contamination.apply(result.rows)
            row[mode] = {
                "rank": first_relevant_rank(kept, set(case.truth_refs), meta, case.truth_predicate),
                "returned": len(kept),
                "dropped_contaminated": dropped,
                "fallback_reason": result.fallback_reason,
                "error": result.error,
                "elapsed_seconds_concurrent": round(result.elapsed_seconds, 2),
            }
        rows.append(row)

    latency = {}
    for mode in modes:
        samples = sorted(
            r.elapsed_seconds for (_, m, _, _), r in results.items() if m == mode and not r.error
        )
        if samples:
            latency[mode] = {
                "n": len(samples),
                "min": round(samples[0], 2),
                "p50": round(statistics.median(samples), 2),
                "p90": round(samples[min(int(len(samples) * 0.9), len(samples) - 1)], 2),
                "max": round(samples[-1], 2),
            }

    return {
        "environment": capture_environment(),
        "config": {
            "depth": depth, "modes": list(modes), "workers": workers,
            "queries": len(cases), "calls": len(tasks),
            "contamination_session_ids": list(contamination.session_ids),
            "contamination_cutoff": (
                contamination.cutoff.isoformat() if contamination.cutoff else None
            ),
        },
        "unresolved_truth_refs": unresolved,
        "wall_seconds": round(wall_seconds, 1),
        "latency_under_concurrency": latency,
        "latency_note": (
            "measured with concurrent workers; NOT comparable to single-user latency. "
            "Use `benchmark latency` for a serial sample."
        ),
        "summary": summarize(rows, depth=depth),
        "results": rows,
    }


def measure_serial_latency(
    queries: Sequence[str], *, modes: Sequence[str], depth: int, repeats: int
) -> dict[str, Any]:
    """Time searches one at a time, which is the only comparable latency number."""

    samples: dict[str, list[float]] = {m: [] for m in modes}
    for _ in range(repeats):
        for query in queries:
            for mode in modes:
                result = run_search(query, mode, depth)
                if not result.error:
                    samples[mode].append(result.elapsed_seconds)
                    print(f"  serial {mode:8s} {result.elapsed_seconds:6.1f}s  {query[:44]}",
                          flush=True)
    out = {}
    for mode, values in samples.items():
        if values:
            ordered = sorted(values)
            out[mode] = {
                "n": len(ordered),
                "min": round(ordered[0], 2),
                "p50": round(statistics.median(ordered), 2),
                "p90": round(ordered[min(int(len(ordered) * 0.9), len(ordered) - 1)], 2),
                "max": round(ordered[-1], 2),
            }
    return out


def _print_report(report: dict[str, Any]) -> None:
    modes = report["config"]["modes"]
    width = 52
    print(f"\n{'query':<{width}}" + "".join(f"{m:>10}" for m in modes))
    print("-" * (width + 10 * len(modes)))
    for row in report["results"]:
        cells = []
        for mode in modes:
            entry = row[mode]
            if entry["rank"]:
                cells.append(f"#{entry['rank']}")
            elif entry["error"]:
                cells.append("ERR")
            elif entry["returned"] == 0:
                cells.append("0rows")
            else:
                cells.append(f">{entry['returned']}")
        flag = "~" if row["ambiguous"] else " "
        print(f"{flag}{row['query'][:width - 1]:<{width - 1}}" + "".join(f"{c:>10}" for c in cells))

    print("\n=== OVERALL (rank of first labeled answer) ===")
    for mode, m in report["summary"]["overall"].items():
        print(
            f"  {mode:8s} hit@1={m['hit_at_1']}/{m['queries']}  hit@5={m['hit_at_5']}/{m['queries']}"
            f"  hit@10={m['hit_at_10']}/{m['queries']}  found@{m['depth']}={m['found']}/{m['queries']}"
            f"  MRR={m['mrr']:.3f}"
        )
    print("\n=== BY STRATUM ===")
    for stratum, modes_summary in report["summary"]["by_stratum"].items():
        print(f"  {stratum}")
        for mode, m in modes_summary.items():
            print(
                f"    {mode:8s} hit@10={m['hit_at_10']}/{m['queries']}"
                f"  found={m['found']}/{m['queries']}  MRR={m['mrr']:.3f}"
            )
    if report["unresolved_truth_refs"]:
        print(f"\n  WARNING stale labels (ref not in timeline.events): "
              f"{len(report['unresolved_truth_refs'])}")
        for ref in report["unresolved_truth_refs"][:5]:
            print(f"    {ref}")
    print(f"\n  wall clock {report['wall_seconds']}s for {report['config']['calls']} calls "
          f"across {report['config']['workers']} workers")
    print(f"  latency under concurrency (not single-user): {report['latency_under_concurrency']}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run", help="score labeled queries against the live deployment")
    run.add_argument("--labels", type=Path, default=DEFAULT_LABELS)
    run.add_argument("--output", type=Path, default=DEFAULT_REPORT)
    run.add_argument("--depth", type=int, default=DEFAULT_DEPTH)
    run.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                     help="parallel search calls; a hybrid call takes tens of seconds")
    run.add_argument("--modes", default=",".join(DEFAULT_MODES))
    run.add_argument("--exclude-session", action="append", default=[],
                     help="drop results from this agent session id (repeatable)")
    run.add_argument("--exclude-agent-sessions-since", default="",
                     help="drop agent_session results at/after this ISO date")
    run.add_argument("--quiet", action="store_true")

    latency = subparsers.add_parser("latency", help="serial latency sample (comparable numbers)")
    latency.add_argument("--labels", type=Path, default=DEFAULT_LABELS)
    latency.add_argument("--output", type=Path, default=Path(".search-eval/latency_report.json"))
    latency.add_argument("--depth", type=int, default=10)
    latency.add_argument("--modes", default="hybrid,keyword")
    latency.add_argument("--sample", type=int, default=6)
    latency.add_argument("--repeats", type=int, default=1)

    args = parser.parse_args(argv)
    assert_private_path(args.output)
    modes = tuple(m.strip() for m in args.modes.split(",") if m.strip())
    cases = load_cases(args.labels)

    if args.command == "latency":
        queries = [c.query for c in cases][: args.sample]
        report = {
            "environment": capture_environment(),
            "config": {"depth": args.depth, "modes": list(modes), "repeats": args.repeats,
                       "queries": queries},
            "serial_latency": measure_serial_latency(
                queries, modes=modes, depth=args.depth, repeats=args.repeats
            ),
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report["serial_latency"], indent=2))
        return 0

    cutoff = parse_timestamp(args.exclude_agent_sessions_since) if \
        args.exclude_agent_sessions_since else None
    contamination = ContaminationFilter(
        session_ids=tuple(args.exclude_session),
        cutoff=_as_utc(cutoff) if cutoff else None,
    )
    print(f"{len(cases)} labeled queries x {len(modes)} modes, depth={args.depth}, "
          f"workers={args.workers}", flush=True)
    report = run_benchmark(
        cases, modes=modes, depth=args.depth, workers=args.workers,
        contamination=contamination, progress=not args.quiet,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    _print_report(report)
    print(f"\nwrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
