"""Offline fusion lab for the hybrid search.

Collects each hybrid leg's raw evidence (BM25 refs, ANN legs, literal refs)
once per labeled query, then re-fuses it in Python under different weights
and reports the benchmark metrics for every weighting -- so a fusion change
is measured before it is written into ``search_hybrid_fuse``.

Legs are collected through ``pdw sql`` against the deployment under test, the
same functions the app calls, with the single instructed representation the
Go client builds. Embeddings come from the deployment's own endpoint.

Artifacts stay under the gitignored ``.search-eval/``; nothing here is a
label and nothing is written elsewhere.

    uv run python scripts/search_fusion_lab.py collect --workers 6
    uv run python scripts/search_fusion_lab.py score
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from personal_data_warehouse.search_benchmark import (  # noqa: E402
    ContaminationFilter,
    ResultRow,
    _as_utc,
    assert_private_path,
    first_relevant_rank,
    load_cases,
    parse_timestamp,
    resolve_truth_metadata,
    trim_to_json,
)
from personal_data_warehouse.postgres import (  # noqa: E402
    SEARCH_HYBRID_EXACT_WEIGHT,
    SEARCH_HYBRID_LEXICAL_HEAD_RANKS,
    SEARCH_HYBRID_LEXICAL_HEAD_WEIGHT,
    SEARCH_HYBRID_SEMANTIC_WEIGHT,
)

EVIDENCE = Path(".search-eval/fusion_evidence.json")
DEPTH = 50
RRF_K = 60
# Increment whenever collection changes the request shape. Evidence is costly
# and cached, but reusing four-vector rows after production moved to one vector
# would make a fast tuning run confidently score a ranker that no longer runs.
COLLECTION_VERSION = 2
# The Go client's sentence detector, mirrored so the lab applies the BM25 head
# bonus to exactly the requests the production fuse does.
SENTENCE_WORDS = {
    "a", "an", "the", "my", "our", "your", "their", "is", "are", "was", "were", "will",
    "would", "can", "of", "for", "with", "that", "this", "at", "on", "in", "to", "from",
    "and", "or", "by", "about", "how", "what", "when", "where", "why", "who", "which",
    "did", "does", "do", "should", "could", "me", "i",
}


def is_sentence(query: str) -> bool:
    fields = query.lower().split()
    if len(fields) < 5:
        return False
    return sum(1 for f in fields if f.strip(".,!?;:'\"") in SENTENCE_WORDS) >= 2


def embed(texts: list[str]) -> list[list[float]]:
    base = os.environ.get("SEARCH_EMBEDDINGS_BASE_URL", "http://100.104.110.27:8485/v1").rstrip("/")
    model = os.environ.get("SEARCH_EMBEDDINGS_MODEL", "Qwen/Qwen3-Embedding-4B")
    req = urllib.request.Request(
        base + "/embeddings",
        data=json.dumps({"input": texts, "model": model}).encode(),
        headers={"Content-Type": "application/json"},
    )
    payload = json.load(urllib.request.urlopen(req, timeout=120))
    out = []
    for item in payload["data"]:
        vec = item["embedding"][:512]
        norm = math.sqrt(sum(x * x for x in vec)) or 1.0
        out.append([x / norm for x in vec])
    return out


def literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in vec) + "]"


def pdw_sql(question: str, sql: str) -> list[dict[str, Any]]:
    done = subprocess.run(
        ["pdw", "sql", "--no-timeout", "--output", "json", "-q", question, sql],
        capture_output=True, text=True, timeout=300,
    )
    if done.returncode != 0:
        raise RuntimeError((done.stderr or done.stdout)[:300])
    rows = json.loads(trim_to_json(done.stdout))
    return rows if isinstance(rows, list) else []


def _lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _arr(values: tuple[str, ...]) -> str:
    return "ARRAY[" + ",".join(_lit(v) for v in values) + "]::text[]" if values else "NULL::text[]"


def case_key(case) -> str:
    """Cache evidence by the complete search request, not only its text."""

    if not case.sources and not case.since:
        # Preserve the existing unscoped evidence cache: collecting its ANN
        # neighborhoods takes hours on a cold production index.
        return case.query
    return json.dumps(
        [case.query, list(case.sources), case.since],
        separators=(",", ":"),
    )


def production_fusion_config() -> dict[str, Any]:
    """The ranker's live constants, imported so the tuning lab cannot drift."""

    return {
        "w_lex": 1.0,
        "w_sem": SEARCH_HYBRID_SEMANTIC_WEIGHT,
        "w_exact": SEARCH_HYBRID_EXACT_WEIGHT,
        "lex_top": (
            SEARCH_HYBRID_LEXICAL_HEAD_RANKS,
            SEARCH_HYBRID_LEXICAL_HEAD_WEIGHT,
        ),
    }


def collect_case(case, model: str, prefix: str) -> dict[str, Any]:
    vectors = embed([prefix + case.query])
    sources = _arr(case.sources)
    since = _lit(case.since) + "::timestamptz" if case.since else "NULL::timestamptz"
    q = _lit(case.query)
    lexical = pdw_sql(
        "fusion lab: BM25 leg",
        f"SELECT ref FROM timeline.search_text({q}, {DEPTH}, {sources}, {since}, NULL)",
    )
    exact = pdw_sql(
        "fusion lab: literal leg",
        f"SELECT ref FROM timeline.search_hybrid_exact({q}, {DEPTH}, {sources}, {since}, NULL)",
    )
    legs = []
    for vec in vectors:
        legs.append(pdw_sql(
            "fusion lab: ANN leg",
            f"SELECT ref, best, fuse FROM timeline.search_hybrid_semantic({_lit(literal(vec))}, "
            f"{_lit(model)}, {DEPTH}, {sources}, {since}, NULL)",
        ))
    return {
        "collection_version": COLLECTION_VERSION,
        "query": case.query,
        "lexical": [r["ref"] for r in lexical],
        "exact": [r["ref"] for r in exact],
        "semantic": [[(r["ref"], int(r["best"]), float(r["fuse"])) for r in leg] for leg in legs],
    }


def collect(cases, *, workers: int, model: str, prefix: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if EVIDENCE.exists():
        out = json.loads(EVIDENCE.read_text())
    todo = [
        c for c in cases
        if not isinstance(out.get(case_key(c)), dict)
        or out[case_key(c)].get("collection_version") != COLLECTION_VERSION
    ]
    print(f"{len(todo)} queries to collect ({len(out)} cached)", flush=True)

    def run(case):
        try:
            return case_key(case), collect_case(case, model, prefix)
        except Exception as error:  # noqa: BLE001
            print(f"  ERR {case.query[:50]}: {str(error)[:120]}", flush=True)
            return case.query, None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for query, evidence in pool.map(run, todo):
            if evidence:
                out[query] = evidence
                print(f"  ok  {query[:60]}", flush=True)
                EVIDENCE.write_text(json.dumps(out))
    return out


def fuse(evidence: dict[str, Any], *, w_lex: float = 1.0, w_sem: float = 1.5, w_exact: float = 2.0,
         sem_pool: int | None = None,
         lex_top: tuple[int, float] | None = None) -> list[str]:
    """Replicates timeline.search_hybrid_fuse with adjustable weights.

    ``lex_top`` is conditionally applied only to a query that is not sentence
    shaped, matching the production function.
    """

    if is_sentence(evidence["query"]):
        lex_top = None
    score: dict[str, float] = {}
    for rank, ref in enumerate(evidence["lexical"], start=1):
        weight = lex_top[1] if lex_top and rank <= lex_top[0] else w_lex
        score[ref] = score.get(ref, 0.0) + weight / (RRF_K + rank)
    merged: dict[str, tuple[float, int]] = {}
    for leg in evidence["semantic"]:
        for ref, best, f in leg:
            if sem_pool is not None and best > sem_pool:
                continue
            fsum, bmin = merged.get(ref, (0.0, 10**9))
            merged[ref] = (fsum + f, min(bmin, best))
    ordered = sorted(merged.items(), key=lambda kv: (-kv[1][0], kv[1][1]))
    for rank, (ref, _) in enumerate(ordered, start=1):
        score[ref] = score.get(ref, 0.0) + w_sem / (RRF_K + rank)
    for rank, ref in enumerate(evidence["exact"], start=1):
        score[ref] = score.get(ref, 0.0) + w_exact / (RRF_K + rank)
    return [ref for ref, _ in sorted(score.items(), key=lambda kv: -kv[1])][:DEPTH]


REF_META = Path(".search-eval/fusion_ref_meta.json")


def load_ref_meta(refs: set[str]) -> dict[str, ResultRow]:
    cached: dict[str, list[str]] = json.loads(REF_META.read_text()) if REF_META.exists() else {}
    out: dict[str, ResultRow] = {
        r: ResultRow(ref=r, source=v[0], context=v[1], event_ts=v[2])
        for r, v in cached.items() if r in refs
    }
    refs = sorted(r for r in refs if r not in cached)
    for start in range(0, len(refs), 400):
        chunk = refs[start:start + 400]
        values = ",".join(
            "(" + _lit(r.split(":", 1)[0]) + "," + _lit(r.split(":", 1)[1]) + ")" for r in chunk
        )
        rows = pdw_sql(
            "fusion lab: resolve candidate refs for soft matching",
            f"WITH want(adapter, event_id) AS (VALUES {values}) "
            "SELECT e.adapter||':'||e.event_id AS ref, e.source, coalesce(e.context,'') AS context, "
            "e.event_ts FROM timeline.events e JOIN want w ON w.adapter=e.adapter AND w.event_id=e.event_id",
        )
        for r in rows:
            out[r["ref"]] = ResultRow(ref=r["ref"], source=r["source"], context=r["context"],
                                      event_ts=str(r["event_ts"]))
            cached[r["ref"]] = [r["source"], r["context"], str(r["event_ts"])]
    REF_META.write_text(json.dumps(cached))
    return out


def score(cases, evidence: dict[str, Any], grid: list[dict[str, Any]], contamination: ContaminationFilter):
    truth_meta = resolve_truth_metadata(sorted({r for c in cases for r in c.truth_refs}))
    cases = [
        c for c in cases
        if case_key(c) in evidence
        and (c.truth_predicate or any(r in truth_meta for r in c.truth_refs))
    ]
    candidates: set[str] = set()
    for c in cases:
        ev = evidence[case_key(c)]
        candidates.update(ev["lexical"]); candidates.update(ev["exact"])
        for leg in ev["semantic"]:
            candidates.update(ref for ref, _, _ in leg)
    meta = load_ref_meta(candidates)
    print(f"{len(cases)} scorable queries, {len(candidates)} candidate refs resolved {len(meta)}")
    results = []
    for cfg in grid:
        ranks_by_stratum: dict[str, list[int | None]] = {}
        for c in cases:
            refs = fuse(evidence[case_key(c)], **cfg)
            rows = [meta.get(r, ResultRow(ref=r)) for r in refs]
            rows, _ = contamination.apply(rows)
            tm = [truth_meta[r] for r in c.truth_refs if r in truth_meta]
            rank = first_relevant_rank(rows, set(c.truth_refs), tm, c.truth_predicate)
            ranks_by_stratum.setdefault(c.stratum, []).append(rank)
            ranks_by_stratum.setdefault("ALL", []).append(rank)
        summary = {}
        for stratum, ranks in ranks_by_stratum.items():
            found = [r for r in ranks if r]
            summary[stratum] = {
                "n": len(ranks), "hit1": sum(1 for r in found if r <= 1),
                "hit5": sum(1 for r in found if r <= 5), "hit10": sum(1 for r in found if r <= 10),
                "found": len(found), "mrr": round(sum(1.0 / r for r in found) / len(ranks), 3),
            }
        results.append({"config": cfg, "summary": summary})
        s = summary["ALL"]
        label = " ".join(f"{k}={v}" for k, v in cfg.items())
        per = "  ".join(f"{st[:4]}:{v['mrr']:.3f}/{v['hit1']}" for st, v in summary.items() if st != "ALL")
        print(f"{label:48} MRR={s['mrr']:.3f} hit@1={s['hit1']} hit@5={s['hit5']} hit@10={s['hit10']} found={s['found']}/{s['n']}   {per}")
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    c = sub.add_parser("collect")
    c.add_argument("--labels", type=Path, default=Path(".search-eval/ground_truth.json"))
    c.add_argument("--workers", type=int, default=4)
    c.add_argument("--model", default=os.environ.get("SEARCH_EMBEDDINGS_MODEL", "Qwen/Qwen3-Embedding-4B"))
    c.add_argument("--prefix", default=os.environ.get(
        "SEARCH_EMBEDDINGS_QUERY_PREFIX",
        "Instruct: Given a personal-archive search query, retrieve the messages, emails, documents and notes that answer it\\nQuery:",
    ))
    s = sub.add_parser("score")
    s.add_argument("--labels", type=Path, default=Path(".search-eval/ground_truth.json"))
    s.add_argument("--exclude-agent-sessions-since", default="")
    s.add_argument("--output", type=Path, default=Path(".search-eval/fusion_lab_report.json"))
    args = parser.parse_args()
    assert_private_path(EVIDENCE)
    cases = load_cases(args.labels)
    if args.command == "collect":
        collect(cases, workers=args.workers, model=args.model, prefix=args.prefix.replace("\\n", "\n"))
        return 0
    assert_private_path(args.output)
    evidence = json.loads(EVIDENCE.read_text())
    cutoff = parse_timestamp(args.exclude_agent_sessions_since) if args.exclude_agent_sessions_since else None
    contamination = ContaminationFilter(cutoff=_as_utc(cutoff) if cutoff else None)
    production = production_fusion_config()
    grid = [production]
    for w_lex in (1.0, 1.5, 2.0, 3.0):
        for w_sem in (0.5, 1.0, 1.5):
            for w_exact in (2.0, 3.0):
                cfg = {"w_lex": w_lex, "w_sem": w_sem, "w_exact": w_exact}
                if cfg not in grid:
                    grid.append(cfg)
    for pool in (100, 200, 300, 500, 750, 1000):
        grid.append({**production, "sem_pool": pool})
    # Sentence queries ignore this head bonus in production; term bags and
    # identifiers use it, so vary its width and strength directly.
    for cutoff in (3, 5, 10):
        for top_w in (2.0, 3.0):
            for w_sem in (0.75, 1.0):
                grid.append({"w_lex": 1.0, "w_sem": w_sem, "w_exact": 3.0,
                             "lex_top": (cutoff, top_w)})
    results = score(cases, evidence, grid, contamination)
    args.output.write_text(json.dumps({
        "captured_at": datetime.now(timezone.utc).isoformat(), "results": results,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
