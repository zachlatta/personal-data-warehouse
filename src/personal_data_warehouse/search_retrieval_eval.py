"""Replay evaluation for timeline keyword and hybrid retrieval.

The corpus is deliberately generated locally from real agent search calls.  It
contains Zach's private queries and timeline references, so callers must write
it below the gitignored ``.search-eval/`` directory.  Only this reproducible
harness belongs in source control.

Historical returned refs are useful *regression* judgments: a replacement
should continue finding results agents previously received.  They are not a
claim that every relevant result was labeled.  The JSONL format therefore
records ``relevance_provenance`` and accepts human-curated ``relevant_refs``
or ``relevant_keys`` without changing the evaluator.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
from typing import Any, Iterable, Iterator, Sequence


SEARCH_CALL_RE = re.compile(
    r"timeline\.search_text(?P<exact>_exact)?\s*\(\s*'(?P<query>(?:''|[^'])*)'",
    re.IGNORECASE,
)
SOURCES_RE = re.compile(
    r"sources\s*=>\s*ARRAY\s*\[(?P<values>[^]]*)\]", re.IGNORECASE
)
SINCE_RE = re.compile(r"since\s*=>\s*'(?P<value>[^']+)'", re.IGNORECASE)
SQL_STRING_RE = re.compile(r"'((?:''|[^'])*)'")


@dataclass(frozen=True)
class EvalCase:
    query_id: str
    query: str
    mode: str
    sources: tuple[str, ...]
    since: str
    relevant_refs: tuple[str, ...]
    relevant_keys: tuple[str, ...]
    relevance_provenance: str
    source: str
    session_id: str
    seq: int
    occurred_at: str


@dataclass(frozen=True)
class SearchResult:
    mode: str
    refs: tuple[str, ...] = ()
    keys: tuple[str, ...] = ()
    fallback_reason: str = ""
    error: str = ""


def _walk_strings(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
        stripped = value.strip()
        if stripped.startswith(("{", "[")):
            try:
                decoded = json.loads(stripped)
            except (TypeError, ValueError):
                return
            yield from _walk_strings(decoded)
    elif isinstance(value, dict):
        for child in value.values():
            yield from _walk_strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_strings(child)


def _walk_dicts(value: Any) -> Iterator[dict[str, Any]]:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(("{", "[")):
            try:
                yield from _walk_dicts(json.loads(stripped))
            except (TypeError, ValueError):
                pass
    elif isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _json_or_text(raw: str) -> Any:
    try:
        return json.loads(raw or "")
    except (TypeError, ValueError):
        return raw or ""


def _row_key(item: dict[str, Any]) -> str:
    ref = item.get("ref")
    if isinstance(ref, str) and ref:
        return canonical_ref(ref)
    source = item.get("source")
    occurred_at = item.get("occurred_at") or item.get("event_ts")
    if isinstance(source, str) and source and isinstance(occurred_at, str) and occurred_at:
        return f"event:{source}|{occurred_at}"
    return ""


def _historical_judgments(
    raw_result: str, *, limit: int = 10
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    refs: list[str] = []
    keys: list[str] = []
    seen_refs: set[str] = set()
    seen_keys: set[str] = set()
    for item in _walk_dicts(_json_or_text(raw_result)):
        ref = item.get("ref")
        if isinstance(ref, str) and ref and ref not in seen_refs:
            refs.append(ref)
            seen_refs.add(ref)
        key = _row_key(item)
        if key and key not in seen_keys:
            keys.append(key)
            seen_keys.add(key)
            if len(keys) >= limit:
                break
    return tuple(refs), tuple(keys)


def _search_texts(raw_input: str) -> list[str]:
    decoded = _json_or_text(raw_input)
    texts = list(_walk_strings(decoded))
    if isinstance(decoded, str) and decoded not in texts:
        texts.append(decoded)
    # A command is commonly present both as a dictionary value and as a JSON
    # string discovered recursively.  Preserve order but avoid duplicate cases.
    return list(dict.fromkeys(texts))


def _scope(sql: str) -> tuple[tuple[str, ...], str]:
    sources: tuple[str, ...] = ()
    match = SOURCES_RE.search(sql)
    if match:
        sources = tuple(
            literal.replace("''", "'")
            for literal in SQL_STRING_RE.findall(match.group("values"))
        )
    since_match = SINCE_RE.search(sql)
    return sources, since_match.group("value") if since_match else ""


def extract_cases(records: Iterable[dict[str, Any]]) -> list[EvalCase]:
    """Extract literal timeline search calls and their historical top refs."""

    cases: list[EvalCase] = []
    seen_ids: set[str] = set()
    for record in records:
        occurrences: list[tuple[str, re.Match[str], str]] = []
        for text in _search_texts(str(record.get("tool_input_json") or "")):
            matches = list(SEARCH_CALL_RE.finditer(text))
            for index, match in enumerate(matches):
                query = match.group("query").replace("''", "'").strip()
                if not query or query.startswith("$"):
                    continue
                end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
                occurrences.append((text[match.start() : end], match, query))
        # A result envelope from a command containing several searches cannot
        # be assigned to one query without inventing relevance judgments.
        refs, keys = (
            _historical_judgments(str(record.get("tool_result_json") or ""))
            if len(occurrences) == 1
            else ((), ())
        )
        match_index = 0
        for call_text, match, query in occurrences:
            sources, since = _scope(call_text)
            identity = (
                f"{record.get('source','')}|{record.get('session_id','')}|"
                f"{record.get('seq',0)}|{match_index}|{query}"
            )
            query_id = hashlib.sha256(identity.encode()).hexdigest()[:20]
            match_index += 1
            if query_id in seen_ids:
                continue
            seen_ids.add(query_id)
            cases.append(
                EvalCase(
                    query_id=query_id,
                    query=query,
                    mode="exact" if match.group("exact") else "keyword",
                    sources=sources,
                    since=since,
                    relevant_refs=refs,
                    relevant_keys=keys,
                    relevance_provenance=(
                        "historical_search_results" if keys else "none"
                    ),
                    source=str(record.get("source") or ""),
                    session_id=str(record.get("session_id") or ""),
                    seq=int(record.get("seq") or 0),
                    occurred_at=str(record.get("occurred_at") or ""),
                )
            )
    return cases


def parse_search_response(raw: str) -> SearchResult:
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as error:
        return SearchResult(mode="", error=f"invalid JSON response: {error}")
    rows = [row for row in payload.get("rows", []) if isinstance(row, dict)]
    refs = tuple(str(row["ref"]) for row in rows if row.get("ref"))
    keys = tuple(key for row in rows if (key := _row_key(row)))
    return SearchResult(
        mode=str(payload.get("mode") or ""),
        refs=refs,
        keys=keys,
        fallback_reason=str(payload.get("fallback_reason") or ""),
    )


def canonical_ref(ref: str) -> str:
    """Normalize refs whose retrieval granularity changed without identity changing."""

    if ref.startswith("agent_session_turn:"):
        payload = ref.removeprefix("agent_session_turn:")
        provider, separator, remainder = payload.partition("|")
        session_id, separator2, _seq = remainder.rpartition("|")
        if separator and separator2 and provider and session_id:
            return f"agent_session:{provider}|{session_id}"
    return ref


def _recall(relevant: Sequence[str], returned: Sequence[str], k: int) -> float | None:
    if not relevant:
        return None
    relevant_set = {canonical_ref(ref) for ref in relevant}
    returned_set = {canonical_ref(ref) for ref in returned[:k]}
    return len(relevant_set & returned_set) / len(relevant_set)


def _mrr(relevant: Sequence[str], returned: Sequence[str], k: int) -> float | None:
    if not relevant:
        return None
    relevant_set = {canonical_ref(ref) for ref in relevant}
    for rank, ref in enumerate(returned[:k], start=1):
        if canonical_ref(ref) in relevant_set:
            return 1.0 / rank
    return 0.0


def _hit(relevant: Sequence[str], returned: Sequence[str], k: int) -> float | None:
    """Return whether at least one relevant result appears in the first k."""

    if not relevant:
        return None
    relevant_set = {canonical_ref(ref) for ref in relevant}
    return float(any(canonical_ref(ref) in relevant_set for ref in returned[:k]))


def evaluate_case(
    case: EvalCase,
    *,
    keyword: SearchResult,
    hybrid: SearchResult | None,
    k: int,
) -> dict[str, Any]:
    relevant = case.relevant_keys or tuple(canonical_ref(ref) for ref in case.relevant_refs)
    hybrid_refs = hybrid.refs if hybrid else ()
    keyword_set = set(keyword.keys[:k])
    return {
        "query_id": case.query_id,
        "query": case.query,
        "original_mode": case.mode,
        "relevance_provenance": case.relevance_provenance,
        "relevant_refs": list(case.relevant_refs),
        "relevant_keys": list(relevant),
        "keyword_refs": list(keyword.refs),
        "hybrid_refs": list(hybrid_refs),
        "keyword_recall_at_k": _recall(relevant, keyword.keys, k),
        "hybrid_recall_at_k": (
            _recall(relevant, hybrid.keys, k) if hybrid else None
        ),
        "keyword_mrr": _mrr(relevant, keyword.keys, k),
        "hybrid_mrr": _mrr(relevant, hybrid.keys, k) if hybrid else None,
        "keyword_hit_at_1": _hit(relevant, keyword.keys, 1),
        "hybrid_hit_at_1": _hit(relevant, hybrid.keys, 1) if hybrid else None,
        "keyword_hit_at_5": _hit(relevant, keyword.keys, min(5, k)),
        "hybrid_hit_at_5": (
            _hit(relevant, hybrid.keys, min(5, k)) if hybrid else None
        ),
        "hybrid_novel_refs": [
            ref
            for index, ref in enumerate(hybrid_refs[:k])
            if index >= len(hybrid.keys) or hybrid.keys[index] not in keyword_set
        ],
        "keyword_error": keyword.error,
        "hybrid_error": hybrid.error if hybrid else "",
        "hybrid_fallback_reason": hybrid.fallback_reason if hybrid else "",
    }


def _pdw_json(args: list[str], *, timeout: float) -> Any:
    completed = subprocess.run(
        ["pdw", *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return json.loads(completed.stdout)


def mine_records(*, since: str, until: str) -> list[dict[str, Any]]:
    sql = f"""
WITH calls AS (
  SELECT source, session_id, seq, occurred_at, tool_input_json, tool_result_json
  FROM marts_ai_conversations.events
  WHERE occurred_at >= '{since}'::timestamptz
    AND occurred_at < '{until}'::timestamptz
    AND tool_input_json LIKE '%timeline.search_text%'
    AND (
      (tool_name IN ('Bash', 'bash', 'exec_command')
       AND tool_input_json LIKE '%pdw sql%')
      OR tool_name IN (
        '_query', 'Personal Data Warehouse:query', 'api_tool.call_tool'
      )
      OR tool_name ILIKE '%Personal_Data_Warehouse%query%'
      OR tool_name ILIKE 'codex_apps.personal data warehouse_query%'
    )
)
SELECT c.source, c.session_id, c.seq, c.occurred_at, c.tool_input_json,
       left(COALESCE(NULLIF(c.tool_result_json, ''), result.tool_result_json, ''), 50000)
         AS tool_result_json
FROM calls c
LEFT JOIN LATERAL (
  SELECT e.tool_result_json
  FROM marts_ai_conversations.events e
  WHERE e.source = c.source AND e.session_id = c.session_id
    AND e.seq > c.seq AND e.tool_result_json <> ''
  ORDER BY e.seq
  LIMIT 1
) result ON true
ORDER BY c.occurred_at, c.source, c.session_id, c.seq
LIMIT 2000
"""
    return _pdw_json(
        [
            "sql",
            "--output",
            "json",
            "-q",
            "mine bounded real timeline searches for the private retrieval replay evaluation",
            sql,
        ],
        timeout=30,
    )


def run_search(case: EvalCase, *, mode: str, max_results: int) -> SearchResult:
    request: dict[str, Any] = {
        "query": case.query,
        "mode": mode,
        "max_results": max_results,
    }
    if case.sources:
        request["sources"] = list(case.sources)
    if case.since:
        request["since"] = case.since
    try:
        payload = _pdw_json(
            ["call", "search", "--data", json.dumps(request)], timeout=90
        )
        return parse_search_response(json.dumps(payload))
    except (subprocess.SubprocessError, OSError, ValueError) as error:
        return SearchResult(mode=mode, error=str(error))


def _write_cases(path: Path, cases: Iterable[EvalCase]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for case in cases:
            row = asdict(case)
            row["sources"] = list(case.sources)
            row["relevant_refs"] = list(case.relevant_refs)
            row["relevant_keys"] = list(case.relevant_keys)
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _read_cases(path: Path) -> list[EvalCase]:
    cases = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            row["sources"] = tuple(row.get("sources", ()))
            row["relevant_refs"] = tuple(row.get("relevant_refs", ()))
            row["relevant_keys"] = tuple(row.get("relevant_keys", ()))
            cases.append(EvalCase(**row))
    return cases


def _mean(rows: Sequence[dict[str, Any]], field: str) -> float | None:
    values = [float(row[field]) for row in rows if row.get(field) is not None]
    return sum(values) / len(values) if values else None


def summarize(rows: Sequence[dict[str, Any]], *, k: int) -> dict[str, Any]:
    judged = [row for row in rows if row.get("relevant_keys")]
    return {
        "queries": len(rows),
        "judged_queries": len(judged),
        "k": k,
        "keyword_recall_at_k": _mean(judged, "keyword_recall_at_k"),
        "hybrid_recall_at_k": _mean(judged, "hybrid_recall_at_k"),
        "keyword_mrr": _mean(judged, "keyword_mrr"),
        "hybrid_mrr": _mean(judged, "hybrid_mrr"),
        "keyword_hit_at_1": _mean(judged, "keyword_hit_at_1"),
        "hybrid_hit_at_1": _mean(judged, "hybrid_hit_at_1"),
        "keyword_hit_at_5": _mean(judged, "keyword_hit_at_5"),
        "hybrid_hit_at_5": _mean(judged, "hybrid_hit_at_5"),
        "hybrid_fallbacks": sum(bool(row.get("hybrid_fallback_reason")) for row in rows),
        "keyword_errors": sum(bool(row.get("keyword_error")) for row in rows),
        "hybrid_errors": sum(bool(row.get("hybrid_error")) for row in rows),
        "note": (
            "historical_search_results are replay/regression judgments, not exhaustive "
            "human relevance labels"
        ),
    }


def _default_window() -> tuple[str, str]:
    until = date.today() + timedelta(days=1)
    return (until - timedelta(days=30)).isoformat(), until.isoformat()


def main(argv: Sequence[str] | None = None) -> int:
    default_since, default_until = _default_window()
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    mine = subparsers.add_parser("mine", help="mine private replay cases through pdw")
    mine.add_argument("--since", default=default_since)
    mine.add_argument("--until", default=default_until)
    mine.add_argument("--output", type=Path, default=Path(".search-eval/queries.jsonl"))

    run = subparsers.add_parser("run", help="run keyword/hybrid replay evaluation")
    run.add_argument("--input", type=Path, default=Path(".search-eval/queries.jsonl"))
    run.add_argument("--output", type=Path, default=Path(".search-eval/report.json"))
    run.add_argument("--limit", type=int, default=0)
    run.add_argument("--max-results", type=int, default=10)
    run.add_argument(
        "--judged-only",
        action="store_true",
        help="skip cases without historical or human relevance judgments",
    )

    args = parser.parse_args(argv)
    if args.command == "mine":
        cases = extract_cases(mine_records(since=args.since, until=args.until))
        _write_cases(args.output, cases)
        print(json.dumps({"output": str(args.output), "queries": len(cases)}))
        return 0

    cases = _read_cases(args.input)
    if args.judged_only:
        cases = [case for case in cases if case.relevant_keys]
    if args.limit:
        cases = cases[: args.limit]
    rows = []
    for index, case in enumerate(cases, start=1):
        keyword_mode = "exact" if case.mode == "exact" else "keyword"
        keyword = run_search(case, mode=keyword_mode, max_results=args.max_results)
        hybrid = (
            run_search(case, mode="hybrid", max_results=args.max_results)
            if case.mode != "exact"
            else None
        )
        rows.append(
            evaluate_case(
                case, keyword=keyword, hybrid=hybrid, k=args.max_results
            )
        )
        print(f"[{index}/{len(cases)}] {case.query_id}", flush=True)
    report = {
        "summary": summarize(rows, k=args.max_results),
        "results": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
