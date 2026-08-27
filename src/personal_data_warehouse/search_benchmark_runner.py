"""Weekly search benchmark: latency and labeled quality, as health rows.

Contract C8 says search quality is measured. It was, by hand, against labels in
a gitignored directory -- which is how the label set was lost once (2026-08)
and retrieval quality became unmeasurable until someone rebuilt it. This runner
keeps the labels in the warehouse (``private.search_benchmark_labels``, loaded
with ``search_benchmark publish-labels``), runs the benchmark through the app's
own ``search`` tool from Dagster every week, and writes the result to
``ops.search_benchmark_runs`` so ``marts_ops.search_benchmark`` and
``/pipelines`` show p50/p90 latency and MRR next to the other health rows.

Two measurements, deliberately separate:

* **Latency** over a fixed set of term-bag probe queries, serial, through the
  same HTTP path an agent uses. Comparable week to week; not comparable to the
  concurrent numbers the CLI harness reports.
* **Quality** over the labeled cases, scored with the harness's own ranking
  rules (``truth_refs`` and ``truth_predicate``; the timestamp soft-match needs
  a resolver and is skipped here, so this MRR is the stricter of the two).
"""

from __future__ import annotations

import json
import logging
import statistics
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, Sequence

import requests

from personal_data_warehouse.search_benchmark import (
    BenchmarkCase,
    ResultRow,
    first_relevant_rank,
)

logger = logging.getLogger(__name__)

#: Term-bag queries in the shape that retrieves well; common words on purpose,
#: because a rare term never shows the broad-pool cost.
DEFAULT_PROBE_QUERIES: tuple[str, ...] = (
    "runway burn rate months cash remaining",
    "trip planning flights hotel booking",
    "invoice payment received thanks",
    "meeting notes action items follow up",
    "offer letter start date salary",
    "shipping tracking order delivered",
)
DEFAULT_DEPTH = 50
LATENCY_DEPTH = 20
#: The goal set for the tool: p50 under two seconds, end to end.
LATENCY_P50_TARGET_MS = 2000
#: Below this the labeled set says retrieval regressed (the 2026-08 hybrid
#: landed at 0.403; the pre-hybrid keyword path scored 0.292).
MRR_FLOOR = 0.30


class SearchClient(Protocol):
    def search(self, query: str, *, mode: str, max_results: int,
               sources: Sequence[str] = (), since: str = "") -> tuple[list[ResultRow], float]: ...


class AppSearchClient:
    """The app's ``search`` tool over HTTP, the way pdw and the MCP reach it."""

    def __init__(self, *, base_url: str, secret_token: str, client_name: str = "search-benchmark",
                 timeout: float = 120.0, session: requests.Session | None = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._secret_token = secret_token
        self._client_name = client_name
        self._timeout = timeout
        self._session = session or requests.Session()

    def search(self, query: str, *, mode: str, max_results: int,
               sources: Sequence[str] = (), since: str = "") -> tuple[list[ResultRow], float]:
        payload: dict[str, Any] = {"query": query, "mode": mode, "max_results": max_results}
        if sources:
            payload["sources"] = list(sources)
        if since:
            payload["since"] = since
        started = time.monotonic()
        response = self._session.post(
            f"{self._base_url}/api/tools/search",
            json=payload,
            headers={
                "Authorization": f"Bearer {self._client_name}:{self._secret_token}",
                # Cloudflare 403s default urllib-ish agents in front of the app.
                "User-Agent": "personal-data-warehouse-search-benchmark/1",
            },
            timeout=self._timeout,
        )
        elapsed = time.monotonic() - started
        response.raise_for_status()
        data = (response.json() or {}).get("data") or {}
        if data.get("error"):
            raise RuntimeError(str(data["error"]))
        rows = []
        for item in data.get("rows") or []:
            if isinstance(item, dict):
                rows.append(ResultRow(
                    ref=str(item.get("ref") or ""), source=str(item.get("source") or ""),
                    context=str(item.get("context") or ""),
                    event_ts=str(item.get("event_ts") or item.get("occurred_at") or ""),
                    title=str(item.get("title") or ""), text=str(item.get("text") or ""),
                ))
        return rows, elapsed


@dataclass(frozen=True)
class SearchBenchmarkRun:
    """One benchmark result per mode, as written to ops.search_benchmark_runs."""

    mode: str
    probe_queries: int
    latency_p50_ms: int
    latency_p90_ms: int
    latency_max_ms: int
    labeled_cases: int
    found: int
    hit_at_1: int
    hit_at_5: int
    hit_at_10: int
    mrr_milli: int
    errors: int
    note: str


def _percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((pct / 100) * (len(ordered) - 1))))
    return ordered[index]


def label_rows_to_cases(rows: Sequence[dict[str, Any]]) -> list[BenchmarkCase]:
    cases: list[BenchmarkCase] = []
    for row in rows:
        refs = tuple(json.loads(row.get("truth_refs_json") or "[]"))
        predicate = json.loads(row.get("truth_predicate_json") or "null")
        if not refs and not predicate:
            continue
        cases.append(BenchmarkCase(
            query=str(row["query"]), stratum=str(row.get("stratum") or "unclassified"),
            verdict=str(row.get("verdict") or "FOUND"), truth_refs=refs, truth_predicate=predicate,
            ambiguous=False, note=str(row.get("note") or ""),
            sources=tuple(json.loads(row.get("sources_json") or "[]")), since=str(row.get("since") or ""),
        ))
    return cases


class SearchBenchmarkRunner:
    def __init__(self, *, warehouse: Any, client: SearchClient, mode: str = "hybrid",
                 probe_queries: Sequence[str] = DEFAULT_PROBE_QUERIES, depth: int = DEFAULT_DEPTH,
                 logger_: Any = None) -> None:
        self._warehouse = warehouse
        self._client = client
        self._mode = mode
        self._probes = tuple(probe_queries)
        self._depth = depth
        self._log = logger_ or logger

    def measure(self) -> SearchBenchmarkRun:
        timings: list[float] = []
        errors = 0
        for query in self._probes:
            try:
                _, elapsed = self._client.search(query, mode=self._mode, max_results=LATENCY_DEPTH)
                timings.append(elapsed)
            except Exception as error:  # noqa: BLE001 - one failed probe is a count, not a crash
                errors += 1
                self._log.warning("benchmark probe failed for %r: %s", query, error)

        cases = label_rows_to_cases(self._warehouse.load_search_benchmark_labels())
        ranks: list[int | None] = []
        for case in cases:
            if case.verdict == "NOT_IN_CORPUS":
                continue
            try:
                rows, _ = self._client.search(case.query, mode=self._mode, max_results=self._depth,
                                              sources=case.sources, since=case.since)
            except Exception as error:  # noqa: BLE001
                errors += 1
                self._log.warning("benchmark case failed for %r: %s", case.query, error)
                ranks.append(None)
                continue
            ranks.append(first_relevant_rank(rows, set(case.truth_refs), (), case.truth_predicate))
        found = [r for r in ranks if r]
        mrr = (sum(1.0 / r for r in found) / len(ranks)) if ranks else 0.0
        note = "" if cases else "no labels in private.search_benchmark_labels; run `search_benchmark publish-labels`"
        return SearchBenchmarkRun(
            mode=self._mode, probe_queries=len(timings),
            latency_p50_ms=int(_percentile(timings, 50) * 1000), latency_p90_ms=int(_percentile(timings, 90) * 1000),
            latency_max_ms=int(max(timings) * 1000) if timings else 0,
            labeled_cases=len(ranks), found=len(found),
            hit_at_1=sum(1 for r in found if r <= 1), hit_at_5=sum(1 for r in found if r <= 5),
            hit_at_10=sum(1 for r in found if r <= 10), mrr_milli=int(round(mrr * 1000)),
            errors=errors, note=note,
        )

    def run(self) -> SearchBenchmarkRun:
        result = self.measure()
        self._warehouse.write_search_benchmark_runs([result], collected_at=datetime.now(tz=UTC))
        return result
