"""Contract C8 measured weekly: latency and labeled quality as health rows."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.search_benchmark import BenchmarkCase, ResultRow
from personal_data_warehouse.search_benchmark_runner import (
    SearchBenchmarkRun,
    SearchBenchmarkRunner,
    label_rows_to_cases,
)
from tests.test_postgres_warehouse import warehouse  # noqa: F401 - fixture


class FakeClient:
    def __init__(self, hits: dict[str, list[str]], seconds: float = 0.4) -> None:
        self.hits = hits
        self.seconds = seconds
        self.calls: list[tuple[str, str, int]] = []

    def search(self, query, *, mode, max_results, sources=(), since=""):
        self.calls.append((query, mode, max_results))
        refs = self.hits.get(query, [])
        return [ResultRow(ref=ref, source="gmail") for ref in refs], self.seconds


def test_labels_round_trip_through_the_private_table(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_pipeline_health_tables()
    cases = [
        BenchmarkCase(query="offer letter start date", stratum="gmail", verdict="FOUND",
                      truth_refs=("gmail_email:m1",), truth_predicate=None, ambiguous=False, note="",
                      sources=("gmail",), since=""),
        BenchmarkCase(query="pizza night", stratum="slack", verdict="FOUND",
                      truth_refs=(), truth_predicate={"source": "slack"}, ambiguous=False, note="predicate case"),
    ]
    assert warehouse.publish_search_benchmark_labels(cases) == 2
    loaded = label_rows_to_cases(warehouse.load_search_benchmark_labels())
    assert {c.query for c in loaded} == {"offer letter start date", "pizza night"}
    assert next(c for c in loaded if c.query == "pizza night").truth_predicate == {"source": "slack"}
    # Publishing a smaller set retires the rest: the table mirrors the file.
    assert warehouse.publish_search_benchmark_labels(cases[:1]) == 1
    assert [c.query for c in label_rows_to_cases(warehouse.load_search_benchmark_labels())] == ["offer letter start date"]


def test_runner_measures_latency_and_mrr_and_writes_a_health_row(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_pipeline_health_tables()
    warehouse.publish_search_benchmark_labels([
        BenchmarkCase(query="offer letter start date", stratum="gmail", verdict="FOUND",
                      truth_refs=("gmail_email:m1",), truth_predicate=None, ambiguous=False, note=""),
        BenchmarkCase(query="never found", stratum="gmail", verdict="FOUND",
                      truth_refs=("gmail_email:zzz",), truth_predicate=None, ambiguous=False, note=""),
        BenchmarkCase(query="not here", stratum="gmail", verdict="NOT_IN_CORPUS",
                      truth_refs=("x",), truth_predicate=None, ambiguous=False, note=""),
    ])
    client = FakeClient({"offer letter start date": ["gmail_email:other", "gmail_email:m1"]}, seconds=0.25)
    result = SearchBenchmarkRunner(warehouse=warehouse, client=client, probe_queries=("a b c", "d e f")).run()

    assert result.probe_queries == 2
    assert result.latency_p50_ms == 250
    assert result.labeled_cases == 2  # NOT_IN_CORPUS is not scored
    assert result.found == 1
    assert result.hit_at_5 == 1 and result.hit_at_1 == 0
    assert result.mrr_milli == 250  # (1/2 + 0) / 2
    assert result.errors == 0

    row = warehouse._query_dicts("SELECT * FROM @marts_search_benchmark")[0]
    assert row["mode"] == "hybrid"
    assert row["status"] == "attention"  # MRR 0.25 is under the 0.30 floor
    assert float(row["mrr"]) == 0.25


def test_benchmark_view_judges_latency_and_staleness(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_pipeline_health_tables()
    now = datetime.now(tz=UTC)

    def run(**over) -> SearchBenchmarkRun:
        base = dict(mode="hybrid", probe_queries=6, latency_p50_ms=900, latency_p90_ms=1800, latency_max_ms=2500,
                    labeled_cases=40, found=30, hit_at_1=10, hit_at_5=20, hit_at_10=25, mrr_milli=410, errors=0, note="")
        base.update(over)
        return SearchBenchmarkRun(**base)

    warehouse.write_search_benchmark_runs([run()], collected_at=now)
    assert warehouse._query_dicts("SELECT status FROM @marts_search_benchmark")[0]["status"] == "ok"
    warehouse.write_search_benchmark_runs([run(latency_p50_ms=4200)], collected_at=now + timedelta(seconds=1))
    assert warehouse._query_dicts("SELECT status FROM @marts_search_benchmark")[0]["status"] == "attention"
    warehouse.write_search_benchmark_runs([run(labeled_cases=0, found=0, mrr_milli=0, note="no labels")], collected_at=now + timedelta(seconds=2))
    assert warehouse._query_dicts("SELECT status FROM @marts_search_benchmark")[0]["status"] == "ok"
    warehouse._command("UPDATE @search_benchmark_runs SET collected_at = %s", (now - timedelta(days=12),))
    assert warehouse._query_dicts("SELECT status FROM @marts_search_benchmark")[0]["status"] == "unknown"
