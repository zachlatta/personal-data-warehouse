"""Contract C8 measured weekly: latency and labeled quality as health rows."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.search_benchmark import BenchmarkCase, ResultRow
from personal_data_warehouse.search_benchmark_runner import (
    AppSearchClient,
    SearchBenchmarkRun,
    SearchBenchmarkRunner,
    label_rows_to_cases,
)
from tests.test_postgres_warehouse import warehouse  # noqa: F401 - fixture


class FakeClient:
    def __init__(self, hits: dict, seconds: float = 0.4) -> None:
        self.hits = hits
        self.seconds = seconds
        self.calls: list[tuple[str, str, int, tuple[str, ...]]] = []

    def search(self, query, *, mode, max_results, sources=(), since="", priorities=()):
        scope = tuple(priorities)
        self.calls.append((query, mode, max_results, scope))
        refs = self.hits.get((query, scope), self.hits.get(query, []))
        elapsed = self.seconds * (0.5 if scope else 1.0)
        return [ResultRow(ref=ref, source="gmail", priority="self") for ref in refs], elapsed


def test_app_search_client_sends_scope_and_retains_hit_priority() -> None:
    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "data": {
                    "priority_scope": "selected",
                    "selected_priorities": ["self", "direct", "cc"],
                    "rows": [
                        {
                            "ref": "agent_turn:bg",
                            "source": "agent",
                            "priority": "background",
                        }
                    ]
                }
            }

    class Session:
        def __init__(self) -> None:
            self.payload = None

        def post(self, _url, *, json, headers, timeout):
            self.payload = json
            return Response()

    session = Session()
    client = AppSearchClient(
        base_url="https://warehouse.example",
        secret_token="secret",
        session=session,
    )
    rows, elapsed = client.search(
        "prior conclusion",
        mode="hybrid",
        max_results=20,
        priorities=("self", "direct", "cc"),
    )

    assert session.payload["priorities"] == ["self", "direct", "cc"]
    assert rows[0].priority == "background"
    assert elapsed >= 0


def test_app_search_client_rejects_a_scope_mismatch() -> None:
    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"data": {"priority_scope": "all", "selected_priorities": [], "rows": []}}

    class Session:
        def post(self, _url, **_kwargs):
            return Response()

    client = AppSearchClient(
        base_url="https://warehouse.example",
        secret_token="secret",
        session=Session(),
    )
    try:
        client.search(
            "prior conclusion",
            mode="hybrid",
            max_results=20,
            priorities=("self", "direct", "cc"),
        )
    except RuntimeError as error:
        assert "scope" in str(error).lower()
    else:
        raise AssertionError("scope mismatch was silently benchmarked")


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
    assert result.attention_probe_queries == 2
    assert result.attention_latency_p50_ms == 125
    assert result.attention_labeled_cases == 2
    assert result.attention_comparable_cases == 2
    assert result.attention_found == 1
    assert result.attention_recall_lost == 0

    row = warehouse._query_dicts("SELECT * FROM @marts_search_benchmark")[0]
    assert row["mode"] == "hybrid"
    assert row["status"] == "attention"  # MRR 0.25 is under the 0.30 floor
    assert float(row["mrr"]) == 0.25
    assert float(row["attention_mrr"]) == 0.25
    assert int(row["attention_latency_p50_delta_ms"]) == -125


def test_runner_pairs_all_and_attention_and_measures_recall_loss(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_pipeline_health_tables()
    warehouse.publish_search_benchmark_labels([
        BenchmarkCase(query="background answer", stratum="agent", verdict="FOUND",
                      truth_refs=("agent_turn:bg",), truth_predicate=None, ambiguous=False, note=""),
        BenchmarkCase(query="direct answer", stratum="gmail", verdict="FOUND",
                      truth_refs=("gmail_email:self",), truth_predicate=None, ambiguous=False, note=""),
    ])
    attention = ("self", "direct", "cc")
    client = FakeClient({
        ("background answer", ()): ["agent_turn:bg"],
        ("background answer", attention): [],
        ("direct answer", ()): ["gmail_email:self"],
        ("direct answer", attention): ["gmail_email:self"],
    }, seconds=0.4)
    result = SearchBenchmarkRunner(
        warehouse=warehouse, client=client, probe_queries=("probe",)
    ).run()

    assert result.found == 2 and result.attention_found == 1
    assert result.attention_recall_lost == 1
    assert result.attention_recall_retained == 1
    assert result.attention_recall_gained == 0
    assert {scope for _, _, _, scope in client.calls} == {(), attention}
    row = warehouse._query_dicts("SELECT * FROM @marts_search_benchmark")[0]
    assert float(row["attention_recall_loss_rate"]) == 0.5


def test_benchmark_view_judges_latency_and_staleness(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_pipeline_health_tables()
    now = datetime.now(tz=UTC)

    def run(**over) -> SearchBenchmarkRun:
        base = dict(mode="hybrid", probe_queries=6, latency_p50_ms=900, latency_p90_ms=1800, latency_max_ms=2500,
                    labeled_cases=40, found=30, hit_at_1=10, hit_at_5=20, hit_at_10=25, mrr_milli=410, errors=0, note="")
        base.update(over)
        return SearchBenchmarkRun(**base)

    warehouse.write_search_benchmark_runs([run()], collected_at=now)
    first = warehouse._query_dicts(
        "SELECT status, attention_latency_p50_delta_ms FROM @marts_search_benchmark"
    )[0]
    assert first["status"] == "ok"
    assert first["attention_latency_p50_delta_ms"] is None
    warehouse.write_search_benchmark_runs([run(latency_p50_ms=4200)], collected_at=now + timedelta(seconds=1))
    assert warehouse._query_dicts("SELECT status FROM @marts_search_benchmark")[0]["status"] == "attention"
    warehouse.write_search_benchmark_runs([run(labeled_cases=0, found=0, mrr_milli=0, note="no labels")], collected_at=now + timedelta(seconds=2))
    assert warehouse._query_dicts("SELECT status FROM @marts_search_benchmark")[0]["status"] == "ok"
    warehouse._command("UPDATE @search_benchmark_runs SET collected_at = %s", (now - timedelta(days=12),))
    assert warehouse._query_dicts("SELECT status FROM @marts_search_benchmark")[0]["status"] == "unknown"


def test_runner_says_when_every_latency_probe_failed(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_pipeline_health_tables()

    class FailingClient:
        def search(self, query, *, mode, max_results, sources=(), since="", priorities=()):
            raise RuntimeError("statement timeout")

    result = SearchBenchmarkRunner(warehouse=warehouse, client=FailingClient(), probe_queries=("a", "b")).run()
    assert result.probe_queries == 0 and result.errors == 2
    assert "latency probes failed" in result.note
    row = warehouse._query_dicts("SELECT status, note FROM @marts_search_benchmark")[0]
    assert row["status"] == "no_data"
    assert "unmeasured" in row["note"]


def test_host_saturation_parses_psi_loadavg_and_cpu_count(tmp_path) -> None:
    from personal_data_warehouse.search_benchmark_runner import sample_host_saturation

    (tmp_path / "pressure").mkdir()
    (tmp_path / "pressure" / "io").write_text(
        "some avg10=31.42 avg60=25.10 avg300=18.00 total=123456\n"
        "full avg10=20.15 avg60=15.02 avg300=9.87 total=98765\n"
    )
    (tmp_path / "pressure" / "cpu").write_text("some avg10=4.50 avg60=3.00 avg300=2.00 total=555\n")
    (tmp_path / "loadavg").write_text("19.70 15.20 12.00 3/1200 4242\n")
    sample = sample_host_saturation(tmp_path, cpu_count=28)
    assert sample.io_pressure_full_avg10 == 20.15
    assert sample.cpu_pressure_some_avg10 == 4.5
    assert sample.load_1m == 19.7
    assert sample.cpu_count == 28
    assert sample.note == ""


def test_host_saturation_reports_missing_procfs_as_minus_one_with_a_note(tmp_path) -> None:
    from personal_data_warehouse.search_benchmark_runner import sample_host_saturation

    # A kernel without PSI has /proc/loadavg but no /proc/pressure; a Mac has
    # neither. Each field degrades on its own and the note names the file.
    (tmp_path / "loadavg").write_text("1.25 1.00 0.90 1/100 1\n")
    sample = sample_host_saturation(tmp_path, cpu_count=8)
    assert sample.io_pressure_full_avg10 == -1 and sample.cpu_pressure_some_avg10 == -1
    assert sample.load_1m == 1.25 and sample.cpu_count == 8
    assert "pressure/io" in sample.note and "pressure/cpu" in sample.note and "loadavg" not in sample.note
    # A file present but in an unexpected shape is unreadable too, never a 0.
    (tmp_path / "pressure").mkdir()
    (tmp_path / "pressure" / "io").write_text("garbage\n")
    assert sample_host_saturation(tmp_path, cpu_count=8).io_pressure_full_avg10 == -1
    assert sample_host_saturation(tmp_path / "nope", cpu_count=0).cpu_count == -1


def test_runner_stores_the_worse_of_the_two_host_samples(warehouse: PostgresWarehouse) -> None:
    from personal_data_warehouse.search_benchmark_runner import HostSaturation

    warehouse.ensure_pipeline_health_tables()
    samples = iter([
        HostSaturation(io_pressure_full_avg10=0.2, cpu_pressure_some_avg10=60.0, load_1m=3.0, cpu_count=28),
        HostSaturation(io_pressure_full_avg10=20.15, cpu_pressure_some_avg10=2.0, load_1m=19.7, cpu_count=28),
    ])
    result = SearchBenchmarkRunner(warehouse=warehouse, client=FakeClient({}, seconds=3.2),
                                   probe_queries=("a b", "c d"), sample_host=lambda: next(samples)).run()
    assert (result.io_pressure_full_avg10, result.cpu_pressure_some_avg10, result.load_1m, result.cpu_count) == (20.15, 60.0, 19.7, 28)
    row = warehouse._query_dicts("SELECT saturation, io_pressure_full_avg10, load_1m FROM @marts_search_benchmark")[0]
    assert row["saturation"] == "io_bound"
    assert float(row["io_pressure_full_avg10"]) == 20.15


def test_runner_notes_an_unmeasured_host(warehouse: PostgresWarehouse) -> None:
    from personal_data_warehouse.search_benchmark_runner import sample_host_saturation

    warehouse.ensure_pipeline_health_tables()
    result = SearchBenchmarkRunner(warehouse=warehouse, client=FakeClient({}), probe_queries=("a b",),
                                   sample_host=lambda: sample_host_saturation("/definitely/not/proc")).run()
    assert result.io_pressure_full_avg10 == -1 and result.load_1m == -1
    assert "stored -1" in result.note
    assert warehouse._query_dicts("SELECT saturation FROM @marts_search_benchmark")[0]["saturation"] == "unmeasured"
