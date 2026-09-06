from __future__ import annotations

import json
import subprocess
from collections.abc import Callable

import pytest

from personal_data_warehouse import search_benchmark
from personal_data_warehouse.search_benchmark import SearchResult
from scripts import contract_audit


def _search_payload(**overrides: object) -> str:
    payload: dict[str, object] = {
        "mode": "hybrid",
        "rows": [],
        "fallback_reason": "",
        "error": "",
        "priority_scope": "all",
        "selected_priorities": [],
        "returned_priority_counts": {},
    }
    payload.update(overrides)
    return json.dumps(payload)


@pytest.mark.parametrize(
    ("completed", "priorities"),
    [
        (
            subprocess.CompletedProcess(
                ["pdw"], 1, stdout="", stderr="authentication failed"
            ),
            (),
        ),
        (subprocess.CompletedProcess(["pdw"], 0, stdout="not json", stderr=""), ()),
        (
            subprocess.CompletedProcess(
                ["pdw"],
                0,
                stdout=_search_payload(error="embedded search failure"),
                stderr="",
            ),
            (),
        ),
        (
            subprocess.CompletedProcess(
                ["pdw"],
                0,
                stdout=_search_payload(mode="keyword"),
                stderr="",
            ),
            (),
        ),
        (
            subprocess.CompletedProcess(
                ["pdw"],
                0,
                stdout=_search_payload(fallback_reason="embeddings unavailable"),
                stderr="",
            ),
            (),
        ),
        (
            subprocess.CompletedProcess(
                ["pdw"],
                0,
                stdout=_search_payload(priority_scope=""),
                stderr="",
            ),
            (),
        ),
        (
            subprocess.CompletedProcess(
                ["pdw"],
                0,
                stdout=_search_payload(
                    priority_scope="selected", selected_priorities=["self"]
                ),
                stderr="",
            ),
            tuple(search_benchmark.ATTENTION_PRIORITIES),
        ),
    ],
    ids=[
        "nonzero-exit",
        "malformed-json",
        "payload-error",
        "wrong-effective-mode",
        "fallback",
        "missing-scope-echo",
        "incorrect-scope-echo",
    ],
)
def test_validated_search_probe_rejects_fast_invalid_responses(
    monkeypatch: pytest.MonkeyPatch,
    completed: subprocess.CompletedProcess[str],
    priorities: tuple[str, ...],
) -> None:
    monkeypatch.setattr(search_benchmark.subprocess, "run", lambda *args, **kwargs: completed)

    elapsed, issue = contract_audit._validated_search_probe(
        "bounded test query", priorities=priorities
    )

    assert elapsed is None
    assert issue


def test_validated_search_probe_rejects_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def time_out(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd=["pdw", "search"], timeout=90)

    monkeypatch.setattr(search_benchmark.subprocess, "run", time_out)

    elapsed, issue = contract_audit._validated_search_probe(
        "bounded test query", priorities=()
    )

    assert elapsed is None
    assert issue


def test_validated_search_probe_accepts_correctly_scoped_empty_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completed = subprocess.CompletedProcess(
        ["pdw"], 0, stdout=_search_payload(), stderr=""
    )
    monkeypatch.setattr(search_benchmark.subprocess, "run", lambda *args, **kwargs: completed)

    elapsed, issue = contract_audit._validated_search_probe(
        "bounded test query", priorities=()
    )

    assert elapsed is not None
    assert elapsed >= 0
    assert issue == ""


def test_validated_search_probe_accepts_correctly_attention_scoped_empty_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attention = tuple(search_benchmark.ATTENTION_PRIORITIES)
    completed = subprocess.CompletedProcess(
        ["pdw"],
        0,
        stdout=_search_payload(
            priority_scope="selected", selected_priorities=list(attention)
        ),
        stderr="",
    )
    monkeypatch.setattr(search_benchmark.subprocess, "run", lambda *args, **kwargs: completed)

    elapsed, issue = contract_audit._validated_search_probe(
        "bounded test query", priorities=attention
    )

    assert elapsed is not None
    assert issue == ""


def _benchmark_host_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "saturation": "io_bound",
        "io_pressure_full_avg10": 11.4,
        "cpu_pressure_some_avg10": 0.04,
        "load_1m": 3.12,
        "cpu_count": 28,
        "latency_p50_ms": 487,
        "collected_at": "2026-09-01T12:34:00+00:00",
    }
    row.update(overrides)
    return row


def test_c6_measures_paired_scopes_serially_and_grades_the_slower_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, tuple[str, ...], float]] = []
    all_times = iter((0.3, 0.5, 0.7))
    attention_times = iter((8.0, 9.0, 10.0))
    attention = tuple(search_benchmark.ATTENTION_PRIORITIES)

    def fake_run_search(
        query: str,
        mode: str,
        depth: int,
        *,
        priorities: tuple[str, ...],
        timeout: float,
    ) -> SearchResult:
        assert mode == "hybrid"
        assert depth == 20
        calls.append((query, priorities, timeout))
        elapsed = next(attention_times if priorities else all_times)
        return SearchResult(
            mode="hybrid",
            elapsed_seconds=elapsed,
            priority_scope="selected" if priorities else "all",
            selected_priorities=priorities,
        )

    monkeypatch.setattr(contract_audit, "run_search", fake_run_search)
    monkeypatch.setattr(
        contract_audit, "pdw_sql", lambda intent, sql: [_benchmark_host_row()]
    )

    verdict = contract_audit.c6_performance()

    assert verdict.status == contract_audit.RED
    assert [priorities for _, priorities, _ in calls] == [
        (),
        attention,
        attention,
        (),
        (),
        attention,
    ]
    assert all(timeout == 90.0 for _, _, timeout in calls)
    assert "all tiers: 3/3 valid, p50 0.50s, errors=0" in verdict.evidence
    assert (
        "attention (self,direct,cc): 3/3 valid, p50 9.00s, errors=0"
        in verdict.evidence
    )
    assert "historical benchmark context" in verdict.evidence
    assert "no contemporaneous host-pressure measurement" in verdict.evidence


def test_c6_invalid_probes_remain_in_the_grade_and_are_not_latency_samples(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attention = tuple(search_benchmark.ATTENTION_PRIORITIES)

    def fake_run_search(
        query: str,
        mode: str,
        depth: int,
        *,
        priorities: tuple[str, ...],
        timeout: float,
    ) -> SearchResult:
        if priorities == attention:
            return SearchResult(
                mode="hybrid",
                error="fast authentication failure",
                elapsed_seconds=0.001,
            )
        return SearchResult(
            mode="hybrid",
            elapsed_seconds=0.2,
            priority_scope="all",
        )

    monkeypatch.setattr(contract_audit, "run_search", fake_run_search)
    monkeypatch.setattr(contract_audit, "pdw_sql", lambda intent, sql: [])

    verdict = contract_audit.c6_performance()

    assert verdict.status == contract_audit.RED
    assert "all tiers: 3/3 valid, p50 0.20s, errors=0" in verdict.evidence
    assert "attention (self,direct,cc): 0/3 valid, p50 unmeasured, errors=3" in verdict.evidence
    assert "0.00s" not in verdict.evidence


def test_c6_fast_probes_need_no_current_saturation_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_search(
        query: str,
        mode: str,
        depth: int,
        *,
        priorities: tuple[str, ...],
        timeout: float,
    ) -> SearchResult:
        return SearchResult(
            mode="hybrid",
            elapsed_seconds=0.4,
            priority_scope="selected" if priorities else "all",
            selected_priorities=priorities,
        )

    monkeypatch.setattr(contract_audit, "run_search", fake_run_search)
    monkeypatch.setattr(
        contract_audit,
        "pdw_sql",
        lambda intent, sql: [_benchmark_host_row(saturation="idle")],
    )

    verdict = contract_audit.c6_performance()

    assert verdict.status == contract_audit.GREEN
    assert "fast responses do not require a saturation claim" in verdict.evidence
    assert "historical benchmark context" in verdict.evidence


def _search_benchmark_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "mode": "hybrid",
        "status": "ok",
        "probe_queries": 3,
        "latency_p50_ms": 487,
        "labeled_cases": 68,
        "mrr": 0.45,
        "errors": 0,
        "attention_priorities_json": '["self", "direct", "cc"]',
        "attention_probe_queries": 3,
        "attention_latency_p50_ms": 8439,
        "attention_labeled_cases": 68,
        "attention_comparable_cases": 68,
        "attention_found": 36,
        "attention_mrr": 0.19,
        "attention_errors": 0,
        "attention_recall_lost": 8,
        "attention_recall_gained": 1,
        "attention_recall_retained": 35,
        "all_relevant_lower_tier": 8,
        "collected_at": "2026-09-01T12:34:00+00:00",
    }
    row.update(overrides)
    return row


def _c8_sql_fixture(
    benchmark: dict[str, object] | None,
    captured_sql: list[str] | None = None,
) -> Callable[[str, str], list[dict[str, object]]]:
    def fake_pdw_sql(intent: str, sql: str) -> list[dict[str, object]]:
        if intent == "search health":
            return [{"component": "chunks", "status": "ok", "seq_lag": 0}]
        assert intent == "search benchmark"
        if captured_sql is not None:
            captured_sql.append(sql)
        return [benchmark] if benchmark is not None else []

    return fake_pdw_sql


def test_c8_preserves_all_tier_quality_but_grades_slow_attention_latency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sql: list[str] = []
    monkeypatch.setattr(
        contract_audit,
        "pdw_sql",
        _c8_sql_fixture(_search_benchmark_row(), captured_sql=sql),
    )

    verdict = contract_audit.c8_search_quality()

    assert verdict.status == contract_audit.RED
    assert "all-tier MRR 0.45 over 68 labels, p50 487ms, errors=0" in verdict.evidence
    assert "attention p50 8439ms over 3 probes, errors=0" in verdict.evidence
    assert "attention MRR 0.19 is diagnostic, not graded against the all-tier floor" in verdict.evidence
    assert "8 all-tier relevant answers were lower-tier; exclusions are expected" in verdict.evidence
    assert "attention_latency_p50_ms" in sql[0]
    assert "attention_errors" in sql[0]
    assert "WHERE mode = 'hybrid'" in sql[0]


@pytest.mark.parametrize(
    "benchmark",
    [
        _search_benchmark_row(attention_probe_queries=0, attention_latency_p50_ms=0),
        _search_benchmark_row(
            attention_latency_p50_ms=500, labeled_cases=0, mrr=0.0
        ),
        _search_benchmark_row(
            attention_latency_p50_ms=500,
            attention_labeled_cases=0, attention_comparable_cases=0
        ),
        _search_benchmark_row(attention_latency_p50_ms=500, status="unknown"),
        _search_benchmark_row(
            attention_latency_p50_ms=500, attention_priorities_json="[]"
        ),
    ],
    ids=[
        "missing-attention-probes",
        "missing-all-tier-labels",
        "missing-attention-labels",
        "stale",
        "wrong-attention-scope",
    ],
)
def test_c8_missing_or_stale_measurements_cannot_be_green(
    monkeypatch: pytest.MonkeyPatch, benchmark: dict[str, object]
) -> None:
    monkeypatch.setattr(contract_audit, "pdw_sql", _c8_sql_fixture(benchmark))

    verdict = contract_audit.c8_search_quality()

    assert verdict.status == contract_audit.YELLOW


def test_c8_does_not_grade_expected_attention_exclusions_as_a_quality_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _search_benchmark_row(
        attention_latency_p50_ms=500,
        attention_mrr=0.01,
        attention_recall_lost=20,
        attention_recall_gained=0,
        attention_recall_retained=15,
        all_relevant_lower_tier=20,
    )
    monkeypatch.setattr(contract_audit, "pdw_sql", _c8_sql_fixture(benchmark))

    verdict = contract_audit.c8_search_quality()

    assert verdict.status == contract_audit.GREEN
    assert "attention MRR 0.01 is diagnostic, not graded" in verdict.evidence
    assert "20 all-tier relevant answers were lower-tier" in verdict.evidence


def test_c8_empty_search_health_cannot_be_green(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_pdw_sql(intent: str, sql: str) -> list[dict[str, object]]:
        return [] if intent == "search health" else [
            _search_benchmark_row(attention_latency_p50_ms=500)
        ]

    monkeypatch.setattr(contract_audit, "pdw_sql", fake_pdw_sql)

    verdict = contract_audit.c8_search_quality()

    assert verdict.status != contract_audit.GREEN


def test_c8_attention_errors_are_graded_without_treating_exclusions_as_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _search_benchmark_row(
        attention_latency_p50_ms=500,
        attention_errors=2,
        attention_recall_lost=20,
        all_relevant_lower_tier=20,
    )
    monkeypatch.setattr(contract_audit, "pdw_sql", _c8_sql_fixture(benchmark))

    verdict = contract_audit.c8_search_quality()

    assert verdict.status == contract_audit.RED
    assert "attention p50 500ms over 3 probes, errors=2" in verdict.evidence
    assert "20 all-tier relevant answers were lower-tier; exclusions are expected" in verdict.evidence
