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


def _real_traffic_rows(**overrides: object) -> list[dict[str, object]]:
    row: dict[str, object] = {
        "status": "ok", "requests": 1000, "errors": 2,
        "p50_ms": 900, "p90_ms": 1800, "share_over_2s": "0.05000000000000000000",
        "slow_requests": 50, "idle_slow_requests": 0, "idle_slow_share": "0.00000000000000000000",
        "saturation_mix": {"io_bound": 5}, "slowest_leg_mix": {"lexical": 5},
    }
    row.update(overrides)
    return [dict(row, window=window) for window in ("rolling_24h", "rolling_7d")]


def _c6_sql(intent: str, sql: str) -> list[dict[str, object]]:
    if "search_request_latency" in sql:
        return _real_traffic_rows()
    return [_benchmark_host_row()]


@pytest.mark.parametrize(("overrides", "expected"), [
    ({}, "green"),
    ({"p50_ms": 2000}, "red"),
    ({"p50_ms": 2001}, "red"),
    ({"p90_ms": 2000}, "green"),
    ({"p90_ms": 2001}, "yellow"),
    ({"idle_slow_requests": 5, "idle_slow_share": 0.1}, "yellow"),
    ({"idle_slow_requests": 4, "idle_slow_share": 0.8}, "green"),
    ({"idle_slow_requests": 5, "idle_slow_share": 0.099}, "green"),
    ({"status": "no_data", "requests": 19}, "yellow"),
    ({"status": "unknown"}, "yellow"),
    ({"p50_ms": None}, "yellow"),
])
def test_c6_real_traffic_thresholds(monkeypatch, overrides, expected):
    monkeypatch.setattr(contract_audit, "pdw_sql", lambda *args: _real_traffic_rows(**overrides))
    status, evidence = contract_audit._real_traffic_latency()
    assert status == expected
    assert "rolling_24h" in evidence and "rolling_7d" in evidence
    assert "per-search host pressure" in evidence
    assert "lexical" in evidence


@pytest.mark.parametrize("rows", [None, [], _real_traffic_rows()[:1]])
def test_c6_missing_real_traffic_is_yellow(monkeypatch, rows):
    monkeypatch.setattr(contract_audit, "pdw_sql", lambda *args: rows)
    status, evidence = contract_audit._real_traffic_latency()
    assert status == contract_audit.YELLOW
    assert "unavailable" in evidence


@pytest.mark.parametrize(("idle", "share", "expected"), [
    (5, "0.10000000000000000000", "yellow"),
    (6, "0.40000000000000000000", "yellow"),
    (5, "0.09999999999999999999", "green"),
    (4, "0.40000000000000000000", "green"),
    (5, 0.1, "yellow"),
    (5, None, "yellow"),
    (5, "invalid", "yellow"),
    (0, None, "green"),
])
def test_c6_idle_rule_reads_decimal_shares_through_sql_json(monkeypatch, idle, share, expected):
    rows = _real_traffic_rows(idle_slow_requests=idle, idle_slow_share=share)
    # Exercise pdw_sql's real JSON decoding rather than handing it Python floats.
    monkeypatch.setattr(contract_audit.subprocess, "run", lambda *args, **kwargs:
                        subprocess.CompletedProcess(args[0], 0, stdout=json.dumps({"rows": rows})))
    status, evidence = contract_audit._real_traffic_latency()
    assert status == expected
    assert "check crashed" not in evidence
    assert str(share) in evidence


def test_c6_decimal_idle_share_cannot_mask_a_red_probe(monkeypatch, capsys):
    monkeypatch.setattr(contract_audit, "CHECKS", [contract_audit.c6_performance])
    monkeypatch.setattr(contract_audit, "_validated_search_probe", lambda *args, **kwargs: (8.0, ""))
    rows = _real_traffic_rows(idle_slow_requests=6, idle_slow_share="0.40000000000000000000")
    monkeypatch.setattr(contract_audit.subprocess, "run", lambda args, **kwargs:
                        subprocess.CompletedProcess(args, 0, stdout=json.dumps(
                            {"rows": rows if "search_request_latency" in args[-1] else []})))
    assert contract_audit.main(["--json"]) == 1
    verdict = json.loads(capsys.readouterr().out)[0]
    assert verdict["status"] == "red"
    assert "check crashed" not in verdict["evidence"]
    assert "paired serial hybrid probes" in verdict["evidence"]


def test_c6_real_traffic_uses_worse_window(monkeypatch):
    rows = _real_traffic_rows(idle_slow_requests=5, idle_slow_share="0.10000000000000000000")
    rows[1]["p50_ms"] = 2500
    monkeypatch.setattr(contract_audit, "pdw_sql", lambda *args: rows)
    assert contract_audit._real_traffic_latency()[0] == contract_audit.RED


def test_c6_slow_real_traffic_overrides_fast_probes(monkeypatch):
    monkeypatch.setattr(contract_audit, "_validated_search_probe", lambda *args, **kwargs: (0.1, ""))
    monkeypatch.setattr(contract_audit, "pdw_sql", lambda intent, sql:
                        _real_traffic_rows(p50_ms=2200) if "search_request_latency" in sql else [])
    assert contract_audit.c6_performance().status == contract_audit.RED


def test_skip_latency_still_grades_real_traffic(monkeypatch, capsys):
    monkeypatch.setattr(contract_audit, "CHECKS", [contract_audit.c6_performance])
    monkeypatch.setattr(contract_audit, "pdw_sql", lambda *args: _real_traffic_rows(p50_ms=2200))
    monkeypatch.setattr(contract_audit, "_validated_search_probe", lambda *args, **kwargs:
                        pytest.fail("skip-latency must not run probes"))
    assert contract_audit.main(["--skip-latency", "--json"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["status"] == contract_audit.RED
    assert "rolling_24h" in payload[0]["evidence"]


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
        client_name: str,
    ) -> SearchResult:
        assert mode == "hybrid"
        assert depth == 20
        # Labelled so the real-search grade leaves the audit's probes out.
        assert client_name == "contract-audit"
        calls.append((query, priorities, timeout))
        elapsed = next(attention_times if priorities else all_times)
        return SearchResult(
            mode="hybrid",
            elapsed_seconds=elapsed,
            priority_scope="selected" if priorities else "all",
            selected_priorities=priorities,
        )

    monkeypatch.setattr(contract_audit, "run_search", fake_run_search)
    monkeypatch.setattr(contract_audit, "SEARCH_PROBE_QUERIES", ("one", "two", "three"))
    monkeypatch.setattr(
        contract_audit, "pdw_sql", _c6_sql
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
        client_name: str,
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

    monkeypatch.setattr(contract_audit, "SEARCH_PROBE_QUERIES", ("one", "two", "three"))
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
        client_name: str,
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
        _c6_sql,
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


def test_latency_probes_time_the_short_queries_the_guide_teaches() -> None:
    """The guide says to search a name or an identifier alone, and the hybrid
    literal leg only runs for a query of at most three words. Timing only
    4-6 word term bags let C6 read green at 1.3s on 2026-09-30 while a
    one-word search took 4-11s. Both the weekly benchmark and this audit must
    time both shapes."""
    from personal_data_warehouse import search_benchmark_runner as runner
    from personal_data_warehouse.postgres import SEARCH_HYBRID_EXACT_MAX_WORDS

    for probes in (runner.DEFAULT_PROBE_QUERIES, contract_audit.SEARCH_PROBE_QUERIES):
        word_counts = [len(query.split()) for query in probes]
        assert sum(count <= 2 for count in word_counts) >= 2, probes
        assert any(count > SEARCH_HYBRID_EXACT_MAX_WORDS for count in word_counts), probes


def _c7_sql(responses: dict[str, list[dict] | None]) -> Callable[[str, str], list[dict] | None]:
    def fake(intent: str, sql: str) -> list[dict] | None:
        for needle, rows in responses.items():
            if needle in sql:
                return rows
        raise AssertionError(f"unexpected C7 query: {sql}")

    return fake


def _c7_healthy() -> dict[str, list[dict] | None]:
    return {
        "marts_ops.pipeline_health": [
            {"pipeline": "gmail", "status": "ok", "snapshot_age_seconds": 120},
            {"pipeline": "slack", "status": "attention", "snapshot_age_seconds": 120},
        ],
        "marts_ops.mart_view_health": [
            {"view_name": "huddles", "status": "attention", "input_status": "attention",
             "cause_pipelines": ["slack"], "probe_status": "ok", "snapshot_age_seconds": 120},
        ],
        "marts_ops.timeline_adapter_health": [{"n": 27, "unknown": 0}],
        "marts_ops.table_freshness": [{"n": 160}],
        "marts_ops.collation_health": [{"n": 300}],
    }


def test_c7_grades_whether_health_is_inspectable_not_whether_it_is_good(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C7 is "health is inspectable": a source that is down and SAYS so is the
    contract working. Down sources are graded by their own contracts (C11,
    S1-S3); C7 reports them as evidence only."""
    monkeypatch.setattr(contract_audit, "pdw_sql", _c7_sql(_c7_healthy()))
    verdict = contract_audit.c7_pipeline_health()
    assert verdict.status == contract_audit.GREEN, verdict.evidence
    assert "slack" in verdict.evidence and "attention" in verdict.evidence


def test_c7_is_red_when_a_health_level_cannot_be_read(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = _c7_healthy()
    responses["marts_ops.timeline_adapter_health"] = None
    monkeypatch.setattr(contract_audit, "pdw_sql", _c7_sql(responses))
    verdict = contract_audit.c7_pipeline_health()
    assert verdict.status == contract_audit.RED
    assert "timeline_adapter_health" in verdict.evidence


def test_c7_is_yellow_when_a_mart_cannot_name_why_it_is_not_ok(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = _c7_healthy()
    responses["marts_ops.mart_view_health"] = [
        {"view_name": "notifications", "status": "late", "input_status": "late",
         "cause_pipelines": [], "probe_status": "ok", "snapshot_age_seconds": 120},
    ]
    monkeypatch.setattr(contract_audit, "pdw_sql", _c7_sql(responses))
    verdict = contract_audit.c7_pipeline_health()
    assert verdict.status == contract_audit.YELLOW
    assert "notifications" in verdict.evidence


def test_c7_is_yellow_when_the_collector_snapshot_is_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = _c7_healthy()
    responses["marts_ops.pipeline_health"] = [
        {"pipeline": "gmail", "status": "unknown", "snapshot_age_seconds": 99999},
    ]
    monkeypatch.setattr(contract_audit, "pdw_sql", _c7_sql(responses))
    verdict = contract_audit.c7_pipeline_health()
    assert verdict.status == contract_audit.YELLOW


def test_c12_is_green_while_agents_md_fits_and_names_live_tests() -> None:
    verdict = contract_audit.c12_future_developers()
    assert verdict.status == contract_audit.GREEN, verdict.evidence
    assert "AGENTS.md" in verdict.evidence


def test_c12_is_red_when_agents_md_outgrows_one_sitting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(contract_audit, "AGENTS_MD_MAX_BYTES", 100)
    assert contract_audit.c12_future_developers().status == contract_audit.RED


# --- C4/C10 must see the database's own integrity -----------------------------
#
# On 2026-10-03 the audit graded C4 and C10 green while production held 9,125
# page checksum failures (base_muse.events unreadable, every backup since 10-01
# flagged `error(s) detected`): neither check read anything but row presence.


def _integrity_sql(
    *,
    checksum: str,
    integrity: str = "ok",
    backup_status: str = "ok",
    structural_rows: list[dict] | None = None,
):
    def fake(intent: str, sql: str, **_: object):
        if "collation_health" in sql:
            rows = [{"object_id": "data_checksums", "scope": "database", "status": checksum, "detail": "9125 page checksum failure(s)"}]
            if structural_rows:
                rows.extend(structural_rows)
            return rows
        if "pgbackrest_health" in sql:
            return [
                {
                    "stanza": "pdw", "status": backup_status, "backup_count": 15,
                    "last_full_at": "2026-09-27T22:48:09Z", "full_age_seconds": 400000,
                    "last_archived_at": "2026-10-03T08:55:32Z", "restore_status": "ok",
                    "last_restore_label": "x", "restore_age_seconds": 86400,
                    "expire_status": "ok", "last_expire_error": None,
                    "integrity_status": integrity, "error_backup_count": 9,
                    "last_clean_backup_label": "20260927-223107F_20260928-071503I",
                }
            ]
        if "information_schema" in sql:
            return [{"n": 25}]
        return []

    return fake


def test_c10_is_red_while_the_database_fails_page_checksums(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(contract_audit, "pdw_sql", _integrity_sql(checksum="failing"))
    verdict = contract_audit.c10_backups()
    assert verdict.status == contract_audit.RED
    assert "checksum" in verdict.evidence


def test_c10_and_c4_detect_index_or_heap_corruption_with_clean_page_checksums(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A page damaged in RAM before write carries a valid checksum (2026-10-05 / 2026-10-25)."""
    corrupt_index = [
        {
            "object_id": "index:timeline_events_priority_time_idx",
            "scope": "index",
            "status": "failing",
            "detail": "bt_index_check failed: index tuple size does not equal lp_len",
        }
    ]
    monkeypatch.setattr(
        contract_audit, "pdw_sql", _integrity_sql(checksum="ok", structural_rows=corrupt_index)
    )
    c10 = contract_audit.c10_backups()
    assert c10.status == contract_audit.RED
    assert "index:timeline_events_priority_time_idx=failing" in c10.evidence

    c4 = contract_audit.c4_raw_data_queryable()
    assert c4.status == contract_audit.YELLOW
    assert "index:timeline_events_priority_time_idx=failing" in c4.evidence

    attention_heap = [
        {
            "object_id": "heap:timeline.events",
            "scope": "heap",
            "status": "attention",
            "detail": "verify_heapam timed out",
        }
    ]
    monkeypatch.setattr(
        contract_audit, "pdw_sql", _integrity_sql(checksum="ok", structural_rows=attention_heap)
    )
    assert contract_audit.c10_backups().status == contract_audit.YELLOW
    assert contract_audit.c4_raw_data_queryable().status == contract_audit.YELLOW


def test_c11_grades_failing_derived_or_enrichment_pipelines(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, str] = {}
    rows = {
        "slack health": [{"conversation_type": "im", "status": "ok"}],
        "plaid health": [{"institution_name": "Bank", "status": "ok"}],
        "simplefin health": [{"org_name": "Org", "name": "Checking", "status": "ok", "shared_with_plaid": 0}],
        "source pipeline health": [
            {"pipeline": "finance_ledger", "status": "failing"},
            {"pipeline": "receipt_enrichment", "status": "stale"},
        ],
    }

    def fake_pdw_sql(intent: str, sql: str, **_kwargs):
        seen[intent] = sql
        return rows[intent]

    monkeypatch.setattr(contract_audit, "pdw_sql", fake_pdw_sql)
    verdict = contract_audit.c11_source_slas()
    assert verdict.status == contract_audit.YELLOW
    assert "kind IN ('source', 'derived', 'enrichment')" in " ".join(seen["source pipeline health"].split())
    assert "pipelines down 2" in verdict.evidence
    assert "pipeline:finance_ledger=failing" in verdict.evidence
    assert "pipeline:receipt_enrichment=stale" in verdict.evidence


def test_c10_is_not_green_when_the_newest_backup_copied_corrupt_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        contract_audit, "pdw_sql", _integrity_sql(checksum="ok", integrity="errors_detected")
    )
    verdict = contract_audit.c10_backups()
    assert verdict.status != contract_audit.GREEN
    assert "20260927-223107F_20260928-071503I" in verdict.evidence


def test_c10_reads_green_only_with_clean_pages_and_clean_backups(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(contract_audit, "pdw_sql", _integrity_sql(checksum="ok"))
    assert contract_audit.c10_backups().status == contract_audit.GREEN


def test_c4_cannot_vouch_for_raw_data_while_pages_fail_checksums(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(contract_audit, "pdw_sql", _integrity_sql(checksum="failing"))
    assert contract_audit.c4_raw_data_queryable().status != contract_audit.GREEN


def _s2_sql(*, landing: list[dict] | None) -> Callable[..., list[dict] | None]:
    coverage = [
        {"source": source, "recordings": 10, "untranscribed": 0, "untranscribable": 0, "unenriched": 0, "matched": 5}
        for source in ("alice_voice_recordings", "apple_notes", "apple_voice_memos", "google_recorder")
    ]

    def fake(intent: str, sql: str, **_kwargs) -> list[dict] | None:
        return landing if "voice_memo_landing_health" in sql else coverage

    return fake


def _landing(status: str, p50: float | None, *, pixel_status: str = "ok") -> list[dict]:
    return [
        {"source": "apple_voice_memos", "status": status, "pdw_landing_p50_seconds": p50,
         "device_delivery_p50_seconds": 17.0, "end_to_transcript_p50_seconds": 40.0, "stuck_untranscribed": 0},
        {"source": "google_recorder", "status": pixel_status, "pdw_landing_p50_seconds": 21.0,
         "device_delivery_p50_seconds": None, "end_to_transcript_p50_seconds": 21.0, "stuck_untranscribed": 0},
    ]


def test_s2_grades_how_fast_a_memo_lands_not_only_whether_it_does(monkeypatch: pytest.MonkeyPatch) -> None:
    """S2 read green on 2026-10-16 while a short memo took p50 9 min to transcribe.

    Coverage alone cannot see speed, so the landing view's verdict is part of the grade.
    """
    monkeypatch.setattr(contract_audit, "pdw_sql", _s2_sql(landing=_landing("ok", 22.0)))
    verdict = contract_audit.s2_voice()
    assert verdict.status == contract_audit.GREEN, verdict.evidence
    assert "22" in verdict.evidence

    monkeypatch.setattr(contract_audit, "pdw_sql", _s2_sql(landing=_landing("attention", 95.0)))
    assert contract_audit.s2_voice().status == contract_audit.YELLOW

    monkeypatch.setattr(contract_audit, "pdw_sql", _s2_sql(landing=_landing("failing", 900.0)))
    assert contract_audit.s2_voice().status == contract_audit.RED


def test_s2_grades_the_pixels_landing_speed_too(monkeypatch: pytest.MonkeyPatch) -> None:
    """The Pixel's Recorder is the phone Zach carries from 2026-10-23, so a slow
    or stuck Pixel memo must be able to colour S2 even while Apple is fine."""
    monkeypatch.setattr(contract_audit, "pdw_sql", _s2_sql(landing=_landing("ok", 22.0, pixel_status="failing")))
    verdict = contract_audit.s2_voice()
    assert verdict.status == contract_audit.RED
    assert "google_recorder" in verdict.evidence


def test_s2_wants_every_voice_source_including_the_pixel(monkeypatch: pytest.MonkeyPatch) -> None:
    coverage = [
        {"source": source, "recordings": 10, "untranscribed": 0, "untranscribable": 0, "unenriched": 0, "matched": 5}
        for source in ("alice_voice_recordings", "apple_notes", "apple_voice_memos")
    ]
    monkeypatch.setattr(
        contract_audit,
        "pdw_sql",
        lambda intent, sql, **_k: _landing("ok", 22.0) if "voice_memo_landing_health" in sql else coverage,
    )
    assert contract_audit.s2_voice().status != contract_audit.GREEN


def test_s2_withholds_green_when_landing_speed_cannot_be_read(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(contract_audit, "pdw_sql", _s2_sql(landing=None))
    verdict = contract_audit.s2_voice()
    assert verdict.status == contract_audit.YELLOW
    assert "voice_memo_landing_health" in verdict.evidence
