"""Unit tests for the labeled search-retrieval benchmark.

Every fixture here is synthetic.  Real labels contain private queries and
timeline references and live only under the gitignored ``.search-eval/``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json

import pytest

from personal_data_warehouse.search_benchmark import (
    BenchmarkCase,
    ContaminationFilter,
    PrivateOutputError,
    ResultRow,
    assert_private_path,
    first_relevant_rank,
    load_cases,
    matches_predicate,
    parse_search_payload,
    partition_stale_cases,
    summarize,
    trim_to_json,
)


def _row(ref: str, *, source: str = "gmail", context: str = "", ts: str = "2026-05-01T00:00:00Z",
         title: str = "", text: str = "") -> ResultRow:
    return ResultRow(ref=ref, source=source, context=context, event_ts=ts, title=title, text=text)


class TestPredicateMatching:
    def test_source_predicate_accepts_listed_source(self):
        assert matches_predicate(_row("a:1", source="gmail"), {"sources": ["gmail", "slack"]})

    def test_source_predicate_rejects_other_source(self):
        assert not matches_predicate(_row("a:1", source="photo"), {"sources": ["gmail"]})

    def test_since_predicate_rejects_older_event(self):
        row = _row("a:1", ts="2025-01-01T00:00:00Z")
        assert not matches_predicate(row, {"since": "2026-01-01"})
        assert matches_predicate(_row("a:1", ts="2026-06-01T00:00:00Z"), {"since": "2026-01-01"})

    def test_text_regex_searches_title_and_text_case_insensitively(self):
        row = _row("a:1", title="Out Of Office", text="body")
        assert matches_predicate(row, {"text_regex": "out of office"})
        assert not matches_predicate(row, {"text_regex": "vacation"})

    def test_all_conditions_must_hold(self):
        row = _row("a:1", source="gmail", ts="2026-06-01T00:00:00Z", title="Renewal")
        assert matches_predicate(row, {"sources": ["gmail"], "text_regex": "renewal"})
        assert not matches_predicate(row, {"sources": ["slack"], "text_regex": "renewal"})

    def test_empty_predicate_never_matches(self):
        # An empty predicate would make every result relevant; that is a
        # labeling mistake, not a match-everything instruction.
        assert not matches_predicate(_row("a:1"), {})


class TestFirstRelevantRank:
    def test_exact_ref_match_returns_one_based_rank(self):
        rows = [_row("a:1"), _row("a:2"), _row("a:3")]
        assert first_relevant_rank(rows, {"a:3"}, [], None) == 3

    def test_returns_none_when_absent(self):
        rows = [_row("a:1"), _row("a:2")]
        assert first_relevant_rank(rows, {"a:9"}, [], None) is None

    def test_soft_match_accepts_neighbour_in_same_context_window(self):
        # The semantic branch can return a different event from the same chunk
        # window, so a near-miss in the same (source, context) stream counts.
        truth_meta = [("slack", "C123", datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc))]
        rows = [_row("slack_message:x", source="slack", context="C123",
                     ts="2026-05-01T12:30:00Z")]
        assert first_relevant_rank(rows, {"slack_message:other"}, truth_meta, None) == 1

    def test_soft_match_rejects_far_neighbour(self):
        truth_meta = [("slack", "C123", datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc))]
        rows = [_row("slack_message:x", source="slack", context="C123",
                     ts="2026-05-01T20:00:00Z")]
        assert first_relevant_rank(rows, {"slack_message:other"}, truth_meta, None) is None

    def test_soft_match_requires_same_context(self):
        truth_meta = [("slack", "C123", datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc))]
        rows = [_row("slack_message:x", source="slack", context="C999",
                     ts="2026-05-01T12:05:00Z")]
        assert first_relevant_rank(rows, {"slack_message:other"}, truth_meta, None) is None

    def test_predicate_match_counts_when_no_ref_matches(self):
        rows = [_row("a:1", source="photo"), _row("a:2", source="gmail", title="Out of office")]
        rank = first_relevant_rank(rows, {"zz:9"}, [], {"sources": ["gmail"]})
        assert rank == 2

    def test_earliest_relevant_position_wins(self):
        rows = [_row("a:1", source="gmail"), _row("a:2", source="gmail")]
        assert first_relevant_rank(rows, {"a:2"}, [], {"sources": ["gmail"]}) == 1


class TestContaminationFilter:
    def test_drops_result_from_a_listed_session(self):
        f = ContaminationFilter(session_ids=("sess-abc",), cutoff=None)
        assert f.is_contaminated(_row("agent_session_turn:claude_code|sess-abc|4"))

    def test_keeps_unrelated_result(self):
        f = ContaminationFilter(session_ids=("sess-abc",), cutoff=None)
        assert not f.is_contaminated(_row("gmail_email:acct|123"))

    def test_drops_agent_sessions_at_or_after_cutoff(self):
        f = ContaminationFilter(
            session_ids=(), cutoff=datetime(2026, 8, 22, tzinfo=timezone.utc)
        )
        assert f.is_contaminated(
            _row("agent_session:x", source="agent_session", ts="2026-08-23T00:00:00Z")
        )
        assert not f.is_contaminated(
            _row("agent_session:x", source="agent_session", ts="2026-08-21T00:00:00Z")
        )

    def test_cutoff_only_applies_to_agent_sessions(self):
        f = ContaminationFilter(
            session_ids=(), cutoff=datetime(2026, 8, 22, tzinfo=timezone.utc)
        )
        assert not f.is_contaminated(
            _row("gmail_email:x", source="gmail", ts="2026-08-23T00:00:00Z")
        )

    def test_apply_reports_how_many_were_dropped(self):
        f = ContaminationFilter(session_ids=("bad",), cutoff=None)
        kept, dropped = f.apply([_row("x:bad"), _row("y:good")])
        assert [r.ref for r in kept] == ["y:good"]
        assert dropped == 1


class TestParseSearchPayload:
    def test_reads_rows_and_fallback(self):
        payload = {
            "mode": "hybrid",
            "fallback_reason": "",
            "rows": [{"ref": "a:1", "source": "gmail", "event_ts": "2026-01-01T00:00:00Z",
                      "title": "t", "text": "b", "context": "c"}],
        }
        result = parse_search_payload(json.dumps(payload))
        assert result.mode == "hybrid"
        assert [r.ref for r in result.rows] == ["a:1"]
        assert result.rows[0].source == "gmail"

    def test_missing_rows_is_empty_not_an_error(self):
        assert parse_search_payload(json.dumps({"mode": "keyword"})).rows == ()

    def test_invalid_json_is_reported_as_error(self):
        assert parse_search_payload("not json").error


class TestTrimToJson:
    def test_returns_array_payload_intact(self):
        # `pdw sql --output json` emits an array; anchoring on "{" would land
        # inside it and decode only the first element.
        raw = '[\n {"a": 1},\n {"a": 2}\n]'
        assert json.loads(trim_to_json(raw)) == [{"a": 1}, {"a": 2}]

    def test_strips_preamble_before_an_object(self):
        assert json.loads(trim_to_json('note: hi\n{"a": 1}')) == {"a": 1}

    def test_strips_preamble_before_an_array(self):
        assert json.loads(trim_to_json('note: hi\n[{"a": 1}]')) == [{"a": 1}]

    def test_passes_through_when_no_payload_marker(self):
        assert trim_to_json("plain") == "plain"


class TestSummarize:
    def _case(self, name, stratum, rank):
        return {"query": name, "stratum": stratum,
                "hybrid": {"rank": rank}, "keyword": {"rank": None}, "exact": {"rank": None}}

    def test_counts_hits_at_thresholds(self):
        rows = [self._case("a", "entity", 1), self._case("b", "entity", 7),
                self._case("c", "entity", None)]
        summary = summarize(rows, depth=50)
        assert summary["overall"]["hybrid"]["hit_at_1"] == 1
        assert summary["overall"]["hybrid"]["hit_at_10"] == 2
        assert summary["overall"]["hybrid"]["found"] == 2
        assert summary["overall"]["hybrid"]["queries"] == 3

    def test_mrr_uses_reciprocal_rank_over_all_queries(self):
        rows = [self._case("a", "entity", 1), self._case("b", "entity", 2)]
        assert summarize(rows, depth=50)["overall"]["hybrid"]["mrr"] == pytest.approx(0.75)

    def test_breaks_out_by_stratum(self):
        rows = [self._case("a", "natural_language", 1), self._case("b", "entity", None)]
        summary = summarize(rows, depth=50)
        assert summary["by_stratum"]["natural_language"]["hybrid"]["hit_at_1"] == 1
        assert summary["by_stratum"]["entity"]["hybrid"]["found"] == 0


class TestLoadCases:
    def test_rejects_case_without_refs_or_predicate(self, tmp_path: Path):
        path = tmp_path / "gt.json"
        path.write_text(json.dumps([{"query": "q", "verdict": "FOUND"}]))
        with pytest.raises(ValueError, match="truth_refs"):
            load_cases(path)

    def test_not_in_corpus_cases_are_skipped_by_default(self, tmp_path: Path):
        path = tmp_path / "gt.json"
        path.write_text(json.dumps([
            {"query": "a", "verdict": "NOT_IN_CORPUS"},
            {"query": "b", "verdict": "FOUND", "truth_refs": ["x:1"]},
        ]))
        cases = load_cases(path)
        assert [c.query for c in cases] == ["b"]

    def test_defaults_stratum_when_absent(self, tmp_path: Path):
        path = tmp_path / "gt.json"
        path.write_text(json.dumps([{"query": "a", "verdict": "FOUND", "truth_refs": ["x:1"]}]))
        assert load_cases(path)[0].stratum == "unclassified"

    def test_reads_predicate_only_case(self, tmp_path: Path):
        path = tmp_path / "gt.json"
        path.write_text(json.dumps([
            {"query": "a", "verdict": "FOUND", "truth_predicate": {"sources": ["gmail"]}}
        ]))
        case = load_cases(path)[0]
        assert case.truth_predicate == {"sources": ["gmail"]}
        assert case.truth_refs == ()


class TestPrivacyGuard:
    def test_rejects_output_outside_the_private_directory(self, tmp_path: Path):
        with pytest.raises(PrivateOutputError):
            assert_private_path(tmp_path / "report.json")

    def test_accepts_path_under_search_eval(self, tmp_path: Path):
        target = tmp_path / ".search-eval" / "report.json"
        assert_private_path(target) == target


class TestBenchmarkCase:
    def test_case_exposes_its_relevance_basis(self):
        case = BenchmarkCase(query="q", stratum="entity", verdict="FOUND",
                             truth_refs=("a:1",), truth_predicate=None,
                             ambiguous=False, note="")
        assert case.relevance_basis == "refs"

    def test_case_with_both_reports_both_bases(self):
        case = BenchmarkCase(query="q", stratum="entity", verdict="FOUND",
                             truth_refs=("a:1",), truth_predicate={"sources": ["gmail"]},
                             ambiguous=True, note="")
        assert case.relevance_basis == "refs+predicate"

    def test_predicate_case_reports_predicate_basis(self):
        case = BenchmarkCase(query="q", stratum="entity", verdict="FOUND",
                             truth_refs=(), truth_predicate={"sources": ["gmail"]},
                             ambiguous=True, note="")
        assert case.relevance_basis == "predicate"


def test_smoke_report_names_every_source_that_failed() -> None:
    # Every labeled query in the benchmark is UNSCOPED, so `sources => [...]`
    # has no coverage at all -- and two production regressions hid in exactly
    # that gap for hours: a plan flip made seven source tokens raise
    # "query specifies index X but planner chose index Y", and a scoped hybrid
    # search blew the app's statement budget at 73s. Both are one call per
    # token away from being obvious, so the harness makes that call.
    from personal_data_warehouse.search_benchmark import summarize_smoke

    results = [
        {"source": "gmail", "mode": "hybrid", "error": "", "elapsed_seconds": 3.1, "rows": 5},
        {"source": "photo", "mode": "hybrid", "error": "planner chose index", "elapsed_seconds": 0.2, "rows": 0},
        {"source": "whoop", "mode": "hybrid", "error": "", "elapsed_seconds": 44.0, "rows": 5},
    ]
    report = summarize_smoke(results, slow_seconds=30.0)
    assert report["failed"] == ["photo/hybrid"]
    assert report["slow"] == ["whoop/hybrid"]
    assert report["ok"] == 2


def test_smoke_report_is_clean_when_every_source_answers() -> None:
    from personal_data_warehouse.search_benchmark import summarize_smoke

    results = [
        {"source": s, "mode": "hybrid", "error": "", "elapsed_seconds": 1.0, "rows": 3}
        for s in ("gmail", "photo", "whoop")
    ]
    report = summarize_smoke(results, slow_seconds=30.0)
    assert report["failed"] == [] and report["slow"] == [] and report["ok"] == 3


def test_smoke_subcommand_needs_no_labels(monkeypatch, tmp_path) -> None:
    # The label file is the expensive, private artifact. Asking "does every
    # source still answer?" must not require it -- that check should be
    # runnable on any deployment, including one with no labels at all.
    import personal_data_warehouse.search_benchmark as module

    called = {}

    def fake_run_smoke(query, *, modes, depth, workers, progress=True):
        called["query"] = query
        return {"checked": 2, "ok": 2, "failed": [], "slow": [], "slow_seconds": 25.0,
                "results": [], "environment": {}, "config": {}}

    def explode(*_args, **_kwargs):
        raise AssertionError("smoke must not load labels")

    monkeypatch.setattr(module, "run_smoke", fake_run_smoke)
    monkeypatch.setattr(module, "load_cases", explode)
    out = tmp_path / ".search-eval" / "smoke.json"
    assert module.main(["smoke", "--output", str(out)]) == 0
    assert called["query"]


def test_smoke_subcommand_exits_nonzero_when_a_source_fails(monkeypatch, tmp_path) -> None:
    import personal_data_warehouse.search_benchmark as module

    monkeypatch.setattr(module, "run_smoke", lambda *a, **k: {
        "checked": 2, "ok": 1, "failed": ["photo/hybrid"], "slow": [], "slow_seconds": 25.0,
        "results": [], "environment": {}, "config": {}})
    monkeypatch.setattr(module, "load_cases", lambda *a, **k: [])
    out = tmp_path / ".search-eval" / "smoke.json"
    assert module.main(["smoke", "--output", str(out)]) == 1


def test_smoke_does_not_pay_for_the_corpus_stamp(monkeypatch, tmp_path) -> None:
    # Counting 7M chunks and 6M embeddings takes minutes. A scored report needs
    # it, because two scores over different corpora are not comparable; a
    # post-deploy health check does not, and a check nobody wants to wait ten
    # minutes for is a check nobody runs.
    import personal_data_warehouse.search_benchmark as module

    seen = {}

    def fake_capture(*, include_corpus: bool = True):
        seen["include_corpus"] = include_corpus
        return {}

    monkeypatch.setattr(module, "capture_environment", fake_capture)
    monkeypatch.setattr(module, "_pdw_json", lambda *a, **k: [{"source": "gmail"}])
    monkeypatch.setattr(module, "run_search",
                        lambda *a, **k: module.SearchResult(mode="hybrid", rows=(), elapsed_seconds=0.1))
    module.run_smoke("probe", modes=("hybrid",), depth=5, workers=1, progress=False)
    assert seen["include_corpus"] is False


def test_search_failure_carries_the_cli_stderr(monkeypatch) -> None:
    # CalledProcessError stringifies to "returned non-zero exit status 1" and
    # drops stderr, which is where the reason lives. A harness that hides the
    # reason sends you to reproduce every failure by hand.
    import subprocess as subprocess_module

    import personal_data_warehouse.search_benchmark as module

    def fake_run(*_args, **_kwargs):
        return subprocess_module.CompletedProcess(
            args=["pdw"], returncode=1, stdout="", stderr="statement timeout: budget exceeded"
        )

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    result = module.run_search("anything", "hybrid", 10)
    assert "statement timeout: budget exceeded" in result.error


def test_run_search_uses_first_class_cli_with_structured_flags(monkeypatch) -> None:
    import subprocess as subprocess_module

    import personal_data_warehouse.search_benchmark as module

    seen = {}

    def fake_run(args, **kwargs):
        seen["args"] = args
        seen["kwargs"] = kwargs
        return subprocess_module.CompletedProcess(
            args=args,
            returncode=0,
            stdout=json.dumps(
                {
                    "query": "budget approval",
                    "mode": "hybrid",
                    "rows": [{"ref": "slack_message:abc", "source": "slack"}],
                }
            ),
            stderr="",
        )

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    result = module.run_search(
        "budget approval",
        "hybrid",
        7,
        sources=("slack", "gmail"),
        since="2026-08-01",
    )

    assert seen["args"] == [
        "pdw",
        "search",
        "--output",
        "json",
        "--mode",
        "hybrid",
        "--max-results",
        "7",
        "--source",
        "slack,gmail",
        "--since",
        "2026-08-01",
        "--",
        "budget approval",
    ]
    assert seen["kwargs"]["timeout"] == 420.0
    assert result.rows[0].ref == "slack_message:abc"


class TestPartitionStaleCases:
    def _case(self, query, refs=(), predicate=None):
        return BenchmarkCase(
            query=query, stratum="x", verdict="FOUND", truth_refs=tuple(refs),
            truth_predicate=predicate, ambiguous=False, note="",
        )

    def test_case_with_no_live_ref_is_set_aside_not_scored(self):
        live = {"a:1": ("gmail", "", None)}
        scorable, stale = partition_stale_cases(
            [self._case("q1", ["a:1"]), self._case("q2", ["gone:1", "gone:2"])], live
        )
        assert [c.query for c in scorable] == ["q1"]
        assert [c.query for c in stale] == ["q2"]

    def test_case_with_one_live_ref_stays_scorable(self):
        scorable, stale = partition_stale_cases(
            [self._case("q", ["gone:1", "a:1"])], {"a:1": ("gmail", "", None)}
        )
        assert [c.query for c in scorable] == ["q"] and stale == []

    def test_predicate_case_is_never_stale(self):
        scorable, stale = partition_stale_cases(
            [self._case("q", ["gone:1"], predicate={"sources": ["gmail"]})], {}
        )
        assert [c.query for c in scorable] == ["q"] and stale == []
