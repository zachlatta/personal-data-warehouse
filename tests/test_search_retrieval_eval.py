from __future__ import annotations

import json

import pytest

from personal_data_warehouse.search_retrieval_eval import (
    EvalCase,
    SearchResult,
    canonical_ref,
    evaluate_case,
    extract_cases,
    parse_search_response,
)


def _nested_result(rows: list[dict]) -> str:
    return json.dumps({"content": [{"content": json.dumps(rows)}]})


def test_extract_cases_decodes_shell_json_and_sql_literals() -> None:
    records = [
        {
            "source": "codex",
            "session_id": "session-1",
            "seq": 42,
            "occurred_at": "2026-08-01T00:00:00Z",
            "tool_input_json": json.dumps(
                {
                    "cmd": (
                        "pdw sql --output json -q x \"SELECT * FROM "
                        "timeline.search_text('launch team''s budget', 20, "
                        "sources => ARRAY['slack','gmail'], since => '2026-01-01')\""
                    )
                }
            ),
            "tool_result_json": _nested_result(
                [
                    {"ref": "slack_message:one", "source": "slack"},
                    {"ref": "gmail_email:two", "source": "gmail"},
                ]
            ),
        }
    ]

    cases = extract_cases(records)

    assert len(cases) == 1
    case = cases[0]
    assert case.query == "launch team's budget"
    assert case.mode == "keyword"
    assert case.sources == ("slack", "gmail")
    assert case.since == "2026-01-01"
    assert case.relevant_refs == ("slack_message:one", "gmail_email:two")
    assert case.relevant_keys == ("slack_message:one", "gmail_email:two")
    assert case.relevance_provenance == "historical_search_results"


def test_extract_cases_keeps_exact_and_drops_unexpanded_variables() -> None:
    records = [
        {
            "source": "claude_code",
            "session_id": "session-2",
            "seq": 7,
            "occurred_at": "2026-08-02T00:00:00Z",
            "tool_input_json": json.dumps(
                {
                    "command": (
                        "SELECT * FROM timeline.search_text_exact('1,441.52', 10); "
                        "SELECT * FROM timeline.search_text('$q', 10)"
                    )
                }
            ),
            "tool_result_json": _nested_result(
                [{"ref": "finance_transaction:one", "source": "finance"}]
            ),
        }
    ]

    cases = extract_cases(records)

    assert [case.query for case in cases] == ["1,441.52"]
    assert cases[0].mode == "exact"


def test_extract_cases_does_not_mix_scope_or_labels_across_two_calls() -> None:
    records = [
        {
            "source": "codex",
            "session_id": "session-3",
            "seq": 8,
            "occurred_at": "2026-08-02T00:00:00Z",
            "tool_input_json": json.dumps(
                {
                    "cmd": (
                        "SELECT * FROM timeline.search_text('first query', 10); "
                        "SELECT * FROM timeline.search_text('second query', 10, "
                        "sources => ARRAY['gmail'])"
                    )
                }
            ),
            "tool_result_json": _nested_result(
                [{"ref": "gmail_email:ambiguous", "source": "gmail"}]
            ),
        }
    ]

    cases = extract_cases(records)

    assert [case.sources for case in cases] == [(), ("gmail",)]
    assert all(not case.relevant_refs for case in cases)
    assert all(not case.relevant_keys for case in cases)
    assert all(case.relevance_provenance == "none" for case in cases)


def test_parse_search_response_handles_cli_envelope() -> None:
    response = {
        "mode": "hybrid",
        "fallback_reason": "",
        "rows": [
            {"ref": "slack_message:one", "source": "slack", "text": "hello"},
            {"ref": "gmail_email:two", "source": "gmail", "text": "world"},
        ],
    }
    parsed = parse_search_response(json.dumps(response))
    assert parsed.mode == "hybrid"
    assert parsed.refs == ("slack_message:one", "gmail_email:two")
    assert parsed.keys == ("slack_message:one", "gmail_email:two")
    assert parsed.fallback_reason == ""


def test_canonical_ref_treats_session_turn_as_the_same_session() -> None:
    assert canonical_ref("agent_session_turn:codex|session-1|42") == (
        "agent_session:codex|session-1"
    )
    assert canonical_ref("agent_session:codex|session-1") == (
        "agent_session:codex|session-1"
    )
    assert canonical_ref("slack_message:one") == "slack_message:one"


def test_evaluate_case_reports_recall_mrr_and_novelty() -> None:
    case = EvalCase(
        query_id="q1",
        query="budget approval",
        mode="keyword",
        sources=(),
        since="",
        relevant_refs=("a", "b"),
        relevant_keys=("a", "b"),
        relevance_provenance="human",
        source="codex",
        session_id="s",
        seq=1,
        occurred_at="2026-08-01T00:00:00Z",
    )
    keyword = SearchResult(mode="keyword", refs=("a", "x", "b"), keys=("a", "x", "b"))
    hybrid = SearchResult(mode="hybrid", refs=("x", "b", "new"), keys=("x", "b", "new"))

    result = evaluate_case(case, keyword=keyword, hybrid=hybrid, k=3)

    assert result["keyword_recall_at_k"] == pytest.approx(1.0)
    assert result["hybrid_recall_at_k"] == pytest.approx(0.5)
    assert result["keyword_mrr"] == pytest.approx(1.0)
    assert result["hybrid_mrr"] == pytest.approx(0.5)
    assert result["keyword_hit_at_1"] == pytest.approx(1.0)
    assert result["hybrid_hit_at_1"] == pytest.approx(0.0)
    assert result["keyword_hit_at_5"] == pytest.approx(1.0)
    assert result["hybrid_hit_at_5"] == pytest.approx(1.0)
    assert result["hybrid_novel_refs"] == ["new"]


def test_evaluate_case_does_not_claim_recall_without_judgments() -> None:
    case = EvalCase(
        query_id="q2",
        query="semantic question",
        mode="keyword",
        sources=(),
        since="",
        relevant_refs=(),
        relevant_keys=(),
        relevance_provenance="none",
        source="codex",
        session_id="s",
        seq=2,
        occurred_at="2026-08-01T00:00:00Z",
    )
    result = evaluate_case(
        case,
        keyword=SearchResult(mode="keyword", refs=("a",), keys=("a",)),
        hybrid=SearchResult(mode="hybrid", refs=("b",), keys=("b",)),
        k=10,
    )
    assert result["keyword_recall_at_k"] is None
    assert result["hybrid_recall_at_k"] is None
    assert result["keyword_hit_at_1"] is None
    assert result["hybrid_hit_at_5"] is None


def test_extract_cases_uses_source_and_time_when_historical_rows_omit_ref() -> None:
    records = [
        {
            "source": "codex",
            "session_id": "session-4",
            "seq": 9,
            "occurred_at": "2026-08-02T00:00:00Z",
            "tool_input_json": json.dumps(
                {"cmd": "pdw sql \"SELECT * FROM timeline.search_text('budget', 10)\""}
            ),
            "tool_result_json": _nested_result(
                [{"source": "slack", "occurred_at": "2026-08-01T12:00:00Z"}]
            ),
        }
    ]

    cases = extract_cases(records)

    assert cases[0].relevant_refs == ()
    assert cases[0].relevant_keys == ("event:slack|2026-08-01T12:00:00Z",)
    assert cases[0].relevance_provenance == "historical_search_results"
