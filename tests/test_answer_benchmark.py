from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from personal_data_warehouse.agent_runner import AgentRunEvent
from personal_data_warehouse.answer_benchmark import (
    EXCLUDED_ADAPTERS,
    AgentAttempt,
    AnswerBenchmarkError,
    AnswerBenchmarkRun,
    AnswerBenchmarkRunner,
    AnswerQuestion,
    ManagedAnswerAgent,
    WarehouseRefResolver,
    build_prompt,
    grade,
    load_questions,
    question_from_row,
    question_row,
    summarize,
    write_questions,
)
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.search_benchmark import PrivateOutputError, ResultRow
from tests.test_postgres_warehouse import warehouse  # noqa: F401 - fixture

T0 = datetime(2026, 10, 27, 6, 17, tzinfo=UTC)
GOLD = "gmail_email:acct|msg-1"


def _question(**overrides: Any) -> AnswerQuestion:
    raw: dict[str, Any] = {
        "question_id": "q-01",
        "question": "When did the venue confirm my booking for the 2026-09-12 dinner?",
        "original_question": "when did they confirm the dinner booking?? check my email",
        "as_of": "2026-09-10",
        "gold_answer": "September 3, 2026",
        "key_facts": [["sept?(ember)?\\.? 3", "2026-09-03", "9/3"]],
        "gold_refs": [GOLD],
        "gold_source": "gmail",
        "category": "email",
        "origin_source": "claude_code",
        "origin_session": "sess-origin",
        "origin_asked_at": "2026-09-10T14:00:00Z",
        "leak_markers": [
            "when did they confirm the dinner booking?? check my email",
            "The confirmation came from the events coordinator thread",
        ],
        "exclude_refs": ["gmail_email:acct|zrl-bot-reply"],
    }
    raw.update(overrides)
    return AnswerQuestion.from_mapping(raw)


def _command_event(index: int, item_id: str, command: str, output: str, exit_code: int = 0) -> list[AgentRunEvent]:
    started = {"type": "item.started", "item": {"id": item_id, "type": "command_execution", "command": command, "status": "in_progress"}}
    completed = {
        "type": "item.completed",
        "item": {"id": item_id, "type": "command_execution", "command": command, "status": "completed",
                 "exit_code": exit_code, "aggregated_output": output},
    }
    return [
        AgentRunEvent(index, "stdout", "item.started", started, json.dumps(started), T0),
        AgentRunEvent(index + 1, "stdout", "item.completed", completed, json.dumps(completed), T0),
    ]


def _attempt(answer: str, sources: list[str], events: list[AgentRunEvent] | None = None, **overrides: Any) -> AgentAttempt:
    values: dict[str, Any] = {
        "run_id": "agent-1", "status": "completed", "error": "",
        "final_output": {"answer": answer, "sources": sources},
        "events": events or [], "started_at": T0, "completed_at": T0 + timedelta(seconds=42),
        "provider": "codex", "model": "gpt-6.1-sol",
    }
    values.update(overrides)
    return AgentAttempt(**values)


class FakeResolver:
    def __init__(self, rows: dict[str, ResultRow] | None = None) -> None:
        self.rows = rows or {GOLD: ResultRow(ref=GOLD, source="gmail", context="thread-1", occurred_at="2026-09-03T15:00:00+00:00")}

    def resolve(self, refs):
        return {ref: self.rows[ref] for ref in refs if ref in self.rows}


# -- the question file ---------------------------------------------------------


def test_a_question_without_a_source_or_leak_markers_is_refused() -> None:
    with pytest.raises(AnswerBenchmarkError, match="gold_refs or gold_predicate"):
        _question(gold_refs=[])
    with pytest.raises(AnswerBenchmarkError, match="leak_markers is required"):
        _question(leak_markers=[])


def test_a_gold_source_may_not_be_an_agent_session() -> None:
    with pytest.raises(AnswerBenchmarkError, match="agent session"):
        _question(gold_refs=["agent_session_turn:abc"])


def test_a_leak_marker_the_agent_is_shown_would_flag_every_honest_run() -> None:
    with pytest.raises(AnswerBenchmarkError, match="appears in the question"):
        _question(leak_markers=["confirm my booking for the 2026-09-12 dinner"])


def test_bad_patterns_and_dates_are_refused() -> None:
    with pytest.raises(AnswerBenchmarkError, match="does not compile"):
        _question(key_facts=[["(unclosed"]])
    with pytest.raises(AnswerBenchmarkError, match="YYYY-MM-DD"):
        _question(as_of="last week")


def test_question_files_are_private_and_round_trip(tmp_path: Path) -> None:
    with pytest.raises(PrivateOutputError):
        write_questions(tmp_path / "questions.json", [_question()])
    path = tmp_path / ".search-eval" / "answer_questions.json"
    write_questions(path, [_question()])
    assert load_questions(path) == [_question()]


def test_the_prompt_carries_the_question_and_nothing_gold() -> None:
    question = _question()
    prompt = build_prompt(question)
    assert question.question in prompt and "pdw" in prompt
    assert "September 3" not in prompt and GOLD not in prompt
    assert all(marker not in prompt for marker in question.leak_markers)


# -- grading -------------------------------------------------------------------


def test_a_right_answer_with_the_gold_source_is_correct_and_counted() -> None:
    events = (
        _command_event(0, "c1", "pdw search 'dinner booking'", "1 hit")
        + _command_event(2, "c2", "pdw context gmail_email:acct|msg-1", "thread text")
    )
    graded = grade(_question(), _attempt("They confirmed on Sept 3.", [GOLD], events), FakeResolver())
    assert graded.correct and graded.source_correct and not graded.contaminated
    assert (graded.pdw_calls, graded.invented_calls, graded.sql_errors) == (2, 0, 0)
    assert graded.elapsed_ms == 42_000


def test_a_missing_fact_is_wrong_and_a_neighbouring_message_is_the_right_source() -> None:
    neighbour = "gmail_email:acct|msg-2"
    resolver = FakeResolver()
    resolver.rows[neighbour] = ResultRow(ref=neighbour, source="gmail", context="thread-1",
                                         occurred_at="2026-09-03T15:20:00+00:00")
    graded = grade(_question(), _attempt("Sometime in early September.", [neighbour]), resolver)
    assert not graded.correct and graded.facts_matched == 0
    assert graded.source_correct


def test_a_dropped_hit_alone_is_a_prevented_leak_not_contamination() -> None:
    attempt = _attempt("Confirmed 2026-09-03.", [GOLD], exclusion_counts={"search_rows_dropped": 3, "sql_rows_dropped": 1})
    graded = grade(_question(), attempt, FakeResolver())
    assert graded.correct and not graded.contaminated
    assert graded.proxy_drops == 4 and graded.exclusion_counts["search_rows_dropped"] == 3


def test_text_only_sql_that_reaches_the_origin_session_is_contaminated() -> None:
    # SQL selecting only text carries no id a proxy could match; the marker does.
    events = _command_event(
        0, "c1",
        "pdw sql \"SELECT text FROM marts_ai_conversations.events WHERE text ILIKE '%dinner booking%'\"",
        "text\nWhen did they confirm the dinner booking??   check my email\n",
    )
    graded = grade(_question(), _attempt("Confirmed Sept 3.", [GOLD], events), FakeResolver())
    assert graded.contaminated and not graded.correct and not graded.source_correct
    assert "leak marker" in graded.contamination_reason


def test_the_agent_phrasing_the_answer_like_the_original_is_not_copying() -> None:
    # A marker in the agent's own message (not in any tool output) is the agent's
    # wording; only something a tool returned can be copied.
    question = _question()
    said = {"type": "item.completed", "item": {"id": "m1", "type": "agent_message",
            "text": question.leak_markers[1] + " on Sept 3."}}
    events = _command_event(0, "c1", "pdw search 'dinner'", "1 hit") + [
        AgentRunEvent(2, "stdout", "item.completed", said, json.dumps(said), T0)
    ]
    graded = grade(question, _attempt("Confirmed Sept 3.", [GOLD], events), FakeResolver())
    assert not graded.contaminated and graded.correct


@pytest.mark.parametrize("cited", ["agent_session_turn:sess-origin|7", "gmail_email:acct|zrl-bot-reply"])
def test_citing_an_agent_session_or_an_excluded_copy_is_contaminated(cited: str) -> None:
    graded = grade(_question(), _attempt("Confirmed Sept 3.", [cited]), FakeResolver())
    assert graded.contaminated and not graded.correct


def test_invented_commands_and_sql_errors_are_counted() -> None:
    events = (
        _command_event(0, "c1", "pdw", "guide")
        + _command_event(2, "c2", "pdw find dinner", "unknown command", exit_code=2)
        + _command_event(4, "c3", "pdw sql 'SELECT nope'", "pdw sql: ERROR: column (SQLSTATE 42703)", exit_code=1)
        + _command_event(6, "c4", "ls -la", "")
    )
    graded = grade(_question(), _attempt("Sept 3", [GOLD], events), FakeResolver())
    assert (graded.pdw_calls, graded.invented_calls, graded.sql_errors) == (3, 1, 1)


def test_an_errored_run_is_wrong_and_unanswered() -> None:
    graded = grade(_question(), _attempt("Sept 3", [GOLD], status="error", error="timed out"), FakeResolver())
    assert not graded.correct and not graded.answered and not graded.source_correct


def test_summary_counts_every_question_including_errors() -> None:
    q = _question()
    graded = [
        grade(q, _attempt("Sept 3", [GOLD]), FakeResolver()),
        grade(q, _attempt("Sept 3", [GOLD], exclusion_counts={"search_rows_dropped": 2}), FakeResolver()),
        grade(q, _attempt("", [], status="error", completed_at=T0 + timedelta(seconds=600)), FakeResolver()),
    ]
    summary = summarize(graded, run_id="r", provider="codex", model="m", started_at=T0)
    assert (summary.questions, summary.answered, summary.correct, summary.errors) == (3, 2, 2, 1)
    assert (summary.proxy_drops, summary.runs_with_proxy_drops) == (2, 1)
    assert summary.elapsed_p50_ms == 42_000 and summary.elapsed_p90_ms == 600_000
    assert summary.under_call_target == 3


# -- the managed agent -----------------------------------------------------------


def test_managed_agent_hides_agent_sessions_and_the_questions_copies() -> None:
    calls: list[dict[str, Any]] = []

    class FakeRunner:
        def run_with_pdw(self, request, **kwargs):
            calls.append({"request": request, **kwargs})
            return SimpleNamespace(
                run_id="agent-9", status="completed", error="", final_output_json={"answer": "x", "sources": []},
                events=[], started_at=T0, completed_at=T0, provider="codex", model="gpt-6.1-sol",
                exclusion_counts={"context_calls_refused": 1},
            )

    question = _question()
    attempt = ManagedAnswerAgent(runner=FakeRunner(), provider="codex", model="gpt-6.1-sol").ask(question)
    (call,) = calls
    assert call["exclude_adapters"] == EXCLUDED_ADAPTERS
    assert call["exclude_refs"] == tuple(question.exclude_refs)
    assert call["request"].task_type == "answer_benchmark" and call["request"].model == "gpt-6.1-sol"
    assert GOLD not in call["request"].prompt
    assert attempt.exclusion_counts == {"context_calls_refused": 1}


# -- warehouse -----------------------------------------------------------------


def test_questions_round_trip_through_the_private_table(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_pipeline_health_tables()
    one, two = _question(), _question(question_id="q-02", origin_asked_at="")
    now = datetime.now(tz=UTC)
    assert warehouse.publish_answer_benchmark_questions([question_row(q, updated_at=now) for q in (one, two)]) == 2
    loaded = [question_from_row(row) for row in warehouse.load_answer_benchmark_questions()]
    assert [q.question_id for q in loaded] == ["q-01", "q-02"]
    assert loaded[0].key_facts == one.key_facts and loaded[0].leak_markers == one.leak_markers
    assert loaded[0].as_of == "2026-09-10" and loaded[1].origin_asked_at == ""
    warehouse.publish_answer_benchmark_questions([question_row(one, updated_at=now)])
    assert [row["question_id"] for row in warehouse.load_answer_benchmark_questions()] == ["q-01"]


def test_a_run_writes_private_results_and_a_mart_row(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_pipeline_health_tables()
    class Agent:
        def ask(self, question):
            return _attempt("Confirmed Sept 3.", [GOLD])

    summary, graded = AnswerBenchmarkRunner(
        agent=Agent(), resolver=FakeResolver(), provider="codex", model="gpt-6.1-sol", warehouse=warehouse,
    ).run([_question(), _question(question_id="q-02"), _question(question_id="q-03", active=False)])
    assert summary.questions == 2 and summary.correct == 2
    stored = warehouse._query_dicts("SELECT question_id, correct, answer_text FROM @answer_benchmark_results ORDER BY 1")
    assert [(r["question_id"], r["correct"]) for r in stored] == [("q-01", 1), ("q-02", 1)]
    row = warehouse._query_dicts("SELECT * FROM @marts_answer_benchmark")[0]
    assert row["status"] == "ok" and row["correct_rate"] == pytest.approx(1)
    assert row["model"] == "gpt-6.1-sol" and row["history_runs"] == 1
    # The public mart carries counts only: no question or answer text.
    assert not any("Sept" in str(value) or "dinner" in str(value) for value in row.values())


def _run(**overrides: Any):
    values: dict[str, Any] = dict(
        run_id="answer-1", provider="codex", model="gpt-6.1-sol", prompt_version="v1", questions=50,
        answered=50, correct=46, source_correct=44, contaminated=0, errors=0, invented_calls=0,
        sql_errors=0, proxy_drops=0, runs_with_proxy_drops=0, elapsed_p50_ms=40_000,
        elapsed_p90_ms=90_000, pdw_calls_p50=4, under_call_target=40, note="", started_at=T0,
    )
    values.update(overrides)
    return AnswerBenchmarkRun(**values)


def _status(warehouse: PostgresWarehouse) -> str:
    return warehouse._query_dicts("SELECT status FROM @marts_answer_benchmark")[0]["status"]


def test_the_mart_reads_no_data_until_a_run_exists(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_pipeline_health_tables()
    assert _status(warehouse) == "no_data"


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, "ok"),
        ({"correct": 44}, "attention"),
        ({"elapsed_p50_ms": 61_000}, "attention"),
        ({"pdw_calls_p50": 6}, "attention"),
        ({"contaminated": 1}, "attention"),
        ({"errors": 1, "answered": 49}, "attention"),
        ({"proxy_drops": 30, "runs_with_proxy_drops": 20}, "ok"),
        ({"correct": 34}, "failing"),
        ({"answered": 0, "correct": 0, "errors": 50}, "failing"),
        ({"questions": 0, "answered": 0, "correct": 0}, "no_data"),
    ],
)
def test_the_mart_judges_the_latest_run_against_the_targets(
    warehouse: PostgresWarehouse, overrides: dict[str, Any], expected: str
) -> None:
    warehouse.ensure_pipeline_health_tables()
    warehouse.write_answer_benchmark_run(_run(**overrides), [], collected_at=datetime.now(tz=UTC))
    assert _status(warehouse) == expected


def test_the_mart_shows_the_trend_and_goes_stale(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_pipeline_health_tables()
    now = datetime.now(tz=UTC)
    warehouse.write_answer_benchmark_run(_run(run_id="a", correct=40, started_at=now - timedelta(days=8)), [],
                                         collected_at=now - timedelta(days=7))
    warehouse.write_answer_benchmark_run(_run(run_id="b", correct=46), [], collected_at=now)
    row = warehouse._query_dicts("SELECT * FROM @marts_answer_benchmark")[0]
    assert row["run_id"] == "b" and row["history_runs"] == 2
    assert float(row["correct_rate_change"]) == pytest.approx(0.12)
    warehouse._command("UPDATE @answer_benchmark_runs SET collected_at = collected_at - interval '12 days'")
    assert _status(warehouse) == "no_data"


def test_warehouse_resolver_reads_timeline_refs() -> None:
    calls: list[str] = []

    class Stub:
        def query_timeline_refs(self, sql: str):
            calls.append(sql)
            return [{"ref": GOLD, "source": "gmail", "context": "t", "occurred_at": T0, "title": "x", "snippet": "y"}]

    resolved = WarehouseRefResolver(Stub()).resolve([GOLD, "bad-ref", GOLD])
    assert resolved[GOLD].source == "gmail" and resolved[GOLD].timestamp == T0
    assert "('gmail_email','acct|msg-1')" in calls[0] and "bad-ref" not in calls[0]
