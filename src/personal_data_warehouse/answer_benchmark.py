"""Answer benchmark (contract C16): does an agent with only PDW answer Zach right?

The other contracts measure the machine -- adapters current, search ranking,
latency. Nothing measured the thing they exist for: whether an agent asked a
real question gets the right answer. This harness asks a fixed set of real
questions Zach asked agents before, each with a gold answer checked at its
source, to a fresh agent whose only tool is the ``pdw`` CLI, and grades four
things per question: the answer, the cited source, the PDW calls it took and
the wall-clock time.

Privacy: questions, gold answers and the agent's answers are Zach's private
data. The editing file lives under the gitignored ``.search-eval/`` (enforced
by the search benchmark's ``assert_private_path``), the warehouse copy in
``private.answer_benchmark_questions``, per-question results in
``private.answer_benchmark_results``. Only counts reach
``ops.answer_benchmark_runs`` and ``marts_ops.answer_benchmark``. Runs never
write ``ops.ai_processing_agent_runs``, so nothing reaches the timeline.

Copying: every question came from a past agent session that holds its answer.
The run asks the tool proxy to hide agent-session hits and each question's
``exclude_refs`` (a dropped row is a prevented leak and is only counted). A
run is *contaminated* -- and its answer graded wrong -- only when a leak marker
(Zach's original wording, or a sentence of the original agent's answer that the
gold source does not contain) shows up in anything the agent read, or when it
cites an agent session or an excluded ref. That catches SQL that returns only
text, which no proxy cell filter can recognise.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import uuid
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence

from personal_data_warehouse.agent_runner import (
    AgentRunEvent,
    AgentRunRequest,
    extract_tool_arguments,
    extract_tool_name,
    extract_tool_result,
)
from personal_data_warehouse.search_benchmark import (
    ResultRow,
    assert_private_path,
    first_relevant_rank,
    parse_timestamp,
)

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

logger = logging.getLogger(__name__)

DEFAULT_QUESTIONS = Path(".search-eval/answer_questions.json")
DEFAULT_REPORT_DIR = Path(".search-eval/answer_reports")
PROMPT_VERSION = "answer-benchmark-v1"
TASK_TYPE = "answer_benchmark"
#: Every question came from an agent session, and a gold source is never one.
#: Muse's memory files are agent-written notes that restate past answers
#: (found 2026-10-28 while building the first question set), so they go too.
EXCLUDED_ADAPTERS = ("agent_session", "agent_session_turn", "muse_file")
#: One question at a time; a question that takes ten minutes has failed anyway.
DEFAULT_QUESTION_TIMEOUT_SECONDS = 600

#: C16's proposed targets (vision 2026-10-27, accepted by Zach 2026-10-27).
CORRECT_TARGET = 0.90
MEDIAN_SECONDS_TARGET = 60
#: "under 6 PDW calls": the median run makes at most five.
PDW_CALLS_TARGET = 6
#: Below this share correct the contract is failing, not merely short.
CORRECT_FAILING_FLOOR = 0.70
#: The run is weekly; a row older than this reads no_data.
STALE_DAYS = 10
MIN_LEAK_MARKER_CHARS = 20
MAX_TRACE_CHARS = 4_000

ANSWER_SCHEMA: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["answer", "sources"],
    "properties": {
        "answer": {"type": "string"},
        "sources": {"type": "array", "items": {"type": "string"}},
    },
}

PROMPT_FRAME = """\
Zach asks you a question about his own data. Answer it from his personal data
warehouse using the `pdw` CLI in your shell; run bare `pdw` for its guide.

Question: {question}

Reply with JSON only: {{"answer": "<a short, direct answer>", "sources":
["<the ref of each warehouse record your answer comes from, as pdw prints it,
e.g. gmail_email:...>"]}}. If you cannot find it, say so in `answer` and leave
`sources` empty.
"""


class AnswerBenchmarkError(ValueError):
    """A question file entry that cannot be graded as written."""


@dataclass(frozen=True)
class AnswerQuestion:
    question_id: str
    question: str
    gold_answer: str
    key_facts: tuple[tuple[str, ...], ...]
    gold_refs: tuple[str, ...] = ()
    gold_predicate: Mapping[str, Any] | None = None
    gold_source: str = ""
    category: str = ""
    as_of: str = ""
    origin_source: str = ""
    origin_session: str = ""
    origin_asked_at: str = ""
    checked_evidence: str = ""
    leak_markers: tuple[str, ...] = ()
    exclude_refs: tuple[str, ...] = ()
    verification_note: str = ""
    original_question: str = ""
    active: bool = True

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> AnswerQuestion:
        def strings(value: Any) -> tuple[str, ...]:
            return tuple(str(item) for item in (value or ()) if str(item).strip())

        question = cls(
            question_id=str(raw.get("question_id") or "").strip(),
            question=str(raw.get("question") or "").strip(),
            gold_answer=str(raw.get("gold_answer") or "").strip(),
            key_facts=tuple(strings(fact) for fact in raw.get("key_facts") or ()),
            gold_refs=strings(raw.get("gold_refs")),
            gold_predicate=raw.get("gold_predicate") or None,
            gold_source=str(raw.get("gold_source") or ""),
            category=str(raw.get("category") or ""),
            as_of=str(raw.get("as_of") or ""),
            origin_source=str(raw.get("origin_source") or ""),
            origin_session=str(raw.get("origin_session") or ""),
            origin_asked_at=str(raw.get("origin_asked_at") or ""),
            checked_evidence=str(raw.get("checked_evidence") or ""),
            leak_markers=strings(raw.get("leak_markers")),
            exclude_refs=strings(raw.get("exclude_refs")),
            verification_note=str(raw.get("verification_note") or ""),
            original_question=str(raw.get("original_question") or ""),
            active=bool(raw.get("active", True)),
        )
        question.validate()
        return question

    def validate(self) -> None:
        """Refuse an entry that would grade wrongly rather than score it."""

        name = self.question_id or "<no question_id>"
        problems = []
        if not self.question_id:
            problems.append("question_id is empty")
        if not self.question or not self.gold_answer:
            problems.append("question and gold_answer are required")
        try:
            date.fromisoformat(self.as_of)
        except ValueError:
            problems.append(f"as_of {self.as_of!r} is not a YYYY-MM-DD date")
        if not self.key_facts or any(not fact for fact in self.key_facts):
            problems.append("key_facts needs at least one fact, each with at least one pattern")
        for fact in self.key_facts:
            for pattern in fact:
                try:
                    re.compile(pattern)
                except re.error as error:
                    problems.append(f"key_facts pattern {pattern!r} does not compile: {error}")
        if not self.gold_refs and not self.gold_predicate:
            problems.append("gold_refs or gold_predicate is required (an unsourced answer cannot be graded)")
        for ref in (*self.gold_refs, *self.exclude_refs):
            if ":" not in ref:
                problems.append(f"{ref!r} is not a timeline ref (adapter:event_id)")
        for ref in self.gold_refs:
            if _ref_adapter(ref) in EXCLUDED_ADAPTERS:
                problems.append(f"gold ref {ref!r} is an agent session; the gold source must be the record itself")
        if not self.leak_markers:
            problems.append("leak_markers is required: copying from the origin session must be detectable")
        visible = _normalize(f"{self.question}\n{self.gold_answer}")
        for marker in self.leak_markers:
            if len(marker) < MIN_LEAK_MARKER_CHARS:
                problems.append(f"leak marker {marker[:20]!r}... is under {MIN_LEAK_MARKER_CHARS} chars")
            elif _normalize(marker) in visible:
                # The agent is shown the question, and a correct answer may echo
                # the gold one; a marker inside either flags every honest run.
                problems.append(f"leak marker {marker[:30]!r}... appears in the question or gold answer")
        if problems:
            raise AnswerBenchmarkError(f"{name}: " + "; ".join(problems))

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["key_facts"] = [list(fact) for fact in self.key_facts]
        for key in ("gold_refs", "leak_markers", "exclude_refs"):
            record[key] = list(record[key])
        record["gold_predicate"] = dict(self.gold_predicate) if self.gold_predicate else None
        return record


def load_questions(path: Path) -> list[AnswerQuestion]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise AnswerBenchmarkError(f"{path} must hold a JSON list of questions")
    questions = [AnswerQuestion.from_mapping(item) for item in raw]
    seen: set[str] = set()
    for question in questions:
        if question.question_id in seen:
            raise AnswerBenchmarkError(f"duplicate question_id {question.question_id}")
        seen.add(question.question_id)
    return questions


def write_questions(path: Path, questions: Sequence[AnswerQuestion]) -> None:
    assert_private_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [question.to_record() for question in questions]
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def question_row(question: AnswerQuestion, *, updated_at: datetime) -> dict[str, Any]:
    """One private.answer_benchmark_questions row."""

    return {
        "question_id": question.question_id,
        "question": question.question,
        "original_question": question.original_question,
        "gold_answer": question.gold_answer,
        "key_facts_json": json.dumps([list(fact) for fact in question.key_facts], ensure_ascii=False),
        "gold_refs_json": json.dumps(list(question.gold_refs)),
        "gold_predicate_json": json.dumps(question.gold_predicate) if question.gold_predicate else "",
        "gold_source": question.gold_source,
        "category": question.category,
        "as_of": date.fromisoformat(question.as_of),
        "origin_source": question.origin_source,
        "origin_session": question.origin_session,
        # Absence is the epoch, as everywhere in the warehouse.
        "origin_asked_at": parse_timestamp(question.origin_asked_at) or EPOCH,
        "checked_evidence": question.checked_evidence,
        "leak_markers_json": json.dumps(list(question.leak_markers), ensure_ascii=False),
        "exclude_refs_json": json.dumps(list(question.exclude_refs)),
        "verification_note": question.verification_note,
        "is_active": 1 if question.active else 0,
        "updated_at": updated_at,
        "sync_version": int(updated_at.timestamp()),
    }


def question_from_row(row: Mapping[str, Any]) -> AnswerQuestion:
    def loads(key: str, default: Any) -> Any:
        value = row.get(key)
        return json.loads(value) if value else default

    as_of = row.get("as_of")
    asked = row.get("origin_asked_at")
    if isinstance(asked, datetime) and asked == EPOCH:
        asked = ""
    return AnswerQuestion.from_mapping(
        {
            "question_id": row.get("question_id"),
            "question": row.get("question"),
            "original_question": row.get("original_question"),
            "gold_answer": row.get("gold_answer"),
            "key_facts": loads("key_facts_json", []),
            "gold_refs": loads("gold_refs_json", []),
            "gold_predicate": loads("gold_predicate_json", None),
            "gold_source": row.get("gold_source"),
            "category": row.get("category"),
            "as_of": as_of.isoformat() if isinstance(as_of, (date, datetime)) else as_of,
            "origin_source": row.get("origin_source"),
            "origin_session": row.get("origin_session"),
            "origin_asked_at": asked.isoformat() if isinstance(asked, datetime) else asked,
            "checked_evidence": row.get("checked_evidence"),
            "leak_markers": loads("leak_markers_json", []),
            "exclude_refs": loads("exclude_refs_json", []),
            "verification_note": row.get("verification_note"),
            "active": bool(row.get("is_active", 1)),
        }
    )


# -- the agent ---------------------------------------------------------------


@dataclass(frozen=True)
class AgentAttempt:
    """What one fresh agent did with one question."""

    run_id: str
    status: str
    error: str
    final_output: Mapping[str, Any]
    events: Sequence[AgentRunEvent]
    started_at: datetime
    completed_at: datetime
    provider: str = ""
    model: str = ""
    #: Rows and calls the tool proxy withheld (search/context/sql rows dropped,
    #: context calls refused). Information only: a drop is a prevented leak.
    exclusion_counts: Mapping[str, int] = field(default_factory=dict)


class AnswerAgent(Protocol):
    def ask(self, question: AnswerQuestion) -> AgentAttempt: ...


def build_prompt(question: AnswerQuestion) -> str:
    return PROMPT_FRAME.format(question=question.question)


class ManagedAnswerAgent:
    """A fresh managed-container agent per question, with only ``pdw``.

    ``runner`` is a ``ContainerAgentRunner`` (or anything with its
    ``run_with_pdw``); the tool proxy hides agent sessions and the question's
    ``exclude_refs`` and reports what it withheld in ``exclusion_counts``.
    """

    def __init__(self, *, runner: Any, provider: str, model: str, app: Any = None) -> None:
        self._runner = runner
        self._provider = provider
        self._model = model
        self._app = app

    def ask(self, question: AnswerQuestion) -> AgentAttempt:
        request = AgentRunRequest(
            prompt=build_prompt(question),
            schema=ANSWER_SCHEMA,
            task_type=TASK_TYPE,
            subject_id=question.question_id,
            prompt_version=PROMPT_VERSION,
            provider=self._provider,
            model=self._model,
        )
        result = self._runner.run_with_pdw(
            request,
            app=self._app,
            exclude_adapters=EXCLUDED_ADAPTERS,
            exclude_refs=tuple(question.exclude_refs),
        )
        return AgentAttempt(
            run_id=result.run_id,
            status=result.status,
            error=result.error,
            final_output=result.final_output_json,
            events=result.events,
            started_at=result.started_at,
            completed_at=result.completed_at,
            provider=result.provider,
            model=result.model,
            exclusion_counts=dict(getattr(result, "exclusion_counts", None) or {}),
        )


# -- grading -----------------------------------------------------------------


@dataclass(frozen=True)
class ToolCall:
    name: str
    command: str
    output: str
    exit_code: Any = None


def tool_calls(events: Sequence[AgentRunEvent]) -> list[ToolCall]:
    """One entry per tool call, with its output, for either provider.

    A codex command arrives as ``item.started`` then ``item.completed`` under one
    item id, so calls are keyed by that id and the completed event's output wins.
    """

    calls: dict[str, ToolCall] = {}
    for event in events:
        payload = event.event_json
        name = extract_tool_name(payload)
        if not name:
            continue
        item = payload.get("item")
        call_id = ""
        if isinstance(item, Mapping):
            call_id = str(item.get("id") or "")
        call_id = call_id or str(payload.get("tool_use_id") or payload.get("id") or "") or f"event-{event.event_index}"
        arguments = extract_tool_arguments(payload)
        result = extract_tool_result(payload)
        previous = calls.get(call_id)
        output = str(result.get("output") or result.get("value") or "") if result else ""
        calls[call_id] = ToolCall(
            name=name,
            command=str(arguments.get("command") or (previous.command if previous else "")),
            output=output or (previous.output if previous else ""),
            exit_code=result.get("exit_code") if result else (previous.exit_code if previous else None),
        )
    return list(calls.values())


def is_pdw_call(call: ToolCall) -> bool:
    return call.name == "pdw" or call.name.startswith("pdw ")


def is_invented_call(call: ToolCall) -> bool:
    """`pdw <word>` where the word is no subcommand: the CLI answers `unknown command`."""

    if call.name != "pdw":
        return False
    tokens = call.command.replace("'", " ").replace('"', " ").split()
    for index, token in enumerate(tokens):
        if token == "pdw" or token.endswith("/pdw"):
            rest = [item for item in tokens[index + 1 :] if not item.startswith("-")]
            return bool(rest) and rest[0] not in {"|", "&&", ";", "2>&1", ">"}
    return False


def is_sql_error(call: ToolCall) -> bool:
    if call.name != "pdw sql":
        return False
    failed = call.exit_code not in (None, 0, "0")
    return failed or "pdw sql: ERROR" in call.output or "SQLSTATE" in call.output


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.casefold()).strip()


def _strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _strings(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _strings(item)


def leaked_markers(question: AnswerQuestion, events: Sequence[AgentRunEvent]) -> list[str]:
    """The leak markers that appear anywhere in what the agent saw or wrote."""

    if not question.leak_markers:
        return []
    haystack = _normalize("\n".join(text for event in events for text in _strings(event.event_json)))
    return [marker for marker in question.leak_markers if _normalize(marker) in haystack]


def _ref_adapter(ref: str) -> str:
    return ref.split(":", 1)[0].strip()


def facts_matched(question: AnswerQuestion, answer: str) -> int:
    return sum(
        1
        for fact in question.key_facts
        if any(re.search(pattern, answer, re.IGNORECASE) for pattern in fact)
    )


class RefResolver(Protocol):
    def resolve(self, refs: Sequence[str]) -> dict[str, ResultRow]: ...


@dataclass(frozen=True)
class GradedAnswer:
    question_id: str
    agent_run_id: str
    provider: str
    model: str
    status: str
    error: str
    answer_text: str
    cited_refs: tuple[str, ...]
    facts_matched: int
    facts_total: int
    correct: bool
    source_correct: bool
    contaminated: bool
    contamination_reason: str
    pdw_calls: int
    invented_calls: int
    sql_errors: int
    proxy_drops: int
    exclusion_counts: Mapping[str, int]
    elapsed_ms: int
    tool_trace: tuple[str, ...]
    started_at: datetime
    completed_at: datetime

    @property
    def answered(self) -> bool:
        return self.status == "completed"


def grade(question: AnswerQuestion, attempt: AgentAttempt, resolver: RefResolver) -> GradedAnswer:
    output = attempt.final_output or {}
    answer = str(output.get("answer") or "").strip()
    cited = tuple(dict.fromkeys(str(ref).strip() for ref in output.get("sources") or () if str(ref).strip()))
    calls = tool_calls(attempt.events)
    pdw = [call for call in calls if is_pdw_call(call)]

    reasons: list[str] = []
    markers = leaked_markers(question, attempt.events)
    if markers:
        reasons.append(f"{len(markers)} leak marker(s) in the agent's trace")
    excluded = set(question.exclude_refs)
    bad_cites = [ref for ref in cited if _ref_adapter(ref) in EXCLUDED_ADAPTERS or ref in excluded]
    if bad_cites:
        reasons.append(f"cited {len(bad_cites)} agent-session or excluded ref(s)")
    contaminated = bool(reasons)

    matched = facts_matched(question, answer)
    answered = attempt.status == "completed"
    correct = answered and not contaminated and matched == len(question.key_facts)

    source_correct = False
    if answered and cited and not contaminated:
        lookup = [*cited, *question.gold_refs]
        resolved = resolver.resolve(lookup) if lookup else {}
        rows = [resolved.get(ref) or ResultRow(ref=ref) for ref in cited]
        truth_meta = [
            (row.source, row.context, row.timestamp)
            for ref in question.gold_refs
            if (row := resolved.get(ref)) is not None and row.timestamp is not None
        ]
        source_correct = (
            first_relevant_rank(rows, set(question.gold_refs), truth_meta, dict(question.gold_predicate or {}) or None)
            is not None
        )

    counts = {str(key): int(value) for key, value in (attempt.exclusion_counts or {}).items()}
    trace = tuple(call.command[:300] for call in pdw)
    return GradedAnswer(
        question_id=question.question_id,
        agent_run_id=attempt.run_id,
        provider=attempt.provider,
        model=attempt.model,
        status=attempt.status,
        error=attempt.error[:1000],
        answer_text=answer[:4000],
        cited_refs=cited,
        facts_matched=matched,
        facts_total=len(question.key_facts),
        correct=correct,
        source_correct=source_correct,
        contaminated=contaminated,
        contamination_reason="; ".join(reasons),
        pdw_calls=len(pdw),
        invented_calls=sum(1 for call in pdw if is_invented_call(call)),
        sql_errors=sum(1 for call in pdw if is_sql_error(call)),
        proxy_drops=sum(counts.values()),
        exclusion_counts=counts,
        elapsed_ms=max(0, int((attempt.completed_at - attempt.started_at).total_seconds() * 1000)),
        tool_trace=trace,
        started_at=attempt.started_at,
        completed_at=attempt.completed_at,
    )


# -- the run -----------------------------------------------------------------


def _percentile(values: Sequence[int], pct: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((pct / 100) * (len(ordered) - 1))))
    return int(ordered[index])


@dataclass(frozen=True)
class AnswerBenchmarkRun:
    """One run's aggregates: the ops.answer_benchmark_runs row. No question text."""

    run_id: str
    provider: str
    model: str
    prompt_version: str
    questions: int
    answered: int
    correct: int
    source_correct: int
    contaminated: int
    errors: int
    invented_calls: int
    sql_errors: int
    proxy_drops: int
    runs_with_proxy_drops: int
    elapsed_p50_ms: int
    elapsed_p90_ms: int
    pdw_calls_p50: int
    under_call_target: int
    note: str
    started_at: datetime


def summarize(
    graded: Sequence[GradedAnswer], *, run_id: str, provider: str, model: str, started_at: datetime, note: str = ""
) -> AnswerBenchmarkRun:
    answered = [item for item in graded if item.answered]
    return AnswerBenchmarkRun(
        run_id=run_id,
        provider=provider,
        model=model,
        prompt_version=PROMPT_VERSION,
        questions=len(graded),
        answered=len(answered),
        correct=sum(1 for item in graded if item.correct),
        source_correct=sum(1 for item in graded if item.source_correct),
        contaminated=sum(1 for item in graded if item.contaminated),
        errors=len(graded) - len(answered),
        invented_calls=sum(item.invented_calls for item in graded),
        sql_errors=sum(item.sql_errors for item in graded),
        proxy_drops=sum(item.proxy_drops for item in graded),
        runs_with_proxy_drops=sum(1 for item in graded if item.proxy_drops),
        # Time and calls over every question asked: a question that timed out
        # took its whole timeout, and leaving it out would flatter the median.
        elapsed_p50_ms=_percentile([item.elapsed_ms for item in graded], 50),
        elapsed_p90_ms=_percentile([item.elapsed_ms for item in graded], 90),
        pdw_calls_p50=_percentile([item.pdw_calls for item in graded], 50),
        under_call_target=sum(1 for item in graded if item.pdw_calls < PDW_CALLS_TARGET),
        note=note,
        started_at=started_at,
    )


def result_row(graded: GradedAnswer, *, run_id: str) -> dict[str, Any]:
    """One private.answer_benchmark_results row."""

    return {
        "run_id": run_id,
        "question_id": graded.question_id,
        "agent_run_id": graded.agent_run_id,
        "provider": graded.provider,
        "model": graded.model,
        "prompt_version": PROMPT_VERSION,
        "status": graded.status,
        "error": graded.error,
        "answer_text": graded.answer_text,
        "cited_refs_json": json.dumps(list(graded.cited_refs)),
        "facts_matched": graded.facts_matched,
        "facts_total": graded.facts_total,
        "correct": int(graded.correct),
        "source_correct": int(graded.source_correct),
        "contaminated": int(graded.contaminated),
        "contamination_reason": graded.contamination_reason,
        "pdw_calls": graded.pdw_calls,
        "invented_calls": graded.invented_calls,
        "sql_errors": graded.sql_errors,
        "proxy_drops": graded.proxy_drops,
        "exclusion_counts_json": json.dumps(dict(graded.exclusion_counts), sort_keys=True),
        "elapsed_ms": graded.elapsed_ms,
        "tool_trace_json": json.dumps(list(graded.tool_trace), ensure_ascii=False)[:MAX_TRACE_CHARS],
        "started_at": graded.started_at,
        "completed_at": graded.completed_at,
        "sync_version": int(graded.completed_at.timestamp() * 1_000_000),
    }


class AnswerBenchmarkRunner:
    """Ask every active question, one at a time, grade, and optionally persist."""

    def __init__(
        self,
        *,
        agent: AnswerAgent,
        resolver: RefResolver,
        provider: str,
        model: str,
        warehouse: Any = None,
        logger_: Any = None,
        clock: Any = None,
    ) -> None:
        self._agent = agent
        self._resolver = resolver
        self._provider = provider
        self._model = model
        self._warehouse = warehouse
        self._log = logger_ or logger
        self._clock = clock or (lambda: datetime.now(tz=UTC))

    def run(
        self, questions: Sequence[AnswerQuestion], *, limit: int | None = None
    ) -> tuple[AnswerBenchmarkRun, list[GradedAnswer]]:
        active = [question for question in questions if question.active]
        if limit is not None:
            active = active[:limit]
        run_id = f"answer-{uuid.uuid4().hex}"
        started_at = self._clock()
        graded: list[GradedAnswer] = []
        for index, question in enumerate(active, start=1):
            try:
                attempt = self._agent.ask(question)
            except Exception as error:  # noqa: BLE001 - one broken question must not end the run
                now = self._clock()
                attempt = AgentAttempt(
                    run_id="", status="error", error=f"{error.__class__.__name__}: {error}",
                    final_output={}, events=(), started_at=now, completed_at=now,
                    provider=self._provider, model=self._model,
                )
            item = grade(question, attempt, self._resolver)
            graded.append(item)
            self._log.info(
                "answer benchmark %d/%d %s: %s%s, %d pdw calls, %.0fs",
                index, len(active), question.question_id,
                "correct" if item.correct else "wrong",
                " (contaminated)" if item.contaminated else "",
                item.pdw_calls, item.elapsed_ms / 1000,
            )
        note = "" if active else "no active questions in private.answer_benchmark_questions"
        summary = summarize(
            graded, run_id=run_id, provider=self._provider, model=self._model,
            started_at=started_at, note=note,
        )
        if self._warehouse is not None:
            self._warehouse.write_answer_benchmark_run(
                summary, [result_row(item, run_id=run_id) for item in graded], collected_at=self._clock()
            )
        return summary, graded


# -- ref resolution ------------------------------------------------------------


def _resolve_sql(refs: Sequence[str]) -> str:
    pairs = [ref.split(":", 1) for ref in dict.fromkeys(refs) if ":" in ref]
    if not pairs:
        return ""
    values = ",".join(
        "(" + ",".join("'" + part.replace("'", "''") + "'" for part in pair) + ")" for pair in pairs
    )
    return (
        f"WITH want(adapter, event_id) AS (VALUES {values}) "
        "SELECT e.adapter || ':' || e.event_id AS ref, e.source, coalesce(e.context, '') AS context, "
        "e.occurred_at, coalesce(e.title, '') AS title, coalesce(e.snippet, '') AS snippet "
        "FROM timeline.events e JOIN want w ON w.adapter = e.adapter AND w.event_id = e.event_id"
    )


def _rows_to_result_rows(rows: Iterable[Mapping[str, Any]]) -> dict[str, ResultRow]:
    out: dict[str, ResultRow] = {}
    for row in rows:
        stamp = row.get("occurred_at")
        out[str(row["ref"])] = ResultRow(
            ref=str(row["ref"]),
            source=str(row.get("source") or ""),
            context=str(row.get("context") or ""),
            occurred_at=stamp.isoformat() if isinstance(stamp, datetime) else str(stamp or ""),
            title=str(row.get("title") or ""),
            text=str(row.get("snippet") or ""),
        )
    return out


class WarehouseRefResolver:
    """Resolve refs against timeline.events through the warehouse connection."""

    def __init__(self, warehouse: Any) -> None:
        self._warehouse = warehouse

    def resolve(self, refs: Sequence[str]) -> dict[str, ResultRow]:
        sql = _resolve_sql(refs)
        return _rows_to_result_rows(self._warehouse.query_timeline_refs(sql)) if sql else {}


class CliRefResolver:
    """Resolve refs read-only through ``pdw sql``: a run that writes nothing."""

    def resolve(self, refs: Sequence[str]) -> dict[str, ResultRow]:
        from personal_data_warehouse.search_benchmark import _pdw_json

        sql = _resolve_sql(refs)
        if not sql:
            return {}
        rows = _pdw_json(
            ["sql", "--output", "json", "-q", "resolve answer benchmark refs", sql], timeout=180
        )
        return _rows_to_result_rows(rows if isinstance(rows, list) else [])


# -- CLI ---------------------------------------------------------------------


def _warehouse():
    from personal_data_warehouse.config import load_settings
    from personal_data_warehouse.warehouse import warehouse_from_settings

    return warehouse_from_settings(load_settings(require_gmail=False))


def _questions_command(args: argparse.Namespace) -> int:
    warehouse = _warehouse()
    try:
        warehouse.ensure_pipeline_health_tables()
        if args.command == "publish":
            questions = load_questions(args.questions)
            count = warehouse.publish_answer_benchmark_questions(
                [question_row(question, updated_at=datetime.now(tz=UTC)) for question in questions]
            )
            print(f"published {count} questions to private.answer_benchmark_questions")
            return 0
        questions = [question_from_row(row) for row in warehouse.load_answer_benchmark_questions()]
        write_questions(args.questions, questions)
        print(f"wrote {len(questions)} questions to {args.questions}")
        return 0
    finally:
        warehouse.close()


def _run_command(args: argparse.Namespace) -> int:
    from personal_data_warehouse.agent_runner import ContainerAgentRunner, agent_config_from_env

    config = replace(agent_config_from_env(), timeout_seconds=args.timeout)
    provider = args.provider or config.provider
    model = args.model or config.model
    agent = ManagedAnswerAgent(runner=ContainerAgentRunner(config), provider=provider, model=model)
    warehouse = _warehouse() if args.write else None
    try:
        if warehouse is not None:
            warehouse.ensure_pipeline_health_tables()
            questions = [question_from_row(row) for row in warehouse.load_answer_benchmark_questions()]
            resolver: RefResolver = WarehouseRefResolver(warehouse)
        else:
            questions = load_questions(args.questions)
            resolver = CliRefResolver()
        if args.only:
            wanted = set(args.only)
            questions = [question for question in questions if question.question_id in wanted]
        summary, graded = AnswerBenchmarkRunner(
            agent=agent, resolver=resolver, provider=provider, model=model, warehouse=warehouse,
        ).run(questions, limit=args.limit)
    finally:
        if warehouse is not None:
            warehouse.close()
    report = {
        "summary": {**asdict(summary), "started_at": summary.started_at.isoformat()},
        "results": [result_row(item, run_id=summary.run_id) for item in graded],
    }
    path = assert_private_path(args.report_dir / f"{summary.run_id}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, default=str, ensure_ascii=False) + "\n", encoding="utf-8")
    print(
        f"{summary.correct}/{summary.questions} correct, {summary.source_correct} with the right source, "
        f"{summary.contaminated} contaminated, median {summary.elapsed_p50_ms / 1000:.0f}s, "
        f"median {summary.pdw_calls_p50} pdw calls; "
        + ("written to the warehouse" if args.write else "not written (pass --write)")
        + f"; per-question report {path}"
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Answer benchmark (contract C16)")
    sub = parser.add_subparsers(dest="command", required=True)
    for name, text in (
        ("publish", "store the private question file in private.answer_benchmark_questions"),
        ("pull", "export private.answer_benchmark_questions to the private question file"),
    ):
        command = sub.add_parser(name, help=text)
        command.add_argument("--questions", type=Path, default=DEFAULT_QUESTIONS)
    run = sub.add_parser("run", help="ask each question of a fresh agent with only pdw, and grade it")
    run.add_argument("--questions", type=Path, default=DEFAULT_QUESTIONS,
                     help="question file for a run without --write")
    run.add_argument("--write", action="store_true",
                     help="read questions from and write results to the warehouse (POSTGRES_DATABASE_URL)")
    run.add_argument("--limit", type=int, default=None)
    run.add_argument("--only", nargs="*", default=[], help="question_ids to run")
    run.add_argument("--provider", default="")
    run.add_argument("--model", default="")
    run.add_argument("--timeout", type=int, default=DEFAULT_QUESTION_TIMEOUT_SECONDS)
    run.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    args = parser.parse_args(argv)
    if args.command in {"publish", "pull"}:
        if args.command == "pull":
            assert_private_path(args.questions)
        return _questions_command(args)
    return _run_command(args)


if __name__ == "__main__":
    raise SystemExit(main())
