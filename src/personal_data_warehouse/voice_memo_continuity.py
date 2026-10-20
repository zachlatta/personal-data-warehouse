"""Merge back-to-back voice recordings of one event into one session.

Zach stops a memo and starts another in the middle of one lecture, meeting or
thought. Each recording used to be its own island: two titles, two summaries,
half the action items in each. This pass runs after enrichment, in two steps:

1. **Judge.** Every pair of consecutive recordings of one source and account
   that sit close in time (a candidate window, not the decision) goes to an
   agent with the end of the first transcript and the start of the second.
   The agent decides from the content whether the second carries on the same
   event. Two unrelated task memos a minute apart stay apart; a lecture split
   by a stop and restart is joined. Each judgment is stored per pair in
   derived_voice_memos.continuity_judgments and redone when either transcript
   changes.
2. **Merge.** A run of recordings joined by same_event judgments is a session
   (marts_voice_memos.sessions computes them). A session with two or more
   members gets one enrichment written over all of them: title, summary,
   participants and action items for the whole event.

Both steps read marts_voice_memos.recordings / .sessions, never a raw source
table (contract C5), so every voice source is covered.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from typing import Any

from personal_data_warehouse.schema import VOICE_MEMO_CONTINUITY_MIN_CONFIDENCE

CONTINUITY_JUDGMENT_PROMPT_VERSION = "voice-memo-continuity-judgment-v1"
SESSION_MERGE_PROMPT_VERSION = "voice-memo-session-merge-v1"
CONTINUITY_JUDGMENT_TASK_TYPE = "voice_memo_continuity_judgment"
SESSION_MERGE_TASK_TYPE = "voice_memo_session_merge"
AGENT_INPUT_FILE = "continuity_input.json"
# A candidate window, not the decision: only recordings this close are asked
# about at all. Measured 2026-10-20 over 60 days of Apple memos, every
# restart of one event (a lecture split by a 3 s stop, a demo session split by
# 8 s, a call split by 83 s) sat well inside it, and so did dozens of unrelated
# task memos dictated a minute apart, which is why the agent decides.
DEFAULT_CONTINUITY_MAX_GAP_SECONDS = 30 * 60
DEFAULT_CONTINUITY_MAX_ERROR_ATTEMPTS = 3
# The agent reads the seam: the end of the earlier recording and the start of
# the later one, plus both summaries.
JUDGMENT_TRANSCRIPT_EDGE_CHARS = 6_000
# A session is merged once no further recording can still join it: either a
# later recording exists (and was judged first in the same pass) or the
# candidate window has passed since the session ended.
MERGE_TRANSCRIPT_MAX_CHARS = 60_000
MERGE_MEMBER_TRANSCRIPT_MIN_CHARS = 4_000

JUDGMENT_VERDICTS = ("same_event", "separate")


@dataclass(frozen=True)
class VoiceMemoContinuitySummary:
    pairs_seen: int
    pairs_judged: int
    pairs_same_event: int
    pairs_failed: int
    sessions_seen: int
    sessions_merged: int
    sessions_failed: int


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_continuity_pair_candidates(
    warehouse,
    *,
    limit: int | None,
    max_gap_seconds: float = DEFAULT_CONTINUITY_MAX_GAP_SECONDS,
    max_error_attempts: int = DEFAULT_CONTINUITY_MAX_ERROR_ATTEMPTS,
) -> list[dict[str, Any]]:
    """Consecutive enriched recordings, close in time, with no current judgment.

    A judgment is current while both transcripts it read are still the ones
    the mart serves, so re-transcribing either recording asks again. Newest
    pairs first: the memo Zach just recorded matters more than the backlog.
    """
    limit_sql = f"LIMIT {int(limit)}" if limit is not None and limit > 0 else ""
    error_sql = (
        f"AND (j.status IS DISTINCT FROM 'error' OR j.attempts < {int(max_error_attempts)})"
        if max_error_attempts > 0
        else ""
    )
    rows = warehouse._query_dicts(
        f"""
        WITH ordered AS (
            SELECT
                r.source,
                r.account,
                r.recording_id,
                r.recorded_at,
                r.duration_seconds,
                r.title,
                r.summary,
                r.transcript,
                COALESCE(r.provider_transcript_id, '') AS provider_transcript_id,
                r.enriched_at,
                LAG(r.recording_id) OVER w AS previous_recording_id,
                LAG(r.recorded_at) OVER w AS previous_recorded_at,
                LAG(r.duration_seconds) OVER w AS previous_duration_seconds,
                LAG(r.title) OVER w AS previous_title,
                LAG(r.summary) OVER w AS previous_summary,
                LAG(r.transcript) OVER w AS previous_transcript,
                LAG(COALESCE(r.provider_transcript_id, '')) OVER w AS previous_provider_transcript_id,
                LAG(r.enriched_at) OVER w AS previous_enriched_at
            FROM @marts_voice_memos_recordings r
            WHERE r.is_deleted = 0
              AND r.recorded_at IS NOT NULL
            WINDOW w AS (PARTITION BY r.source, r.account ORDER BY r.recorded_at, r.recording_id)
        ),
        pairs AS (
            SELECT
                o.*,
                EXTRACT(EPOCH FROM (
                    o.recorded_at
                    - (o.previous_recorded_at + make_interval(secs => COALESCE(o.previous_duration_seconds, 0)::float8))
                ))::float8 AS gap_seconds
            FROM ordered o
            WHERE o.previous_recording_id IS NOT NULL
        )
        SELECT p.*, COALESCE(j.attempts, 0) AS prior_attempts, COALESCE(j.status, '') AS prior_status
        FROM pairs p
        LEFT JOIN @voice_memo_continuity_judgments j
          ON j.source = p.source
         AND j.account = p.account
         AND j.previous_recording_id = p.previous_recording_id
         AND j.recording_id = p.recording_id
        WHERE p.gap_seconds <= {float(max_gap_seconds)}
          AND NULLIF(p.transcript, '') IS NOT NULL
          AND NULLIF(p.previous_transcript, '') IS NOT NULL
          AND p.enriched_at IS NOT NULL
          AND p.previous_enriched_at IS NOT NULL
          AND NOT (
              j.status = 'completed'
              AND j.provider_transcript_id = p.provider_transcript_id
              AND j.previous_provider_transcript_id = p.previous_provider_transcript_id
          ) IS TRUE
          {error_sql}
        ORDER BY p.recorded_at DESC
        {limit_sql}
        """
    )
    return [dict(row) for row in rows]


def load_session_merge_candidates(
    warehouse,
    *,
    limit: int | None,
    now: datetime,
    settle_seconds: float = DEFAULT_CONTINUITY_MAX_GAP_SECONDS,
    max_error_attempts: int = DEFAULT_CONTINUITY_MAX_ERROR_ATTEMPTS,
) -> list[dict[str, Any]]:
    """Multi-recording sessions whose merged enrichment is missing or stale.

    Stale means a member was enriched again after the merge. A session that
    could still grow (it ended inside the candidate window and nothing was
    recorded after it) waits, so a run of three restarts is merged once.
    """
    limit_sql = f"LIMIT {int(limit)}" if limit is not None and limit > 0 else ""
    error_sql = (
        f"AND COALESCE(e.attempts, 0) < {int(max_error_attempts)}" if max_error_attempts > 0 else ""
    )
    cutoff = now.astimezone(UTC).isoformat()
    rows = warehouse._query_dicts(
        f"""
        SELECT
            s.source,
            s.account,
            s.session_id,
            s.recording_ids,
            s.started_at,
            s.ended_at,
            COALESCE(e.attempts, 0) AS prior_attempts
        FROM @marts_voice_memos_sessions s
        LEFT JOIN @voice_memo_session_enrichments e
          ON e.source = s.source
         AND e.account = s.account
         AND e.member_recording_ids = array_to_string(s.recording_ids, ',')
         AND e.status = 'error'
        WHERE s.recording_count > 1
          AND s.members_unenriched = 0
          AND (s.merged_at IS NULL OR s.merged_at < s.members_enriched_at)
          AND (
              s.ended_at < TIMESTAMPTZ {_sql_string(cutoff)} - make_interval(secs => {float(settle_seconds)})
              OR EXISTS (
                  SELECT 1
                  FROM @marts_voice_memos_recordings later
                  WHERE later.source = s.source
                    AND later.account = s.account
                    AND later.is_deleted = 0
                    AND later.recorded_at > s.ended_at
              )
          )
          {error_sql}
        ORDER BY s.started_at DESC
        {limit_sql}
        """
    )
    return [dict(row) for row in rows]


def load_session_members(warehouse, session: Mapping[str, Any]) -> list[dict[str, Any]]:
    ids = ", ".join(_sql_string(str(value)) for value in session.get("recording_ids") or [])
    if not ids:
        return []
    rows = warehouse._query_dicts(
        f"""
        SELECT
            recording_id,
            recorded_at,
            duration_seconds,
            title,
            summary,
            transcript,
            participants_json,
            action_items_json,
            calendar_event_id
        FROM @marts_voice_memos_recordings
        WHERE source = {_sql_string(str(session["source"]))}
          AND account = {_sql_string(str(session["account"]))}
          AND recording_id IN ({ids})
        ORDER BY recorded_at, recording_id
        """
    )
    return [dict(row) for row in rows]


def continuity_system_prompt() -> str:
    return (
        "You decide whether two back-to-back voice recordings made by Zach Latta are one event. "
        "Zach often stops a recording and starts another: sometimes to carry on the same lecture, "
        "meeting, call, conversation or dictated thought, and sometimes to record something new. "
        "Decide from what is said, not from how close in time the recordings are: they are always close."
    )


def continuity_instructions() -> list[str]:
    return [
        "Read the input file. It has the earlier recording (its summary and the END of its transcript), "
        "the later recording (its summary and the START of its transcript) and the seconds between them.",
        "verdict same_event: the later recording carries on the same event or activity as the earlier one. "
        "Examples: the same talk, panel, meeting, call or conversation continues (same speakers, topic picks up "
        "where it stopped, a Q&A after the talk it follows); one dictated note or request continues mid-thought "
        "or is finished in the second recording (\"and also...\", \"continuing from before\", a sentence that was cut off).",
        "verdict separate: the later recording is a new event, even when it is about a related subject. "
        "Examples: a new, self-contained dictated task or note after a finished one, even on the same project; "
        "a follow-up instruction about earlier work that stands on its own; a different conversation or meeting; "
        "Zach saying the earlier recording was a mistake and starting over on something else.",
        "Short dictated memos a minute apart are usually separate tasks. Long recordings of one gathering are "
        "usually one event. Decide each pair on its content.",
        "confidence: 0 to 1, how sure you are of the verdict. reason: one or two sentences naming the content "
        "that decided it.",
        "You may use the pdw CLI to check a calendar event or a name if the transcripts alone do not settle it; "
        "most pairs need no research.",
    ]


def continuity_judgment_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["verdict", "confidence", "reason"],
        "properties": {
            "verdict": {"type": "string", "enum": list(JUDGMENT_VERDICTS)},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "reason": {"type": "string"},
        },
    }


def continuity_task_input(pair: Mapping[str, Any]) -> str:
    def side(prefix: str, *, tail: bool) -> dict[str, Any]:
        transcript = str(pair.get(f"{prefix}transcript") or "")
        if len(transcript) > JUDGMENT_TRANSCRIPT_EDGE_CHARS:
            transcript = (
                "..." + transcript[-JUDGMENT_TRANSCRIPT_EDGE_CHARS:]
                if tail
                else transcript[:JUDGMENT_TRANSCRIPT_EDGE_CHARS] + "..."
            )
        return {
            "recording_id": pair.get(f"{prefix}recording_id"),
            "recorded_at": _iso(pair.get(f"{prefix}recorded_at")),
            "duration_seconds": _float_or_none(pair.get(f"{prefix}duration_seconds")),
            "title": pair.get(f"{prefix}title") or "",
            "summary": pair.get(f"{prefix}summary") or "",
            "transcript_end" if tail else "transcript_start": transcript,
        }

    return json.dumps(
        {
            "instructions": continuity_instructions(),
            "seconds_between_recordings": round(float(pair.get("gap_seconds") or 0.0), 1),
            "earlier_recording": side("previous_", tail=True),
            "later_recording": side("", tail=False),
        },
        ensure_ascii=False,
        indent=2,
    )


def session_merge_system_prompt() -> str:
    return (
        "You write the record of one event that Zach Latta recorded as several back-to-back voice recordings, "
        "because he stopped recording and started again. Each recording was already enriched on its own; "
        "write one title, summary, participant list and action item list for the whole event."
    )


def session_merge_instructions() -> list[str]:
    return [
        "Read the input file: the member recordings in order, each with its own title, summary, participants, "
        "action items and transcript.",
        "title: one specific title for the whole event, in the style of the members' titles.",
        "summary: one summary of the whole event that reads as a single record, not one paragraph per recording. "
        "Keep every substantive point, decision and name from the member summaries.",
        "participants: the union of the members' participants, full names, no duplicates.",
        "action_items: every action item from the members, merged where two say the same thing, in order.",
        "evidence: short notes on how the recordings connect (for example where the first one stopped and the "
        "next one picked up).",
        "Do not invent anything that no member recording supports.",
    ]


def session_merge_schema() -> dict[str, Any]:
    strings = {"type": "array", "items": {"type": "string"}}
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["title", "summary", "participants", "action_items", "evidence"],
        "properties": {
            "title": {"type": "string"},
            "summary": {"type": "string"},
            "participants": strings,
            "action_items": strings,
            "evidence": strings,
        },
    }


def session_merge_task_input(members: Sequence[Mapping[str, Any]]) -> str:
    budget = max(MERGE_MEMBER_TRANSCRIPT_MIN_CHARS, MERGE_TRANSCRIPT_MAX_CHARS // max(len(members), 1))
    items = []
    for index, member in enumerate(members, start=1):
        transcript = str(member.get("transcript") or "")
        if len(transcript) > budget:
            half = budget // 2
            transcript = transcript[:half] + "\n...\n" + transcript[-half:]
        items.append(
            {
                "position": index,
                "recording_id": member.get("recording_id"),
                "recorded_at": _iso(member.get("recorded_at")),
                "duration_seconds": _float_or_none(member.get("duration_seconds")),
                "title": member.get("title") or "",
                "summary": member.get("summary") or "",
                "participants": _json_list(member.get("participants_json")),
                "action_items": _json_list(member.get("action_items_json")),
                "calendar_event_id": member.get("calendar_event_id") or "",
                "transcript": transcript,
            }
        )
    return json.dumps(
        {"instructions": session_merge_instructions(), "recordings": items},
        ensure_ascii=False,
        indent=2,
    )


class VoiceMemoContinuityRunner:
    def __init__(
        self,
        *,
        warehouse,
        client,
        logger,
        provider: str,
        now: Callable[[], datetime] | None = None,
        max_gap_seconds: float = DEFAULT_CONTINUITY_MAX_GAP_SECONDS,
        max_error_attempts: int = DEFAULT_CONTINUITY_MAX_ERROR_ATTEMPTS,
    ) -> None:
        self._warehouse = warehouse
        self._client = client
        self._logger = logger
        self._provider = provider
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._max_gap_seconds = max_gap_seconds
        self._max_error_attempts = max_error_attempts

    def sync(self, *, limit: int | None) -> VoiceMemoContinuitySummary:
        self._warehouse.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
        pairs = load_continuity_pair_candidates(
            self._warehouse,
            limit=limit,
            max_gap_seconds=self._max_gap_seconds,
            max_error_attempts=self._max_error_attempts,
        )
        judged = same_event = pairs_failed = 0
        for index, pair in enumerate(pairs, start=1):
            label = f"{pair['previous_recording_id']} -> {pair['recording_id']}"
            try:
                self._logger.info("[%s/%s] judging %s", index, len(pairs), label)
                result = self._client.create_agentic_structured(
                    system_prompt=continuity_system_prompt(),
                    user_prompt=_user_prompt(),
                    schema=continuity_judgment_schema(),
                    tools=[],
                    tool_executor=lambda _name, _arguments: {},
                    min_tool_calls=0,
                    require_tool_call=False,
                    input_files={AGENT_INPUT_FILE: continuity_task_input(pair)},
                    logger=self._logger,
                    recording_id=str(pair["recording_id"]),
                    prompt_version=CONTINUITY_JUDGMENT_PROMPT_VERSION,
                    task_type=CONTINUITY_JUDGMENT_TASK_TYPE,
                )
                verdict = str(result.get("verdict") or "")
                if verdict not in JUDGMENT_VERDICTS:
                    raise ValueError(f"unknown verdict {verdict!r}")
                row = self._judgment_row(pair, status="completed", error="", attempts=0, result=result)
                self._warehouse.insert_voice_memo_continuity_judgments([row])
                judged += 1
                if verdict == "same_event" and row["confidence"] >= VOICE_MEMO_CONTINUITY_MIN_CONFIDENCE:
                    same_event += 1
                self._logger.info("[%s/%s] %s: %s (%.2f)", index, len(pairs), label, verdict, row["confidence"])
            except Exception as exc:
                pairs_failed += 1
                attempts = int(pair.get("prior_attempts") or 0) + 1 if pair.get("prior_status") == "error" else 1
                self._warehouse.insert_voice_memo_continuity_judgments(
                    [self._judgment_row(pair, status="error", error=str(exc), attempts=attempts, result={})]
                )
                self._logger.warning("[%s/%s] failed %s: %s", index, len(pairs), label, exc)

        sessions = load_session_merge_candidates(
            self._warehouse,
            limit=limit,
            now=self._now(),
            settle_seconds=self._max_gap_seconds,
            max_error_attempts=self._max_error_attempts,
        )
        merged = sessions_failed = 0
        for index, session in enumerate(sessions, start=1):
            members_key = ",".join(str(value) for value in session.get("recording_ids") or [])
            try:
                members = load_session_members(self._warehouse, session)
                if len(members) < 2:
                    continue
                self._logger.info("[%s/%s] merging session %s", index, len(sessions), members_key)
                result = self._client.create_agentic_structured(
                    system_prompt=session_merge_system_prompt(),
                    user_prompt=_user_prompt(),
                    schema=session_merge_schema(),
                    tools=[],
                    tool_executor=lambda _name, _arguments: {},
                    min_tool_calls=0,
                    require_tool_call=False,
                    input_files={AGENT_INPUT_FILE: session_merge_task_input(members)},
                    logger=self._logger,
                    recording_id=str(session["session_id"]),
                    prompt_version=SESSION_MERGE_PROMPT_VERSION,
                    task_type=SESSION_MERGE_TASK_TYPE,
                )
                if not str(result.get("title") or "").strip() or not str(result.get("summary") or "").strip():
                    raise ValueError("merged session needs a title and a summary")
                self._warehouse.insert_voice_memo_session_enrichments(
                    [self._session_row(session, members_key, status="completed", error="", attempts=0, result=result)]
                )
                merged += 1
            except Exception as exc:
                sessions_failed += 1
                attempts = int(session.get("prior_attempts") or 0) + 1
                self._warehouse.insert_voice_memo_session_enrichments(
                    [self._session_row(session, members_key, status="error", error=str(exc), attempts=attempts, result={})]
                )
                self._logger.warning("[%s/%s] failed session %s: %s", index, len(sessions), members_key, exc)

        return VoiceMemoContinuitySummary(
            pairs_seen=len(pairs),
            pairs_judged=judged,
            pairs_same_event=same_event,
            pairs_failed=pairs_failed,
            sessions_seen=len(sessions),
            sessions_merged=merged,
            sessions_failed=sessions_failed,
        )

    def _judgment_row(
        self,
        pair: Mapping[str, Any],
        *,
        status: str,
        error: str,
        attempts: int,
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        now = self._now()
        return {
            "source": pair["source"],
            "account": pair["account"],
            "previous_recording_id": pair["previous_recording_id"],
            "recording_id": pair["recording_id"],
            "previous_provider_transcript_id": str(pair.get("previous_provider_transcript_id") or ""),
            "provider_transcript_id": str(pair.get("provider_transcript_id") or ""),
            "gap_seconds": float(pair.get("gap_seconds") or 0.0),
            "provider": self._provider,
            "model": self._client.model,
            "prompt_version": CONTINUITY_JUDGMENT_PROMPT_VERSION,
            "status": status,
            "error": error,
            "attempts": attempts,
            "verdict": str(result.get("verdict") or ""),
            "confidence": _clamp_confidence(result.get("confidence")),
            "reason": str(result.get("reason") or ""),
            "raw_result_json": json.dumps(dict(result), ensure_ascii=False, default=str) if result else "",
            "created_at": now,
            "sync_version": int(now.timestamp() * 1000),
        }

    def _session_row(
        self,
        session: Mapping[str, Any],
        members_key: str,
        *,
        status: str,
        error: str,
        attempts: int,
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        now = self._now()
        return {
            "source": session["source"],
            "account": session["account"],
            "session_id": session["session_id"],
            "member_recording_ids": members_key,
            "provider": self._provider,
            "model": self._client.model,
            "prompt_version": SESSION_MERGE_PROMPT_VERSION,
            "status": status,
            "error": error,
            "attempts": attempts,
            "title": str(result.get("title") or "").strip(),
            "summary": str(result.get("summary") or "").strip(),
            "participants_json": _json_or_empty(result.get("participants")),
            "action_items_json": _json_or_empty(result.get("action_items")),
            "evidence_json": _json_or_empty(result.get("evidence")),
            "raw_result_json": json.dumps(dict(result), ensure_ascii=False, default=str) if result else "",
            "created_at": now,
            "sync_version": int(now.timestamp() * 1000),
        }


def _user_prompt() -> str:
    return json.dumps(
        {
            "task": f"Read \"$AGENT_INPUT_DIR/{AGENT_INPUT_FILE}\" and follow its instructions.",
            "output": "Return only the JSON object the schema describes.",
        }
    )


def _clamp_confidence(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return min(max(number, 0.0), 1.0)


def _float_or_none(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value or "")


def _json_list(value: Any) -> list[Any]:
    if not value:
        return []
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _json_or_empty(value: Any) -> str:
    if value is None:
        return ""
    return json.dumps(list(value) if isinstance(value, (list, tuple)) else value, ensure_ascii=False)
