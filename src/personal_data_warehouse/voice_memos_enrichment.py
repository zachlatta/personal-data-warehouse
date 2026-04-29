from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from difflib import SequenceMatcher
import json
import re
import time
from typing import Any
from zoneinfo import ZoneInfo

import requests

from personal_data_warehouse.clickhouse import _sql_string
from personal_data_warehouse.clickhouse_readonly import (
    ClickHouseReadOnlyRunner,
    ClickHouseReadOnlyService,
    strip_markdown_code_fence,
)


ENRICHMENT_PROVIDER = "openai"
ENRICHMENT_PROMPT_VERSION = "voice-memo-enrichment-v20"
DEFAULT_ENRICHMENT_MAX_TOOL_CALLS = 8
DEFAULT_ENRICHMENT_MIN_TOOL_CALLS = 3
DEFAULT_OPENAI_REASONING_EFFORT = "high"
DEFAULT_RECORDING_LOCAL_TIMEZONE = "America/New_York"
DEFAULT_ENRICHMENT_LOOKBACK_WEEKS = 8
LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL = "[LOCAL_TRANSCRIPT_ASSEMBLY]"
LOCAL_TRANSCRIPT_ASSEMBLY_MIN_SOURCE_CHARS = 12_000


@dataclass(frozen=True)
class VoiceMemosEnrichmentSummary:
    recordings_seen: int
    recordings_enriched: int
    recordings_failed: int


class OpenAIResponsesClient:
    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        base_url: str = "https://api.openai.com",
        timeout_seconds: int = 1800,
        reasoning_effort: str | None = DEFAULT_OPENAI_REASONING_EFFORT,
        max_response_retries: int = 2,
        retry_delay_seconds: float = 2.0,
        session=None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._reasoning_effort = reasoning_effort
        self._max_response_retries = max_response_retries
        self._retry_delay_seconds = retry_delay_seconds
        self._session = session or requests.Session()
        self._sleep = sleep

    @property
    def model(self) -> str:
        return self._model

    def create_structured(self, *, system_prompt: str, user_prompt: str, schema: Mapping[str, Any]) -> Mapping[str, Any]:
        return self.create_agentic_structured(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            schema=schema,
            tools=[],
            tool_executor=lambda _name, _arguments: {},
            max_tool_calls=0,
        )

    def create_agentic_structured(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        schema: Mapping[str, Any],
        tools: Sequence[Mapping[str, Any]],
        tool_executor: Callable[[str, Mapping[str, Any]], Mapping[str, Any]],
        max_tool_calls: int = DEFAULT_ENRICHMENT_MAX_TOOL_CALLS,
        min_tool_calls: int = 0,
        require_tool_call: bool = False,
        result_validator: Callable[[Mapping[str, Any]], Sequence[str]] | None = None,
        max_validation_retries: int = 2,
        logger=None,
        recording_id: str = "",
    ) -> Mapping[str, Any]:
        conversation: list[Mapping[str, Any]] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        tool_trace: list[dict[str, Any]] = []

        response_index = 0
        started = time.monotonic()
        required_tool_retry_sent = False
        required_tool_calls = max(1 if require_tool_call else 0, min_tool_calls)
        validation_retries = 0
        final_only_repair = False
        while True:
            response_index += 1
            if logger:
                logger.info(
                    "OpenAI enrichment request %s started for %s; prior_tool_calls=%s input_items=%s",
                    response_index,
                    recording_id,
                    len(tool_trace),
                    len(conversation),
                )
            if final_only_repair:
                request_tools = []
            elif len(tool_trace) >= max_tool_calls:
                request_tools = []
            else:
                request_tools = schema_discovery_tools(tools) if required_tool_calls and not tool_trace else tools
            payload = self._post_response_with_retries(
                input_items=conversation,
                schema=schema,
                tools=request_tools,
                tool_choice="required" if request_tools and len(tool_trace) < required_tool_calls else None,
                logger=logger,
                recording_id=recording_id,
                response_index=response_index,
            )
            calls = response_function_calls(payload)
            if logger:
                logger.info(
                    "OpenAI enrichment request %s finished for %s; function_calls=%s elapsed=%.1fs",
                    response_index,
                    recording_id,
                    len(calls),
                    time.monotonic() - started,
            )
            if not calls:
                if tools and len(tool_trace) < required_tool_calls:
                    if required_tool_retry_sent:
                        raise RuntimeError(
                            f"OpenAI enrichment returned final output after {len(tool_trace)} tool calls; "
                            f"{required_tool_calls} required"
                        )
                    required_tool_retry_sent = True
                    conversation.append(
                        {
                            "role": "user",
                            "content": (
                                f"You returned final JSON after {len(tool_trace)} warehouse tool calls, but "
                                f"{required_tool_calls} are required before final output. Use show_schema if you need "
                                "table/column context, then make focused sql read-only queries to verify calendar, "
                                "attendee identity, speaker identity, and domain-term evidence. Then return the structured JSON."
                            ),
                        }
                    )
                    if logger:
                        logger.warning(
                            "OpenAI enrichment returned final output for %s before required tool calls; retrying",
                            recording_id,
                        )
                    continue
                result = json.loads(response_output_text(payload))
                if tool_trace:
                    result["__tool_calls"] = tool_trace
                validation_issues = list(result_validator(result) if result_validator else [])
                if validation_issues:
                    result["__validation_issues"] = validation_issues
                    if validation_retries < max_validation_retries:
                        validation_retries += 1
                        final_only_repair = True
                        conversation.append(
                            {
                                "role": "user",
                                "content": validation_repair_prompt(result=result, issues=validation_issues),
                            }
                        )
                        if logger:
                            logger.warning(
                                "OpenAI enrichment final output for %s failed validation; retrying repair %s/%s: %s",
                                recording_id,
                                validation_retries,
                                max_validation_retries,
                                "; ".join(validation_issues[:5]),
                            )
                        continue
                    if logger:
                        logger.warning(
                            "OpenAI enrichment final output for %s still has validation issues after repair: %s",
                            recording_id,
                            "; ".join(validation_issues[:5]),
                        )
                if logger:
                    logger.info(
                        "OpenAI enrichment final structured output ready for %s; total_tool_calls=%s elapsed=%.1fs",
                        recording_id,
                        len(tool_trace),
                        time.monotonic() - started,
                    )
                return result

            if len(tool_trace) + len(calls) > max_tool_calls:
                raise RuntimeError(f"OpenAI enrichment exceeded {max_tool_calls} tool calls")

            conversation.extend(payload.get("output") or [])
            for call_index, call in enumerate(calls, start=1):
                name = str(call.get("name", ""))
                arguments = parse_function_arguments(call.get("arguments"))
                if logger:
                    logger.info(
                        "OpenAI tool call %s.%s for %s: %s %s",
                        response_index,
                        call_index,
                        recording_id,
                        name,
                        summarize_tool_arguments(arguments),
                    )
                output = tool_executor(name, arguments)
                if logger:
                    logger.info(
                        "OpenAI tool call %s.%s finished for %s: %s",
                        response_index,
                        call_index,
                        recording_id,
                        summarize_tool_output(output),
                    )
                tool_trace.append(
                    {
                        "name": name,
                        "arguments": dict(arguments),
                        "output": output,
                    }
                )
                conversation.append(
                    {
                        "type": "function_call_output",
                        "call_id": str(call.get("call_id", "")),
                        "output": json.dumps(output, sort_keys=True, default=str),
                    }
                )

    def _post_response(
        self,
        *,
        input_items: Sequence[Mapping[str, Any]],
        schema: Mapping[str, Any],
        tools: Sequence[Mapping[str, Any]],
        tool_choice: str | Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        body: dict[str, Any] = {
            "model": self._model,
            "input": list(input_items),
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "voice_memo_enrichment",
                    "strict": True,
                    "schema": schema,
                }
            },
        }
        if tools:
            body["tools"] = list(tools)
        if self._reasoning_effort:
            body["reasoning"] = {"effort": self._reasoning_effort}
        if tool_choice is not None:
            body["tool_choice"] = tool_choice
        response = self._session.post(
            f"{self._base_url}/v1/responses",
            headers={
                "authorization": f"Bearer {self._api_key}",
                "content-type": "application/json",
            },
            json=body,
            timeout=self._timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def _post_response_with_retries(
        self,
        *,
        input_items: Sequence[Mapping[str, Any]],
        schema: Mapping[str, Any],
        tools: Sequence[Mapping[str, Any]],
        tool_choice: str | Mapping[str, Any] | None = None,
        logger=None,
        recording_id: str = "",
        response_index: int = 0,
    ) -> Mapping[str, Any]:
        attempts = self._max_response_retries + 1
        for attempt in range(1, attempts + 1):
            try:
                return self._post_response(
                    input_items=input_items,
                    schema=schema,
                    tools=tools,
                    tool_choice=tool_choice,
                )
            except requests.RequestException as exc:
                if attempt >= attempts:
                    raise
                if logger:
                    logger.warning(
                        "OpenAI enrichment request %s attempt %s/%s failed for %s: %s; retrying in %.1fs",
                        response_index,
                        attempt,
                        attempts,
                        recording_id,
                        exc,
                        self._retry_delay_seconds,
                    )
                self._sleep(self._retry_delay_seconds)
        raise RuntimeError("unreachable OpenAI retry state")


class ClickHouseEnrichmentTool:
    def __init__(self, query_service: ClickHouseReadOnlyService) -> None:
        self._query_service = query_service

    def execute(self, name: str, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        if name == "show_schema":
            result = self._query_service.schema_overview()
            return result.as_tool_payload()
        if name != "sql":
            return {"error": f"unknown tool: {name}"}
        sql = strip_markdown_code_fence(str(arguments.get("query", arguments.get("sql", ""))))
        result = self._query_service.execute_one(sql)
        return result.as_tool_payload()


class VoiceMemosEnrichmentRunner:
    def __init__(
        self,
        *,
        warehouse,
        client: OpenAIResponsesClient,
        logger,
        now: Callable[[], datetime] | None = None,
        provider: str = ENRICHMENT_PROVIDER,
        prompt_version: str = ENRICHMENT_PROMPT_VERSION,
    ) -> None:
        self._warehouse = warehouse
        self._client = client
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._provider = provider
        self._prompt_version = prompt_version
        self._query_tool = ClickHouseEnrichmentTool(
            ClickHouseReadOnlyService(
                ClickHouseReadOnlyRunner(warehouse),
                max_rows=50,
                max_field_chars=3000,
            )
        )

    def sync(self, *, limit: int | None, recorded_after: datetime | None = None) -> VoiceMemosEnrichmentSummary:
        self._warehouse.ensure_voice_memos_tables()
        recordings = load_enrichment_candidates(
            self._warehouse,
            provider=self._provider,
            model=self._client.model,
            prompt_version=self._prompt_version,
            limit=limit,
            recorded_after=recorded_after,
        )
        enriched = 0
        failed = 0
        for index, recording in enumerate(recordings, start=1):
            recording_id = str(recording.get("recording_id", ""))
            try:
                self._logger.info("[%s/%s] enriching %s", index, len(recordings), recording_id)
                calendar_candidates = load_calendar_candidates(self._warehouse, recording)
                transcript_segments = load_transcript_segments(self._warehouse, recording)
                prompt = enrichment_user_prompt(
                    recording=recording,
                    calendar_candidates=calendar_candidates,
                    transcript_segments=transcript_segments,
                )
                result = self._client.create_agentic_structured(
                    system_prompt=enrichment_system_prompt(),
                    user_prompt=prompt,
                    schema=enrichment_schema(),
                    tools=warehouse_tool_definitions(),
                    tool_executor=self._query_tool.execute,
                    min_tool_calls=DEFAULT_ENRICHMENT_MIN_TOOL_CALLS,
                    require_tool_call=True,
                    result_validator=lambda result, recording=recording, transcript_segments=transcript_segments: validate_enrichment_result(
                        recording=recording,
                        transcript_segments=transcript_segments,
                        result=result,
                    ),
                    logger=self._logger,
                    recording_id=recording_id,
                )
                result = apply_segment_preserving_transcript_fallback(
                    recording=recording,
                    transcript_segments=transcript_segments,
                    result=result,
                )
                result = ensure_recording_level_fields(
                    recording=recording,
                    transcript_segments=transcript_segments,
                    result=result,
                )
                self._warehouse.insert_voice_memo_enrichments(
                    [
                        enrichment_row(
                            recording=recording,
                            result=result,
                            provider=self._provider,
                            model=self._client.model,
                            prompt_version=self._prompt_version,
                            status="completed",
                            error="",
                            created_at=self._now(),
                        )
                    ]
                )
                enriched += 1
            except Exception as exc:
                failed += 1
                self._warehouse.insert_voice_memo_enrichments(
                    [
                        failed_enrichment_row(
                            recording=recording,
                            provider=self._provider,
                            model=self._client.model,
                            prompt_version=self._prompt_version,
                            error=str(exc),
                            created_at=self._now(),
                        )
                    ]
                )
                self._logger.warning("[%s/%s] failed %s: %s", index, len(recordings), recording_id, exc)
        return VoiceMemosEnrichmentSummary(
            recordings_seen=len(recordings),
            recordings_enriched=enriched,
            recordings_failed=failed,
        )


def load_enrichment_candidates(
    warehouse,
    *,
    provider: str,
    model: str,
    prompt_version: str,
    limit: int | None,
    recorded_after: datetime | None = None,
) -> list[dict[str, Any]]:
    filters = [
        "r.provider = 'assemblyai'",
        "r.status = 'completed'",
        "e.recording_id = ''",
    ]
    if recorded_after is not None:
        if recorded_after.tzinfo is None:
            recorded_after = recorded_after.replace(tzinfo=UTC)
        filters.append(f"f.recorded_at >= parseDateTimeBestEffort({_sql_string(recorded_after.astimezone(UTC).isoformat())})")
    limit_sql = f"LIMIT {int(limit)}" if limit and limit > 0 else ""
    rows = warehouse._query(
        f"""
        SELECT
            f.account,
            f.recording_id,
            f.recorded_at,
            f.title,
            r.transcript_text
        FROM voice_memo_files AS f
        INNER JOIN voice_memo_transcription_runs AS r
            ON f.account = r.account AND f.recording_id = r.recording_id
        LEFT JOIN
        (
            SELECT account, recording_id
            FROM voice_memo_enrichments
            WHERE provider = {_sql_string(provider)}
              AND model = {_sql_string(model)}
              AND prompt_version = {_sql_string(prompt_version)}
              AND status = 'completed'
            GROUP BY account, recording_id
        ) AS e
            ON f.account = e.account AND f.recording_id = e.recording_id
        WHERE {" AND ".join(filters)}
        ORDER BY f.recorded_at DESC
        {limit_sql}
        """
    )
    columns = ("account", "recording_id", "recorded_at", "title", "transcript_text")
    return [dict(zip(columns, row, strict=True)) for row in rows]


def load_calendar_candidates(warehouse, recording: Mapping[str, Any]) -> list[dict[str, Any]]:
    recorded_at = recording["recorded_at"]
    anchors = recording_time_interpretations(recorded_at)
    starts = [anchor["utc"].isoformat() for anchor in anchors]
    starts_sql = "[" + ", ".join(_sql_string(start) for start in starts) + "]"
    earliest = min(anchor["utc"] for anchor in anchors).isoformat()
    latest = max(anchor["utc"] for anchor in anchors).isoformat()
    rows = warehouse._query(
        f"""
        SELECT event_id, summary, start_at, end_at, location, attendees_json
        FROM calendar_events
        WHERE start_at <= parseDateTimeBestEffort({_sql_string(latest)}) + INTERVAL 3 HOUR
          AND end_at >= parseDateTimeBestEffort({_sql_string(earliest)}) - INTERVAL 3 HOUR
          AND is_deleted = 0
        ORDER BY arrayMin(x -> abs(dateDiff('second', start_at, parseDateTimeBestEffort(x))), {starts_sql})
        LIMIT 12
        """
    )
    candidates = []
    for event_id, summary, start_at, end_at, location, attendees_json in rows:
        attendee_details = parse_attendee_details(str(attendees_json or ""))
        identity_hints = load_attendee_identity_hints(warehouse, attendee_details)
        candidates.append(
            {
                "event_id": str(event_id),
                "summary": str(summary),
                "start_at": start_at.isoformat() if hasattr(start_at, "isoformat") else str(start_at),
                "end_at": end_at.isoformat() if hasattr(end_at, "isoformat") else str(end_at),
                "location": str(location),
                "attendees": attendee_summaries_from_details(attendee_details, identity_hints),
                "attendee_details": attendee_details,
                "identity_hints": identity_hints,
            }
        )
    return candidates


def recording_time_interpretations(recorded_at: datetime, *, local_timezone: str = DEFAULT_RECORDING_LOCAL_TIMEZONE) -> list[dict[str, Any]]:
    if recorded_at.tzinfo is None:
        recorded_at = recorded_at.replace(tzinfo=UTC)
    recorded_utc = recorded_at.astimezone(UTC)
    local_as_utc = recorded_at.replace(tzinfo=ZoneInfo(local_timezone)).astimezone(UTC)
    interpretations = [
        {
            "label": "recorded_at_as_utc",
            "utc": recorded_utc,
            "explanation": "Timestamp interpreted literally as UTC.",
        },
        {
            "label": f"recorded_at_as_{local_timezone}_wall_time",
            "utc": local_as_utc,
            "explanation": "Timestamp interpreted as local wall-clock time and converted to UTC.",
        },
    ]
    unique: list[dict[str, Any]] = []
    seen: set[datetime] = set()
    for interpretation in interpretations:
        if interpretation["utc"] in seen:
            continue
        seen.add(interpretation["utc"])
        unique.append(interpretation)
    return unique


def load_transcript_segments(warehouse, recording: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = warehouse._query(
        f"""
        SELECT segment_index, speaker_label, start_ms, end_ms, confidence, text
        FROM voice_memo_transcript_segments
        WHERE account = {_sql_string(str(recording.get("account", "")))}
          AND recording_id = {_sql_string(str(recording.get("recording_id", "")))}
          AND provider = 'assemblyai'
        ORDER BY segment_index
        LIMIT 2000
        """
    )
    return [
        {
            "segment_index": int(segment_index),
            "speaker_label": str(speaker_label),
            "start_ms": int(start_ms),
            "end_ms": int(end_ms),
            "confidence": float(confidence),
            "text": str(text),
        }
        for segment_index, speaker_label, start_ms, end_ms, confidence, text in rows
    ]


def parse_attendee_summaries(attendees_json: str) -> list[str]:
    return attendee_summaries_from_details(parse_attendee_details(attendees_json), {})


def parse_attendee_details(attendees_json: str) -> list[dict[str, str]]:
    if not attendees_json:
        return []
    try:
        payload = json.loads(attendees_json)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    attendees: list[dict[str, str]] = []
    for attendee in payload[:20]:
        if not isinstance(attendee, Mapping):
            continue
        display_name = str(attendee.get("displayName") or "").strip()
        email = str(attendee.get("email") or "").strip()
        if display_name or email:
            attendees.append({"display_name": display_name, "email": email})
    return attendees


def attendee_summaries_from_details(
    attendee_details: Sequence[Mapping[str, str]],
    identity_hints: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    attendees: list[str] = []
    for attendee in attendee_details:
        display_name = str(attendee.get("display_name") or "").strip()
        email = str(attendee.get("email") or "").strip()
        if display_name:
            attendees.append(display_name)
            continue
        hint = identity_hints.get(email.lower())
        possible_names = hint.get("possible_names") if isinstance(hint, Mapping) else None
        if isinstance(possible_names, list) and possible_names:
            attendees.append(str(possible_names[0]))
        elif email:
            attendees.append(email)
    return attendees


def load_attendee_identity_hints(
    warehouse,
    attendee_details: Sequence[Mapping[str, str]],
) -> dict[str, dict[str, Any]]:
    emails = sorted(
        {
            str(attendee.get("email") or "").strip().lower()
            for attendee in attendee_details
            if str(attendee.get("email") or "").strip()
        }
    )
    hints: dict[str, dict[str, Any]] = {}
    if not emails:
        return hints

    email_list_sql = "[" + ",".join(_sql_string(email) for email in emails) + "]"
    try:
        slack_rows = warehouse._query(
            f"""
            SELECT lower(email), name, real_name, display_name, user_id
            FROM slack_users
            WHERE lower(email) IN {email_list_sql}
            LIMIT 50
            """
        )
    except Exception:
        slack_rows = []
    for email, name, real_name, display_name, user_id in slack_rows:
        hint = hints.setdefault(str(email), {"email": str(email), "slack_users": [], "gmail_mentions": [], "possible_names": []})
        hint["slack_users"].append(
            {
                "name": str(name),
                "real_name": str(real_name),
                "display_name": str(display_name),
                "user_id": str(user_id),
            }
        )
        for candidate in (str(display_name), str(real_name)):
            add_possible_identity_name(hint, candidate, email=str(email))

    for email in emails:
        local_part = email.partition("@")[0]
        if not local_part:
            continue
        hint = hints.setdefault(email, {"email": email, "slack_users": [], "gmail_mentions": [], "possible_names": []})
        try:
            gmail_rows = warehouse._query(
                f"""
                SELECT from_address, subject, snippet
                FROM gmail_messages
                WHERE positionCaseInsensitive(from_address, {_sql_string(email)}) > 0
                   OR positionCaseInsensitive(subject, {_sql_string(local_part)}) > 0
                   OR positionCaseInsensitive(snippet, {_sql_string(local_part)}) > 0
                ORDER BY internal_date DESC
                LIMIT 20
                """
            )
        except Exception:
            gmail_rows = []
        for from_address, subject, snippet in gmail_rows:
            mention = {
                "from_address": str(from_address),
                "subject": str(subject),
                "snippet": str(snippet)[:240],
            }
            hint["gmail_mentions"].append(mention)
            for value in mention.values():
                for candidate in possible_identity_names_from_text(value):
                    add_possible_identity_name(hint, candidate, email=email)
    return hints


def add_possible_identity_name(hint: dict[str, Any], candidate: str, *, email: str) -> None:
    candidate = candidate.strip()
    if not candidate or "@" in candidate:
        return
    words = candidate.split()
    if len(words) < 2:
        return
    local_part = email.partition("@")[0].lower()
    if len(local_part) < 3:
        return
    if words[0].lower() not in local_part and local_part not in words[0].lower():
        return
    possible_names = hint.setdefault("possible_names", [])
    if candidate not in possible_names:
        possible_names.append(candidate)


def possible_identity_names_from_text(text: str) -> list[str]:
    names = []
    for match in re.finditer(r"\b[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){1,3}\b", text):
        candidate = match.group(0).strip()
        if candidate in {"Re Fwd", "Google Calendar", "Hack Club"}:
            continue
        names.append(candidate)
    return names


def evidence_names_from_result(result: Mapping[str, Any]) -> list[str]:
    names: list[str] = []
    collect_possible_names_from_value(result.get("evidence"), names)
    collect_possible_names_from_value(result.get("__tool_calls"), names)
    return unique_names(names)


def collect_possible_names_from_value(value: Any, names: list[str]) -> None:
    if isinstance(value, str):
        names.extend(possible_identity_names_from_text(value))
        return
    if isinstance(value, Mapping):
        for item in value.values():
            collect_possible_names_from_value(item, names)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            collect_possible_names_from_value(item, names)


def unique_names(names: Sequence[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for name in names:
        stripped = name.strip()
        normalized = stripped.lower()
        if not stripped or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(stripped)
    return unique


def warehouse_tool_definitions() -> list[dict[str, Any]]:
    return [sql_tool_definition(), show_schema_tool_definition()]


def schema_discovery_tools(tools: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    schema_tools = [tool for tool in tools if tool.get("name") == "show_schema"]
    return schema_tools or list(tools)


def sql_tool_definition() -> dict[str, Any]:
    return {
        "type": "function",
        "name": "sql",
        "description": (
            "Run one read-only ClickHouse SQL query against the personal data warehouse. "
            "Use this freely to inspect calendar, email, Slack, and related context while enriching the transcript. "
            "Call show_schema first so your query uses live table and column names. "
            "Only SELECT, WITH, SHOW, DESCRIBE, DESC, and EXPLAIN statements are accepted."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "query": {
                    "type": "string",
                    "description": "A single read-only ClickHouse SQL statement.",
                }
            },
            "required": ["query"],
        },
    }


def show_schema_tool_definition() -> dict[str, Any]:
    return {
        "type": "function",
        "name": "show_schema",
        "description": (
            "Return a compact ClickHouse schema overview for the current database. "
            "The overview uses currentDatabase(), SHOW TABLES, DESCRIBE TABLE, and up to three sampled rows per table. "
            "Sample cell values are truncated to keep the result compact. Use this before SQL when table or column names are unclear."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {},
        },
    }


def validation_repair_prompt(*, result: Mapping[str, Any], issues: Sequence[str]) -> str:
    return json.dumps(
        {
            "task": "Repair the previous structured JSON so it passes validation. Return corrected structured JSON only.",
            "validation_issues": list(issues),
            "repair_rules": [
                "Do not use tools in this repair turn; use the transcript, diarized_segments, tool evidence, and previous draft already in context.",
                "Preserve the transcript as a transcript. Do not summarize, omit substantive turns, or compress long turns into notes.",
                f"For long recordings, corrected_transcript may be exactly {LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL}; local code will assemble the detailed transcript from diarized_segments.",
                "Format corrected_transcript with one speaker turn per line or paragraph. Never put multiple 'Name:' speaker turns in one paragraph.",
                "Use full names from speaker_map as corrected_transcript prefixes for resolved people.",
                "Do not leave attendees or resolved speakers as one-token names when the existing evidence contains a fuller name.",
                "If the previous tool evidence only supports a one-token attendee name, explain the uncertainty in evidence and prefer the fuller candidate name found in Gmail/calendar context.",
                "If a diarization label is mixed, corrected_transcript can use turn-level attribution, but uncertain turns should use the mixed/unresolved speaker name from speaker_map.",
                "Keep meeting_notes and summary concise, but keep corrected_transcript faithful and detailed.",
            ],
            "previous_result": result,
        },
        sort_keys=True,
        default=str,
    )


def validate_enrichment_result(
    *,
    recording: Mapping[str, Any],
    transcript_segments: Sequence[Mapping[str, Any]],
    result: Mapping[str, Any],
) -> list[str]:
    issues: list[str] = []
    corrected = str(result.get("corrected_transcript") or "")
    source = str(recording.get("transcript_text") or "")
    local_assembly_requested = corrected.strip() == LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL
    if len(source) >= 5_000 and len(corrected) < int(len(source) * 0.60) and not local_assembly_requested:
        issues.append(
            f"corrected_transcript is too compressed: {len(corrected)} chars vs {len(source)} source chars; preserve substantive turns"
        )
    if not str(result.get("meeting_title") or "").strip():
        issues.append("meeting_title is empty; create a useful title even when there is no matched calendar event")
    if not parseable_model_datetime(str(result.get("meeting_start_at") or "")):
        issues.append("meeting_start_at is missing or invalid; use the calendar event start or the recording timestamp")
    if not parseable_model_datetime(str(result.get("meeting_end_at") or "")):
        issues.append("meeting_end_at is missing or invalid; use the calendar event end or estimate from transcript segment duration")

    speaker_map = [item for item in result.get("speaker_map", []) if isinstance(item, Mapping)]
    speaker_names = [str(item.get("speaker_name") or "").strip() for item in speaker_map]
    attendee_names = [str(name).strip() for name in result.get("attendees", []) if isinstance(name, str)]
    incomplete_attendees = incomplete_enrichment_person_names(result.get("attendees") or [])
    if incomplete_attendees:
        issues.append(
            f"attendees contain incomplete names: {incomplete_attendees[:5]}; keep researching calendar, Gmail, Slack, and candidate/context messages for full names"
        )

    for item in speaker_map:
        name = str(item.get("speaker_name") or "").strip()
        confidence = float(item.get("confidence") or 0)
        if malformed_enrichment_person_name(name):
            issues.append(
                f"speaker {item.get('speaker_label')} has malformed speaker_name {name!r}; use a verified full name or a clean mixed/unresolved label"
            )
        if confidence < 0.9 and name and not unresolved_speaker_name_for_enrichment(name):
            issues.append(
                f"speaker {item.get('speaker_label')} maps to resolved name {name!r} with low confidence {confidence:.2f}; either verify the person or mark the label mixed/unresolved"
            )
        if ambiguous_enrichment_person_name(name):
            issues.append(
                f"speaker {item.get('speaker_label')} has ambiguous speaker_name {name!r}; use one verified full name or a clearly mixed/unresolved label"
            )

    prefix_names = corrected_transcript_prefixes(corrected)
    resolved_prefix_names = [*speaker_names, *attendee_names]
    multi_prefix_lines = lines_with_multiple_speaker_prefixes(corrected, resolved_prefix_names)
    if multi_prefix_lines and not local_assembly_requested:
        issues.append(
            f"corrected_transcript has multiple speaker turns in one line/paragraph; split each speaker turn separately: {multi_prefix_lines[:2]}"
        )
    if not local_assembly_requested:
        for name in resolved_prefix_names:
            if unresolved_speaker_name_for_enrichment(name) or " " not in name:
                continue
            first = name.split()[0]
            if first in prefix_names and name not in prefix_names:
                issues.append(f"corrected_transcript uses first-name prefix {first!r}; use full speaker name {name!r}")

    opening_text = "\n".join(str(segment.get("text") or "") for segment in transcript_segments[:6])
    opening_addressee = re.search(r"\bhow are you,\s*([A-Z][A-Za-z]+)\b", opening_text, flags=re.IGNORECASE)
    if opening_addressee and not local_assembly_requested:
        addressee = opening_addressee.group(1)
        phrase = rf"\bhow are you,\s*{re.escape(addressee)}\b"
        if not re.search(rf":[^\n]{{0,160}}{phrase}", corrected, flags=re.IGNORECASE):
            issues.append(
                f"opening dialogue lost addressee evidence: raw early segment has 'How are you, {addressee}?' and corrected_transcript should preserve that addressed turn"
            )
        elif re.search(rf"(?m)^{re.escape(addressee)}(?:\s+\S+)?:[^\n]{{0,160}}{phrase}", corrected, flags=re.IGNORECASE):
            issues.append(
                f"opening dialogue has likely self-address: a line prefixed by {addressee!r} also says 'How are you, {addressee}?'"
            )

    if same_speaker_asks_and_answers_opening_greeting(corrected) and not local_assembly_requested:
        issues.append(
            "opening dialogue has the same speaker asking 'how are you?' and then answering 'I'm doing great'; reassign or merge the short greeting turn"
        )

    return issues


def incomplete_enrichment_person_names(names: Any) -> list[str]:
    if not isinstance(names, Sequence) or isinstance(names, (str, bytes)):
        return []
    incomplete: list[str] = []
    for name in names:
        if not isinstance(name, str):
            continue
        stripped = name.strip()
        if not stripped or unresolved_speaker_name_for_enrichment(stripped):
            continue
        if "@" in stripped:
            incomplete.append(stripped)
            continue
        if "(" in stripped and ")" in stripped:
            incomplete.append(stripped)
            continue
        if len(stripped.split()) == 1:
            incomplete.append(stripped)
    return incomplete


def ambiguous_enrichment_person_name(name: str) -> bool:
    if not name:
        return False
    normalized = f" {name.lower()} "
    if unresolved_speaker_name_for_enrichment(name):
        if any(marker in normalized for marker in (" likely ", " maybe ", " probably ", " possibly ")) or ";" in name or ":" in name:
            return True
        if unresolved_speaker_name_contains_person_guess(name):
            return True
        if " or " in normalized:
            return True
        return bool(re.search(r"\b[A-Z][a-z]+ [A-Z][a-z]+\s*/\s*[A-Z][a-z]+ [A-Z][a-z]+", name))
    return " or " in normalized or "/" in name


def malformed_enrichment_person_name(name: str) -> bool:
    if not name or unresolved_speaker_name_for_enrichment(name):
        return False
    return "@" in name or ("(" in name and ")" in name)


def unresolved_speaker_name_contains_person_guess(name: str) -> bool:
    allowed = {"Unresolved Speaker"}
    for match in re.findall(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", name):
        if match not in allowed:
            return True
    return False


def apply_segment_preserving_transcript_fallback(
    *,
    recording: Mapping[str, Any],
    transcript_segments: Sequence[Mapping[str, Any]],
    result: Mapping[str, Any],
) -> dict[str, Any]:
    current = dict(result)
    issues = list(current.get("__validation_issues") or [])
    local_assembly_requested = str(current.get("corrected_transcript") or "").strip() == LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL
    if not local_assembly_requested and not any(str(issue).startswith("corrected_transcript is too compressed") for issue in issues):
        return current
    if not transcript_segments:
        return current

    current["corrected_transcript"] = build_segment_preserving_corrected_transcript(
        transcript_segments=transcript_segments,
        result=current,
    )
    current.setdefault("evidence", [])
    if isinstance(current["evidence"], list):
        reason = (
            "Corrected transcript assembled locally from diarized segments because the recording is long."
            if local_assembly_requested
            else "Corrected transcript rebuilt from diarized segments after model output was too compressed."
        )
        current["evidence"].append(reason)
    remaining_issues = validate_enrichment_result(recording=recording, transcript_segments=transcript_segments, result=current)
    if remaining_issues:
        current["__validation_issues"] = remaining_issues
    else:
        current.pop("__validation_issues", None)
    return current


def ensure_recording_level_fields(
    *,
    recording: Mapping[str, Any],
    transcript_segments: Sequence[Mapping[str, Any]],
    result: Mapping[str, Any],
) -> dict[str, Any]:
    current = dict(result)
    calendar_event_id = str(current.get("calendar_event_id") or "").strip()
    if calendar_event_id:
        return current

    start, end = recording_time_bounds(recording=recording, transcript_segments=transcript_segments)
    filled = False
    if not parseable_model_datetime(str(current.get("meeting_start_at") or "")):
        current["meeting_start_at"] = start.isoformat()
        filled = True
    if not parseable_model_datetime(str(current.get("meeting_end_at") or "")):
        current["meeting_end_at"] = end.isoformat()
        filled = True
    if not str(current.get("meeting_title") or "").strip():
        current["meeting_title"] = f"Voice Memo {start.strftime('%Y-%m-%d %H:%M UTC')}"
        filled = True
    if filled:
        current.setdefault("evidence", [])
        if isinstance(current["evidence"], list):
            current["evidence"].append(
                "No matching calendar event was selected; recording-level title/time fields were filled from the recording timestamp and transcript segment duration."
            )
    return current


def recording_time_bounds(
    *,
    recording: Mapping[str, Any],
    transcript_segments: Sequence[Mapping[str, Any]],
) -> tuple[datetime, datetime]:
    recorded_at = recording.get("recorded_at")
    if isinstance(recorded_at, datetime):
        start = recorded_at if recorded_at.tzinfo else recorded_at.replace(tzinfo=UTC)
        start = start.astimezone(UTC)
    else:
        start = datetime.fromtimestamp(0, tz=UTC)

    max_end_ms = 0
    for segment in transcript_segments:
        try:
            max_end_ms = max(max_end_ms, int(segment.get("end_ms") or 0))
        except (TypeError, ValueError):
            continue
    end = start + timedelta(milliseconds=max_end_ms) if max_end_ms > 0 else start
    return start, end


def build_segment_preserving_corrected_transcript(
    *,
    transcript_segments: Sequence[Mapping[str, Any]],
    result: Mapping[str, Any],
) -> str:
    speaker_map = [item for item in result.get("speaker_map", []) if isinstance(item, Mapping)]
    verified_names = [name for name in result.get("attendees", []) if isinstance(name, str)]
    speaker_names = [str(item.get("speaker_name") or "") for item in speaker_map]
    evidence_names = evidence_names_from_result(result)
    known_speaker_names = list(
        dict.fromkeys(
            [
                *verified_names,
                *[
                    name
                    for name in speaker_names
                    if " " in name and not unresolved_speaker_name_for_enrichment(name)
                ],
            ]
        )
    )
    known_text_names = list(dict.fromkeys([*known_speaker_names, *evidence_names]))
    label_to_name = {
        str(item.get("speaker_label") or ""): segment_preserving_name_for_label(item, known_names=known_speaker_names)
        for item in speaker_map
    }
    local_opening_names = opening_dialogue_local_speaker_names(transcript_segments, known_names=known_speaker_names)

    lines = []
    for segment in transcript_segments:
        segment_index = int(segment.get("segment_index") or 0)
        label = str(segment.get("speaker_label") or "")
        speaker = local_opening_names.get(segment_index) or label_to_name.get(label) or f"Speaker {label}".strip()
        text = canonicalize_text_verified_name_mentions(str(segment.get("text") or ""), verified_names=known_text_names)
        lines.append(f"{speaker}: {text}")
    return "\n".join(lines)


def segment_preserving_name_for_label(item: Mapping[str, Any], *, known_names: Sequence[str]) -> str:
    name = str(item.get("speaker_name") or "").strip()
    if name and not unresolved_speaker_name_for_enrichment(name):
        return name
    evidence = str(item.get("evidence") or "")
    for known_name in known_names:
        if known_name and re.search(rf"\bmostly\b[^.]{{0,60}}\b{re.escape(known_name)}\b", evidence, flags=re.IGNORECASE):
            return known_name
    for known_name in known_names:
        if known_name and re.search(rf"\b{re.escape(known_name)}\b", evidence, flags=re.IGNORECASE):
            return known_name
    return name or "Unresolved Speaker"


def opening_dialogue_local_speaker_names(
    transcript_segments: Sequence[Mapping[str, Any]],
    *,
    known_names: Sequence[str],
) -> dict[int, str]:
    if len(transcript_segments) < 4:
        return {}
    first_text = str(transcript_segments[0].get("text") or "")
    third_text = str(transcript_segments[2].get("text") or "")
    addressee_match = re.search(r"\bhey,?\s+([A-Z][A-Za-z]+)\b", first_text, flags=re.IGNORECASE)
    greeter_match = re.search(r"\bhow are you,\s*([A-Z][A-Za-z]+)\b", third_text, flags=re.IGNORECASE)
    if not addressee_match or not greeter_match:
        return {}
    addressee_name = full_name_for_first_name(addressee_match.group(1), known_names)
    greeter_name = full_name_for_first_name(greeter_match.group(1), known_names)
    if not addressee_name or not greeter_name:
        return {}

    local = {
        int(transcript_segments[0].get("segment_index") or 0): greeter_name,
        int(transcript_segments[2].get("segment_index") or 0): addressee_name,
    }
    second_text = str(transcript_segments[1].get("text") or "").strip().lower()
    if re.fullmatch(r"hey,?\s+how are you\??", second_text):
        local[int(transcript_segments[1].get("segment_index") or 0)] = greeter_name
    fourth_text = str(transcript_segments[3].get("text") or "").strip().lower()
    if re.match(r"^(i['’]?m|i am) (good|well|great)", fourth_text):
        local[int(transcript_segments[3].get("segment_index") or 0)] = greeter_name
    return local


def full_name_for_first_name(first_name: str, known_names: Sequence[str]) -> str:
    normalized = first_name.strip().lower()
    for name in known_names:
        parts = name.split()
        if parts and parts[0].lower() == normalized:
            return name
    for name in known_names:
        parts = name.split()
        if parts and should_canonicalize_name_token(first_name, parts[0]):
            return name
    return ""


def corrected_transcript_prefixes(corrected: str) -> set[str]:
    prefixes = set()
    for paragraph in re.split(r"\n+", corrected):
        prefix, separator, _rest = paragraph.partition(":")
        if separator and prefix.strip():
            prefixes.add(prefix.strip())
    return prefixes


def same_speaker_asks_and_answers_opening_greeting(corrected: str) -> bool:
    parsed = corrected_turns(corrected)[:8]
    for (speaker, text), (next_speaker, next_text) in zip(parsed, parsed[1:], strict=False):
        if speaker != next_speaker:
            continue
        if re.search(r"\bhow are you\??$", text.lower()) and re.search(
            r"^(i['’]?m|i am) doing (great|good|well)",
            next_text.lower(),
        ):
            return True
    return False


def corrected_turns(corrected: str) -> list[tuple[str, str]]:
    pattern = re.compile(r"(?P<prefix>[A-Z][^:\n]{1,80}):")
    matches = list(pattern.finditer(corrected))
    turns: list[tuple[str, str]] = []
    for index, match in enumerate(matches):
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(corrected)
        turns.append((match.group("prefix").strip(), corrected[start:end].strip()))
    return turns


def lines_with_multiple_speaker_prefixes(corrected: str, speaker_names: Sequence[str]) -> list[str]:
    names = [re.escape(name) for name in speaker_names if name and not unresolved_speaker_name_for_enrichment(name)]
    if not names:
        return []
    pattern = re.compile(rf"\b(?:{'|'.join(names)}):")
    bad = []
    for line in [line.strip() for line in corrected.splitlines() if line.strip()]:
        if len(pattern.findall(line)) > 1:
            bad.append(line[:180])
    return bad


def unresolved_speaker_name_for_enrichment(name: str) -> bool:
    normalized = name.lower()
    return (
        "speaker" in normalized
        or "unresolved" in normalized
        or "mixed" in normalized
        or "interviewer" in normalized
    )


def enrichment_system_prompt() -> str:
    return (
        "You enrich transcripts from personal Voice Memos. Return only structured JSON matching the schema. "
        "Before final JSON, you must use warehouse tools: first call show_schema to inspect live table/column names, then use sql for read-only investigation. "
        "Use read-only queries to verify likely calendar matches, attendees, speaker identities, and domain terms. "
        "The non-negotiable requirements are accurate meeting time/date, accurate attendees and name spellings, and accurate domain terms in corrected_transcript. "
        "Do not invent calendar links, people, or locations. If uncertain, set low confidence and explain the uncertainty in evidence. "
        "Keep corrected_transcript faithful to the transcript; put synthesized narrative writing only in meeting_notes and summary."
    )


def enrichment_user_prompt(
    *,
    recording: Mapping[str, Any],
    calendar_candidates: Sequence[Mapping[str, Any]],
    transcript_segments: Sequence[Mapping[str, Any]] = (),
) -> str:
    transcript = str(recording.get("transcript_text", ""))
    if len(transcript) > 60_000:
        transcript = transcript[:60_000]
    return json.dumps(
        {
            "recording": {
                "recording_id": recording.get("recording_id"),
                "recorded_at": recording.get("recorded_at").isoformat()
                if hasattr(recording.get("recorded_at"), "isoformat")
                else str(recording.get("recorded_at")),
                "title": recording.get("title"),
                "transcript_char_count": len(str(recording.get("transcript_text", ""))),
            },
            "local_transcript_assembly": {
                "sentinel": LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
                "use_when_transcript_char_count_at_least": LOCAL_TRANSCRIPT_ASSEMBLY_MIN_SOURCE_CHARS,
                "explanation": "For long recordings, return the sentinel as corrected_transcript. The pipeline assembles the full detailed transcript locally from diarized_segments after metadata, identity, and term research are complete.",
            },
            "recorded_at_interpretations": recording_time_interpretations(recording.get("recorded_at"))
            if isinstance(recording.get("recorded_at"), datetime)
            else [],
            "calendar_candidates": list(calendar_candidates),
            "transcript": transcript,
            "diarized_segments": list(transcript_segments),
            "instructions": [
                "Before final output, call show_schema first, then use multiple focused sql tool calls to search calendar, email, Slack, and related warehouse context.",
                "Make separate warehouse checks for: the selected calendar event including attendee data, attendee/person identity evidence, and suspicious domain terms or organizations that need spelling verification.",
                "Hard requirements: accurate meeting date/time, accurate attendees and name spellings, and accurate domain terms in corrected_transcript.",
                "attendees means actual meeting participants who speak or are strongly evidenced as present. Do not include calendar invitees merely because they were invited, especially if responseStatus is needsAction and there is no transcript evidence they attended.",
                "If you select a calendar event, meeting_start_at, meeting_end_at, and meeting_location must come from that calendar event or verified ClickHouse context.",
                "Do not leave attendees as raw email addresses when a full name can be resolved. If a calendar attendee has only an email address, use show_schema results to query Slack/email identity data before finalizing attendees or speaker_map.",
                "Do not stop at a one-token name like a Slack display_name or calendar first name. Search Gmail/calendar/Slack context around the event title, email local part, usernames, candidate briefs, and prior messages until you find a full preferred/legal name or have clear evidence none is available.",
                "When a speaker is identified only by a first name, use calendar attendee emails plus Slack/email identity evidence to resolve the full name.",
                "Calendar candidates may include identity_hints for attendee emails. Use possible_names from identity_hints for attendee and speaker names when supported by transcript evidence.",
                "Normalize spoken name mentions in corrected_transcript to verified attendee spellings when ASR produces a close variant, especially in greetings and introductions.",
                "If the transcript says a person or organization name differently than the calendar candidate, query for that spoken name/project before deciding.",
                "For technical/product terms that are unclear or unfamiliar, query Slack/Gmail for likely spelling variants and use the spelling supported by warehouse evidence.",
                "Correct ASR domain-term errors in corrected_transcript, for example Hack Club not Hat Club, Hackatime not Hackertime/Hacker Time, Stardance not Start Dance, OpenRouter, OpenAI, Anthropic, Congressional App Challenge, Challenger, Framework, Spindrift, and Pellegrino.",
                "The source timestamp may be a local wall-clock time incorrectly tagged as UTC. Use recorded_at_interpretations and transcript evidence when matching calendar events.",
                "Direct spoken names, greetings, introductions, and project names are strong evidence. If they conflict with a nearby calendar candidate, query for the spoken name/project before selecting the candidate.",
                "Pick the best calendar event if supported by transcript/time/context evidence.",
                "If no calendar event matches, set calendar_event_id to an empty string and calendar_confidence to 0, but still produce a useful meeting_title, meeting_start_at, meeting_end_at, summary, topics, action_items, evidence, and corrected_transcript. Personal journal entries or ad-hoc voice notes are valid outputs.",
                "For no-calendar recordings, derive meeting_start_at from the recording timestamp and estimate meeting_end_at from diarized segment duration when available.",
                "speaker_map must summarize each original diarized_segments speaker_label as a real person only when that label is stable; otherwise map it to an unresolved mixed label. Do not invent generic speaker labels that are not present in diarized_segments.",
                "If a speaker label might be one of several interviewers, do not write a candidate list as the speaker_name. Use a clearly mixed/unresolved label and put the candidate names in evidence unless you can verify a single person.",
                "Assign real speaker names only when directly supported by greetings, self-introductions, calendar attendees, Slack/email identity evidence, or strongly identifying transcript context. Use low confidence and an unresolved label when uncertain.",
                "Audit every diarized speaker_label across all its turns before assigning a real person. If one label contains turns from multiple apparent people, contradictory greetings, or identity-bearing turns with very low confidence, mark that label as mixed/unresolved instead of assigning a real name.",
                "In greetings, 'Hey NAME' or 'How are you, NAME?' usually means NAME is the addressee, not the speaker. Do not swap addressee and speaker when correcting transcript prefixes.",
                "Do not infer a diarized label's identity from a single low-confidence short greeting or interjection. Prefer the label's longer, high-confidence turns and conversational consistency.",
                "If a speaker label says lines that only the addressee could say, such as 'How are you, PERSON?' under PERSON's prefix, your mapping is wrong. Re-evaluate the speaker_map or mark the label mixed/unresolved.",
                "For opening small talk, reason as a dialogue chain: greeting to addressee, addressee response, return question, original speaker response. Use that chain to validate speaker labels before final output.",
                "Opening greetings are often split across diarization labels. If a low-confidence initial 'Hey NAME' is followed by another label saying 'hey/how are you' and a third label replies 'I'm doing great. How are you, PERSON?', attribute the greeting fragments to PERSON when that matches the later stable speaker, not to a different participant who only appears later.",
                "Before final output, scan corrected_transcript for self-address contradictions. A line prefixed by PERSON must not contain 'How are you, PERSON?' or similar direct address to the same person. In an opening chain, 'I'm doing great. How are you, PERSON?' is spoken by the person who was just greeted, and the next 'I'm good' is PERSON's reply.",
                "In opening greetings, the same speaker should not ask 'how are you?' and then immediately answer 'I'm doing great'. If diarization splits 'Hey NAME, how are you?' into two short segments, merge or attribute those short greeting fragments to the greeter.",
                "If the first few short greeting segments conflict with later stable diarization, prefer a coherent dialogue chain over the raw short-segment labels. It is better to merge or reattribute short opening greetings than to create a transcript where someone asks how they themselves are.",
                "Use diarized_segments as the source of truth for speaker turns. Preserve chronological turn order in corrected_transcript.",
                f"If transcript_char_count is at least {LOCAL_TRANSCRIPT_ASSEMBLY_MIN_SOURCE_CHARS}, set corrected_transcript exactly to {LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL}. Do not emit the full transcript in your JSON for long recordings; focus on calendar matching, attendee identities, speaker_map, domain-term evidence, notes, topics, and action items.",
                "When you are not using the local transcript assembly sentinel, every substantive diarized segment should be represented in corrected_transcript. Do not compress it into a summary.",
                "Format corrected_transcript as speaker turns, one turn per line or paragraph. Never put multiple 'Name:' speaker turns in the same paragraph.",
                "Use full resolved person names as turn prefixes, for example 'Person One:' instead of 'Person:'.",
                "Corrected_transcript attribution is turn-level: do not blindly apply a global speaker label when diarization is mixed. If a low-confidence or contradictory segment has local dialogue evidence for a different speaker, prefix that turn with the locally supported person or an unresolved label.",
                "Do not attribute an opening low-confidence label to a later stable speaker if the dialogue chain contradicts it. For example, a label that later belongs to one participant may still have an early greeting turn that should be another participant or unresolved based on surrounding dialogue.",
                "Only replace an original diarization label with a real person name when local turn evidence or stable speaker_map evidence supports it. If a label is mixed, unstable, or below 0.9 confidence, speaker_map should say mixed/unresolved, while corrected_transcript may still use real names for individual turns that have strong local evidence.",
                "Write corrected_transcript as a faithful speaker-labeled transcript. Correct obvious ASR errors, names, punctuation, and paragraph breaks, but do not summarize, reorder, omit substantive sections, merge unrelated turns, or convert the transcript into prose notes.",
                "Write meeting_notes as a readable narrative brief. This can be synthesized and compressed.",
                "Create a concise useful meeting title.",
                "Extract attendees, topics, and action items.",
            ],
            "clickhouse_context": {
                "result_format": "Tool results are CSV with truncation metadata.",
                "query_notes": [
                    "Use show_schema output to identify table names, column names, and whether fields are scalar or arrays before writing sql.",
                    "For array columns, use array functions such as arrayExists instead of scalar string functions.",
                    "Compare DateTime columns with toDateTime64('YYYY-MM-DD HH:MM:SS', 3, 'UTC') or a range; do not compare DateTime columns to ISO strings with timezone suffixes.",
                    "Do not add FORMAT clauses; the tool already returns CSV.",
                    "Prefer small LIMITs and focused SELECT columns.",
                ],
            },
        },
        sort_keys=True,
        default=str,
    )


def enrichment_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "calendar_event_id": {"type": "string"},
            "calendar_confidence": {"type": "number"},
            "meeting_title": {"type": "string"},
            "meeting_start_at": {"type": "string"},
            "meeting_end_at": {"type": "string"},
            "meeting_location": {"type": "string"},
            "attendees": {"type": "array", "items": {"type": "string"}},
            "speaker_map": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "speaker_label": {
                            "type": "string",
                            "description": "Exact original speaker_label from diarized_segments, for example A, B, Speaker 1, or speaker_0.",
                        },
                        "speaker_name": {
                            "type": "string",
                            "description": "Resolved person name, or an unresolved stable label if identity is not supported.",
                        },
                        "confidence": {
                            "type": "number",
                            "description": "0 to 1 confidence that this original diarization label belongs to speaker_name.",
                        },
                        "evidence": {
                            "type": "string",
                            "description": "Brief evidence for the mapping, including uncertainty where relevant.",
                        },
                    },
                    "required": ["speaker_label", "speaker_name", "confidence", "evidence"],
                },
            },
            "corrected_transcript": {"type": "string"},
            "meeting_notes": {"type": "string"},
            "summary": {"type": "string"},
            "topics": {"type": "array", "items": {"type": "string"}},
            "action_items": {"type": "array", "items": {"type": "string"}},
            "evidence": {"type": "array", "items": {"type": "string"}},
        },
        "required": [
            "calendar_event_id",
            "calendar_confidence",
            "meeting_title",
            "meeting_start_at",
            "meeting_end_at",
            "meeting_location",
            "attendees",
            "speaker_map",
            "corrected_transcript",
            "meeting_notes",
            "summary",
            "topics",
            "action_items",
            "evidence",
        ],
    }


def enrichment_row(
    *,
    recording: Mapping[str, Any],
    result: Mapping[str, Any],
    provider: str,
    model: str,
    prompt_version: str,
    status: str,
    error: str,
    created_at: datetime,
) -> dict[str, Any]:
    result = canonicalize_result_verified_name_mentions(result)
    speaker_names = speaker_names_from_result(result)
    corrected_transcript = normalize_corrected_transcript_prefixes(
        str(result.get("corrected_transcript") or result.get("cleaned_transcript") or ""),
        speaker_names=speaker_names,
    )
    meeting_notes = str(result.get("meeting_notes") or result.get("summary") or "")
    return {
        "account": str(recording.get("account", "")),
        "recording_id": str(recording.get("recording_id", "")),
        "provider": provider,
        "model": model,
        "prompt_version": prompt_version,
        "status": status,
        "error": error,
        "calendar_event_id": str(result.get("calendar_event_id", "")),
        "calendar_confidence": float(result.get("calendar_confidence", 0) or 0),
        "meeting_title": str(result.get("meeting_title", "")),
        "meeting_start_at": parse_model_datetime(str(result.get("meeting_start_at", ""))),
        "meeting_end_at": parse_model_datetime(str(result.get("meeting_end_at", ""))),
        "meeting_location": str(result.get("meeting_location", "")),
        "attendees_json": json.dumps(result.get("attendees") or [], sort_keys=True, separators=(",", ":")),
        "speaker_map_json": json.dumps(result.get("speaker_map") or [], sort_keys=True, separators=(",", ":")),
        "cleaned_transcript": corrected_transcript,
        "corrected_transcript": corrected_transcript,
        "meeting_notes": meeting_notes,
        "summary": str(result.get("summary", "")),
        "topics_json": json.dumps(result.get("topics") or [], sort_keys=True, separators=(",", ":")),
        "action_items_json": json.dumps(result.get("action_items") or [], sort_keys=True, separators=(",", ":")),
        "evidence_json": json.dumps(result.get("evidence") or [], sort_keys=True, separators=(",", ":")),
        "raw_result_json": json.dumps(result, sort_keys=True, separators=(",", ":")),
        "created_at": created_at,
        "sync_version": int(created_at.timestamp() * 1_000_000),
    }


def failed_enrichment_row(
    *,
    recording: Mapping[str, Any],
    provider: str,
    model: str,
    prompt_version: str,
    error: str,
    created_at: datetime,
) -> dict[str, Any]:
    return enrichment_row(
        recording=recording,
        result={
            "calendar_event_id": "",
            "calendar_confidence": 0,
            "meeting_title": "",
            "meeting_start_at": "",
            "meeting_end_at": "",
            "meeting_location": "",
            "attendees": [],
            "speaker_map": [],
            "corrected_transcript": "",
            "meeting_notes": "",
            "summary": "",
            "topics": [],
            "action_items": [],
            "evidence": [],
        },
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        status="error",
        error=error,
        created_at=created_at,
    )


def speaker_names_from_result(result: Mapping[str, Any]) -> list[str]:
    speaker_map = result.get("speaker_map") or []
    if not isinstance(speaker_map, list):
        return []
    names = []
    for item in speaker_map:
        if isinstance(item, Mapping) and item.get("speaker_name"):
            names.append(str(item["speaker_name"]))
    return names


def canonicalize_result_verified_name_mentions(result: Mapping[str, Any]) -> dict[str, Any]:
    verified_names = verified_human_names_from_result(result)
    if not verified_names:
        return dict(result)
    return {
        str(key): (value if key == "__tool_calls" else canonicalize_value_verified_name_mentions(value, verified_names))
        for key, value in result.items()
    }


def verified_human_names_from_result(result: Mapping[str, Any]) -> list[str]:
    names: list[str] = []
    for attendee in result.get("attendees") or []:
        if isinstance(attendee, str) and attendee.strip() and "@" not in attendee:
            names.append(attendee.strip())
    names.extend(speaker_names_from_result(result))

    unique: list[str] = []
    seen: set[str] = set()
    for name in names:
        normalized = name.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        unique.append(name)
    return unique


def canonicalize_value_verified_name_mentions(value: Any, verified_names: Sequence[str]) -> Any:
    if isinstance(value, str):
        return canonicalize_text_verified_name_mentions(value, verified_names=verified_names)
    if isinstance(value, list):
        return [canonicalize_value_verified_name_mentions(item, verified_names) for item in value]
    if isinstance(value, dict):
        return {
            key: canonicalize_value_verified_name_mentions(item, verified_names)
            for key, item in value.items()
        }
    return value


def canonicalize_text_verified_name_mentions(text: str, *, verified_names: Sequence[str]) -> str:
    if not text:
        return text
    replacements: dict[str, str] = {}
    for name in verified_names:
        text = canonicalize_full_name_mentions(text, verified_name=name)
        first_name = name.split()[0] if name.split() else ""
        if len(first_name) < 4:
            continue
        for token in set(re.findall(r"\b[A-Z][A-Za-z]{3,}\b", text)):
            if should_canonicalize_name_token(token, first_name):
                replacements[token] = first_name
    for token, replacement in sorted(replacements.items(), key=lambda item: len(item[0]), reverse=True):
        text = re.sub(rf"\b{re.escape(token)}\b", replacement, text)
    return text


def canonicalize_full_name_mentions(text: str, *, verified_name: str) -> str:
    parts = verified_name.split()
    if len(parts) < 2:
        return text
    first_name = parts[0]
    last_name = parts[-1]
    if len(first_name) < 3 or len(last_name) < 4:
        return text
    pattern = re.compile(rf"\b{re.escape(first_name)}\s+([A-Z][A-Za-z]{{3,}})\b")

    def replace(match: re.Match[str]) -> str:
        spoken_last = match.group(1)
        if spoken_last == last_name:
            return match.group(0)
        if should_canonicalize_name_token(spoken_last, last_name):
            return f"{first_name} {last_name}"
        return match.group(0)

    return pattern.sub(replace, text)


def should_canonicalize_name_token(token: str, verified_first_name: str) -> bool:
    if token.lower() == verified_first_name.lower():
        return False
    if abs(len(token) - len(verified_first_name)) > 2:
        return False
    if token[0].lower() == verified_first_name[0].lower():
        ratio = SequenceMatcher(a=token.lower(), b=verified_first_name.lower()).ratio()
        return ratio >= 0.78 or (len(token) >= 5 and len(verified_first_name) >= 5 and ratio >= 0.6)
    return SequenceMatcher(
        a=phonetic_name_key(token),
        b=phonetic_name_key(verified_first_name),
    ).ratio() >= 0.75


def phonetic_name_key(name: str) -> str:
    normalized = name.lower()
    if normalized[:1] in {"k", "q"}:
        normalized = "c" + normalized[1:]
    normalized = normalized.replace("y", "i")
    return normalized


def normalize_corrected_transcript_prefixes(corrected_transcript: str, *, speaker_names: Sequence[str]) -> str:
    if not corrected_transcript or not speaker_names:
        return corrected_transcript
    allowed = set(speaker_names)
    paragraphs = [paragraph.strip() for paragraph in corrected_transcript.split("\n\n")]
    normalized: list[str] = []
    current_speaker = ""
    for paragraph in paragraphs:
        if not paragraph:
            continue
        prefix, separator, rest = paragraph.partition(":")
        stripped_prefix = prefix.strip()
        if separator and stripped_prefix in allowed:
            current_speaker = stripped_prefix
            normalized.append(f"{stripped_prefix}: {rest.strip()}")
            continue
        if current_speaker and should_prefix_as_continuation(paragraph, stripped_prefix, bool(separator)):
            normalized.append(f"{current_speaker}: {paragraph}")
            continue
        normalized.append(paragraph)
    return "\n\n".join(normalized)


def should_prefix_as_continuation(paragraph: str, prefix_before_colon: str, has_colon: bool) -> bool:
    if not has_colon:
        return True
    words = prefix_before_colon.split()
    if len(words) >= 4:
        return True
    if len(prefix_before_colon) >= 40:
        return True
    if prefix_before_colon[:1].islower():
        return True
    return False


def parse_model_datetime(value: str) -> datetime:
    if not value:
        return datetime.fromtimestamp(0, tz=UTC)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.fromtimestamp(0, tz=UTC)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parseable_model_datetime(value: str) -> bool:
    if not value:
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def response_output_text(payload: Mapping[str, Any]) -> str:
    if text := payload.get("output_text"):
        return str(text)
    for item in payload.get("output", []) or []:
        if not isinstance(item, Mapping):
            continue
        for content in item.get("content", []) or []:
            if isinstance(content, Mapping) and content.get("type") in {"output_text", "text"}:
                if "text" in content:
                    return str(content["text"])
    raise RuntimeError("OpenAI response did not contain output text")


def response_function_calls(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    calls: list[Mapping[str, Any]] = []
    for item in payload.get("output", []) or []:
        if isinstance(item, Mapping) and item.get("type") == "function_call":
            calls.append(item)
    return calls


def parse_function_arguments(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, Mapping) else {}


def summarize_tool_arguments(arguments: Mapping[str, Any]) -> str:
    if sql := (arguments.get("query") or arguments.get("sql")):
        compact = " ".join(str(sql).split())
        return f"sql={compact[:240]}"
    return json.dumps(dict(arguments), sort_keys=True, default=str)[:240]


def summarize_tool_output(output: Mapping[str, Any]) -> str:
    if error := output.get("error"):
        return f"error={str(error)[:240]}"
    csv_text = str(output.get("csv", ""))
    rows = max(0, len(csv_text.splitlines()) - 1) if csv_text else 0
    truncated = output.get("truncated") if isinstance(output.get("truncated"), Mapping) else {}
    return (
        f"rows={rows} truncated_rows={bool(truncated.get('rows'))} "
        f"truncated_fields={len(truncated.get('fields') or [])}"
    )
