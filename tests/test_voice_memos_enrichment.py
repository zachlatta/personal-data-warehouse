from __future__ import annotations

from datetime import UTC, datetime

import requests

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.voice_memos_enrichment import (
    ClickHouseEnrichmentTool,
    ENRICHMENT_PROMPT_VERSION,
    LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
    OpenAIResponsesClient,
    apply_segment_preserving_transcript_fallback,
    ensure_recording_level_fields,
    enrichment_row,
    enrichment_schema,
    enrichment_user_prompt,
    load_enrichment_candidates,
    load_calendar_candidates,
    normalize_corrected_transcript_prefixes,
    canonicalize_text_verified_name_mentions,
    load_attendee_identity_hints,
    parse_attendee_summaries,
    possible_identity_names_from_text,
    parse_function_arguments,
    recording_time_interpretations,
    response_function_calls,
    response_output_text,
    show_schema_tool_definition,
    sql_tool_definition,
    summarize_tool_arguments,
    summarize_tool_output,
    validate_enrichment_result,
    warehouse_tool_definitions,
)


class FakeResponse:
    def __init__(self, payload) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self) -> None:
        self.posts = []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return FakeResponse(
            {
                "output": [
                    {
                        "content": [
                            {
                                "type": "output_text",
                                "text": (
                                    '{"calendar_event_id":"","calendar_confidence":0,"meeting_title":"Test",'
                                    '"meeting_start_at":"","meeting_end_at":"","meeting_location":"","attendees":[],'
                                    '"speaker_map":[],"corrected_transcript":"Speaker A: Hello","meeting_notes":"Notes","summary":"Summary",'
                                    '"topics":[],"action_items":[],"evidence":[]}'
                                ),
                            }
                        ]
                    }
                ]
            }
        )


class FakeToolCallingSession:
    def __init__(self) -> None:
        self.posts = []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        if len(self.posts) == 1:
            return FakeResponse(
                {
                    "output": [
                        {
                            "type": "function_call",
                            "call_id": "call_1",
                            "name": "sql",
                            "arguments": '{"query":"SELECT summary FROM calendar_events LIMIT 1"}',
                        }
                    ]
                }
            )
        return FakeResponse(
            {
                "output": [
                    {
                        "content": [
                            {
                                "type": "output_text",
                                "text": (
                                    '{"calendar_event_id":"event1","calendar_confidence":0.9,"meeting_title":"Tool Matched",'
                                    '"meeting_start_at":"2026-04-27T10:00:00+00:00","meeting_end_at":"2026-04-27T11:00:00+00:00",'
                                    '"meeting_location":"","attendees":[],"speaker_map":[],"corrected_transcript":"Speaker A: Hello",'
                                    '"meeting_notes":"Notes","summary":"Summary","topics":[],"action_items":[],"evidence":["Used ClickHouse"]}'
                                ),
                            }
                        ]
                    }
                ]
            }
        )


class FakeTransientFailureSession:
    def __init__(self) -> None:
        self.posts = []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        if len(self.posts) == 1:
            raise requests.exceptions.ReadTimeout("temporary timeout")
        return FakeResponse(
            {
                "output": [
                    {
                        "content": [
                            {
                                "type": "output_text",
                                "text": (
                                    '{"calendar_event_id":"","calendar_confidence":0,"meeting_title":"Retried",'
                                    '"meeting_start_at":"","meeting_end_at":"","meeting_location":"","attendees":[],'
                                    '"speaker_map":[],"corrected_transcript":"Speaker A: Hello","meeting_notes":"Notes",'
                                    '"summary":"Summary","topics":[],"action_items":[],"evidence":[]}'
                                ),
                            }
                        ]
                    }
                ]
            }
        )


class FakeToolRequiredRetrySession:
    def __init__(self) -> None:
        self.posts = []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        if len(self.posts) == 1:
            return FakeResponse(
                {
                    "output": [
                        {
                            "content": [
                                {
                                    "type": "output_text",
                                    "text": (
                                        '{"calendar_event_id":"","calendar_confidence":0,"meeting_title":"Skipped Tool",'
                                        '"meeting_start_at":"","meeting_end_at":"","meeting_location":"","attendees":[],'
                                        '"speaker_map":[],"corrected_transcript":"Speaker A: Hello","meeting_notes":"Notes",'
                                        '"summary":"Summary","topics":[],"action_items":[],"evidence":[]}'
                                    ),
                                }
                            ]
                        }
                    ]
                }
            )
        if len(self.posts) == 2:
            return FakeResponse(
                {
                    "output": [
                        {
                            "type": "function_call",
                            "call_id": "call_1",
                            "name": "show_schema",
                            "arguments": "{}",
                        }
                    ]
                }
            )
        return FakeResponse(
            {
                "output": [
                    {
                        "content": [
                            {
                                "type": "output_text",
                                "text": (
                                    '{"calendar_event_id":"event1","calendar_confidence":0.9,"meeting_title":"Tool Matched",'
                                    '"meeting_start_at":"2026-04-27T10:00:00+00:00","meeting_end_at":"2026-04-27T11:00:00+00:00",'
                                    '"meeting_location":"","attendees":[],"speaker_map":[],"corrected_transcript":"Speaker A: Hello",'
                                    '"meeting_notes":"Notes","summary":"Summary","topics":[],"action_items":[],"evidence":["Used ClickHouse"]}'
                                ),
                            }
                        ]
                    }
                ]
            }
        )


class FakeMinimumToolCallingSession:
    def __init__(self) -> None:
        self.posts = []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        if len(self.posts) in {1, 2}:
            name = "show_schema" if len(self.posts) == 1 else "sql"
            arguments = "{}" if name == "show_schema" else '{"query":"SELECT 2"}'
            return FakeResponse(
                {
                    "output": [
                        {
                            "type": "function_call",
                            "call_id": f"call_{len(self.posts)}",
                            "name": name,
                            "arguments": arguments,
                        }
                    ]
                }
            )
        return FakeResponse(
            {
                "output": [
                    {
                        "content": [
                            {
                                "type": "output_text",
                                "text": (
                                    '{"calendar_event_id":"event1","calendar_confidence":0.9,"meeting_title":"Enough Tools",'
                                    '"meeting_start_at":"2026-04-27T10:00:00+00:00","meeting_end_at":"2026-04-27T11:00:00+00:00",'
                                    '"meeting_location":"","attendees":[],"speaker_map":[],"corrected_transcript":"Speaker A: Hello",'
                                    '"meeting_notes":"Notes","summary":"Summary","topics":[],"action_items":[],"evidence":["Used ClickHouse"]}'
                                ),
                            }
                        ]
                    }
                ]
            }
        )


class FakeValidationRepairSession:
    def __init__(self) -> None:
        self.posts = []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        if len(self.posts) == 1:
            return FakeResponse(
                {
                    "output": [
                        {
                            "type": "function_call",
                            "call_id": "call_1",
                            "name": "show_schema",
                            "arguments": "{}",
                        }
                    ]
                }
            )
        title = "Needs Repair" if len(self.posts) == 2 else "Repaired"
        transcript = "Alex: Hello." if len(self.posts) == 2 else "Alex Rivera: Hello."
        return FakeResponse(
            {
                "output": [
                    {
                        "content": [
                            {
                                "type": "output_text",
                                "text": (
                                    '{"calendar_event_id":"event1","calendar_confidence":0.9,'
                                    f'"meeting_title":"{title}",'
                                    '"meeting_start_at":"2026-04-27T10:00:00+00:00",'
                                    '"meeting_end_at":"2026-04-27T11:00:00+00:00",'
                                    '"meeting_location":"","attendees":["Alex Rivera"],'
                                    '"speaker_map":[{"speaker_label":"A","speaker_name":"Alex Rivera","confidence":1,"evidence":"test"}],'
                                    f'"corrected_transcript":"{transcript}",'
                                    '"meeting_notes":"Notes","summary":"Summary","topics":[],"action_items":[],"evidence":["Used ClickHouse"]}'
                                ),
                            }
                        ]
                    }
                ]
            }
        )


class FakeReadOnlyService:
    def __init__(self) -> None:
        self.sql = []

    def execute_one(self, sql):
        self.sql.append(sql)

        class Result:
            def as_tool_payload(self):
                return {"sql": sql, "csv": "summary\nCalendar Match", "error": "", "truncated": {"rows": False, "fields": []}}

        return Result()

    def schema_overview(self):
        class Result:
            def as_tool_payload(self):
                return {"sql": "SHOW TABLES", "csv": "# db.table\n\ncolumn\nvalue", "error": "", "truncated": {"rows": False, "fields": []}}

        return Result()


class FakeIdentityWarehouse:
    def _query(self, sql):
        if "FROM slack_users" in sql:
            return [("guest@example.com", "guest", "guest", "", "U1")]
        if "FROM gmail_messages" in sql:
            return [("system@example.com", "Guest Person accepted their invite", "Guest Person joined")]
        return []


def test_load_settings_reads_openai_config(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-5.3-codex")

    settings = load_settings(require_clickhouse=False, require_gmail=False, require_openai=True)

    assert settings.openai is not None
    assert settings.openai.api_key == "test-key"
    assert settings.openai.model == "gpt-5.3-codex"
    assert settings.openai.reasoning_effort == "high"


def test_openai_responses_client_uses_structured_outputs() -> None:
    session = FakeSession()

    result = OpenAIResponsesClient(api_key="test-key", model="gpt-5.3-codex", session=session).create_structured(
        system_prompt="system",
        user_prompt="user",
        schema=enrichment_schema(),
    )

    assert result["meeting_title"] == "Test"
    body = session.posts[0][1]["json"]
    assert body["model"] == "gpt-5.3-codex"
    assert body["text"]["format"]["type"] == "json_schema"
    assert body["text"]["format"]["strict"] is True
    assert body["reasoning"] == {"effort": "high"}


def test_openai_responses_client_executes_function_calls_before_structured_output() -> None:
    session = FakeToolCallingSession()
    service = FakeReadOnlyService()
    tool = ClickHouseEnrichmentTool(service)

    result = OpenAIResponsesClient(api_key="test-key", model="gpt-5.3-codex", session=session).create_agentic_structured(
        system_prompt="system",
        user_prompt="user",
        schema=enrichment_schema(),
        tools=warehouse_tool_definitions(),
        tool_executor=tool.execute,
        max_tool_calls=2,
    )

    assert result["meeting_title"] == "Tool Matched"
    assert service.sql == ["SELECT summary FROM calendar_events LIMIT 1"]
    assert result["__tool_calls"][0]["name"] == "sql"
    first_body = session.posts[0][1]["json"]
    second_body = session.posts[1][1]["json"]
    assert first_body["tools"][0]["name"] == "sql"
    assert first_body["tools"][1]["name"] == "show_schema"
    assert "tool_choice" not in first_body
    assert any(item.get("type") == "function_call_output" for item in second_body["input"])


def test_openai_responses_client_retries_transient_request_failures() -> None:
    session = FakeTransientFailureSession()
    sleeps = []

    result = OpenAIResponsesClient(
        api_key="test-key",
        model="gpt-5.3-codex",
        session=session,
        retry_delay_seconds=0.25,
        sleep=sleeps.append,
    ).create_structured(
        system_prompt="system",
        user_prompt="user",
        schema=enrichment_schema(),
    )

    assert result["meeting_title"] == "Retried"
    assert len(session.posts) == 2
    assert sleeps == [0.25]


def test_openai_responses_client_requires_tool_call_when_requested() -> None:
    session = FakeToolRequiredRetrySession()
    service = FakeReadOnlyService()
    tool = ClickHouseEnrichmentTool(service)

    result = OpenAIResponsesClient(api_key="test-key", model="gpt-5.3-codex", session=session).create_agentic_structured(
        system_prompt="system",
        user_prompt="user",
        schema=enrichment_schema(),
        tools=warehouse_tool_definitions(),
        tool_executor=tool.execute,
        max_tool_calls=2,
        require_tool_call=True,
    )

    assert result["meeting_title"] == "Tool Matched"
    assert service.sql == []
    assert len(session.posts) == 3
    assert session.posts[0][1]["json"]["tool_choice"] == "required"
    assert [tool["name"] for tool in session.posts[0][1]["json"]["tools"]] == ["show_schema"]
    retry_body = session.posts[1][1]["json"]
    assert retry_body["tool_choice"] == "required"
    assert [tool["name"] for tool in retry_body["tools"]] == ["show_schema"]
    assert "0 warehouse tool calls" in retry_body["input"][-1]["content"]
    assert "tool_choice" not in session.posts[2][1]["json"]


def test_openai_responses_client_can_require_multiple_tool_calls() -> None:
    session = FakeMinimumToolCallingSession()
    service = FakeReadOnlyService()
    tool = ClickHouseEnrichmentTool(service)

    result = OpenAIResponsesClient(api_key="test-key", model="gpt-5.3-codex", session=session).create_agentic_structured(
        system_prompt="system",
        user_prompt="user",
        schema=enrichment_schema(),
        tools=warehouse_tool_definitions(),
        tool_executor=tool.execute,
        min_tool_calls=2,
        max_tool_calls=3,
    )

    assert result["meeting_title"] == "Enough Tools"
    assert service.sql == ["SELECT 2"]
    assert session.posts[0][1]["json"]["tool_choice"] == "required"
    assert [tool["name"] for tool in session.posts[0][1]["json"]["tools"]] == ["show_schema"]
    assert session.posts[1][1]["json"]["tool_choice"] == "required"
    assert [tool["name"] for tool in session.posts[1][1]["json"]["tools"]] == ["sql", "show_schema"]
    assert "tool_choice" not in session.posts[2][1]["json"]


def test_openai_responses_client_repairs_failed_validation_without_more_tools() -> None:
    session = FakeValidationRepairSession()
    service = FakeReadOnlyService()
    tool = ClickHouseEnrichmentTool(service)

    result = OpenAIResponsesClient(api_key="test-key", model="gpt-5.3-codex", session=session).create_agentic_structured(
        system_prompt="system",
        user_prompt="user",
        schema=enrichment_schema(),
        tools=warehouse_tool_definitions(),
        tool_executor=tool.execute,
        min_tool_calls=1,
        result_validator=lambda result: ["use full speaker names"]
        if result.get("meeting_title") == "Needs Repair"
        else [],
    )

    assert result["meeting_title"] == "Repaired"
    assert result["corrected_transcript"] == "Alex Rivera: Hello."
    assert len(session.posts) == 3
    assert [tool["name"] for tool in session.posts[0][1]["json"]["tools"]] == ["show_schema"]
    repair_body = session.posts[2][1]["json"]
    assert "tools" not in repair_body
    assert "use full speaker names" in repair_body["input"][-1]["content"]


def test_openai_responses_client_hides_tools_after_tool_budget_is_used() -> None:
    session = FakeValidationRepairSession()
    service = FakeReadOnlyService()
    tool = ClickHouseEnrichmentTool(service)

    result = OpenAIResponsesClient(api_key="test-key", model="gpt-5.3-codex", session=session).create_agentic_structured(
        system_prompt="system",
        user_prompt="user",
        schema=enrichment_schema(),
        tools=warehouse_tool_definitions(),
        tool_executor=tool.execute,
        min_tool_calls=1,
        max_tool_calls=1,
    )

    assert result["meeting_title"] == "Needs Repair"
    assert len(session.posts) == 2
    assert "tools" not in session.posts[1][1]["json"]


def test_function_call_helpers_parse_response_payloads() -> None:
    payload = {"output": [{"type": "function_call", "name": "sql", "arguments": '{"query":"SELECT 1"}'}]}

    calls = response_function_calls(payload)

    assert calls[0]["name"] == "sql"
    assert parse_function_arguments(calls[0]["arguments"]) == {"query": "SELECT 1"}
    assert parse_function_arguments("not json") == {}


def test_validate_enrichment_result_flags_compression_short_prefixes_and_opening_loss() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "x" * 10_000},
        transcript_segments=[
            {"text": "Hey, Priya."},
            {"text": "Hey, how are you?"},
            {"text": "I'm doing great. How are you, Alex?"},
        ],
        result={
            "speaker_map": [
                {"speaker_label": "B", "speaker_name": "Alex Rivera", "confidence": 0.99, "evidence": "test"},
                {"speaker_label": "C", "speaker_name": "Priya Narayan", "confidence": 0.99, "evidence": "test"},
            ],
            "corrected_transcript": "Alex: I'm doing great. How are you.",
        },
    )

    assert any("too compressed" in issue for issue in issues)
    assert any("first-name prefix 'Alex'" in issue for issue in issues)
    assert any("How are you, Alex" in issue for issue in issues)


def test_validate_enrichment_result_flags_same_speaker_asking_and_answering_greeting() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "speaker_map": [
                {"speaker_label": "B", "speaker_name": "Alex Rivera", "confidence": 0.99, "evidence": "test"},
                {"speaker_label": "C", "speaker_name": "Priya Narayan", "confidence": 0.99, "evidence": "test"},
            ],
            "corrected_transcript": "\n".join(
                [
                    "Alex Rivera: Hey, Priya.",
                    "Priya Narayan: Hey, how are you?",
                    "Priya Narayan: I'm doing great. How are you, Alex?",
                    "Alex Rivera: I'm good.",
                ]
            ),
        },
    )

    assert any("same speaker asking" in issue for issue in issues)


def test_validate_enrichment_result_flags_multiple_speaker_turns_on_one_line() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "speaker_map": [
                {"speaker_label": "B", "speaker_name": "Alex Rivera", "confidence": 0.99, "evidence": "test"},
                {"speaker_label": "C", "speaker_name": "Priya Narayan", "confidence": 0.99, "evidence": "test"},
            ],
            "corrected_transcript": "Alex Rivera: Hi. Priya Narayan: Hello.",
        },
    )

    assert any("multiple speaker turns" in issue for issue in issues)


def test_validate_enrichment_result_allows_full_attendee_prefix_outside_speaker_map() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "meeting_title": "Test",
            "meeting_start_at": "2026-04-27T14:00:00+00:00",
            "meeting_end_at": "2026-04-27T14:30:00+00:00",
            "attendees": ["Alex Rivera", "Priya Narayan"],
            "speaker_map": [
                {
                    "speaker_label": "A",
                    "speaker_name": "Unresolved mixed speaker (label A)",
                    "confidence": 0.4,
                    "evidence": "mixed",
                }
            ],
            "corrected_transcript": "Alex Rivera: Hello.\nUnresolved mixed speaker (label A): Hi.",
        },
    )

    assert not any("first-name prefix" in issue for issue in issues)


def test_validate_enrichment_result_allows_local_transcript_assembly_sentinel_for_long_recordings() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "x" * 20_000},
        transcript_segments=[{"text": "I'm doing great. How are you, Alex?"}],
        result={
            "meeting_title": "Long Recording",
            "meeting_start_at": "2026-04-27T14:00:00+00:00",
            "meeting_end_at": "2026-04-27T14:30:00+00:00",
            "attendees": ["Alex Rivera", "Priya Narayan"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 0.99, "evidence": "test"},
                {"speaker_label": "B", "speaker_name": "Priya Narayan", "confidence": 0.99, "evidence": "test"},
            ],
            "corrected_transcript": LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
        },
    )

    assert not any("too compressed" in issue for issue in issues)
    assert not any("opening dialogue" in issue for issue in issues)


def test_validate_enrichment_result_flags_incomplete_attendee_names() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "attendees": ["Riley", "Jordan Ellis"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Jordan Ellis", "confidence": 0.99, "evidence": "test"},
                {"speaker_label": "B", "speaker_name": "Riley", "confidence": 0.99, "evidence": "test"},
            ],
            "corrected_transcript": "Jordan Ellis: Hey, Riley.\nRiley: Hey.",
        },
    )

    assert any("attendees contain incomplete names" in issue for issue in issues)


def test_validate_enrichment_result_flags_attendee_email_name_hybrids() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "attendees": ["Riley (riley@example.com)", "Jordan Ellis"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Jordan Ellis", "confidence": 0.99, "evidence": "test"},
                {"speaker_label": "B", "speaker_name": "Riley (riley@example.com)", "confidence": 0.99, "evidence": "test"},
            ],
            "corrected_transcript": "Jordan Ellis: Hey, Riley.\nRiley (riley@example.com): Hey.",
        },
    )

    assert any("attendees contain incomplete names" in issue for issue in issues)
    assert any("malformed speaker_name" in issue for issue in issues)


def test_validate_enrichment_result_flags_low_confidence_resolved_speaker_names() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "attendees": ["Jordan Ellis", "Casey Morgan"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Jordan Ellis", "confidence": 0.56, "evidence": "weak"},
                {
                    "speaker_label": "B",
                    "speaker_name": "Interviewer (Casey Morgan or Taylor Reed)",
                    "confidence": 0.56,
                    "evidence": "mixed",
                },
            ],
            "corrected_transcript": "Jordan Ellis: Hello.\nInterviewer (Casey Morgan or Taylor Reed): Hi.",
        },
    )

    assert any("Jordan Ellis" in issue and "low confidence" in issue for issue in issues)
    assert any("ambiguous speaker_name" in issue for issue in issues)


def test_validate_enrichment_result_flags_slash_separated_candidate_speaker_names() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "attendees": ["Jordan Ellis", "Casey Morgan", "Taylor Reed"],
            "speaker_map": [
                {
                    "speaker_label": "A",
                    "speaker_name": "Interviewer (mixed: Casey Morgan / Taylor Reed)",
                    "confidence": 0.56,
                    "evidence": "mixed",
                },
            ],
            "corrected_transcript": "Interviewer (mixed: Casey Morgan / Taylor Reed): Hello.",
        },
    )

    assert any("ambiguous speaker_name" in issue for issue in issues)


def test_validate_enrichment_result_flags_uncertainty_inside_speaker_name() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "attendees": ["Jordan Ellis", "Casey Morgan"],
            "speaker_map": [
                {
                    "speaker_label": "A",
                    "speaker_name": "Interviewer (likely Jordan Ellis; may include Casey Morgan)",
                    "confidence": 0.56,
                    "evidence": "mixed",
                },
            ],
            "corrected_transcript": "Interviewer (likely Jordan Ellis; may include Casey Morgan): Hello.",
        },
    )

    assert any("ambiguous speaker_name" in issue for issue in issues)


def test_validate_enrichment_result_flags_person_guess_inside_unresolved_speaker_name() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "attendees": ["Jordan Ellis", "Casey Morgan"],
            "speaker_map": [
                {
                    "speaker_label": "A",
                    "speaker_name": "Mixed/Unresolved (Jordan Ellis + interviewer)",
                    "confidence": 0.56,
                    "evidence": "mixed",
                },
            ],
            "corrected_transcript": "Mixed/Unresolved (Jordan Ellis + interviewer): Hello.",
        },
    )

    assert any("ambiguous speaker_name" in issue for issue in issues)


def test_validate_enrichment_result_allows_plain_mixed_unresolved_speaker_names() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "attendees": ["Jordan Ellis", "Casey Morgan"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Mixed/Unresolved Speaker (A)", "confidence": 0.56, "evidence": "mixed"},
                {"speaker_label": "B", "speaker_name": "Jordan Ellis", "confidence": 0.99, "evidence": "test"},
            ],
            "corrected_transcript": "Mixed/Unresolved Speaker (A): Hello.\nJordan Ellis: Hi.",
        },
    )

    assert not any("ambiguous speaker_name" in issue for issue in issues)


def test_segment_preserving_fallback_rebuilds_compressed_transcript_with_opening_heuristic() -> None:
    segments = [
        {"segment_index": 0, "speaker_label": "A", "text": "Hey, Prya."},
        {"segment_index": 1, "speaker_label": "B", "text": "Hey, how are you?"},
        {"segment_index": 2, "speaker_label": "C", "text": "I'm doing great. How are you, Alex?"},
        {"segment_index": 3, "speaker_label": "A", "text": "I'm good."},
        {"segment_index": 4, "speaker_label": "A", "text": "We have 33 centers."},
    ]

    result = apply_segment_preserving_transcript_fallback(
        recording={"transcript_text": "x" * 10_000},
        transcript_segments=segments,
        result={
            "__validation_issues": ["corrected_transcript is too compressed: 10 chars vs 10000 source chars"],
            "attendees": ["Alex Rivera", "Priya Narayan", "Morgan Lee"],
            "speaker_map": [
                {
                    "speaker_label": "A",
                    "speaker_name": "A (mixed/unresolved)",
                    "confidence": 0.5,
                    "evidence": "mostly consistent with Morgan Lee but opening is mixed",
                },
                {"speaker_label": "B", "speaker_name": "Alex Rivera", "confidence": 0.99, "evidence": "test"},
                {"speaker_label": "C", "speaker_name": "Priya Narayan", "confidence": 0.99, "evidence": "test"},
            ],
            "corrected_transcript": "short",
            "evidence": [],
        },
    )

    corrected = result["corrected_transcript"]
    assert corrected.splitlines()[:5] == [
        "Alex Rivera: Hey, Priya.",
        "Alex Rivera: Hey, how are you?",
        "Priya Narayan: I'm doing great. How are you, Alex?",
        "Alex Rivera: I'm good.",
        "Morgan Lee: We have 33 centers.",
    ]
    assert any("too compressed" in issue for issue in result["__validation_issues"])


def test_segment_preserving_fallback_assembles_local_transcript_sentinel() -> None:
    result = apply_segment_preserving_transcript_fallback(
        recording={"transcript_text": "x" * 20_000},
        transcript_segments=[{"segment_index": 0, "speaker_label": "A", "text": "Hello there."}],
        result={
            "attendees": ["Alex Rivera"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 0.99, "evidence": "test"},
            ],
            "corrected_transcript": LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
            "evidence": [],
        },
    )

    assert result["corrected_transcript"] == "Alex Rivera: Hello there."
    assert any("assembled locally" in evidence for evidence in result["evidence"])


def test_segment_preserving_fallback_does_not_use_context_phrases_as_speaker_names() -> None:
    result = apply_segment_preserving_transcript_fallback(
        recording={"transcript_text": "x" * 20_000},
        transcript_segments=[{"segment_index": 0, "speaker_label": "A", "text": "Hello there."}],
        result={
            "attendees": ["Riley Chen"],
            "speaker_map": [
                {
                    "speaker_label": "A",
                    "speaker_name": "Unresolved mixed speaker (label A)",
                    "confidence": 0.4,
                    "evidence": "Before Riley joins, this label contains setup banter.",
                },
            ],
            "corrected_transcript": LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
            "evidence": ["Before Riley joins, this label contains setup banter."],
        },
    )

    assert result["corrected_transcript"] == "Unresolved mixed speaker (label A): Hello there."
    assert "Before Riley:" not in result["corrected_transcript"]


def test_name_canonicalization_handles_close_first_name_variant() -> None:
    assert (
        canonicalize_text_verified_name_mentions(
            "Hey, Prya.",
            verified_names=["Priya Narayan"],
        )
        == "Hey, Priya."
    )


def test_name_canonicalization_handles_full_name_variant_from_evidence() -> None:
    assert (
        canonicalize_text_verified_name_mentions(
            "I went on a walk with Robyn Correct.",
            verified_names=["Robin Correct"],
        )
        == "I went on a walk with Robin Correct."
    )


def test_segment_preserving_fallback_uses_tool_evidence_names_for_asr_variants() -> None:
    result = apply_segment_preserving_transcript_fallback(
        recording={"transcript_text": "x" * 5_000},
        transcript_segments=[{"segment_index": 0, "speaker_label": "A", "text": "I spoke with Robyn Correct."}],
        result={
            "__validation_issues": ["corrected_transcript is too compressed: 10 chars vs 5000 source chars"],
            "__tool_calls": [
                {
                    "name": "sql",
                    "output": {
                        "csv": "subject,snippet\nRobin Correct,Great connection with Robin Correct",
                    },
                }
            ],
            "attendees": ["Alex Rivera"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 0.99, "evidence": "test"},
            ],
            "corrected_transcript": "short",
            "evidence": [],
        },
    )

    assert "Robin Correct" in result["corrected_transcript"]
    assert "Robyn Correct" not in result["corrected_transcript"]


def test_enrichment_tool_definitions_are_sql_and_show_schema() -> None:
    tools = warehouse_tool_definitions()

    assert [tool["name"] for tool in tools] == ["sql", "show_schema"]
    assert sql_tool_definition()["parameters"]["required"] == ["query"]
    assert show_schema_tool_definition()["parameters"]["properties"] == {}


def test_clickhouse_enrichment_tool_executes_sql_and_show_schema() -> None:
    service = FakeReadOnlyService()
    tool = ClickHouseEnrichmentTool(service)

    sql_result = tool.execute("sql", {"query": "SELECT 1"})
    schema_result = tool.execute("show_schema", {})

    assert service.sql == ["SELECT 1"]
    assert sql_result["csv"] == "summary\nCalendar Match"
    assert schema_result["csv"].startswith("# db.table")


def test_tool_log_summaries_are_compact() -> None:
    assert summarize_tool_arguments({"sql": "SELECT *\nFROM calendar_events LIMIT 10"}).startswith("sql=SELECT * FROM")
    assert summarize_tool_output({"csv": "a\n1\n2", "truncated": {"rows": False, "fields": []}}) == (
        "rows=2 truncated_rows=False truncated_fields=0"
    )
    assert "error=bad" in summarize_tool_output({"error": "bad"})


def test_normalize_corrected_transcript_prefixes_carries_obvious_continuations() -> None:
    corrected = normalize_corrected_transcript_prefixes(
        "Alex Rivera: Hello.\n\nThis is a continuation: with a colon.\n\nUnknown: Leave this alone.\n\nRiley: Hi.",
        speaker_names=["Alex Rivera", "Riley"],
    )

    assert "Alex Rivera: This is a continuation: with a colon." in corrected
    assert "Unknown: Leave this alone." in corrected
    assert "Riley: Hi." in corrected


def test_parse_attendee_summaries_reads_calendar_json() -> None:
    attendees = parse_attendee_summaries('[{"email":"a@example.com"},{"displayName":"Person"}]')

    assert attendees == ["a@example.com", "Person"]


def test_load_attendee_identity_hints_extracts_possible_full_names_without_hardcoding() -> None:
    hints = load_attendee_identity_hints(
        FakeIdentityWarehouse(),
        [{"display_name": "", "email": "guest@example.com"}],
    )

    assert hints["guest@example.com"]["possible_names"] == ["Guest Person"]
    assert hints["guest@example.com"]["gmail_mentions"][0]["subject"] == "Guest Person accepted their invite"


def test_possible_identity_names_from_text_reads_capitalized_full_names() -> None:
    assert "Guest Person" in possible_identity_names_from_text("Guest Person accepted their invite")


def test_enrichment_schema_separates_corrected_transcript_from_notes() -> None:
    schema = enrichment_schema()

    assert "corrected_transcript" in schema["properties"]
    assert "meeting_notes" in schema["properties"]
    assert "cleaned_transcript" not in schema["properties"]
    assert "corrected_transcript" in schema["required"]
    assert "meeting_notes" in schema["required"]


def test_enrichment_user_prompt_includes_diarized_segments_and_speaker_rules() -> None:
    prompt = enrichment_user_prompt(
        recording={"recording_id": "rec1", "recorded_at": datetime(2026, 4, 27, tzinfo=UTC), "title": "Title"},
        calendar_candidates=[],
        transcript_segments=[
            {
                "segment_index": 0,
                "speaker_label": "A",
                "start_ms": 0,
                "end_ms": 1000,
                "confidence": 0.9,
                "text": "Hello",
            }
        ],
    )

    assert '"speaker_label": "A"' in prompt
    assert "source of truth for speaker turns" in prompt
    assert "Do not invent generic speaker labels" in prompt
    assert "recorded_at_interpretations" in prompt
    assert "Corrected_transcript attribution is turn-level" in prompt
    assert "below 0.9 confidence" in prompt
    assert "Hard requirements: accurate meeting date/time" in prompt
    assert "full name can be resolved" in prompt
    assert "calendar attendee emails plus Slack/email identity evidence" in prompt
    assert "relevant_tables" not in prompt
    assert "calendar_events(account" not in prompt
    assert "identity_hints" in prompt
    assert "Audit every diarized speaker_label" in prompt
    assert "NAME is the addressee, not the speaker" in prompt
    assert "opening small talk" in prompt
    assert "How are you, PERSON?" in prompt
    assert "Hack Club not Hat Club" in prompt
    assert "Hackatime not Hackertime" in prompt
    assert "Stardance not Start Dance" in prompt
    assert "Personal journal entries or ad-hoc voice notes are valid outputs" in prompt
    assert LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL in prompt


def test_load_enrichment_candidates_can_scope_to_recent_recordings_without_limit() -> None:
    queries = []

    class Warehouse:
        def _query(self, sql):
            queries.append(sql)
            return []

    load_enrichment_candidates(
        Warehouse(),
        provider="openai",
        model="test-model",
        prompt_version="test-prompt",
        limit=None,
        recorded_after=datetime(2026, 3, 3, tzinfo=UTC),
    )

    assert "f.recorded_at >= parseDateTimeBestEffort('2026-03-03T00:00:00+00:00')" in queries[0]
    assert "LIMIT" not in queries[0]


def test_load_enrichment_candidates_keeps_limit_when_configured() -> None:
    queries = []

    class Warehouse:
        def _query(self, sql):
            queries.append(sql)
            return []

    load_enrichment_candidates(
        Warehouse(),
        provider="openai",
        model="test-model",
        prompt_version="test-prompt",
        limit=12,
        recorded_after=None,
    )

    assert "LIMIT 12" in queries[0]


def test_ensure_recording_level_fields_fills_no_calendar_outputs() -> None:
    result = ensure_recording_level_fields(
        recording={
            "recorded_at": datetime(2026, 4, 27, 14, 0, tzinfo=UTC),
        },
        transcript_segments=[{"end_ms": 90_000}],
        result={
            "calendar_event_id": "",
            "calendar_confidence": 0,
            "meeting_title": "",
            "meeting_start_at": "",
            "meeting_end_at": "",
            "evidence": [],
        },
    )

    assert result["meeting_title"] == "Voice Memo 2026-04-27 14:00 UTC"
    assert result["meeting_start_at"] == "2026-04-27T14:00:00+00:00"
    assert result["meeting_end_at"] == "2026-04-27T14:01:30+00:00"
    assert any("No matching calendar event" in evidence for evidence in result["evidence"])


def test_recording_time_interpretations_include_local_wall_clock_conversion() -> None:
    interpretations = recording_time_interpretations(datetime(2026, 4, 23, 11, 10, tzinfo=UTC))

    assert interpretations[0]["utc"] == datetime(2026, 4, 23, 11, 10, tzinfo=UTC)
    assert interpretations[1]["utc"] == datetime(2026, 4, 23, 15, 10, tzinfo=UTC)


def test_load_calendar_candidates_searches_utc_and_local_wall_clock_anchors() -> None:
    queries = []

    class Warehouse:
        def _query(self, sql):
            queries.append(sql)
            return []

    load_calendar_candidates(Warehouse(), {"recorded_at": datetime(2026, 4, 23, 11, 10, tzinfo=UTC)})

    assert "2026-04-23T11:10:00+00:00" in queries[0]
    assert "2026-04-23T15:10:00+00:00" in queries[0]
    assert "LIMIT 12" in queries[0]


def test_enrichment_row_serializes_structured_result() -> None:
    row = enrichment_row(
        recording={"account": "zach@example.com", "recording_id": "rec1"},
        result={
            "calendar_event_id": "event1",
            "calendar_confidence": 0.8,
            "meeting_title": "Meeting",
            "meeting_start_at": "2026-04-27T10:00:00+00:00",
            "meeting_end_at": "2026-04-27T11:00:00+00:00",
            "meeting_location": "Zoom",
            "attendees": ["a@example.com"],
            "speaker_map": [],
            "corrected_transcript": "Speaker A: Hello",
            "meeting_notes": "Notes",
            "summary": "Summary",
            "topics": ["Topic"],
            "action_items": [],
            "evidence": ["Evidence"],
        },
        provider="openai",
        model="gpt-5.3-codex",
        prompt_version=ENRICHMENT_PROMPT_VERSION,
        status="completed",
        error="",
        created_at=datetime(2026, 4, 27, tzinfo=UTC),
    )

    assert row["calendar_event_id"] == "event1"
    assert row["meeting_start_at"] == datetime(2026, 4, 27, 10, tzinfo=UTC)
    assert row["attendees_json"] == '["a@example.com"]'
    assert row["cleaned_transcript"] == "Speaker A: Hello"
    assert row["corrected_transcript"] == "Speaker A: Hello"
    assert row["meeting_notes"] == "Notes"


def test_enrichment_row_normalizes_corrected_transcript_prefixes() -> None:
    row = enrichment_row(
        recording={"account": "zach@example.com", "recording_id": "rec1"},
        result={
            "calendar_event_id": "",
            "calendar_confidence": 0,
            "meeting_title": "Meeting",
            "meeting_start_at": "",
            "meeting_end_at": "",
            "meeting_location": "",
            "attendees": [],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 1, "evidence": "intro"},
            ],
            "corrected_transcript": "Alex Rivera: Hello.\n\nThis is continued.",
            "meeting_notes": "Notes",
            "summary": "Summary",
            "topics": [],
            "action_items": [],
            "evidence": [],
        },
        provider="openai",
        model="gpt-5.3-codex",
        prompt_version=ENRICHMENT_PROMPT_VERSION,
        status="completed",
        error="",
        created_at=datetime(2026, 4, 27, tzinfo=UTC),
    )

    assert row["corrected_transcript"] == "Alex Rivera: Hello.\n\nAlex Rivera: This is continued."


def test_enrichment_row_canonicalizes_close_name_variants_to_verified_attendees() -> None:
    row = enrichment_row(
        recording={"account": "zach@example.com", "recording_id": "rec1"},
        result={
            "calendar_event_id": "",
            "calendar_confidence": 0,
            "meeting_title": "Meeting",
            "meeting_start_at": "",
            "meeting_end_at": "",
            "meeting_location": "",
            "attendees": ["Alex Rivera", "Taylor Singh"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 1, "evidence": "intro"},
                {"speaker_label": "B", "speaker_name": "Taylor Singh", "confidence": 1, "evidence": "calendar"},
            ],
            "corrected_transcript": "Alex Rivera: Hey Tayler, how are you?\n\nTaylor Singh: Good.",
            "meeting_notes": "Alex greeted Tayler.",
            "summary": "Call with Tayler.",
            "topics": [],
            "action_items": [],
            "evidence": ["Opening line says Tayler, matching Taylor Singh."],
        },
        provider="openai",
        model="gpt-5.3-codex",
        prompt_version=ENRICHMENT_PROMPT_VERSION,
        status="completed",
        error="",
        created_at=datetime(2026, 4, 27, tzinfo=UTC),
    )

    assert "Hey Taylor" in row["corrected_transcript"]
    assert "Tayler" not in row["raw_result_json"]


def test_canonicalize_text_verified_name_mentions_only_rewrites_close_name_variants() -> None:
    text = canonicalize_text_verified_name_mentions(
        "Hey Tayler, maybe we should talk with Morgan and Kory.",
        verified_names=["Taylor Singh", "Morgan Lee", "Cory Person"],
    )

    assert text == "Hey Taylor, maybe we should talk with Morgan and Cory."


def test_response_output_text_reads_output_content() -> None:
    assert response_output_text({"output": [{"content": [{"type": "output_text", "text": "{}"}]}]}) == "{}"
