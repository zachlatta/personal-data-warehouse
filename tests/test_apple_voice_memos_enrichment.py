from __future__ import annotations

from datetime import UTC, datetime

from personal_data_warehouse.apple_voice_memos_enrichment import (
    AGENT_USER_PROMPT_INPUT_FILE,
    AGENT_ENRICHMENT_PROMPT_VERSION,
    ContainerAgentStructuredClient,
    LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
    apply_contact_alias_corrections,
    apply_segment_preserving_transcript_fallback,
    ensure_recording_level_fields,
    enrichment_row,
    enrichment_schema,
    enrichment_task_input,
    enrichment_user_prompt,
    event_identity_first_names,
    event_identity_terms,
    load_calendar_candidates,
    canonicalize_text_verified_name_mentions,
    count_warehouse_cli_calls,
    load_attendee_identity_hints,
    load_contact_alias_hints,
    load_enrichment_candidates,
    load_event_identity_hints,
    normalize_corrected_transcript_prefixes,
    parse_attendee_summaries,
    possible_identity_names_from_text,
    recording_time_interpretations,
    validate_enrichment_result,
    withhold_low_confidence_resolved_speaker_names,
)
from personal_data_warehouse.agent_runner import AgentRunEvent, AgentRunResult


class FakeIdentityWarehouse:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def _query(self, sql):
        self.queries.append(sql)
        if "FROM @slack_users" in sql:
            return [("guest@example.com", "guest", "guest", "", "U1")]
        if "FROM @gmail_messages" in sql:
            return [("system@example.com", "Guest Person accepted their invite", "Guest Person joined")]
        return []


def test_container_agent_structured_client_keeps_prompt_and_stores_extra_input_files() -> None:
    class FakeAgent:
        def __init__(self) -> None:
            self.request = None

        def run(self, request):
            self.request = request
            now = datetime(2026, 4, 27, tzinfo=UTC)
            return AgentRunResult(
                run_id=request.run_id,
                provider=request.provider or "codex",
                model=request.model or "gpt-test",
                task_type=request.task_type,
                subject_id=request.subject_id,
                prompt_version=request.prompt_version,
                input_sha256=request.input_sha256,
                status="completed",
                final_output_json={"ok": True},
                error="",
                exit_code=0,
                started_at=now,
                completed_at=now,
                events=[],
            )

    agent = FakeAgent()
    user_prompt = "Read the extra input file and return JSON."
    large_input = '{"transcript":"' + ("hello " * 10_000) + '"}'

    result = ContainerAgentStructuredClient(agent=agent, provider="codex", model="gpt-test").create_agentic_structured(
        system_prompt="system",
        user_prompt=user_prompt,
        schema={"type": "object"},
        tools=[],
        tool_executor=lambda _name, _arguments: {},
        min_tool_calls=0,
        input_files={AGENT_USER_PROMPT_INPUT_FILE: large_input},
    )

    assert result == {"ok": True}
    assert agent.request is not None
    assert agent.request.input_files == {AGENT_USER_PROMPT_INPUT_FILE: large_input}
    assert user_prompt in agent.request.prompt
    assert large_input not in agent.request.prompt
    assert f"$AGENT_INPUT_DIR/{AGENT_USER_PROMPT_INPUT_FILE}" in agent.request.prompt


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
            "transcript": "Alex: I'm doing great. How are you.",
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
            "transcript": "\n".join(
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
            "transcript": "Alex Rivera: Hi. Priya Narayan: Hello.",
        },
    )

    assert any("multiple speaker turns" in issue for issue in issues)


def test_validate_enrichment_result_allows_full_attendee_prefix_outside_speaker_map() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "title": "Test",
            "start_at": "2026-04-27T14:00:00+00:00",
            "end_at": "2026-04-27T14:30:00+00:00",
            "participants": ["Alex Rivera", "Priya Narayan"],
            "speaker_map": [
                {
                    "speaker_label": "A",
                    "speaker_name": "Unresolved mixed speaker (label A)",
                    "confidence": 0.4,
                    "evidence": "mixed",
                }
            ],
            "transcript": "Alex Rivera: Hello.\nUnresolved mixed speaker (label A): Hi.",
        },
    )

    assert not any("first-name prefix" in issue for issue in issues)


def test_validate_enrichment_result_allows_local_transcript_assembly_sentinel_for_long_recordings() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "x" * 20_000},
        transcript_segments=[{"text": "I'm doing great. How are you, Alex?"}],
        result={
            "title": "Long Recording",
            "start_at": "2026-04-27T14:00:00+00:00",
            "end_at": "2026-04-27T14:30:00+00:00",
            "participants": ["Alex Rivera", "Priya Narayan"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 0.99, "evidence": "test"},
                {"speaker_label": "B", "speaker_name": "Priya Narayan", "confidence": 0.99, "evidence": "test"},
            ],
            "transcript": LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
        },
    )

    assert not any("too compressed" in issue for issue in issues)
    assert not any("opening dialogue" in issue for issue in issues)


def test_validate_enrichment_result_flags_incomplete_attendee_names() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "participants": ["Riley", "Jordan Ellis"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Jordan Ellis", "confidence": 0.99, "evidence": "test"},
                {"speaker_label": "B", "speaker_name": "Riley", "confidence": 0.99, "evidence": "test"},
            ],
            "transcript": "Jordan Ellis: Hey, Riley.\nRiley: Hey.",
        },
    )

    assert any("participants contain incomplete names" in issue for issue in issues)


def test_validate_enrichment_result_flags_attendee_email_name_hybrids() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "participants": ["Riley (riley@example.com)", "Jordan Ellis"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Jordan Ellis", "confidence": 0.99, "evidence": "test"},
                {"speaker_label": "B", "speaker_name": "Riley (riley@example.com)", "confidence": 0.99, "evidence": "test"},
            ],
            "transcript": "Jordan Ellis: Hey, Riley.\nRiley (riley@example.com): Hey.",
        },
    )

    assert any("participants contain incomplete names" in issue for issue in issues)
    assert any("malformed speaker_name" in issue for issue in issues)


def test_validate_enrichment_result_flags_low_confidence_resolved_speaker_names() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "participants": ["Jordan Ellis", "Casey Morgan"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Jordan Ellis", "confidence": 0.56, "evidence": "weak"},
                {
                    "speaker_label": "B",
                    "speaker_name": "Interviewer (Casey Morgan or Taylor Reed)",
                    "confidence": 0.56,
                    "evidence": "mixed",
                },
            ],
            "transcript": "Jordan Ellis: Hello.\nInterviewer (Casey Morgan or Taylor Reed): Hi.",
        },
    )

    assert any("Jordan Ellis" in issue and "low confidence" in issue for issue in issues)
    assert any("ambiguous speaker_name" in issue for issue in issues)


def test_withhold_low_confidence_resolved_speaker_names_removes_unsafe_surnames() -> None:
    result = withhold_low_confidence_resolved_speaker_names(
        {
            "__validation_issues": [
                "speaker A maps to resolved name 'Candidate Alpha' with low confidence 0.58",
            ],
            "participants": ["Candidate Alpha", "Verified Beta"],
            "speaker_map": [
                {
                    "speaker_label": "A",
                    "speaker_name": "Candidate Alpha",
                    "confidence": 0.58,
                    "evidence": "First-name-only lookup found Candidate Alpha.",
                },
                {
                    "speaker_label": "B",
                    "speaker_name": "Verified Beta",
                    "confidence": 0.95,
                    "evidence": "Direct self-introduction.",
                },
            ],
            "transcript": "Candidate Alpha: I can help.\nVerified Beta: Thanks Candidate Alpha.",
            "summary": "Candidate Alpha and Verified Beta discussed the plan.",
            "action_items": ["Candidate Alpha will follow up."],
            "evidence": ["Warehouse lookup weakly suggested Candidate Alpha."],
        }
    )

    assert result["participants"] == ["Candidate", "Verified Beta"]
    assert result["speaker_map"][0]["speaker_name"] == "Unresolved speaker (label A)"
    assert result["speaker_map"][0]["evidence"].startswith("Withheld a low-confidence")
    assert result["transcript"].splitlines()[0] == "Unresolved speaker (label A): I can help."
    assert "Verified Beta: Thanks Candidate." in result["transcript"]
    assert "Candidate Alpha" not in str(result)
    assert "__validation_issues" not in result


def test_withhold_low_confidence_resolved_speaker_names_leaves_first_names() -> None:
    result = withhold_low_confidence_resolved_speaker_names(
        {
            "participants": ["Candidate"],
            "speaker_map": [
                {
                    "speaker_label": "A",
                    "speaker_name": "Candidate",
                    "confidence": 0.58,
                    "evidence": "Only first name known.",
                },
            ],
            "transcript": "Candidate: Hello.",
        }
    )

    assert result["participants"] == ["Candidate"]
    assert result["speaker_map"][0]["speaker_name"] == "Candidate"
    assert result["transcript"] == "Candidate: Hello."


def test_validate_enrichment_result_flags_slash_separated_candidate_speaker_names() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "participants": ["Jordan Ellis", "Casey Morgan", "Taylor Reed"],
            "speaker_map": [
                {
                    "speaker_label": "A",
                    "speaker_name": "Interviewer (mixed: Casey Morgan / Taylor Reed)",
                    "confidence": 0.56,
                    "evidence": "mixed",
                },
            ],
            "transcript": "Interviewer (mixed: Casey Morgan / Taylor Reed): Hello.",
        },
    )

    assert any("ambiguous speaker_name" in issue for issue in issues)


def test_validate_enrichment_result_flags_uncertainty_inside_speaker_name() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "participants": ["Jordan Ellis", "Casey Morgan"],
            "speaker_map": [
                {
                    "speaker_label": "A",
                    "speaker_name": "Interviewer (likely Jordan Ellis; may include Casey Morgan)",
                    "confidence": 0.56,
                    "evidence": "mixed",
                },
            ],
            "transcript": "Interviewer (likely Jordan Ellis; may include Casey Morgan): Hello.",
        },
    )

    assert any("ambiguous speaker_name" in issue for issue in issues)


def test_validate_enrichment_result_flags_person_guess_inside_unresolved_speaker_name() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "participants": ["Jordan Ellis", "Casey Morgan"],
            "speaker_map": [
                {
                    "speaker_label": "A",
                    "speaker_name": "Mixed/Unresolved (Jordan Ellis + interviewer)",
                    "confidence": 0.56,
                    "evidence": "mixed",
                },
            ],
            "transcript": "Mixed/Unresolved (Jordan Ellis + interviewer): Hello.",
        },
    )

    assert any("ambiguous speaker_name" in issue for issue in issues)


def test_validate_enrichment_result_allows_plain_mixed_unresolved_speaker_names() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=[],
        result={
            "participants": ["Jordan Ellis", "Casey Morgan"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Mixed/Unresolved Speaker (A)", "confidence": 0.56, "evidence": "mixed"},
                {"speaker_label": "B", "speaker_name": "Jordan Ellis", "confidence": 0.99, "evidence": "test"},
            ],
            "transcript": "Mixed/Unresolved Speaker (A): Hello.\nJordan Ellis: Hi.",
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
            "__validation_issues": ["transcript is too compressed: 10 chars vs 10000 source chars"],
            "participants": ["Alex Rivera", "Priya Narayan", "Morgan Lee"],
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
            "transcript": "short",
            "evidence": [],
        },
    )

    corrected = result["transcript"]
    # A mixed label stays unresolved however its evidence leans: "mostly
    # consistent with Morgan Lee" at 0.5 is the shape that put 302 turns of a
    # multi-speaker stage event under one organizer's name.
    assert corrected.splitlines()[:5] == [
        "Alex Rivera: Hey, Priya.",
        "Alex Rivera: Hey, how are you?",
        "Priya Narayan: I'm doing great. How are you, Alex?",
        "Alex Rivera: I'm good.",
        "A (mixed/unresolved): We have 33 centers.",
    ]
    assert any("too compressed" in issue for issue in result["__validation_issues"])


def stage_event_segments() -> list[dict]:
    return [
        {"segment_index": 0, "speaker_label": "A", "text": "Welcome, everyone. Please welcome our first speaker."},
        {"segment_index": 1, "speaker_label": "C", "text": "Thank you. I want to talk about open science."},
        {"segment_index": 2, "speaker_label": "C", "text": "Funding should follow the work."},
        {"segment_index": 3, "speaker_label": "A", "text": "Thank you. Next up is our second speaker."},
        {"segment_index": 4, "speaker_label": "C", "text": "Thanks. My talk is about civic institutions."},
        {"segment_index": 5, "speaker_label": "C", "text": "And how they decay."},
    ]


def test_speaker_turns_split_one_mixed_label_into_the_people_who_spoke() -> None:
    """Diarization gave three stage speakers one label; the agenda tells them apart.

    speaker_map can only say a label is mixed. speaker_turns lets the agent put
    a verified person on a contiguous run of segments -- from on-stage
    introductions and the event agenda -- and local assembly applies it.
    """
    result = apply_segment_preserving_transcript_fallback(
        recording={"transcript_text": "x" * 20_000},
        transcript_segments=stage_event_segments(),
        result={
            "participants": ["Morgan Lee", "Riley Chen", "Jordan Ellis"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Morgan Lee", "confidence": 0.95, "evidence": "host"},
                {
                    "speaker_label": "C",
                    "speaker_name": "Unresolved mixed speaker (label C)",
                    "confidence": 0.3,
                    "evidence": "every stage speaker",
                },
            ],
            "speaker_turns": [
                {
                    "start_segment_index": 1,
                    "end_segment_index": 2,
                    "speaker_name": "Riley Chen",
                    "confidence": 0.95,
                    "evidence": "introduced as the first speaker; agenda order",
                },
                {
                    "start_segment_index": 4,
                    "end_segment_index": 5,
                    "speaker_name": "Jordan Ellis",
                    "confidence": 0.93,
                    "evidence": "introduced second; talk title matches the agenda",
                },
            ],
            "transcript": LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
            "evidence": [],
        },
    )

    assert result["transcript"].splitlines() == [
        "Morgan Lee: Welcome, everyone. Please welcome our first speaker.",
        "Riley Chen: Thank you. I want to talk about open science.",
        "Riley Chen: Funding should follow the work.",
        "Morgan Lee: Thank you. Next up is our second speaker.",
        "Jordan Ellis: Thanks. My talk is about civic institutions.",
        "Jordan Ellis: And how they decay.",
    ]


def test_speaker_turns_below_the_confidence_bar_or_unresolved_do_not_name_anyone() -> None:
    result = apply_segment_preserving_transcript_fallback(
        recording={"transcript_text": "x" * 20_000},
        transcript_segments=stage_event_segments(),
        result={
            "participants": ["Morgan Lee", "Riley Chen"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Morgan Lee", "confidence": 0.95, "evidence": "host"},
                {"speaker_label": "C", "speaker_name": "Unresolved speaker (label C)", "confidence": 0.3, "evidence": "mixed"},
            ],
            "speaker_turns": [
                {
                    "start_segment_index": 1,
                    "end_segment_index": 2,
                    "speaker_name": "Riley Chen",
                    "confidence": 0.6,
                    "evidence": "a guess",
                },
                {
                    "start_segment_index": 4,
                    "end_segment_index": 5,
                    "speaker_name": "Unresolved speaker",
                    "confidence": 0.95,
                    "evidence": "unknown",
                },
            ],
            "transcript": LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
            "evidence": [],
        },
    )

    prefixes = [line.split(":", 1)[0] for line in result["transcript"].splitlines()]
    assert prefixes == [
        "Morgan Lee",
        "Unresolved speaker (label C)",
        "Unresolved speaker (label C)",
        "Morgan Lee",
        "Unresolved speaker (label C)",
        "Unresolved speaker (label C)",
    ]


def test_validate_flags_speaker_turns_that_do_not_fit_the_segments() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "short"},
        transcript_segments=stage_event_segments(),
        result={
            "title": "t",
            "start_at": "2026-10-03T23:00:00Z",
            "end_at": "2026-10-04T02:00:00Z",
            "participants": ["Riley Chen", "Jordan Ellis"],
            "speaker_map": [],
            "speaker_turns": [
                {"start_segment_index": 2, "end_segment_index": 1, "speaker_name": "Riley Chen", "confidence": 0.95, "evidence": "e"},
                {"start_segment_index": 4, "end_segment_index": 99, "speaker_name": "Jordan Ellis", "confidence": 0.95, "evidence": "e"},
                {"start_segment_index": 0, "end_segment_index": 1, "speaker_name": "Riley Chen", "confidence": 0.95, "evidence": "e"},
                {"start_segment_index": 1, "end_segment_index": 3, "speaker_name": "Jordan Ellis", "confidence": 0.95, "evidence": "e"},
            ],
            "transcript": LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
        },
    )

    turn_issues = [issue for issue in issues if issue.startswith("speaker_turns")]
    assert any("start_segment_index 2 is after end_segment_index 1" in issue for issue in turn_issues)
    assert any("segment_index 99" in issue for issue in turn_issues)
    assert any("overlap" in issue for issue in turn_issues)


def test_enrichment_schema_requires_speaker_turns() -> None:
    schema = enrichment_schema()
    turns = schema["properties"]["speaker_turns"]
    assert "speaker_turns" in schema["required"]
    assert turns["type"] == "array"
    item = turns["items"]
    assert item["additionalProperties"] is False
    assert set(item["required"]) == set(item["properties"]) == {
        "start_segment_index",
        "end_segment_index",
        "speaker_name",
        "confidence",
        "evidence",
    }


def test_enrichment_prompt_teaches_turn_level_speakers_from_the_agenda() -> None:
    prompt = enrichment_user_prompt(input_file=AGENT_USER_PROMPT_INPUT_FILE)
    assert "speaker_turns" in prompt
    assert "agenda" in prompt
    assert AGENT_ENRICHMENT_PROMPT_VERSION == "apple-voice-memo-enrichment-agent-v9"


def test_segment_preserving_fallback_assembles_local_transcript_sentinel() -> None:
    result = apply_segment_preserving_transcript_fallback(
        recording={"transcript_text": "x" * 20_000},
        transcript_segments=[{"segment_index": 0, "speaker_label": "A", "text": "Hello there."}],
        result={
            "participants": ["Alex Rivera"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 0.99, "evidence": "test"},
            ],
            "transcript": LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
            "evidence": [],
        },
    )

    assert result["transcript"] == "Alex Rivera: Hello there."
    assert any("assembled locally" in evidence for evidence in result["evidence"])


def test_segment_preserving_fallback_does_not_use_context_phrases_as_speaker_names() -> None:
    result = apply_segment_preserving_transcript_fallback(
        recording={"transcript_text": "x" * 20_000},
        transcript_segments=[{"segment_index": 0, "speaker_label": "A", "text": "Hello there."}],
        result={
            "participants": ["Riley Chen"],
            "speaker_map": [
                {
                    "speaker_label": "A",
                    "speaker_name": "Unresolved mixed speaker (label A)",
                    "confidence": 0.4,
                    "evidence": "Before Riley joins, this label contains setup banter.",
                },
            ],
            "transcript": LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
            "evidence": ["Before Riley joins, this label contains setup banter."],
        },
    )

    assert result["transcript"] == "Unresolved mixed speaker (label A): Hello there."
    assert "Before Riley:" not in result["transcript"]


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
            "__validation_issues": ["transcript is too compressed: 10 chars vs 5000 source chars"],
            "__tool_calls": [
                {
                    "name": "sql",
                    "output": {
                        "csv": "subject,snippet\nRobin Correct,Great connection with Robin Correct",
                    },
                }
            ],
            "participants": ["Alex Rivera"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 0.99, "evidence": "test"},
            ],
            "transcript": "short",
            "evidence": [],
        },
    )

    assert "Robin Correct" in result["transcript"]
    assert "Robyn Correct" not in result["transcript"]


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
    warehouse = FakeIdentityWarehouse()
    hints = load_attendee_identity_hints(
        warehouse,
        [{"display_name": "", "email": "guest@example.com"}],
    )

    assert hints["guest@example.com"]["possible_names"] == ["Guest Person"]
    assert hints["guest@example.com"]["gmail_mentions"][0]["subject"] == "Guest Person accepted their invite"
    gmail_query = next(query for query in warehouse.queries if "FROM @gmail_messages" in query)
    assert "ILIKE" in gmail_query
    assert "is_deleted = 0" in gmail_query
    assert "position(lower" not in gmail_query


def test_possible_identity_names_from_text_reads_capitalized_full_names() -> None:
    assert "Guest Person" in possible_identity_names_from_text("Guest Person accepted their invite")


def test_enrichment_schema_uses_simplified_output_fields() -> None:
    schema = enrichment_schema()

    assert "transcript" in schema["properties"]
    assert "title" in schema["properties"]
    assert "start_at" in schema["properties"]
    assert "end_at" in schema["properties"]
    assert "participants" in schema["properties"]
    assert "cleaned_transcript" not in schema["properties"]
    assert "corrected_transcript" not in schema["properties"]
    assert "meeting_notes" not in schema["properties"]
    assert "topics" not in schema["properties"]
    assert "transcript" in schema["required"]
    assert "participants" in schema["required"]


def test_enrichment_user_prompt_includes_task_rules_and_input_file_reference() -> None:
    prompt = enrichment_user_prompt(
        input_file=AGENT_USER_PROMPT_INPUT_FILE,
    )

    assert f"$AGENT_INPUT_DIR/{AGENT_USER_PROMPT_INPUT_FILE}" in prompt
    assert "source of truth for speaker turns" in prompt
    assert "Do not invent generic speaker labels" in prompt
    assert "Transcript attribution is turn-level" in prompt
    assert "below 0.9 confidence" in prompt
    assert "Hard requirements: accurate date/time" in prompt
    assert "full name can be resolved" in prompt
    assert "calendar attendee emails plus Slack/email identity evidence" in prompt
    assert "event_identity_hints" in prompt
    assert "event's likely team, staff, organizers, roster, and attendees" in prompt
    assert "Prefer event-specific organizer/team/roster evidence" in prompt
    assert "global first-name Slack user result is not enough" in prompt
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
    assert '"speaker_label": "A"' not in prompt


def test_enrichment_task_input_includes_diarized_segments_and_recording_context() -> None:
    task_input = enrichment_task_input(
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
        contact_alias_hints=[
            {
                "mention": "Ace",
                "canonical_name": "Taylor Example",
                "given_name": "Taylor",
                "family_name": "Example",
                "aliases": ["Ace"],
                "primary_email": "taylor@example.com",
                "source": "google_people",
                "source_kind": "google_contacts",
                "account": "account@example.com",
                "card_id": "people/c1",
            }
        ],
        event_identity_hints={
            "event_terms": ["Launch Week"],
            "first_name_terms": ["Nova"],
            "warehouse_snippets": [
                {
                    "source": "google_drive_file_texts",
                    "occurred_at": "2026-04-20T10:00:00+00:00",
                    "title": "Launch Week Team Plan",
                    "snippet": "Nova is listed with the Launch Week organizing team.",
                }
            ],
        },
    )

    assert '"speaker_label": "A"' in task_input
    assert "recorded_at_interpretations" in task_input
    assert "diarized_segments" in task_input
    assert "contact_alias_hints" in task_input
    assert "Taylor Example" in task_input
    assert "event_identity_hints" in task_input
    assert "Launch Week Team Plan" in task_input
    assert "source of truth for speaker turns" not in task_input


def test_enrichment_task_input_omits_raw_provider_payload_fields() -> None:
    task_input = enrichment_task_input(
        recording={
            "recording_id": "rec1",
            "recorded_at": datetime(2026, 4, 27, tzinfo=UTC),
            "title": "Title",
            "transcript_text": "Transcript text",
            "raw_result_json": "LEAK_RECORDING_RAW",
        },
        calendar_candidates=[
            {
                "event_id": "event1",
                "summary": "Meeting",
                "start_at": "2026-04-27T10:00:00+00:00",
                "end_at": "2026-04-27T10:30:00+00:00",
                "location": "Zoom",
                "attendees": ["Guest Person"],
                "attendee_details": [{"display_name": "Guest Person", "email": "guest@example.com"}],
                "identity_hints": {
                    "guest@example.com": {
                        "email": "guest@example.com",
                        "possible_names": ["Guest Person"],
                        "gmail_mentions": [
                            {"from_address": "a@example.com", "subject": f"subject {index}", "snippet": "snippet"}
                            for index in range(10)
                        ],
                        "raw_result_json": "LEAK_HINT_RAW",
                    }
                },
                "raw_json": "LEAK_EVENT_RAW",
            }
        ],
        transcript_segments=[
            {
                "segment_index": 0,
                "speaker_label": "A",
                "start_ms": 0,
                "end_ms": 1000,
                "confidence": 0.9,
                "text": "Hello",
                "words_json": "LEAK_WORDS",
                "raw_result_json": "LEAK_SEGMENT_RAW",
            }
        ],
        event_identity_hints={
            "event_terms": ["Launch Week"],
            "first_name_terms": ["Nova"],
            "warehouse_snippets": [
                {
                    "source": "gmail_messages",
                    "occurred_at": "2026-04-20T10:00:00+00:00",
                    "title": "Launch Week",
                    "snippet": "Nova is coordinating Launch Week.",
                    "raw_result_json": "LEAK_EVENT_HINT_RAW",
                }
            ],
            "raw_result_json": "LEAK_EVENT_HINT_ROOT",
        },
    )

    assert "LEAK_" not in task_input
    assert "words_json" not in task_input
    assert "raw_result_json" not in task_input
    assert "subject 0" in task_input
    assert "subject 3" not in task_input


def test_event_identity_terms_and_first_names_use_event_context() -> None:
    recording = {
        "title": "Voice memo",
        "transcript_text": "At Launch Week, Nova asks Quill about the organizing team.",
    }
    transcript_segments = [
        {
            "segment_index": 0,
            "speaker_label": "A",
            "start_ms": 0,
            "end_ms": 1000,
            "confidence": 0.9,
            "text": "At Launch Week, Nova asks Quill about the organizing team.",
        }
    ]

    terms = event_identity_terms(
        recording=recording,
        calendar_candidates=[{"summary": "Launch Week - Team Dinner"}],
        transcript_segments=transcript_segments,
    )
    first_names = event_identity_first_names(
        recording=recording,
        transcript_segments=transcript_segments,
        event_terms=terms,
    )

    assert "Launch Week" in terms
    assert "Nova" in first_names
    assert "Quill" in first_names
    assert "Launch" not in first_names


def test_load_event_identity_hints_queries_event_specific_context() -> None:
    queries = []

    class Warehouse:
        def _query(self, sql):
            queries.append(sql)
            if "FROM @google_drive_file_texts" in sql:
                return [
                    (
                        datetime(2026, 4, 20, tzinfo=UTC),
                        "Launch Week Team Plan",
                        "Nova and Quill are listed on the Launch Week organizing team.",
                    )
                ]
            if "FROM @gmail_messages" in sql:
                return [
                    (
                        datetime(2026, 4, 21, tzinfo=UTC),
                        "lead@example.com",
                        "Launch Week roster",
                        "Nova is coordinating Launch Week.",
                    )
                ]
            if "FROM @slack_messages" in sql:
                return [
                    (
                        datetime(2026, 4, 22, tzinfo=UTC),
                        "Coordinator",
                        "Quill is helping staff Launch Week.",
                    )
                ]
            if "FROM @marts_messages_messages" in sql:
                return [
                    (
                        datetime(2026, 4, 23, tzinfo=UTC),
                        "Nova Example",
                        "Launch Week dinner is at 7, Quill is bringing the slides",
                    )
                ]
            return []

    hints = load_event_identity_hints(
        Warehouse(),
        recording={
            "recording_id": "rec1",
            "recorded_at": datetime(2026, 4, 27, tzinfo=UTC),
            "title": "Voice memo",
            "transcript_text": "At Launch Week, Nova asks Quill about the plan.",
        },
        calendar_candidates=[{"summary": "Launch Week - Team Dinner"}],
        transcript_segments=[],
    )

    assert "Launch Week" in hints["event_terms"]
    assert "Nova" in hints["first_name_terms"]
    assert "Quill" in hints["first_name_terms"]
    # iMessage and WhatsApp reach the agent through the conforming chat mart
    # (C5): a person known only from a text thread was invisible before.
    assert {snippet["source"] for snippet in hints["warehouse_snippets"]} == {
        "google_drive_file_texts",
        "gmail_messages",
        "slack_messages",
        "marts_messages_messages",
    }
    assert any("google_drive_file_texts" in query for query in queries)
    assert any("gmail_messages" in query for query in queries)
    assert any("slack_messages" in query for query in queries)
    assert all("Launch Week" in query for query in queries)
    assert all("Nova" in query for query in queries)


def test_load_contact_alias_hints_reads_human_edited_contact_nicknames() -> None:
    queries = []

    class Warehouse:
        def _query(self, sql):
            queries.append(sql)
            return [
                (
                    "Ace",
                    "google_people",
                    "account@example.com",
                    "google_contacts",
                    "people/c1",
                    "Taylor Example",
                    "Taylor",
                    "Example",
                    "taylor@example.com",
                    "",
                    "",
                    '[{"value":"Ace"}]',
                )
            ]

    hints = load_contact_alias_hints(
        Warehouse(),
        recording={"title": "Call", "transcript_text": "Talked to Ace about launch."},
        transcript_segments=[],
    )

    assert "jsonb_array_elements(c.nicknames)" in queries[0]
    assert hints == [
        {
            "mention": "Ace",
            "canonical_name": "Taylor Example",
            "given_name": "Taylor",
            "family_name": "Example",
            "primary_email": "taylor@example.com",
            "organization": "",
            "job_title": "",
            "aliases": ["Ace"],
            "source": "google_people",
            "source_kind": "google_contacts",
            "account": "account@example.com",
            "card_id": "people/c1",
        }
    ]


def test_apply_contact_alias_corrections_rewrites_final_output_aliases() -> None:
    corrected = apply_contact_alias_corrections(
        result={
            "summary": "Ace will send the draft. Ace Cooper is a different full-name phrase.",
            "action_items": ["Ask Ace to follow up."],
            "evidence": ["Contact alias data maps 'Ace' to Taylor Example."],
            "transcript": "Jordan Example: I talked to Ace about it.",
            "speaker_map": [{"speaker_label": "B", "speaker_name": "Taylor Example", "evidence": "Ace appears in ASR."}],
        },
        contact_alias_hints=[
            {
                "mention": "Ace",
                "canonical_name": "Taylor Example",
                "given_name": "Taylor",
                "aliases": ["Ace"],
            }
        ],
    )

    assert corrected["summary"] == "Taylor will send the draft. Ace Cooper is a different full-name phrase."
    assert corrected["action_items"] == ["Ask Taylor to follow up."]
    assert corrected["evidence"] == ["Contact alias data maps 'Taylor' to Taylor Example."]
    assert corrected["transcript"] == "Jordan Example: I talked to Taylor about it."
    assert corrected["speaker_map"][0]["evidence"] == "Taylor appears in ASR."


def test_load_enrichment_candidates_scans_all_recording_history_without_limit() -> None:
    queries = []

    class Warehouse:
        def _query(self, sql):
            queries.append(sql)
            return []

    load_enrichment_candidates(
        Warehouse(),
        provider="agent_codex",
        prompt_version="test-prompt",
        limit=None,
    )

    assert "f.recorded_at >=" not in queries[0]
    assert "LIMIT" not in queries[0]
    # The mart, never the raw table: enrichment serves the voice DOMAIN, and
    # scanning one source's raw rows is what left the second source with no
    # summaries at all.
    assert "FROM @marts_voice_memos_recordings AS f" in queries[0]
    assert "@apple_voice_memos_files" not in queries[0]
    assert "INNER JOIN @apple_voice_memos_transcription_runs AS r" in queries[0]
    assert "FROM @apple_voice_memos_enrichments" in queries[0]
    assert "r.content_sha256" in queries[0]
    assert "f.content_sha256 = r.content_sha256" in queries[0]
    assert "e.content_sha256 = r.content_sha256" in queries[0]
    assert "AND prompt_version = 'test-prompt'\n              AND status = 'completed'" not in queries[0]
    assert "COALESCE(a.error_attempts, 0) < 5" in queries[0]


def test_load_enrichment_candidates_keeps_limit_when_configured() -> None:
    queries = []

    class Warehouse:
        def _query(self, sql):
            queries.append(sql)
            return []

    load_enrichment_candidates(
        Warehouse(),
        provider="agent_codex",
        prompt_version="test-prompt",
        limit=12,
    )

    assert "LIMIT 12" in queries[0]


def test_load_enrichment_candidates_can_force_current_prompt_version() -> None:
    queries = []

    class Warehouse:
        def _query(self, sql):
            queries.append(sql)
            return []

    load_enrichment_candidates(
        Warehouse(),
        provider="agent_codex",
        prompt_version="test-prompt",
        limit=1,
        force_prompt_version=True,
    )

    assert "AND prompt_version = 'test-prompt'\n              AND status = 'completed'" in queries[0]


def test_load_enrichment_candidates_does_not_use_model_for_completion_and_failure_identity() -> None:
    queries = []

    class Warehouse:
        def _query(self, sql):
            queries.append(sql)
            return []

    load_enrichment_candidates(
        Warehouse(),
        provider="agent_codex",
        prompt_version="test-prompt",
        limit=1,
    )

    assert "model =" not in queries[0]


def test_load_enrichment_candidates_uses_agent_error_budget() -> None:
    queries = []

    class Warehouse:
        def _query(self, sql):
            queries.append(sql)
            return []

    load_enrichment_candidates(
        Warehouse(),
        provider="agent_codex",
        prompt_version="test-prompt",
        limit=1,
        max_error_attempts=7,
    )

    assert "FROM @agent_runs" in queries[0]
    assert "FROM @apple_voice_memos_enrichments" in queries[0]
    assert "provider = 'codex'" in queries[0]
    assert "provider = 'agent_codex'" in queries[0]
    assert "prompt_version = 'test-prompt'" not in queries[0].split("SELECT subject_id, count(*) AS error_attempts", 1)[1]
    assert "COALESCE(a.error_attempts, 0) < 7" in queries[0]


def test_load_enrichment_candidates_can_disable_agent_error_budget() -> None:
    queries = []

    class Warehouse:
        def _query(self, sql):
            queries.append(sql)
            return []

    load_enrichment_candidates(
        Warehouse(),
        provider="agent_codex",
        prompt_version="test-prompt",
        limit=1,
        max_error_attempts=0,
    )

    assert "FROM @agent_runs" not in queries[0]
    assert "error_attempts" not in queries[0]


def test_ensure_recording_level_fields_fills_no_calendar_outputs() -> None:
    result = ensure_recording_level_fields(
        recording={
            "recorded_at": datetime(2026, 4, 27, 14, 0, tzinfo=UTC),
        },
        transcript_segments=[{"end_ms": 90_000}],
        result={
            "calendar_event_id": "",
            "calendar_confidence": 0,
            "title": "",
            "start_at": "",
            "end_at": "",
            "evidence": [],
        },
    )

    assert result["title"] == "Voice Memo 2026-04-27 14:00 UTC"
    assert result["start_at"] == "2026-04-27T14:00:00+00:00"
    assert result["end_at"] == "2026-04-27T14:01:30+00:00"
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
        recording={"account": "zach@example.com", "recording_id": "rec1", "content_sha256": "audio-hash"},
        result={
            "calendar_event_id": "event1",
            "calendar_confidence": 0.8,
            "title": "Meeting",
            "start_at": "2026-04-27T10:00:00+00:00",
            "end_at": "2026-04-27T11:00:00+00:00",
            "location": "Zoom",
            "participants": ["a@example.com"],
            "speaker_map": [],
            "transcript": "Speaker A: Hello",
            "summary": "Summary",
            "action_items": [],
            "evidence": ["Evidence"],
        },
        provider="agent_codex",
        model="gpt-5.3-codex",
        prompt_version=AGENT_ENRICHMENT_PROMPT_VERSION,
        status="completed",
        error="",
        created_at=datetime(2026, 4, 27, tzinfo=UTC),
    )

    assert row["calendar_event_id"] == "event1"
    assert row["content_sha256"] == "audio-hash"
    assert row["start_at"] == datetime(2026, 4, 27, 10, tzinfo=UTC)
    assert row["participants_json"] == '["a@example.com"]'
    assert row["transcript"] == "Speaker A: Hello"


def test_enrichment_row_normalizes_corrected_transcript_prefixes() -> None:
    row = enrichment_row(
        recording={"account": "zach@example.com", "recording_id": "rec1"},
        result={
            "calendar_event_id": "",
            "calendar_confidence": 0,
            "title": "Meeting",
            "start_at": "",
            "end_at": "",
            "location": "",
            "participants": [],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 1, "evidence": "intro"},
            ],
            "transcript": "Alex Rivera: Hello.\n\nThis is continued.",
            "summary": "Summary",
            "action_items": [],
            "evidence": [],
        },
        provider="agent_codex",
        model="gpt-5.3-codex",
        prompt_version=AGENT_ENRICHMENT_PROMPT_VERSION,
        status="completed",
        error="",
        created_at=datetime(2026, 4, 27, tzinfo=UTC),
    )

    assert row["transcript"] == "Alex Rivera: Hello.\n\nAlex Rivera: This is continued."


def test_enrichment_row_canonicalizes_close_name_variants_to_verified_attendees() -> None:
    row = enrichment_row(
        recording={"account": "zach@example.com", "recording_id": "rec1"},
        result={
            "calendar_event_id": "",
            "calendar_confidence": 0,
            "title": "Meeting",
            "start_at": "",
            "end_at": "",
            "location": "",
            "participants": ["Alex Rivera", "Taylor Singh"],
            "speaker_map": [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 1, "evidence": "intro"},
                {"speaker_label": "B", "speaker_name": "Taylor Singh", "confidence": 1, "evidence": "calendar"},
            ],
            "transcript": "Alex Rivera: Hey Tayler, how are you?\n\nTaylor Singh: Good.",
            "summary": "Call with Tayler.",
            "action_items": [],
            "evidence": ["Opening line says Tayler, matching Taylor Singh."],
        },
        provider="agent_codex",
        model="gpt-5.3-codex",
        prompt_version=AGENT_ENRICHMENT_PROMPT_VERSION,
        status="completed",
        error="",
        created_at=datetime(2026, 4, 27, tzinfo=UTC),
    )

    assert "Hey Taylor" in row["transcript"]
    assert "Tayler" not in row["raw_result_json"]


def _command_event(index: int, command: str) -> AgentRunEvent:
    return AgentRunEvent(
        event_index=index,
        stream="stdout",
        event_type="item.completed",
        event_json={"type": "item.completed", "item": {"type": "command_execution", "command": command}},
        text="{}",
        created_at=datetime(2026, 4, 27, tzinfo=UTC),
    )


def test_count_warehouse_cli_calls_counts_executed_pdw_reads() -> None:
    events = [
        _command_event(0, "pdw search --priority self,direct,cc 'event person'"),
        _command_event(1, "pdw schema"),
        _command_event(2, "pdw sql -q 'attendees' 'SELECT 1'"),
        _command_event(3, "pdw columns google_calendar.events"),
        _command_event(4, "pdw call get_object --data '{}'"),
    ]

    assert count_warehouse_cli_calls(events) == 5


def test_count_warehouse_cli_calls_ignores_non_research_commands() -> None:
    events = [
        _command_event(0, "pdw help"),
        _command_event(1, "pdw version"),
        _command_event(2, "rg -n TODO ."),
    ]

    assert count_warehouse_cli_calls(events) == 0


def test_count_warehouse_cli_calls_ignores_prompt_text_that_only_mentions_pdw() -> None:
    """The prompt itself shows `pdw sql` examples; quoting them is not research."""

    echoed_prompt = AgentRunEvent(
        event_index=0,
        stream="stdout",
        event_type="user_message",
        event_json={
            "type": "user_message",
            "text": "Before final output, run `pdw schema` and multiple focused `pdw sql -q ...` calls.",
        },
        text="Before final output, run `pdw schema` and multiple `pdw sql` calls.",
        created_at=datetime(2026, 4, 27, tzinfo=UTC),
    )

    assert count_warehouse_cli_calls([echoed_prompt]) == 0


def test_canonicalize_text_verified_name_mentions_only_rewrites_close_name_variants() -> None:
    text = canonicalize_text_verified_name_mentions(
        "Hey Tayler, maybe we should talk with Morgan and Kory.",
        verified_names=["Taylor Singh", "Morgan Lee", "Cory Person"],
    )

    assert text == "Hey Taylor, maybe we should talk with Morgan and Cory."


def _long_session_segments() -> list[dict]:
    # Two sessions of one long recording: label A is a moderator in the first
    # session and a different person in the second, which a single
    # whole-recording speaker_map entry cannot express.
    return [
        {"segment_index": 0, "speaker_label": "A", "start_ms": 0, "end_ms": 600_000, "text": "Welcome to the first panel."},
        {"segment_index": 1, "speaker_label": "B", "start_ms": 600_000, "end_ms": 1_200_000, "text": "Thanks for having me."},
        {"segment_index": 2, "speaker_label": "A", "start_ms": 1_800_000, "end_ms": 2_400_000, "text": "I run the second session."},
        {"segment_index": 3, "speaker_label": "C", "start_ms": 2_400_000, "end_ms": 2_430_000, "text": "Yeah."},
    ]


def _valid_long_result(**overrides) -> dict:
    result = {
        "title": "Long Event",
        "start_at": "2026-04-27T14:00:00+00:00",
        "end_at": "2026-04-27T14:45:00+00:00",
        "participants": ["Alex Rivera", "Priya Narayan"],
        "speaker_map": [
            {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 0.99, "evidence": "test"},
            {"speaker_label": "B", "speaker_name": "Priya Narayan", "confidence": 0.99, "evidence": "test"},
        ],
        "speaker_turns": [],
        "summary": "s" * 5_000,
        "transcript": LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
    }
    result.update(overrides)
    return result


def test_validate_enrichment_result_flags_a_substantial_label_missing_from_speaker_map() -> None:
    segments = _long_session_segments()
    issues = validate_enrichment_result(
        recording={"transcript_text": "x" * 20_000},
        transcript_segments=segments,
        result=_valid_long_result(
            speaker_map=[
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 0.99, "evidence": "test"},
            ]
        ),
    )

    # B speaks for ten minutes and is unmapped; C's single 30-second "Yeah." is not substantial.
    assert any("speaker_map is missing diarized labels ['B']" in issue for issue in issues)


def test_validate_enrichment_result_requires_a_summary_that_scales_with_a_long_recording() -> None:
    segments = _long_session_segments()  # 40.5 minutes
    short = validate_enrichment_result(
        recording={"transcript_text": "x" * 20_000},
        transcript_segments=segments,
        result=_valid_long_result(summary="A panel happened."),
    )
    long_enough = validate_enrichment_result(
        recording={"transcript_text": "x" * 20_000},
        transcript_segments=segments,
        result=_valid_long_result(summary="s" * 2_100),
    )

    assert any("summary is too short for a 40-minute recording" in issue for issue in short)
    assert not any("summary is too short" in issue for issue in long_enough)


def test_validate_enrichment_result_does_not_demand_a_long_summary_for_a_short_note() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "Remember to email Priya."},
        transcript_segments=[
            {"segment_index": 0, "speaker_label": "A", "start_ms": 0, "end_ms": 20_000, "text": "Remember to email Priya."}
        ],
        result=_valid_long_result(
            summary="Reminder to email Priya.",
            transcript="Alex Rivera: Remember to email Priya.",
            speaker_map=[{"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 0.99, "evidence": "test"}],
        ),
    )

    assert not any("summary is too short" in issue for issue in issues)


def test_enrichment_prompt_teaches_sessions_spans_and_sectioned_summaries() -> None:
    prompt = enrichment_user_prompt(input_file=AGENT_USER_PROMPT_INPUT_FILE)

    assert "speaker_turns" in prompt
    assert "sessions" in prompt
    assert "sectioned summary" in prompt
    assert "one speaker_turns entry per turn" in prompt
    assert "introduced by first name only" in prompt
    assert "recording owner" in prompt
    assert AGENT_ENRICHMENT_PROMPT_VERSION == "apple-voice-memo-enrichment-agent-v9"


def test_an_unresolved_label_is_not_named_after_someone_its_evidence_merely_mentions() -> None:
    # Measured on the 2026-10-03 benchmark: the agent marked the interviewer's
    # label unresolved with evidence "interviewer turns in <guest>'s session",
    # and local assembly printed every interviewer line as the guest.
    result = apply_segment_preserving_transcript_fallback(
        recording={"transcript_text": "x" * 20_000},
        transcript_segments=[
            {"segment_index": 0, "speaker_label": "C", "start_ms": 0, "end_ms": 5_000, "text": "What do you mean by that?"},
            {"segment_index": 1, "speaker_label": "D", "start_ms": 5_000, "end_ms": 9_000, "text": "I mean power."},
        ],
        result={
            "participants": ["Alex Rivera"],
            "speaker_map": [
                {
                    "speaker_label": "C",
                    "speaker_name": "Unresolved speaker C",
                    "confidence": 0.98,
                    "evidence": "Substantive interviewer turns in Alex Rivera's session; the introduced name is unclear.",
                },
                {"speaker_label": "D", "speaker_name": "Alex Rivera", "confidence": 0.99, "evidence": "long answers"},
            ],
            "speaker_turns": [],
            "transcript": LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
            "evidence": [],
        },
    )

    assert result["transcript"].splitlines() == [
        "Unresolved speaker C: What do you mean by that?",
        "Alex Rivera: I mean power.",
    ]


def test_mixed_or_unresolved_label_wording_is_not_flagged_as_an_ambiguous_name() -> None:
    issues = validate_enrichment_result(
        recording={"transcript_text": "x" * 20_000},
        transcript_segments=[],
        result=_valid_long_result(
            speaker_map=[
                {"speaker_label": "A", "speaker_name": "Mixed or unresolved speakers A", "confidence": 0.98, "evidence": "x"},
                {"speaker_label": "B", "speaker_name": "Priya Narayan", "confidence": 0.99, "evidence": "x"},
            ]
        ),
    )

    assert not any("ambiguous speaker_name" in issue for issue in issues)
