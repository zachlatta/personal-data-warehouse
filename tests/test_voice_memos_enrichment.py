from __future__ import annotations

from datetime import UTC, datetime

from personal_data_warehouse.voice_memos_enrichment import (
    AGENT_ENRICHMENT_PROMPT_VERSION,
    LOCAL_TRANSCRIPT_ASSEMBLY_SENTINEL,
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
    recording_time_interpretations,
    validate_enrichment_result,
)


class FakeIdentityWarehouse:
    def _query(self, sql):
        if "FROM slack_users" in sql:
            return [("guest@example.com", "guest", "guest", "", "U1")]
        if "FROM gmail_messages" in sql:
            return [("system@example.com", "Guest Person accepted their invite", "Guest Person joined")]
        return []


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
    hints = load_attendee_identity_hints(
        FakeIdentityWarehouse(),
        [{"display_name": "", "email": "guest@example.com"}],
    )

    assert hints["guest@example.com"]["possible_names"] == ["Guest Person"]
    assert hints["guest@example.com"]["gmail_mentions"][0]["subject"] == "Guest Person accepted their invite"


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
    assert "Transcript attribution is turn-level" in prompt
    assert "below 0.9 confidence" in prompt
    assert "Hard requirements: accurate date/time" in prompt
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
        provider="agent_codex",
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
        provider="agent_codex",
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
        recording={"account": "zach@example.com", "recording_id": "rec1"},
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


def test_canonicalize_text_verified_name_mentions_only_rewrites_close_name_variants() -> None:
    text = canonicalize_text_verified_name_mentions(
        "Hey Tayler, maybe we should talk with Morgan and Kory.",
        verified_names=["Taylor Singh", "Morgan Lee", "Cory Person"],
    )

    assert text == "Hey Taylor, maybe we should talk with Morgan and Cory."
