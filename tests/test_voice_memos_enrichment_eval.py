from __future__ import annotations

import json

from personal_data_warehouse.voice_memos_enrichment_eval import score_enrichment_row


def base_row(**overrides):
    row = {
        "recording_id": "rec1",
        "status": "completed",
        "error": "",
        "calendar_event_id": "event1",
        "calendar_confidence": 0.95,
        "meeting_start_at": "2026-04-27T10:00:00+00:00",
        "meeting_end_at": "2026-04-27T11:00:00+00:00",
        "attendees_json": json.dumps(["Alex Rivera", "Riley Chen"]),
        "speaker_map_json": json.dumps(
            [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 0.95, "evidence": "intro"},
                {"speaker_label": "B", "speaker_name": "Riley", "confidence": 0.95, "evidence": "greeting"},
            ]
        ),
        "corrected_transcript": "\n\n".join(
            [
                "Alex Rivera: Hello Riley.",
                "Riley: Hello Alex.",
                "Alex Rivera: Tell me about your project.",
                "Riley: It is a workflow tool.",
            ]
            * 6
        ),
        "meeting_notes": "These are detailed notes. " * 30,
        "summary": "This is a useful summary of the meeting. " * 5,
        "action_items_json": json.dumps(["Follow up"]),
        "evidence_json": json.dumps(["Calendar matched"]),
        "raw_result_json": json.dumps({"__tool_calls": [{"output": {"csv": "a\n1", "error": ""}}]}),
    }
    row.update(overrides)
    return row


def test_score_enrichment_row_passes_good_output() -> None:
    score = score_enrichment_row(base_row(), diarized_labels=["A", "B"])

    assert score.passed
    assert score.score >= 0.9
    assert score.issues == []


def test_score_enrichment_row_penalizes_missing_speaker_labels() -> None:
    score = score_enrichment_row(base_row(), diarized_labels=["A", "B", "C"])

    assert not score.passed
    assert any("speaker_map missing diarized labels" in issue for issue in score.issues)


def test_score_enrichment_row_penalizes_unknown_transcript_prefix() -> None:
    row = base_row(corrected_transcript="\n\n".join(["Alex Rivera: Hello.", "Unknown: Hi.", "Riley: Bye."] * 12))

    score = score_enrichment_row(row, diarized_labels=["A", "B"])

    assert not score.passed
    assert any("prefixes outside speaker_map" in issue for issue in score.issues)


def test_score_enrichment_row_scores_single_newline_turns() -> None:
    row = base_row(
        corrected_transcript="\n".join(
            [
                "Alex Rivera: Hello. This is a longer turn with enough words for scoring.",
                "Riley: Hi. This is another longer turn with enough words for scoring.",
            ]
            * 12
        )
    )

    score = score_enrichment_row(row, diarized_labels=["A", "B"])

    assert score.components["corrected_transcript"] == 1.0


def test_score_enrichment_row_allows_attendee_turn_level_prefixes() -> None:
    row = base_row(
        speaker_map_json=json.dumps(
            [
                {
                    "speaker_label": "A",
                    "speaker_name": "Unresolved mixed speaker (label A)",
                    "confidence": 0.5,
                    "evidence": "mixed",
                }
            ]
        ),
        attendees_json=json.dumps(["Alex Rivera", "Riley Chen"]),
        corrected_transcript="\n".join(["Alex Rivera: Hello.", "Unresolved mixed speaker (label A): Hi."] * 12),
    )

    score = score_enrichment_row(row, diarized_labels=["A"])

    assert not any("prefixes outside speaker_map" in issue for issue in score.issues)


def test_score_enrichment_row_allows_unresolved_low_confidence_speaker() -> None:
    row = base_row(
        speaker_map_json=json.dumps(
            [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 0.95, "evidence": "intro"},
                {
                    "speaker_label": "B",
                    "speaker_name": "Speaker B (mixed diarization)",
                    "confidence": 0.4,
                    "evidence": "mixed",
                },
            ]
        ),
        corrected_transcript="\n\n".join(["Alex Rivera: Hello.", "Speaker B (mixed diarization): Hi."] * 12),
    )

    score = score_enrichment_row(row, diarized_labels=["A", "B"])

    assert score.components["speaker_map"] >= 0.9


def test_score_enrichment_row_fails_low_confidence_real_speaker_name() -> None:
    row = base_row(
        speaker_map_json=json.dumps(
            [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 0.95, "evidence": "intro"},
                {"speaker_label": "B", "speaker_name": "Riley", "confidence": 0.6, "evidence": "weak"},
            ]
        )
    )

    score = score_enrichment_row(row, diarized_labels=["A", "B"])

    assert not score.passed
    assert any("maps to 'Riley' with confidence 0.60" in issue for issue in score.issues)


def test_score_enrichment_row_fails_candidate_list_speaker_name() -> None:
    row = base_row(
        speaker_map_json=json.dumps(
            [
                {
                    "speaker_label": "A",
                    "speaker_name": "Interviewer (mixed: Casey Morgan / Taylor Reed)",
                    "confidence": 0.5,
                    "evidence": "mixed",
                },
                {"speaker_label": "B", "speaker_name": "Riley Chen", "confidence": 0.95, "evidence": "intro"},
            ]
        ),
        corrected_transcript="\n\n".join(
            ["Interviewer (mixed: Casey Morgan / Taylor Reed): Hello.", "Riley Chen: Hi."] * 12
        ),
    )

    score = score_enrichment_row(row, diarized_labels=["A", "B"])

    assert not score.passed
    assert any("ambiguous candidate speaker name" in issue for issue in score.issues)


def test_score_enrichment_row_fails_uncertainty_inside_speaker_name() -> None:
    row = base_row(
        speaker_map_json=json.dumps(
            [
                {
                    "speaker_label": "A",
                    "speaker_name": "Interviewer (likely Casey Morgan; may include Taylor Reed)",
                    "confidence": 0.5,
                    "evidence": "mixed",
                },
                {"speaker_label": "B", "speaker_name": "Riley Chen", "confidence": 0.95, "evidence": "intro"},
            ]
        ),
        corrected_transcript="\n\n".join(
            ["Interviewer (likely Casey Morgan; may include Taylor Reed): Hello.", "Riley Chen: Hi."] * 12
        ),
    )

    score = score_enrichment_row(row, diarized_labels=["A", "B"])

    assert not score.passed
    assert any("ambiguous candidate speaker name" in issue for issue in score.issues)


def test_score_enrichment_row_fails_person_guess_inside_unresolved_speaker_name() -> None:
    row = base_row(
        speaker_map_json=json.dumps(
            [
                {
                    "speaker_label": "A",
                    "speaker_name": "Mixed/Unresolved (Casey Morgan + interviewer)",
                    "confidence": 0.5,
                    "evidence": "mixed",
                },
                {"speaker_label": "B", "speaker_name": "Riley Chen", "confidence": 0.95, "evidence": "intro"},
            ]
        ),
        corrected_transcript="\n\n".join(
            ["Mixed/Unresolved (Casey Morgan + interviewer): Hello.", "Riley Chen: Hi."] * 12
        ),
    )

    score = score_enrichment_row(row, diarized_labels=["A", "B"])

    assert not score.passed
    assert any("ambiguous candidate speaker name" in issue for issue in score.issues)


def test_score_enrichment_row_treats_must_haves_as_severe() -> None:
    row = base_row(
        meeting_start_at="",
        meeting_end_at="",
        attendees_json=json.dumps([]),
        corrected_transcript="\n\n".join(
            ["Alex Rivera: Hat Club discussed Start Dance.", "Riley: Hackertime and Open Router are useful."] * 12
        ),
    )

    score = score_enrichment_row(row, diarized_labels=["A", "B"])

    assert not score.passed
    assert any("meeting_start_at/end_at" in issue for issue in score.issues)
    assert any("attendees are empty" in issue for issue in score.issues)
    assert any("bad ASR terms" in issue for issue in score.issues)


def test_score_enrichment_row_fails_unresolved_attendee_emails_and_known_incomplete_names() -> None:
    row = base_row(
        attendees_json=json.dumps(["host@example.com", "guest@example.com"]),
        speaker_map_json=json.dumps(
            [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 0.95, "evidence": "intro"},
                {"speaker_label": "B", "speaker_name": "Alex", "confidence": 0.95, "evidence": "calendar attendee"},
            ]
        ),
    )

    score = score_enrichment_row(row, diarized_labels=["A", "B"])

    assert not score.passed
    assert any("unresolved email addresses" in issue for issue in score.issues)
    assert any("Alex" in issue and "incomplete" in issue for issue in score.issues)


def test_score_enrichment_row_fails_incomplete_attendee_names() -> None:
    row = base_row(attendees_json=json.dumps(["Alex Rivera", "Alex"]))

    score = score_enrichment_row(row, diarized_labels=["A", "B"])

    assert not score.passed
    assert any("attendees contain incomplete names" in issue for issue in score.issues)


def test_score_enrichment_row_fails_attendee_email_name_hybrids() -> None:
    row = base_row(
        attendees_json=json.dumps(["Riley (riley@example.com)", "Alex Rivera"]),
        speaker_map_json=json.dumps(
            [
                {"speaker_label": "A", "speaker_name": "Alex Rivera", "confidence": 0.95, "evidence": "intro"},
                {"speaker_label": "B", "speaker_name": "Riley (riley@example.com)", "confidence": 0.95, "evidence": "calendar"},
            ]
        ),
    )

    score = score_enrichment_row(row, diarized_labels=["A", "B"])

    assert not score.passed
    assert any("unresolved email" in issue or "incomplete names" in issue for issue in score.issues)
