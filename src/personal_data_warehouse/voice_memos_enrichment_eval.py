from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
from typing import Any


PASSING_SCORE = 0.85


@dataclass(frozen=True)
class RubricScore:
    recording_id: str
    score: float
    passed: bool
    components: dict[str, float]
    issues: list[str] = field(default_factory=list)


def score_enrichment_row(row: dict[str, Any], *, diarized_labels: list[str]) -> RubricScore:
    components: dict[str, float] = {}
    issues: list[str] = []

    components["status"] = 1.0 if row.get("status") == "completed" and not row.get("error") else 0.0
    if components["status"] < 1:
        issues.append(f"status/error is not clean: {row.get('status')} {row.get('error')}")

    components["calendar"] = score_calendar(row, issues)
    components["must_haves"] = score_must_haves(row, issues)

    speaker_map = parse_json_list(row.get("speaker_map_json"))
    components["speaker_map"] = score_speaker_map(speaker_map, diarized_labels, issues)
    attendees = parse_json_list(row.get("attendees_json"))
    components["corrected_transcript"] = score_corrected_transcript(
        str(row.get("corrected_transcript") or ""),
        speaker_names=[
            *[str(item.get("speaker_name", "")) for item in speaker_map if isinstance(item, dict)],
            *[str(attendee) for attendee in attendees if isinstance(attendee, str)],
        ],
        issues=issues,
    )
    components["notes"] = score_notes(row, issues)
    components["tools"] = score_tools(row, issues)

    weights = {
        "status": 0.08,
        "calendar": 0.17,
        "must_haves": 0.22,
        "speaker_map": 0.18,
        "corrected_transcript": 0.18,
        "notes": 0.07,
        "tools": 0.10,
    }
    score = sum(components[name] * weight for name, weight in weights.items())
    return RubricScore(
        recording_id=str(row.get("recording_id", "")),
        score=round(score, 3),
        passed=score >= PASSING_SCORE and not severe_issues(issues),
        components={name: round(value, 3) for name, value in components.items()},
        issues=issues,
    )


def score_calendar(row: dict[str, Any], issues: list[str]) -> float:
    confidence = float(row.get("calendar_confidence") or 0)
    event_id = str(row.get("calendar_event_id") or "")
    if event_id:
        if confidence >= 0.9:
            return 1.0
        if confidence >= 0.75:
            issues.append(f"calendar confidence is moderate: {confidence:.2f}")
            return 0.8
        issues.append(f"calendar confidence is low for selected event: {confidence:.2f}")
        return 0.4
    if confidence <= 0.1:
        return 0.8
    issues.append("calendar confidence is nonzero but no event_id was selected")
    return 0.3


def score_must_haves(row: dict[str, Any], issues: list[str]) -> float:
    score = 0.0
    if parse_datetime_like(str(row.get("meeting_start_at") or "")) and parse_datetime_like(str(row.get("meeting_end_at") or "")):
        score += 0.3
    else:
        issues.append("meeting_start_at/end_at are missing or not parseable")

    attendees = parse_json_list(row.get("attendees_json"))
    unresolved_attendee_emails = [
        attendee
        for attendee in attendees
        if isinstance(attendee, str) and "@" in attendee and not unresolved_speaker_name(attendee)
    ]
    incomplete_attendee_names = incomplete_human_names(attendees)
    if (
        attendees
        and all(isinstance(attendee, str) and attendee.strip() for attendee in attendees)
        and not unresolved_attendee_emails
        and not incomplete_attendee_names
    ):
        score += 0.3
    else:
        if unresolved_attendee_emails:
            issues.append(f"attendees contain unresolved email addresses: {unresolved_attendee_emails[:5]}")
        elif incomplete_attendee_names:
            issues.append(f"attendees contain incomplete names: {incomplete_attendee_names[:5]}")
        else:
            issues.append("attendees are empty or malformed")

    corrected = str(row.get("corrected_transcript") or "")
    bad_terms = bad_transcription_terms(corrected)
    if not bad_terms:
        score += 0.25
    else:
        issues.append(f"corrected_transcript contains likely bad ASR terms: {bad_terms[:5]}")

    speaker_map = parse_json_list(row.get("speaker_map_json"))
    names = [str(item.get("speaker_name", "")) for item in speaker_map if isinstance(item, dict)]
    bad_names = incomplete_speaker_names_for_unresolved_attendees(names, attendees)
    if names and all(name == name.strip() and not malformed_person_name(name) for name in names) and not bad_names:
        score += 0.15
    else:
        if bad_names:
            issues.append(f"speaker names are incomplete when full names are known: {bad_names[:5]}")
        else:
            issues.append("speaker names are empty or malformed")
    return score


def score_speaker_map(speaker_map: list[Any], diarized_labels: list[str], issues: list[str]) -> float:
    if not diarized_labels:
        return 0.7
    if not speaker_map:
        issues.append("speaker_map is empty")
        return 0.0

    mapped_labels = [str(item.get("speaker_label", "")) for item in speaker_map if isinstance(item, dict)]
    missing = sorted(set(diarized_labels) - set(mapped_labels))
    extra = sorted(set(mapped_labels) - set(diarized_labels))
    if missing:
        issues.append(f"speaker_map missing diarized labels: {missing}")
    if extra:
        issues.append(f"speaker_map contains non-diarized labels: {extra}")

    valid = 1.0 - 0.2 * len(missing) - 0.2 * len(extra)
    for item in speaker_map:
        if not isinstance(item, dict):
            valid -= 0.2
            continue
        confidence = float(item.get("confidence") or 0)
        name = str(item.get("speaker_name") or "")
        if ambiguous_candidate_speaker_name(name):
            issues.append(f"speaker_map contains ambiguous candidate speaker name for {item.get('speaker_label')}: {name!r}")
            valid -= 0.2
        elif confidence < 0.9 and not unresolved_speaker_name(name):
            issues.append(f"speaker {item.get('speaker_label')} maps to {name!r} with confidence {confidence:.2f}")
            valid -= 0.2
        if confidence >= 0.9 and unresolved_speaker_name(name):
            valid -= 0.05
    return max(0.0, min(1.0, valid))


def score_corrected_transcript(corrected: str, *, speaker_names: list[str], issues: list[str]) -> float:
    if len(corrected) < 500:
        issues.append("corrected_transcript is too short")
        return 0.0

    line_turns = [line.strip() for line in corrected.splitlines() if line.strip()]
    paragraphs = line_turns if len(line_turns) > 1 else [paragraph.strip() for paragraph in re.split(r"\n\s*\n", corrected) if paragraph.strip()]
    if not paragraphs:
        issues.append("corrected_transcript has no paragraphs")
        return 0.0

    allowed = set(speaker_names)
    unprefixed = []
    unknown_prefixes = []
    for paragraph in paragraphs:
        prefix, separator, _rest = paragraph.partition(":")
        if not separator:
            unprefixed.append(paragraph[:80])
            continue
        if prefix.strip() not in allowed:
            unknown_prefixes.append(prefix.strip())

    prefix_score = 1.0
    if unprefixed:
        issues.append(f"corrected_transcript has unprefixed paragraphs: {unprefixed[:3]}")
        prefix_score -= min(0.5, 0.1 * len(unprefixed))
    if unknown_prefixes:
        issues.append(f"corrected_transcript uses prefixes outside speaker_map: {sorted(set(unknown_prefixes))[:5]}")
        prefix_score -= min(0.5, 0.1 * len(set(unknown_prefixes)))

    turn_count_score = min(1.0, len(paragraphs) / 20)
    narrative_penalty = 0.0
    narrative_openers = ("the meeting opened", "the call covered", "hack club met", "this was a")
    if corrected.strip().lower().startswith(narrative_openers):
        issues.append("corrected_transcript starts like narrative notes")
        narrative_penalty = 0.4
    return max(0.0, min(1.0, 0.7 * prefix_score + 0.3 * turn_count_score - narrative_penalty))


def score_notes(row: dict[str, Any], issues: list[str]) -> float:
    meeting_notes = str(row.get("meeting_notes") or "")
    summary = str(row.get("summary") or "")
    action_items = parse_json_list(row.get("action_items_json"))
    evidence = parse_json_list(row.get("evidence_json"))
    score = 0.0
    if len(summary) >= 120:
        score += 0.25
    else:
        issues.append("summary is short")
    if len(meeting_notes) >= 400:
        score += 0.35
    else:
        issues.append("meeting_notes are short")
    if action_items:
        score += 0.2
    else:
        issues.append("action_items are empty")
    if evidence:
        score += 0.2
    else:
        issues.append("evidence is empty")
    return score


def score_tools(row: dict[str, Any], issues: list[str]) -> float:
    raw = row.get("raw_result_json") or "{}"
    try:
        payload = json.loads(str(raw))
    except json.JSONDecodeError:
        issues.append("raw_result_json is invalid JSON")
        return 0.0
    calls = payload.get("__tool_calls") or []
    if not calls:
        issues.append("no ClickHouse tool calls were recorded")
        return 0.4
    errors = [call for call in calls if isinstance(call, dict) and call.get("output", {}).get("error")]
    if errors:
        issues.append(f"{len(errors)} ClickHouse tool calls returned errors")
        return max(0.0, 1.0 - 0.3 * len(errors))
    return 1.0


def parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def parse_datetime_like(value: str) -> bool:
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}[T ][0-2]\d:[0-5]\d", value.strip()))


def bad_transcription_terms(text: str) -> list[str]:
    patterns = {
        r"\bhat club\b": "Hat Club",
        r"\bhackertime\b": "Hackertime",
        r"\bhack\s+time\b": "Hack Time",
        r"\bstart\s+dance\b": "Start Dance",
        r"\bstar\s+dance\b": "Star Dance",
        r"\bopen router\b": "Open Router",
        r"\bopen ai\b": "Open AI",
        r"\bspindrift\b": "",
    }
    lowered = text.lower()
    bad = []
    for pattern, label in patterns.items():
        if label and re.search(pattern, lowered):
            bad.append(label)
    return bad


def malformed_person_name(name: str) -> bool:
    if unresolved_speaker_name(name):
        return False
    normalized = name.strip()
    if not normalized:
        return True
    if "@" in normalized or ("(" in normalized and ")" in normalized):
        return True
    return bool(re.search(r"\b(speaker|unknown|unidentified)\b", normalized, flags=re.IGNORECASE))


def incomplete_speaker_names_for_unresolved_attendees(names: list[str], attendees: list[Any]) -> list[str]:
    unresolved_attendee_emails = [
        str(attendee)
        for attendee in attendees
        if isinstance(attendee, str) and "@" in attendee and not unresolved_speaker_name(attendee)
    ]
    if not unresolved_attendee_emails:
        return []
    incomplete = []
    for name in names:
        stripped = name.strip()
        if (
            stripped
            and not unresolved_speaker_name(stripped)
            and " " not in stripped
            and "@" not in stripped
        ):
            incomplete.append(stripped)
    return incomplete


def incomplete_human_names(names: list[Any]) -> list[str]:
    incomplete = []
    for name in names:
        if not isinstance(name, str):
            continue
        stripped = name.strip()
        if not stripped or unresolved_speaker_name(stripped):
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


def unresolved_speaker_name(name: str) -> bool:
    normalized = name.lower()
    return (
        "speaker" in normalized
        or "unresolved" in normalized
        or "mixed" in normalized
        or "interviewer" in normalized
    )


def ambiguous_candidate_speaker_name(name: str) -> bool:
    normalized = f" {name.lower()} "
    if unresolved_speaker_name(name) and (
        any(marker in normalized for marker in (" likely ", " maybe ", " probably ", " possibly "))
        or ";" in name
        or ":" in name
    ):
        return True
    if unresolved_speaker_name(name) and unresolved_speaker_name_contains_person_guess(name):
        return True
    if " or " in normalized:
        return True
    return bool(re.search(r"\b[A-Z][a-z]+ [A-Z][a-z]+\s*/\s*[A-Z][a-z]+ [A-Z][a-z]+", name))


def unresolved_speaker_name_contains_person_guess(name: str) -> bool:
    allowed = {"Unresolved Speaker"}
    for match in re.findall(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", name):
        if match not in allowed:
            return True
    return False


def severe_issues(issues: list[str]) -> bool:
    severe_markers = (
        "status/error is not clean",
        "speaker_map missing diarized labels",
        "speaker_map contains non-diarized labels",
        "speaker_map contains ambiguous candidate speaker name",
        "speaker ",
        "corrected_transcript starts like narrative notes",
        "corrected_transcript uses prefixes outside speaker_map",
        "meeting_start_at/end_at are missing or not parseable",
        "attendees are empty or malformed",
        "attendees contain unresolved email addresses",
        "attendees contain incomplete names",
        "corrected_transcript contains likely bad ASR terms",
        "speaker names are incomplete when full names are known",
    )
    return any(any(issue.startswith(marker) for marker in severe_markers) for issue in issues)
