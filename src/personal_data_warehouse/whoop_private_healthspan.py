"""Typed rows from WHOOP's Healthspan, Advanced Labs and Coach payloads.

These three are UI (BFF) payloads, and the rest of this source keeps such
payloads as raw ``documents`` only, because a typed column over a restyled
screen goes quietly null. They are typed anyway, for one reason: the questions
they exist to answer -- "what is driving my WHOOP Age up", "which biomarker is
out of range", "what did the coach tell me" -- need numbers and text an agent
can sort and compare, and nobody answers them from a 90 KB JSON tree. So the
raw payload is still stored beside every typed row, and each parser here
fails LOUDLY on a payload it cannot read rather than returning nothing:
:class:`HealthspanShapeError` turns a restyle into a failed collection on
/pipelines instead of a week that silently has no contributors.

Shapes were captured from the live API on 2026-10-11; see
``docs/whoop-private-api.md``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, timezone
from typing import Any
import re

from personal_data_warehouse.whoop_sync import sync_version_from_datetime

EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)
EPOCH_DATE = date(1970, 1, 1)


class HealthspanShapeError(ValueError):
    """A Healthspan / labs / coach payload no longer has the shape we parse."""


# ------------------------------------------------------------- Healthspan --


#: A contributor's value is a display string; these turn it into a number in
#: the contributor's own unit. "7:29" with unit "h" is hours and minutes, not
#: 7.29 hours -- the one trap here worth a name.
_UNIT_BY_DISPLAY = {
    "h": "hours",
    "steps": "steps",
    "bpm": "bpm",
    "ml/kg/min": "ml_kg_min",
    "kg": "kg",
    "lb": "lb",
    "lbs": "lb",
}


def parse_display_number(display: Any) -> float | None:
    """'67%' -> 67, '7:29' -> 7.4833 (h:mm), '11,020' -> 11020, '16K' -> 16000, '--' -> None."""

    text = str(display or "").strip()
    if not text or set(text) <= {"-", " "}:
        return None
    clock = re.fullmatch(r"(-?\d+):(\d{2})\s*h?", text)
    if clock:
        hours, minutes = int(clock.group(1)), int(clock.group(2))
        sign = -1 if hours < 0 else 1
        return round(sign * (abs(hours) + minutes / 60), 6)
    match = re.search(r"-?\d[\d,]*(?:\.\d+)?", text)
    if not match:
        return None
    value = float(match.group(0).replace(",", ""))
    if re.search(r"\d\s*[kK]\b", text):
        value *= 1000
    return value


def _contributor_unit(value_display: str, unit_display: str, left_label: str) -> str:
    unit = (unit_display or "").strip().lower()
    if unit in _UNIT_BY_DISPLAY:
        return _UNIT_BY_DISPLAY[unit]
    if "%" in value_display or "%" in left_label:
        return "percent"
    if unit in {"", "xx"}:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", unit).strip("_")


def _younger_threshold(
    styles: Sequence[str], range_min: float | None, range_max: float | None
) -> tuple[str, float | None]:
    """Where on the contributor's own scale it starts LOWERING WHOOP Age.

    The bar under each contributor is ten divisions styled OLDER_n / NEUTRAL /
    YOUNGER_n across the labelled range. The boundary between the OLDER side
    and the YOUNGER side is the target, and which end is YOUNGER says whether
    more is better (steps) or less is (RHR). The divisions are drawn evenly
    across the labels, so this is the bar's own boundary -- WHOOP does not
    publish the exact cut-off, and a value within a division of it is a
    coin-flip, not a verdict.
    """

    kinds = ["younger" if s.startswith("YOUNGER") else "older" if s.startswith("OLDER") else "neutral" for s in styles]
    if not kinds or range_min is None or range_max is None or "younger" not in kinds or "older" not in kinds:
        return "", None
    count = len(kinds)
    step = (range_max - range_min) / count
    if kinds[-1] == "younger":
        index = next(i for i, kind in enumerate(kinds) if kind == "younger")
        return "higher", round(range_min + step * index, 6)
    if kinds[0] == "younger":
        index = next(i for i, kind in enumerate(kinds) if kind != "younger")
        return "lower", round(range_min + step * index, 6)
    return "", None


def _required(mapping: Any, key: str, where: str) -> Any:
    if not isinstance(mapping, Mapping) or key not in mapping:
        raise HealthspanShapeError(f"Healthspan payload has no {key!r} in {where}")
    return mapping[key]


@dataclass(frozen=True)
class HealthspanWeek:
    week_start: date
    previous_week_start: date | None
    week_row: dict[str, Any]
    contributor_rows: list[dict[str, Any]]


def _week_bounds(display: str, requested: date) -> tuple[date, date]:
    """'Oct 11 - Oct 17' around the requested day; the display carries no year."""

    parts = [p.strip() for p in str(display or "").split("-")]
    parsed: list[date] = []
    for part in parts[:2]:
        try:
            month_day = datetime.strptime(part, "%b %d")
        except ValueError:
            return requested, requested + timedelta(days=6)
        candidates = [
            date(year, month_day.month, month_day.day)
            for year in (requested.year - 1, requested.year, requested.year + 1)
        ]
        parsed.append(min(candidates, key=lambda d: abs((d - requested).days)))
    if len(parsed) == 1:
        parsed.append(parsed[0])
    return parsed[0], parsed[1]


def parse_healthspan(
    *, account: str, requested_day: str, payload: Any, synced_at: datetime
) -> HealthspanWeek:
    """One Healthspan week -> a ``healthspan_weeks`` row and its contributor rows."""

    content = _required(payload, "unlocked_content", "the response")
    if not isinstance(content, Mapping):
        raise HealthspanShapeError("Healthspan unlocked_content is not an object (the screen is locked?)")
    amoeba = _required(content, "whoop_age_amoeba", "unlocked_content")
    picker = content.get("date_picker") or {}
    requested = date.fromisoformat(requested_day)
    week_start, week_end = _week_bounds(picker.get("current_date_range_display", ""), requested)
    previous = picker.get("previous_date_time")
    previous_week_start = date.fromisoformat(str(previous)[:10]) if previous else None

    style = amoeba.get("style_values") or {}
    whoop_age = _float_or_none(style.get("age"))
    if whoop_age is None:
        whoop_age = parse_display_number(amoeba.get("age_value_display"))
    if whoop_age is None:
        raise HealthspanShapeError("Healthspan week carries no WHOOP Age")
    years_difference = _float_or_none(style.get("years_difference"))
    if years_difference is None:
        magnitude = parse_display_number(amoeba.get("years_difference_value_display")) or 0.0
        younger = "YOUNGER" in str(amoeba.get("years_difference_subtitle_display", "")).upper()
        years_difference = -magnitude if younger else magnitude
    pace = _float_or_none(style.get("pace_of_aging"))
    if pace is None:
        pace = parse_display_number(amoeba.get("pace_of_aging_display"))
    previous_amoeba = content.get("previous_whoop_age_amoeba") or {}
    previous_style = previous_amoeba.get("style_values") or {}

    week_row = {
        "account": account,
        "week_start": week_start,
        "week_end": week_end,
        "whoop_age": whoop_age,
        # WHOOP's own sign: years_difference is NEGATIVE when WHOOP Age is
        # younger than chronological age ("0.1 years younger" is -0.1).
        "years_difference": years_difference,
        "chronological_age": round(whoop_age - years_difference, 3),
        "pace_of_aging": pace if pace is not None else 0.0,
        "is_calibrating": int(bool(content.get("is_calibrating") or amoeba.get("is_calibrating"))),
        "previous_whoop_age": _float_or_none(previous_style.get("age"))
        or parse_display_number(previous_amoeba.get("age_value_display"))
        or 0.0,
        "previous_pace_of_aging": _float_or_none(previous_style.get("pace_of_aging"))
        or parse_display_number(previous_amoeba.get("pace_of_aging_display"))
        or 0.0,
        "age_subtitle": str(amoeba.get("age_subtitle_display") or ""),
        "next_update_display": str(payload.get("navigation_subtitle") or ""),
        "raw_json": {"whoop_age_amoeba": amoeba, "previous_whoop_age_amoeba": previous_amoeba, "date_picker": picker},
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }

    contributors: list[dict[str, Any]] = []
    for section in content.get("items") or []:
        if not isinstance(section, Mapping) or section.get("type") != "TILE_SECTION":
            continue
        section_content = section.get("content") or {}
        pillar = str(section_content.get("title_display") or "")
        for item in section_content.get("items") or []:
            if not isinstance(item, Mapping) or item.get("type") != "CONTRIBUTORS_CONTAINER":
                continue
            for entry in (item.get("content") or {}).get("contributors") or []:
                contributors.append(
                    _contributor_row(
                        account=account,
                        week_start=week_start,
                        pillar=pillar,
                        sort_order=len(contributors),
                        entry=entry,
                        synced_at=synced_at,
                    )
                )
    if not contributors:
        raise HealthspanShapeError(
            f"Healthspan week {week_start} parsed to zero contributors; the screen was restyled"
        )
    return HealthspanWeek(
        week_start=week_start,
        previous_week_start=previous_week_start,
        week_row=week_row,
        contributor_rows=contributors,
    )


def _contributor_row(
    *, account: str, week_start: date, pillar: str, sort_order: int, entry: Mapping[str, Any], synced_at: datetime
) -> dict[str, Any]:
    body = entry.get("content") or {}
    title = str(entry.get("title_display") or "")
    six = body.get("top_range_value") or {}
    thirty = body.get("expanded_bottom_range_value") or {}
    guidance: Mapping[str, Any] = {}
    trend_key = ""
    for expanded in body.get("expanded_items") or []:
        expanded_content = (expanded or {}).get("content") or {}
        key = ((expanded_content.get("destination") or {}).get("parameters") or {}).get("trend_key")
        if key and not trend_key:
            trend_key = str(key)
        if (expanded or {}).get("type") == "HEALTHSPAN_VOW":
            guidance = expanded_content
    contributor_key = trend_key or re.sub(r"[^A-Z0-9]+", "_", title.upper()).strip("_")
    left_label = str(body.get("range_left_label_display") or "")
    right_label = str(body.get("range_right_label_display") or "")
    range_min = parse_display_number(left_label)
    range_max = parse_display_number(right_label)
    styles = [str((division or {}).get("style") or "") for division in body.get("range_divisions") or []]
    direction, threshold = _younger_threshold(styles, range_min, range_max)
    six_display = str(six.get("value_display") or "")
    six_value = parse_display_number(six_display)
    thirty_display = str(thirty.get("value_display") or "")
    thirty_value = parse_display_number(thirty_display)
    impact_display = str(body.get("impact_value_display") or "")
    impact = parse_display_number(impact_display)
    return {
        "account": account,
        "week_start": week_start,
        "contributor_key": contributor_key,
        "pillar": pillar,
        "title": title,
        "sort_order": sort_order,
        "unit": _contributor_unit(six_display, str(six.get("value_unit_display") or ""), left_label),
        "has_value": int(six_value is not None),
        "six_month_value": six_value if six_value is not None else 0.0,
        "six_month_display": six_display,
        "thirty_day_value": thirty_value if thirty_value is not None else 0.0,
        "thirty_day_display": thirty_display,
        "impact_years": impact if impact is not None else 0.0,
        "impact_display": impact_display,
        "impact_direction": str(body.get("impact_value_style") or ""),
        "range_min": range_min if range_min is not None else 0.0,
        "range_max": range_max if range_max is not None else 0.0,
        "range_min_display": left_label,
        "range_max_display": right_label,
        "younger_direction": direction,
        "younger_threshold": threshold if threshold is not None else 0.0,
        "six_month_range_fraction": _float_or_none(six.get("value_range_percentage")) or 0.0,
        "thirty_day_range_fraction": _float_or_none(thirty.get("value_range_percentage")) or 0.0,
        "range_divisions": ",".join(styles),
        "guidance_title": str(guidance.get("title_display") or ""),
        "guidance_text": str(guidance.get("subtitle_display") or ""),
        "is_enabled": int(bool(entry.get("is_enabled"))),
        "raw_json": dict(entry),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def healthspan_biomarker_impacts(payload: Any) -> list[dict[str, str]]:
    """The 'Biomarkers impacting sleep/strain/fitness' chips, per pillar."""

    impacts: list[dict[str, str]] = []
    content = (payload or {}).get("unlocked_content") or {}
    for section in content.get("items") or []:
        section_content = (section or {}).get("content") or {}
        for item in section_content.get("items") or []:
            if (item or {}).get("type") != "LABS_IMPACT_CARD":
                continue
            card = item.get("content") or {}
            for chip in card.get("trends") or []:
                impacts.append(
                    {
                        "pillar": str(section_content.get("title_display") or card.get("category") or ""),
                        "biomarker": str(chip.get("title_display") or ""),
                        "trend": str(chip.get("trend") or ""),
                    }
                )
    return impacts


# ------------------------------------------------------------------ labs --


def lab_test_ids(history_payload: Any) -> list[str]:
    """Test ids from ``tests/history/paginated``, newest first."""

    ids: list[str] = []
    for section in (history_payload or {}).get("sections") or []:
        for item in (section or {}).get("items") or []:
            if (item or {}).get("type") != "LABS_TEST_ITEM":
                continue
            test_id = str(((item.get("content") or {}).get("id")) or "")
            if test_id and test_id not in ids:
                ids.append(test_id)
    return ids


def lab_test_biomarkers(test_payload: Any) -> list[Mapping[str, Any]]:
    """The biomarkers one test measured (the SEARCHABLE_BIOMARKERS_LIST)."""

    found: list[Mapping[str, Any]] = []
    for item in _walk_items(test_payload):
        if item.get("type") == "SEARCHABLE_BIOMARKERS_LIST":
            found.extend((item.get("content") or {}).get("biomarkers") or [])
    return found


def _walk_items(payload: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        if "type" in payload and isinstance(payload.get("content"), Mapping):
            yield payload
        for value in payload.values():
            yield from _walk_items(value)
    elif isinstance(payload, list):
        for value in payload:
            yield from _walk_items(value)


_LABEL_STATUS = {
    "ACCENT_POSITIVE_TREND": "OPTIMAL",
    "BIOMARKER_SUFFICIENT": "SUFFICIENT",
    "ACCENT_NEGATIVE_TREND": "OUT_OF_RANGE",
}


def biomarker_range_bands(detail_payload: Any) -> list[dict[str, Any]]:
    """The biomarker's range bands, in its own unit, from the detail chart.

    The detail screen draws the reference ranges as a band chart whose y-axis
    labels are the band boundaries, each styled with the status of the band
    that STARTS there (eGFR: 0 out of range, 60 sufficient, 95 optimal, 120 out
    of range, 150 the top). That chart is the only place the numeric ranges
    appear; the summary list carries only a 0-1 meter.
    """

    for item in _walk_items(detail_payload):
        if item.get("type") != "BIOMARKER_TREND_SEGMENT":
            continue
        graph = (item.get("content") or {}).get("graph") or {}
        labels = ((graph.get("plane") or {}).get("start_yaxis") or {}).get("labels") or []
        points = [
            (parse_display_number(label.get("label")), _LABEL_STATUS.get(str(label.get("style") or ""), ""))
            for label in labels
        ]
        points = [(value, status) for value, status in points if value is not None]
        bands = []
        for (low, status), (high, _next) in zip(points, points[1:], strict=False):
            bands.append({"low": low, "high": high, "status": status})
        return bands
    return []


def _optimal_range(bands: Sequence[Mapping[str, Any]]) -> tuple[float | None, float | None]:
    optimal = [band for band in bands if band.get("status") == "OPTIMAL"]
    if not optimal:
        return None, None
    return min(float(b["low"]) for b in optimal), max(float(b["high"]) for b in optimal)


def biomarker_education(detail_payload: Any) -> dict[str, str]:
    """WHOOP's explanation of the biomarker: what it is, why it matters, how to improve."""

    keys = {
        "what is": "description",
        "why it matters": "why_it_matters",
        "relationship to whoop": "relationship_to_whoop_metrics",
        "relationship to other": "relationship_to_other_biomarkers",
        "how to improve": "how_to_improve",
    }
    found = {value: "" for value in keys.values()}
    found["coach_summary"] = ""
    found["categories"] = ""
    header = (detail_payload or {}).get("header") or {}
    found["categories"] = ", ".join(str(p) for p in header.get("pills") or [])
    for item in _walk_items(detail_payload):
        content = item.get("content") or {}
        if item.get("type") == "WHOOP_COACH_VOW" and not found["coach_summary"]:
            found["coach_summary"] = str(content.get("vow") or "")
        if item.get("type") == "EXPANDABLE_LIST":
            for entry in content.get("items") or []:
                title = str(entry.get("title_display") or "").lower()
                for prefix, column in keys.items():
                    if title.startswith(prefix):
                        found[column] = str(entry.get("description_display") or "")
    return found


def lab_result_rows(
    *,
    account: str,
    test_id: str,
    test_record: Mapping[str, Any],
    test_payload: Any,
    biomarker_details: Mapping[str, Any],
    synced_at: datetime,
) -> list[dict[str, Any]]:
    """One row per biomarker one lab test measured."""

    biomarkers = lab_test_biomarkers(test_payload)
    if not biomarkers:
        raise HealthspanShapeError(f"lab test {test_id} parsed to zero biomarkers; the screen was restyled")
    tested_at = _parse_instant(test_record.get("test_date"))
    rows = []
    for biomarker in biomarkers:
        biomarker_id = str(biomarker.get("id") or "")
        detail = biomarker_details.get(biomarker_id) or {}
        bands = biomarker_range_bands(detail)
        optimal_low, optimal_high = _optimal_range(bands)
        education = biomarker_education(detail)
        value_display = str(biomarker.get("value") if biomarker.get("value") is not None else "")
        value = parse_display_number(value_display)
        rows.append(
            {
                "account": account,
                "test_id": test_id,
                "biomarker_id": biomarker_id,
                "biomarker_name": str(biomarker.get("title") or ""),
                "test_name": str(test_record.get("display_name") or ""),
                "tested_at": tested_at,
                "test_source": str(test_record.get("test_source") or ""),
                "upload_source": str(test_record.get("upload_source") or ""),
                "has_value": int(value is not None),
                "result_value": value if value is not None else 0.0,
                "value_display": value_display,
                "unit": str(biomarker.get("unit") or ""),
                "status": str(biomarker.get("status_type") or ""),
                "has_optimal_range": int(optimal_low is not None),
                "optimal_low": optimal_low if optimal_low is not None else 0.0,
                "optimal_high": optimal_high if optimal_high is not None else 0.0,
                "range_bands": bands,
                "range_meter_fraction": _float_or_none((biomarker.get("range_meter") or {}).get("indicator_percent"))
                or 0.0,
                **education,
                "raw_json": dict(biomarker),
                "synced_at": synced_at,
                "sync_version": sync_version_from_datetime(synced_at),
            }
        )
    return rows


# ----------------------------------------------------------------- coach --


@dataclass(frozen=True)
class CoachConversationRef:
    conversation_id: str
    title: str
    started_on: date
    started_at: datetime
    #: 'second' when the list title carried a clock time ("NEW CHAT 07:00:45"),
    #: 'day' when only the day header did ("What's the ideal bedtime for me?").
    started_at_precision: str


def coach_conversation_refs(
    pages: Iterable[Any], *, today: date, offset: timedelta
) -> list[CoachConversationRef]:
    """Every conversation in the chat history, dated from its day header.

    Messages carry no timestamp at all, so the conversation's date comes from
    the list: a day header ("TODAY", "TUE, OCTOBER 6" -- no year) and, for an
    untitled chat, a clock time in the title. The list is newest first, so a
    header's year is the latest one that does not put the day in the future
    and agrees with the weekday the header names.
    """

    refs: list[CoachConversationRef] = []
    seen: set[str] = set()
    tz = timezone(offset)
    # Every page is the history from its offset to the END, not one slice of
    # it (measured 2026-10-11: page one already held all 190 conversations,
    # page two the last 177), so a conversation appears on many pages and the
    # first sighting wins.
    for page in pages:
        for group in (page or {}).get("chat_history") or []:
            day = _coach_header_day(str(group.get("title") or ""), today)
            for conversation in group.get("conversations") or []:
                conversation_id = str(conversation.get("id") or "")
                if not conversation_id or conversation_id in seen:
                    continue
                seen.add(conversation_id)
                title = str(conversation.get("title") or "")
                clock = re.search(r"(\d{1,2}):(\d{2}):(\d{2})\s*$", title)
                if day is None:
                    started_on, started_at, precision = EPOCH_DATE, EPOCH_UTC, ""
                elif clock:
                    local = datetime.combine(
                        day, time(int(clock.group(1)), int(clock.group(2)), int(clock.group(3))), tzinfo=tz
                    )
                    started_on, started_at, precision = day, local.astimezone(UTC), "second"
                else:
                    started_on = day
                    started_at = datetime.combine(day, time(0, 0), tzinfo=tz).astimezone(UTC)
                    precision = "day"
                refs.append(
                    CoachConversationRef(
                        conversation_id=conversation_id,
                        title=title,
                        started_on=started_on,
                        started_at=started_at,
                        started_at_precision=precision,
                    )
                )
    return refs


def _coach_header_day(header: str, today: date) -> date | None:
    text = header.strip().upper()
    if text == "TODAY":
        return today
    if text == "YESTERDAY":
        return today - timedelta(days=1)
    match = re.fullmatch(r"([A-Z]{3}),\s*([A-Z]+)\s+(\d{1,2})", text)
    if not match:
        return None
    weekday, month_name, day_number = match.groups()
    try:
        month = datetime.strptime(month_name.title(), "%B").month
    except ValueError:
        return None
    for year in range(today.year, today.year - 8, -1):
        try:
            candidate = date(year, month, int(day_number))
        except ValueError:
            continue
        if candidate > today:
            continue
        if candidate.strftime("%a").upper() == weekday:
            return candidate
    return None


def coach_message_rows(
    *, account: str, ref: CoachConversationRef, payload: Any, synced_at: datetime
) -> list[dict[str, Any]]:
    messages = (payload or {}).get("messages")
    if not isinstance(messages, list):
        raise HealthspanShapeError(f"coach conversation {ref.conversation_id} has no messages list")
    rows = []
    for index, message in enumerate(messages):
        if not isinstance(message, Mapping):
            continue
        message_id = str(message.get("id") or f"{ref.conversation_id}:{index}")
        rows.append(
            {
                "account": account,
                "conversation_id": ref.conversation_id,
                "message_id": message_id,
                "message_index": index,
                "turn_id": str(message.get("turn_id") or ""),
                "role": str(message.get("role") or ""),
                "agent": str(message.get("agent") or ""),
                "content": str(message.get("content") or ""),
                "visible_to_user": int(bool(message.get("visible_to_user"))),
                "message_status": str(message.get("message_status") or ""),
                "conversation_title": ref.title,
                "started_on": ref.started_on,
                "conversation_started_at": ref.started_at,
                "started_at_precision": ref.started_at_precision,
                "raw_json": dict(message),
                "synced_at": synced_at,
                "sync_version": sync_version_from_datetime(synced_at),
            }
        )
    return rows


# ----------------------------------------------------------------- utils --


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_instant(value: Any) -> datetime:
    text = str(value or "").strip()
    if not text:
        return EPOCH_UTC
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return EPOCH_UTC
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
