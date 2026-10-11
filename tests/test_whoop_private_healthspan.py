"""Healthspan, Advanced Labs and WHOOP Coach parsers.

Each test pins a trap that reads as "no data" rather than an error when it
goes wrong: '6:45' h parsed as 6.45 hours, a lower-is-better contributor
(RHR) whose target is read from the wrong end of its bar, a lean-body-mass
row with no scan stored as a real 0, a chat-history day header with no year
dated a year off.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from personal_data_warehouse.whoop_private_healthspan import (
    HealthspanShapeError,
    biomarker_range_bands,
    coach_conversation_refs,
    coach_message_rows,
    healthspan_biomarker_impacts,
    lab_result_rows,
    lab_test_biomarkers,
    lab_test_ids,
    parse_display_number,
    parse_healthspan,
)
from tests import whoop_healthspan_fixtures as fixtures

NOW = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("display", "expected"),
    [
        ("67%", 67.0),
        ("6:45", 6.75),
        ("0:17", 17 / 60),
        ("11,020", 11020.0),
        ("16K", 16000.0),
        ("0K", 0.0),
        ("40bpm", 40.0),
        ("+1.1", 1.1),
        ("-0.7", -0.7),
        ("--", None),
        ("", None),
    ],
)
def test_display_numbers_parse_in_the_contributors_own_unit(display, expected) -> None:
    if expected is None:
        assert parse_display_number(display) is None
    else:
        assert parse_display_number(display) == pytest.approx(expected, abs=1e-5)


def _week(**kwargs):
    return parse_healthspan(
        account="a", requested_day="2026-10-11", payload=fixtures.healthspan_payload(**kwargs), synced_at=NOW
    )


def test_a_week_carries_whoop_age_chronological_age_and_pace() -> None:
    week = _week(age=31.2, years_difference=-0.6, pace=0.9)

    assert week.week_start == date(2026, 10, 11)
    assert week.previous_week_start == date(2026, 10, 4)
    row = week.week_row
    assert row["week_end"] == date(2026, 10, 17)
    assert row["whoop_age"] == 31.2
    # WHOOP's sign is kept: negative is younger, so chronological = age - diff.
    assert row["years_difference"] == -0.6
    assert row["chronological_age"] == pytest.approx(31.8)
    assert row["pace_of_aging"] == 0.9
    assert row["previous_whoop_age"] == pytest.approx(31.0)


def test_every_contributor_is_typed_with_its_value_impact_and_guidance() -> None:
    rows = {row["contributor_key"]: row for row in _week().contributor_rows}

    assert list(rows) == [
        "SLEEP_CONSISTENCY",
        "SLEEP_HOURS",
        "STEPS",
        "STRENGTH_TRAINING_TIME",
        "RHR",
        "BODY_COMPOSITION_LEAN",
    ]
    sleep = rows["SLEEP_HOURS"]
    assert (sleep["pillar"], sleep["unit"]) == ("Sleep", "hours")
    assert sleep["six_month_value"] == pytest.approx(6.75)
    assert sleep["thirty_day_value"] == pytest.approx(7 + 5 / 60)
    assert rows["STEPS"]["six_month_value"] == 9250
    assert rows["STEPS"]["unit"] == "steps"
    assert rows["SLEEP_CONSISTENCY"]["unit"] == "percent"
    strength = rows["STRENGTH_TRAINING_TIME"]
    assert strength["impact_years"] == pytest.approx(0.9)
    assert strength["impact_direction"] == "OLDER"
    assert strength["guidance_title"] == "Time to Reassess"
    assert rows["STEPS"]["impact_years"] == pytest.approx(-0.3)


def test_the_younger_threshold_reads_the_bar_from_the_younger_end() -> None:
    rows = {row["contributor_key"]: row for row in _week().contributor_rows}

    # Ten divisions over 0-16K, YOUNGER from the sixth: more steps is younger.
    assert (rows["STEPS"]["younger_direction"], rows["STEPS"]["younger_threshold"]) == ("higher", 8000)
    # Sleep consistency's bar turns YOUNGER at the sixth of ten over 40-100%.
    assert rows["SLEEP_CONSISTENCY"]["younger_threshold"] == pytest.approx(70)
    # RHR is drawn the other way round: LOWER is younger.
    assert rows["RHR"]["younger_direction"] == "lower"
    assert rows["RHR"]["younger_threshold"] == pytest.approx(60)


def test_a_contributor_with_no_data_is_flagged_not_stored_as_a_reading() -> None:
    lean = {row["contributor_key"]: row for row in _week().contributor_rows}["BODY_COMPOSITION_LEAN"]

    assert lean["has_value"] == 0
    assert lean["is_enabled"] == 0
    assert lean["younger_direction"] == ""


def test_a_week_that_crosses_new_year_is_dated_in_both_years() -> None:
    week = parse_healthspan(
        account="a",
        requested_day="2025-12-29",
        payload=fixtures.healthspan_payload(week_range="Dec 29 - Jan 3", previous="2025-12-21"),
        synced_at=NOW,
    )

    assert week.week_row["week_start"] == date(2025, 12, 29)
    assert week.week_row["week_end"] == date(2026, 1, 3)


def test_a_restyled_screen_fails_loudly_instead_of_writing_an_empty_week() -> None:
    payload = fixtures.healthspan_payload()
    payload["unlocked_content"]["items"] = []

    with pytest.raises(HealthspanShapeError):
        parse_healthspan(account="a", requested_day="2026-10-11", payload=payload, synced_at=NOW)
    with pytest.raises(HealthspanShapeError):
        parse_healthspan(account="a", requested_day="2026-10-11", payload={"locked_content": {}}, synced_at=NOW)


def test_the_biomarkers_impacting_each_pillar_are_read() -> None:
    impacts = healthspan_biomarker_impacts(fixtures.healthspan_payload())

    assert {(i["pillar"], i["biomarker"]) for i in impacts} == {
        ("Sleep", "Potassium"),
        ("Strain", "Potassium"),
        ("Fitness", "Potassium"),
    }


def test_range_bands_come_from_the_detail_charts_axis_labels() -> None:
    assert biomarker_range_bands(fixtures.EGFR_DETAIL) == [
        {"low": 0.0, "high": 60.0, "status": "OUT_OF_RANGE"},
        {"low": 60.0, "high": 95.0, "status": "SUFFICIENT"},
        {"low": 95.0, "high": 120.0, "status": "OPTIMAL"},
        {"low": 120.0, "high": 150.0, "status": "OUT_OF_RANGE"},
    ]


def test_a_lab_test_is_one_row_per_biomarker_with_its_optimal_range() -> None:
    assert lab_test_ids(fixtures.LAB_TEST_HISTORY) == ["test-metabolic"]
    biomarkers = lab_test_biomarkers(fixtures.LAB_TEST_PAGE)
    details = {
        "potassium": fixtures.POTASSIUM_DETAIL,
        "estimated_glomerular_filtration_rate": fixtures.EGFR_DETAIL,
    }

    rows = {
        row["biomarker_id"]: row
        for row in lab_result_rows(
            account="a",
            test_id="test-metabolic",
            test_record=fixtures.LAB_TEST_RECORD,
            test_payload=fixtures.LAB_TEST_PAGE,
            biomarker_details=details,
            synced_at=NOW,
        )
    }

    assert len(biomarkers) == 2
    egfr = rows["estimated_glomerular_filtration_rate"]
    assert egfr["result_value"] == 131.0
    assert egfr["status"] == "OUT_OF_RANGE"
    assert (egfr["optimal_low"], egfr["optimal_high"], egfr["has_optimal_range"]) == (95.0, 120.0, 1)
    assert egfr["tested_at"] == datetime(2026, 2, 10, 14, 0, tzinfo=UTC)
    assert egfr["test_name"] == "Comprehensive Metabolic Panel"
    assert egfr["how_to_improve"] == "Synthetic advice."
    assert egfr["categories"] == "Heart Health, Fitness"
    assert (rows["potassium"]["optimal_low"], rows["potassium"]["optimal_high"]) == (3.8, 4.6)


def test_a_lab_test_page_with_no_biomarkers_fails_loudly() -> None:
    with pytest.raises(HealthspanShapeError):
        lab_result_rows(
            account="a",
            test_id="t",
            test_record=fixtures.LAB_TEST_RECORD,
            test_payload={"sections": []},
            biomarker_details={},
            synced_at=NOW,
        )


def test_coach_conversations_are_dated_from_their_day_headers() -> None:
    refs = coach_conversation_refs(
        [fixtures.COACH_PAGE_ONE, fixtures.COACH_PAGE_TWO],
        today=date(2026, 10, 11),
        offset=timedelta(hours=-4),
    )

    # Page two repeats page one's tail; the first sighting wins.
    assert [ref.conversation_id for ref in refs] == ["conv-today", "conv-oct6", "conv-2025"]
    today, oct6, older = refs
    assert today.started_at == datetime(2026, 10, 11, 11, 0, 45, tzinfo=UTC)
    assert today.started_at_precision == "second"
    assert (oct6.started_on, oct6.started_at_precision) == (date(2026, 10, 6), "day")
    # "SUN, NOVEMBER 2" has no year: 2026-11-02 is in the future, and only
    # 2025-11-02 was a Sunday.
    assert older.started_on == date(2025, 11, 2)


def test_coach_messages_keep_their_order_role_and_visibility() -> None:
    ref = coach_conversation_refs([fixtures.COACH_PAGE_ONE], today=date(2026, 10, 11), offset=timedelta(0))[0]

    rows = coach_message_rows(
        account="a", ref=ref, payload=fixtures.coach_conversation(ref.conversation_id), synced_at=NOW
    )

    assert [(r["message_index"], r["role"], r["visible_to_user"]) for r in rows] == [
        (0, "user", 1),
        (1, "tool", 0),
        (2, "assistant", 1),
    ]
    assert rows[0]["content"] == "Should I run today?"
    assert rows[0]["conversation_started_at"] == ref.started_at
