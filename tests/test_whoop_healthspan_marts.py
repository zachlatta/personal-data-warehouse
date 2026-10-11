"""marts_health.healthspan_* and lab_results, end to end from the parsers.

The rows are produced by the real parsers from API-shaped fixtures and read
back through the views, so a column the parser writes and the view forgets
(or translates under the wrong sign) fails here rather than in an answer.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.schema import (
    WHOOP_PRIVATE_HEALTHSPAN_CONTRIBUTOR_COLUMNS,
    WHOOP_PRIVATE_HEALTHSPAN_WEEK_COLUMNS,
    WHOOP_PRIVATE_LAB_RESULT_COLUMNS,
)
from personal_data_warehouse.whoop_private_healthspan import lab_result_rows, parse_healthspan
from tests import whoop_healthspan_fixtures as fixtures
from tests.test_marts_layering import _column_types, _dicts, _row, warehouse  # noqa: F401 - fixture

NOW = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)


def _seed(wh: PostgresWarehouse) -> None:
    wh.ensure_whoop_private_tables()
    for day, previous, age, pace in (
        ("2026-10-04", "2026-09-27", 31.0, 0.5),
        ("2026-10-11", "2026-10-04", 31.2, 0.9),
    ):
        week = parse_healthspan(
            account="zach",
            requested_day=day,
            payload=fixtures.healthspan_payload(
                week_range=f"Oct {int(day[-2:])} - Oct {int(day[-2:]) + 6}", previous=previous, age=age, pace=pace
            ),
            synced_at=NOW,
        )
        wh.insert_whoop_private_healthspan_weeks([week.week_row])
        wh.insert_whoop_private_healthspan_contributors(week.contributor_rows)
    details = {"potassium": fixtures.POTASSIUM_DETAIL, "estimated_glomerular_filtration_rate": fixtures.EGFR_DETAIL}
    for test_id, tested in (("old", "2025-06-01T14:00:00.000Z"), ("new", "2026-02-10T14:00:00.000Z")):
        wh.insert_whoop_private_lab_results(
            lab_result_rows(
                account="zach",
                test_id=test_id,
                test_record={**fixtures.LAB_TEST_RECORD, "test_date": tested},
                test_payload=fixtures.LAB_TEST_PAGE,
                biomarker_details=details,
                synced_at=NOW,
            )
        )


def test_healthspan_weeks_read_younger_as_positive_and_change_week_on_week(
    warehouse: PostgresWarehouse,
) -> None:
    _seed(warehouse)
    weeks = {row["week_start"]: row for row in _dicts(warehouse, "marts_health_healthspan_weeks")}

    current = weeks[date(2026, 10, 11)]
    # WHOOP stores -0.6 for "0.6 years younger"; the view says what it means.
    assert current["years_younger"] == pytest.approx(0.6)
    assert current["chronological_age"] == pytest.approx(31.8)
    assert current["whoop_age_change"] == pytest.approx(0.2)
    assert current["pace_of_aging_change"] == pytest.approx(0.4)
    assert weeks[date(2026, 10, 4)]["whoop_age_change"] is None


def test_contributors_expose_gap_to_younger_and_null_for_no_data(warehouse: PostgresWarehouse) -> None:
    _seed(warehouse)
    rows = {
        row["contributor_key"]: row
        for row in _dicts(warehouse, "marts_health_healthspan_contributors")
        if row["week_start"] == date(2026, 10, 11)
    }

    # Sleep consistency 61% against a bar that turns younger at 70%.
    assert rows["SLEEP_CONSISTENCY"]["gap_to_younger"] == pytest.approx(9)
    # Steps 9,250 is already past 8,000: nothing left to close.
    assert rows["STEPS"]["gap_to_younger"] == 0
    # RHR is lower-is-younger: 64 bpm against 60.
    assert rows["RHR"]["gap_to_younger"] == pytest.approx(4)
    assert rows["STRENGTH_TRAINING_TIME"]["impact_years"] == pytest.approx(0.9)
    # No lean-body-mass scan: no value, no impact, no range -- not zeros.
    lean = rows["BODY_COMPOSITION_LEAN"]
    for column in ("six_month_value", "thirty_day_value", "impact_years", "gap_to_younger", "range_min", "younger_threshold"):
        assert lean[column] is None, column


def test_lab_results_flag_the_latest_result_per_biomarker(warehouse: PostgresWarehouse) -> None:
    _seed(warehouse)
    rows = _dicts(warehouse, "marts_health_lab_results")

    latest = {(row["biomarker_id"], row["test_id"]) for row in rows if row["is_latest"] == 1}
    assert latest == {("potassium", "new"), ("estimated_glomerular_filtration_rate", "new")}
    egfr = next(r for r in rows if r["biomarker_id"] == "estimated_glomerular_filtration_rate" and r["test_id"] == "new")
    assert (egfr["optimal_low"], egfr["optimal_high"], egfr["status"]) == (95.0, 120.0, "OUT_OF_RANGE")
    assert egfr["tested_at"] == datetime(2026, 2, 10, 14, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("logical", "table", "columns", "key"),
    [
        ("marts_health_healthspan_weeks", "whoop_private_healthspan_weeks", WHOOP_PRIVATE_HEALTHSPAN_WEEK_COLUMNS, {}),
        (
            "marts_health_healthspan_contributors",
            "whoop_private_healthspan_contributors",
            WHOOP_PRIVATE_HEALTHSPAN_CONTRIBUTOR_COLUMNS,
            {"contributor_key": "X"},
        ),
        ("marts_health_lab_results", "whoop_private_lab_results", WHOOP_PRIVATE_LAB_RESULT_COLUMNS, {"test_id": "t", "biomarker_id": "b"}),
    ],
)
def test_healthspan_marts_translate_every_exposed_date_and_timestamp(
    warehouse: PostgresWarehouse, logical, table, columns, key
) -> None:
    """A row whose every date and timestamp is the epoch sentinel reads back NULL."""
    warehouse.ensure_whoop_private_tables()
    insert = getattr(warehouse, f"insert_{table}")
    insert([_row(columns, table=table, account="sentinel", sync_version=1, **key)])

    temporal = [
        column
        for column, data_type in _column_types(warehouse, logical)
        if data_type in ("timestamp with time zone", "date")
    ]
    assert temporal, logical
    row = next(row for row in _dicts(warehouse, logical) if row["account"] == "sentinel")
    for column in temporal:
        assert row[column] is None, f"{logical}.{column} still exposes the epoch sentinel"


def test_redating_a_coach_conversation_moves_only_rows_that_differ(warehouse: PostgresWarehouse) -> None:
    from datetime import timedelta

    from personal_data_warehouse.whoop_private_healthspan import coach_conversation_refs, coach_message_rows

    warehouse.ensure_whoop_private_tables()
    wrong = coach_conversation_refs([fixtures.COACH_PAGE_ONE], today=date(2026, 10, 11), offset=timedelta(0))
    for ref in wrong:
        warehouse.insert_whoop_private_coach_messages(
            coach_message_rows(account="zach", ref=ref, payload=fixtures.coach_conversation(ref.conversation_id), synced_at=NOW)
        )
    right = coach_conversation_refs([fixtures.COACH_PAGE_ONE], today=date(2026, 10, 11), offset=timedelta(hours=-4))
    later = datetime(2026, 10, 11, 13, 0, tzinfo=UTC)

    warehouse.redate_whoop_private_coach_conversations(account="zach", refs=right, synced_at=later)

    rows = warehouse._query(
        "SELECT conversation_id, conversation_started_at, synced_at FROM "
        + warehouse.sql_relation("whoop_private_coach_messages")
    )
    by_conversation = {row[0]: (row[1], row[2]) for row in rows}
    assert by_conversation["conv-today"] == (datetime(2026, 10, 11, 11, 0, 45, tzinfo=UTC), later)
    # A day-precision chat starts at local midnight, which also moved.
    assert by_conversation["conv-oct6"][0] == datetime(2026, 10, 6, 4, 0, tzinfo=UTC)

    warehouse.redate_whoop_private_coach_conversations(
        account="zach", refs=right, synced_at=datetime(2026, 10, 11, 14, 0, tzinfo=UTC)
    )
    unchanged = warehouse._query("SELECT DISTINCT synced_at FROM " + warehouse.sql_relation("whoop_private_coach_messages"))
    # Nothing differed the second time, so nothing was re-synced.
    assert unchanged == [(later,)]
