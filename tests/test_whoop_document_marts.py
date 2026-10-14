"""marts_health views over WHOOP's raw documents, end to end.

Each payload below is trimmed from the live API's shape (2026-10-14) with
invented values: the views walk JSON paths, so a path that is wrong only shows
up as an empty or NULL column, and these tests are what catch it.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.whoop_private_sync import document_to_row
from tests.test_marts_layering import _dicts, warehouse  # noqa: F401 - fixture

ACCOUNT = "zach"
NOW = datetime(2026, 10, 14, 2, 0, tzinfo=UTC)

STRENGTH_LOG = {
    "activity_id": "lift-1",
    "weightlifting_workout_id": "wl-1",
    "name": "Lower Body Strength 30",
    "during": "['2026-07-29T23:08:00.319Z','2026-07-29T23:22:31.940Z')",
    "total_effective_volume_kg": 2000.0,
    "scaled_msk_strain_score": 3.8,
    "total_strain_score": 7.9,
    "workout_groups": [
        {
            "workout_exercises": [
                {
                    "sets": [
                        {
                            "weight_kg": 80.0,
                            "number_of_reps": 8,
                            "during": "['2026-07-29T23:08:12.879Z','2026-07-29T23:16:27.522Z')",
                            "msk_total_volume_kg": 640.0,
                            "time_in_seconds": None,
                            "complete": True,
                        },
                        {
                            "weight_kg": 85.0,
                            "number_of_reps": 6,
                            "during": "['2026-07-29T23:17:09.908Z','2026-07-29T23:18:12.824Z')",
                            "msk_total_volume_kg": 510.0,
                            "time_in_seconds": None,
                            "complete": False,
                        },
                    ],
                    "exercise_details": {
                        "exercise_id": "LEGPRESS_PULLEYMACHINE",
                        "name": "Leg Press",
                        "translated_muscle_groups": "Legs",
                        "translated_equipment": "Machine",
                        "translated_movement_pattern": "Squat",
                        "exercise_type": "STRENGTH",
                    },
                }
            ]
        }
    ],
}

STRAIN_DEEP_DIVE = {
    "sections": [
        {
            "items": [
                {
                    "type": "SCORE_GAUGE",
                    "content": {
                        "score_display": "8.0",
                        "gauge_fill_percentage": 0.38,
                        "score_target": 0.6,
                        "lower_optimal_percentage": 0.5,
                        "higher_optimal_percentage": 0.75,
                    },
                }
            ]
        }
    ]
}


def _tile(metric: str, value: str, unit: str, band: str, status: str = "POSITIVE_RANGE") -> dict:
    return {
        "type": "KEY_METRIC_TILE",
        "content": {
            "id": metric,
            "key_metric_tile_title_display": metric.title(),
            "key_metric_tile_stat_value_display": value,
            "key_metric_tile_suffix_display": unit,
            "key_metric_tile_trend_display": band,
            "key_metric_tile_trend_type": status,
        },
    }


HEALTH_MONITOR = {
    "title": "HEALTH MONITOR",
    "items": [
        _tile("RHR", "60", "bpm", "within 58 - 62"),
        _tile("SKIN_TEMPERATURE_FAHRENHEIT", "+0.5", "°F", "within -1.2 to +0.7"),
        _tile("HRV", "32", "ms", "low < 32", "CONCERN_RANGE"),
        {"type": "MENU_ITEM", "content": {}},
    ],
}


def _stat(display: str) -> dict:
    return {"stat_display": display}


STRESS = {
    "stress_state": "BALANCED",
    "gauge": {"gauge_score_display": "1.8", "gauge_subtext_display": "MEDIUM"},
    "stress_graph": {"stress_graph_vow_display": "47% of your time was medium."},
    "trend_list": [
        {
            "header": {"title_display": "TOTAL DAY"},
            "trend_stats": {"low": _stat("10:59"), "medium": _stat("11:08"), "high": _stat("1:36")},
        },
        {
            "header": {"title_display": "SLEEP"},
            "trend_stats": {"low": _stat("5:30"), "medium": _stat("1:00"), "high": _stat("0:15")},
        },
    ],
}


def _trend(points: list[tuple[str, str]], *, units: str | None = None, bars: bool = False) -> dict:
    details = [{"primary_contextual_display": label, "value_display": value} for label, value in points]
    if bars:
        plot = {"bar_groups": [{"bars": [{"data_scrubber_details": d}]} for d in details]}
    else:
        plot = {"segments": [{"points": [{"data_scrubber_details": d} for d in details]}]}
    return {
        "six_month_time_segment": {
            "metrics": [{"metric_units_display": units}],
            "graph": {"plots": [{"type": "BAR_PLOT" if bars else "LINE_PLOT", "plot": plot}]},
        }
    }


def _doc(kind: str, key: str, payload, *, collected_at: datetime = NOW) -> dict:
    return document_to_row(
        account=ACCOUNT, kind=kind, doc_key=key, payload=payload, collected_at=collected_at, synced_at=NOW
    )


def _seed(wh: PostgresWarehouse) -> None:
    wh.ensure_whoop_private_tables()
    wh.insert_whoop_private_documents(
        [
            _doc("strength_workout", "lift-1", STRENGTH_LOG),
            _doc("strength_workout", "walk-1", {}),
            _doc(
                "strength_prs",
                "2026-10-13",
                {"tiles": [{"exercise_id": "LEGPRESS", "name": "Leg Press", "volume_input_value": "180",
                            "volume_input_units": "lbs", "volume_input_format": "WEIGHT"}]},
            ),
            _doc("strength_prs", "2026-10-14", {"tiles": [{"exercise_id": "LEGPRESS", "name": "Leg Press",
                                                           "volume_input_value": "190", "volume_input_units": "lbs"}]}),
            _doc("strain_deep_dive", "2026-10-12", STRAIN_DEEP_DIVE),
            _doc("health_monitor", "2026-10-12", HEALTH_MONITOR),
            _doc("stress", "2026-10-12", STRESS),
            _doc("stress", "2025-10-23", {}),
            # A December point in a January snapshot belongs to the year before.
            _doc(
                "trend",
                "STEPS:2026-01",
                _trend([("TUE, DEC 30", "9,000"), ("WED, JAN 7", "10,000")], bars=True),
                collected_at=datetime(2026, 1, 7, 12, tzinfo=UTC),
            ),
            # A newer snapshot restates Jan 7 and wins.
            _doc(
                "trend",
                "STEPS:2026-02",
                _trend([("WED, JAN 7", "10,500"), ("TUE, FEB 3", "12,345")], bars=True),
                collected_at=datetime(2026, 2, 3, 12, tzinfo=UTC),
            ),
            _doc("trend", "STRESS_DURING_SLEEP:2026-10", _trend([("MON, OCT 12", "2:34")], units="hr")),
            _doc(
                "cardio_details",
                "run-1",
                {"map": {"gps_coordinates": [{"latitude": 40.0, "longitude": -75.0},
                                             {"latitude": 40.01, "longitude": -75.01}]}},
            ),
            _doc("cardio_details", "walk-1", {"map": None}),
        ]
    )


def test_strength_sets_are_one_row_per_set_with_the_exercise_and_workout(warehouse: PostgresWarehouse) -> None:
    _seed(warehouse)
    rows = _dicts(warehouse, "marts_health_strength_sets", order_by="set_index")

    assert len(rows) == 2  # the workout with no log contributes nothing
    first, second = rows
    assert first["workout_id"] == "lift-1"
    assert first["exercise_name"] == "Leg Press"
    assert first["muscle_groups"] == "Legs"
    assert first["reps"] == 8
    assert first["weight_kg"] == pytest.approx(80.0)
    assert first["weight_lb"] == pytest.approx(176.4)
    assert first["volume_kg"] == pytest.approx(640.0)
    assert first["set_start_at"] == datetime(2026, 7, 29, 23, 8, 12, 879000, tzinfo=UTC)
    assert first["workout_start_at"] == datetime(2026, 7, 29, 23, 8, 0, 319000, tzinfo=UTC)
    assert first["is_complete"] == 1 and second["is_complete"] == 0
    assert first["workout_muscular_strain"] == pytest.approx(3.8)


def test_strength_prs_mark_the_newest_snapshot(warehouse: PostgresWarehouse) -> None:
    _seed(warehouse)
    latest = [row for row in _dicts(warehouse, "marts_health_strength_prs") if row["is_latest"] == 1]

    assert [(row["snapshot_day"], row["pr_value"], row["pr_units"]) for row in latest] == [
        (date(2026, 10, 14), 190.0, "lbs")
    ]


def test_strain_targets_are_in_strain_units(warehouse: PostgresWarehouse) -> None:
    _seed(warehouse)
    (row,) = _dicts(warehouse, "marts_health_strain_targets")

    assert row["day"] == date(2026, 10, 12)
    assert row["strain"] == pytest.approx(8.0)
    assert row["target_strain"] == pytest.approx(12.6)
    assert (row["optimal_low"], row["optimal_high"]) == (pytest.approx(10.5), pytest.approx(15.75))


def test_health_monitor_parses_each_baseline_shape(warehouse: PostgresWarehouse) -> None:
    _seed(warehouse)
    rows = {row["metric"]: row for row in _dicts(warehouse, "marts_health_health_monitor")}

    assert set(rows) == {"RHR", "SKIN_TEMPERATURE_FAHRENHEIT", "HRV"}
    assert (rows["RHR"]["value"], rows["RHR"]["baseline_low"], rows["RHR"]["baseline_high"]) == (60, 58, 62)
    skin = rows["SKIN_TEMPERATURE_FAHRENHEIT"]
    assert (skin["value"], skin["baseline_low"], skin["baseline_high"]) == (0.5, -1.2, 0.7)
    hrv = rows["HRV"]
    assert hrv["baseline_low"] is None and hrv["baseline_value"] == 32
    assert hrv["baseline_relation"] == "low" and hrv["status"] == "CONCERN_RANGE"


def test_stress_days_read_hours_per_scope(warehouse: PostgresWarehouse) -> None:
    _seed(warehouse)
    rows = {row["scope"]: row for row in _dicts(warehouse, "marts_health_stress_days")}

    assert set(rows) == {"TOTAL DAY", "SLEEP"}  # the empty early day has no gauge
    total = rows["TOTAL DAY"]
    assert total["stress_score"] == pytest.approx(1.8)
    assert total["low_hours"] == pytest.approx(10 + 59 / 60)
    assert total["high_hours"] == pytest.approx(1.6)


def test_trend_points_infer_the_year_and_prefer_the_newest_snapshot(warehouse: PostgresWarehouse) -> None:
    _seed(warehouse)
    steps = {
        row["day"]: row["value"]
        for row in _dicts(warehouse, "marts_health_trend_points")
        if row["metric"] == "STEPS"
    }

    assert steps == {date(2025, 12, 30): 9000, date(2026, 1, 7): 10500, date(2026, 2, 3): 12345}
    (sleep,) = [r for r in _dicts(warehouse, "marts_health_trend_points") if r["metric"] == "STRESS_DURING_SLEEP"]
    assert sleep["value"] == pytest.approx(2 + 34 / 60) and sleep["unit"] == "hr"


def test_workout_routes_are_ordered_points(warehouse: PostgresWarehouse) -> None:
    _seed(warehouse)
    rows = _dicts(warehouse, "marts_health_workout_routes", order_by="point_index")

    assert [(r["workout_id"], r["point_index"], r["latitude"]) for r in rows] == [
        ("run-1", 1, 40.0),
        ("run-1", 2, 40.01),
    ]


def test_single_row_snapshots_are_rekeyed_by_day_once(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_whoop_private_tables()
    collected = datetime(2026, 10, 13, 20, tzinfo=UTC)
    warehouse.insert_whoop_private_documents(
        [
            _doc("health_tab", "current", {"v": 1}, collected_at=collected),
            _doc("weekly_plan", "current", {"v": 1}, collected_at=collected),
            _doc("achievements", "HEALTHSPAN", {"v": 1}, collected_at=collected),
            _doc("trend", "VO2_MAX", {"v": 1}, collected_at=collected),
            # Already on the new key: a stale old copy of the same day goes.
            _doc("trend", "STEPS:2026-10", {"v": 2}, collected_at=collected),
            _doc("trend", "STEPS", {"v": 1}, collected_at=collected),
            _doc("advanced_labs", "current", {"v": 1}, collected_at=collected),
        ]
    )

    warehouse.ensure_whoop_private_tables()
    warehouse.ensure_whoop_private_tables()

    keys = {
        (row[0], row[1]): row[2]
        for row in warehouse._query("SELECT kind, doc_key, raw_json FROM @whoop_private_documents")
    }
    assert keys == {
        ("health_tab", "2026-10-13"): {"v": 1},
        ("weekly_plan", "2026-10-13"): {"v": 1},
        ("achievements", "HEALTHSPAN:2026-10-13"): {"v": 1},
        ("trend", "VO2_MAX:2026-10"): {"v": 1},
        ("trend", "STEPS:2026-10"): {"v": 2},
        # The labs cursor is not a snapshot and keeps its key.
        ("advanced_labs", "current"): {"v": 1},
    }
