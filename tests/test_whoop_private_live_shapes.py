"""Mapper tests built from REAL private-API responses.

Every fixture below is the shape of an actual response captured on 2026-08-23,
trimmed but not restructured. They exist because the journal and sports mappers
were originally written from endpoint names alone, and both silently lost data
against the real payloads: the journal mapper returned zero rows for a day with
two logged behaviours, and the sports mapper dropped sport id 0 -- Running.

Neither failure would have raised. Both would have looked like "no data".
"""

from __future__ import annotations

from datetime import UTC, datetime

from personal_data_warehouse.whoop_private_sync import journal_entries_to_rows, sports_to_rows

NOW = datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC)

# The live journal payload nests entries two levels down and splits each one
# into the question (behavior_tracker) and the answer (tracker_input).
LIVE_JOURNAL = {
    "metadata": {},
    "journal": {
        "journal_entry_id": None,
        "user_id": 7654321,
        "cycle_id": 123456789,
        "notes": None,
        "tracked_behaviors": [
            {
                "behavior_tracker": {
                    "id": 28,
                    "title": "Read in Bed",
                    "question_text": "Read (non-screened device) while in bed?",
                    "category": "NIGHTTIME",
                    "question_type": "YES_NO",
                    "internal_name": "read-in-bed",
                },
                "tracker_input": {
                    "journal_entry_id": 1635358679,
                    "behavior_tracker_id": 28,
                    "answered_yes": False,
                    "magnitude_input_value": None,
                    "time_input_value": None,
                    "source": "USER",
                },
            },
            {
                "behavior_tracker": {
                    "id": 143,
                    "title": "Sunlight",
                    "question_text": "Spend time in natural sunlight?",
                    "category": "DAYTIME",
                    "question_type": "YES_NO",
                    "internal_name": "sunlight",
                },
                "tracker_input": {
                    "journal_entry_id": 1635358679,
                    "behavior_tracker_id": 143,
                    "answered_yes": True,
                    "magnitude_input_value": None,
                    "source": "USER",
                },
            },
        ],
    },
}


def test_journal_maps_the_real_nested_payload() -> None:
    rows = journal_entries_to_rows(account="a", day="2026-08-22", payload=LIVE_JOURNAL, synced_at=NOW)

    assert len(rows) == 2, "the real payload nests entries under journal.tracked_behaviors"
    by_id = {row["question_id"]: row for row in rows}
    assert by_id["28"]["question_text"] == "Read (non-screened device) while in bed?"
    assert by_id["28"]["answer"] == "false"
    assert by_id["143"]["answer"] == "true"
    assert by_id["143"]["behavior_id"] == "143"


def test_journal_prefers_a_magnitude_answer_over_the_yes_no_flag() -> None:
    """A magnitude behaviour's number is the answer; the yes/no flag is just its gate."""
    payload = {
        "journal": {
            "tracked_behaviors": [
                {
                    "behavior_tracker": {"id": 7, "question_text": "How many drinks?"},
                    "tracker_input": {"answered_yes": True, "magnitude_input_value": 3.0},
                }
            ]
        }
    }

    rows = journal_entries_to_rows(account="a", day="2026-08-22", payload=payload, synced_at=NOW)

    assert rows[0]["answer"] == "3.0"


def test_journal_keeps_an_unanswered_day_empty_rather_than_inventing_rows() -> None:
    payload = {"journal": {"tracked_behaviors": []}}

    assert journal_entries_to_rows(account="a", day="2026-08-23", payload=payload, synced_at=NOW) == []


LIVE_SPORTS = [
    {"id": 0, "name": "Running", "category": "cardiovascular", "has_gps": True, "has_survey": True, "is_current": True},
    {"id": -1, "name": "Activity", "category": "cardiovascular", "has_gps": True, "has_survey": True},
    {"id": 45, "name": "Weightlifting", "category": "strength", "has_gps": False},
]


def test_sports_keeps_sport_id_zero() -> None:
    """id 0 is Running -- the most common sport there is.

    `sport.get("id") or sport.get("sport_id")` treated 0 as absent and dropped
    the row, so every run would have failed to resolve its sport name.
    """
    rows = sports_to_rows(account="a", payload=LIVE_SPORTS, synced_at=NOW)

    assert len(rows) == 3
    assert {row["sport_id"] for row in rows} == {"0", "-1", "45"}
    running = next(row for row in rows if row["sport_id"] == "0")
    assert running["name"] == "Running"
