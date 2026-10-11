"""Healthspan, Advanced Labs and WHOOP Coach payloads, shaped like the live API.

Structure is copied from responses captured on 2026-10-11 and trimmed; every
NUMBER and every sentence is synthetic, because this repository is public and
the real ones are someone's health record. Keep the nesting faithful when
editing: the parsers walk it exactly, and a fixture that is easier to read
than the real payload is a fixture that hides the bug.
"""

from __future__ import annotations

from typing import Any


def _division(styles: list[str]) -> list[dict[str, str]]:
    return [{"style": style} for style in styles]


def _contributor(
    *,
    title: str,
    trend_key: str,
    left: str,
    right: str,
    divisions: list[str],
    six: str,
    thirty: str,
    unit: str | None,
    impact: str,
    impact_style: str,
    guidance_title: str,
    enabled: bool = True,
) -> dict[str, Any]:
    return {
        "title_display": title,
        "is_enabled": enabled,
        "content": {
            "range_divisions": _division(divisions),
            "range_left_label_display": left,
            "range_right_label_display": right,
            "top_range_value": {
                "value_display": six,
                "value_unit_display": unit,
                "value_range_percentage": 0.5,
                "style": "ENABLED",
                "expanded_value_label_display": "6 Month avg.",
            },
            "impact_value_display": impact,
            "impact_value_label_display": "years",
            "impact_value_style": impact_style,
            "expanded_bottom_range_value": {
                "value_display": thirty,
                "value_unit_display": unit,
                "value_range_percentage": 0.5,
                "style": "ENABLED",
                "expanded_value_label_display": "30 Day avg.",
            },
            "expanded_items": [
                {
                    "type": "HEALTHSPAN_VOW",
                    "content": {
                        "title_display": guidance_title,
                        "subtitle_display": f"Synthetic guidance for {title.lower()}.",
                        "cta_text_display": "VIEW TREND",
                        "destination": {
                            "screen": "TRENDS",
                            "parameters": {"trend_key": trend_key, "duration": 2, "date": "2026-10-11"},
                        },
                        "style": "INLINE",
                    },
                }
            ],
        },
        "is_open": False,
    }


OLDER_TO_YOUNGER = ["OLDER_4", "OLDER_4", "OLDER_3", "OLDER_2", "OLDER_1", "YOUNGER_1", "YOUNGER_2", "YOUNGER_3", "YOUNGER_4", "YOUNGER_4"]
YOUNGER_TO_OLDER = ["YOUNGER_4", "YOUNGER_4", "YOUNGER_3", "YOUNGER_2", "YOUNGER_1", "OLDER_1", "OLDER_2", "OLDER_3", "OLDER_4", "OLDER_4"]


def healthspan_payload(
    *,
    week_range: str = "Oct 11 - Oct 17",
    previous: str | None = "2026-10-04",
    age: float = 31.2,
    years_difference: float = -0.6,
    pace: float = 0.9,
) -> dict[str, Any]:
    contributors_sleep = [
        _contributor(
            title="SLEEP CONSISTENCY",
            trend_key="SLEEP_CONSISTENCY",
            left="40%",
            right="100%",
            divisions=["OLDER_4", "OLDER_4", "OLDER_4", "OLDER_4", "OLDER_2", "YOUNGER_1", "YOUNGER_3", "YOUNGER_4", "YOUNGER_4", "YOUNGER_4"],
            six="61%",
            thirty="64%",
            unit=None,
            impact="+0.5",
            impact_style="OLDER",
            guidance_title="Keep Pushing",
        ),
        _contributor(
            title="HOURS OF SLEEP",
            trend_key="SLEEP_HOURS",
            left="5h",
            right="8h",
            divisions=OLDER_TO_YOUNGER,
            six="6:45",
            thirty="7:05",
            unit="h",
            impact="0.0",
            impact_style="NEUTRAL",
            guidance_title="Shift Your Attention",
        ),
    ]
    contributors_strain = [
        _contributor(
            title="STEPS",
            trend_key="STEPS",
            left="0K",
            right="16K",
            divisions=OLDER_TO_YOUNGER,
            six="9,250",
            thirty="10,100",
            unit="Steps",
            impact="-0.3",
            impact_style="YOUNGER",
            guidance_title="Outperforming",
        ),
        _contributor(
            title="STRENGTH ACTIVITY TIME (WEEKLY)",
            trend_key="STRENGTH_TRAINING_TIME",
            left="0h",
            right="2h",
            divisions=OLDER_TO_YOUNGER,
            six="0:20",
            thirty="0:05",
            unit="h",
            impact="+0.9",
            impact_style="OLDER",
            guidance_title="Time to Reassess",
        ),
    ]
    contributors_fitness = [
        _contributor(
            title="RHR",
            trend_key="RHR",
            left="40bpm",
            right="80bpm",
            divisions=YOUNGER_TO_OLDER,
            six="64",
            thirty="62",
            unit="bpm",
            impact="+0.2",
            impact_style="OLDER",
            guidance_title="Time to Reassess",
        ),
        _contributor(
            title="LEAN BODY MASS",
            trend_key="BODY_COMPOSITION_LEAN",
            left="--",
            right="--",
            divisions=["DISABLED_4"] * 10,
            six="--",
            thirty="--",
            unit="xx",
            impact="--",
            impact_style="DISABLED",
            guidance_title="There is currently no data",
            enabled=False,
        ),
    ]

    def section(title: str, contributors: list[dict[str, Any]], pillar: str) -> dict[str, Any]:
        return {
            "type": "TILE_SECTION",
            "content": {
                "title_display": title,
                "primary_description_display": "6 Month avg.",
                "secondary_description_display": "30 Day avg.",
                "items": [
                    {"type": "CONTRIBUTORS_CONTAINER", "content": {"contributors": contributors}},
                    {
                        "type": "LABS_IMPACT_CARD",
                        "content": {
                            "title": f"BIOMARKERS IMPACTING {title.upper()}",
                            "trends": [{"title_display": "Potassium", "trend": "POSITIVE_RANGE"}],
                            "button": {
                                "text": "DIVE INTO THE IMPACT",
                                "action": {
                                    "screen": "GENERIC_SCREEN",
                                    "parameters": {
                                        "request": {
                                            "path": f"healthspan-service/v1/healthspan/bff/bloodwork-insights?healthspanPillar={pillar}",
                                            "method": "GET",
                                        }
                                    },
                                },
                            },
                            "category": pillar.lower(),
                        },
                    },
                ],
            },
        }

    return {
        "metadata": {"ai_context_metadata": {"screen_identifier": "HEALTHSPAN"}},
        "navigation_title": "HEALTHSPAN",
        "navigation_subtitle": "NEXT UPDATE IN 7 DAYS",
        "unlocked_content": {
            "date_picker": {"current_date_range_display": week_range, "previous_date_time": previous},
            "is_calibrating": False,
            "whoop_age_amoeba": {
                "style_values": {"age": age, "pace_of_aging": pace, "years_difference": years_difference},
                "age_value_display": f"{age:.1f}",
                "age_subtitle_display": f"{abs(years_difference):.1f} years younger",
                "years_difference_value_display": f"{abs(years_difference):.1f}",
                "years_difference_subtitle_display": "YEARS YOUNGER",
                "pace_of_aging_display": f"{pace:.1f}x",
                "is_calibrating": False,
            },
            "previous_whoop_age_amoeba": {
                "style_values": {"age": age - 0.2, "pace_of_aging": pace - 0.4, "years_difference": years_difference - 0.2},
                "age_value_display": f"{age - 0.2:.1f}",
                "pace_of_aging_display": f"{pace - 0.4:.1f}x",
            },
            "items": [
                {"type": "PACE_OF_AGING_METER", "content": {"title_display": "PACE OF AGING", "ticks": []}},
                section("Sleep", contributors_sleep, "SLEEP"),
                section("Strain", contributors_strain, "STRAIN"),
                section("Fitness", contributors_fitness, "FITNESS"),
            ],
        },
        "locked_content": None,
    }


LAB_TEST_HISTORY = {
    "sections": [
        {
            "id": "advanced_labs_test_history_2026_section",
            "items": [
                {
                    "type": "LABS_TEST_ITEM",
                    "content": {
                        "id": "test-metabolic",
                        "title": "Comprehensive Metabolic Panel (2)",
                        "subtitle": "Feb 10, 2026",
                        "destination": {
                            "screen": "ADVANCED_LABS_TEST_RESULTS",
                            "parameters": {"biomarker_test_id": "test-metabolic"},
                        },
                    },
                }
            ],
        },
        {"id": "have_other_tests_section", "items": [{"type": "EMPTY_TILE", "content": {"id": "have_other_tests_tile"}}]},
    ]
}

LAB_TEST_RECORD = {
    "id": "test-metabolic",
    "display_name": "Comprehensive Metabolic Panel",
    "test_date": "2026-02-10T14:00:00.000Z",
    "status": "COMPLETE",
    "test_source": "MANUAL_UPLOAD",
    "upload_source": "example_lab",
}


def _biomarker(biomarker_id: str, title: str, value: float, unit: str, status: str) -> dict[str, Any]:
    return {
        "id": biomarker_id,
        "title": title,
        "value": value,
        "unit": unit,
        "trend": {"title_display": status.title(), "trend": "POSITIVE_RANGE"},
        "status_type": status,
        "range_meter": {"sections": [], "indicator_percent": 0.42},
    }


LAB_TEST_PAGE = {
    "navigation_bar": {"navigation_title": "TEST RESULTS"},
    "sections": [
        {
            "id": "searchable_list_id",
            "items": [
                {
                    "type": "SEARCHABLE_BIOMARKERS_LIST",
                    "content": {
                        "biomarkers": [
                            _biomarker("potassium", "Potassium", 4.3, "mmol/L", "OPTIMAL"),
                            _biomarker("estimated_glomerular_filtration_rate", "eGFR", 131.0, "mL/min/1.73m2", "OUT_OF_RANGE"),
                        ]
                    },
                }
            ],
        }
    ],
}


def biomarker_detail(labels: list[tuple[str, str]]) -> dict[str, Any]:
    """A detail screen whose band chart has the given (label, style) y-axis."""
    return {
        "navigation_title": "BIOMARKER DETAILS",
        "header": {"title": "Synthetic", "pills": ["Heart Health", "Fitness"]},
        "sections": [
            {
                "id": "coach_entry_point",
                "items": [{"type": "WHOOP_COACH_VOW", "content": {"vow": "Synthetic coach summary."}}],
            },
            {
                "id": "biomarker_trend_segment",
                "items": [
                    {
                        "type": "BIOMARKER_TREND_SEGMENT",
                        "content": {
                            "metrics": {"supertitle": "LATEST", "metric_value": "1.0"},
                            "graph": {
                                "plane": {
                                    "start_yaxis": {
                                        "labels": [
                                            {"label": label, "position": i / 4, "style": style}
                                            for i, (label, style) in enumerate(labels)
                                        ]
                                    }
                                },
                                "plots": [],
                            },
                        },
                    }
                ],
            },
            {
                "id": "learn_more_carousel",
                "items": [
                    {
                        "type": "EXPANDABLE_LIST",
                        "content": {
                            "items": [
                                {"title_display": "What is Synthetic?", "description_display": "A synthetic marker."},
                                {"title_display": "Why it matters", "description_display": "Synthetic reasons."},
                                {"title_display": "How to improve", "description_display": "Synthetic advice."},
                            ]
                        },
                    }
                ],
            },
        ],
        "status": "OPTIMAL",
    }


EGFR_DETAIL = biomarker_detail(
    [
        ("0.0", "ACCENT_NEGATIVE_TREND"),
        ("60.0", "BIOMARKER_SUFFICIENT"),
        ("95.0", "ACCENT_POSITIVE_TREND"),
        ("120.0", "ACCENT_NEGATIVE_TREND"),
        ("150.0", "ACCENT_NEGATIVE_TREND"),
    ]
)
POTASSIUM_DETAIL = biomarker_detail(
    [("3.0", "ACCENT_NEGATIVE_TREND"), ("3.8", "ACCENT_POSITIVE_TREND"), ("4.6", "BIOMARKER_SUFFICIENT"), ("5.5", "ACCENT_NEGATIVE_TREND")]
)

COACH_PAGE_ONE = {
    "chat_history": [
        {"title": "TODAY", "conversations": [{"title": "NEW CHAT 07:00:45", "id": "conv-today"}]},
        {"title": "TUE, OCTOBER 6", "conversations": [{"title": "Is my run too much?", "id": "conv-oct6"}]},
        {"title": "SUN, NOVEMBER 2", "conversations": [{"title": "NEW CHAT 21:15:00", "id": "conv-2025"}]},
    ],
    "next_token": "page-2",
}
# Every page is the history from its offset to the END, so page two repeats.
COACH_PAGE_TWO = {
    "chat_history": [
        {"title": "SUN, NOVEMBER 2", "conversations": [{"title": "NEW CHAT 21:15:00", "id": "conv-2025"}]},
    ],
    "next_token": "page-3",
}


def coach_conversation(conversation_id: str) -> dict[str, Any]:
    def message(index: int, role: str, content: str, *, visible: bool = True) -> dict[str, Any]:
        return {
            "chat_session_id": conversation_id,
            "id": f"{conversation_id}-m{index}",
            "turn_id": f"{conversation_id}-t{index // 2}",
            "content": content,
            "message_status": "COMPLETE",
            "role": role,
            "agent": "coach-chat",
            "visible_to_user": visible,
        }

    return {
        "id": conversation_id,
        "messages": [
            message(0, "user", "Should I run today?"),
            message(1, "tool", "{\"recovery\": 40}", visible=False),
            message(2, "assistant", "Keep it easy today; your recovery is low."),
        ],
    }
