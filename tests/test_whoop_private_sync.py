"""Tests for the WHOOP private-API sync runner.

The private API is undocumented and its worst traps are silent: a heart-rate
step the server rejects returns an empty series, a ``during`` range cast instead
of parsed raises nothing until Postgres sees it, and a dead browser session can
turn a */15 schedule into ~96 red runs a day. Each test below pins one of
those.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.whoop_private_api import (
    WhoopPrivateAuthError,
    WhoopPrivateClient,
    WhoopPrivateRateLimitedError,
    WhoopPrivateSession,
)
from personal_data_warehouse.whoop_private_sync import (
    WHOOP_PRIVATE_COLLECTIONS,
    WHOOP_PRIVATE_HEART_RATE_STEP_SECONDS,
    WHOOP_PRIVATE_STATUS_ACTION_REQUIRED,
    WHOOP_PRIVATE_STATUS_RATE_LIMITED,
    WHOOP_PRIVATE_WORKOUT_HEART_RATE_STEP_SECONDS,
    WhoopPrivateActionRequiredError,
    WhoopPrivateSyncRunner,
    cycle_to_row,
    document_to_row,
    heart_rate_samples_to_rows,
    journal_entries_to_rows,
    parse_pg_range,
    plan_windows,
    public_whoop_private_sync_summary,
    recovery_to_row,
    session_from_row,
    sleep_event_rows,
    sleep_to_row,
    sports_to_rows,
    whoop_private_credential_sha256,
    whoop_private_reauthorization_skip_reason,
    workout_heart_rate_samples_to_rows,
    workout_to_row,
)

NOW = datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC)
ACCOUNT = "zach@example.com"
EPOCH_UTC = datetime.fromtimestamp(0, tz=UTC)


class NullLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass


class RecordingLogger(NullLogger):
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, message, *args, **kwargs):
        self.warnings.append(str(message) % args if args else str(message))


class FakeWhoopPrivateWarehouse:
    """The warehouse surface `whoop_private` sync writes through."""

    def __init__(
        self,
        *,
        state=None,
        session=None,
        stored_documents=None,
        earliest_cycle_day=None,
    ) -> None:
        self.state = dict(state or {})
        self.session_row = session
        self.ensure_called = False
        self.cycles: list[dict] = []
        self.sleeps: list[dict] = []
        self.recoveries: list[dict] = []
        self.workouts: list[dict] = []
        self.sleep_events: list[dict] = []
        self.heart_rate_samples: list[dict] = []
        self.workout_heart_rate_samples: list[dict] = []
        self.journal_entries: list[dict] = []
        self.sports: list[dict] = []
        self.documents: list[dict] = []
        self.document_batches: list[list[dict]] = []
        # Documents already in the warehouse from earlier runs: the backfill
        # cursor IS the table, so this is what makes a resumed run resume.
        self.stored_documents: list[dict] = list(stored_documents or [])
        self.earliest_cycle_day = earliest_cycle_day
        self.state_rows: list[dict] = []
        self.rotations: list[dict] = []

    def ensure_whoop_private_tables(self) -> None:
        self.ensure_called = True

    def load_whoop_private_sync_state(self):
        return self.state

    def load_whoop_private_session(self, *, account):
        return self.session_row

    def rotate_whoop_private_session(
        self,
        *,
        account,
        expected_refresh_token,
        access_token,
        refresh_token,
        access_expires_at,
        refresh_expires_at,
        updated_at,
    ) -> None:
        self.rotations.append(
            {
                "account": account,
                "expected_refresh_token": expected_refresh_token,
                "access_token": access_token,
                "refresh_token": refresh_token,
                "access_expires_at": access_expires_at,
                "refresh_expires_at": refresh_expires_at,
                "updated_at": updated_at,
            }
        )

    def insert_whoop_private_cycles(self, rows):
        self.cycles.extend(rows)

    def insert_whoop_private_sleeps(self, rows):
        self.sleeps.extend(rows)

    def insert_whoop_private_recoveries(self, rows):
        self.recoveries.extend(rows)

    def insert_whoop_private_workouts(self, rows):
        self.workouts.extend(rows)

    def insert_whoop_private_sleep_events(self, rows):
        self.sleep_events.extend(rows)

    def insert_whoop_private_heart_rate_samples(self, rows):
        self.heart_rate_samples.extend(rows)

    def insert_whoop_private_workout_heart_rate_samples(self, rows):
        self.workout_heart_rate_samples.extend(rows)

    def insert_whoop_private_journal_entries(self, rows):
        self.journal_entries.extend(rows)

    def insert_whoop_private_sports(self, rows):
        self.sports.extend(rows)

    def insert_whoop_private_documents(self, rows):
        self.documents.extend(rows)
        self.document_batches.append(list(rows))

    def whoop_private_document_keys(self, *, account, kinds):
        return {
            (str(row["kind"]), str(row["doc_key"]))
            for row in self.stored_documents
            if row["account"] == account and row["kind"] in set(kinds)
        }

    def whoop_private_earliest_cycle_day(self, *, account):
        return self.earliest_cycle_day

    def insert_whoop_private_sync_state(self, **row):
        self.state_rows.append(row)


CYCLE_RECORD = {
    "cycle": {
        "id": 1234,
        "user_id": 7654321,
        "during": "['2026-08-22T11:30:00.000Z','2026-08-23T11:00:00.000Z')",
        "days": "['2026-08-22','2026-08-23')",
        "day_strain": 12.5,
        "scaled_strain": 11.0,
        "day_kilojoules": 9000.5,
        "day_avg_heart_rate": 70,
        "day_max_heart_rate": 150,
        "intensity_score": 1.25,
        "sleep_need": 28800.0,
        "predicted_end": "2026-08-23T11:30:00.000Z",
        "data_state": "complete",
        "timezone_offset": "-04:00",
        "created_at": "2026-08-22T11:31:00.000Z",
        "updated_at": "2026-08-23T10:00:00.000Z",
    },
    "recovery": {
        "activity_id": "sleep-abc",
        "recovery_score": 66,
        "resting_heart_rate": 52,
        # SECONDS in the private API; the public API's hrv_rmssd_milli is 1000x.
        "hrv_rmssd": 0.0731,
        "skin_temp_celsius": 33.4,
        "spo2": 96.5,
        "calibrating": False,
        "prob_covid": 0.02,
        "hr_baseline": 51,
        "hrv_component": 0.5,
        "rhr_component": 0.4,
        "recovery_rate": 1.0,
        "state": "COMPLETE",
        "algo_version": "8.0",
        "history_size": 60,
        "survey_response_id": "survey-1",
        "created_at": "2026-08-23T11:05:00.000Z",
        "updated_at": "2026-08-23T11:06:00.000Z",
    },
    "sleeps": [
        {
            "activity_id": "sleep-abc",
            "during": "['2026-08-23T03:12:00.000Z','2026-08-23T11:00:00.000Z')",
            "optimal_sleep_times": "['2026-08-23T03:00:00.000Z','2026-08-23T11:30:00.000Z')",
            "is_nap": False,
            "score": 92,
            "state": "COMPLETE",
            "latency": 600,
            "arousal_time": 1200,
            "total_wake_events": 4,
            "in_sleep_efficiency": 0.94,
            "debt_pre": 1800,
            "debt_post": 900,
            "habitual_sleep_need": 28000,
            "credit_from_naps": 0,
            "need_from_strain": 1200,
            "quality_duration": 26000,
            "light_sleep_duration": 13000,
            "slow_wave_sleep_duration": 6000,
            "rem_sleep_duration": 7000,
            "wake_duration": 1200,
            "no_data_duration": 0,
            "time_in_bed": 28200,
            "disturbances": 9,
            "cycles_count": 5,
            "respiratory_rate": 15.9,
            "sleep_consistency": 0.8,
            "projected_score": 90,
            "projected_sleep": 27000,
            "algo_version": "8.0",
            "survey_response_id": "survey-2",
            "timezone_offset": "-04:00",
            "created_at": "2026-08-23T11:01:00.000Z",
            "updated_at": "2026-08-23T11:02:00.000Z",
        }
    ],
    "workouts": [
        {
            "activity_id": "workout-xyz",
            "sport_id": 1,
            "during": "['2026-08-22T18:00:00.000Z','2026-08-22T19:00:00.000Z')",
            "score": 8.4,
            "intensity_score": 8.4,
            "raw_intensity_score": 8.9,
            "cumulative_workout_intensity": 9.1,
            "kilojoules": 2500.5,
            "average_heart_rate": 132,
            "max_heart_rate": 171,
            "percent_recorded": 100.0,
            "total_steps": 5400,
            "msk_score": 7.0,
            "zone_durations": {"zone_one_milli": 600000},
            "zone_durations_v2": {"zone_one_milli": 500000},
            "gps_data": {"points": []},
            "source": "STRAP",
            "survey_response_id": "",
            "timezone_offset": "-04:00",
        }
    ],
    "v2_activities": [],
}


class FakeWhoopPrivateClient:
    """Replays payloads and records exactly which endpoint got which window."""

    def __init__(self, *, fail_error=None, fail_on=()) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.fail_error = fail_error
        self.fail_on = set(fail_on)
        self.session = WhoopPrivateSession(
            account=ACCOUNT,
            access_token="access-1",
            refresh_token="refresh-1",
            access_expires_at=NOW + timedelta(hours=12),
            refresh_expires_at=NOW + timedelta(days=25),
        )

    def _record(self, endpoint: str, **params):
        self.calls.append((endpoint, params))
        if self.fail_error is not None and (not self.fail_on or endpoint in self.fail_on):
            raise self.fail_error

    def bootstrap(self):
        self._record("bootstrap")
        from personal_data_warehouse.whoop_private_api import WhoopPrivateIdentity

        return WhoopPrivateIdentity(
            user_id="7654321",
            timezone_offset="-04:00",
            raw={"profile": {"user_id": 7654321, "timezone_offset": "-04:00"}},
        )

    def cycles_details(self, *, user_id, start, end, limit):
        self._record("cycles_details", user_id=user_id, start=start, end=end, limit=limit)
        return {"records": [CYCLE_RECORD]}

    def sleep_events(self, *, activity_id):
        self._record("sleep_events", activity_id=activity_id)
        return [
            {"during": "['2026-08-23T03:12:00.000Z','2026-08-23T04:00:00.000Z')", "type": "LIGHT"},
            {"during": "['2026-08-23T04:00:00.000Z','2026-08-23T04:45:00.000Z')", "type": "SWS"},
        ]

    def heart_rate(self, *, user_id, start, end, step):
        self._record("heart_rate", user_id=user_id, start=start, end=end, step=step)
        return {
            "name": "heart_rate",
            "start": int(start.timestamp() * 1000),
            "values": [
                {"time": int(start.timestamp() * 1000), "data": 61},
                {"time": int(start.timestamp() * 1000) + step * 1000, "data": 63},
            ],
        }

    def sports_catalog(self, *, country_code="US"):
        self._record("sports_catalog", country_code=country_code)
        return [
            {
                "id": 1,
                "name": "Running",
                "category": "CARDIO",
                "has_gps": True,
                "has_survey": False,
                "activity_type_internal_name": "running",
                "is_current": True,
            }
        ]

    def journal_entries(self, *, day):
        self._record("journal_entries", day=day)
        return {
            "entries": [
                {
                    "question_id": 17,
                    "question_text": "Did you drink any alcohol?",
                    "answer": "false",
                    "behavior_id": 9,
                }
            ]
        }

    def trend(self, *, metric, end_date):
        self._record("trend", metric=metric, end_date=end_date)
        return {"header_name_display": metric, "education_carousel": [], "series": [1, 2, 3]}

    def stress(self, *, day):
        self._record("stress", day=day)
        return {"design_items": [], "stress_score": 1.4}

    def cardio_details(self, *, activity_id):
        self._record("cardio_details", activity_id=activity_id)
        return {"map": {"polyline": "abc"}}

    def sleep_deep_dive(self, *, day):
        self._record("sleep_deep_dive", day=day)
        return {"onboarding_overlays": [], "sleep": {}}

    def strain_deep_dive(self, *, day):
        self._record("strain_deep_dive", day=day)
        return {
            "header": {"deep_dive_score_type": "STRAIN"},
            "sections": [
                {
                    "items": [
                        {
                            "type": "SCORE_GAUGE",
                            "content": {
                                "id": "STRAIN_SCORE_GAUGE",
                                "score_display": "15.3",
                                "gauge_fill_percentage": 0.7280952188951144,
                                "score_target": 0.7285798037142287,
                                "lower_optimal_percentage": 0.6333417084761335,
                                "higher_optimal_percentage": 0.8860181885714286,
                            },
                        }
                    ]
                }
            ],
        }

    def behavior_impact(self, *, day):
        self._record("behavior_impact", day=day)
        return {"impact_summary_card": {"content": {"title": "BEHAVIOR INSIGHTS", "items": []}}}

    def health_tab(self):
        self._record("health_tab")
        return {"sections": [{"items": [{"type": "WHOOP_AGE_AMOEBA", "content": {"style_values": {"age": 28.2}}}]}]}


def _session_row(**overrides):
    row = {
        "account": ACCOUNT,
        "session_key": "default",
        "access_token": "access-1",
        "refresh_token": "refresh-1",
        "access_expires_at": NOW + timedelta(hours=12),
        "refresh_expires_at": NOW + timedelta(days=25),
    }
    row.update(overrides)
    return row


def _settings(monkeypatch, **env):
    defaults = {
        "WHOOP_PRIVATE_ACCOUNT": ACCOUNT,
        "WHOOP_PRIVATE_INCREMENTAL_LOOKBACK_DAYS": "1",
        "WHOOP_PRIVATE_BACKFILL_WINDOW_DAYS": "7",
        "WHOOP_PRIVATE_FULL_SYNC_START": "2026-08-01T00:00:00Z",
        "WHOOP_PRIVATE_HEART_RATE_CHUNK_HOURS": "6",
        "WHOOP_PRIVATE_HEART_RATE_CHUNKS_PER_RUN": "2",
        "WHOOP_PRIVATE_HEART_RATE_RECENT_HOURS": "6",
        "WHOOP_PRIVATE_JOURNAL_DAYS_PER_RUN": "2",
        "WHOOP_PRIVATE_DOCUMENTS_LOOKBACK_DAYS": "1",
    }
    defaults.update(env)
    for name, value in defaults.items():
        monkeypatch.setenv(name, value)
    return load_settings(require_postgres=False, require_gmail=False, require_whoop_private=True)


def _run(monkeypatch, *, warehouse, client=None, logger=None, now=NOW, **env):
    settings = _settings(monkeypatch, **env)
    return WhoopPrivateSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logger or NullLogger(),
        client_factory=lambda _config, _session: client or FakeWhoopPrivateClient(),
        now=lambda: now,
    ).sync_all()


# ---------------------------------------------------------------- config ----


def test_load_settings_accepts_whoop_private_config(monkeypatch) -> None:
    settings = _settings(monkeypatch)

    assert settings.whoop_private is not None
    assert settings.whoop_private.account == ACCOUNT
    assert settings.whoop_private.enabled is True
    assert settings.whoop_private.backfill_window_days == 7
    assert settings.whoop_private.base_url == "https://api.prod.whoop.com"


def test_whoop_private_falls_back_to_the_public_whoop_account(monkeypatch) -> None:
    for name in ("WHOOP_PRIVATE_ACCOUNT",):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("WHOOP_ACCOUNT", "shared@example.com")

    settings = load_settings(require_postgres=False, require_gmail=False, require_whoop_private=True)

    assert settings.whoop_private is not None
    assert settings.whoop_private.account == "shared@example.com"


def test_load_settings_rejects_an_unusable_backfill_window(monkeypatch) -> None:
    monkeypatch.setenv("WHOOP_PRIVATE_ACCOUNT", ACCOUNT)
    monkeypatch.setenv("WHOOP_PRIVATE_BACKFILL_WINDOW_DAYS", "0")

    with pytest.raises(ValueError, match="WHOOP_PRIVATE_BACKFILL_WINDOW_DAYS"):
        load_settings(require_postgres=False, require_gmail=False, require_whoop_private=True)


# --------------------------------------------------------------- mappers ----


def test_pg_ranges_are_parsed_not_cast() -> None:
    """`during` is PostgreSQL range notation, not a timestamp."""
    start, end = parse_pg_range("['2026-08-22T18:00:00.000Z','2026-08-22T19:00:00.000Z')")

    assert start == datetime(2026, 8, 22, 18, tzinfo=UTC)
    assert end == datetime(2026, 8, 22, 19, tzinfo=UTC)
    # An absent range is the warehouse epoch sentinel, never NULL.
    assert parse_pg_range("") == (EPOCH_UTC, EPOCH_UTC)
    assert parse_pg_range("[,)") == (EPOCH_UTC, EPOCH_UTC)


def test_cycle_row_parses_the_range_and_keeps_the_raw_payload() -> None:
    row = cycle_to_row(account=ACCOUNT, payload=CYCLE_RECORD["cycle"], synced_at=NOW)

    assert row["account"] == ACCOUNT
    assert row["cycle_id"] == "1234"
    assert row["start_at"] == datetime(2026, 8, 22, 11, 30, tzinfo=UTC)
    assert row["end_at"] == datetime(2026, 8, 23, 11, 0, tzinfo=UTC)
    assert row["day_strain"] == 12.5
    # `days` is a DATE range and lands in DATE columns, not timestamps.
    assert row["day_start"] == date(2026, 8, 22)
    assert row["day_end"] == date(2026, 8, 23)
    assert row["data_state"] == "complete"
    assert row["raw_json"]["scaled_strain"] == 11.0
    assert row["synced_at"] == NOW


def test_recovery_row_stores_hrv_in_both_units() -> None:
    """Private hrv_rmssd is SECONDS; the public API's is milliseconds.

    Mixing them is a 1000x error, so the row carries the source unit and the
    derived one explicitly rather than leaving a bare `hrv_rmssd`.
    """
    row = recovery_to_row(account=ACCOUNT, payload=CYCLE_RECORD["recovery"], synced_at=NOW)

    assert row["hrv_rmssd_seconds"] == 0.0731
    assert row["hrv_rmssd_milli"] == pytest.approx(73.1)
    # Booleans are bigint 0/1 in this warehouse, never bool.
    assert row["calibrating"] == 0
    assert row["activity_id"] == "sleep-abc"


def test_sleep_and_workout_rows_parse_every_range_column() -> None:
    sleep = sleep_to_row(account=ACCOUNT, payload=CYCLE_RECORD["sleeps"][0], synced_at=NOW)
    workout = workout_to_row(account=ACCOUNT, payload=CYCLE_RECORD["workouts"][0], synced_at=NOW)

    assert sleep["activity_id"] == "sleep-abc"
    assert sleep["start_at"] == datetime(2026, 8, 23, 3, 12, tzinfo=UTC)
    assert sleep["optimal_sleep_start"] == datetime(2026, 8, 23, 3, 0, tzinfo=UTC)
    assert sleep["is_nap"] == 0
    assert workout["activity_id"] == "workout-xyz"
    assert workout["start_at"] == datetime(2026, 8, 22, 18, tzinfo=UTC)
    assert workout["end_at"] == datetime(2026, 8, 22, 19, tzinfo=UTC)
    assert workout["zone_durations_json"] == {"zone_one_milli": 600000}
    assert workout["gps_data_json"] == {"points": []}


def test_sleep_event_rows_index_the_hypnogram_in_order() -> None:
    rows = sleep_event_rows(
        account=ACCOUNT,
        activity_id="sleep-abc",
        payload=[
            {"during": "['2026-08-23T03:12:00.000Z','2026-08-23T04:00:00.000Z')", "type": "LIGHT"},
            {"during": "['2026-08-23T04:00:00.000Z','2026-08-23T04:45:00.000Z')", "type": "SWS"},
        ],
        synced_at=NOW,
    )

    assert [row["event_index"] for row in rows] == [0, 1]
    assert [row["stage"] for row in rows] == ["LIGHT", "SWS"]
    assert rows[0]["started_at"] == datetime(2026, 8, 23, 3, 12, tzinfo=UTC)
    assert rows[1]["ended_at"] == datetime(2026, 8, 23, 4, 45, tzinfo=UTC)


def test_heart_rate_rows_convert_millisecond_epochs_and_record_the_grain() -> None:
    payload = {
        "name": "heart_rate",
        "values": [{"time": 1_787_000_000_000, "data": 61}, {"time": 1_787_000_060_000, "data": 0}],
    }

    rows = heart_rate_samples_to_rows(account=ACCOUNT, payload=payload, step_seconds=60, synced_at=NOW)

    assert rows[0]["sample_at"] == datetime.fromtimestamp(1_787_000_000, tz=UTC)
    assert rows[0]["heart_rate"] == 61
    assert rows[0]["step_seconds"] == 60
    # A zero reading is a real gap marker, not a row to invent.
    assert len(rows) == 1

    workout_rows = workout_heart_rate_samples_to_rows(
        account=ACCOUNT, activity_id="workout-xyz", payload=payload, synced_at=NOW
    )
    assert workout_rows[0]["activity_id"] == "workout-xyz"
    assert workout_rows[0]["heart_rate"] == 61


def test_journal_and_sport_rows_carry_their_keys() -> None:
    journal = journal_entries_to_rows(
        account=ACCOUNT,
        day="2026-08-23",
        payload={
            "entries": [
                {"question_id": 17, "question_text": "Alcohol?", "answer": "false", "behavior_id": 9}
            ]
        },
        synced_at=NOW,
    )
    sports = sports_to_rows(
        account=ACCOUNT,
        payload=[{"id": 1, "name": "Running", "category": "CARDIO", "has_gps": True}],
        synced_at=NOW,
    )

    assert journal[0]["day"] == date(2026, 8, 23)
    assert journal[0]["question_id"] == "17"
    assert journal[0]["answer"] == "false"
    assert journal[0]["behavior_id"] == "9"
    assert sports[0]["sport_id"] == "1"
    assert sports[0]["has_gps"] == 1


def test_documents_keep_tier_two_payloads_raw() -> None:
    row = document_to_row(
        account=ACCOUNT,
        kind="trend",
        doc_key="VO2_MAX",
        payload={"education_carousel": [], "series": [1]},
        collected_at=NOW,
        synced_at=NOW,
    )

    assert row["kind"] == "trend"
    assert row["doc_key"] == "VO2_MAX"
    assert row["raw_json"] == {"education_carousel": [], "series": [1]}
    # A UI payload gets no typed columns; that is the whole point of documents.
    assert set(row) == {"account", "kind", "doc_key", "collected_at", "raw_json", "synced_at", "sync_version"}


# --------------------------------------------------------------- windows ----


def test_backfill_is_bounded_newest_first_and_resumable() -> None:
    floor = datetime(2026, 8, 1, tzinfo=UTC)

    first = plan_windows(
        cursor=EPOCH_UTC,
        now=NOW,
        floor=floor,
        backfill_span=timedelta(days=7),
        lookback=timedelta(days=1),
    )
    assert first.sync_type == "full"
    assert first.windows[0].end == NOW
    assert first.windows[0].start == NOW - timedelta(days=7)
    assert first.next_cursor == NOW - timedelta(days=7)

    second = plan_windows(
        cursor=first.next_cursor,
        now=NOW,
        floor=floor,
        backfill_span=timedelta(days=7),
        lookback=timedelta(days=1),
    )
    # Newest first: the recency window leads, the older backfill slice follows.
    assert [window.sync_type for window in second.windows] == ["incremental", "backfill"]
    assert second.windows[0] == second.windows[0].__class__(NOW - timedelta(days=1), NOW, "incremental")
    assert second.windows[1].end == NOW - timedelta(days=7)
    assert second.windows[1].start == NOW - timedelta(days=14)
    assert second.next_cursor == NOW - timedelta(days=14)
    assert second.backfill_complete is False

    # The floor is never crossed: the last slice stops exactly on it.
    last = plan_windows(
        cursor=floor + timedelta(days=1),
        now=NOW,
        floor=floor,
        backfill_span=timedelta(days=7),
        lookback=timedelta(days=1),
    )
    assert last.windows[-1].start == floor
    assert last.next_cursor == floor

    done = plan_windows(
        cursor=floor,
        now=NOW,
        floor=floor,
        backfill_span=timedelta(days=7),
        lookback=timedelta(days=1),
    )
    assert done.backfill_complete is True
    assert [window.sync_type for window in done.windows] == ["incremental"]
    assert done.next_cursor == floor


# ---------------------------------------------------------------- runner ----


def test_sync_writes_every_collection_and_records_a_watermark_per_collection(monkeypatch) -> None:
    warehouse = FakeWhoopPrivateWarehouse(session=_session_row())
    client = FakeWhoopPrivateClient()

    summaries = _run(monkeypatch, warehouse=warehouse, client=client)

    assert warehouse.ensure_called
    assert warehouse.cycles and warehouse.sleeps and warehouse.recoveries and warehouse.workouts
    assert warehouse.sleep_events and warehouse.heart_rate_samples
    assert warehouse.workout_heart_rate_samples and warehouse.journal_entries
    assert warehouse.sports and warehouse.documents
    assert {row["collection"] for row in warehouse.state_rows} == set(WHOOP_PRIVATE_COLLECTIONS)
    assert all(row["status"] == "ok" for row in warehouse.state_rows)
    assert summaries[0].records_written > 0
    assert public_whoop_private_sync_summary(summaries[0])["has_token"] is False


def test_minute_grain_runs_continuously_and_six_second_grain_only_inside_workouts(monkeypatch) -> None:
    """metrics-service accepts only 6/60/600, and 6s over a whole day is
    600 points an hour. Six-second detail is therefore workout-scoped."""
    warehouse = FakeWhoopPrivateWarehouse(session=_session_row())
    client = FakeWhoopPrivateClient()

    _run(monkeypatch, warehouse=warehouse, client=client)

    heart_rate_calls = [params for endpoint, params in client.calls if endpoint == "heart_rate"]
    continuous = [call for call in heart_rate_calls if call["step"] == WHOOP_PRIVATE_HEART_RATE_STEP_SECONDS]
    workout_scoped = [
        call for call in heart_rate_calls if call["step"] == WHOOP_PRIVATE_WORKOUT_HEART_RATE_STEP_SECONDS
    ]

    assert WHOOP_PRIVATE_HEART_RATE_STEP_SECONDS == 60
    assert WHOOP_PRIVATE_WORKOUT_HEART_RATE_STEP_SECONDS == 6
    assert continuous, "minute-grain heart rate must be collected continuously"
    assert len(workout_scoped) == 1
    # Exactly the workout's `during` bounds, parsed from the range notation.
    assert workout_scoped[0]["start"] == datetime(2026, 8, 22, 18, tzinfo=UTC)
    assert workout_scoped[0]["end"] == datetime(2026, 8, 22, 19, tzinfo=UTC)
    assert warehouse.workout_heart_rate_samples[0]["activity_id"] == "workout-xyz"


def test_tier_two_endpoints_land_in_documents_with_kind_and_key(monkeypatch) -> None:
    warehouse = FakeWhoopPrivateWarehouse(session=_session_row())
    client = FakeWhoopPrivateClient()

    _run(monkeypatch, warehouse=warehouse, client=client)

    kinds = {(row["kind"], row["doc_key"]) for row in warehouse.documents}
    assert ("trend", "VO2_MAX") in kinds
    assert ("trend", "STRESS_DURING_SLEEP") in kinds
    assert ("cardio_details", "workout-xyz") in kinds
    assert any(kind == "stress" for kind, _key in kinds)
    assert any(kind == "sleep_deep_dive" for kind, _key in kinds)
    # Trends carry a UI template; none of it becomes a column.
    trend = next(row for row in warehouse.documents if row["kind"] == "trend")
    assert "header_name_display" in trend["raw_json"]


def test_a_second_run_resumes_the_backfill_from_the_stored_watermark(monkeypatch) -> None:
    cursor = NOW - timedelta(days=7)
    state = {
        (ACCOUNT, "cycles"): {
            "watermark_updated_at": cursor,
            "last_sync_type": "backfill",
            "status": "ok",
            "updated_at": cursor,
        }
    }
    warehouse = FakeWhoopPrivateWarehouse(state=state, session=_session_row())
    client = FakeWhoopPrivateClient()

    _run(monkeypatch, warehouse=warehouse, client=client)

    cycle_calls = [params for endpoint, params in client.calls if endpoint == "cycles_details"]
    assert any(call["end"] == cursor for call in cycle_calls), "the older slice resumes at the watermark"
    cycles_state = next(row for row in warehouse.state_rows if row["collection"] == "cycles")
    assert cycles_state["watermark_updated_at"] == NOW - timedelta(days=14)


def test_a_rejected_session_records_action_required_once_and_is_then_skippable(monkeypatch) -> None:
    warehouse = FakeWhoopPrivateWarehouse(session=_session_row())
    logger = RecordingLogger()
    client = FakeWhoopPrivateClient(fail_error=WhoopPrivateAuthError("session rejected"))

    with pytest.raises(WhoopPrivateActionRequiredError, match="publish-session"):
        _run(monkeypatch, warehouse=warehouse, client=client, logger=logger)

    assert {row["collection"] for row in warehouse.state_rows} == set(WHOOP_PRIVATE_COLLECTIONS)
    assert {row["status"] for row in warehouse.state_rows} == {WHOOP_PRIVATE_STATUS_ACTION_REQUIRED}
    fingerprint = whoop_private_credential_sha256("refresh-1")
    assert {row["credential_sha256"] for row in warehouse.state_rows} == {fingerprint}
    assert any("publish-session" in warning for warning in logger.warnings)

    # The recorded state is what stops ~96 red runs a day, and it clears itself
    # the moment a different session is published.
    recorded = {
        (row["account"], row["collection"]): row for row in warehouse.state_rows
    }
    assert whoop_private_reauthorization_skip_reason(
        recorded, account=ACCOUNT, refresh_token="refresh-1"
    ) is not None
    assert whoop_private_reauthorization_skip_reason(
        recorded, account=ACCOUNT, refresh_token="a-freshly-published-token"
    ) is None


def test_rate_limiting_ends_the_run_cleanly_instead_of_hammering(monkeypatch) -> None:
    warehouse = FakeWhoopPrivateWarehouse(session=_session_row())
    client = FakeWhoopPrivateClient(fail_error=WhoopPrivateRateLimitedError(retry_after=42))

    summaries = _run(monkeypatch, warehouse=warehouse, client=client)

    assert summaries[0].rate_limited is True
    statuses = {row["status"] for row in warehouse.state_rows}
    assert WHOOP_PRIVATE_STATUS_RATE_LIMITED in statuses
    assert WHOOP_PRIVATE_STATUS_ACTION_REQUIRED not in statuses


def test_an_unexpected_failure_is_recorded_and_fails_the_run(monkeypatch) -> None:
    warehouse = FakeWhoopPrivateWarehouse(session=_session_row())
    client = FakeWhoopPrivateClient(fail_error=RuntimeError("boom refresh-1"), fail_on={"sports_catalog"})

    with pytest.raises(RuntimeError, match="WHOOP private sync failed"):
        _run(monkeypatch, warehouse=warehouse, client=client)

    failed = next(row for row in warehouse.state_rows if row["collection"] == "sports")
    assert failed["status"] == "failed"
    # The session's tokens must never reach a stored error string.
    assert "refresh-1" not in failed["error"]


def test_a_never_published_session_asks_for_setup_without_a_red_run(monkeypatch) -> None:
    warehouse = FakeWhoopPrivateWarehouse(session=None)
    logger = RecordingLogger()

    summaries = _run(monkeypatch, warehouse=warehouse, logger=logger)

    assert summaries[0].skipped_reason
    assert "publish-session" in summaries[0].skipped_reason
    assert {row["status"] for row in warehouse.state_rows} == {WHOOP_PRIVATE_STATUS_ACTION_REQUIRED}


def test_an_expired_refresh_window_is_action_required_not_a_doomed_refresh(monkeypatch) -> None:
    warehouse = FakeWhoopPrivateWarehouse(
        session=_session_row(refresh_expires_at=NOW - timedelta(days=1))
    )
    client = FakeWhoopPrivateClient()

    with pytest.raises(WhoopPrivateActionRequiredError, match="publish-session"):
        _run(monkeypatch, warehouse=warehouse, client=client)

    assert client.calls == []
    assert {row["status"] for row in warehouse.state_rows} == {WHOOP_PRIVATE_STATUS_ACTION_REQUIRED}


def test_session_rotation_is_persisted_so_the_thirty_day_window_slides(monkeypatch) -> None:
    """Every refresh mints a new refresh token; persisting it is what keeps the
    source hands-off forever."""

    class FakeHttp:
        def __init__(self):
            self.calls = []

        def request(self, method, url, **kwargs):
            self.calls.append((method, url, kwargs))

            class Response:
                status_code = 200

                @staticmethod
                def json():
                    return {
                        "access_token": "access-2",
                        "access_token_expires_in": 86_400,
                        "refresh_token": "refresh-2",
                        "refresh_token_expires_in": 2_592_000,
                    }

            return Response()

    warehouse = FakeWhoopPrivateWarehouse(session=_session_row())
    settings = _settings(monkeypatch)
    runner = WhoopPrivateSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        now=lambda: NOW,
        http_factory=lambda: FakeHttp(),
    )
    session = session_from_row(_session_row(), account=ACCOUNT)

    client = runner._build_client(settings.whoop_private, session)
    assert isinstance(client, WhoopPrivateClient)
    client.refresh()

    assert warehouse.rotations[0]["expected_refresh_token"] == "refresh-1"
    assert warehouse.rotations[0]["refresh_token"] == "refresh-2"
    assert warehouse.rotations[0]["access_token"] == "access-2"
    assert warehouse.rotations[0]["refresh_expires_at"] == NOW + timedelta(seconds=2_592_000)

    # A second rotation compares against the token it actually refreshed from.
    client.refresh()
    assert warehouse.rotations[1]["expected_refresh_token"] == "refresh-2"


def test_a_disabled_source_does_not_call_the_api(monkeypatch) -> None:
    warehouse = FakeWhoopPrivateWarehouse(session=_session_row())
    client = FakeWhoopPrivateClient()

    summaries = _run(
        monkeypatch,
        warehouse=warehouse,
        client=client,
        WHOOP_PRIVATE_ENABLED="0",
    )

    assert client.calls == []
    assert summaries[0].skipped_reason


def test_every_row_mapper_fills_exactly_its_warehouse_column_tuple() -> None:
    """The mappers and `postgres.py` must agree column for column.

    ``_insert_rows`` reads ``row[column]`` for every declared column, so a
    missing key is a KeyError at write time and an extra key is silently
    dropped -- which is how a typo becomes a permanently empty column.
    """
    from personal_data_warehouse import schema

    synced_at = NOW
    rows_by_columns = [
        (schema.WHOOP_PRIVATE_CYCLE_COLUMNS, cycle_to_row(account=ACCOUNT, payload=CYCLE_RECORD["cycle"], synced_at=synced_at)),
        (schema.WHOOP_PRIVATE_SLEEP_COLUMNS, sleep_to_row(account=ACCOUNT, payload=CYCLE_RECORD["sleeps"][0], synced_at=synced_at)),
        (schema.WHOOP_PRIVATE_RECOVERY_COLUMNS, recovery_to_row(account=ACCOUNT, payload=CYCLE_RECORD["recovery"], synced_at=synced_at)),
        (schema.WHOOP_PRIVATE_WORKOUT_COLUMNS, workout_to_row(account=ACCOUNT, payload=CYCLE_RECORD["workouts"][0], synced_at=synced_at)),
        (
            schema.WHOOP_PRIVATE_SLEEP_EVENT_COLUMNS,
            sleep_event_rows(
                account=ACCOUNT,
                activity_id="sleep-abc",
                payload=[{"during": "['2026-08-23T03:12:00.000Z','2026-08-23T04:00:00.000Z')", "type": "LIGHT"}],
                synced_at=synced_at,
            )[0],
        ),
        (
            schema.WHOOP_PRIVATE_HEART_RATE_SAMPLE_COLUMNS,
            heart_rate_samples_to_rows(
                account=ACCOUNT,
                payload={"values": [{"time": 1_787_000_000_000, "data": 61}]},
                step_seconds=60,
                synced_at=synced_at,
            )[0],
        ),
        (
            schema.WHOOP_PRIVATE_WORKOUT_HEART_RATE_SAMPLE_COLUMNS,
            workout_heart_rate_samples_to_rows(
                account=ACCOUNT,
                activity_id="workout-xyz",
                payload={"values": [{"time": 1_787_000_000_000, "data": 61}]},
                synced_at=synced_at,
            )[0],
        ),
        (
            schema.WHOOP_PRIVATE_JOURNAL_ENTRY_COLUMNS,
            journal_entries_to_rows(
                account=ACCOUNT,
                day="2026-08-23",
                payload={"entries": [{"question_id": 17, "answer": "false"}]},
                synced_at=synced_at,
            )[0],
        ),
        (
            schema.WHOOP_PRIVATE_SPORT_COLUMNS,
            sports_to_rows(account=ACCOUNT, payload=[{"id": 1, "name": "Running"}], synced_at=synced_at)[0],
        ),
        (
            schema.WHOOP_PRIVATE_DOCUMENT_COLUMNS,
            document_to_row(
                account=ACCOUNT,
                kind="trend",
                doc_key="VO2_MAX",
                payload={},
                collected_at=synced_at,
                synced_at=synced_at,
            ),
        ),
    ]

    for columns, row in rows_by_columns:
        assert set(row) == set(columns), f"row/column drift: {set(row) ^ set(columns)}"


# ------------------------------------------------- strain coach & friends ----


def test_the_strain_coach_target_lands_in_documents(monkeypatch) -> None:
    """The Strain Coach target is the one strain fact no table can reconstruct.

    It is a Tier-2 UI payload, so it stays raw_json under its own kind rather
    than becoming a typed column that goes quietly null on the next restyle.
    """
    warehouse = FakeWhoopPrivateWarehouse(session=_session_row())
    client = FakeWhoopPrivateClient()

    _run(monkeypatch, warehouse=warehouse, client=client)

    strain = [row for row in warehouse.documents if row["kind"] == "strain_deep_dive"]
    assert strain, "the strain deep dive carries score_target / lower_ / higher_optimal_percentage"
    assert strain[0]["doc_key"] == "2026-08-23"
    gauge = strain[0]["raw_json"]["sections"][0]["items"][0]["content"]
    assert gauge["score_target"] == pytest.approx(0.7285798037142287)


def test_behavior_insights_and_the_health_tab_land_in_documents(monkeypatch) -> None:
    warehouse = FakeWhoopPrivateWarehouse(session=_session_row())
    client = FakeWhoopPrivateClient()

    _run(monkeypatch, warehouse=warehouse, client=client)

    kinds = {(row["kind"], row["doc_key"]) for row in warehouse.documents}
    # Behavior insights are per-day: WHOOP's own attribution of journal
    # behaviors to the day's recovery, which nothing else in the warehouse holds.
    assert ("behavior_impact", "2026-08-23") in kinds
    # The health tab is account-state, not a day, so it gets a fixed key.
    assert ("health_tab", "current") in kinds


def test_whoop_age_and_pace_of_aging_are_collected_trends(monkeypatch) -> None:
    """WHOOP Age has no other home; the trend carries its whole history."""
    warehouse = FakeWhoopPrivateWarehouse(session=_session_row())
    client = FakeWhoopPrivateClient()

    _run(monkeypatch, warehouse=warehouse, client=client)

    trends = {row["doc_key"] for row in warehouse.documents if row["kind"] == "trend"}
    assert {"WHOOP_AGE", "PACE_OF_AGING"} <= trends


# ------------------------------------------------------ document backfill ----


def test_day_documents_backfill_towards_the_first_cycle(monkeypatch) -> None:
    """Historic days are walked backwards, bounded per run.

    The floor is the account's first cycle, not `full_sync_start`: a member has
    no deep dives before they had a WHOOP, and walking to 2015 would spend
    thousands of requests re-confirming that.
    """
    warehouse = FakeWhoopPrivateWarehouse(
        session=_session_row(),
        earliest_cycle_day=date(2026, 8, 18),
    )
    client = FakeWhoopPrivateClient()

    _run(
        monkeypatch,
        warehouse=warehouse,
        client=client,
        WHOOP_PRIVATE_DOCUMENTS_BACKFILL_DAYS_PER_RUN="2",
    )

    days = sorted({row["doc_key"] for row in warehouse.documents if row["kind"] == "strain_deep_dive"})
    # The lookback day (today) plus two backfilled days, newest first.
    assert days == ["2026-08-21", "2026-08-22", "2026-08-23"]


def test_a_backfilled_day_is_never_fetched_twice(monkeypatch) -> None:
    """The documents table is the cursor, so a resumed run skips what it has."""
    stored = [
        {"account": ACCOUNT, "kind": kind, "doc_key": day}
        for kind in ("strain_deep_dive", "behavior_impact", "stress", "sleep_deep_dive")
        for day in ("2026-08-22", "2026-08-21")
    ]
    warehouse = FakeWhoopPrivateWarehouse(
        session=_session_row(),
        stored_documents=stored,
        earliest_cycle_day=date(2026, 8, 18),
    )
    client = FakeWhoopPrivateClient()

    _run(
        monkeypatch,
        warehouse=warehouse,
        client=client,
        WHOOP_PRIVATE_DOCUMENTS_BACKFILL_DAYS_PER_RUN="2",
    )

    fetched = sorted(params["day"] for endpoint, params in client.calls if endpoint == "strain_deep_dive")
    # Today is always refreshed (an in-progress day changes); 08-22 and 08-21
    # are already stored, so the budget moves on to the next two older days.
    assert fetched == ["2026-08-19", "2026-08-20", "2026-08-23"]


def test_the_backfill_stops_at_the_first_cycle_and_does_not_run_forever(monkeypatch) -> None:
    stored = [
        {"account": ACCOUNT, "kind": kind, "doc_key": f"2026-08-{day:02d}"}
        for kind in ("strain_deep_dive", "behavior_impact", "stress", "sleep_deep_dive")
        for day in range(18, 23)
    ]
    warehouse = FakeWhoopPrivateWarehouse(
        session=_session_row(),
        stored_documents=stored,
        earliest_cycle_day=date(2026, 8, 18),
    )
    client = FakeWhoopPrivateClient()

    _run(
        monkeypatch,
        warehouse=warehouse,
        client=client,
        WHOOP_PRIVATE_DOCUMENTS_BACKFILL_DAYS_PER_RUN="30",
    )

    fetched = sorted(params["day"] for endpoint, params in client.calls if endpoint == "strain_deep_dive")
    assert fetched == ["2026-08-23"], "nothing older than the first cycle should be requested"


def test_no_document_backfill_before_cycles_have_landed(monkeypatch) -> None:
    """Cycles sync first; with no cycle yet there is no floor, so do not guess."""
    warehouse = FakeWhoopPrivateWarehouse(session=_session_row(), earliest_cycle_day=None)
    client = FakeWhoopPrivateClient()

    _run(
        monkeypatch,
        warehouse=warehouse,
        client=client,
        WHOOP_PRIVATE_DOCUMENTS_BACKFILL_DAYS_PER_RUN="30",
    )

    days = sorted({row["doc_key"] for row in warehouse.documents if row["kind"] == "strain_deep_dive"})
    assert days == ["2026-08-23"]


def test_a_partially_stored_day_only_fetches_the_kinds_it_is_missing(monkeypatch) -> None:
    """`stress` and `sleep_deep_dive` predate the newer kinds, so historic days
    are half-filled. Re-fetching a stored 1.7 MB `stress` day would spend the
    run's budget restating it."""
    stored = [
        {"account": ACCOUNT, "kind": kind, "doc_key": "2026-08-22"}
        for kind in ("stress", "sleep_deep_dive")
    ]
    warehouse = FakeWhoopPrivateWarehouse(
        session=_session_row(),
        stored_documents=stored,
        earliest_cycle_day=date(2026, 8, 22),
    )
    client = FakeWhoopPrivateClient()

    _run(
        monkeypatch,
        warehouse=warehouse,
        client=client,
        WHOOP_PRIVATE_DOCUMENTS_BACKFILL_DAYS_PER_RUN="5",
    )

    day_kinds = {"stress", "sleep_deep_dive", "strain_deep_dive", "behavior_impact"}
    backfilled = {
        endpoint
        for endpoint, params in client.calls
        if endpoint in day_kinds and params.get("day") == "2026-08-22"
    }
    assert backfilled == {"strain_deep_dive", "behavior_impact"}


def test_historic_days_backfill_every_day_keyed_kind(monkeypatch) -> None:
    """All four kinds get history, megabytes included.

    `stress` and `sleep_deep_dive` are ~1.7 MB and ~935 KB per day against ~5 KB
    and 326 bytes for the other two, so this is a deliberate storage trade: an
    unpulled history is not recoverable later, and the per-run day budget is the
    dial for making the pull lighter.
    """
    warehouse = FakeWhoopPrivateWarehouse(
        session=_session_row(),
        earliest_cycle_day=date(2026, 8, 21),
    )
    client = FakeWhoopPrivateClient()

    _run(
        monkeypatch,
        warehouse=warehouse,
        client=client,
        WHOOP_PRIVATE_DOCUMENTS_BACKFILL_DAYS_PER_RUN="2",
    )

    historic = {
        (row["kind"], row["doc_key"])
        for row in warehouse.documents
        if row["doc_key"] in {"2026-08-21", "2026-08-22"}
    }
    assert historic == {
        (kind, day)
        for kind in ("stress", "sleep_deep_dive", "strain_deep_dive", "behavior_impact")
        for day in ("2026-08-21", "2026-08-22")
    }


def test_documents_are_flushed_per_day_not_accumulated(monkeypatch) -> None:
    """A 20-day run would otherwise hold tens of MB of UI payload in memory."""
    warehouse = FakeWhoopPrivateWarehouse(
        session=_session_row(),
        earliest_cycle_day=date(2026, 8, 21),
    )
    client = FakeWhoopPrivateClient()

    _run(
        monkeypatch,
        warehouse=warehouse,
        client=client,
        WHOOP_PRIVATE_DOCUMENTS_BACKFILL_DAYS_PER_RUN="2",
    )

    day_batches = [
        batch
        for batch in warehouse.document_batches
        if {row["doc_key"] for row in batch} <= {"2026-08-21", "2026-08-22", "2026-08-23"}
    ]
    assert len(day_batches) == 3, "one insert per day, not one insert for the whole run"
