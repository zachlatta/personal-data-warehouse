"""Sync the WHOOP private (app) API into ``base_whoop_private``.

The public developer API (``base_whoop``) is summary-grain: one row per cycle,
sleep, recovery and workout, and no time series at all. This source adds what
``app.whoop.com`` itself sees -- the per-6-second heart rate inside a workout,
the minute-grain heart rate the rest of the day, the sleep hypnogram, the
journal, and the trend metrics that have no public endpoint. See
``docs/whoop-private-api.md`` for the reconnaissance behind every choice here.

Four things in this module look arbitrary and are not:

* **Six-second heart rate runs continuously, and there is only one grid.**
  ``metrics-service`` accepts only ``step`` 6, 60 or 600 (everything else is an
  HTTP 400), and it serves step 6 for ANY window, not only inside a workout --
  verified against the live API on 2026-08-26 at one, sixty and two hundred and
  forty days back. So the whole history is collected at 6s: 14,400 points a day,
  ~5.2M rows a year, and no second workout-scoped copy of the same readings.
  ``collection_signature`` in ``ops.whoop_private_sync_state`` is what made the
  switch a re-walk rather than a seam -- a changed grain restarts that
  collection's backfill, exactly as ``adapter_signature`` does for the timeline.
* **``during`` / ``days`` / ``optimal_sleep_times`` are PostgreSQL range
  literals**, not timestamps. They are parsed, never cast.
* **Tier-2 (BFF) payloads go to ``documents`` as raw JSON.** They are UI
  responses -- ``education_carousel``, ``design_items`` -- that WHOOP can
  restyle without notice. A typed column over one of those is a silent-breakage
  machine.
* **The refresh token rotates on every refresh.** Persisting the rotation is
  what slides the 30-day window forward and keeps the source hands-off; a
  rejected session is recorded as ``action_required`` and then skipped, because
  a */15 schedule turns one dead credential into ~96 red runs a day.

Nothing here logs, returns, or stores a token: the only credential identity
that leaves this module is a SHA-256 fingerprint.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
import argparse
import hashlib
import inspect
import json
import logging

from personal_data_warehouse.config import Settings, WhoopPrivateConfig, load_settings
from personal_data_warehouse.warehouse import warehouse_from_settings
from personal_data_warehouse.whoop_private_api import (
    WhoopPrivateApiError,
    WhoopPrivateAuthError,
    WhoopPrivateClient,
    WhoopPrivateIdentity,
    WhoopPrivateRateLimitedError,
    WhoopPrivateSession,
)
from personal_data_warehouse.whoop_sync import (
    EPOCH_UTC,
    parse_rfc3339,
    state_datetime,
    sync_version_from_datetime,
    truncate_error,
)

LOGGER = logging.getLogger(__name__)

#: DATE columns get their own absent-value sentinel; see postgres.DATE_COLUMNS.
EPOCH_DATE = date(1970, 1, 1)

#: ``ops.whoop_private_sync_state.collection`` values, in sync order. ``cycles``
#: runs first because it is what discovers the sleep activity ids and the
#: workout windows the next two collections need.
WHOOP_PRIVATE_COLLECTIONS = (
    "cycles",
    "sleep_events",
    "heart_rate",
    "journal",
    "sports",
    "documents",
)

WHOOP_PRIVATE_STATUS_OK = "ok"
WHOOP_PRIVATE_STATUS_FAILED = "failed"
WHOOP_PRIVATE_STATUS_ACTION_REQUIRED = "action_required"
#: Deliberately NOT one of pipeline_health's error statuses: being rate limited
#: is a clean early end to a run, not a failure to page on.
WHOOP_PRIVATE_STATUS_RATE_LIMITED = "rate_limited"

#: metrics-service accepts only 6/60/600, and serves 6 for any window. This is
#: the ONE grain the heart-rate series is stored at; changing it changes
#: `whoop_private_collection_signature` and re-walks every day of history.
WHOOP_PRIVATE_HEART_RATE_STEP_SECONDS = 6

PUBLISH_SESSION_HINT = (
    "run `pdw whoop publish-session` on the Mac whose Chrome holds an "
    "app.whoop.com login"
)


class WhoopPrivateActionRequiredError(WhoopPrivateApiError):
    """The captured browser session is dead; only a human can replace it."""


@dataclass(frozen=True)
class SyncWindow:
    start: datetime
    end: datetime
    sync_type: str


@dataclass(frozen=True)
class SyncPlan:
    """One collection's bounded slice of work for this run."""

    windows: tuple[SyncWindow, ...]
    next_cursor: datetime
    sync_type: str
    backfill_complete: bool


@dataclass
class WhoopPrivateSyncSummary:
    account: str
    sync_type: str
    records_written: int
    collections: dict[str, int] = field(default_factory=dict)
    rate_limited: bool = False
    action_required: bool = False
    skipped_reason: str = ""


def public_whoop_private_sync_summary(summary: WhoopPrivateSyncSummary) -> dict[str, Any]:
    return {
        "account": summary.account,
        "sync_type": summary.sync_type,
        "records_written": summary.records_written,
        "collections": dict(summary.collections),
        "rate_limited": summary.rate_limited,
        "action_required": summary.action_required,
        "skipped": summary.skipped_reason,
        # Explicit: a summary never carries session material.
        "has_token": False,
    }


# --------------------------------------------------------------- credential --


def whoop_private_credential_sha256(refresh_token: str) -> str:
    """A stable, non-secret identity for one captured session."""
    if not refresh_token:
        return ""
    return hashlib.sha256(refresh_token.encode("utf-8")).hexdigest()


def whoop_private_reauthorization_skip_reason(
    state_by_key: Mapping[tuple[str, str], Mapping[str, Any]],
    *,
    account: str,
    refresh_token: str,
) -> str | None:
    """Skip a permanently rejected session, but never a replacement session.

    Every private-API collection shares one browser session. Once all of them
    recorded ``action_required`` for the same credential, another */15 run can
    only repeat the same doomed refresh. The fingerprint makes the quiet period
    self-clearing: a newly published session differs and is tried on the very
    next tick.
    """
    fingerprint = whoop_private_credential_sha256(refresh_token)
    if not fingerprint:
        return None
    states = [state_by_key.get((account, collection)) for collection in WHOOP_PRIVATE_COLLECTIONS]
    if not all(
        state
        and str(state.get("status") or "") == WHOOP_PRIVATE_STATUS_ACTION_REQUIRED
        and str(state.get("credential_sha256") or "") == fingerprint
        for state in states
    ):
        return None
    return (
        "WHOOP private session was rejected; skipping repeated API calls for the "
        f"same dead credential. To repair it, {PUBLISH_SESSION_HINT}. Sync resumes "
        "automatically once a different session is published; /pipelines stays in "
        "attention until a successful sync clears this state."
    )


def session_from_row(row: Any, *, account: str) -> WhoopPrivateSession | None:
    """Adapt whatever ``load_whoop_private_session`` returns into a session."""
    if row is None:
        return None
    if isinstance(row, WhoopPrivateSession):
        return row
    if not isinstance(row, Mapping):
        return None
    access_token = _text(row.get("access_token"))
    refresh_token = _text(row.get("refresh_token"))
    if not refresh_token and not access_token:
        return None
    return WhoopPrivateSession(
        account=_text(row.get("account")) or account,
        access_token=access_token,
        refresh_token=refresh_token,
        access_expires_at=parse_rfc3339(row.get("access_expires_at")),
        refresh_expires_at=parse_rfc3339(row.get("refresh_expires_at")),
    )


# ------------------------------------------------------------------ windows --


def plan_windows(
    *,
    cursor: datetime,
    now: datetime,
    floor: datetime,
    backfill_span: timedelta,
    lookback: timedelta,
) -> SyncPlan:
    """Bounded, newest-first, resumable.

    ``cursor`` is the oldest instant this collection has already covered, so a
    run walks one ``backfill_span`` further back and stores where it stopped.
    The recency window is emitted alongside it (WHOOP rescores a night hours
    after it ends), and suppressed when the backfill slice already covers it.
    """
    started_empty = cursor <= EPOCH_UTC
    if started_empty or cursor > now:
        cursor = now
    windows: list[SyncWindow] = []
    next_cursor = cursor
    backfilling = cursor > floor
    if backfilling:
        start = max(floor, cursor - backfill_span)
        windows.append(SyncWindow(start, cursor, "backfill"))
        next_cursor = start

    recent_start = now - lookback
    if not any(window.start <= recent_start and window.end >= now for window in windows):
        windows.append(SyncWindow(recent_start, now, "incremental"))

    windows.sort(key=lambda window: window.end, reverse=True)
    sync_type = "full" if started_empty else ("backfill" if backfilling else "incremental")
    return SyncPlan(
        windows=tuple(windows),
        next_cursor=next_cursor,
        sync_type=sync_type,
        backfill_complete=not backfilling,
    )


def whoop_private_collection_signature(collection: str) -> str:
    """What a collection's stored rows depend on, beyond the window they cover.

    A cursor answers "how far back have we walked"; it cannot answer "walked
    asking WHAT". When the request that produced the rows changes -- the
    heart-rate grain is the case that exists -- resuming the cursor leaves every
    historic row at the old answer and nothing ever revisits them, because the
    walk has already reached its floor. Storing the signature beside the cursor
    turns that into an automatic re-walk on the next tick, with no manual
    force-full-sync and no operator who has to know. It is the same contract
    ``adapter_signature`` gives a timeline adapter.

    An empty string means the collection's rows depend on nothing but their
    window, which is true of every collection but this one.
    """
    if collection == "heart_rate":
        return f"step={WHOOP_PRIVATE_HEART_RATE_STEP_SECONDS}"
    return ""


def stored_signature(state: Mapping[str, Any] | None) -> str:
    """The signature a previous run recorded. Absent reads as empty.

    An existing database predates the column, so every row reads "" and every
    collection whose signature is non-empty re-walks exactly once. That is the
    intended migration, not an accident of the default.
    """
    return str((state or {}).get("collection_signature", "") or "")


def chunk_window(window: SyncWindow, span: timedelta) -> list[SyncWindow]:
    """Split a window into newest-first slices of at most ``span``."""
    if span <= timedelta(0) or window.end <= window.start:
        return [window]
    slices: list[SyncWindow] = []
    end = window.end
    while end > window.start:
        start = max(window.start, end - span)
        slices.append(SyncWindow(start, end, window.sync_type))
        end = start
    return slices


# ------------------------------------------------------------------ parsing --


def parse_pg_range(value: Any) -> tuple[datetime, datetime]:
    """Parse ``['start','end')`` -- range notation, not a timestamp.

    Absent bounds become the warehouse's epoch sentinel, never NULL.
    """
    text = _text(value).strip()
    if len(text) < 2:
        return (EPOCH_UTC, EPOCH_UTC)
    body = text[1:-1] if text[0] in "[(" and text[-1] in ")]" else text
    parts = [part.strip().strip('"').strip("'") for part in body.split(",")]
    if len(parts) < 2:
        return (EPOCH_UTC, EPOCH_UTC)
    return (parse_rfc3339(parts[0]), parse_rfc3339(parts[1]))


def parse_pg_date_range(value: Any) -> tuple[date, date]:
    """The DATE-range flavour of ``parse_pg_range`` (``days``).

    Day-granularity columns carry their own epoch sentinel, ``1970-01-01``, so
    an absent range must not be handed a timestamp.
    """
    start, end = parse_pg_range(value)
    return (
        EPOCH_DATE if start <= EPOCH_UTC else start.date(),
        EPOCH_DATE if end <= EPOCH_UTC else end.date(),
    )


def parse_timezone_offset(value: Any) -> timedelta:
    """``"-04:00"`` -> a timedelta. Day boundaries here are user-local."""
    text = _text(value).strip()
    if not text or text[0] not in "+-" or ":" not in text:
        return timedelta(0)
    sign = -1 if text[0] == "-" else 1
    try:
        hours, minutes = text[1:].split(":", 1)
        return sign * timedelta(hours=int(hours), minutes=int(minutes))
    except ValueError:
        return timedelta(0)


def local_day(moment: datetime, offset: timedelta) -> date:
    return (moment.astimezone(UTC) + offset).date()


def local_days(windows: Iterable[SyncWindow], offset: timedelta) -> list[str]:
    """Every user-local calendar day the windows touch, newest first."""
    seen: dict[str, None] = {}
    for window in sorted(windows, key=lambda item: item.end, reverse=True):
        day = local_day(window.end, offset)
        first = local_day(window.start, offset)
        while day >= first:
            seen.setdefault(day.isoformat(), None)
            day = day - timedelta(days=1)
    return list(seen)


# ------------------------------------------------------------- row mappers --


def cycle_to_row(*, account: str, payload: Mapping[str, Any], synced_at: datetime) -> dict[str, Any]:
    start_at, end_at = parse_pg_range(payload.get("during"))
    day_start, day_end = parse_pg_date_range(payload.get("days"))
    return {
        "account": account,
        "cycle_id": _text(payload.get("id")),
        "whoop_user_id": _int(payload.get("user_id")),
        "start_at": start_at,
        "end_at": end_at,
        # `days` is a DATE range: the user-local calendar day(s) the cycle is
        # awake for. Parsed, never cast, and split like `during`.
        "day_start": day_start,
        "day_end": day_end,
        "day_strain": _float(payload.get("day_strain")),
        "scaled_strain": _float(payload.get("scaled_strain")),
        "day_kilojoules": _float(payload.get("day_kilojoules")),
        "day_avg_heart_rate": _int(payload.get("day_avg_heart_rate")),
        "day_max_heart_rate": _int(payload.get("day_max_heart_rate")),
        "intensity_score": _float(payload.get("intensity_score")),
        "sleep_need": _float(payload.get("sleep_need")),
        # A cleaner in-progress signal than the epoch sentinel: WHOOP says when
        # it expects the cycle to close and whether the data is final.
        "predicted_end": parse_rfc3339(payload.get("predicted_end")),
        "data_state": _text(payload.get("data_state")),
        "timezone_offset": _text(payload.get("timezone_offset")),
        "created_at": parse_rfc3339(payload.get("created_at")),
        "updated_at": parse_rfc3339(payload.get("updated_at")),
        "raw_json": dict(payload),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def sleep_to_row(
    *,
    account: str,
    payload: Mapping[str, Any],
    synced_at: datetime,
    cycle_id: str = "",
) -> dict[str, Any]:
    start_at, end_at = parse_pg_range(payload.get("during"))
    optimal_start, optimal_end = parse_pg_range(payload.get("optimal_sleep_times"))
    return {
        "account": account,
        "activity_id": _text(payload.get("activity_id") or payload.get("id")),
        "cycle_id": _text(payload.get("cycle_id") or cycle_id),
        "whoop_user_id": _int(payload.get("user_id")),
        "start_at": start_at,
        "end_at": end_at,
        "is_nap": _bool_int(payload.get("is_nap")),
        "score": _float(payload.get("score")),
        "state": _text(payload.get("state")),
        "latency": _float(payload.get("latency")),
        "arousal_time": _float(payload.get("arousal_time")),
        "total_wake_events": _int(payload.get("total_wake_events")),
        "in_sleep_efficiency": _float(payload.get("in_sleep_efficiency")),
        "debt_pre": _float(payload.get("debt_pre")),
        "debt_post": _float(payload.get("debt_post")),
        "habitual_sleep_need": _float(payload.get("habitual_sleep_need")),
        "credit_from_naps": _float(payload.get("credit_from_naps")),
        "need_from_strain": _float(payload.get("need_from_strain")),
        "quality_duration": _float(payload.get("quality_duration")),
        "light_sleep_duration": _float(payload.get("light_sleep_duration")),
        "slow_wave_sleep_duration": _float(payload.get("slow_wave_sleep_duration")),
        "rem_sleep_duration": _float(payload.get("rem_sleep_duration")),
        "wake_duration": _float(payload.get("wake_duration")),
        "no_data_duration": _float(payload.get("no_data_duration")),
        "time_in_bed": _float(payload.get("time_in_bed")),
        "disturbances": _int(payload.get("disturbances")),
        "cycles_count": _int(payload.get("cycles_count")),
        "respiratory_rate": _float(payload.get("respiratory_rate")),
        "sleep_consistency": _float(payload.get("sleep_consistency")),
        "projected_score": _float(payload.get("projected_score")),
        "projected_sleep": _float(payload.get("projected_sleep")),
        "optimal_sleep_start": optimal_start,
        "optimal_sleep_end": optimal_end,
        "algo_version": _text(payload.get("algo_version")),
        "survey_response_id": _text(payload.get("survey_response_id")),
        "timezone_offset": _text(payload.get("timezone_offset")),
        "created_at": parse_rfc3339(payload.get("created_at")),
        "updated_at": parse_rfc3339(payload.get("updated_at")),
        "raw_json": dict(payload),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def recovery_to_row(*, account: str, payload: Mapping[str, Any], synced_at: datetime) -> dict[str, Any]:
    # The private API reports hrv_rmssd in SECONDS while the public API reports
    # hrv_rmssd_milli in milliseconds. Storing only one of them is how a 1000x
    # error gets into a chart, so both are explicit and named for their unit.
    hrv_seconds = _float(payload.get("hrv_rmssd"))
    return {
        "account": account,
        "activity_id": _text(payload.get("activity_id")),
        "recovery_score": _float(payload.get("recovery_score")),
        "resting_heart_rate": _int(payload.get("resting_heart_rate")),
        "hrv_rmssd_seconds": hrv_seconds,
        "hrv_rmssd_milli": hrv_seconds * 1000.0,
        "skin_temp_celsius": _float(payload.get("skin_temp_celsius")),
        "spo2": _float(payload.get("spo2")),
        "calibrating": _bool_int(payload.get("calibrating")),
        "prob_covid": _float(payload.get("prob_covid")),
        "hr_baseline": _float(payload.get("hr_baseline")),
        "hrv_component": _float(payload.get("hrv_component")),
        "rhr_component": _float(payload.get("rhr_component")),
        "recovery_rate": _float(payload.get("recovery_rate")),
        "state": _text(payload.get("state")),
        "algo_version": _text(payload.get("algo_version")),
        "history_size": _int(payload.get("history_size")),
        "survey_response_id": _text(payload.get("survey_response_id")),
        "created_at": parse_rfc3339(payload.get("created_at")),
        "updated_at": parse_rfc3339(payload.get("updated_at")),
        "raw_json": dict(payload),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def workout_to_row(*, account: str, payload: Mapping[str, Any], synced_at: datetime) -> dict[str, Any]:
    start_at, end_at = parse_pg_range(payload.get("during"))
    return {
        "account": account,
        "activity_id": _text(payload.get("activity_id") or payload.get("id")),
        "sport_id": _text(payload.get("sport_id")),
        "start_at": start_at,
        "end_at": end_at,
        "score": _float(payload.get("score")),
        "intensity_score": _float(payload.get("intensity_score")),
        "raw_intensity_score": _float(payload.get("raw_intensity_score")),
        "cumulative_workout_intensity": _float(payload.get("cumulative_workout_intensity")),
        "kilojoules": _float(payload.get("kilojoules")),
        "average_heart_rate": _int(payload.get("average_heart_rate")),
        "max_heart_rate": _int(payload.get("max_heart_rate")),
        "percent_recorded": _float(payload.get("percent_recorded")),
        "total_steps": _int(payload.get("total_steps")),
        "msk_score": _float(payload.get("msk_score")),
        "zone_durations_json": _mapping(payload.get("zone_durations")),
        "zone_durations_v2_json": _mapping(payload.get("zone_durations_v2")),
        "gps_data_json": _mapping(payload.get("gps_data")),
        "source": _text(payload.get("source")),
        "survey_response_id": _text(payload.get("survey_response_id")),
        "timezone_offset": _text(payload.get("timezone_offset")),
        "created_at": parse_rfc3339(payload.get("created_at")),
        "updated_at": parse_rfc3339(payload.get("updated_at")),
        "raw_json": dict(payload),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def sleep_event_rows(
    *,
    account: str,
    activity_id: str,
    payload: Any,
    synced_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, event in enumerate(_records(payload)):
        started_at, ended_at = parse_pg_range(event.get("during"))
        rows.append(
            {
                "account": account,
                "activity_id": activity_id,
                "event_index": index,
                "stage": _text(event.get("type")),
                "started_at": started_at,
                "ended_at": ended_at,
                "raw_json": dict(event),
                "synced_at": synced_at,
                "sync_version": sync_version_from_datetime(synced_at),
            }
        )
    return rows


def heart_rate_samples_to_rows(
    *,
    account: str,
    payload: Any,
    step_seconds: int,
    synced_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for point in _metric_values(payload):
        heart_rate = _int(point.get("data"))
        if heart_rate <= 0:
            # metrics-service pads gaps with zeroes; a zero heart rate is the
            # absence of a reading, not a reading of zero.
            continue
        rows.append(
            {
                "account": account,
                "sample_at": _epoch_millis(point.get("time")),
                "heart_rate": heart_rate,
                "step_seconds": int(step_seconds),
                "raw_json": dict(point),
                "synced_at": synced_at,
                "sync_version": sync_version_from_datetime(synced_at),
            }
        )
    return rows


#: The day's answers come back under one of these keys depending on whether the
#: draft exists; the endpoint is a mobile BFF, so tolerate the wrappers.
_JOURNAL_ENTRY_KEYS = ("entries", "journal_entries", "responses", "drafts", "records", "answers")


def journal_entries_to_rows(
    *,
    account: str,
    day: str,
    payload: Any,
    synced_at: datetime,
) -> list[dict[str, Any]]:
    entries = _journal_entries(payload)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in entries:
        # The live payload splits each entry into the question
        # (behavior_tracker) and the answer (tracker_input). Older/looser
        # shapes put both on the entry itself, so fall back to the entry.
        behavior = entry.get("behavior_tracker")
        if not isinstance(behavior, Mapping):
            behavior = entry
        answer_input = entry.get("tracker_input")
        if not isinstance(answer_input, Mapping):
            answer_input = entry

        raw_id = behavior.get("id")
        for fallback in ("question_id", "questionId", "behavior_id", "behaviorId"):
            if raw_id is None:
                raw_id = behavior.get(fallback)
        if raw_id is None:
            raw_id = answer_input.get("behavior_tracker_id")
        question_id = _text(raw_id)
        if not question_id or question_id in seen:
            continue
        seen.add(question_id)
        rows.append(
            {
                "account": account,
                "day": _date(day),
                "question_id": question_id,
                "question_text": _text(
                    behavior.get("question_text")
                    or behavior.get("questionText")
                    or behavior.get("question")
                    or behavior.get("title")
                ),
                "answer": _journal_answer_text(answer_input),
                # In the live API the behaviour tracker id IS the question id;
                # only a looser payload carries a distinct behavior_id.
                "behavior_id": _text(
                    behavior.get("behavior_id") or behavior.get("behaviorId")
                ) or question_id,
                "raw_json": dict(entry),
                "synced_at": synced_at,
                "sync_version": sync_version_from_datetime(synced_at),
            }
        )
    return rows


def _journal_entries(payload: Any) -> list[Mapping[str, Any]]:
    """The day's entries live at ``journal.tracked_behaviors`` in the live API."""
    if isinstance(payload, Mapping):
        journal = payload.get("journal")
        if isinstance(journal, Mapping):
            tracked = journal.get("tracked_behaviors")
            if isinstance(tracked, list):
                return [item for item in tracked if isinstance(item, Mapping)]
    return _records(payload, keys=_JOURNAL_ENTRY_KEYS)


def _journal_answer_text(answer_input: Mapping[str, Any]) -> str:
    """The answer, preferring a magnitude value over the yes/no gate.

    For a magnitude behaviour ("how many drinks?") ``answered_yes`` is merely
    true and the number is the real answer, so reporting the flag would throw
    away the measurement.
    """
    magnitude = answer_input.get("magnitude_input_value")
    if magnitude is not None:
        return str(magnitude)
    answered = answer_input.get("answered_yes")
    if isinstance(answered, bool):
        return "true" if answered else "false"
    return _answer_text(answer_input)


def sports_to_rows(*, account: str, payload: Any, synced_at: datetime) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sport in _records(payload, keys=("sports", "records", "history")):
        # `a or b` would treat sport id 0 -- Running -- as absent and silently
        # drop it, so test explicitly for None.
        raw_id = sport.get("id")
        if raw_id is None:
            raw_id = sport.get("sport_id")
        sport_id = _text(raw_id)
        if not sport_id:
            continue
        rows.append(
            {
                "account": account,
                "sport_id": sport_id,
                "name": _text(sport.get("name")),
                "category": _text(sport.get("category")),
                "has_gps": _bool_int(sport.get("has_gps")),
                "has_survey": _bool_int(sport.get("has_survey")),
                "activity_type_internal_name": _text(
                    sport.get("activity_type_internal_name") or sport.get("internal_name")
                ),
                "is_current": _bool_int(sport.get("is_current")),
                "raw_json": dict(sport),
                "synced_at": synced_at,
                "sync_version": sync_version_from_datetime(synced_at),
            }
        )
    return rows


def document_to_row(
    *,
    account: str,
    kind: str,
    doc_key: str,
    payload: Any,
    collected_at: datetime,
    synced_at: datetime,
) -> dict[str, Any]:
    """A Tier-2 payload, stored faithfully and given no typed columns."""
    return {
        "account": account,
        "kind": kind,
        "doc_key": doc_key,
        "collected_at": collected_at,
        "raw_json": payload if isinstance(payload, (dict, list)) else {"value": payload},
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


# ------------------------------------------------------------------ runner --


class WhoopPrivateSyncRunner:
    """Walks every private-API collection for one account, once."""

    def __init__(
        self,
        *,
        settings: Settings,
        warehouse,
        logger,
        client_factory: Callable[[WhoopPrivateConfig, WhoopPrivateSession], Any] | None = None,
        http_factory: Callable[[], Any] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._settings = settings
        self._warehouse = warehouse
        self._logger = logger
        self._client_factory = client_factory
        self._http_factory = http_factory
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._credential_sha256 = ""
        self._sleep_ids: list[str] = []
        self._workout_windows: list[tuple[str, datetime, datetime]] = []

    # -- entry point ---------------------------------------------------

    def sync_all(self) -> list[WhoopPrivateSyncSummary]:
        config = self._settings.whoop_private
        if config is None or not config.account:
            raise RuntimeError("The WHOOP private API is not configured")
        synced_at = self._now()
        if not config.enabled:
            return [self._skipped(config.account, "WHOOP_PRIVATE_ENABLED is false", record_state=False)]

        self._warehouse.ensure_whoop_private_tables()
        account = config.account
        # A collection that no longer exists keeps whatever status it last
        # recorded, forever: marts_ops.pipeline_health judges this pipeline from
        # the status column of EVERY row in this table, and no run will ever
        # touch a retired one again. `ok` is the lucky case, not the safe one.
        _call_supported(
            self._warehouse.prune_whoop_private_sync_state,
            account=account,
            keep_collections=WHOOP_PRIVATE_COLLECTIONS,
        )
        state_by_key = self._warehouse.load_whoop_private_sync_state() or {}

        session = self._load_session(config)
        if session is None:
            reason = f"no WHOOP private session has been published for {account}; {PUBLISH_SESSION_HINT}"
            self._logger.warning(reason)
            self._record_all(
                account=account,
                state_by_key=state_by_key,
                status=WHOOP_PRIVATE_STATUS_ACTION_REQUIRED,
                error=reason,
                updated_at=synced_at,
            )
            # Never published is a setup gap, not an incident: keep the run
            # green and let /pipelines carry the attention.
            return [self._skipped(account, reason)]

        self._credential_sha256 = whoop_private_credential_sha256(session.refresh_token)
        if session.refresh_token_expired(now=synced_at):
            reason = (
                f"the WHOOP private refresh token for {account} expired on "
                f"{session.refresh_expires_at.date()}; {PUBLISH_SESSION_HINT}"
            )
            self._fail_action_required(
                account=account, state_by_key=state_by_key, reason=reason, updated_at=synced_at
            )

        skip_reason = whoop_private_reauthorization_skip_reason(
            state_by_key, account=account, refresh_token=session.refresh_token
        )
        if skip_reason is not None:
            self._logger.warning(skip_reason)
            raise WhoopPrivateActionRequiredError(skip_reason)

        client = self._build_client(config, session)
        summary = WhoopPrivateSyncSummary(account=account, sync_type="mixed", records_written=0)

        try:
            identity = client.bootstrap()
        except WhoopPrivateAuthError as error:
            self._fail_action_required(
                account=account,
                state_by_key=state_by_key,
                reason=f"the WHOOP private session was rejected ({error}); {PUBLISH_SESSION_HINT}",
                updated_at=synced_at,
            )
        except WhoopPrivateRateLimitedError as error:
            self._record_all(
                account=account,
                state_by_key=state_by_key,
                status=WHOOP_PRIVATE_STATUS_RATE_LIMITED,
                error=str(error),
                updated_at=synced_at,
            )
            summary.rate_limited = True
            return [summary]

        failures: list[str] = []
        remaining = list(WHOOP_PRIVATE_COLLECTIONS)
        for collection in WHOOP_PRIVATE_COLLECTIONS:
            remaining.remove(collection)
            state = state_by_key.get((account, collection))
            signature = whoop_private_collection_signature(collection)
            try:
                written, sync_type, watermark = self._sync_collection(
                    collection=collection,
                    config=config,
                    client=client,
                    identity=identity,
                    state=state,
                    synced_at=synced_at,
                    signature=signature,
                )
            except WhoopPrivateAuthError as error:
                # The session is global: every remaining collection is dead too.
                reason = f"the WHOOP private session was rejected ({error}); {PUBLISH_SESSION_HINT}"
                for pending in [collection, *remaining]:
                    self._insert_state(
                        account=account,
                        collection=pending,
                        watermark_updated_at=state_datetime(
                            state_by_key.get((account, pending)), "watermark_updated_at"
                        ),
                        last_sync_type=str((state_by_key.get((account, pending)) or {}).get("last_sync_type", "unknown")),
                        status=WHOOP_PRIVATE_STATUS_ACTION_REQUIRED,
                        error=truncate_error(self._redact(str(reason), session)),
                        updated_at=synced_at,
                        # A run that wrote nothing has re-walked nothing: claiming
                        # the current signature here would retire the re-walk
                        # before it happened.
                        collection_signature=stored_signature(state_by_key.get((account, pending))),
                    )
                self._logger.warning(reason)
                raise WhoopPrivateActionRequiredError(reason) from error
            except WhoopPrivateRateLimitedError as error:
                self._insert_state(
                    account=account,
                    collection=collection,
                    watermark_updated_at=state_datetime(state, "watermark_updated_at"),
                    last_sync_type=str((state or {}).get("last_sync_type", "unknown")),
                    status=WHOOP_PRIVATE_STATUS_RATE_LIMITED,
                    error=truncate_error(str(error)),
                    updated_at=synced_at,
                    collection_signature=stored_signature(state),
                )
                self._logger.warning(
                    "WHOOP private API rate limited during %s; ending this run cleanly", collection
                )
                summary.rate_limited = True
                break
            except Exception as error:  # noqa: BLE001 - recorded, then re-raised in aggregate
                self._insert_state(
                    account=account,
                    collection=collection,
                    watermark_updated_at=state_datetime(state, "watermark_updated_at"),
                    last_sync_type=str((state or {}).get("last_sync_type", "unknown")),
                    status=WHOOP_PRIVATE_STATUS_FAILED,
                    error=truncate_error(self._redact(str(error), session)),
                    updated_at=synced_at,
                    collection_signature=stored_signature(state),
                )
                failures.append(f"{collection}: {self._redact(str(error), session)}")
                continue

            summary.collections[collection] = written
            summary.records_written += written
            self._insert_state(
                account=account,
                collection=collection,
                watermark_updated_at=watermark,
                last_sync_type=sync_type,
                status=WHOOP_PRIVATE_STATUS_OK,
                error="",
                updated_at=synced_at,
                collection_signature=signature,
            )

        if failures:
            raise RuntimeError("WHOOP private sync failed for: " + "; ".join(failures))
        return [summary]

    # -- collections ---------------------------------------------------

    def _sync_collection(
        self,
        *,
        collection: str,
        config: WhoopPrivateConfig,
        client,
        identity: WhoopPrivateIdentity,
        state: Mapping[str, Any] | None,
        synced_at: datetime,
        signature: str = "",
    ) -> tuple[int, str, datetime]:
        handler = {
            "cycles": self._sync_cycles,
            "sleep_events": self._sync_sleep_events,
            "heart_rate": self._sync_heart_rate,
            "journal": self._sync_journal,
            "sports": self._sync_sports,
            "documents": self._sync_documents,
        }[collection]
        return handler(
            config=config,
            client=client,
            identity=identity,
            state=state,
            synced_at=synced_at,
            signature=signature,
        )

    def _sync_cycles(self, *, config, client, identity, state, synced_at, signature=""):
        plan = self._plan(
            state=state,
            config=config,
            now=synced_at,
            backfill_span=timedelta(days=config.backfill_window_days),
            lookback=timedelta(days=config.incremental_lookback_days),
            signature=signature,
        )
        account = config.account
        self._sleep_ids = []
        self._workout_windows = []
        written = 0
        for window in plan.windows:
            payload = client.cycles_details(
                user_id=identity.user_id,
                start=window.start,
                end=window.end,
                limit=config.cycles_page_limit,
            )
            records = _records(payload)
            if len(records) >= config.cycles_page_limit:
                self._logger.warning(
                    "WHOOP private cycles window %s..%s filled the %s-record limit; "
                    "reduce WHOOP_PRIVATE_BACKFILL_WINDOW_DAYS or raise WHOOP_PRIVATE_CYCLES_PAGE_LIMIT",
                    window.start,
                    window.end,
                    config.cycles_page_limit,
                )
            written += self._write_cycle_records(account=account, records=records, synced_at=synced_at)
        # Newest first, so a bounded follow-up collection spends its budget on
        # what was asked about most recently.
        self._workout_windows.sort(key=lambda item: item[1], reverse=True)
        return written, plan.sync_type, plan.next_cursor

    def _write_cycle_records(self, *, account: str, records: Sequence[Mapping[str, Any]], synced_at) -> int:
        cycles, sleeps, recoveries, workouts = [], [], [], []
        for record in records:
            cycle = _mapping(record.get("cycle"))
            cycle_id = _text(cycle.get("id"))
            if cycle:
                cycles.append(cycle_to_row(account=account, payload=cycle, synced_at=synced_at))
            recovery = _mapping(record.get("recovery"))
            if recovery:
                recoveries.append(recovery_to_row(account=account, payload=recovery, synced_at=synced_at))
            for sleep in _sequence(record.get("sleeps")):
                row = sleep_to_row(account=account, payload=sleep, synced_at=synced_at, cycle_id=cycle_id)
                sleeps.append(row)
                if row["activity_id"]:
                    self._sleep_ids.append(row["activity_id"])
            for workout in _sequence(record.get("workouts")):
                row = workout_to_row(account=account, payload=workout, synced_at=synced_at)
                workouts.append(row)
                if row["activity_id"] and row["start_at"] > EPOCH_UTC and row["end_at"] > row["start_at"]:
                    self._workout_windows.append((row["activity_id"], row["start_at"], row["end_at"]))
        if cycles:
            self._warehouse.insert_whoop_private_cycles(cycles)
        if sleeps:
            self._warehouse.insert_whoop_private_sleeps(sleeps)
        if recoveries:
            self._warehouse.insert_whoop_private_recoveries(recoveries)
        if workouts:
            self._warehouse.insert_whoop_private_workouts(workouts)
        return len(cycles) + len(sleeps) + len(recoveries) + len(workouts)

    def _sync_sleep_events(self, *, config, client, identity, state, synced_at, signature=""):
        written = 0
        rows: list[dict[str, Any]] = []
        for activity_id in _unique(self._sleep_ids)[: config.max_sleep_event_requests]:
            payload = client.sleep_events(activity_id=activity_id)
            rows.extend(
                sleep_event_rows(
                    account=config.account,
                    activity_id=activity_id,
                    payload=payload,
                    synced_at=synced_at,
                )
            )
        if rows:
            self._warehouse.insert_whoop_private_sleep_events(rows)
            written = len(rows)
        return written, "follows_cycles", self._cycles_watermark(state, synced_at)

    def _sync_heart_rate(self, *, config, client, identity, state, synced_at, signature=""):
        """Every hour of the account's life, at the one grain, chunk by chunk.

        Three things here are the difference between this and a naive loop, and
        each of them was a real problem first:

        * **A chunk is written before the next is fetched.** At 6s a six-hour
          chunk is 3,600 points carrying their own raw_json, and a run's budget
          is dozens of chunks; accumulating them all before one insert was
          affordable at minute grain and is not now.
        * **The stale grain is deleted over exactly the window just written,
          after it is written.** A 60s sample colliding with a 6s sample on the
          primary key loses to it, but one whose millisecond offset misses the
          6s grid survives beside it and double-counts that minute in every
          average. Deleting after the insert means the series is never briefly
          empty.
        * **The floor is the account's first cycle**, not `full_sync_start`. A
          member has no heart rate before they had a WHOOP, and production spent
          real runs walking an account whose first cycle is 2025-10-23 back to
          2025-02-03 asking for windows that cannot contain a reading.
        """
        chunk = timedelta(hours=config.heart_rate_chunk_hours)
        plan = self._plan(
            state=state,
            config=config,
            now=synced_at,
            backfill_span=chunk * config.heart_rate_chunks_per_run,
            lookback=timedelta(hours=config.heart_rate_recent_hours),
            signature=signature,
            floor=self._heart_rate_floor(config),
        )
        written = 0
        for window in plan.windows:
            for slice_ in chunk_window(window, chunk):
                payload = client.heart_rate(
                    user_id=identity.user_id,
                    start=slice_.start,
                    end=slice_.end,
                    step=WHOOP_PRIVATE_HEART_RATE_STEP_SECONDS,
                )
                rows = heart_rate_samples_to_rows(
                    account=config.account,
                    payload=payload,
                    step_seconds=WHOOP_PRIVATE_HEART_RATE_STEP_SECONDS,
                    synced_at=synced_at,
                )
                if rows:
                    self._warehouse.insert_whoop_private_heart_rate_samples(rows)
                    written += len(rows)
                _call_supported(
                    self._warehouse.delete_whoop_private_heart_rate_samples,
                    account=config.account,
                    start=slice_.start,
                    end=slice_.end,
                    keep_step_seconds=WHOOP_PRIVATE_HEART_RATE_STEP_SECONDS,
                )
        return written, plan.sync_type, plan.next_cursor

    def _heart_rate_floor(self, config: WhoopPrivateConfig) -> datetime | None:
        """The account's first cycle day, or None to keep the configured floor.

        Cycles sync first, so on a fresh database this floor tightens as the
        cycle backfill walks back -- and because the cursor is stored where it
        stopped, a heart-rate walk that declared itself complete against a
        too-recent floor simply resumes once the real one appears.
        """
        earliest = _call_supported(
            self._warehouse.whoop_private_earliest_cycle_day, account=config.account
        )
        if earliest is None:
            return None
        return datetime(earliest.year, earliest.month, earliest.day, tzinfo=UTC)

    def _sync_journal(self, *, config, client, identity, state, synced_at, signature=""):
        plan = self._plan(
            state=state,
            config=config,
            now=synced_at,
            backfill_span=timedelta(days=config.journal_days_per_run),
            lookback=timedelta(days=1),
            signature=signature,
        )
        offset = parse_timezone_offset(identity.timezone_offset)
        days = local_days(plan.windows, offset)[: config.journal_days_per_run + 1]
        rows: list[dict[str, Any]] = []
        for day in days:
            payload = client.journal_entries(day=day)
            rows.extend(
                journal_entries_to_rows(
                    account=config.account, day=day, payload=payload, synced_at=synced_at
                )
            )
        if rows:
            self._warehouse.insert_whoop_private_journal_entries(rows)
        return len(rows), plan.sync_type, plan.next_cursor

    def _sync_sports(self, *, config, client, identity, state, synced_at, signature=""):
        rows = sports_to_rows(
            account=config.account,
            payload=client.sports_catalog(country_code=config.sports_country_code),
            synced_at=synced_at,
        )
        if rows:
            self._warehouse.insert_whoop_private_sports(rows)
        return len(rows), "snapshot", synced_at

    #: Document kinds keyed by a local day. ``trend`` and ``health_tab`` are
    #: whole-series/current-state snapshots a single call already covers, and
    #: ``cardio_details`` is keyed by activity, so none of them are here.
    DAY_DOCUMENT_KINDS = ("stress", "sleep_deep_dive", "strain_deep_dive", "behavior_impact")

    #: The historic backfill walks all of them. These are not cheap -- measured
    #: against the live API on 2026-08-23, a recent `stress` day is ~1.7 MB and
    #: `sleep_deep_dive` ~935 KB, against ~5 KB for `strain_deep_dive` and 326
    #: BYTES for `behavior_impact` -- so a full walk is a few hundred MB of
    #: jsonb. That is a deliberate trade: storage is recoverable, an unpulled
    #: history is not, and the per-run budget below is the dial to turn down if
    #: it ever needs turning down.
    BACKFILL_DOCUMENT_KINDS = DAY_DOCUMENT_KINDS

    def _day_document(self, *, client, kind: str, day: str):
        return {
            "stress": lambda: client.stress(day=day),
            "sleep_deep_dive": lambda: client.sleep_deep_dive(day=day),
            "strain_deep_dive": lambda: client.strain_deep_dive(day=day),
            "behavior_impact": lambda: client.behavior_impact(day=day),
        }[kind]()

    def _document_backfill_days(
        self, *, config, warehouse, recent: list[str], today: date
    ) -> tuple[list[str], set[tuple[str, str]]]:
        """Historic days still missing a document, newest first and bounded.

        The documents table is the cursor: a day is skipped once it is stored,
        so an interrupted backfill resumes with no watermark to repair, and a
        day WHOOP has no data for is stored once (empty) and never re-asked.
        """
        budget = max(0, config.documents_backfill_days_per_run)
        if budget == 0:
            return [], set()
        floor = warehouse.whoop_private_earliest_cycle_day(account=config.account)
        if floor is None:
            # Cycles sync first and had nothing, so there is no floor. Not a
            # reason to guess one.
            return [], set()
        stored = warehouse.whoop_private_document_keys(
            account=config.account, kinds=self.BACKFILL_DOCUMENT_KINDS
        )
        recent_days = set(recent)
        wanted: list[str] = []
        day = today - timedelta(days=1)
        while day >= floor and len(wanted) < budget:
            key = day.isoformat()
            if key not in recent_days and not all(
                (kind, key) in stored for kind in self.BACKFILL_DOCUMENT_KINDS
            ):
                wanted.append(key)
            day -= timedelta(days=1)
        return wanted, stored

    def _sync_documents(self, *, config, client, identity, state, synced_at, signature=""):
        offset = parse_timezone_offset(identity.timezone_offset)
        today = local_day(synced_at, offset)
        recent = [
            (today - timedelta(days=index)).isoformat()
            for index in range(max(1, config.documents_lookback_days))
        ]
        account = config.account
        rows: list[dict[str, Any]] = []

        for metric in config.trend_metrics:
            rows.append(
                document_to_row(
                    account=account,
                    kind="trend",
                    doc_key=metric,
                    payload=client.trend(metric=metric, end_date=recent[0]),
                    collected_at=synced_at,
                    synced_at=synced_at,
                )
            )
        # WHOOP Age, Pace of Aging and the Health Monitor are account state
        # rather than a day, so one row is kept current under a fixed key.
        rows.append(
            document_to_row(
                account=account,
                kind="health_tab",
                doc_key="current",
                payload=client.health_tab(),
                collected_at=synced_at,
                synced_at=synced_at,
            )
        )
        backfill, stored_keys = self._document_backfill_days(
            config=config, warehouse=self._warehouse, recent=recent, today=today
        )
        if backfill:
            self._logger.info(
                "whoop_private documents: refreshing %d recent day(s) and backfilling %d "
                "historic day(s) from %s",
                len(recent),
                len(backfill),
                backfill[-1],
            )
        recent_days = set(recent)
        written = 0
        for day in [*recent, *backfill]:
            # Flushed per day rather than accumulated: a `stress` day alone is
            # ~1.7 MB, so a 20-day run would otherwise hold tens of MB of
            # payload in memory before the first insert.
            day_rows: list[dict[str, Any]] = []
            for kind in self.DAY_DOCUMENT_KINDS:
                # A recent day is always refreshed -- an in-progress day still
                # changes. A historic day fetches only what it is missing, so a
                # half-filled day does not spend the run's budget restating
                # itself.
                if day not in recent_days and (kind, day) in stored_keys:
                    continue
                day_rows.append(
                    document_to_row(
                        account=account,
                        kind=kind,
                        doc_key=day,
                        payload=self._day_document(client=client, kind=kind, day=day),
                        collected_at=parse_rfc3339(f"{day}T00:00:00Z"),
                        synced_at=synced_at,
                    )
                )
            if day_rows:
                self._warehouse.insert_whoop_private_documents(day_rows)
                written += len(day_rows)
        for activity_id, start, _end in self._workout_windows[: config.max_workout_requests]:
            rows.append(
                document_to_row(
                    account=account,
                    kind="cardio_details",
                    doc_key=activity_id,
                    payload=client.cardio_details(activity_id=activity_id),
                    collected_at=start,
                    synced_at=synced_at,
                )
            )
        if rows:
            self._warehouse.insert_whoop_private_documents(rows)
        return written + len(rows), "snapshot", synced_at

    # -- plumbing ------------------------------------------------------

    def _plan(
        self,
        *,
        state,
        config: WhoopPrivateConfig,
        now: datetime,
        backfill_span,
        lookback,
        signature: str = "",
        floor: datetime | None = None,
    ) -> SyncPlan:
        configured_floor = parse_rfc3339(config.full_sync_start)
        if configured_floor <= EPOCH_UTC:
            configured_floor = now - timedelta(days=3650)
        # A collection's own floor may only tighten the configured one: it is
        # there to stop a walk early, never to reach back past what was asked
        # for.
        if floor is not None and floor > configured_floor:
            configured_floor = floor
        if config.force_full_sync or stored_signature(state) != signature:
            # A different signature means the stored rows were produced by a
            # different question. Resuming the cursor would leave every one of
            # them at the old answer forever.
            cursor = EPOCH_UTC
        else:
            cursor = state_datetime(state, "watermark_updated_at")
        return plan_windows(
            cursor=cursor,
            now=now,
            floor=configured_floor,
            backfill_span=backfill_span,
            lookback=lookback,
        )

    def _cycles_watermark(self, state, synced_at: datetime) -> datetime:
        watermark = state_datetime(state, "watermark_updated_at")
        return watermark if watermark > EPOCH_UTC else synced_at

    def _load_session(self, config: WhoopPrivateConfig) -> WhoopPrivateSession | None:
        row = _call_supported(
            self._warehouse.load_whoop_private_session,
            account=config.account,
            session_key=config.session_key,
        )
        return session_from_row(row, account=config.account)

    def _build_client(self, config: WhoopPrivateConfig, session: WhoopPrivateSession):
        if self._client_factory is not None:
            return self._client_factory(config, session)
        # The refresh token rotates on EVERY refresh, so the compare-and-swap
        # witness has to be the token this client last held, not the one the
        # run started with.
        held = {"refresh_token": session.refresh_token}

        def on_session_rotated(rotated: WhoopPrivateSession) -> None:
            _call_supported(
                self._warehouse.rotate_whoop_private_session,
                account=config.account,
                session_key=config.session_key,
                expected_refresh_token=held["refresh_token"],
                expected_refresh_token_sha256=whoop_private_credential_sha256(held["refresh_token"]),
                access_token=rotated.access_token,
                refresh_token=rotated.refresh_token,
                access_expires_at=rotated.access_expires_at,
                refresh_expires_at=rotated.refresh_expires_at,
                refresh_token_sha256=whoop_private_credential_sha256(rotated.refresh_token),
                updated_at=self._now(),
            )
            held["refresh_token"] = rotated.refresh_token
            self._credential_sha256 = whoop_private_credential_sha256(rotated.refresh_token)

        kwargs: dict[str, Any] = {}
        if self._http_factory is not None:
            kwargs["http"] = self._http_factory()
        return WhoopPrivateClient(
            session=session,
            base_url=config.base_url,
            timeout=float(config.request_timeout_seconds),
            now=self._now,
            on_session_rotated=on_session_rotated,
            **kwargs,
        )

    def _insert_state(self, **row) -> None:
        row.setdefault("credential_sha256", self._credential_sha256)
        _call_supported(self._warehouse.insert_whoop_private_sync_state, **row)

    def _record_all(self, *, account, state_by_key, status, error, updated_at) -> None:
        for collection in WHOOP_PRIVATE_COLLECTIONS:
            state = state_by_key.get((account, collection))
            self._insert_state(
                account=account,
                collection=collection,
                watermark_updated_at=state_datetime(state, "watermark_updated_at"),
                last_sync_type=str((state or {}).get("last_sync_type", "unknown")),
                status=status,
                error=truncate_error(error),
                updated_at=updated_at,
                collection_signature=stored_signature(state),
            )

    def _fail_action_required(self, *, account, state_by_key, reason, updated_at) -> None:
        self._record_all(
            account=account,
            state_by_key=state_by_key,
            status=WHOOP_PRIVATE_STATUS_ACTION_REQUIRED,
            error=reason,
            updated_at=updated_at,
        )
        self._logger.warning(reason)
        raise WhoopPrivateActionRequiredError(reason)

    def _skipped(self, account: str, reason: str, *, record_state: bool = True) -> WhoopPrivateSyncSummary:
        return WhoopPrivateSyncSummary(
            account=account,
            sync_type="skipped",
            records_written=0,
            action_required=record_state,
            skipped_reason=reason,
        )

    @staticmethod
    def _redact(message: str, session: WhoopPrivateSession | None) -> str:
        if session is None:
            return message
        redacted = message
        for secret in (session.access_token, session.refresh_token):
            if secret:
                redacted = redacted.replace(secret, "[redacted]")
        return redacted


# ------------------------------------------------------------------ helpers --


def _call_supported(method: Callable[..., Any], **kwargs: Any) -> Any:
    """Call ``method`` with only the keywords it declares.

    The warehouse side of this source is owned by ``postgres.py``; the exact
    spelling of a compare-and-swap witness (token vs. fingerprint) and of the
    optional ``session_key`` belongs to the schema, not to the sync. Sending
    everything we know and letting the store take what it declares keeps the
    two from having to agree on optional arguments.
    """
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        return method(**kwargs)
    parameters = list(signature.parameters.values())
    if any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters):
        return method(**kwargs)
    accepted = {
        parameter.name
        for parameter in parameters
        if parameter.kind
        in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    }
    return method(**{name: value for name, value in kwargs.items() if name in accepted})


def _records(payload: Any, *, keys: Sequence[str] = ("records",)) -> list[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, Mapping)]
        return []
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]
    return []


def _metric_values(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        values = payload.get("values")
        if isinstance(values, list):
            return [item for item in values if isinstance(item, Mapping)]
    return []


def _sequence(value: Any) -> list[Mapping[str, Any]]:
    return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _unique(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _epoch_millis(value: Any) -> datetime:
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=UTC)
    except (TypeError, ValueError, OSError, OverflowError):
        return EPOCH_UTC


def _answer_text(entry: Mapping[str, Any]) -> str:
    for key in ("answer", "answered_value", "value", "response", "answer_text"):
        if key in entry and entry[key] is not None:
            value = entry[key]
            if isinstance(value, (Mapping, list)):
                return json.dumps(value, sort_keys=True, default=str)
            if isinstance(value, bool):
                return "true" if value else "false"
            return str(value)
    return ""


def _date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(_text(value)[:10])
    except ValueError:
        return EPOCH_DATE


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0


def _float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _bool_int(value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, str):
        return 1 if value.strip().lower() in {"1", "true", "yes", "y", "on"} else 0
    return 1 if value else 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync the WHOOP private (app) API into the personal data warehouse."
    )
    parser.add_argument(
        "--force-full",
        action="store_true",
        help="Restart every collection's backfill from the newest end (bounded per run as usual).",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings(require_gmail=False, require_whoop_private=True)
    if args.force_full and settings.whoop_private is not None:
        from dataclasses import replace

        settings = replace(settings, whoop_private=replace(settings.whoop_private, force_full_sync=True))
    warehouse = warehouse_from_settings(settings)
    summaries = WhoopPrivateSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logging.getLogger("personal_data_warehouse.whoop_private_sync"),
    ).sync_all()
    print(
        json.dumps(
            [public_whoop_private_sync_summary(summary) for summary in summaries],
            sort_keys=True,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
