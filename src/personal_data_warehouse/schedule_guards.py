from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
import os

from dagster import DagsterRunStatus, RunsFilter, SkipReason

ACTIVE_RUN_STATUSES = (
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
)
IN_PROGRESS_RUN_STATUSES = (
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
    DagsterRunStatus.CANCELING,
)


def skip_if_job_active(context, *, job_name: str) -> SkipReason | dict:
    stale_after = timedelta(minutes=_int_env("SCHEDULE_ACTIVE_RUN_STALE_AFTER_MINUTES", 10))
    updated_after = datetime.now(tz=UTC) - stale_after
    runs = context.instance.get_runs(
        filters=RunsFilter(job_name=job_name, statuses=ACTIVE_RUN_STATUSES, updated_after=updated_after),
        limit=1,
    )
    if runs:
        return SkipReason(f"Skipping {job_name}; an earlier run was updated in the last {stale_after}.")
    return {}


def skip_if_job_in_progress(context, *, job_name: str) -> SkipReason | dict:
    runs = context.instance.get_runs(
        filters=RunsFilter(job_name=job_name, statuses=IN_PROGRESS_RUN_STATUSES),
        limit=1,
    )
    if runs:
        return SkipReason(f"Skipping {job_name}; an earlier run is already queued or in progress.")
    return {}


#: A keepalive sensor (a long-running client relaunched whenever no run is
#: active) waits out a cooldown after this many consecutive failed runs.
#: Otherwise a crash-looping client produces a red run every sensor tick:
#: ~3k failed WhatsApp runs over 2.5 days in 2026-07 buried real signal.
KEEPALIVE_CRASH_STREAK = 3
KEEPALIVE_CRASH_COOLDOWN_SECONDS = 900


def keepalive_crash_backoff_skip(
    finished_runs: Sequence[tuple[str, float | None]],
    *,
    job_name: str,
    now: float,
    hint: str = "",
    streak: int = KEEPALIVE_CRASH_STREAK,
    cooldown_seconds: float = KEEPALIVE_CRASH_COOLDOWN_SECONDS,
) -> SkipReason | None:
    """Skip while the newest ``streak`` finished runs are all failures.

    ``finished_runs`` is newest-first ``(status, end_time_epoch)``. Returns
    ``None`` (launch) once the newest failure is older than the cooldown, so a
    persistent crash still retries a few times an hour and self-heals the
    moment a run succeeds.
    """
    if len(finished_runs) < streak:
        return None
    window = list(finished_runs[:streak])
    if any(status != "FAILURE" for status, _end in window):
        return None
    newest_end = max((end for _status, end in window if end is not None), default=None)
    if newest_end is None:
        return None
    remaining = cooldown_seconds - (now - newest_end)
    if remaining <= 0:
        return None
    message = (
        f"{job_name} failed {streak} consecutive runs; in crash cooldown for another "
        f"{int(remaining)}s before relaunching."
    )
    return SkipReason(f"{message} {hint}".strip())


def finished_job_runs(instance, *, job_name: str, limit: int = KEEPALIVE_CRASH_STREAK) -> list[tuple[str, float | None]]:
    """Newest-first ``(status, end_time)`` of a job's runs, for :func:`keepalive_crash_backoff_skip`."""
    records = instance.get_run_records(filters=RunsFilter(job_name=job_name), limit=limit)
    return [(record.dagster_run.status.value, record.end_time) for record in records]


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default
