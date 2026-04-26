from __future__ import annotations

from datetime import UTC, datetime, timedelta
import os

from dagster import DagsterRunStatus, RunsFilter, SkipReason

ACTIVE_RUN_STATUSES = (
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
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


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default
