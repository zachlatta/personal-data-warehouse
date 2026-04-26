from __future__ import annotations

from dagster import DagsterRunStatus, RunsFilter, SkipReason

ACTIVE_RUN_STATUSES = (
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
)


def skip_if_job_active(context, *, job_name: str) -> SkipReason | dict:
    runs = context.instance.get_runs(
        filters=RunsFilter(job_name=job_name, statuses=ACTIVE_RUN_STATUSES),
        limit=1,
    )
    if runs:
        return SkipReason(f"Skipping {job_name}; an earlier run is still queued or active.")
    return {}
