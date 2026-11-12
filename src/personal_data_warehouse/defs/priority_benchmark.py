from __future__ import annotations

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    RetryPolicy,
    asset,
    define_asset_job,
    definitions,
    schedule,
)

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.priority_benchmark import LabelsUnpublishedError, measure
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

PRIORITY_BENCHMARK_POSTGRES_LOCK_ID = 8_407_113_000
PRIORITY_BENCHMARK_CRON = "37 5 * * *"


@asset(group_name="search", retry_policy=RetryPolicy(max_retries=1, delay=60))
def priority_benchmark(context) -> MaterializeResult:
    """Refresh C2's private labeled correctness measurement without invoking an agent."""
    settings = load_settings(require_gmail=False)
    warehouse = warehouse_from_settings(settings)
    try:
        with exclusive_sync_lock(
            name="priority_benchmark",
            postgres_lock_id=PRIORITY_BENCHMARK_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                return MaterializeResult(
                    metadata={"skipped": "another priority benchmark is active"}
                )
            warehouse.ensure_priority_benchmark_tables()
            try:
                results = measure(warehouse)
            except LabelsUnpublishedError:
                # A normal bootstrap state, like search's missing credentials.
                # Do not write a measurement: the mart remains explicitly failing.
                context.log.info(
                    "Skipping priority benchmark: labels have not been published"
                )
                return MaterializeResult(metadata={"skipped": "labels unpublished"})
            return MaterializeResult(
                metadata={
                    "adapters": len({row["adapter"] for row in results}),
                    "labeled": sum(row["labeled"] for row in results),
                    "correct": sum(row["correct"] for row in results),
                    "missing": sum(row["missing"] for row in results),
                }
            )
    finally:
        warehouse.close()


priority_benchmark_job = define_asset_job(
    "priority_benchmark_job",
    selection=[priority_benchmark],
    tags={"dagster/max_runtime": "900"},
)


@schedule(
    cron_schedule=PRIORITY_BENCHMARK_CRON,
    execution_timezone="UTC",
    job=priority_benchmark_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def priority_benchmark_daily(context):
    return skip_if_job_active(context, job_name="priority_benchmark_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[priority_benchmark],
        jobs=[priority_benchmark_job],
        schedules=[priority_benchmark_daily],
    )
