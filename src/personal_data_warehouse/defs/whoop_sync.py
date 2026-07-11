from __future__ import annotations

import os

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
    define_asset_job,
    definitions,
    schedule,
)

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings
from personal_data_warehouse.whoop_sync import WhoopSyncRunner, public_whoop_sync_summary

WHOOP_SYNC_POSTGRES_LOCK_ID = 8_407_112_468


def whoop_schedule_default_status() -> DefaultScheduleStatus:
    enabled = os.getenv("WHOOP_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
    configured = bool(
        (os.getenv("WHOOP_ACCOUNT") or os.getenv("GMAIL_ACCOUNTS"))
        and os.getenv("WHOOP_CLIENT_ID")
        and os.getenv("WHOOP_CLIENT_SECRET")
        and (os.getenv("WHOOP_TOKEN_JSON") or os.getenv("WHOOP_TOKEN_JSON_B64"))
    )
    return DefaultScheduleStatus.RUNNING if enabled and configured else DefaultScheduleStatus.STOPPED


@asset(
    group_name="whoop",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def whoop_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_whoop=True)
    if settings.whoop is not None and not settings.whoop.enabled:
        return MaterializeResult(metadata={"skipped": "WHOOP_ENABLED is false"})
    warehouse = warehouse_from_settings(settings)
    with exclusive_sync_lock(name="whoop", postgres_lock_id=WHOOP_SYNC_POSTGRES_LOCK_ID) as acquired:
        if not acquired:
            context.log.warning("Skipping WHOOP sync because another WHOOP sync is already running")
            summaries = []
        else:
            summaries = WhoopSyncRunner(settings=settings, warehouse=warehouse, logger=context.log).sync_all()

    public_summaries = [public_whoop_sync_summary(summary) for summary in summaries]
    return MaterializeResult(
        metadata={
            "whoop": MetadataValue.json(public_summaries),
            "account_count": len(public_summaries),
            "records_written": sum(summary.records_written for summary in summaries),
        }
    )


whoop_sync_job = define_asset_job(
    "whoop_sync_job",
    selection=[whoop_sync],
)


@schedule(
    cron_schedule="*/5 * * * *",
    job=whoop_sync_job,
    default_status=whoop_schedule_default_status(),
)
def whoop_sync_every_five_minutes(context):
    return skip_if_job_active(context, job_name="whoop_sync_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[whoop_sync],
        jobs=[whoop_sync_job],
        schedules=[whoop_sync_every_five_minutes],
    )
