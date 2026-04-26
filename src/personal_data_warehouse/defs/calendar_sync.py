from __future__ import annotations

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

from personal_data_warehouse.calendar_sync import CalendarSyncRunner
from personal_data_warehouse.clickhouse import ClickHouseWarehouse
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.sync_locks import exclusive_sync_lock

CALENDAR_SYNC_POSTGRES_LOCK_ID = 7_403_111_838


@asset(
    group_name="calendar",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def calendar_event_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_calendar=True)
    warehouse = ClickHouseWarehouse(settings.clickhouse_url or "")
    with exclusive_sync_lock(name="calendar", postgres_lock_id=CALENDAR_SYNC_POSTGRES_LOCK_ID) as acquired:
        if not acquired:
            context.log.warning("Skipping Calendar sync because another Calendar sync is already running")
            summaries = []
        else:
            summaries = CalendarSyncRunner(
                settings=settings,
                warehouse=warehouse,
                logger=context.log,
            ).sync_all()

    return MaterializeResult(
        metadata={
            "calendars": MetadataValue.json(
                [
                    {
                        "account": summary.account,
                        "calendar_id": summary.calendar_id,
                        "sync_type": summary.sync_type,
                        "events_written": summary.events_written,
                        "deleted_events": summary.deleted_events,
                    }
                    for summary in summaries
                ]
            ),
            "calendar_count": len(summaries),
            "events_written": sum(summary.events_written for summary in summaries),
            "deleted_events": sum(summary.deleted_events for summary in summaries),
        }
    )


calendar_event_sync_job = define_asset_job(
    "calendar_event_sync_job",
    selection=[calendar_event_sync],
)


@schedule(
    cron_schedule="* * * * *",
    job=calendar_event_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def calendar_event_sync_every_minute():
    return {}


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[calendar_event_sync],
        jobs=[calendar_event_sync_job],
        schedules=[calendar_event_sync_every_minute],
    )
