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

from personal_data_warehouse.clickhouse import ClickHouseWarehouse
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.slack_sync import SlackSyncRunner
from personal_data_warehouse.sync_locks import exclusive_sync_lock

SLACK_SYNC_POSTGRES_LOCK_ID = 7_403_111_837


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_slack=True)
    warehouse = ClickHouseWarehouse(settings.clickhouse_url or "")
    with exclusive_sync_lock(name="slack", postgres_lock_id=SLACK_SYNC_POSTGRES_LOCK_ID) as acquired:
        if not acquired:
            context.log.warning("Skipping Slack sync because another Slack sync is already running")
            summaries = []
        else:
            summaries = SlackSyncRunner(settings=settings, warehouse=warehouse, logger=context.log).sync_all()

    return MaterializeResult(
        metadata={
            "workspaces": MetadataValue.json(
                [
                    {
                        "account": summary.account,
                        "team_id": summary.team_id,
                        "sync_type": summary.sync_type,
                        "conversations_seen": summary.conversations_seen,
                        "messages_written": summary.messages_written,
                        "users_written": summary.users_written,
                        "files_written": summary.files_written,
                    }
                    for summary in summaries
                ]
            ),
            "workspace_count": len(summaries),
            "messages_written": sum(summary.messages_written for summary in summaries),
            "files_written": sum(summary.files_written for summary in summaries),
        }
    )


slack_workspace_sync_job = define_asset_job(
    "slack_workspace_sync_job",
    selection=[slack_workspace_sync],
)


@schedule(
    cron_schedule="* * * * *",
    job=slack_workspace_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_sync_every_minute():
    return {}


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[slack_workspace_sync],
        jobs=[slack_workspace_sync_job],
        schedules=[slack_workspace_sync_every_minute],
    )
