from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
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

from personal_data_warehouse.clickhouse import ClickHouseWarehouse
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.slack_sync import SlackSyncRunner, SlackSyncSummary
from personal_data_warehouse.sync_locks import exclusive_sync_lock

SLACK_SYNC_POSTGRES_LOCK_ID = 7_403_111_837


def run_intelligent_slack_sync(*, settings, warehouse, logger, now: datetime | None = None) -> list[SlackSyncSummary]:
    current_time = now or datetime.now(tz=UTC)
    summaries: list[SlackSyncSummary] = []

    for conversation_types, window_minutes in [
        (("im",), _int_env("SLACK_ASSET_DM_WINDOW_MINUTES", 240)),
        (("mpim",), _int_env("SLACK_ASSET_MPIM_WINDOW_MINUTES", 240)),
        (("private_channel",), _int_env("SLACK_ASSET_PRIVATE_WINDOW_MINUTES", 180)),
        (("public_channel",), _int_env("SLACK_ASSET_PUBLIC_WINDOW_MINUTES", 120)),
    ]:
        summaries.extend(
            SlackSyncRunner(
                settings=settings,
                warehouse=warehouse,
                logger=logger,
                history_window=timedelta(minutes=window_minutes),
                sync_users=False,
                sync_members=False,
                freshness_priority=True,
                conversation_types=conversation_types,
                sync_thread_replies=False,
            ).sync_all()
        )

    coverage = _coverage_stage_for_time(current_time)
    if coverage is not None:
        coverage_settings = replace(settings, slack_force_full_sync=True)
        summaries.extend(
            SlackSyncRunner(
                settings=coverage_settings,
                warehouse=warehouse,
                logger=logger,
                sync_users=False,
                sync_members=False,
                use_existing_conversations=True,
                archived_only=coverage["archived_only"],
                conversation_types=coverage["conversation_types"],
                not_full_only=True,
                zero_messages_only=coverage["zero_messages_only"],
                skip_known_errors=True,
                conversation_limit=coverage["limit"],
                sync_thread_replies=False,
            ).sync_all()
        )

    if current_time.minute % _int_env("SLACK_ASSET_METADATA_EVERY_MINUTES", 15) == 0:
        summaries.extend(
            SlackSyncRunner(
                settings=settings,
                warehouse=warehouse,
                logger=logger,
                sync_users=True,
                sync_members=False,
                use_existing_conversations=True,
                conversation_limit=0,
                sync_thread_replies=False,
            ).sync_all()
        )

    return summaries


def _coverage_stage_for_time(now: datetime) -> dict[str, object] | None:
    stage = now.minute % 6
    if stage == 1:
        return {
            "conversation_types": ("mpim",),
            "archived_only": False,
            "zero_messages_only": False,
            "limit": _int_env("SLACK_ASSET_MPIM_COVERAGE_LIMIT", 500),
        }
    if stage == 2:
        return {
            "conversation_types": ("private_channel",),
            "archived_only": False,
            "zero_messages_only": False,
            "limit": _int_env("SLACK_ASSET_PRIVATE_COVERAGE_LIMIT", 100),
        }
    if stage == 3:
        return {
            "conversation_types": ("private_channel",),
            "archived_only": True,
            "zero_messages_only": False,
            "limit": _int_env("SLACK_ASSET_ARCHIVED_PRIVATE_COVERAGE_LIMIT", 100),
        }
    if stage == 4:
        return {
            "conversation_types": ("public_channel",),
            "archived_only": True,
            "zero_messages_only": True,
            "limit": _int_env("SLACK_ASSET_ARCHIVED_PUBLIC_ZERO_COVERAGE_LIMIT", 200),
        }
    if stage == 5:
        return {
            "conversation_types": ("public_channel",),
            "archived_only": True,
            "zero_messages_only": False,
            "limit": _int_env("SLACK_ASSET_ARCHIVED_PUBLIC_COVERAGE_LIMIT", 200),
        }
    return {
        "conversation_types": ("public_channel",),
        "archived_only": False,
        "zero_messages_only": False,
        "limit": _int_env("SLACK_ASSET_PUBLIC_COVERAGE_LIMIT", 200),
    }


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


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
            summaries = run_intelligent_slack_sync(settings=settings, warehouse=warehouse, logger=context.log)

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
