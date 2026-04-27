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
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.slack_sync import SlackSyncRunner, SlackSyncSummary
from personal_data_warehouse.sync_locks import exclusive_sync_lock

SLACK_SYNC_POSTGRES_LOCK_ID = 7_403_111_837


def _rate_limit_budget_seconds() -> int:
    return _int_env("SLACK_ASSET_RATE_LIMIT_BUDGET_SECONDS", 120)


def run_slack_freshness_sync(*, settings, warehouse, logger) -> list[SlackSyncSummary]:
    summaries: list[SlackSyncSummary] = []

    for conversation_types, window_minutes, conversation_limit in [
        (("im",), _int_env("SLACK_ASSET_DM_WINDOW_MINUTES", 240), _int_env("SLACK_ASSET_DM_FRESHNESS_LIMIT", 500)),
        (("mpim",), _int_env("SLACK_ASSET_MPIM_WINDOW_MINUTES", 240), _int_env("SLACK_ASSET_MPIM_FRESHNESS_LIMIT", 250)),
        (
            ("private_channel",),
            _int_env("SLACK_ASSET_PRIVATE_WINDOW_MINUTES", 180),
            _int_env("SLACK_ASSET_PRIVATE_FRESHNESS_LIMIT", 100),
        ),
        (
            ("public_channel",),
            _int_env("SLACK_ASSET_PUBLIC_WINDOW_MINUTES", 120),
            _int_env("SLACK_ASSET_PUBLIC_FRESHNESS_LIMIT", 100),
        ),
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
                use_existing_conversations=True,
                conversation_types=conversation_types,
                conversation_limit=conversation_limit,
                sync_thread_replies=False,
                max_rate_limit_sleep_seconds=_rate_limit_budget_seconds(),
            ).sync_all()
        )

    if _bool_env("SLACK_ASSET_READ_STATE_WITH_FRESHNESS", True):
        summaries.extend(run_slack_read_state_sync(settings=settings, warehouse=warehouse, logger=logger))

    return summaries


def run_slack_coverage_sync(*, settings, warehouse, logger, now: datetime | None = None) -> list[SlackSyncSummary]:
    current_time = now or datetime.now(tz=UTC)
    summaries: list[SlackSyncSummary] = []
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
                max_rate_limit_sleep_seconds=_rate_limit_budget_seconds(),
            ).sync_all()
        )

    return summaries


def run_slack_metadata_sync(
    *,
    settings,
    warehouse,
    logger,
    now: datetime | None = None,
    respect_interval: bool = False,
) -> list[SlackSyncSummary]:
    current_time = now or datetime.now(tz=UTC)
    summaries: list[SlackSyncSummary] = []
    if respect_interval and current_time.minute % _int_env("SLACK_ASSET_METADATA_EVERY_MINUTES", 15) != 0:
        return summaries

    metadata_conversation_types = _metadata_conversation_types_for_time(current_time)
    summaries.extend(
        SlackSyncRunner(
            settings=settings,
            warehouse=warehouse,
            logger=logger,
            sync_users=False,
            sync_members=False,
            conversation_types=metadata_conversation_types,
            conversation_page_limit=_int_env("SLACK_ASSET_METADATA_CONVERSATION_PAGE_LIMIT", 1),
            sync_conversations_only=True,
            sync_thread_replies=False,
            max_rate_limit_sleep_seconds=_rate_limit_budget_seconds(),
        ).sync_all()
    )

    return summaries


def run_slack_user_sync(*, settings, warehouse, logger) -> list[SlackSyncSummary]:
    return SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logger,
        sync_users=True,
        sync_members=False,
        use_existing_conversations=True,
        conversation_limit=0,
        sync_thread_replies=False,
        max_rate_limit_sleep_seconds=_rate_limit_budget_seconds(),
    ).sync_all()


def run_slack_thread_sync(*, settings, warehouse, logger) -> list[SlackSyncSummary]:
    return SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logger,
        sync_users=False,
        sync_members=False,
        sync_thread_replies_only=True,
        skip_completed_threads=True,
        skip_known_errors=True,
        thread_order=os.getenv("SLACK_ASSET_THREAD_ORDER", "recent"),
        thread_limit=_int_env("SLACK_ASSET_THREAD_LIMIT", 1),
        thread_since_days=_int_env("SLACK_ASSET_THREAD_SINCE_DAYS", settings.slack_thread_audit_days),
        max_rate_limit_sleep_seconds=_rate_limit_budget_seconds(),
    ).sync_all()


def run_slack_read_state_sync(*, settings, warehouse, logger) -> list[SlackSyncSummary]:
    return SlackSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logger,
        sync_users=False,
        sync_members=False,
        sync_conversation_info_only=True,
        conversation_limit=_int_env("SLACK_ASSET_READ_STATE_LIMIT", 25),
        max_rate_limit_sleep_seconds=_rate_limit_budget_seconds(),
    ).sync_all()


def _metadata_conversation_types_for_time(now: datetime) -> tuple[str, ...]:
    stage = ((now.hour * 60) + now.minute) // _int_env("SLACK_ASSET_METADATA_EVERY_MINUTES", 15)
    return (
        ("im",),
        ("mpim",),
        ("private_channel",),
        ("public_channel",),
    )[stage % 4]


def run_intelligent_slack_sync(*, settings, warehouse, logger, now: datetime | None = None) -> list[SlackSyncSummary]:
    current_time = now or datetime.now(tz=UTC)
    summaries = [
        *run_slack_freshness_sync(settings=settings, warehouse=warehouse, logger=logger),
        *run_slack_coverage_sync(settings=settings, warehouse=warehouse, logger=logger, now=current_time),
        *run_slack_metadata_sync(settings=settings, warehouse=warehouse, logger=logger, now=current_time, respect_interval=True),
    ]
    if current_time.minute == 0:
        summaries.extend(run_slack_user_sync(settings=settings, warehouse=warehouse, logger=logger))
    return summaries


def _coverage_stage_for_time(now: datetime) -> dict[str, object] | None:
    stage = now.minute % 6
    if stage == 1:
        return {
            "conversation_types": ("mpim",),
            "archived_only": False,
            "zero_messages_only": False,
            "limit": _int_env("SLACK_ASSET_MPIM_COVERAGE_LIMIT", 50),
        }
    if stage == 2:
        return {
            "conversation_types": ("private_channel",),
            "archived_only": False,
            "zero_messages_only": False,
            "limit": _int_env("SLACK_ASSET_PRIVATE_COVERAGE_LIMIT", 25),
        }
    if stage == 3:
        return {
            "conversation_types": ("private_channel",),
            "archived_only": True,
            "zero_messages_only": False,
            "limit": _int_env("SLACK_ASSET_ARCHIVED_PRIVATE_COVERAGE_LIMIT", 10),
        }
    if stage == 4:
        return {
            "conversation_types": ("public_channel",),
            "archived_only": True,
            "zero_messages_only": True,
            "limit": _int_env("SLACK_ASSET_ARCHIVED_PUBLIC_ZERO_COVERAGE_LIMIT", 25),
        }
    if stage == 5:
        return {
            "conversation_types": ("public_channel",),
            "archived_only": True,
            "zero_messages_only": False,
            "limit": _int_env("SLACK_ASSET_ARCHIVED_PUBLIC_COVERAGE_LIMIT", 25),
        }
    return {
        "conversation_types": ("public_channel",),
        "archived_only": False,
        "zero_messages_only": False,
        "limit": _int_env("SLACK_ASSET_PUBLIC_COVERAGE_LIMIT", 25),
    }


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_sync(context) -> MaterializeResult:
    return _run_locked_slack_stage(
        context,
        stage_name="freshness",
        run_fn=run_slack_freshness_sync,
    )


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_coverage_sync(context) -> MaterializeResult:
    return _run_locked_slack_stage(
        context,
        stage_name="coverage",
        run_fn=run_slack_coverage_sync,
    )


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_metadata_sync(context) -> MaterializeResult:
    return _run_locked_slack_stage(
        context,
        stage_name="metadata",
        run_fn=run_slack_metadata_sync,
    )


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_user_sync(context) -> MaterializeResult:
    return _run_locked_slack_stage(
        context,
        stage_name="users",
        run_fn=run_slack_user_sync,
    )


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_thread_sync(context) -> MaterializeResult:
    return _run_locked_slack_stage(
        context,
        stage_name="threads",
        run_fn=run_slack_thread_sync,
    )


@asset(
    group_name="slack",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def slack_workspace_read_state_sync(context) -> MaterializeResult:
    return _run_locked_slack_stage(
        context,
        stage_name="read_state",
        run_fn=run_slack_read_state_sync,
    )


def _run_locked_slack_stage(context, *, stage_name: str, run_fn) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_slack=True)
    warehouse = ClickHouseWarehouse(settings.clickhouse_url or "")
    with exclusive_sync_lock(name="slack", postgres_lock_id=SLACK_SYNC_POSTGRES_LOCK_ID) as acquired:
        if not acquired:
            context.log.warning("Skipping Slack %s sync because another Slack sync is already running", stage_name)
            summaries = []
        else:
            summaries = run_fn(settings=settings, warehouse=warehouse, logger=context.log)

    return MaterializeResult(
        metadata={
            "sync_stage": stage_name,
            "lock_acquired": acquired,
            "skipped_due_to_lock": not acquired,
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

slack_workspace_coverage_sync_job = define_asset_job(
    "slack_workspace_coverage_sync_job",
    selection=[slack_workspace_coverage_sync],
)

slack_workspace_metadata_sync_job = define_asset_job(
    "slack_workspace_metadata_sync_job",
    selection=[slack_workspace_metadata_sync],
)

slack_workspace_user_sync_job = define_asset_job(
    "slack_workspace_user_sync_job",
    selection=[slack_workspace_user_sync],
)

slack_workspace_thread_sync_job = define_asset_job(
    "slack_workspace_thread_sync_job",
    selection=[slack_workspace_thread_sync],
)

slack_workspace_read_state_sync_job = define_asset_job(
    "slack_workspace_read_state_sync_job",
    selection=[slack_workspace_read_state_sync],
)


@schedule(
    cron_schedule="* * * * *",
    job=slack_workspace_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_sync_every_minute(context):
    return skip_if_job_active(context, job_name="slack_workspace_sync_job")


@schedule(
    cron_schedule="*/7 * * * *",
    job=slack_workspace_coverage_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_coverage_sync_every_seven_minutes(context):
    return skip_if_job_active(context, job_name="slack_workspace_coverage_sync_job")


@schedule(
    cron_schedule="*/15 * * * *",
    job=slack_workspace_metadata_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_metadata_sync_every_fifteen_minutes(context):
    return skip_if_job_active(context, job_name="slack_workspace_metadata_sync_job")


@schedule(
    cron_schedule="0 * * * *",
    job=slack_workspace_user_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_user_sync_hourly(context):
    return skip_if_job_active(context, job_name="slack_workspace_user_sync_job")


@schedule(
    cron_schedule="*/5 * * * *",
    job=slack_workspace_thread_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_thread_sync_every_five_minutes(context):
    return skip_if_job_active(context, job_name="slack_workspace_thread_sync_job")


@schedule(
    cron_schedule="2-59/5 * * * *",
    job=slack_workspace_read_state_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def slack_workspace_read_state_sync_every_five_minutes(context):
    return skip_if_job_active(context, job_name="slack_workspace_read_state_sync_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[
            slack_workspace_sync,
            slack_workspace_coverage_sync,
            slack_workspace_metadata_sync,
            slack_workspace_user_sync,
            slack_workspace_thread_sync,
            slack_workspace_read_state_sync,
        ],
        jobs=[
            slack_workspace_sync_job,
            slack_workspace_coverage_sync_job,
            slack_workspace_metadata_sync_job,
            slack_workspace_user_sync_job,
            slack_workspace_thread_sync_job,
            slack_workspace_read_state_sync_job,
        ],
        schedules=[
            slack_workspace_sync_every_minute,
            slack_workspace_coverage_sync_every_seven_minutes,
            slack_workspace_metadata_sync_every_fifteen_minutes,
            slack_workspace_user_sync_hourly,
            slack_workspace_thread_sync_every_five_minutes,
            slack_workspace_read_state_sync_every_five_minutes,
        ],
    )
