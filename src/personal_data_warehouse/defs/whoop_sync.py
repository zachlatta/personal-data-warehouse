from __future__ import annotations

import os

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    SkipReason,
    asset,
    define_asset_job,
    definitions,
    schedule,
)

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings
from personal_data_warehouse.whoop_sync import (
    WhoopSyncRunner,
    public_whoop_sync_summary,
    select_whoop_token_json,
    whoop_reauthorization_skip_reason,
)

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
            "action_required": sum(summary.action_required for summary in summaries),
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
    active = skip_if_job_active(context, job_name="whoop_sync_job")
    if isinstance(active, SkipReason):
        return active

    # All WHOOP endpoints share one token. Once the current token has been
    # rejected permanently, do not launch another green no-op run every five
    # minutes. The stored fingerprint means a replacement token immediately
    # clears this guard without a manual state reset.
    try:
        settings = load_settings(require_gmail=False, require_whoop=True)
        if settings.whoop is None:
            return active
        warehouse = warehouse_from_settings(settings)
        try:
            state = warehouse.load_whoop_sync_state()
            stored_token_json = warehouse.load_whoop_oauth_token(
                account=settings.whoop.account
            )
        finally:
            warehouse.close()
        runtime_token_json = select_whoop_token_json(
            bootstrap_token_json=settings.whoop.token_json,
            stored_token_json=stored_token_json,
            state_by_key=state,
            account=settings.whoop.account,
        )
        reason = whoop_reauthorization_skip_reason(
            state,
            account=settings.whoop.account,
            token_json=runtime_token_json,
        )
    except Exception as exc:
        # A fresh schema may not have the fingerprint column until the first
        # asset run ensures it. Let that run proceed; the asset remains the
        # fail-loud authority for unexpected configuration/database errors.
        context.log.warning("WHOOP credential guard unavailable: %s", exc)
        return active
    return active if reason is None else SkipReason(reason)


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[whoop_sync],
        jobs=[whoop_sync_job],
        schedules=[whoop_sync_every_five_minutes],
    )
