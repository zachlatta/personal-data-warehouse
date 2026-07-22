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

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.plaid_sync import PlaidSyncRunner
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

PLAID_SYNC_POSTGRES_LOCK_ID = 7_403_111_847


@asset(
    group_name="plaid",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def plaid_finance_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_plaid=True)
    if settings.plaid is None:
        raise ValueError("Plaid is not configured")
    warehouse = warehouse_from_settings(settings)
    try:
        with exclusive_sync_lock(name="plaid", postgres_lock_id=PLAID_SYNC_POSTGRES_LOCK_ID) as acquired:
            if not acquired:
                context.log.warning("Skipping Plaid sync because another Plaid sync is already running")
                summary = None
            else:
                summary = PlaidSyncRunner(
                    config=settings.plaid,
                    warehouse=warehouse,
                    logger=context.log,
                ).sync_all()
    finally:
        warehouse.close()

    summary_json = {} if summary is None else {
        "items": summary.items,
        "accounts": summary.accounts,
        "transactions": summary.transactions,
        "removed_transactions": summary.removed_transactions,
        "investment_securities": summary.investment_securities,
        "investment_holdings": summary.investment_holdings,
        "investment_transactions": summary.investment_transactions,
        "liabilities": summary.liabilities,
        # Non-zero means an Item needs `pdw ingest plaid link` re-run; the run
        # stays green because no retry can clear it. plaid.sync_state carries
        # which institution/product and why.
        "action_required": summary.action_required,
    }
    return MaterializeResult(
        metadata={
            "lock_acquired": acquired,
            "skipped_due_to_lock": not acquired,
            "summary": MetadataValue.json(summary_json),
            **summary_json,
        }
    )


plaid_finance_sync_job = define_asset_job(
    "plaid_finance_sync_job",
    selection=[plaid_finance_sync],
)


@schedule(
    cron_schedule="*/30 * * * *",
    job=plaid_finance_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def plaid_finance_sync_every_thirty_minutes(context):
    return skip_if_job_active(context, job_name="plaid_finance_sync_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[plaid_finance_sync],
        jobs=[plaid_finance_sync_job],
        schedules=[plaid_finance_sync_every_thirty_minutes],
    )
