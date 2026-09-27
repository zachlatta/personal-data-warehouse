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
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.simplefin_sync import SimpleFINSyncRunner
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

SIMPLEFIN_SYNC_POSTGRES_LOCK_ID = 7_403_111_871


@asset(
    group_name="simplefin",
    retry_policy=RetryPolicy(max_retries=2, delay=120),
)
def simplefin_finance_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_simplefin=True)
    if settings.simplefin is None:
        raise ValueError("SimpleFIN is not configured")
    warehouse = warehouse_from_settings(settings)
    try:
        with exclusive_sync_lock(name="simplefin", postgres_lock_id=SIMPLEFIN_SYNC_POSTGRES_LOCK_ID) as acquired:
            if not acquired:
                context.log.warning("Skipping SimpleFIN sync because another SimpleFIN sync is already running")
                summary = None
            else:
                summary = SimpleFINSyncRunner(
                    config=settings.simplefin,
                    warehouse=warehouse,
                    logger=context.log,
                ).sync_all()
    finally:
        warehouse.close()

    summary_json = {} if summary is None else {
        "accounts": summary.accounts,
        "transactions": summary.transactions,
        "removed_transactions": summary.removed_transactions,
        "holdings": summary.holdings,
        "requests": summary.requests,
        # Non-zero means the access URL was refused: claim a new setup token
        # and set SIMPLEFIN_ACCESS_URL. The run stays green because no retry
        # can clear it; ops.simplefin_sync_state carries the verdict.
        "action_required": summary.action_required,
        # The bridge's own "connection needs attention" messages: the repair
        # is a re-login inside the SimpleFIN Bridge, not here.
        "attention": summary.attention,
    }
    return MaterializeResult(
        metadata={
            "lock_acquired": acquired,
            "skipped_due_to_lock": not acquired,
            "summary": MetadataValue.json(summary_json),
            **summary_json,
        }
    )


simplefin_finance_sync_job = define_asset_job(
    "simplefin_finance_sync_job",
    selection=[simplefin_finance_sync],
    # Well under the global 4-hour run-monitoring cap (see
    # tests/test_dagster_job_runtime_caps.py): a normal run is one request.
    tags={"dagster/max_runtime": "1800"},
)


@schedule(
    # The bridge refreshes each institution about once a day; hourly keeps a
    # balance at most an hour behind the bridge without hammering it. Offset
    # from Plaid's */30 so the two provider pulls and the ledger don't stack.
    cron_schedule="7 * * * *",
    job=simplefin_finance_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def simplefin_finance_sync_hourly(context):
    return skip_if_job_active(context, job_name="simplefin_finance_sync_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[simplefin_finance_sync],
        jobs=[simplefin_finance_sync_job],
        schedules=[simplefin_finance_sync_hourly],
    )
