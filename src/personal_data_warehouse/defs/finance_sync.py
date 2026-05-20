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
from personal_data_warehouse.finance_sync import FinanceSyncRunner
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

FINANCE_SYNC_POSTGRES_LOCK_ID = 7_403_111_839


@asset(
    group_name="finance",
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def finance_account_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_finance=False)
    warehouse = warehouse_from_settings(settings)
    if settings.plaid is None:
        context.log.warning("Skipping Finance sync because Plaid finance sync is not configured")
        warehouse.ensure_finance_tables()
        summaries = []
        return _finance_materialize_result(
            summaries=summaries,
            lock_acquired=False,
            skipped_due_to_lock=False,
            skipped_due_to_config=True,
        )

    with exclusive_sync_lock(name="finance", postgres_lock_id=FINANCE_SYNC_POSTGRES_LOCK_ID) as acquired:
        if not acquired:
            context.log.warning("Skipping Finance sync because another Finance sync is already running")
            summaries = []
        else:
            summaries = FinanceSyncRunner(
                settings=settings,
                warehouse=warehouse,
                logger=context.log,
            ).sync_all()

    return _finance_materialize_result(
        summaries=summaries,
        lock_acquired=acquired,
        skipped_due_to_lock=not acquired,
        skipped_due_to_config=False,
    )


def _finance_materialize_result(
    *,
    summaries,
    lock_acquired: bool,
    skipped_due_to_lock: bool,
    skipped_due_to_config: bool,
) -> MaterializeResult:
    return MaterializeResult(
        metadata={
            "lock_acquired": lock_acquired,
            "skipped_due_to_lock": skipped_due_to_lock,
            "skipped_due_to_config": skipped_due_to_config,
            "items": MetadataValue.json(
                [
                    {
                        "item_name": summary.item_name,
                        "item_id": summary.item_id,
                        "institution_name": summary.institution_name,
                        "accounts_written": summary.accounts_written,
                        "transactions_added": summary.transactions_added,
                        "transactions_modified": summary.transactions_modified,
                        "transactions_removed": summary.transactions_removed,
                        "investment_holdings_written": summary.investment_holdings_written,
                        "investment_securities_written": summary.investment_securities_written,
                        "investment_transactions_written": summary.investment_transactions_written,
                        "liabilities_written": summary.liabilities_written,
                    }
                    for summary in summaries
                ]
            ),
            "item_count": len(summaries),
            "accounts_written": sum(summary.accounts_written for summary in summaries),
            "transactions_written": sum(
                summary.transactions_added + summary.transactions_modified + summary.transactions_removed
                for summary in summaries
            ),
            "investment_holdings_written": sum(summary.investment_holdings_written for summary in summaries),
            "investment_transactions_written": sum(
                summary.investment_transactions_written for summary in summaries
            ),
            "liabilities_written": sum(summary.liabilities_written for summary in summaries),
        }
    )


finance_account_sync_job = define_asset_job(
    "finance_account_sync_job",
    selection=[finance_account_sync],
)


@schedule(
    cron_schedule="0 * * * *",
    job=finance_account_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def finance_account_sync_hourly(context):
    return skip_if_job_active(context, job_name="finance_account_sync_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[finance_account_sync],
        jobs=[finance_account_sync_job],
        schedules=[finance_account_sync_hourly],
    )
