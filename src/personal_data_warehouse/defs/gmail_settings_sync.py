from __future__ import annotations

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    asset,
    define_asset_job,
    definitions,
    schedule,
)

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.gmail_settings_sync import GmailSettingsSyncRunner
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

GMAIL_SETTINGS_SYNC_POSTGRES_LOCK_ID = 7_403_111_877


@asset(group_name="gmail")
def gmail_settings_sync(context) -> MaterializeResult:
    """Snapshot every mailbox's labels and filters into base_gmail.

    The upstream mutation worker also runs this inline for the accounts whose
    labels or filters it just changed, so a review opened right after an
    approved change already reads the new state; the schedule catches edits
    Zach makes in Gmail himself.
    """
    settings = load_settings(require_gmail_client_secrets=False)
    warehouse = warehouse_from_settings(settings)
    summary = None
    try:
        warehouse.ensure_tables()
        with exclusive_sync_lock(
            name="gmail_settings", postgres_lock_id=GMAIL_SETTINGS_SYNC_POSTGRES_LOCK_ID
        ) as acquired:
            if acquired:
                summary = GmailSettingsSyncRunner.from_settings(
                    settings=settings, warehouse=warehouse, logger=context.log
                ).run()
            else:
                context.log.warning("Skipping Gmail settings sync because another one is running")
    finally:
        warehouse.close()
    if summary is not None and summary.failed_accounts:
        # Recorded per account in ops.gmail_settings_sync_state first; the run
        # goes red so a lost grant is not a green schedule.
        raise RuntimeError(f"Gmail settings sync failed for {', '.join(summary.failed_accounts)}")
    summary_json = {} if summary is None else {
        "accounts_synced": summary.accounts_synced,
        "labels": summary.labels,
        "filters": summary.filters,
        "labels_deleted": summary.labels_deleted,
        "filters_deleted": summary.filters_deleted,
    }
    return MaterializeResult(metadata={"summary": MetadataValue.json(summary_json), **summary_json})


gmail_settings_sync_job = define_asset_job(
    "gmail_settings_sync_job",
    selection=[gmail_settings_sync],
    # Two API calls per mailbox; well under the global 4-hour run cap
    # (tests/test_dagster_job_runtime_caps.py).
    tags={"dagster/max_runtime": "900"},
)


@schedule(
    cron_schedule="13,43 * * * *",
    job=gmail_settings_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def gmail_settings_sync_schedule(context):
    return skip_if_job_active(context, job_name="gmail_settings_sync_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[gmail_settings_sync],
        jobs=[gmail_settings_sync_job],
        schedules=[gmail_settings_sync_schedule],
    )
