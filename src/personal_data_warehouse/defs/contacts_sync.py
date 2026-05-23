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
from personal_data_warehouse.contacts_sync import ContactsSyncRunner
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

CONTACTS_SYNC_POSTGRES_LOCK_ID = 7_403_111_842


@asset(
    group_name="contacts",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def google_contacts_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_contacts=True)
    warehouse = warehouse_from_settings(settings)
    with exclusive_sync_lock(name="contacts", postgres_lock_id=CONTACTS_SYNC_POSTGRES_LOCK_ID) as acquired:
        if not acquired:
            context.log.warning("Skipping Google Contacts sync because another Contacts sync is already running")
            summaries = []
        else:
            summaries = ContactsSyncRunner(
                settings=settings,
                warehouse=warehouse,
                logger=context.log,
            ).sync_all()

    return MaterializeResult(
        metadata={
            "lock_acquired": acquired,
            "skipped_due_to_lock": not acquired,
            "accounts": MetadataValue.json(
                [
                    {
                        "account": summary.account,
                        "source": summary.source,
                        "source_kind": summary.source_kind,
                        "address_book_id": summary.address_book_id,
                        "sync_type": summary.sync_type,
                        "cards_written": summary.cards_written,
                        "deleted_cards": summary.deleted_cards,
                        "tombstones_written": summary.tombstones_written,
                    }
                    for summary in summaries
                ]
            ),
            "account_count": len(summaries),
            "cards_written": sum(summary.cards_written for summary in summaries),
            "deleted_cards": sum(summary.deleted_cards for summary in summaries),
            "tombstones_written": sum(summary.tombstones_written for summary in summaries),
        }
    )


contacts_sync_job = define_asset_job(
    "contacts_sync_job",
    selection=[google_contacts_sync],
)


@schedule(
    cron_schedule="0 * * * *",
    job=contacts_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def contacts_sync_hourly(context):
    return skip_if_job_active(context, job_name="contacts_sync_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[google_contacts_sync],
        jobs=[contacts_sync_job],
        schedules=[contacts_sync_hourly],
    )
