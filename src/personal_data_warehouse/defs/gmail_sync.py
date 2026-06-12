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

from personal_data_warehouse.config import (
    GmailAccount,
    Settings,
    load_settings,
)
from personal_data_warehouse.warehouse import warehouse_from_settings
from personal_data_warehouse.gmail_sync import (
    GMAIL_ATTACHMENT_STORAGE_KIND,
    GMAIL_ATTACHMENT_STORAGE_METADATA_KIND,
    GMAIL_ATTACHMENT_STORAGE_SOURCE,
    GmailSyncRunner,
)
from personal_data_warehouse.objectstore import ObjectStore, build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress


def build_attachment_object_store_factory(*, settings: Settings, logger):
    if not settings.gmail_attachment_storage_enabled:
        logger.info(
            "Gmail attachment blob storage is disabled "
            "(set GMAIL_ATTACHMENT_GOOGLE_DRIVE_FOLDER_ID or VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID to enable)"
        )
        return None

    folder_id = settings.gmail_attachment_google_drive_folder_id
    drive_account = settings.gmail_attachment_google_drive_account

    def factory(account: GmailAccount) -> ObjectStore:
        upload_account = drive_account or account.email_address
        return build_object_store(
            google_drive_spec(
                folder_id=folder_id,
                account=upload_account,
                source=GMAIL_ATTACHMENT_STORAGE_SOURCE,
                blob_kind=GMAIL_ATTACHMENT_STORAGE_KIND,
                metadata_kind=GMAIL_ATTACHMENT_STORAGE_METADATA_KIND,
            ),
            settings=settings,
        )

    logger.info(
        "Gmail attachment blob storage is enabled via Google Drive folder %s (upload account: %s)",
        folder_id,
        drive_account or "<source mailbox>",
    )
    return factory


@asset(
    group_name="gmail",
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
def gmail_mailbox_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail_client_secrets=False)
    attachment_object_store_factory = build_attachment_object_store_factory(
        settings=settings,
        logger=context.log,
    )
    warehouse = warehouse_from_settings(settings)
    summaries = GmailSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=context.log,
        attachment_object_store_factory=attachment_object_store_factory,
    ).sync_all()

    return MaterializeResult(
        metadata={
            "mailboxes": MetadataValue.json(
                [
                    {
                        "account": summary.account,
                        "sync_type": summary.sync_type,
                        "next_history_id": summary.next_history_id,
                        "messages_written": summary.messages_written,
                        "deleted_messages": summary.deleted_messages,
                        "attachments_written": summary.attachments_written,
                        "attachments_stored": summary.attachments_stored,
                        "attachment_text_chars": summary.attachment_text_chars,
                        "attachment_backfill_candidates": summary.attachment_backfill_candidates,
                        "attachment_backfill_rows_written": summary.attachment_backfill_rows_written,
                        "query": summary.query,
                    }
                    for summary in summaries
                ]
            ),
            "mailbox_count": len(summaries),
            "messages_written": sum(summary.messages_written for summary in summaries),
            "deleted_messages": sum(summary.deleted_messages for summary in summaries),
            "attachments_written": sum(summary.attachments_written for summary in summaries),
            "attachments_stored": sum(summary.attachments_stored for summary in summaries),
            "attachment_text_chars": sum(summary.attachment_text_chars for summary in summaries),
            "attachment_backfill_candidates": sum(
                summary.attachment_backfill_candidates for summary in summaries
            ),
            "attachment_backfill_rows_written": sum(
                summary.attachment_backfill_rows_written for summary in summaries
            ),
        }
    )


gmail_mailbox_sync_job = define_asset_job(
    "gmail_mailbox_sync_job",
    selection=[gmail_mailbox_sync],
)


# A full mailbox sync takes ~9 minutes, so an every-minute cadence ran it
# effectively back-to-back and continuously. The skip_if_job_in_progress guard
# already prevents overlapping runs, but spacing the cron to every 15 minutes
# guarantees a real idle gap between runs, draining sustained CPU/memory pressure
# on the host (which was thrashing swap and starving the Dagster gRPC heartbeat).
@schedule(
    cron_schedule="*/15 * * * *",
    job=gmail_mailbox_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def gmail_mailbox_sync_every_fifteen_minutes(context):
    return skip_if_job_in_progress(context, job_name="gmail_mailbox_sync_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[gmail_mailbox_sync],
        jobs=[gmail_mailbox_sync_job],
        schedules=[gmail_mailbox_sync_every_fifteen_minutes],
    )
