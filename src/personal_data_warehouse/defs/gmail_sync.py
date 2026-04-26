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
from personal_data_warehouse.gmail_sync import GmailSyncRunner
from personal_data_warehouse.schedule_guards import skip_if_job_active


@asset(
    group_name="gmail",
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
def gmail_mailbox_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail_client_secrets=False)
    warehouse = ClickHouseWarehouse(settings.clickhouse_url or "")
    summaries = GmailSyncRunner(settings=settings, warehouse=warehouse, logger=context.log).sync_all()

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


@schedule(
    cron_schedule="* * * * *",
    job=gmail_mailbox_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def gmail_mailbox_sync_every_minute(context):
    return skip_if_job_active(context, job_name="gmail_mailbox_sync_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[gmail_mailbox_sync],
        jobs=[gmail_mailbox_sync_job],
        schedules=[gmail_mailbox_sync_every_minute],
    )
