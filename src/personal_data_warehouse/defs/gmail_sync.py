from __future__ import annotations

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
from dotenv import load_dotenv

from personal_data_warehouse.config import (
    DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_BASE_URL,
    DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_TIMEOUT_SECONDS,
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
    attachment_ai_fallback_config_from_settings,
)
from personal_data_warehouse.ollama_resource import OllamaResource
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse_voice_memos.cli import build_google_drive_service
from personal_data_warehouse_voice_memos.google_drive_storage import GoogleDriveObjectStore
from personal_data_warehouse_voice_memos.storage import ObjectStore


def ollama_resource_from_env() -> OllamaResource:
    load_dotenv()
    return OllamaResource(
        base_url=os.getenv("GMAIL_ATTACHMENT_AI_FALLBACK_BASE_URL")
        or DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_BASE_URL,
        request_timeout_seconds=int(
            os.getenv(
                "GMAIL_ATTACHMENT_AI_FALLBACK_TIMEOUT_SECONDS",
                str(DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_TIMEOUT_SECONDS),
            )
        ),
    )


def prepare_attachment_ai_fallback(*, settings, ollama: OllamaResource, logger):
    config = attachment_ai_fallback_config_from_settings(settings, client=ollama)
    if config is None:
        logger.info("Gmail attachment AI fallback is disabled")
        return None
    try:
        ollama.ensure_model(config.model, pull=config.pull_model)
    except Exception as exc:
        logger.warning(
            "Gmail attachment AI fallback is enabled but %s model %s is not ready at %s: %s",
            config.provider,
            config.model,
            config.base_url,
            exc,
        )
        return None
    logger.info(
        "Gmail attachment AI fallback is ready via %s model %s at %s",
        config.provider,
        config.model,
        config.base_url,
    )
    return config


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
        service = build_google_drive_service(account=upload_account, settings=settings)
        return GoogleDriveObjectStore(
            folder_id=folder_id,
            service=service,
            source=GMAIL_ATTACHMENT_STORAGE_SOURCE,
            legacy_sources=(),
            audio_kind=GMAIL_ATTACHMENT_STORAGE_KIND,
            metadata_kind=GMAIL_ATTACHMENT_STORAGE_METADATA_KIND,
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
def gmail_mailbox_sync(context, ollama: OllamaResource) -> MaterializeResult:
    settings = load_settings(require_gmail_client_secrets=False)
    attachment_ai_fallback = prepare_attachment_ai_fallback(
        settings=settings,
        ollama=ollama,
        logger=context.log,
    )
    attachment_object_store_factory = build_attachment_object_store_factory(
        settings=settings,
        logger=context.log,
    )
    warehouse = warehouse_from_settings(settings)
    summaries = GmailSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=context.log,
        attachment_ai_fallback=attachment_ai_fallback,
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


@schedule(
    cron_schedule="* * * * *",
    job=gmail_mailbox_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def gmail_mailbox_sync_every_minute(context):
    return skip_if_job_in_progress(context, job_name="gmail_mailbox_sync_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[gmail_mailbox_sync],
        jobs=[gmail_mailbox_sync_job],
        schedules=[gmail_mailbox_sync_every_minute],
        resources={"ollama": ollama_resource_from_env()},
    )
