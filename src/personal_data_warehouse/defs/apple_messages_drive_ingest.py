from __future__ import annotations

import os

from dagster import (
    DefaultSensorStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RunRequest,
    RetryPolicy,
    SkipReason,
    asset,
    define_asset_job,
    definitions,
    sensor,
)

from personal_data_warehouse.apple_messages_drive_ingest import (
    AppleMessagesDriveIngestRunner,
    GoogleDriveAppleMessagesPromoter,
    build_google_drive_service,
    has_drive_batch_payloads,
    iter_drive_batch_payloads,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

APPLE_MESSAGES_DRIVE_INGEST_POSTGRES_LOCK_ID = 8_407_112_440
APPLE_MESSAGES_SENSOR_INTERVAL_SECONDS = 60


@asset(
    group_name="apple_messages",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def apple_messages_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_apple_messages=True)
    if settings.apple_messages is None:
        raise RuntimeError("Apple Messages sync is not configured")
    warehouse = warehouse_from_settings(settings)

    with exclusive_sync_lock(
        name="apple_messages_drive_ingest",
        postgres_lock_id=APPLE_MESSAGES_DRIVE_INGEST_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Apple Messages Drive ingest because another run is already active")
            summary = None
        else:
            service = build_google_drive_service(account=settings.apple_messages.google_drive_account, settings=settings)
            summary = AppleMessagesDriveIngestRunner(
                warehouse=warehouse,
                batch_source=lambda: iter_drive_batch_payloads(
                    service=service,
                    folder_id=settings.apple_messages.google_drive_folder_id,
                ),
                promoter_factory=lambda: GoogleDriveAppleMessagesPromoter(
                    service=build_google_drive_service(
                        account=settings.apple_messages.google_drive_account,
                        settings=settings,
                    ),
                    folder_id=settings.apple_messages.google_drive_folder_id,
                ),
                promotion_workers=int(os.getenv("APPLE_MESSAGES_DRIVE_INGEST_PROMOTION_WORKERS", "8")),
                logger=context.log,
            ).sync()

    return MaterializeResult(
        metadata={
            "batches_seen": MetadataValue.int(summary.batches_seen if summary else 0),
            "handles_written": MetadataValue.int(summary.handles_written if summary else 0),
            "chats_written": MetadataValue.int(summary.chats_written if summary else 0),
            "chat_handles_written": MetadataValue.int(summary.chat_handles_written if summary else 0),
            "messages_written": MetadataValue.int(summary.messages_written if summary else 0),
            "chat_messages_written": MetadataValue.int(summary.chat_messages_written if summary else 0),
            "attachments_written": MetadataValue.int(summary.attachments_written if summary else 0),
            "files_promoted": MetadataValue.int(summary.files_promoted if summary else 0),
        }
    )


apple_messages_drive_ingest_job = define_asset_job(
    "apple_messages_drive_ingest_job",
    selection=[apple_messages_drive_ingest],
)


@sensor(
    job=apple_messages_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=APPLE_MESSAGES_SENSOR_INTERVAL_SECONDS,
)
def apple_messages_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="apple_messages_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active

    settings = load_settings(require_gmail=False, require_apple_messages=True)
    if settings.apple_messages is None:
        raise RuntimeError("Apple Messages sync is not configured")
    service = build_google_drive_service(account=settings.apple_messages.google_drive_account, settings=settings)
    if not has_drive_batch_payloads(
        service=service,
        folder_id=settings.apple_messages.google_drive_folder_id,
        stage="inbox",
    ):
        return SkipReason("No Apple Messages inbox batches found in Google Drive.")

    return RunRequest(tags={"apple_messages_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[apple_messages_drive_ingest],
        jobs=[apple_messages_drive_ingest_job],
        sensors=[apple_messages_drive_inbox_sensor],
    )
