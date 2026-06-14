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

from personal_data_warehouse.whatsapp_drive_ingest import (
    WhatsAppDriveIngestRunner,
    has_batch_payloads,
    iter_batch_payloads,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

WHATSAPP_DRIVE_INGEST_POSTGRES_LOCK_ID = 8_407_112_441
WHATSAPP_SENSOR_INTERVAL_SECONDS = 60


def _whatsapp_object_store(settings):
    return build_object_store(
        google_drive_spec(
            folder_id=settings.whatsapp.google_drive_folder_id,
            account=settings.whatsapp.google_drive_account,
            source="whatsapp",
            blob_kind="whatsapp_media_item",
            metadata_kind="whatsapp_export_batch",
        ),
        settings=settings,
    )


@asset(
    group_name="whatsapp",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def whatsapp_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_whatsapp=True)
    if settings.whatsapp is None:
        raise RuntimeError("WhatsApp sync is not configured")
    warehouse = warehouse_from_settings(settings)

    with exclusive_sync_lock(
        name="whatsapp_drive_ingest",
        postgres_lock_id=WHATSAPP_DRIVE_INGEST_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping WhatsApp Drive ingest because another run is already active")
            summary = None
        else:
            object_store = _whatsapp_object_store(settings)
            summary = WhatsAppDriveIngestRunner(
                warehouse=warehouse,
                batch_source=lambda: iter_batch_payloads(object_store=object_store),
                object_store_factory=lambda: _whatsapp_object_store(settings),
                promotion_workers=int(os.getenv("WHATSAPP_DRIVE_INGEST_PROMOTION_WORKERS", "8")),
                logger=context.log,
            ).sync()

    return MaterializeResult(
        metadata={
            "batches_seen": MetadataValue.int(summary.batches_seen if summary else 0),
            "chats_written": MetadataValue.int(summary.chats_written if summary else 0),
            "contacts_written": MetadataValue.int(summary.contacts_written if summary else 0),
            "messages_written": MetadataValue.int(summary.messages_written if summary else 0),
            "media_items_written": MetadataValue.int(summary.media_items_written if summary else 0),
            "files_promoted": MetadataValue.int(summary.files_promoted if summary else 0),
        }
    )


whatsapp_drive_ingest_job = define_asset_job(
    "whatsapp_drive_ingest_job",
    selection=[whatsapp_drive_ingest],
)


@sensor(
    job=whatsapp_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=WHATSAPP_SENSOR_INTERVAL_SECONDS,
)
def whatsapp_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="whatsapp_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active

    try:
        settings = load_settings(require_gmail=False, require_whatsapp=True)
    except ValueError as exc:
        return SkipReason(f"WhatsApp is not configured: {exc}")
    if settings.whatsapp is None:
        raise RuntimeError("WhatsApp sync is not configured")
    if not has_batch_payloads(object_store=_whatsapp_object_store(settings), stage="inbox"):
        return SkipReason("No WhatsApp inbox batches found in object storage.")

    return RunRequest(tags={"whatsapp_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[whatsapp_drive_ingest],
        jobs=[whatsapp_drive_ingest_job],
        sensors=[whatsapp_drive_inbox_sensor],
    )
