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

from personal_data_warehouse.apple_notes_drive_ingest import (
    AppleNotesDriveIngestRunner,
    has_metadata_payloads,
    iter_metadata_payloads,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

APPLE_NOTES_DRIVE_INGEST_POSTGRES_LOCK_ID = 8_407_112_439
APPLE_NOTES_SENSOR_INTERVAL_SECONDS = 60


def _apple_notes_object_store(settings):
    return build_object_store(
        google_drive_spec(
            folder_id=settings.apple_notes.google_drive_folder_id,
            account=settings.apple_notes.google_drive_account,
            source="apple_notes",
            blob_kind="apple_note_body_html",
            metadata_kind="apple_note_revision_metadata",
        ),
        settings=settings,
    )


@asset(
    group_name="apple_notes",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def apple_notes_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_apple_notes=True)
    if settings.apple_notes is None:
        raise RuntimeError("Apple Notes sync is not configured")
    warehouse = warehouse_from_settings(settings)

    with exclusive_sync_lock(
        name="apple_notes_drive_ingest",
        postgres_lock_id=APPLE_NOTES_DRIVE_INGEST_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Apple Notes Drive ingest because another run is already active")
            summary = None
        else:
            object_store = _apple_notes_object_store(settings)
            summary = AppleNotesDriveIngestRunner(
                warehouse=warehouse,
                metadata_source=lambda: iter_metadata_payloads(
                    object_store=object_store,
                    object_store_factory=lambda: _apple_notes_object_store(settings),
                    download_workers=int(os.getenv("APPLE_NOTES_DRIVE_INGEST_DOWNLOAD_WORKERS", "8")),
                ),
                object_store_factory=lambda: _apple_notes_object_store(settings),
                promotion_workers=int(os.getenv("APPLE_NOTES_DRIVE_INGEST_PROMOTION_WORKERS", "8")),
                logger=context.log,
            ).sync()

    return MaterializeResult(
        metadata={
            "metadata_seen": MetadataValue.int(summary.metadata_seen if summary else 0),
            "notes_written": MetadataValue.int(summary.notes_written if summary else 0),
            "revisions_written": MetadataValue.int(summary.revisions_written if summary else 0),
            "attachments_written": MetadataValue.int(summary.attachments_written if summary else 0),
            "files_promoted": MetadataValue.int(summary.files_promoted if summary else 0),
        }
    )


apple_notes_drive_ingest_job = define_asset_job(
    "apple_notes_drive_ingest_job",
    selection=[apple_notes_drive_ingest],
)


@sensor(
    job=apple_notes_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=APPLE_NOTES_SENSOR_INTERVAL_SECONDS,
)
def apple_notes_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="apple_notes_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active

    settings = load_settings(require_gmail=False, require_apple_notes=True)
    if settings.apple_notes is None:
        raise RuntimeError("Apple Notes sync is not configured")
    if not has_metadata_payloads(object_store=_apple_notes_object_store(settings), stage="inbox"):
        return SkipReason("No Apple Notes inbox metadata found in object storage.")

    return RunRequest(tags={"apple_notes_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[apple_notes_drive_ingest],
        jobs=[apple_notes_drive_ingest_job],
        sensors=[apple_notes_drive_inbox_sensor],
    )
