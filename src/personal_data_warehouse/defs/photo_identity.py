from __future__ import annotations

import os

from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    RunRequest,
    SkipReason,
    asset,
    define_asset_job,
    definitions,
    schedule,
    sensor,
)

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.photos_drive_ingest import photos_drive_ingest, photos_object_store
from personal_data_warehouse.photo_identity import PhotoIdentityRunner, has_unresolved_photo_files
from personal_data_warehouse.schedule_guards import skip_if_job_active, skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

# 7_403_111_847 was taken by plaid_sync while this branch was in flight.
PHOTO_IDENTITY_POSTGRES_LOCK_ID = 7_403_111_849
PHOTO_IDENTITY_SENSOR_INTERVAL_SECONDS = 120
PHOTO_IDENTITY_BATCH_SIZE_ENV = "PHOTO_IDENTITY_BATCH_SIZE"


def photo_identity_batch_size() -> int:
    return int(os.getenv(PHOTO_IDENTITY_BATCH_SIZE_ENV, "500"))


@asset(
    group_name="photos",
    deps=[photos_drive_ingest],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def photo_identity(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_photos=True)
    if settings.photos is None:
        raise RuntimeError("Photo sync is not configured")
    warehouse = warehouse_from_settings(settings)

    batch_size = photo_identity_batch_size()
    with exclusive_sync_lock(
        name="photo_identity",
        postgres_lock_id=PHOTO_IDENTITY_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping photo identity because another run is already active")
            summary = None
        else:
            summary = PhotoIdentityRunner(
                warehouse=warehouse,
                object_store=photos_object_store(settings),
                logger=context.log,
                batch_size=batch_size if batch_size > 0 else None,
            ).sync()

    return MaterializeResult(
        metadata={
            "files_seen": MetadataValue.int(summary.files_seen if summary else 0),
            "files_linked": MetadataValue.int(summary.files_linked if summary else 0),
            "assets_created": MetadataValue.int(summary.assets_created if summary else 0),
            "assets_updated": MetadataValue.int(summary.assets_updated if summary else 0),
            "fingerprints_computed": MetadataValue.int(summary.fingerprints_computed if summary else 0),
            "thumbnails_generated": MetadataValue.int(summary.thumbnails_generated if summary else 0),
            "undecodable_files": MetadataValue.int(summary.undecodable_files if summary else 0),
        }
    )


photo_identity_job = define_asset_job(
    "photo_identity_job",
    selection=[photo_identity],
)


@schedule(
    cron_schedule="29 * * * *",
    job=photo_identity_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def photo_identity_hourly(context):
    return skip_if_job_active(context, job_name="photo_identity_job")


@sensor(
    job=photo_identity_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=PHOTO_IDENTITY_SENSOR_INTERVAL_SECONDS,
)
def photo_identity_backlog_sensor(context):
    active = skip_if_job_in_progress(context, job_name="photo_identity_job")
    if isinstance(active, SkipReason):
        return active

    try:
        settings = load_settings(require_gmail=False, require_photos=True)
    except ValueError as exc:
        return SkipReason(f"Photo sync is not configured: {exc}")
    if settings.photos is None:
        return SkipReason("Photo sync is not configured.")

    warehouse = warehouse_from_settings(settings)
    try:
        try:
            backlog = has_unresolved_photo_files(warehouse)
        except Exception as exc:
            # The photo tables may not exist yet on a brand-new deploy (the
            # Drive ingest creates them on first promotion). Skip until then;
            # never run DDL from a sensor.
            return SkipReason(f"Photo tables are not ready yet: {exc}")
        if not backlog:
            return SkipReason("No photo files are waiting for identity resolution.")
    finally:
        warehouse.close()

    return RunRequest(tags={"photos_trigger": "identity_backlog"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[photo_identity],
        jobs=[photo_identity_job],
        schedules=[photo_identity_hourly],
        sensors=[photo_identity_backlog_sensor],
    )
