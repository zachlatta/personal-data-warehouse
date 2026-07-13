from __future__ import annotations

from dagster import (
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
    sensor,
)

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.photos_drive_ingest import (
    PhotosDriveIngestRunner,
    has_metadata_payloads,
    iter_metadata_payloads,
)
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

# 846 is alice_voice_recordings' derived id (845 + 1); 847 is plaid_sync.
PHOTOS_DRIVE_INGEST_POSTGRES_LOCK_ID = 7_403_111_850
PHOTOS_SENSOR_INTERVAL_SECONDS = 60


def photos_object_store(settings):
    return build_object_store(
        google_drive_spec(
            folder_id=settings.photos.google_drive_folder_id,
            account=settings.photos.google_drive_account,
            source="photos",
            blob_kind="photo_file",
            metadata_kind="photo_metadata",
        ),
        settings=settings,
    )


@asset(
    group_name="photos",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def photos_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_photos=True)
    if settings.photos is None:
        raise RuntimeError("Photo sync is not configured")
    warehouse = warehouse_from_settings(settings)

    with exclusive_sync_lock(
        name="photos_drive_ingest",
        postgres_lock_id=PHOTOS_DRIVE_INGEST_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping photos Drive ingest because another run is already active")
            summary = None
        else:
            object_store = photos_object_store(settings)
            summary = PhotosDriveIngestRunner(
                warehouse=warehouse,
                metadata_source=lambda: iter_metadata_payloads(object_store=object_store),
                object_store=object_store,
                logger=context.log,
            ).sync()

    return MaterializeResult(
        metadata={
            "metadata_seen": MetadataValue.int(summary.metadata_seen if summary else 0),
            "rows_written": MetadataValue.int(summary.rows_written if summary else 0),
            "objects_promoted": MetadataValue.int(summary.objects_promoted if summary else 0),
        }
    )


photos_drive_ingest_job = define_asset_job(
    "photos_drive_ingest_job",
    selection=[photos_drive_ingest],
)


@sensor(
    job=photos_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=PHOTOS_SENSOR_INTERVAL_SECONDS,
)
def photos_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="photos_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active

    settings = load_settings(require_gmail=False, require_photos=True)
    if settings.photos is None:
        raise RuntimeError("Photo sync is not configured")
    if not has_metadata_payloads(object_store=photos_object_store(settings), stage="inbox"):
        return SkipReason("No photo inbox metadata found in object storage.")

    return RunRequest(tags={"photos_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[photos_drive_ingest],
        jobs=[photos_drive_ingest_job],
        sensors=[photos_drive_inbox_sensor],
    )
