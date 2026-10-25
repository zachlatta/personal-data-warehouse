from __future__ import annotations

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

from personal_data_warehouse.android_screen_time_drive_ingest import (
    BATCH_KIND,
    BLOB_KIND,
    SOURCE,
    AndroidScreenTimeDriveIngestRunner,
    has_batch_payloads,
    iter_batch_payloads,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

ANDROID_SCREEN_TIME_DRIVE_INGEST_POSTGRES_LOCK_ID = 8_407_112_614
# Each device sends a batch every five minutes; a new device's first run sends
# about ten days (~150k events, ~75 batches). Bound a run so a backlog drains
# over several runs instead of one long one.
ANDROID_SCREEN_TIME_MAX_BATCHES_PER_RUN = 200
ANDROID_SCREEN_TIME_SENSOR_INTERVAL_SECONDS = 60


def _android_screen_time_object_store(settings):
    return build_object_store(
        google_drive_spec(
            folder_id=settings.android_screen_time.google_drive_folder_id,
            account=settings.android_screen_time.google_drive_account,
            source=SOURCE,
            blob_kind=BLOB_KIND,
            metadata_kind=BATCH_KIND,
        ),
        settings=settings,
    )


def _settings():
    settings = load_settings(require_gmail=False, require_android_screen_time=True)
    if settings.android_screen_time is None:
        raise RuntimeError("Android screen time sync is not configured")
    return settings


@asset(
    group_name="android_screen_time",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def android_screen_time_drive_ingest(context) -> MaterializeResult:
    settings = _settings()
    warehouse = warehouse_from_settings(settings)
    with exclusive_sync_lock(
        name="android_screen_time_drive_ingest",
        postgres_lock_id=ANDROID_SCREEN_TIME_DRIVE_INGEST_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning(
                "Skipping Android screen time Drive ingest because another run is already active"
            )
            summary = None
        else:
            object_store = _android_screen_time_object_store(settings)
            summary = AndroidScreenTimeDriveIngestRunner(
                warehouse=warehouse,
                batch_source=lambda: iter_batch_payloads(object_store=object_store),
                object_store=object_store,
                logger=context.log,
                max_batches=ANDROID_SCREEN_TIME_MAX_BATCHES_PER_RUN,
            ).sync()
    return MaterializeResult(
        metadata={
            "batches_seen": MetadataValue.int(summary.batches_seen if summary else 0),
            "events_written": MetadataValue.int(summary.events_written if summary else 0),
            "daily_written": MetadataValue.int(summary.daily_written if summary else 0),
            "files_promoted": MetadataValue.int(summary.files_promoted if summary else 0),
        }
    )


android_screen_time_drive_ingest_job = define_asset_job(
    "android_screen_time_drive_ingest_job",
    selection=[android_screen_time_drive_ingest],
)


@sensor(
    job=android_screen_time_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=ANDROID_SCREEN_TIME_SENSOR_INTERVAL_SECONDS,
)
def android_screen_time_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="android_screen_time_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active
    settings = _settings()
    if not has_batch_payloads(object_store=_android_screen_time_object_store(settings), stage="inbox"):
        return SkipReason("No Android screen time inbox batches found in object storage.")
    return RunRequest(tags={"android_screen_time_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[android_screen_time_drive_ingest],
        jobs=[android_screen_time_drive_ingest_job],
        sensors=[android_screen_time_drive_inbox_sensor],
    )
