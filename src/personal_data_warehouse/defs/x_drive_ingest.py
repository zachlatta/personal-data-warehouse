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
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings
from personal_data_warehouse.x_drive_ingest import (
    BATCH_KIND,
    BLOB_KIND,
    SOURCE,
    XDriveIngestRunner,
    has_batch_payloads,
    iter_batch_payloads,
)

X_DRIVE_INGEST_POSTGRES_LOCK_ID = 8_407_112_517
# A first backfill is a few dozen batches (one per stream page); bound each run
# so it finishes well inside the job runtime cap and the sensor picks up the rest.
X_MAX_BATCHES_PER_RUN = 40
X_SENSOR_INTERVAL_SECONDS = 60


def _x_object_store(settings):
    return build_object_store(
        google_drive_spec(
            folder_id=settings.x.google_drive_folder_id,
            account=settings.x.google_drive_account,
            source=SOURCE,
            blob_kind=BLOB_KIND,
            metadata_kind=BATCH_KIND,
        ),
        settings=settings,
    )


@asset(
    group_name="x",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def x_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_x=True)
    if settings.x is None:
        raise RuntimeError("X sync is not configured")
    warehouse = warehouse_from_settings(settings)
    with exclusive_sync_lock(name="x_drive_ingest", postgres_lock_id=X_DRIVE_INGEST_POSTGRES_LOCK_ID) as acquired:
        if not acquired:
            context.log.warning("Skipping X Drive ingest because another run is already active")
            summary = None
        else:
            object_store = _x_object_store(settings)
            summary = XDriveIngestRunner(
                warehouse=warehouse,
                batch_source=lambda: iter_batch_payloads(object_store=object_store),
                object_store=object_store,
                logger=context.log,
                max_batches=X_MAX_BATCHES_PER_RUN,
            ).sync()
    return MaterializeResult(
        metadata={
            "batches_seen": MetadataValue.int(summary.batches_seen if summary else 0),
            "posts_written": MetadataValue.int(summary.posts_written if summary else 0),
            "relations_written": MetadataValue.int(summary.relations_written if summary else 0),
            "files_promoted": MetadataValue.int(summary.files_promoted if summary else 0),
        }
    )


x_drive_ingest_job = define_asset_job("x_drive_ingest_job", selection=[x_drive_ingest])


@sensor(
    job=x_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=X_SENSOR_INTERVAL_SECONDS,
)
def x_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="x_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active
    settings = load_settings(require_gmail=False, require_x=True)
    if settings.x is None:
        raise RuntimeError("X sync is not configured")
    if not has_batch_payloads(object_store=_x_object_store(settings), stage="inbox"):
        return SkipReason("No X inbox batches found in object storage.")
    return RunRequest(tags={"x_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[x_drive_ingest],
        jobs=[x_drive_ingest_job],
        sensors=[x_drive_inbox_sensor],
    )
