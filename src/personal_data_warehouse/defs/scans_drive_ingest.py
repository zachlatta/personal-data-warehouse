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
from personal_data_warehouse.scans_drive_ingest import (
    BATCH_KIND,
    BLOB_KIND,
    SOURCE,
    ScansDriveIngestRunner,
    has_batch_payloads,
    iter_batch_payloads,
)
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.timeline_fast_lane import land_sources_on_timeline
from personal_data_warehouse.warehouse import warehouse_from_settings

SCANS_DRIVE_INGEST_POSTGRES_LOCK_ID = 8_407_112_615
# Someone is standing at the scanner waiting for the notification, so the
# inbox is checked twice a minute rather than every minute or five.
SCANS_SENSOR_INTERVAL_SECONDS = 30
SCANS_MAX_BATCHES_PER_RUN = 200


def scans_object_store(settings):
    return build_object_store(
        google_drive_spec(
            folder_id=settings.scans.google_drive_folder_id,
            account=settings.scans.google_drive_account,
            source=SOURCE,
            blob_kind=BLOB_KIND,
            metadata_kind=BATCH_KIND,
        ),
        settings=settings,
    )


def _settings():
    settings = load_settings(require_gmail=False, require_scans=True)
    if settings.scans is None:
        raise RuntimeError("Scans are not configured")
    return settings


@asset(
    group_name="scans",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def scans_drive_ingest(context) -> MaterializeResult:
    settings = _settings()
    warehouse = warehouse_from_settings(settings)
    try:
        with exclusive_sync_lock(
            name="scans_drive_ingest",
            postgres_lock_id=SCANS_DRIVE_INGEST_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                context.log.warning("Skipping scans Drive ingest because another run is already active")
                summary = None
            else:
                object_store = scans_object_store(settings)
                summary = ScansDriveIngestRunner(
                    warehouse=warehouse,
                    batch_source=lambda: iter_batch_payloads(object_store=object_store),
                    object_store=object_store,
                    logger=context.log,
                    max_batches=SCANS_MAX_BATCHES_PER_RUN,
                ).sync()
    finally:
        warehouse.close()
    # The scan's timeline row is what pages Zach, so it lands in this run.
    fast_lane = {"enabled": False, "rows": 0}
    if summary is not None and summary.scans_written:
        fast_lane = land_sources_on_timeline(
            postgres_url=settings.postgres_database_url or "",
            sources=[SOURCE],
            logger=context.log,
        )
    return MaterializeResult(
        metadata={
            "timeline_fast_lane": MetadataValue.json(fast_lane),
            "batches_seen": MetadataValue.int(summary.batches_seen if summary else 0),
            "scans_written": MetadataValue.int(summary.scans_written if summary else 0),
            "files_promoted": MetadataValue.int(summary.files_promoted if summary else 0),
        }
    )


scans_drive_ingest_job = define_asset_job(
    "scans_drive_ingest_job",
    selection=[scans_drive_ingest],
    tags={"dagster/max_runtime": str(30 * 60)},
)


@sensor(
    job=scans_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=SCANS_SENSOR_INTERVAL_SECONDS,
)
def scans_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="scans_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active
    settings = _settings()
    if not has_batch_payloads(object_store=scans_object_store(settings), stage="inbox"):
        return SkipReason("No scan batches found in the Drive inbox.")
    return RunRequest(tags={"scans_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[scans_drive_ingest],
        jobs=[scans_drive_ingest_job],
        sensors=[scans_drive_inbox_sensor],
    )
