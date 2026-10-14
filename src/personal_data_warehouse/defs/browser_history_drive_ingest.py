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

from personal_data_warehouse.browser_history_drive_ingest import (
    BrowserHistoryDriveIngestRunner,
    has_batch_payloads,
    iter_batch_payloads,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

BROWSER_HISTORY_DRIVE_INGEST_POSTGRES_LOCK_ID = 8_407_112_503
# A first backfill is dozens of 5,000-visit batches; bound each run so it
# finishes well inside the job runtime cap and the sensor picks up the rest.
BROWSER_HISTORY_MAX_BATCHES_PER_RUN = 40
BROWSER_HISTORY_SENSOR_INTERVAL_SECONDS = 60


def _browser_history_object_store(settings):
    return build_object_store(
        google_drive_spec(
            folder_id=settings.browser_history.google_drive_folder_id,
            account=settings.browser_history.google_drive_account,
            source="browser_history",
            blob_kind="browser_history_blob",
            metadata_kind="browser_history_export_batch",
        ),
        settings=settings,
    )


@asset(
    group_name="browser_history",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def browser_history_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_browser_history=True)
    if settings.browser_history is None:
        raise RuntimeError("Browser history sync is not configured")
    warehouse = warehouse_from_settings(settings)
    with exclusive_sync_lock(
        name="browser_history_drive_ingest",
        postgres_lock_id=BROWSER_HISTORY_DRIVE_INGEST_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping browser history Drive ingest because another run is already active")
            summary = None
        else:
            object_store = _browser_history_object_store(settings)
            summary = BrowserHistoryDriveIngestRunner(
                warehouse=warehouse,
                batch_source=lambda: iter_batch_payloads(object_store=object_store),
                object_store=object_store,
                logger=context.log,
                max_batches=BROWSER_HISTORY_MAX_BATCHES_PER_RUN,
            ).sync()
    return MaterializeResult(
        metadata={
            "batches_seen": MetadataValue.int(summary.batches_seen if summary else 0),
            "visits_written": MetadataValue.int(summary.visits_written if summary else 0),
            "files_promoted": MetadataValue.int(summary.files_promoted if summary else 0),
        }
    )


browser_history_drive_ingest_job = define_asset_job(
    "browser_history_drive_ingest_job",
    selection=[browser_history_drive_ingest],
)


@sensor(
    job=browser_history_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=BROWSER_HISTORY_SENSOR_INTERVAL_SECONDS,
)
def browser_history_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="browser_history_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active
    settings = load_settings(require_gmail=False, require_browser_history=True)
    if settings.browser_history is None:
        raise RuntimeError("Browser history sync is not configured")
    if not has_batch_payloads(object_store=_browser_history_object_store(settings), stage="inbox"):
        return SkipReason("No browser history inbox batches found in object storage.")
    return RunRequest(tags={"browser_history_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[browser_history_drive_ingest],
        jobs=[browser_history_drive_ingest_job],
        sensors=[browser_history_drive_inbox_sensor],
    )
