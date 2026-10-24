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

from personal_data_warehouse.android_messages_drive_ingest import (
    BATCH_KIND,
    BLOB_KIND,
    SOURCE,
    AndroidMessagesDriveIngestRunner,
    has_batch_payloads,
    iter_batch_payloads,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

ANDROID_MESSAGES_DRIVE_INGEST_POSTGRES_LOCK_ID = 8_407_112_612
# The phone sends a batch within a couple of minutes of each text;
# bound a run anyway so a backlog (the phone offline for a day) drains over
# several runs instead of one long one.
ANDROID_MESSAGES_MAX_BATCHES_PER_RUN = 200
ANDROID_MESSAGES_SENSOR_INTERVAL_SECONDS = 60


def _android_messages_object_store(settings):
    return build_object_store(
        google_drive_spec(
            folder_id=settings.android_messages.google_drive_folder_id,
            account=settings.android_messages.google_drive_account,
            source=SOURCE,
            blob_kind=BLOB_KIND,
            metadata_kind=BATCH_KIND,
        ),
        settings=settings,
    )


def _settings():
    settings = load_settings(require_gmail=False, require_android_messages=True)
    if settings.android_messages is None:
        raise RuntimeError("Android message sync is not configured")
    return settings


@asset(
    group_name="android_messages",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def android_messages_drive_ingest(context) -> MaterializeResult:
    settings = _settings()
    warehouse = warehouse_from_settings(settings)
    with exclusive_sync_lock(
        name="android_messages_drive_ingest",
        postgres_lock_id=ANDROID_MESSAGES_DRIVE_INGEST_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning(
                "Skipping Android message Drive ingest because another run is already active"
            )
            summary = None
        else:
            object_store = _android_messages_object_store(settings)
            summary = AndroidMessagesDriveIngestRunner(
                warehouse=warehouse,
                batch_source=lambda: iter_batch_payloads(object_store=object_store),
                object_store=object_store,
                logger=context.log,
                max_batches=ANDROID_MESSAGES_MAX_BATCHES_PER_RUN,
            ).sync()
    return MaterializeResult(
        metadata={
            "batches_seen": MetadataValue.int(summary.batches_seen if summary else 0),
            "messages_written": MetadataValue.int(summary.messages_written if summary else 0),
            "files_promoted": MetadataValue.int(summary.files_promoted if summary else 0),
        }
    )


android_messages_drive_ingest_job = define_asset_job(
    "android_messages_drive_ingest_job",
    selection=[android_messages_drive_ingest],
)


@sensor(
    job=android_messages_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=ANDROID_MESSAGES_SENSOR_INTERVAL_SECONDS,
)
def android_messages_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="android_messages_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active
    settings = _settings()
    if not has_batch_payloads(object_store=_android_messages_object_store(settings), stage="inbox"):
        return SkipReason("No Android message inbox batches found in object storage.")
    return RunRequest(tags={"android_messages_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[android_messages_drive_ingest],
        jobs=[android_messages_drive_ingest_job],
        sensors=[android_messages_drive_inbox_sensor],
    )
