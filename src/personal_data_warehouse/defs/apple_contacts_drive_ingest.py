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

from personal_data_warehouse.apple_contacts_drive_ingest import (
    AppleContactsDriveIngestRunner,
    has_batch_payloads,
    iter_batch_payloads,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

APPLE_CONTACTS_DRIVE_INGEST_POSTGRES_LOCK_ID = 8_407_112_470
APPLE_CONTACTS_SENSOR_INTERVAL_SECONDS = 60


def _apple_contacts_object_store(settings):
    return build_object_store(
        google_drive_spec(
            folder_id=settings.apple_contacts.google_drive_folder_id,
            account=settings.apple_contacts.google_drive_account,
            source="apple_contacts",
            blob_kind="apple_contact_blob",
            metadata_kind="apple_contact_export_batch",
        ),
        settings=settings,
    )


@asset(
    group_name="apple_contacts",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def apple_contacts_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_apple_contacts=True)
    if settings.apple_contacts is None:
        raise RuntimeError("Apple Contacts sync is not configured")
    warehouse = warehouse_from_settings(settings)
    with exclusive_sync_lock(
        name="apple_contacts_drive_ingest",
        postgres_lock_id=APPLE_CONTACTS_DRIVE_INGEST_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Apple Contacts Drive ingest because another run is already active")
            summary = None
        else:
            object_store = _apple_contacts_object_store(settings)
            summary = AppleContactsDriveIngestRunner(
                warehouse=warehouse,
                batch_source=lambda: iter_batch_payloads(object_store=object_store),
                object_store=object_store,
                logger=context.log,
            ).sync()
    return MaterializeResult(
        metadata={
            "batches_seen": MetadataValue.int(summary.batches_seen if summary else 0),
            "contacts_written": MetadataValue.int(summary.contacts_written if summary else 0),
            "files_promoted": MetadataValue.int(summary.files_promoted if summary else 0),
        }
    )


apple_contacts_drive_ingest_job = define_asset_job(
    "apple_contacts_drive_ingest_job",
    selection=[apple_contacts_drive_ingest],
)


@sensor(
    job=apple_contacts_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=APPLE_CONTACTS_SENSOR_INTERVAL_SECONDS,
)
def apple_contacts_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="apple_contacts_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active
    settings = load_settings(require_gmail=False, require_apple_contacts=True)
    if settings.apple_contacts is None:
        raise RuntimeError("Apple Contacts sync is not configured")
    if not has_batch_payloads(object_store=_apple_contacts_object_store(settings), stage="inbox"):
        return SkipReason("No Apple Contacts inbox batches found in object storage.")
    return RunRequest(tags={"apple_contacts_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[apple_contacts_drive_ingest],
        jobs=[apple_contacts_drive_ingest_job],
        sensors=[apple_contacts_drive_inbox_sensor],
    )
