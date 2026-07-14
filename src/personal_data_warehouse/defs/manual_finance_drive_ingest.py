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
from personal_data_warehouse.manual_finance_drive_ingest import (
    ManualFinanceDriveIngestRunner,
    has_metadata_payloads,
    iter_metadata_payloads,
)
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

MANUAL_FINANCE_DRIVE_INGEST_POSTGRES_LOCK_ID = 7_403_111_852
MANUAL_FINANCE_SENSOR_INTERVAL_SECONDS = 60


def manual_finance_object_store(settings):
    return build_object_store(
        google_drive_spec(
            folder_id=settings.manual_finance.google_drive_folder_id,
            account=settings.manual_finance.google_drive_account,
            source="manual_finance",
            blob_kind="manual_finance_document",
            metadata_kind="manual_finance_metadata",
        ),
        settings=settings,
    )


@asset(
    group_name="finance",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def manual_finance_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_manual_finance=True)
    if settings.manual_finance is None:
        raise RuntimeError("Manual finance ingest is not configured")
    warehouse = warehouse_from_settings(settings)

    with exclusive_sync_lock(
        name="manual_finance_drive_ingest",
        postgres_lock_id=MANUAL_FINANCE_DRIVE_INGEST_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning(
                "Skipping manual finance Drive ingest because another run is already active"
            )
            summary = None
        else:
            object_store = manual_finance_object_store(settings)
            summary = ManualFinanceDriveIngestRunner(
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


manual_finance_drive_ingest_job = define_asset_job(
    "manual_finance_drive_ingest_job",
    selection=[manual_finance_drive_ingest],
)


@sensor(
    job=manual_finance_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=MANUAL_FINANCE_SENSOR_INTERVAL_SECONDS,
)
def manual_finance_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="manual_finance_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active

    try:
        settings = load_settings(require_gmail=False, require_manual_finance=True)
    except ValueError as exc:
        return SkipReason(f"Manual finance ingest is not configured: {exc}")
    if settings.manual_finance is None:
        return SkipReason("Manual finance ingest is not configured.")
    if not has_metadata_payloads(object_store=manual_finance_object_store(settings), stage="inbox"):
        return SkipReason("No manual finance inbox metadata found in object storage.")

    return RunRequest(tags={"finance_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[manual_finance_drive_ingest],
        jobs=[manual_finance_drive_ingest_job],
        sensors=[manual_finance_drive_inbox_sensor],
    )
