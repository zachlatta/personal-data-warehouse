from __future__ import annotations

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
    define_asset_job,
    definitions,
    schedule,
)

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.google_drive_source_sync import (
    GoogleDriveSourceSyncRunner,
    build_drive_client_factory,
)
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

GOOGLE_DRIVE_SOURCE_SYNC_POSTGRES_LOCK_ID = 8_407_112_443


@asset(
    group_name="google_drive",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def google_drive_source_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False)
    if settings.google_drive_source is None:
        context.log.info(
            "Google Drive source sync is disabled (set GOOGLE_DRIVE_SOURCE_ENABLED=1 to re-enable) "
            "or no accounts are configured"
        )
        return MaterializeResult(metadata={"accounts": MetadataValue.int(0)})

    warehouse = warehouse_from_settings(settings)
    summaries = []
    with exclusive_sync_lock(
        name="google_drive_source_sync",
        postgres_lock_id=GOOGLE_DRIVE_SOURCE_SYNC_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Google Drive source sync because another run is already active")
        else:
            summaries = GoogleDriveSourceSyncRunner(
                settings=settings,
                warehouse=warehouse,
                client_factory=build_drive_client_factory(settings),
            ).sync_all()

    return MaterializeResult(
        metadata={
            "accounts": MetadataValue.int(len(summaries)),
            "files_seen": MetadataValue.int(sum(s.files_seen for s in summaries)),
            "files_written": MetadataValue.int(sum(s.files_written for s in summaries)),
            "texts_written": MetadataValue.int(sum(s.texts_written for s in summaries)),
            "summaries": MetadataValue.json(
                [
                    {
                        "account": s.account,
                        "status": s.status,
                        "sync_type": s.sync_type,
                        "files_seen": s.files_seen,
                        "files_written": s.files_written,
                        "texts_written": s.texts_written,
                        "error": s.error,
                    }
                    for s in summaries
                ]
            ),
        }
    )


google_drive_source_sync_job = define_asset_job(
    "google_drive_source_sync_job",
    selection=[google_drive_source_sync],
)


# Drive changes are low-velocity compared with Gmail and a first full crawl is
# heavy, so a 30-minute cadence (with overlap guard) leaves a real idle gap.
@schedule(
    cron_schedule="*/30 * * * *",
    job=google_drive_source_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def google_drive_source_sync_every_thirty_minutes(context):
    return skip_if_job_in_progress(context, job_name="google_drive_source_sync_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[google_drive_source_sync],
        jobs=[google_drive_source_sync_job],
        schedules=[google_drive_source_sync_every_thirty_minutes],
    )
