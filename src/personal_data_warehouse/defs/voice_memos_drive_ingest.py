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

from personal_data_warehouse.clickhouse import ClickHouseWarehouse
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.voice_memos_drive_ingest import (
    GoogleDriveVoiceMemosPromoter,
    VoiceMemosDriveIngestRunner,
    build_google_drive_service,
    iter_drive_metadata_payloads,
)

VOICE_MEMOS_DRIVE_INGEST_POSTGRES_LOCK_ID = 7_403_111_839


@asset(
    group_name="voice_memos",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def voice_memos_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_voice_memos=True)
    if settings.voice_memos is None:
        raise RuntimeError("Voice Memos sync is not configured")
    warehouse = ClickHouseWarehouse(settings.clickhouse_url or "")

    with exclusive_sync_lock(
        name="voice_memos_drive_ingest",
        postgres_lock_id=VOICE_MEMOS_DRIVE_INGEST_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Voice Memos Drive ingest because another run is already active")
            summary = None
        else:
            service = build_google_drive_service(account=settings.voice_memos.account, settings=settings)
            summary = VoiceMemosDriveIngestRunner(
                warehouse=warehouse,
                metadata_source=lambda: iter_drive_metadata_payloads(
                    service=service,
                    folder_id=settings.voice_memos.google_drive_folder_id,
                ),
                promoter=GoogleDriveVoiceMemosPromoter(
                    service=service,
                    folder_id=settings.voice_memos.google_drive_folder_id,
                ),
                logger=context.log,
            ).sync()

    return MaterializeResult(
        metadata={
            "metadata_seen": MetadataValue.int(summary.metadata_seen if summary else 0),
            "rows_written": MetadataValue.int(summary.rows_written if summary else 0),
            "recordings_promoted": MetadataValue.int(summary.recordings_promoted if summary else 0),
        }
    )


voice_memos_drive_ingest_job = define_asset_job(
    "voice_memos_drive_ingest_job",
    selection=[voice_memos_drive_ingest],
)


@schedule(
    cron_schedule="*/5 * * * *",
    job=voice_memos_drive_ingest_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def voice_memos_drive_ingest_every_five_minutes(context):
    return skip_if_job_active(context, job_name="voice_memos_drive_ingest_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[voice_memos_drive_ingest],
        jobs=[voice_memos_drive_ingest_job],
        schedules=[voice_memos_drive_ingest_every_five_minutes],
    )
