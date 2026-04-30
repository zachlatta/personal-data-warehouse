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

from personal_data_warehouse.clickhouse import ClickHouseWarehouse
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.apple_voice_memos_drive_ingest import (
    GoogleDriveVoiceMemosPromoter,
    VoiceMemosDriveIngestRunner,
    build_google_drive_service,
    has_drive_metadata_payloads,
    iter_drive_metadata_payloads,
)

VOICE_MEMOS_DRIVE_INGEST_POSTGRES_LOCK_ID = 7_403_111_839
VOICE_MEMOS_SENSOR_INTERVAL_SECONDS = 60


@asset(
    group_name="apple_voice_memos",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def apple_voice_memos_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_voice_memos=True)
    if settings.voice_memos is None:
        raise RuntimeError("Voice Memos sync is not configured")
    warehouse = ClickHouseWarehouse(settings.clickhouse_url or "")

    with exclusive_sync_lock(
        name="apple_voice_memos_drive_ingest",
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


apple_voice_memos_drive_ingest_job = define_asset_job(
    "apple_voice_memos_drive_ingest_job",
    selection=[apple_voice_memos_drive_ingest],
)


@sensor(
    job=apple_voice_memos_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=VOICE_MEMOS_SENSOR_INTERVAL_SECONDS,
)
def apple_voice_memos_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="apple_voice_memos_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active

    settings = load_settings(require_gmail=False, require_voice_memos=True)
    if settings.voice_memos is None:
        raise RuntimeError("Voice Memos sync is not configured")
    service = build_google_drive_service(account=settings.voice_memos.account, settings=settings)
    if not has_drive_metadata_payloads(
        service=service,
        folder_id=settings.voice_memos.google_drive_folder_id,
        stage="inbox",
    ):
        return SkipReason("No Voice Memos inbox metadata found in Google Drive.")

    return RunRequest(tags={"apple_voice_memos_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[apple_voice_memos_drive_ingest],
        jobs=[apple_voice_memos_drive_ingest_job],
        sensors=[apple_voice_memos_drive_inbox_sensor],
    )
