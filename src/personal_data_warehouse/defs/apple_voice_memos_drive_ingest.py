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

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.apple_voice_memos_drive_ingest import (
    VoiceMemosDriveIngestRunner,
    has_metadata_payloads,
    iter_metadata_payloads,
)
from personal_data_warehouse.warehouse import warehouse_from_settings

VOICE_MEMOS_DRIVE_INGEST_POSTGRES_LOCK_ID = 7_403_111_839
VOICE_MEMOS_SENSOR_INTERVAL_SECONDS = 60


def _voice_memos_object_store(settings):
    return build_object_store(
        google_drive_spec(
            folder_id=settings.voice_memos.google_drive_folder_id,
            account=settings.voice_memos.account,
            source="apple_voice_memos",
            blob_kind="voice_memo_audio",
            metadata_kind="voice_memo_metadata",
            legacy_sources=("voice_memos",),
        ),
        settings=settings,
    )


@asset(
    group_name="apple_voice_memos",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def apple_voice_memos_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_voice_memos=True)
    if settings.voice_memos is None:
        raise RuntimeError("Voice Memos sync is not configured")
    warehouse = warehouse_from_settings(settings)

    with exclusive_sync_lock(
        name="apple_voice_memos_drive_ingest",
        postgres_lock_id=VOICE_MEMOS_DRIVE_INGEST_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Voice Memos Drive ingest because another run is already active")
            summary = None
        else:
            object_store = _voice_memos_object_store(settings)
            summary = VoiceMemosDriveIngestRunner(
                warehouse=warehouse,
                metadata_source=lambda: iter_metadata_payloads(object_store=object_store),
                object_store=object_store,
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
    if not has_metadata_payloads(object_store=_voice_memos_object_store(settings), stage="inbox"):
        return SkipReason("No Voice Memos inbox metadata found in object storage.")

    return RunRequest(tags={"apple_voice_memos_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[apple_voice_memos_drive_ingest],
        jobs=[apple_voice_memos_drive_ingest_job],
        sensors=[apple_voice_memos_drive_inbox_sensor],
    )
