from __future__ import annotations

import os

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
from personal_data_warehouse.defs.apple_voice_memos_drive_ingest import apple_voice_memos_drive_ingest
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.apple_voice_memos_transcription import (
    ASSEMBLYAI_PROVIDER,
    GoogleDriveVoiceMemoAudioSource,
    VoiceMemosTranscriptionRunner,
    assemblyai_client_from_settings,
)
from personal_data_warehouse.warehouse import warehouse_from_settings

VOICE_MEMOS_TRANSCRIPTION_POSTGRES_LOCK_ID = 7_403_111_840
DEFAULT_VOICE_MEMOS_TRANSCRIPTION_BATCH_SIZE = 3
VOICE_MEMOS_TRANSCRIPTION_SENSOR_INTERVAL_SECONDS = 60


@asset(
    group_name="apple_voice_memos",
    deps=[apple_voice_memos_drive_ingest],
    retry_policy=RetryPolicy(max_retries=2, delay=120),
)
def apple_voice_memos_transcription(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_voice_memos=True, require_assemblyai=True)
    if settings.voice_memos is None:
        raise RuntimeError("Voice Memos sync is not configured")
    if settings.assemblyai is None:
        raise RuntimeError("AssemblyAI is not configured")

    batch_size = int(
        os.getenv(
            "VOICE_MEMOS_TRANSCRIPTION_BATCH_SIZE",
            str(DEFAULT_VOICE_MEMOS_TRANSCRIPTION_BATCH_SIZE),
        )
    )
    warehouse = warehouse_from_settings(settings)

    with exclusive_sync_lock(
        name="apple_voice_memos_transcription",
        postgres_lock_id=VOICE_MEMOS_TRANSCRIPTION_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Voice Memos transcription because another run is already active")
            summary = None
        else:
            object_store = build_object_store(
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
            summary = VoiceMemosTranscriptionRunner(
                warehouse=warehouse,
                audio_source=GoogleDriveVoiceMemoAudioSource(object_store=object_store),
                transcription_client=assemblyai_client_from_settings(settings),
                logger=context.log,
            ).sync(limit=batch_size)

    return MaterializeResult(
        metadata={
            "recordings_seen": MetadataValue.int(summary.recordings_seen if summary else 0),
            "recordings_transcribed": MetadataValue.int(summary.recordings_transcribed if summary else 0),
            "recordings_failed": MetadataValue.int(summary.recordings_failed if summary else 0),
            "segments_written": MetadataValue.int(summary.segments_written if summary else 0),
        }
    )


apple_voice_memos_transcription_job = define_asset_job(
    "apple_voice_memos_transcription_job",
    selection="*apple_voice_memos_transcription",
)


@sensor(
    job=apple_voice_memos_transcription_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=VOICE_MEMOS_TRANSCRIPTION_SENSOR_INTERVAL_SECONDS,
)
def apple_voice_memos_transcription_backlog_sensor(context):
    active = skip_if_job_in_progress(context, job_name="apple_voice_memos_transcription_job")
    if isinstance(active, SkipReason):
        return active

    settings = load_settings(require_gmail=False, require_assemblyai=True)
    warehouse = warehouse_from_settings(settings)
    if not warehouse.load_untranscribed_apple_voice_memos_files(provider=ASSEMBLYAI_PROVIDER, limit=1):
        return SkipReason("No untranscribed Voice Memos found in Postgres.")

    return RunRequest(tags={"apple_voice_memos_trigger": "transcription_backlog"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[apple_voice_memos_transcription],
        jobs=[apple_voice_memos_transcription_job],
        sensors=[apple_voice_memos_transcription_backlog_sensor],
    )
