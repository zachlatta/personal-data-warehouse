from __future__ import annotations

import os

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
from personal_data_warehouse.defs.voice_memos_drive_ingest import voice_memos_drive_ingest
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.voice_memos_drive_ingest import build_google_drive_service
from personal_data_warehouse.voice_memos_transcription import (
    AssemblyAIClient,
    GoogleDriveVoiceMemoAudioSource,
    VoiceMemosTranscriptionRunner,
)

VOICE_MEMOS_TRANSCRIPTION_POSTGRES_LOCK_ID = 7_403_111_840
DEFAULT_VOICE_MEMOS_TRANSCRIPTION_BATCH_SIZE = 3


@asset(
    group_name="voice_memos",
    deps=[voice_memos_drive_ingest],
    retry_policy=RetryPolicy(max_retries=2, delay=120),
)
def voice_memos_transcription(context) -> MaterializeResult:
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
    warehouse = ClickHouseWarehouse(settings.clickhouse_url or "")

    with exclusive_sync_lock(
        name="voice_memos_transcription",
        postgres_lock_id=VOICE_MEMOS_TRANSCRIPTION_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Voice Memos transcription because another run is already active")
            summary = None
        else:
            service = build_google_drive_service(account=settings.voice_memos.account, settings=settings)
            summary = VoiceMemosTranscriptionRunner(
                warehouse=warehouse,
                audio_source=GoogleDriveVoiceMemoAudioSource(service=service),
                transcription_client=AssemblyAIClient(
                    api_key=settings.assemblyai.api_key,
                    base_url=settings.assemblyai.base_url,
                    poll_interval_seconds=settings.assemblyai.poll_interval_seconds,
                    timeout_seconds=settings.assemblyai.timeout_seconds,
                    speaker_options={
                        key: value
                        for key, value in {
                            "min_speakers_expected": settings.assemblyai.min_speakers_expected,
                            "max_speakers_expected": settings.assemblyai.max_speakers_expected,
                        }.items()
                        if value is not None
                    },
                ),
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


voice_memos_transcription_job = define_asset_job(
    "voice_memos_transcription_job",
    selection="*voice_memos_transcription",
)


@schedule(
    cron_schedule="*/15 * * * *",
    job=voice_memos_transcription_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def voice_memos_transcription_every_fifteen_minutes(context):
    return skip_if_job_active(context, job_name="voice_memos_transcription_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[voice_memos_transcription],
        jobs=[voice_memos_transcription_job],
        schedules=[voice_memos_transcription_every_fifteen_minutes],
    )
