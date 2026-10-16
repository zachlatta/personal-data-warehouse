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
from personal_data_warehouse.defs.alice_voice_recordings import alice_object_store
from personal_data_warehouse.defs.apple_notes_drive_ingest import apple_notes_object_store
from personal_data_warehouse.defs.apple_voice_memos_drive_ingest import apple_voice_memos_drive_ingest
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.timeline_fast_lane import land_sources_on_timeline
from personal_data_warehouse.apple_voice_memos_transcription import (
    ASSEMBLYAI_PROVIDER,
    GoogleDriveVoiceMemoAudioSource,
    VoiceMemosTranscriptionRunner,
    VoiceMemosTranscriptionSummary,
    assemblyai_client_from_settings,
)
from personal_data_warehouse.warehouse import warehouse_from_settings

VOICE_MEMOS_TRANSCRIPTION_POSTGRES_LOCK_ID = 7_403_111_840
DEFAULT_VOICE_MEMOS_TRANSCRIPTION_BATCH_SIZE = 3
VOICE_MEMOS_TRANSCRIPTION_SENSOR_INTERVAL_SECONDS = 60


def transcribe_voice_recordings(*, settings, logger, limit: int) -> VoiceMemosTranscriptionSummary | None:
    """Transcribe up to ``limit`` waiting recordings from every voice source, under the lock.

    The one transcription path: the asset below and the resident landing worker
    (``voice_memo_landing_worker``) both call it. None when another run holds
    the lock.
    """
    if settings.voice_memos is None:
        raise RuntimeError("Voice Memos sync is not configured")
    if settings.assemblyai is None:
        raise RuntimeError("AssemblyAI is not configured")
    warehouse = warehouse_from_settings(settings)
    try:
        with exclusive_sync_lock(
            name="apple_voice_memos_transcription",
            postgres_lock_id=VOICE_MEMOS_TRANSCRIPTION_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                logger.warning("Skipping Voice Memos transcription because another run is already active")
                return None
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
            # Transcription is a domain pass over marts_voice_memos.recordings,
            # not an Apple pass, so it must be able to fetch any voice source's
            # bytes. Alice recordings normally live in the same Drive account
            # (their config falls back to it), but not necessarily, so its
            # store is built when it is configured rather than assumed.
            object_stores = {}
            if settings.alice_voice_recordings is not None:
                object_stores["alice_voice_recordings"] = alice_object_store(
                    settings.alice_voice_recordings, settings
                )
            # Audio saved in Apple Notes lives in the notes source's own Drive
            # objects; without this store the mart would list those recordings
            # and the runner could not fetch a single byte of them.
            if settings.apple_notes is not None:
                object_stores["apple_notes"] = apple_notes_object_store(settings)
            return VoiceMemosTranscriptionRunner(
                warehouse=warehouse,
                audio_source=GoogleDriveVoiceMemoAudioSource(
                    object_store=object_store,
                    object_stores=object_stores,
                ),
                transcription_client=assemblyai_client_from_settings(settings),
                logger=logger,
            ).sync(limit=limit)
    finally:
        warehouse.close()


def voice_recordings_await_transcription(settings) -> bool:
    warehouse = warehouse_from_settings(settings)
    try:
        return bool(warehouse.load_untranscribed_voice_recordings(provider=ASSEMBLYAI_PROVIDER, limit=1))
    finally:
        warehouse.close()


def voice_memos_transcription_batch_size() -> int:
    return int(
        os.getenv(
            "VOICE_MEMOS_TRANSCRIPTION_BATCH_SIZE",
            str(DEFAULT_VOICE_MEMOS_TRANSCRIPTION_BATCH_SIZE),
        )
    )


@asset(
    group_name="apple_voice_memos",
    deps=[apple_voice_memos_drive_ingest],
    retry_policy=RetryPolicy(max_retries=2, delay=120),
)
def apple_voice_memos_transcription(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_voice_memos=True, require_assemblyai=True)
    summary = transcribe_voice_recordings(
        settings=settings,
        logger=context.log,
        limit=voice_memos_transcription_batch_size(),
    )
    # A transcript changes the recording's timeline row; land it now rather
    # than at the next five-minute timeline_sync tick.
    fast_lane = {"enabled": False, "rows": 0}
    if summary is not None and summary.recordings_transcribed:
        fast_lane = land_sources_on_timeline(
            postgres_url=settings.postgres_database_url or "",
            sources=["voice_memos"],
            logger=context.log,
        )

    return MaterializeResult(
        metadata={
            "timeline_fast_lane": MetadataValue.json(fast_lane),
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
    if not voice_recordings_await_transcription(settings):
        return SkipReason("No untranscribed Voice Memos found in Postgres.")

    return RunRequest(tags={"apple_voice_memos_trigger": "transcription_backlog"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[apple_voice_memos_transcription],
        jobs=[apple_voice_memos_transcription_job],
        sensors=[apple_voice_memos_transcription_backlog_sensor],
    )
