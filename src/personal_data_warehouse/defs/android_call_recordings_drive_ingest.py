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
from personal_data_warehouse.android_call_recordings_drive_ingest import (
    AUDIO_KIND,
    METADATA_KIND,
    SOURCE,
    AndroidCallRecordingsDriveIngestRunner,
    AndroidCallRecordingsDriveIngestSummary,
    has_metadata_payloads,
    iter_metadata_payloads,
)
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.timeline_fast_lane import land_sources_on_timeline
from personal_data_warehouse.warehouse import warehouse_from_settings

ANDROID_CALL_RECORDINGS_DRIVE_INGEST_POSTGRES_LOCK_ID = 7_403_111_856
ANDROID_CALL_RECORDINGS_SENSOR_INTERVAL_SECONDS = 60


def android_call_recordings_object_store(settings):
    """The Drive store the app files Pixel recordings in.

    The same folder and account as every other voice upload (the app's
    per-source folder falls back to the shared one, as Voice Memos' does);
    only the pdw_source tag differs, so transcription's default voice store
    can fetch these bytes too.
    """
    return build_object_store(
        google_drive_spec(
            folder_id=settings.voice_memos.google_drive_folder_id,
            account=settings.voice_memos.account,
            source=SOURCE,
            blob_kind=AUDIO_KIND,
            metadata_kind=METADATA_KIND,
        ),
        settings=settings,
    )


def ingest_android_call_recordings_inbox(*, settings, logger) -> AndroidCallRecordingsDriveIngestSummary | None:
    """Promote the Drive inbox's Pixel recordings, under the ingest lock.

    The one ingest path: the asset below and the resident voice landing worker
    both call it. None when another run holds the lock.
    """
    if settings.voice_memos is None:
        raise RuntimeError("Voice Memos storage is not configured, so Android call recordings has no Drive store")
    warehouse = warehouse_from_settings(settings)
    try:
        with exclusive_sync_lock(
            name="android_call_recordings_drive_ingest",
            postgres_lock_id=ANDROID_CALL_RECORDINGS_DRIVE_INGEST_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                logger.warning("Skipping Android call recordings Drive ingest because another run is already active")
                return None
            object_store = android_call_recordings_object_store(settings)
            return AndroidCallRecordingsDriveIngestRunner(
                warehouse=warehouse,
                metadata_source=lambda: iter_metadata_payloads(object_store=object_store),
                object_store=object_store,
                logger=logger,
            ).sync()
    finally:
        warehouse.close()


def android_call_recordings_inbox_has_metadata(settings) -> bool:
    if settings.voice_memos is None:
        return False
    return has_metadata_payloads(object_store=android_call_recordings_object_store(settings), stage="inbox")


@asset(
    group_name="android_call_recordings",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def android_call_recordings_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_voice_memos=True)
    summary = ingest_android_call_recordings_inbox(settings=settings, logger=context.log)
    fast_lane = {"enabled": False, "rows": 0}
    if summary is not None and summary.rows_written:
        fast_lane = land_sources_on_timeline(
            postgres_url=settings.postgres_database_url or "",
            sources=["voice_memos"],
            logger=context.log,
        )
    return MaterializeResult(
        metadata={
            "timeline_fast_lane": MetadataValue.json(fast_lane),
            "metadata_seen": MetadataValue.int(summary.metadata_seen if summary else 0),
            "rows_written": MetadataValue.int(summary.rows_written if summary else 0),
            "recordings_promoted": MetadataValue.int(summary.recordings_promoted if summary else 0),
        }
    )


android_call_recordings_drive_ingest_job = define_asset_job(
    "android_call_recordings_drive_ingest_job",
    selection=[android_call_recordings_drive_ingest],
)


@sensor(
    job=android_call_recordings_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=ANDROID_CALL_RECORDINGS_SENSOR_INTERVAL_SECONDS,
)
def android_call_recordings_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="android_call_recordings_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active
    settings = load_settings(require_gmail=False, require_voice_memos=True)
    if not android_call_recordings_inbox_has_metadata(settings):
        return SkipReason("No Android call recordings inbox metadata found in object storage.")
    return RunRequest(tags={"android_call_recordings_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[android_call_recordings_drive_ingest],
        jobs=[android_call_recordings_drive_ingest_job],
        sensors=[android_call_recordings_drive_inbox_sensor],
    )
