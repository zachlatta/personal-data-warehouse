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
from personal_data_warehouse.google_recorder_drive_ingest import (
    AUDIO_KIND,
    METADATA_KIND,
    SOURCE,
    GoogleRecorderDriveIngestRunner,
    GoogleRecorderDriveIngestSummary,
    has_metadata_payloads,
    iter_metadata_payloads,
)
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.timeline_fast_lane import land_sources_on_timeline
from personal_data_warehouse.warehouse import warehouse_from_settings

GOOGLE_RECORDER_DRIVE_INGEST_POSTGRES_LOCK_ID = 7_403_111_855
GOOGLE_RECORDER_SENSOR_INTERVAL_SECONDS = 60


def google_recorder_object_store(settings):
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


def ingest_google_recorder_inbox(*, settings, logger) -> GoogleRecorderDriveIngestSummary | None:
    """Promote the Drive inbox's Pixel recordings, under the ingest lock.

    The one ingest path: the asset below and the resident voice landing worker
    both call it. None when another run holds the lock.
    """
    if settings.voice_memos is None:
        raise RuntimeError("Voice Memos storage is not configured, so Google Recorder has no Drive store")
    warehouse = warehouse_from_settings(settings)
    try:
        with exclusive_sync_lock(
            name="google_recorder_drive_ingest",
            postgres_lock_id=GOOGLE_RECORDER_DRIVE_INGEST_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                logger.warning("Skipping Google Recorder Drive ingest because another run is already active")
                return None
            object_store = google_recorder_object_store(settings)
            return GoogleRecorderDriveIngestRunner(
                warehouse=warehouse,
                metadata_source=lambda: iter_metadata_payloads(object_store=object_store),
                object_store=object_store,
                logger=logger,
            ).sync()
    finally:
        warehouse.close()


def google_recorder_inbox_has_metadata(settings) -> bool:
    if settings.voice_memos is None:
        return False
    return has_metadata_payloads(object_store=google_recorder_object_store(settings), stage="inbox")


@asset(
    group_name="google_recorder",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def google_recorder_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_voice_memos=True)
    summary = ingest_google_recorder_inbox(settings=settings, logger=context.log)
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


google_recorder_drive_ingest_job = define_asset_job(
    "google_recorder_drive_ingest_job",
    selection=[google_recorder_drive_ingest],
)


@sensor(
    job=google_recorder_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=GOOGLE_RECORDER_SENSOR_INTERVAL_SECONDS,
)
def google_recorder_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="google_recorder_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active
    settings = load_settings(require_gmail=False, require_voice_memos=True)
    if not google_recorder_inbox_has_metadata(settings):
        return SkipReason("No Google Recorder inbox metadata found in object storage.")
    return RunRequest(tags={"google_recorder_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[google_recorder_drive_ingest],
        jobs=[google_recorder_drive_ingest_job],
        sensors=[google_recorder_drive_inbox_sensor],
    )
