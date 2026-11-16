"""One store/ingest/asset/sensor factory for paired device audio sources."""

from __future__ import annotations

from types import SimpleNamespace

from dagster import (
    DefaultSensorStatus,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    RunRequest,
    SkipReason,
    asset,
    define_asset_job,
    sensor,
)

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.paired_audio_inbox import PairedAudioIngestRunner
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.timeline_fast_lane import land_sources_on_timeline
from personal_data_warehouse.warehouse import warehouse_from_settings


def build_paired_audio_defs(inbox, lock_id):
    runtime = SimpleNamespace()

    def make_store(settings):
        """The source-tagged store for paired audio uploads.

        The same folder and account as every other voice upload (the app's
        per-source folder falls back to the shared one, as Voice Memos' does);
        only the pdw_source tag differs, so transcription's default voice store
        can fetch these bytes too.
        """
        return build_object_store(
            google_drive_spec(
                folder_id=settings.voice_memos.google_drive_folder_id,
                account=settings.voice_memos.account,
                source=inbox.source,
                blob_kind=inbox.audio_kind,
                metadata_kind=inbox.metadata_kind,
            ),
            settings=settings,
        )

    def ingest_inbox(*, settings, logger):
        """Promote paired audio and sidecars, under the ingest lock.

        The one ingest path: the asset below and the resident voice landing worker
        both call it. None when another run holds the lock.
        """
        if settings.voice_memos is None:
            raise RuntimeError(f"Voice Memos storage is not configured, so {inbox.label} has no Drive store")
        warehouse = warehouse_from_settings(settings)
        try:
            with exclusive_sync_lock(
                name=f"{inbox.source}_drive_ingest",
                postgres_lock_id=lock_id,
            ) as acquired:
                if not acquired:
                    logger.warning(f"Skipping {inbox.label} Drive ingest because another run is already active")
                    return None
                object_store = runtime.store(settings)
                return PairedAudioIngestRunner(
                    inbox=inbox,
                    warehouse=warehouse,
                    metadata_source=lambda: inbox.iter_metadata_payloads(object_store=object_store),
                    object_store=object_store,
                    logger=logger,
                ).sync()
        finally:
            warehouse.close()

    def inbox_has_metadata(settings) -> bool:
        if settings.voice_memos is None:
            return False
        return inbox.has_metadata_payloads(object_store=runtime.store(settings), stage="inbox")

    @asset(
        name=f"{inbox.source}_drive_ingest",
        group_name=inbox.source,
        retry_policy=RetryPolicy(max_retries=3, delay=60),
    )
    def ingest_asset(context) -> MaterializeResult:
        settings = load_settings(require_gmail=False, require_voice_memos=True)
        summary = runtime.ingest(settings=settings, logger=context.log)
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

    ingest_job = define_asset_job(
        f"{inbox.source}_drive_ingest_job",
        selection=[ingest_asset],
    )

    @sensor(
        name=f"{inbox.source}_drive_inbox_sensor",
        job=ingest_job,
        default_status=DefaultSensorStatus.RUNNING,
        minimum_interval_seconds=60,
    )
    def inbox_sensor(context):
        active = skip_if_job_in_progress(context, job_name=f"{inbox.source}_drive_ingest_job")
        if isinstance(active, SkipReason):
            return active
        settings = load_settings(require_gmail=False, require_voice_memos=True)
        if not runtime.has_metadata(settings):
            return SkipReason(f"No {inbox.label} inbox metadata found in object storage.")
        return RunRequest(tags={f"{inbox.source}_trigger": "drive_inbox"})

    runtime.store, runtime.ingest, runtime.has_metadata = make_store, ingest_inbox, inbox_has_metadata
    runtime.asset, runtime.job, runtime.sensor = ingest_asset, ingest_job, inbox_sensor
    return runtime
