from __future__ import annotations

import os

from dagster import (
    DefaultScheduleStatus,
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
    schedule,
    sensor,
)

from personal_data_warehouse.agent_resource import AgentResource
from personal_data_warehouse.apple_voice_memos_transcription import assemblyai_client_from_settings
from personal_data_warehouse.audio_attachment_enrichment import (
    DEFAULT_AUDIO_ENRICHMENT_ERROR_WINDOW_DAYS,
    DEFAULT_AUDIO_ENRICHMENT_MAX_ERROR_ATTEMPTS,
    WHATSAPP_AUDIO_SOURCE,
    AudioAttachmentTranscriptionRunner,
    has_audio_enrichment_candidate,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.whatsapp_drive_ingest import (
    _whatsapp_object_store,
    whatsapp_drive_ingest,
)
from personal_data_warehouse.schedule_guards import skip_if_job_active, skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

WHATSAPP_AUDIO_TRANSCRIPTION_POSTGRES_LOCK_ID = 8_407_112_464
DEFAULT_WHATSAPP_AUDIO_TRANSCRIPTION_BATCH_SIZE = 10
WHATSAPP_AUDIO_TRANSCRIPTION_SENSOR_INTERVAL_SECONDS = 120
WHATSAPP_AUDIO_TRANSCRIPTION_BATCH_SIZE_ENV = "WHATSAPP_AUDIO_TRANSCRIPTION_BATCH_SIZE"
WHATSAPP_AUDIO_TRANSCRIPTION_MAX_ERROR_ATTEMPTS_ENV = "WHATSAPP_AUDIO_TRANSCRIPTION_MAX_ERROR_ATTEMPTS"
WHATSAPP_AUDIO_TRANSCRIPTION_ERROR_WINDOW_DAYS_ENV = "WHATSAPP_AUDIO_TRANSCRIPTION_ERROR_WINDOW_DAYS"
WHATSAPP_AUDIO_TEXT_MAX_CHARS = 20_000


@asset(
    group_name="whatsapp",
    deps=[whatsapp_drive_ingest],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def whatsapp_audio_transcription(context, agent: AgentResource) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_whatsapp=True, require_assemblyai=True, require_agent=True)
    if settings.whatsapp is None:
        raise RuntimeError("WhatsApp sync is not configured")
    if settings.assemblyai is None:
        raise RuntimeError("AssemblyAI is not configured")

    batch_size = whatsapp_audio_transcription_batch_size()
    warehouse = warehouse_from_settings(settings)
    # The candidate scan reads whatsapp_media_items; ensure the source tables
    # exist even when the WhatsApp ingest has not promoted any media yet.
    warehouse.ensure_whatsapp_tables()
    with exclusive_sync_lock(
        name="whatsapp_audio_transcription",
        postgres_lock_id=WHATSAPP_AUDIO_TRANSCRIPTION_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping WhatsApp audio transcription because another run is already active")
            summary = None
        else:
            summary = whatsapp_audio_transcription_runner(
                settings=settings,
                warehouse=warehouse,
                logger=context.log,
                agent=agent,
            ).sync(limit=batch_size if batch_size > 0 else None)

    return MaterializeResult(
        metadata={
            "attachments_seen": MetadataValue.int(summary.attachments_seen if summary else 0),
            "attachments_enriched": MetadataValue.int(summary.attachments_enriched if summary else 0),
            "attachments_not_useful": MetadataValue.int(summary.attachments_not_useful if summary else 0),
            "attachments_failed": MetadataValue.int(summary.attachments_failed if summary else 0),
        }
    )


whatsapp_audio_transcription_job = define_asset_job(
    "whatsapp_audio_transcription_job",
    selection=[whatsapp_audio_transcription],
)


@schedule(
    cron_schedule="13 * * * *",
    job=whatsapp_audio_transcription_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def whatsapp_audio_transcription_hourly(context):
    return skip_if_job_active(context, job_name="whatsapp_audio_transcription_job")


@sensor(
    job=whatsapp_audio_transcription_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=WHATSAPP_AUDIO_TRANSCRIPTION_SENSOR_INTERVAL_SECONDS,
)
def whatsapp_audio_transcription_backlog_sensor(context):
    active = skip_if_job_in_progress(context, job_name="whatsapp_audio_transcription_job")
    if isinstance(active, SkipReason):
        return active

    try:
        settings = load_settings(require_gmail=False, require_whatsapp=True, require_assemblyai=True, require_agent=True)
    except ValueError as exc:
        return SkipReason(f"WhatsApp audio transcription is not configured: {exc}")
    if settings.whatsapp is None:
        return SkipReason("WhatsApp sync is not configured.")

    warehouse = warehouse_from_settings(settings)
    try:
        try:
            has_candidate = has_audio_enrichment_candidate(
                warehouse,
                source=WHATSAPP_AUDIO_SOURCE,
                provider=f"agent_{settings.agent.provider}",
                prompt_version=WHATSAPP_AUDIO_SOURCE.prompt_version,
                max_error_attempts=whatsapp_audio_transcription_max_error_attempts(),
                error_window_days=whatsapp_audio_transcription_error_window_days(),
            )
        except Exception as exc:
            # The media/enrichment tables may not exist yet on a brand-new deploy
            # (the WhatsApp ingest creates them on first promotion). Skip until then
            # rather than failing the sensor tick; never run DDL from a sensor.
            return SkipReason(f"WhatsApp audio transcription tables are not ready yet: {exc}")
        if not has_candidate:
            return SkipReason("No WhatsApp voice messages are waiting for transcription.")
    finally:
        warehouse.close()

    return RunRequest(tags={"whatsapp_audio_trigger": "transcription_backlog"})


def whatsapp_audio_transcription_runner(
    *,
    settings,
    warehouse,
    logger,
    agent: AgentResource | None = None,
) -> AudioAttachmentTranscriptionRunner:
    if settings.agent is None:
        raise RuntimeError("Agent runner is not configured")
    agent_resource = agent if agent is not None and agent.is_configured else AgentResource.from_config(settings.agent)
    return AudioAttachmentTranscriptionRunner(
        source=WHATSAPP_AUDIO_SOURCE,
        warehouse=warehouse,
        agent=agent_resource,
        object_store_factory=whatsapp_audio_object_store_factory(settings=settings),
        transcription_client=assemblyai_client_from_settings(settings),
        logger=logger,
        provider=settings.agent.provider,
        model=settings.agent.model,
        text_max_chars=WHATSAPP_AUDIO_TEXT_MAX_CHARS,
        max_error_attempts=whatsapp_audio_transcription_max_error_attempts(),
        error_window_days=whatsapp_audio_transcription_error_window_days(),
    )


def whatsapp_audio_object_store_factory(*, settings):
    # WhatsApp has a single account/object store; the runner keys its store
    # cache by account, so return the same store regardless of the account
    # string it passes in.
    store = _whatsapp_object_store(settings)

    def factory(_account: str):
        return store

    return factory


def whatsapp_audio_transcription_batch_size() -> int:
    return int(
        os.getenv(
            WHATSAPP_AUDIO_TRANSCRIPTION_BATCH_SIZE_ENV,
            str(DEFAULT_WHATSAPP_AUDIO_TRANSCRIPTION_BATCH_SIZE),
        )
    )


def whatsapp_audio_transcription_max_error_attempts() -> int:
    value = os.getenv(WHATSAPP_AUDIO_TRANSCRIPTION_MAX_ERROR_ATTEMPTS_ENV, "").strip()
    if not value:
        return DEFAULT_AUDIO_ENRICHMENT_MAX_ERROR_ATTEMPTS
    attempts = int(value)
    if attempts < 0:
        raise ValueError(f"{WHATSAPP_AUDIO_TRANSCRIPTION_MAX_ERROR_ATTEMPTS_ENV} must be non-negative")
    return attempts


def whatsapp_audio_transcription_error_window_days() -> int:
    value = os.getenv(WHATSAPP_AUDIO_TRANSCRIPTION_ERROR_WINDOW_DAYS_ENV, "").strip()
    if not value:
        return DEFAULT_AUDIO_ENRICHMENT_ERROR_WINDOW_DAYS
    days = int(value)
    if days < 0:
        raise ValueError(f"{WHATSAPP_AUDIO_TRANSCRIPTION_ERROR_WINDOW_DAYS_ENV} must be non-negative")
    return days


@definitions
def defs() -> Definitions:
    # The shared "agent" resource is registered once by
    # defs/apple_voice_memos_enrichment.py; registering another instance here
    # would make the merged code location reject the duplicate key.
    return Definitions(
        assets=[whatsapp_audio_transcription],
        jobs=[whatsapp_audio_transcription_job],
        schedules=[whatsapp_audio_transcription_hourly],
        sensors=[whatsapp_audio_transcription_backlog_sensor],
    )
