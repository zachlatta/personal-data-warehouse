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
    APPLE_MESSAGES_AUDIO_SOURCE,
    DEFAULT_AUDIO_ENRICHMENT_ERROR_WINDOW_DAYS,
    DEFAULT_AUDIO_ENRICHMENT_MAX_ERROR_ATTEMPTS,
    AudioAttachmentTranscriptionRunner,
    has_audio_enrichment_candidate,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.apple_messages_drive_ingest import (
    _apple_messages_object_store,
    apple_messages_drive_ingest,
)
from personal_data_warehouse.schedule_guards import skip_if_job_active, skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

APPLE_MESSAGES_AUDIO_TRANSCRIPTION_POSTGRES_LOCK_ID = 8_407_112_463
DEFAULT_APPLE_MESSAGES_AUDIO_TRANSCRIPTION_BATCH_SIZE = 10
APPLE_MESSAGES_AUDIO_TRANSCRIPTION_SENSOR_INTERVAL_SECONDS = 120
APPLE_MESSAGES_AUDIO_TRANSCRIPTION_BATCH_SIZE_ENV = "APPLE_MESSAGES_AUDIO_TRANSCRIPTION_BATCH_SIZE"
APPLE_MESSAGES_AUDIO_TRANSCRIPTION_MAX_ERROR_ATTEMPTS_ENV = "APPLE_MESSAGES_AUDIO_TRANSCRIPTION_MAX_ERROR_ATTEMPTS"
APPLE_MESSAGES_AUDIO_TRANSCRIPTION_ERROR_WINDOW_DAYS_ENV = "APPLE_MESSAGES_AUDIO_TRANSCRIPTION_ERROR_WINDOW_DAYS"
APPLE_MESSAGES_AUDIO_TEXT_MAX_CHARS = 20_000


@asset(
    group_name="apple_messages",
    deps=[apple_messages_drive_ingest],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def apple_messages_audio_transcription(context, agent: AgentResource) -> MaterializeResult:
    settings = load_settings(
        require_gmail=False, require_apple_messages=True, require_assemblyai=True, require_agent=True
    )
    if settings.apple_messages is None:
        raise RuntimeError("Apple Messages sync is not configured")
    if settings.assemblyai is None:
        raise RuntimeError("AssemblyAI is not configured")

    batch_size = apple_messages_audio_transcription_batch_size()
    warehouse = warehouse_from_settings(settings)
    # The candidate scan reads apple_message_attachments; ensure the source
    # tables exist even when the Apple Messages ingest has not promoted any
    # attachments yet.
    warehouse.ensure_apple_messages_tables()
    with exclusive_sync_lock(
        name="apple_messages_audio_transcription",
        postgres_lock_id=APPLE_MESSAGES_AUDIO_TRANSCRIPTION_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Apple Messages audio transcription because another run is already active")
            summary = None
        else:
            summary = apple_messages_audio_transcription_runner(
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


apple_messages_audio_transcription_job = define_asset_job(
    "apple_messages_audio_transcription_job",
    selection=[apple_messages_audio_transcription],
)


@schedule(
    cron_schedule="59 * * * *",
    job=apple_messages_audio_transcription_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def apple_messages_audio_transcription_hourly(context):
    return skip_if_job_active(context, job_name="apple_messages_audio_transcription_job")


@sensor(
    job=apple_messages_audio_transcription_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=APPLE_MESSAGES_AUDIO_TRANSCRIPTION_SENSOR_INTERVAL_SECONDS,
)
def apple_messages_audio_transcription_backlog_sensor(context):
    active = skip_if_job_in_progress(context, job_name="apple_messages_audio_transcription_job")
    if isinstance(active, SkipReason):
        return active

    try:
        settings = load_settings(
            require_gmail=False, require_apple_messages=True, require_assemblyai=True, require_agent=True
        )
    except ValueError as exc:
        return SkipReason(f"Apple Messages audio transcription is not configured: {exc}")
    if settings.apple_messages is None:
        return SkipReason("Apple Messages sync is not configured.")

    warehouse = warehouse_from_settings(settings)
    try:
        try:
            has_candidate = has_audio_enrichment_candidate(
                warehouse,
                source=APPLE_MESSAGES_AUDIO_SOURCE,
                provider=f"agent_{settings.agent.provider}",
                model=settings.agent.model,
                prompt_version=APPLE_MESSAGES_AUDIO_SOURCE.prompt_version,
                max_error_attempts=apple_messages_audio_transcription_max_error_attempts(),
                error_window_days=apple_messages_audio_transcription_error_window_days(),
            )
        except Exception as exc:
            # The attachment/enrichment tables may not exist yet on a brand-new
            # deploy (Apple Messages ingest creates them on first promotion). Skip
            # until then rather than failing the sensor tick; never run DDL from a
            # sensor.
            return SkipReason(f"Apple Messages audio transcription tables are not ready yet: {exc}")
        if not has_candidate:
            return SkipReason("No Apple Messages voice messages are waiting for transcription.")
    finally:
        warehouse.close()

    return RunRequest(tags={"apple_messages_audio_trigger": "transcription_backlog"})


def apple_messages_audio_transcription_runner(
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
        source=APPLE_MESSAGES_AUDIO_SOURCE,
        warehouse=warehouse,
        agent=agent_resource,
        object_store_factory=apple_messages_audio_object_store_factory(settings=settings),
        transcription_client=assemblyai_client_from_settings(settings),
        logger=logger,
        provider=settings.agent.provider,
        model=settings.agent.model,
        text_max_chars=APPLE_MESSAGES_AUDIO_TEXT_MAX_CHARS,
        max_error_attempts=apple_messages_audio_transcription_max_error_attempts(),
        error_window_days=apple_messages_audio_transcription_error_window_days(),
    )


def apple_messages_audio_object_store_factory(*, settings):
    # Apple Messages has a single account/object store; the runner keys its
    # store cache by account, so return the same store regardless of the
    # account string it passes in.
    store = _apple_messages_object_store(settings)

    def factory(_account: str):
        return store

    return factory


def apple_messages_audio_transcription_batch_size() -> int:
    return int(
        os.getenv(
            APPLE_MESSAGES_AUDIO_TRANSCRIPTION_BATCH_SIZE_ENV,
            str(DEFAULT_APPLE_MESSAGES_AUDIO_TRANSCRIPTION_BATCH_SIZE),
        )
    )


def apple_messages_audio_transcription_max_error_attempts() -> int:
    value = os.getenv(APPLE_MESSAGES_AUDIO_TRANSCRIPTION_MAX_ERROR_ATTEMPTS_ENV, "").strip()
    if not value:
        return DEFAULT_AUDIO_ENRICHMENT_MAX_ERROR_ATTEMPTS
    attempts = int(value)
    if attempts < 0:
        raise ValueError(f"{APPLE_MESSAGES_AUDIO_TRANSCRIPTION_MAX_ERROR_ATTEMPTS_ENV} must be non-negative")
    return attempts


def apple_messages_audio_transcription_error_window_days() -> int:
    value = os.getenv(APPLE_MESSAGES_AUDIO_TRANSCRIPTION_ERROR_WINDOW_DAYS_ENV, "").strip()
    if not value:
        return DEFAULT_AUDIO_ENRICHMENT_ERROR_WINDOW_DAYS
    days = int(value)
    if days < 0:
        raise ValueError(f"{APPLE_MESSAGES_AUDIO_TRANSCRIPTION_ERROR_WINDOW_DAYS_ENV} must be non-negative")
    return days


@definitions
def defs() -> Definitions:
    # The shared "agent" resource is registered once by
    # defs/apple_voice_memos_enrichment.py; registering another instance here
    # would make the merged code location reject the duplicate key.
    return Definitions(
        assets=[apple_messages_audio_transcription],
        jobs=[apple_messages_audio_transcription_job],
        schedules=[apple_messages_audio_transcription_hourly],
        sensors=[apple_messages_audio_transcription_backlog_sensor],
    )
