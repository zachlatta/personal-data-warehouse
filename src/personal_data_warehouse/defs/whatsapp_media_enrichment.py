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
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.whatsapp_drive_ingest import (
    _whatsapp_object_store,
    whatsapp_drive_ingest,
)
from personal_data_warehouse.file_attachment_enrichment import (
    DEFAULT_ATTACHMENT_ENRICHMENT_ERROR_WINDOW_DAYS,
    DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS,
    WHATSAPP_SOURCE,
    FileAttachmentEnrichmentRunner,
    has_file_enrichment_candidate,
)
from personal_data_warehouse.schedule_guards import skip_if_job_active, skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

WHATSAPP_MEDIA_ENRICHMENT_POSTGRES_LOCK_ID = 8_407_112_467
DEFAULT_WHATSAPP_MEDIA_ENRICHMENT_BATCH_SIZE = 25
WHATSAPP_MEDIA_ENRICHMENT_SENSOR_INTERVAL_SECONDS = 120
WHATSAPP_MEDIA_ENRICHMENT_BATCH_SIZE_ENV = "WHATSAPP_MEDIA_ENRICHMENT_BATCH_SIZE"
WHATSAPP_MEDIA_ENRICHMENT_MAX_ERROR_ATTEMPTS_ENV = "WHATSAPP_MEDIA_ENRICHMENT_MAX_ERROR_ATTEMPTS"
WHATSAPP_MEDIA_ENRICHMENT_ERROR_WINDOW_DAYS_ENV = "WHATSAPP_MEDIA_ENRICHMENT_ERROR_WINDOW_DAYS"
WHATSAPP_MEDIA_TEXT_MAX_CHARS = 20_000


@asset(
    group_name="whatsapp",
    deps=[whatsapp_drive_ingest],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def whatsapp_media_enrichment(context, agent: AgentResource) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_whatsapp=True, require_agent=True)
    if settings.whatsapp is None:
        raise RuntimeError("WhatsApp sync is not configured")

    batch_size = whatsapp_media_enrichment_batch_size()
    warehouse = warehouse_from_settings(settings)
    # The candidate scan reads whatsapp_media_items; ensure the source tables
    # exist even when the WhatsApp ingest has not promoted any media yet.
    warehouse.ensure_whatsapp_tables()
    with exclusive_sync_lock(
        name="whatsapp_media_enrichment",
        postgres_lock_id=WHATSAPP_MEDIA_ENRICHMENT_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping WhatsApp media enrichment because another run is already active")
            summary = None
        else:
            summary = whatsapp_media_enrichment_runner(
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


whatsapp_media_enrichment_job = define_asset_job(
    "whatsapp_media_enrichment_job",
    selection=[whatsapp_media_enrichment],
)


@schedule(
    cron_schedule="47 * * * *",
    job=whatsapp_media_enrichment_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def whatsapp_media_enrichment_hourly(context):
    return skip_if_job_active(context, job_name="whatsapp_media_enrichment_job")


@sensor(
    job=whatsapp_media_enrichment_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=WHATSAPP_MEDIA_ENRICHMENT_SENSOR_INTERVAL_SECONDS,
)
def whatsapp_media_enrichment_backlog_sensor(context):
    active = skip_if_job_in_progress(context, job_name="whatsapp_media_enrichment_job")
    if isinstance(active, SkipReason):
        return active

    try:
        settings = load_settings(require_gmail=False, require_whatsapp=True, require_agent=True)
    except ValueError as exc:
        return SkipReason(f"WhatsApp media enrichment is not configured: {exc}")
    if settings.whatsapp is None:
        return SkipReason("WhatsApp sync is not configured.")

    warehouse = warehouse_from_settings(settings)
    try:
        has_candidate = has_file_enrichment_candidate(
            warehouse,
            source=WHATSAPP_SOURCE,
            provider=f"agent_{settings.agent.provider}",
            model=settings.agent.model,
            prompt_version=WHATSAPP_SOURCE.prompt_version,
            max_error_attempts=whatsapp_media_enrichment_max_error_attempts(),
            error_window_days=whatsapp_media_enrichment_error_window_days(),
        )
    except Exception as exc:
        # The media/enrichment tables may not exist yet on a brand-new deploy
        # (the WhatsApp ingest creates them on first promotion). Skip until then
        # rather than failing the sensor tick; never run DDL from a sensor.
        return SkipReason(f"WhatsApp media enrichment tables are not ready yet: {exc}")
    if not has_candidate:
        return SkipReason("No WhatsApp media is waiting for agent enrichment.")

    return RunRequest(tags={"whatsapp_media_trigger": "enrichment_backlog"})


def whatsapp_media_enrichment_runner(
    *,
    settings,
    warehouse,
    logger,
    agent: AgentResource | None = None,
) -> FileAttachmentEnrichmentRunner:
    if settings.agent is None:
        raise RuntimeError("Agent runner is not configured")
    agent_resource = agent if agent is not None and agent.is_configured else AgentResource.from_config(settings.agent)
    return FileAttachmentEnrichmentRunner(
        source=WHATSAPP_SOURCE,
        warehouse=warehouse,
        agent=agent_resource,
        object_store_factory=whatsapp_media_object_store_factory(settings=settings),
        logger=logger,
        provider=settings.agent.provider,
        model=settings.agent.model,
        text_max_chars=WHATSAPP_MEDIA_TEXT_MAX_CHARS,
        max_error_attempts=whatsapp_media_enrichment_max_error_attempts(),
        error_window_days=whatsapp_media_enrichment_error_window_days(),
    )


def whatsapp_media_object_store_factory(*, settings):
    # WhatsApp has a single account/object store; the enrichment runner keys its
    # store cache by account, so return the same store regardless of the account
    # string it passes in.
    store = _whatsapp_object_store(settings)

    def factory(_account: str):
        return store

    return factory


def whatsapp_media_enrichment_batch_size() -> int:
    return int(
        os.getenv(
            WHATSAPP_MEDIA_ENRICHMENT_BATCH_SIZE_ENV,
            str(DEFAULT_WHATSAPP_MEDIA_ENRICHMENT_BATCH_SIZE),
        )
    )


def whatsapp_media_enrichment_max_error_attempts() -> int:
    value = os.getenv(WHATSAPP_MEDIA_ENRICHMENT_MAX_ERROR_ATTEMPTS_ENV, "").strip()
    if not value:
        return DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS
    attempts = int(value)
    if attempts < 0:
        raise ValueError(f"{WHATSAPP_MEDIA_ENRICHMENT_MAX_ERROR_ATTEMPTS_ENV} must be non-negative")
    return attempts


def whatsapp_media_enrichment_error_window_days() -> int:
    value = os.getenv(WHATSAPP_MEDIA_ENRICHMENT_ERROR_WINDOW_DAYS_ENV, "").strip()
    if not value:
        return DEFAULT_ATTACHMENT_ENRICHMENT_ERROR_WINDOW_DAYS
    days = int(value)
    if days < 0:
        raise ValueError(f"{WHATSAPP_MEDIA_ENRICHMENT_ERROR_WINDOW_DAYS_ENV} must be non-negative")
    return days


@definitions
def defs() -> Definitions:
    # The shared "agent" resource is registered once by
    # defs/apple_voice_memos_enrichment.py; registering another instance here
    # would make the merged code location reject the duplicate key.
    return Definitions(
        assets=[whatsapp_media_enrichment],
        jobs=[whatsapp_media_enrichment_job],
        schedules=[whatsapp_media_enrichment_hourly],
        sensors=[whatsapp_media_enrichment_backlog_sensor],
    )
