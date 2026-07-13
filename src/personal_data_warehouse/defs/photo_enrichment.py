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
from personal_data_warehouse.defs.photo_identity import photo_identity
from personal_data_warehouse.defs.photos_drive_ingest import photos_object_store
from personal_data_warehouse.file_attachment_enrichment import (
    DEFAULT_ATTACHMENT_ENRICHMENT_ERROR_WINDOW_DAYS,
    DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS,
    PHOTOS_SOURCE,
    FileAttachmentEnrichmentRunner,
    has_file_enrichment_candidate,
)
from personal_data_warehouse.schedule_guards import skip_if_job_active, skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

PHOTO_ENRICHMENT_POSTGRES_LOCK_ID = 7_403_111_848
DEFAULT_PHOTO_ENRICHMENT_BATCH_SIZE = 25
PHOTO_ENRICHMENT_SENSOR_INTERVAL_SECONDS = 120
PHOTO_ENRICHMENT_BATCH_SIZE_ENV = "PHOTO_ENRICHMENT_BATCH_SIZE"
PHOTO_ENRICHMENT_MAX_ERROR_ATTEMPTS_ENV = "PHOTO_ENRICHMENT_MAX_ERROR_ATTEMPTS"
PHOTO_ENRICHMENT_ERROR_WINDOW_DAYS_ENV = "PHOTO_ENRICHMENT_ERROR_WINDOW_DAYS"
PHOTO_TEXT_MAX_CHARS = 20_000


@asset(
    group_name="photos",
    deps=[photo_identity],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def photo_enrichment(context, agent: AgentResource) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_photos=True, require_agent=True)
    if settings.photos is None:
        raise RuntimeError("Photo sync is not configured")

    batch_size = photo_enrichment_batch_size()
    warehouse = warehouse_from_settings(settings)
    # The candidate scan reads the photo marts; ensure the tables/views exist
    # even before the first Drive ingest promotes anything.
    warehouse.ensure_photos_tables()
    with exclusive_sync_lock(
        name="photo_enrichment",
        postgres_lock_id=PHOTO_ENRICHMENT_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping photo enrichment because another run is already active")
            summary = None
        else:
            summary = photo_enrichment_runner(
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


photo_enrichment_job = define_asset_job(
    "photo_enrichment_job",
    selection=[photo_enrichment],
)


@schedule(
    cron_schedule="23 * * * *",
    job=photo_enrichment_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def photo_enrichment_hourly(context):
    return skip_if_job_active(context, job_name="photo_enrichment_job")


@sensor(
    job=photo_enrichment_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=PHOTO_ENRICHMENT_SENSOR_INTERVAL_SECONDS,
)
def photo_enrichment_backlog_sensor(context):
    active = skip_if_job_in_progress(context, job_name="photo_enrichment_job")
    if isinstance(active, SkipReason):
        return active

    try:
        settings = load_settings(require_gmail=False, require_photos=True, require_agent=True)
    except ValueError as exc:
        return SkipReason(f"Photo enrichment is not configured: {exc}")
    if settings.photos is None:
        return SkipReason("Photo sync is not configured.")

    warehouse = warehouse_from_settings(settings)
    try:
        try:
            has_candidate = has_file_enrichment_candidate(
                warehouse,
                source=PHOTOS_SOURCE,
                provider=f"agent_{settings.agent.provider}",
                prompt_version=PHOTOS_SOURCE.prompt_version,
                max_error_attempts=photo_enrichment_max_error_attempts(),
                error_window_days=photo_enrichment_error_window_days(),
            )
        except Exception as exc:
            # The photo tables/views may not exist yet on a brand-new deploy.
            # Skip until then rather than failing the sensor tick; never run
            # DDL from a sensor.
            return SkipReason(f"Photo enrichment tables are not ready yet: {exc}")
        if not has_candidate:
            return SkipReason("No photos are waiting for agent enrichment.")
    finally:
        warehouse.close()

    return RunRequest(tags={"photos_trigger": "enrichment_backlog"})


def photo_enrichment_runner(
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
        source=PHOTOS_SOURCE,
        warehouse=warehouse,
        agent=agent_resource,
        object_store_factory=photo_object_store_factory(settings=settings),
        logger=logger,
        provider=settings.agent.provider,
        model=settings.agent.model,
        text_max_chars=PHOTO_TEXT_MAX_CHARS,
        max_error_attempts=photo_enrichment_max_error_attempts(),
        error_window_days=photo_enrichment_error_window_days(),
    )


def photo_object_store_factory(*, settings):
    # Photos use a single account/object store; the enrichment runner keys its
    # store cache by account, so return the same store regardless of the
    # account string it passes in.
    store = photos_object_store(settings)

    def factory(_account: str):
        return store

    return factory


def photo_enrichment_batch_size() -> int:
    return int(os.getenv(PHOTO_ENRICHMENT_BATCH_SIZE_ENV, str(DEFAULT_PHOTO_ENRICHMENT_BATCH_SIZE)))


def photo_enrichment_max_error_attempts() -> int:
    value = os.getenv(PHOTO_ENRICHMENT_MAX_ERROR_ATTEMPTS_ENV, "").strip()
    if not value:
        return DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS
    attempts = int(value)
    if attempts < 0:
        raise ValueError(f"{PHOTO_ENRICHMENT_MAX_ERROR_ATTEMPTS_ENV} must be non-negative")
    return attempts


def photo_enrichment_error_window_days() -> int:
    value = os.getenv(PHOTO_ENRICHMENT_ERROR_WINDOW_DAYS_ENV, "").strip()
    if not value:
        return DEFAULT_ATTACHMENT_ENRICHMENT_ERROR_WINDOW_DAYS
    days = int(value)
    if days < 0:
        raise ValueError(f"{PHOTO_ENRICHMENT_ERROR_WINDOW_DAYS_ENV} must be non-negative")
    return days


@definitions
def defs() -> Definitions:
    # The shared "agent" resource is registered once by
    # defs/apple_voice_memos_enrichment.py; registering another instance here
    # would make the merged code location reject the duplicate key.
    return Definitions(
        assets=[photo_enrichment],
        jobs=[photo_enrichment_job],
        schedules=[photo_enrichment_hourly],
        sensors=[photo_enrichment_backlog_sensor],
    )
