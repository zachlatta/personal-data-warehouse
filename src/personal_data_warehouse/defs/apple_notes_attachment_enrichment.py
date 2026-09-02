"""Vision and deterministic enrichment for Apple Notes attachments.

Both passes read the conformed ``marts_files.attachments`` relation. Audio
attachments deliberately do not run through the generic audio pass: Apple
Notes recordings are a first-class source in ``marts_voice_memos.recordings``
and the domain transcription/enrichment assets already process them there.
"""

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

from personal_data_warehouse.agent_resource import AgentResource
from personal_data_warehouse.attachment_text_extraction import (
    APPLE_NOTES_TEXT_SOURCE,
    DEFAULT_TEXT_EXTRACTION_BATCH_SIZE,
    AttachmentTextExtractionRunner,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.apple_notes_drive_ingest import (
    apple_notes_drive_ingest,
    apple_notes_object_store,
)
from personal_data_warehouse.file_attachment_enrichment import (
    APPLE_NOTES_SOURCE,
    DEFAULT_ATTACHMENT_ENRICHMENT_ERROR_WINDOW_DAYS,
    DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS,
    FileAttachmentEnrichmentRunner,
)
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

APPLE_NOTES_ATTACHMENT_ENRICHMENT_POSTGRES_LOCK_ID = 8_407_112_482
APPLE_NOTES_ATTACHMENT_TEXT_POSTGRES_LOCK_ID = 8_407_112_483
APPLE_NOTES_ATTACHMENT_ENRICHMENT_BATCH_SIZE_ENV = "APPLE_NOTES_ATTACHMENT_ENRICHMENT_BATCH_SIZE"
APPLE_NOTES_ATTACHMENT_TEXT_BATCH_SIZE_ENV = "APPLE_NOTES_ATTACHMENT_TEXT_BATCH_SIZE"
DEFAULT_APPLE_NOTES_ATTACHMENT_ENRICHMENT_BATCH_SIZE = 25


def apple_notes_attachment_object_store_factory(*, settings):
    store = apple_notes_object_store(settings)

    def factory(_account: str):
        return store

    return factory


def apple_notes_attachment_enrichment_batch_size() -> int:
    value = os.getenv(APPLE_NOTES_ATTACHMENT_ENRICHMENT_BATCH_SIZE_ENV, "").strip()
    size = int(value) if value else DEFAULT_APPLE_NOTES_ATTACHMENT_ENRICHMENT_BATCH_SIZE
    if size < 0:
        raise ValueError(f"{APPLE_NOTES_ATTACHMENT_ENRICHMENT_BATCH_SIZE_ENV} must be non-negative")
    return size


def apple_notes_attachment_text_batch_size() -> int:
    value = os.getenv(APPLE_NOTES_ATTACHMENT_TEXT_BATCH_SIZE_ENV, "").strip()
    size = int(value) if value else DEFAULT_TEXT_EXTRACTION_BATCH_SIZE
    if size < 0:
        raise ValueError(f"{APPLE_NOTES_ATTACHMENT_TEXT_BATCH_SIZE_ENV} must be non-negative")
    return size


@asset(
    group_name="apple_notes",
    deps=[apple_notes_drive_ingest],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def apple_notes_attachment_enrichment(context, agent: AgentResource) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_apple_notes=True, require_agent=True)
    if settings.apple_notes is None or settings.agent is None:
        raise RuntimeError("Apple Notes and the agent runner must be configured")
    warehouse = warehouse_from_settings(settings)
    warehouse.ensure_apple_notes_tables()
    with exclusive_sync_lock(
        name="apple_notes_attachment_enrichment",
        postgres_lock_id=APPLE_NOTES_ATTACHMENT_ENRICHMENT_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning(
                "Skipping Apple Notes attachment enrichment because another run is active"
            )
            summary = None
        else:
            agent_resource = (
                agent if agent.is_configured else AgentResource.from_config(settings.agent)
            )
            summary = FileAttachmentEnrichmentRunner(
                source=APPLE_NOTES_SOURCE,
                warehouse=warehouse,
                agent=agent_resource,
                object_store_factory=apple_notes_attachment_object_store_factory(settings=settings),
                logger=context.log,
                provider=settings.agent.provider,
                model=settings.agent.model,
                max_error_attempts=DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS,
                error_window_days=DEFAULT_ATTACHMENT_ENRICHMENT_ERROR_WINDOW_DAYS,
            ).sync(limit=apple_notes_attachment_enrichment_batch_size() or None)

    return MaterializeResult(
        metadata={
            "attachments_seen": MetadataValue.int(summary.attachments_seen if summary else 0),
            "attachments_enriched": MetadataValue.int(
                summary.attachments_enriched if summary else 0
            ),
            "attachments_not_useful": MetadataValue.int(
                summary.attachments_not_useful if summary else 0
            ),
            "attachments_failed": MetadataValue.int(summary.attachments_failed if summary else 0),
        }
    )


@asset(
    group_name="apple_notes",
    deps=[apple_notes_drive_ingest],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def apple_notes_attachment_text(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_apple_notes=True)
    if settings.apple_notes is None:
        raise RuntimeError("Apple Notes sync is not configured")
    warehouse = warehouse_from_settings(settings)
    warehouse.ensure_apple_notes_tables()
    with exclusive_sync_lock(
        name="apple_notes_attachment_text",
        postgres_lock_id=APPLE_NOTES_ATTACHMENT_TEXT_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning(
                "Skipping Apple Notes attachment text extraction because another run is active"
            )
            summary = None
        else:
            summary = AttachmentTextExtractionRunner(
                source=APPLE_NOTES_TEXT_SOURCE,
                warehouse=warehouse,
                object_store_factory=apple_notes_attachment_object_store_factory(settings=settings),
                logger=context.log,
            ).sync(limit=apple_notes_attachment_text_batch_size() or None)

    return MaterializeResult(
        metadata={
            "attachments_seen": MetadataValue.int(summary.seen if summary else 0),
            "attachments_extracted": MetadataValue.int(summary.extracted if summary else 0),
            "attachments_classified": MetadataValue.int(summary.classified if summary else 0),
            "attachments_empty": MetadataValue.int(summary.empty if summary else 0),
            "attachments_failed": MetadataValue.int(summary.failed if summary else 0),
        }
    )


apple_notes_attachment_enrichment_job = define_asset_job(
    "apple_notes_attachment_enrichment_job", selection=[apple_notes_attachment_enrichment]
)
apple_notes_attachment_text_job = define_asset_job(
    "apple_notes_attachment_text_job", selection=[apple_notes_attachment_text]
)


@schedule(
    cron_schedule="7 * * * *",
    job=apple_notes_attachment_enrichment_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def apple_notes_attachment_enrichment_hourly(context):
    return skip_if_job_active(context, job_name="apple_notes_attachment_enrichment_job")


@schedule(
    cron_schedule="37 * * * *",
    job=apple_notes_attachment_text_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def apple_notes_attachment_text_hourly(context):
    return skip_if_job_active(context, job_name="apple_notes_attachment_text_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[apple_notes_attachment_enrichment, apple_notes_attachment_text],
        jobs=[apple_notes_attachment_enrichment_job, apple_notes_attachment_text_job],
        schedules=[
            apple_notes_attachment_enrichment_hourly,
            apple_notes_attachment_text_hourly,
        ],
    )
