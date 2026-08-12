"""Deterministic text extraction / format classification for iMessage attachments.

Runs alongside (not inside) the agent vision pass: this one owns the formats the
vision pipeline does not — vCards and text files, which it parses into searchable
text, and app-extension payloads, video, and archives, which it retires with a
stable ``unsupported`` classification instead of leaving them invisible.

Deliberately its own asset rather than a pre-pass inside
``apple_messages_attachment_enrichment``: it has different cost characteristics
(no agent container, no model spend), different failure modes, and its own
before/after counts are what make attachment coverage auditable.
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

from personal_data_warehouse.attachment_text_extraction import (
    APPLE_MESSAGES_TEXT_SOURCE,
    DEFAULT_TEXT_EXTRACTION_BATCH_SIZE,
    AttachmentTextExtractionRunner,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.apple_messages_attachment_enrichment import (
    apple_messages_attachment_object_store_factory,
)
from personal_data_warehouse.defs.apple_messages_drive_ingest import apple_messages_drive_ingest
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

APPLE_MESSAGES_ATTACHMENT_TEXT_POSTGRES_LOCK_ID = 8_407_112_471
APPLE_MESSAGES_ATTACHMENT_TEXT_BATCH_SIZE_ENV = "APPLE_MESSAGES_ATTACHMENT_TEXT_BATCH_SIZE"


@asset(
    group_name="apple_messages",
    deps=[apple_messages_drive_ingest],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def apple_messages_attachment_text(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_apple_messages=True)
    if settings.apple_messages is None:
        raise RuntimeError("Apple Messages sync is not configured")

    batch_size = apple_messages_attachment_text_batch_size()
    warehouse = warehouse_from_settings(settings)
    warehouse.ensure_apple_messages_tables()
    with exclusive_sync_lock(
        name="apple_messages_attachment_text",
        postgres_lock_id=APPLE_MESSAGES_ATTACHMENT_TEXT_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning(
                "Skipping Apple Messages attachment text extraction because another run is already active"
            )
            summary = None
        else:
            summary = AttachmentTextExtractionRunner(
                source=APPLE_MESSAGES_TEXT_SOURCE,
                warehouse=warehouse,
                object_store_factory=apple_messages_attachment_object_store_factory(settings=settings),
                logger=context.log,
            ).sync(limit=batch_size if batch_size > 0 else None)

    return MaterializeResult(
        metadata={
            "attachments_seen": MetadataValue.int(summary.seen if summary else 0),
            "attachments_extracted": MetadataValue.int(summary.extracted if summary else 0),
            "attachments_classified": MetadataValue.int(summary.classified if summary else 0),
            "attachments_empty": MetadataValue.int(summary.empty if summary else 0),
            "attachments_failed": MetadataValue.int(summary.failed if summary else 0),
        }
    )


apple_messages_attachment_text_job = define_asset_job(
    "apple_messages_attachment_text_job",
    selection=[apple_messages_attachment_text],
)


@schedule(
    cron_schedule="23 * * * *",
    job=apple_messages_attachment_text_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def apple_messages_attachment_text_hourly(context):
    return skip_if_job_active(context, job_name="apple_messages_attachment_text_job")


def apple_messages_attachment_text_batch_size() -> int:
    value = os.getenv(APPLE_MESSAGES_ATTACHMENT_TEXT_BATCH_SIZE_ENV, "").strip()
    if not value:
        return DEFAULT_TEXT_EXTRACTION_BATCH_SIZE
    size = int(value)
    if size < 0:
        raise ValueError(f"{APPLE_MESSAGES_ATTACHMENT_TEXT_BATCH_SIZE_ENV} must be non-negative")
    return size


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[apple_messages_attachment_text],
        jobs=[apple_messages_attachment_text_job],
        schedules=[apple_messages_attachment_text_hourly],
    )
