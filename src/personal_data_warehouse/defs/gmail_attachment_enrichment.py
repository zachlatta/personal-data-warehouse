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
from personal_data_warehouse.config import GmailAccount, load_settings
from personal_data_warehouse.defs.gmail_sync import (
    build_attachment_object_store_factory,
    gmail_mailbox_sync,
)
from personal_data_warehouse.gmail_attachment_enrichment import (
    AGENT_ATTACHMENT_PROMPT_VERSION,
    DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS,
    GmailAttachmentEnrichmentRunner,
    load_attachment_enrichment_candidates,
)
from personal_data_warehouse.schedule_guards import skip_if_job_active, skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

GMAIL_ATTACHMENT_ENRICHMENT_POSTGRES_LOCK_ID = 7_403_111_842
DEFAULT_GMAIL_ATTACHMENT_ENRICHMENT_BATCH_SIZE = 25
GMAIL_ATTACHMENT_ENRICHMENT_SENSOR_INTERVAL_SECONDS = 120
GMAIL_ATTACHMENT_ENRICHMENT_BATCH_SIZE_ENV = "GMAIL_ATTACHMENT_ENRICHMENT_BATCH_SIZE"
GMAIL_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS_ENV = "GMAIL_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS"


@asset(
    group_name="gmail",
    deps=[gmail_mailbox_sync],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def gmail_attachment_enrichment(context, agent: AgentResource) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_agent=True)

    batch_size = gmail_attachment_enrichment_batch_size()
    warehouse = warehouse_from_settings(settings)
    with exclusive_sync_lock(
        name="gmail_attachment_enrichment",
        postgres_lock_id=GMAIL_ATTACHMENT_ENRICHMENT_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Gmail attachment enrichment because another run is already active")
            summary = None
        else:
            summary = gmail_attachment_enrichment_runner(
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


gmail_attachment_enrichment_job = define_asset_job(
    "gmail_attachment_enrichment_job",
    selection=[gmail_attachment_enrichment],
)


@schedule(
    cron_schedule="41 * * * *",
    job=gmail_attachment_enrichment_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def gmail_attachment_enrichment_hourly(context):
    return skip_if_job_active(context, job_name="gmail_attachment_enrichment_job")


@sensor(
    job=gmail_attachment_enrichment_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=GMAIL_ATTACHMENT_ENRICHMENT_SENSOR_INTERVAL_SECONDS,
)
def gmail_attachment_enrichment_backlog_sensor(context):
    active = skip_if_job_in_progress(context, job_name="gmail_attachment_enrichment_job")
    if isinstance(active, SkipReason):
        return active

    settings = load_settings(require_gmail=False, require_agent=True)
    warehouse = warehouse_from_settings(settings)
    candidates = load_attachment_enrichment_candidates(
        warehouse,
        provider=f"agent_{settings.agent.provider}",
        model=settings.agent.model,
        prompt_version=AGENT_ATTACHMENT_PROMPT_VERSION,
        limit=1,
        max_error_attempts=gmail_attachment_enrichment_max_error_attempts(),
    )
    if not candidates:
        return SkipReason("No Gmail attachments are waiting for agent enrichment.")

    return RunRequest(tags={"gmail_attachment_trigger": "enrichment_backlog"})


def gmail_attachment_enrichment_runner(
    *,
    settings,
    warehouse,
    logger,
    agent: AgentResource | None = None,
) -> GmailAttachmentEnrichmentRunner:
    if settings.agent is None:
        raise RuntimeError("Agent runner is not configured")
    agent_resource = agent if agent is not None and agent.is_configured else AgentResource.from_config(settings.agent)
    return GmailAttachmentEnrichmentRunner(
        warehouse=warehouse,
        agent=agent_resource,
        object_store_factory=gmail_attachment_object_store_factory(settings=settings, logger=logger),
        logger=logger,
        provider=settings.agent.provider,
        model=settings.agent.model,
        prompt_version=AGENT_ATTACHMENT_PROMPT_VERSION,
        text_max_chars=settings.gmail_attachment_text_max_chars,
        max_error_attempts=gmail_attachment_enrichment_max_error_attempts(),
    )


def gmail_attachment_object_store_factory(*, settings, logger):
    account_factory = build_attachment_object_store_factory(settings=settings, logger=logger)
    if account_factory is None:
        raise RuntimeError(
            "Gmail attachment blob storage is not configured; agent enrichment reads attachment bytes "
            "from the object store (set GMAIL_ATTACHMENT_GOOGLE_DRIVE_FOLDER_ID)"
        )

    def factory(account_email: str):
        return account_factory(GmailAccount(email_address=account_email))

    return factory


def gmail_attachment_enrichment_batch_size() -> int:
    return int(
        os.getenv(
            GMAIL_ATTACHMENT_ENRICHMENT_BATCH_SIZE_ENV,
            str(DEFAULT_GMAIL_ATTACHMENT_ENRICHMENT_BATCH_SIZE),
        )
    )


def gmail_attachment_enrichment_max_error_attempts() -> int:
    value = os.getenv(GMAIL_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS_ENV, "").strip()
    if not value:
        return DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS
    attempts = int(value)
    if attempts < 0:
        raise ValueError(f"{GMAIL_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS_ENV} must be non-negative")
    return attempts


@definitions
def defs() -> Definitions:
    # The shared "agent" resource is registered once by
    # defs/apple_voice_memos_enrichment.py; registering another instance here
    # would make the merged code location reject the duplicate key.
    return Definitions(
        assets=[gmail_attachment_enrichment],
        jobs=[gmail_attachment_enrichment_job],
        schedules=[gmail_attachment_enrichment_hourly],
        sensors=[gmail_attachment_enrichment_backlog_sensor],
    )
