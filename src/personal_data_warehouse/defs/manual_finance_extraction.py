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
from personal_data_warehouse.defs.manual_finance_drive_ingest import (
    manual_finance_drive_ingest,
    manual_finance_object_store,
)
from personal_data_warehouse.manual_finance_extraction import (
    DEFAULT_ERROR_WINDOW_DAYS,
    DEFAULT_MAX_ERROR_ATTEMPTS,
    DEFAULT_RENDER_MAX_PAGES,
    PROMPT_VERSION,
    RENDER_MAX_PAGES_CEILING,
    ManualFinanceExtractionRunner,
    has_extraction_candidate,
)
from personal_data_warehouse.schedule_guards import skip_if_job_active, skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

MANUAL_FINANCE_EXTRACTION_POSTGRES_LOCK_ID = 7_403_111_853
MANUAL_FINANCE_EXTRACTION_SENSOR_INTERVAL_SECONDS = 120
DEFAULT_MANUAL_FINANCE_EXTRACTION_BATCH_SIZE = 25
MANUAL_FINANCE_EXTRACTION_BATCH_SIZE_ENV = "MANUAL_FINANCE_EXTRACTION_BATCH_SIZE"
MANUAL_FINANCE_EXTRACTION_MAX_ERROR_ATTEMPTS_ENV = "MANUAL_FINANCE_EXTRACTION_MAX_ERROR_ATTEMPTS"
MANUAL_FINANCE_EXTRACTION_ERROR_WINDOW_DAYS_ENV = "MANUAL_FINANCE_EXTRACTION_ERROR_WINDOW_DAYS"
MANUAL_FINANCE_RENDER_MAX_PAGES_ENV = "MANUAL_FINANCE_RENDER_MAX_PAGES"


@asset(
    group_name="finance",
    deps=[manual_finance_drive_ingest],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def manual_finance_extraction(context, agent: AgentResource) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_manual_finance=True, require_agent=True)
    if settings.manual_finance is None:
        raise RuntimeError("Manual finance ingest is not configured")

    batch_size = manual_finance_extraction_batch_size()
    warehouse = warehouse_from_settings(settings)
    with exclusive_sync_lock(
        name="manual_finance_extraction",
        postgres_lock_id=MANUAL_FINANCE_EXTRACTION_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning(
                "Skipping manual finance extraction because another run is already active"
            )
            summary = None
        else:
            summary = manual_finance_extraction_runner(
                settings=settings,
                warehouse=warehouse,
                logger=context.log,
                agent=agent,
            ).sync(limit=batch_size if batch_size > 0 else None)

    return MaterializeResult(
        metadata={
            "documents_seen": MetadataValue.int(summary.documents_seen if summary else 0),
            "documents_extracted": MetadataValue.int(summary.documents_extracted if summary else 0),
            "documents_not_useful": MetadataValue.int(summary.documents_not_useful if summary else 0),
            "documents_failed": MetadataValue.int(summary.documents_failed if summary else 0),
            "documents_unreadable": MetadataValue.int(summary.documents_unreadable if summary else 0),
        }
    )


manual_finance_extraction_job = define_asset_job(
    "manual_finance_extraction_job",
    selection=[manual_finance_extraction],
)


@schedule(
    cron_schedule="53 * * * *",
    job=manual_finance_extraction_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def manual_finance_extraction_hourly(context):
    return skip_if_job_active(context, job_name="manual_finance_extraction_job")


@sensor(
    job=manual_finance_extraction_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=MANUAL_FINANCE_EXTRACTION_SENSOR_INTERVAL_SECONDS,
)
def manual_finance_extraction_backlog_sensor(context):
    active = skip_if_job_in_progress(context, job_name="manual_finance_extraction_job")
    if isinstance(active, SkipReason):
        return active

    try:
        settings = load_settings(require_gmail=False, require_manual_finance=True, require_agent=True)
    except ValueError as exc:
        return SkipReason(f"Manual finance extraction is not configured: {exc}")
    if settings.manual_finance is None or settings.agent is None:
        return SkipReason("Manual finance extraction is not configured.")

    warehouse = warehouse_from_settings(settings)
    try:
        try:
            has_candidate = has_extraction_candidate(
                warehouse,
                provider=f"agent_{settings.agent.provider}",
                prompt_version=PROMPT_VERSION,
                max_error_attempts=manual_finance_extraction_max_error_attempts(),
                error_window_days=manual_finance_extraction_error_window_days(),
            )
        except Exception as exc:
            # The manual_finance tables may not exist yet on a fresh deploy.
            return SkipReason(f"Manual finance tables are not ready yet: {exc}")
        if not has_candidate:
            return SkipReason("No manual finance documents are waiting for extraction.")
    finally:
        warehouse.close()

    return RunRequest(tags={"finance_trigger": "extraction_backlog"})


def manual_finance_extraction_runner(
    *,
    settings,
    warehouse,
    logger,
    agent: AgentResource | None = None,
) -> ManualFinanceExtractionRunner:
    if settings.agent is None:
        raise RuntimeError("Agent runner is not configured")
    agent_resource = agent if agent is not None and agent.is_configured else AgentResource.from_config(settings.agent)
    return ManualFinanceExtractionRunner(
        warehouse=warehouse,
        agent=agent_resource,
        object_store_factory=lambda: manual_finance_object_store(settings),
        logger=logger,
        provider=settings.agent.provider,
        model=settings.agent.model,
        max_error_attempts=manual_finance_extraction_max_error_attempts(),
        error_window_days=manual_finance_extraction_error_window_days(),
        render_max_pages=manual_finance_render_max_pages(),
    )


def manual_finance_extraction_batch_size() -> int:
    return int(
        os.getenv(
            MANUAL_FINANCE_EXTRACTION_BATCH_SIZE_ENV,
            str(DEFAULT_MANUAL_FINANCE_EXTRACTION_BATCH_SIZE),
        )
    )


def manual_finance_extraction_max_error_attempts() -> int:
    value = os.getenv(MANUAL_FINANCE_EXTRACTION_MAX_ERROR_ATTEMPTS_ENV, "").strip()
    if not value:
        return DEFAULT_MAX_ERROR_ATTEMPTS
    attempts = int(value)
    if attempts < 0:
        raise ValueError(f"{MANUAL_FINANCE_EXTRACTION_MAX_ERROR_ATTEMPTS_ENV} must be non-negative")
    return attempts


def manual_finance_extraction_error_window_days() -> int:
    value = os.getenv(MANUAL_FINANCE_EXTRACTION_ERROR_WINDOW_DAYS_ENV, "").strip()
    if not value:
        return DEFAULT_ERROR_WINDOW_DAYS
    days = int(value)
    if days < 0:
        raise ValueError(f"{MANUAL_FINANCE_EXTRACTION_ERROR_WINDOW_DAYS_ENV} must be non-negative")
    return days


def manual_finance_render_max_pages() -> int:
    value = os.getenv(MANUAL_FINANCE_RENDER_MAX_PAGES_ENV, "").strip()
    if not value:
        return DEFAULT_RENDER_MAX_PAGES
    pages = int(value)
    if not 1 <= pages <= RENDER_MAX_PAGES_CEILING:
        raise ValueError(
            f"{MANUAL_FINANCE_RENDER_MAX_PAGES_ENV} must be between 1 and {RENDER_MAX_PAGES_CEILING}"
        )
    return pages


@definitions
def defs() -> Definitions:
    # The shared "agent" resource is registered once by
    # defs/apple_voice_memos_enrichment.py; registering another instance here
    # would make the merged code location reject the duplicate key.
    return Definitions(
        assets=[manual_finance_extraction],
        jobs=[manual_finance_extraction_job],
        schedules=[manual_finance_extraction_hourly],
        sensors=[manual_finance_extraction_backlog_sensor],
    )
