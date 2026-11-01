from __future__ import annotations

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
from personal_data_warehouse.defs.scans_drive_ingest import scans_drive_ingest, scans_object_store
from personal_data_warehouse.file_attachment_enrichment import (
    DEFAULT_ATTACHMENT_ENRICHMENT_ERROR_WINDOW_DAYS,
    DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS,
    SCANS_SOURCE,
    FileAttachmentEnrichmentRunner,
    has_file_enrichment_candidate,
)
from personal_data_warehouse.schedule_guards import skip_if_job_active, skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

SCAN_ENRICHMENT_POSTGRES_LOCK_ID = 8_407_112_616
SCAN_ENRICHMENT_BATCH_SIZE = 25
SCAN_ENRICHMENT_SENSOR_INTERVAL_SECONDS = 60
SCAN_TEXT_MAX_CHARS = 20_000


def _settings():
    return load_settings(require_gmail=False, require_scans=True, require_agent=True)


@asset(
    group_name="scans",
    deps=[scans_drive_ingest],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def scan_enrichment(context, agent: AgentResource) -> MaterializeResult:
    """Read each scan's first page with the vision agent, so it is searchable."""
    settings = _settings()
    if settings.scans is None:
        raise RuntimeError("Scans are not configured")
    warehouse = warehouse_from_settings(settings)
    warehouse.ensure_scans_tables()
    with exclusive_sync_lock(
        name="scan_enrichment",
        postgres_lock_id=SCAN_ENRICHMENT_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping scan enrichment because another run is already active")
            summary = None
        else:
            summary = scan_enrichment_runner(
                settings=settings, warehouse=warehouse, logger=context.log, agent=agent
            ).sync(limit=SCAN_ENRICHMENT_BATCH_SIZE)
    return MaterializeResult(
        metadata={
            "attachments_seen": MetadataValue.int(summary.attachments_seen if summary else 0),
            "attachments_enriched": MetadataValue.int(summary.attachments_enriched if summary else 0),
            "attachments_not_useful": MetadataValue.int(summary.attachments_not_useful if summary else 0),
            "attachments_failed": MetadataValue.int(summary.attachments_failed if summary else 0),
        }
    )


scan_enrichment_job = define_asset_job("scan_enrichment_job", selection=[scan_enrichment])


@schedule(
    cron_schedule="41 * * * *",
    job=scan_enrichment_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def scan_enrichment_hourly(context):
    return skip_if_job_active(context, job_name="scan_enrichment_job")


@sensor(
    job=scan_enrichment_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=SCAN_ENRICHMENT_SENSOR_INTERVAL_SECONDS,
)
def scan_enrichment_backlog_sensor(context):
    active = skip_if_job_in_progress(context, job_name="scan_enrichment_job")
    if isinstance(active, SkipReason):
        return active
    try:
        settings = _settings()
    except ValueError as exc:
        return SkipReason(f"Scan enrichment is not configured: {exc}")
    if settings.scans is None:
        return SkipReason("Scans are not configured.")
    warehouse = warehouse_from_settings(settings)
    try:
        try:
            has_candidate = has_file_enrichment_candidate(
                warehouse,
                source=SCANS_SOURCE,
                provider=f"agent_{settings.agent.provider}",
                prompt_version=SCANS_SOURCE.prompt_version,
                max_error_attempts=DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS,
                error_window_days=DEFAULT_ATTACHMENT_ENRICHMENT_ERROR_WINDOW_DAYS,
            )
        except Exception as exc:
            # The tables may not exist before the first scan lands; never run
            # DDL from a sensor.
            return SkipReason(f"Scan enrichment tables are not ready yet: {exc}")
        if not has_candidate:
            return SkipReason("No scans are waiting for agent enrichment.")
    finally:
        warehouse.close()
    return RunRequest(tags={"scans_trigger": "enrichment_backlog"})


def scan_enrichment_runner(*, settings, warehouse, logger, agent: AgentResource | None = None) -> FileAttachmentEnrichmentRunner:
    if settings.agent is None:
        raise RuntimeError("Agent runner is not configured")
    agent_resource = agent if agent is not None and agent.is_configured else AgentResource.from_config(settings.agent)
    return FileAttachmentEnrichmentRunner(
        source=SCANS_SOURCE,
        warehouse=warehouse,
        agent=agent_resource,
        object_store_factory=scan_object_store_factory(settings=settings),
        logger=logger,
        provider=settings.agent.provider,
        model=settings.agent.model,
        text_max_chars=SCAN_TEXT_MAX_CHARS,
        max_error_attempts=DEFAULT_ATTACHMENT_ENRICHMENT_MAX_ERROR_ATTEMPTS,
        error_window_days=DEFAULT_ATTACHMENT_ENRICHMENT_ERROR_WINDOW_DAYS,
    )


def scan_object_store_factory(*, settings):
    # Every scan is in the one scans store, whatever account the runner asks for.
    store = scans_object_store(settings)

    def factory(_account: str):
        return store

    return factory


@definitions
def defs() -> Definitions:
    # The shared "agent" resource is registered once by
    # defs/apple_voice_memos_enrichment.py.
    return Definitions(
        assets=[scan_enrichment],
        jobs=[scan_enrichment_job],
        schedules=[scan_enrichment_hourly],
        sensors=[scan_enrichment_backlog_sensor],
    )
