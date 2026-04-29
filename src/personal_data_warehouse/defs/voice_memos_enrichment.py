from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RunRequest,
    RetryPolicy,
    SkipReason,
    asset,
    define_asset_job,
    definitions,
    schedule,
    sensor,
)

from personal_data_warehouse.agent_resource import AgentResource
from personal_data_warehouse.clickhouse import ClickHouseWarehouse
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.calendar_sync import calendar_event_sync
from personal_data_warehouse.defs.voice_memos_transcription import voice_memos_transcription
from personal_data_warehouse.schedule_guards import skip_if_job_active, skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.voice_memos_enrichment import (
    AGENT_ENRICHMENT_PROMPT_VERSION,
    DEFAULT_ENRICHMENT_LOOKBACK_WEEKS,
    ContainerAgentStructuredClient,
    VoiceMemosEnrichmentRunner,
    load_enrichment_candidates,
)

VOICE_MEMOS_ENRICHMENT_POSTGRES_LOCK_ID = 7_403_111_841
DEFAULT_VOICE_MEMOS_ENRICHMENT_BATCH_SIZE = 0
VOICE_MEMOS_ENRICHMENT_SENSOR_INTERVAL_SECONDS = 60
UNCONFIGURED_AGENT_RESOURCE = AgentResource.disabled()


@asset(
    group_name="voice_memos",
    deps=[voice_memos_transcription, calendar_event_sync],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def voice_memos_enrichment(context, agent: AgentResource) -> MaterializeResult:
    settings = load_settings(
        require_gmail=False,
        require_agent=True,
    )

    batch_size = int(
        os.getenv(
            "VOICE_MEMOS_ENRICHMENT_BATCH_SIZE",
            str(DEFAULT_VOICE_MEMOS_ENRICHMENT_BATCH_SIZE),
        )
    )
    lookback_weeks = int(os.getenv("VOICE_MEMOS_ENRICHMENT_LOOKBACK_WEEKS", str(DEFAULT_ENRICHMENT_LOOKBACK_WEEKS)))
    if lookback_weeks < 1:
        raise ValueError("VOICE_MEMOS_ENRICHMENT_LOOKBACK_WEEKS must be at least 1")
    recorded_after = datetime.now(tz=UTC) - timedelta(weeks=lookback_weeks)
    warehouse = ClickHouseWarehouse(settings.clickhouse_url or "")
    with exclusive_sync_lock(
        name="voice_memos_enrichment",
        postgres_lock_id=VOICE_MEMOS_ENRICHMENT_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Voice Memos enrichment because another run is already active")
            summary = None
        else:
            summary = VoiceMemosEnrichmentRunner(
                warehouse=warehouse,
                client=voice_memos_enrichment_client(
                    settings=settings,
                    warehouse=warehouse,
                    logger=context.log,
                    agent=agent,
                ),
                logger=context.log,
                provider=voice_memos_enrichment_provider(settings),
                prompt_version=voice_memos_enrichment_prompt_version(),
            ).sync(limit=batch_size if batch_size > 0 else None, recorded_after=recorded_after)

    return MaterializeResult(
        metadata={
            "recordings_seen": MetadataValue.int(summary.recordings_seen if summary else 0),
            "recordings_enriched": MetadataValue.int(summary.recordings_enriched if summary else 0),
            "recordings_failed": MetadataValue.int(summary.recordings_failed if summary else 0),
            "lookback_weeks": MetadataValue.int(lookback_weeks),
        }
    )


voice_memos_enrichment_job = define_asset_job(
    "voice_memos_enrichment_job",
    selection="*voice_memos_enrichment",
)


@schedule(
    cron_schedule="17 * * * *",
    job=voice_memos_enrichment_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def voice_memos_enrichment_hourly(context):
    return skip_if_job_active(context, job_name="voice_memos_enrichment_job")


@sensor(
    job=voice_memos_enrichment_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=VOICE_MEMOS_ENRICHMENT_SENSOR_INTERVAL_SECONDS,
)
def voice_memos_enrichment_backlog_sensor(context):
    active = skip_if_job_in_progress(context, job_name="voice_memos_enrichment_job")
    if isinstance(active, SkipReason):
        return active

    settings = load_settings(
        require_gmail=False,
        require_agent=True,
    )

    lookback_weeks = int(os.getenv("VOICE_MEMOS_ENRICHMENT_LOOKBACK_WEEKS", str(DEFAULT_ENRICHMENT_LOOKBACK_WEEKS)))
    if lookback_weeks < 1:
        raise ValueError("VOICE_MEMOS_ENRICHMENT_LOOKBACK_WEEKS must be at least 1")
    recorded_after = datetime.now(tz=UTC) - timedelta(weeks=lookback_weeks)
    warehouse = ClickHouseWarehouse(settings.clickhouse_url or "")
    candidates = load_enrichment_candidates(
        warehouse,
        provider=voice_memos_enrichment_provider(settings),
        model=voice_memos_enrichment_model(settings),
        prompt_version=voice_memos_enrichment_prompt_version(),
        limit=1,
        recorded_after=recorded_after,
    )
    if not candidates:
        return SkipReason("No unenriched Voice Memos transcripts found in ClickHouse.")

    return RunRequest(tags={"voice_memos_trigger": "enrichment_backlog"})


def voice_memos_enrichment_provider(settings) -> str:
    return f"agent_{settings.agent.provider}"


def voice_memos_enrichment_model(settings) -> str:
    return settings.agent.model


def voice_memos_enrichment_prompt_version() -> str:
    return AGENT_ENRICHMENT_PROMPT_VERSION


def voice_memos_enrichment_client(*, settings, warehouse, logger, agent: AgentResource | None = None):
    if settings.agent is None:
        raise RuntimeError("Agent runner is not configured")
    agent_resource = agent if agent is not None and agent.is_configured else agent_resource_from_settings(settings)
    return ContainerAgentStructuredClient(
        agent=agent_resource,
        provider=settings.agent.provider,
        model=settings.agent.model,
        warehouse=warehouse,
        logger=logger,
    )


def agent_resource_from_settings(settings) -> AgentResource:
    if settings.agent is None:
        raise RuntimeError("Agent runner is not configured")
    return AgentResource.from_config(settings.agent)


@definitions
def defs() -> Definitions:
    settings = load_settings(require_gmail=False)
    resources = {"agent": UNCONFIGURED_AGENT_RESOURCE}
    if settings.agent is not None:
        resources["agent"] = agent_resource_from_settings(settings)
    return Definitions(
        assets=[voice_memos_enrichment],
        jobs=[voice_memos_enrichment_job],
        schedules=[voice_memos_enrichment_hourly],
        sensors=[voice_memos_enrichment_backlog_sensor],
        resources=resources,
    )
