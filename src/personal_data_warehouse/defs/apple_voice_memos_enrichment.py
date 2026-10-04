from __future__ import annotations

import dataclasses
import os

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
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.calendar_sync import calendar_event_sync
from personal_data_warehouse.defs.apple_voice_memos_transcription import apple_voice_memos_transcription
from personal_data_warehouse.schedule_guards import skip_if_job_active, skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.apple_voice_memos_enrichment import (
    AGENT_ENRICHMENT_PROMPT_VERSION,
    DEFAULT_ENRICHMENT_MAX_ERROR_ATTEMPTS,
    ContainerAgentStructuredClient,
    VoiceMemosEnrichmentRunner,
    load_enrichment_candidates,
)
from personal_data_warehouse.warehouse import warehouse_from_settings

VOICE_MEMOS_ENRICHMENT_POSTGRES_LOCK_ID = 7_403_111_841
DEFAULT_VOICE_MEMOS_ENRICHMENT_BATCH_SIZE = 10
VOICE_MEMOS_ENRICHMENT_SENSOR_INTERVAL_SECONDS = 60
VOICE_MEMOS_ENRICHMENT_FORCE_PROMPT_VERSION_ENV = "VOICE_MEMOS_ENRICHMENT_FORCE_PROMPT_VERSION"
VOICE_MEMOS_ENRICHMENT_MAX_ERROR_ATTEMPTS_ENV = "VOICE_MEMOS_ENRICHMENT_MAX_ERROR_ATTEMPTS"
VOICE_MEMOS_ENRICHMENT_MODEL_ENV = "VOICE_MEMOS_ENRICHMENT_MODEL"
VOICE_MEMOS_ENRICHMENT_EFFORT_ENV = "VOICE_MEMOS_ENRICHMENT_REASONING_EFFORT"
# Voice enrichment runs on GPT-6 Astra, not the fleet default (AGENT_MODEL,
# gpt-5.6-sol in production): a multi-hour, many-speaker recording needs the
# agent to attribute speakers across hundreds of turns and research every
# name, and that is where the stronger model earns its cost. Override
# per-deployment with VOICE_MEMOS_ENRICHMENT_MODEL / _REASONING_EFFORT.
DEFAULT_VOICE_MEMOS_ENRICHMENT_MODEL = "gpt-6-astra"
UNCONFIGURED_AGENT_RESOURCE = AgentResource.disabled()


@asset(
    group_name="apple_voice_memos",
    deps=[apple_voice_memos_transcription, calendar_event_sync],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def apple_voice_memos_enrichment(context, agent: AgentResource) -> MaterializeResult:
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
    warehouse = warehouse_from_settings(settings)
    with exclusive_sync_lock(
        name="apple_voice_memos_enrichment",
        postgres_lock_id=VOICE_MEMOS_ENRICHMENT_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping Voice Memos enrichment because another run is already active")
            summary = None
        else:
            summary = VoiceMemosEnrichmentRunner(
                warehouse=warehouse,
                client=apple_voice_memos_enrichment_client(
                    settings=settings,
                    warehouse=warehouse,
                    logger=context.log,
                    agent=agent,
                ),
                logger=context.log,
                provider=apple_voice_memos_enrichment_provider(settings),
                prompt_version=apple_voice_memos_enrichment_prompt_version(),
                force_prompt_version=apple_voice_memos_enrichment_force_prompt_version(),
                max_error_attempts=apple_voice_memos_enrichment_max_error_attempts(),
            ).sync(limit=batch_size if batch_size > 0 else None)

    return MaterializeResult(
        metadata={
            "recordings_seen": MetadataValue.int(summary.recordings_seen if summary else 0),
            "recordings_enriched": MetadataValue.int(summary.recordings_enriched if summary else 0),
            "recordings_failed": MetadataValue.int(summary.recordings_failed if summary else 0),
        }
    )


apple_voice_memos_enrichment_job = define_asset_job(
    "apple_voice_memos_enrichment_job",
    selection="*apple_voice_memos_enrichment",
)


@schedule(
    cron_schedule="17 * * * *",
    job=apple_voice_memos_enrichment_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def apple_voice_memos_enrichment_hourly(context):
    return skip_if_job_active(context, job_name="apple_voice_memos_enrichment_job")


@sensor(
    job=apple_voice_memos_enrichment_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=VOICE_MEMOS_ENRICHMENT_SENSOR_INTERVAL_SECONDS,
)
def apple_voice_memos_enrichment_backlog_sensor(context):
    active = skip_if_job_in_progress(context, job_name="apple_voice_memos_enrichment_job")
    if isinstance(active, SkipReason):
        return active

    settings = load_settings(
        require_gmail=False,
        require_agent=True,
    )

    warehouse = warehouse_from_settings(settings)
    try:
        candidates = load_enrichment_candidates(
            warehouse,
            provider=apple_voice_memos_enrichment_provider(settings),
            prompt_version=apple_voice_memos_enrichment_prompt_version(),
            limit=1,
            force_prompt_version=apple_voice_memos_enrichment_force_prompt_version(),
            max_error_attempts=apple_voice_memos_enrichment_max_error_attempts(),
        )
        if not candidates:
            return SkipReason("No unenriched Voice Memos transcripts found in Postgres.")
    finally:
        warehouse.close()

    return RunRequest(tags={"apple_voice_memos_trigger": "enrichment_backlog"})


def apple_voice_memos_enrichment_provider(settings) -> str:
    return f"agent_{settings.agent.provider}"


def apple_voice_memos_enrichment_prompt_version() -> str:
    return AGENT_ENRICHMENT_PROMPT_VERSION


def apple_voice_memos_enrichment_force_prompt_version() -> bool:
    value = os.getenv(VOICE_MEMOS_ENRICHMENT_FORCE_PROMPT_VERSION_ENV, "")
    return value.strip().lower() in {"1", "true", "yes", "on"}


def apple_voice_memos_enrichment_max_error_attempts() -> int:
    value = os.getenv(VOICE_MEMOS_ENRICHMENT_MAX_ERROR_ATTEMPTS_ENV, "").strip()
    if not value:
        return DEFAULT_ENRICHMENT_MAX_ERROR_ATTEMPTS
    attempts = int(value)
    if attempts < 0:
        raise ValueError(f"{VOICE_MEMOS_ENRICHMENT_MAX_ERROR_ATTEMPTS_ENV} must be non-negative")
    return attempts


def voice_memo_agent_config(base):
    """The fleet AgentConfig with the voice-memo model/effort applied."""
    model = os.getenv(VOICE_MEMOS_ENRICHMENT_MODEL_ENV, "").strip() or DEFAULT_VOICE_MEMOS_ENRICHMENT_MODEL
    effort = os.getenv(VOICE_MEMOS_ENRICHMENT_EFFORT_ENV, "").strip() or base.reasoning_effort
    if model == base.model and effort == base.reasoning_effort:
        return base
    return dataclasses.replace(base, model=model, reasoning_effort=effort)


def apple_voice_memos_enrichment_client(*, settings, warehouse, logger, agent: AgentResource | None = None):
    if settings.agent is None:
        raise RuntimeError("Agent runner is not configured")
    config = voice_memo_agent_config(settings.agent)
    if config is settings.agent and agent is not None and agent.is_configured:
        agent_resource = agent
    else:
        # The injected fleet resource carries the fleet model and effort, so a
        # voice-specific config needs its own resource.
        agent_resource = AgentResource.from_config(config)
    return ContainerAgentStructuredClient(
        agent=agent_resource,
        provider=config.provider,
        # Recorded as the enrichment row's model -- must match what runs.
        model=config.model,
        warehouse=warehouse,
        logger=logger,
    )


def agent_resource_from_settings(settings) -> AgentResource:
    if settings.agent is None:
        raise RuntimeError("Agent runner is not configured")
    return AgentResource.from_config(settings.agent)


@definitions
def defs() -> Definitions:
    settings = load_settings(require_postgres=False, require_gmail=False)
    resources = {"agent": UNCONFIGURED_AGENT_RESOURCE}
    if settings.agent is not None:
        resources["agent"] = agent_resource_from_settings(settings)
    return Definitions(
        assets=[apple_voice_memos_enrichment],
        jobs=[apple_voice_memos_enrichment_job],
        schedules=[apple_voice_memos_enrichment_hourly],
        sensors=[apple_voice_memos_enrichment_backlog_sensor],
        resources=resources,
    )
