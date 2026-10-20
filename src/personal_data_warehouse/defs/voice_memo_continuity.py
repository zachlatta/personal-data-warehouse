from __future__ import annotations

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
from personal_data_warehouse.apple_voice_memos_enrichment import ContainerAgentStructuredClient
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.apple_voice_memos_enrichment import apple_voice_memos_enrichment
from personal_data_warehouse.schedule_guards import skip_if_job_active, skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.voice_memo_continuity import (
    DEFAULT_CONTINUITY_MAX_GAP_SECONDS,
    VoiceMemoContinuityRunner,
    load_continuity_pair_candidates,
    load_session_merge_candidates,
)
from personal_data_warehouse.warehouse import warehouse_from_settings

VOICE_MEMO_CONTINUITY_POSTGRES_LOCK_ID = 8_407_112_504
DEFAULT_VOICE_MEMO_CONTINUITY_BATCH_SIZE = 20
VOICE_MEMO_CONTINUITY_SENSOR_INTERVAL_SECONDS = 120
VOICE_MEMO_CONTINUITY_ENABLED_ENV = "VOICE_MEMO_CONTINUITY_ENABLED"
VOICE_MEMO_CONTINUITY_MAX_GAP_SECONDS_ENV = "VOICE_MEMO_CONTINUITY_MAX_GAP_SECONDS"


@asset(
    group_name="apple_voice_memos",
    deps=[apple_voice_memos_enrichment],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def voice_memo_continuity(context, agent: AgentResource) -> MaterializeResult:
    """Join back-to-back recordings of one event into one session (marts_voice_memos.sessions)."""
    if not voice_memo_continuity_enabled():
        context.log.info("Voice memo continuity is disabled (%s=0)", VOICE_MEMO_CONTINUITY_ENABLED_ENV)
        return MaterializeResult(metadata={"enabled": MetadataValue.bool(False)})
    settings = load_settings(require_gmail=False, require_agent=True)
    batch_size = int(
        os.getenv("VOICE_MEMO_CONTINUITY_BATCH_SIZE", str(DEFAULT_VOICE_MEMO_CONTINUITY_BATCH_SIZE))
    )
    warehouse = warehouse_from_settings(settings)
    with exclusive_sync_lock(
        name="voice_memo_continuity",
        postgres_lock_id=VOICE_MEMO_CONTINUITY_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping voice memo continuity because another run is already active")
            summary = None
        else:
            summary = VoiceMemoContinuityRunner(
                warehouse=warehouse,
                client=voice_memo_continuity_client(settings=settings, warehouse=warehouse, logger=context.log, agent=agent),
                logger=context.log,
                provider=f"agent_{settings.agent.provider}",
                max_gap_seconds=voice_memo_continuity_max_gap_seconds(),
            ).sync(limit=batch_size if batch_size > 0 else None)

    return MaterializeResult(
        metadata={
            "pairs_seen": MetadataValue.int(summary.pairs_seen if summary else 0),
            "pairs_judged": MetadataValue.int(summary.pairs_judged if summary else 0),
            "pairs_same_event": MetadataValue.int(summary.pairs_same_event if summary else 0),
            "pairs_failed": MetadataValue.int(summary.pairs_failed if summary else 0),
            "sessions_seen": MetadataValue.int(summary.sessions_seen if summary else 0),
            "sessions_merged": MetadataValue.int(summary.sessions_merged if summary else 0),
            "sessions_failed": MetadataValue.int(summary.sessions_failed if summary else 0),
        }
    )


voice_memo_continuity_job = define_asset_job(
    "voice_memo_continuity_job",
    selection="voice_memo_continuity",
)


@schedule(
    cron_schedule="47 * * * *",
    job=voice_memo_continuity_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def voice_memo_continuity_hourly(context):
    return skip_if_job_active(context, job_name="voice_memo_continuity_job")


@sensor(
    job=voice_memo_continuity_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=VOICE_MEMO_CONTINUITY_SENSOR_INTERVAL_SECONDS,
)
def voice_memo_continuity_backlog_sensor(context):
    if not voice_memo_continuity_enabled():
        return SkipReason(f"{VOICE_MEMO_CONTINUITY_ENABLED_ENV}=0")
    active = skip_if_job_in_progress(context, job_name="voice_memo_continuity_job")
    if isinstance(active, SkipReason):
        return active

    settings = load_settings(require_gmail=False, require_agent=True)
    warehouse = warehouse_from_settings(settings)
    try:
        from datetime import UTC, datetime

        max_gap = voice_memo_continuity_max_gap_seconds()
        if not load_continuity_pair_candidates(warehouse, limit=1, max_gap_seconds=max_gap) and not (
            load_session_merge_candidates(warehouse, limit=1, now=datetime.now(tz=UTC), settle_seconds=max_gap)
        ):
            return SkipReason("No back-to-back voice recordings waiting to be judged or merged.")
    finally:
        warehouse.close()

    return RunRequest(tags={"apple_voice_memos_trigger": "continuity_backlog"})


def voice_memo_continuity_enabled() -> bool:
    return os.getenv(VOICE_MEMO_CONTINUITY_ENABLED_ENV, "1").strip().lower() not in {"0", "false", "no", "off"}


def voice_memo_continuity_max_gap_seconds() -> float:
    value = os.getenv(VOICE_MEMO_CONTINUITY_MAX_GAP_SECONDS_ENV, "").strip()
    return float(value) if value else float(DEFAULT_CONTINUITY_MAX_GAP_SECONDS)


def voice_memo_continuity_client(*, settings, warehouse, logger, agent: AgentResource | None = None):
    """The fleet agent: judging a seam is a short read, not the enrichment model's job."""
    if settings.agent is None:
        raise RuntimeError("Agent runner is not configured")
    agent_resource = agent if agent is not None and agent.is_configured else AgentResource.from_config(settings.agent)
    return ContainerAgentStructuredClient(
        agent=agent_resource,
        provider=settings.agent.provider,
        model=settings.agent.model,
        warehouse=warehouse,
        logger=logger,
    )


@definitions
def defs() -> Definitions:
    # The "agent" resource is the fleet one apple_voice_memos_enrichment's defs
    # declare; declaring a second instance under the same key fails the merge.
    return Definitions(
        assets=[voice_memo_continuity],
        jobs=[voice_memo_continuity_job],
        schedules=[voice_memo_continuity_hourly],
        sensors=[voice_memo_continuity_backlog_sensor],
    )
