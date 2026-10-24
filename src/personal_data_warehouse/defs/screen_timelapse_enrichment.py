from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta
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
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_active, skip_if_job_in_progress
from personal_data_warehouse.screen_timelapse_enrichment import (
    DEFAULT_SCREEN_DESCRIPTION_LOOKBACK,
    DEFAULT_SCREEN_DESCRIPTION_MAX_ERROR_ATTEMPTS,
    DEFAULT_TREE_TEXT_BACKFILL_LIMIT,
    SCREEN_DESCRIPTION_PROMPT_VERSION,
    ScreenTimelapseEnrichmentRunner,
    backfill_accessibility_tree_text,
    load_screen_description_candidates,
)
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

SCREEN_TIMELAPSE_ENRICHMENT_POSTGRES_LOCK_ID = 8_407_112_505
DEFAULT_SCREEN_TIMELAPSE_ENRICHMENT_BATCH_SIZE = 30
SCREEN_TIMELAPSE_ENRICHMENT_SENSOR_INTERVAL_SECONDS = 120
SCREEN_TIMELAPSE_ENRICHMENT_ENABLED_ENV = "SCREEN_TIMELAPSE_ENRICHMENT_ENABLED"
SCREEN_TIMELAPSE_ENRICHMENT_BATCH_SIZE_ENV = "SCREEN_TIMELAPSE_ENRICHMENT_BATCH_SIZE"
# How far back frames are read. 24 hours while the prompt is iterated on a
# day of frames; widen it to backfill the history.
SCREEN_TIMELAPSE_ENRICHMENT_LOOKBACK_HOURS_ENV = "SCREEN_TIMELAPSE_ENRICHMENT_LOOKBACK_HOURS"
SCREEN_TIMELAPSE_ENRICHMENT_MAX_ERROR_ATTEMPTS_ENV = "SCREEN_TIMELAPSE_ENRICHMENT_MAX_ERROR_ATTEMPTS"
SCREEN_TIMELAPSE_ENRICHMENT_MODEL_ENV = "SCREEN_TIMELAPSE_ENRICHMENT_MODEL"
SCREEN_TIMELAPSE_ENRICHMENT_EFFORT_ENV = "SCREEN_TIMELAPSE_ENRICHMENT_REASONING_EFFORT"
# A frame a minute per device while in use means many short reads, so this
# pass runs at low effort unless told otherwise: the photo eval of 2026-10-11
# found gpt-6.1-sol low as good as higher efforts for describing an image, at
# a fraction of the time.
DEFAULT_SCREEN_TIMELAPSE_ENRICHMENT_EFFORT = "low"
SCREEN_TIMELAPSE_SOURCE = "screen_timelapse"


@asset(
    group_name="screen_timelapse",
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def screen_timelapse_enrichment(context, agent: AgentResource) -> MaterializeResult:
    """Say what was on each recent screen frame (marts_screen_timelapse.active_screens)."""
    if not screen_timelapse_enrichment_enabled():
        context.log.info("Screen timelapse enrichment is disabled (%s=0)", SCREEN_TIMELAPSE_ENRICHMENT_ENABLED_ENV)
        return MaterializeResult(metadata={"enabled": MetadataValue.bool(False)})
    settings = load_settings(require_gmail=False, require_agent=True, require_voice_memos=True)
    warehouse = warehouse_from_settings(settings)
    with exclusive_sync_lock(
        name="screen_timelapse_enrichment",
        postgres_lock_id=SCREEN_TIMELAPSE_ENRICHMENT_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping screen timelapse enrichment because another run is already active")
            return MaterializeResult(metadata={"skipped": MetadataValue.bool(True)})
        store = screen_timelapse_object_store(settings)
        warehouse.ensure_screen_timelapse_tables()
        trees_filled = backfill_accessibility_tree_text(
            warehouse, store, limit=DEFAULT_TREE_TEXT_BACKFILL_LIMIT, logger=context.log
        )
        summary = screen_timelapse_enrichment_runner(
            settings=settings,
            warehouse=warehouse,
            object_store=store,
            logger=context.log,
            agent=agent,
        ).sync(limit=screen_timelapse_enrichment_batch_size())

    return MaterializeResult(
        metadata={
            "trees_text_filled": MetadataValue.int(trees_filled),
            "frames_seen": MetadataValue.int(summary.frames_seen),
            "frames_described": MetadataValue.int(summary.frames_described),
            "frames_reused": MetadataValue.int(summary.frames_reused),
            "frames_active": MetadataValue.int(summary.frames_active),
            "frames_failed": MetadataValue.int(summary.frames_failed),
        }
    )


screen_timelapse_enrichment_job = define_asset_job(
    "screen_timelapse_enrichment_job",
    selection=[screen_timelapse_enrichment],
    # A run reads at most one batch; an hour is far past any healthy one.
    tags={"dagster/max_runtime": "3600"},
)


@schedule(
    cron_schedule="17 * * * *",
    job=screen_timelapse_enrichment_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def screen_timelapse_enrichment_hourly(context):
    return skip_if_job_active(context, job_name="screen_timelapse_enrichment_job")


@sensor(
    job=screen_timelapse_enrichment_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=SCREEN_TIMELAPSE_ENRICHMENT_SENSOR_INTERVAL_SECONDS,
)
def screen_timelapse_enrichment_backlog_sensor(context):
    if not screen_timelapse_enrichment_enabled():
        return SkipReason(f"{SCREEN_TIMELAPSE_ENRICHMENT_ENABLED_ENV}=0")
    active = skip_if_job_in_progress(context, job_name="screen_timelapse_enrichment_job")
    if isinstance(active, SkipReason):
        return active
    try:
        settings = load_settings(require_gmail=False, require_agent=True, require_voice_memos=True)
    except ValueError as exc:
        return SkipReason(f"Screen timelapse enrichment is not configured: {exc}")
    warehouse = warehouse_from_settings(settings)
    try:
        try:
            waiting = load_screen_description_candidates(
                warehouse,
                prompt_version=SCREEN_DESCRIPTION_PROMPT_VERSION,
                since=datetime.now(tz=UTC) - screen_timelapse_enrichment_lookback(),
                limit=1,
                max_error_attempts=screen_timelapse_enrichment_max_error_attempts(),
            )
        except Exception as exc:
            # The descriptions table appears on the first run; never run DDL
            # from a sensor.
            return SkipReason(f"Screen timelapse tables are not ready yet: {exc}")
        if not waiting:
            return SkipReason("No screen timelapse frames are waiting to be read.")
    finally:
        warehouse.close()
    return RunRequest(tags={"screen_timelapse_trigger": "enrichment_backlog"})


def screen_timelapse_object_store(settings):
    """The Drive store the app files screen timelapse frames in.

    Reading by storage_file_id needs only the account the app writes with,
    which is the voice uploads' account (google_recorder_object_store reads
    the app's uploads the same way).
    """
    return build_object_store(
        google_drive_spec(
            folder_id=settings.voice_memos.google_drive_folder_id,
            account=settings.voice_memos.account,
            source=SCREEN_TIMELAPSE_SOURCE,
            blob_kind="screen_timelapse_screenshot",
            metadata_kind="screen_timelapse_accessibility_tree",
        ),
        settings=settings,
    )


def screen_timelapse_agent_config(base):
    """The fleet AgentConfig with this pass's model and effort applied."""
    model = os.getenv(SCREEN_TIMELAPSE_ENRICHMENT_MODEL_ENV, "").strip()
    effort = os.getenv(SCREEN_TIMELAPSE_ENRICHMENT_EFFORT_ENV, "").strip() or DEFAULT_SCREEN_TIMELAPSE_ENRICHMENT_EFFORT
    return dataclasses.replace(base, model=model or base.model, reasoning_effort=effort)


def screen_timelapse_enrichment_runner(
    *, settings, warehouse, object_store, logger, agent: AgentResource | None = None
) -> ScreenTimelapseEnrichmentRunner:
    if settings.agent is None:
        raise RuntimeError("Agent runner is not configured")
    config = screen_timelapse_agent_config(settings.agent)
    if config == settings.agent and agent is not None and agent.is_configured:
        agent_resource = agent
    else:
        # The injected fleet resource carries the fleet effort; this pass
        # needs its own.
        agent_resource = AgentResource.from_config(config)
    return ScreenTimelapseEnrichmentRunner(
        warehouse=warehouse,
        agent=agent_resource,
        object_store=object_store,
        logger=logger,
        provider=config.provider,
        model=config.model,
        lookback=screen_timelapse_enrichment_lookback(),
        max_error_attempts=screen_timelapse_enrichment_max_error_attempts(),
    )


def screen_timelapse_enrichment_enabled() -> bool:
    value = os.getenv(SCREEN_TIMELAPSE_ENRICHMENT_ENABLED_ENV, "1").strip().lower()
    return value not in {"0", "false", "no", "off"}


def screen_timelapse_enrichment_batch_size() -> int:
    value = os.getenv(SCREEN_TIMELAPSE_ENRICHMENT_BATCH_SIZE_ENV, "").strip()
    return int(value) if value else DEFAULT_SCREEN_TIMELAPSE_ENRICHMENT_BATCH_SIZE


def screen_timelapse_enrichment_lookback() -> timedelta:
    value = os.getenv(SCREEN_TIMELAPSE_ENRICHMENT_LOOKBACK_HOURS_ENV, "").strip()
    if not value:
        return DEFAULT_SCREEN_DESCRIPTION_LOOKBACK
    hours = float(value)
    if hours <= 0:
        raise ValueError(f"{SCREEN_TIMELAPSE_ENRICHMENT_LOOKBACK_HOURS_ENV} must be positive")
    return timedelta(hours=hours)


def screen_timelapse_enrichment_max_error_attempts() -> int:
    value = os.getenv(SCREEN_TIMELAPSE_ENRICHMENT_MAX_ERROR_ATTEMPTS_ENV, "").strip()
    return int(value) if value else DEFAULT_SCREEN_DESCRIPTION_MAX_ERROR_ATTEMPTS


@definitions
def defs() -> Definitions:
    # The "agent" resource is the fleet one the other enrichment defs declare.
    return Definitions(
        assets=[screen_timelapse_enrichment],
        jobs=[screen_timelapse_enrichment_job],
        schedules=[screen_timelapse_enrichment_hourly],
        sensors=[screen_timelapse_enrichment_backlog_sensor],
    )
