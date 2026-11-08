from __future__ import annotations

from datetime import UTC, datetime
import os

from dagster import (
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
    sensor,
)

from personal_data_warehouse.agent_sessions_drive_ingest import (
    AgentSessionsDriveIngestRunner,
    AgentSessionsDriveIngestSummary,
    BATCH_KIND,
    has_batch_payloads,
    iter_batch_payloads,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.timeline_fast_lane import land_sources_on_timeline
from personal_data_warehouse.warehouse import warehouse_from_settings

AGENT_SESSIONS_SENSOR_INTERVAL_SECONDS = 60
# With the resident landing worker on, the sensor only launches for a batch the
# worker has left in the inbox this long: the worker lands one in seconds.
AGENT_SESSIONS_WORKER_GRACE_SECONDS = 120


def _agent_sessions_object_store(settings):
    return build_object_store(
        google_drive_spec(
            folder_id=settings.agent_sessions.google_drive_folder_id,
            account=settings.agent_sessions.google_drive_account,
            source="agent_sessions",
            blob_kind="agent_sessions_blob",
            metadata_kind="agent_sessions_export_batch",
        ),
        settings=settings,
    )


def ingest_agent_sessions_inbox(*, settings, logger) -> AgentSessionsDriveIngestSummary:
    """Promote the Drive inbox's agent-session batches into the warehouse.

    The one ingest path: the asset below and the resident landing worker
    (``agent_sessions_landing_worker``) both call it. There is deliberately no
    advisory lock (a stale one blocked this ingest on 2026-06-22): rows upsert
    by primary key, and the sensor keeps the two apart by leaving a batch to
    the worker until it is AGENT_SESSIONS_WORKER_GRACE_SECONDS old.
    """
    if settings.agent_sessions is None:
        raise RuntimeError("Agent sessions sync is not configured")
    warehouse = warehouse_from_settings(settings)
    try:
        object_store = _agent_sessions_object_store(settings)
        return AgentSessionsDriveIngestRunner(
            warehouse=warehouse,
            batch_source=lambda: iter_batch_payloads(object_store=object_store),
            object_store_factory=lambda: _agent_sessions_object_store(settings),
            promotion_workers=int(os.getenv("AGENT_SESSIONS_DRIVE_INGEST_PROMOTION_WORKERS", "8")),
            logger=logger,
        ).sync()
    finally:
        warehouse.close()


def land_agent_sessions_on_timeline(*, settings, summary, logger) -> dict:
    """Land what an ingest wrote on the timeline now, not at the next timeline_sync tick."""
    sources = []
    if summary is not None and summary.events_written:
        sources.append("agent_sessions")
    if summary is not None and int(getattr(summary, "files_written", 0) or 0):
        # Muse workspace files ride these batches but have their own adapter.
        sources.append("muse")
    if not sources:
        return {"enabled": False, "rows": 0}
    return land_sources_on_timeline(
        postgres_url=settings.postgres_database_url or "",
        sources=sources,
        logger=logger,
    )


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def _landing_worker_enabled() -> bool:
    return os.getenv("AGENT_SESSIONS_LANDING_WORKER_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}


def oldest_inbox_batch_age_seconds(object_store, *, now: datetime) -> float | None:
    """Age of the oldest inbox batch by its ``exported_at`` tag; None for an empty inbox.

    A batch whose tag does not parse counts as old, so it is never stranded.
    """
    oldest: float | None = None
    for listing in object_store.list_objects(kind=BATCH_KIND, stage="inbox"):
        try:
            exported = datetime.fromisoformat(str(listing.app_properties.get("exported_at", "")))
            if exported.tzinfo is None:
                exported = exported.replace(tzinfo=UTC)
            age = (now - exported).total_seconds()
        except ValueError:
            age = float("inf")
        oldest = age if oldest is None else max(oldest, age)
    return oldest


def agent_sessions_inbox_has_batches(settings) -> bool:
    if settings.agent_sessions is None:
        return False
    return has_batch_payloads(object_store=_agent_sessions_object_store(settings), stage="inbox")


@asset(
    group_name="agent_sessions",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def agent_sessions_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_agent_sessions=True)
    summary = ingest_agent_sessions_inbox(settings=settings, logger=context.log)
    fast_lane = land_agent_sessions_on_timeline(settings=settings, summary=summary, logger=context.log)
    files_written = int(getattr(summary, "files_written", 0) or 0) if summary is not None else 0
    return MaterializeResult(
        metadata={
            "timeline_fast_lane": MetadataValue.json(fast_lane),
            "batches_seen": MetadataValue.int(summary.batches_seen if summary else 0),
            "events_written": MetadataValue.int(summary.events_written if summary else 0),
            "files_promoted": MetadataValue.int(summary.files_promoted if summary else 0),
            "muse_files_written": MetadataValue.int(files_written),
            "codex_rows_renormalized": MetadataValue.int(
                int(getattr(summary, "codex_rows_renormalized", 0) or 0) if summary else 0
            ),
        }
    )


agent_sessions_drive_ingest_job = define_asset_job(
    "agent_sessions_drive_ingest_job",
    selection=[agent_sessions_drive_ingest],
)


@sensor(
    job=agent_sessions_drive_ingest_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=AGENT_SESSIONS_SENSOR_INTERVAL_SECONDS,
)
def agent_sessions_drive_inbox_sensor(context):
    active = skip_if_job_in_progress(context, job_name="agent_sessions_drive_ingest_job")
    if isinstance(active, SkipReason):
        return active

    try:
        settings = load_settings(require_gmail=False, require_agent_sessions=True)
    except ValueError as exc:
        return SkipReason(f"Agent sessions are not configured: {exc}")
    if settings.agent_sessions is None:
        raise RuntimeError("Agent sessions sync is not configured")
    if not _landing_worker_enabled():
        if not agent_sessions_inbox_has_batches(settings):
            return SkipReason("No agent session inbox batches found in object storage.")
        return RunRequest(tags={"agent_sessions_trigger": "drive_inbox"})
    age = oldest_inbox_batch_age_seconds(_agent_sessions_object_store(settings), now=_utcnow())
    if age is None:
        return SkipReason("No agent session inbox batches found in object storage.")
    if age < AGENT_SESSIONS_WORKER_GRACE_SECONDS:
        return SkipReason(f"Leaving a {age:.0f}s-old batch to the resident landing worker.")

    return RunRequest(tags={"agent_sessions_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[agent_sessions_drive_ingest],
        jobs=[agent_sessions_drive_ingest_job],
        sensors=[agent_sessions_drive_inbox_sensor],
    )
