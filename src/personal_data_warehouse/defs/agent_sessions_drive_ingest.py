from __future__ import annotations

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
    has_batch_payloads,
    iter_batch_payloads,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.objectstore import build_object_store, google_drive_spec
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.warehouse import warehouse_from_settings

AGENT_SESSIONS_SENSOR_INTERVAL_SECONDS = 60


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


@asset(
    group_name="agent_sessions",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def agent_sessions_drive_ingest(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False, require_agent_sessions=True)
    if settings.agent_sessions is None:
        raise RuntimeError("Agent sessions sync is not configured")
    warehouse = warehouse_from_settings(settings)
    try:
        object_store = _agent_sessions_object_store(settings)
        summary = AgentSessionsDriveIngestRunner(
            warehouse=warehouse,
            batch_source=lambda: iter_batch_payloads(object_store=object_store),
            object_store_factory=lambda: _agent_sessions_object_store(settings),
            promotion_workers=int(os.getenv("AGENT_SESSIONS_DRIVE_INGEST_PROMOTION_WORKERS", "8")),
            logger=context.log,
        ).sync()
    finally:
        warehouse.close()

    return MaterializeResult(
        metadata={
            "batches_seen": MetadataValue.int(summary.batches_seen if summary else 0),
            "events_written": MetadataValue.int(summary.events_written if summary else 0),
            "files_promoted": MetadataValue.int(summary.files_promoted if summary else 0),
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
    if not has_batch_payloads(object_store=_agent_sessions_object_store(settings), stage="inbox"):
        return SkipReason("No agent session inbox batches found in object storage.")

    return RunRequest(tags={"agent_sessions_trigger": "drive_inbox"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[agent_sessions_drive_ingest],
        jobs=[agent_sessions_drive_ingest_job],
        sensors=[agent_sessions_drive_inbox_sensor],
    )
