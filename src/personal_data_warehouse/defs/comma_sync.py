"""Poll comma.ai every fifteen minutes for the car's drives.

comma keeps a free account's drives for about three days, so this poller is
what keeps them. The sensor sits out until COMMA_API_TOKEN is set. A refused
token ends the run without retries, since only a new token fixes it, and
`/pipelines` shows it as action_required from ops.comma_sync_state.
"""

from __future__ import annotations

import os
import time

from dagster import (
    DefaultSensorStatus,
    Definitions,
    Failure,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    RunRequest,
    SkipReason,
    asset,
    define_asset_job,
    definitions,
    sensor,
)

from personal_data_warehouse.comma_api import CommaAuthError, CommaClient
from personal_data_warehouse.comma_sync import CommaSyncRunner
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

# Unique across every *_POSTGRES_LOCK_ID (tests/test_sync_locks.py).
COMMA_SYNC_POSTGRES_LOCK_ID = 8_407_113_031
COMMA_SENSOR_TICK_SECONDS = 60


@asset(group_name="comma", retry_policy=RetryPolicy(max_retries=2, delay=60))
def comma_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False)
    config = settings.comma
    if config is None or not config.enabled:
        return MaterializeResult(metadata={"skipped": MetadataValue.text("comma is not configured")})
    with exclusive_sync_lock(name="comma_sync", postgres_lock_id=COMMA_SYNC_POSTGRES_LOCK_ID) as acquired:
        if not acquired:
            context.log.warning("Skipping the comma sync because another run is active")
            return MaterializeResult(metadata={"skipped": MetadataValue.text("another run active")})
        warehouse = warehouse_from_settings(settings)
        try:
            warehouse.ensure_comma_tables()
            client = CommaClient(token=config.token, timeout=config.request_timeout_seconds)
            summary = CommaSyncRunner(warehouse=warehouse, client=client, token=config.token).sync()
        except CommaAuthError as exc:
            raise Failure(description=str(exc), metadata={"action_required": True}, allow_retries=False) from exc
        finally:
            warehouse.close()
    return MaterializeResult(
        metadata={
            "devices": MetadataValue.int(summary.devices),
            "routes_seen": MetadataValue.int(summary.routes_seen),
            "routes_written": MetadataValue.int(summary.routes_written),
        }
    )


comma_sync_job = define_asset_job(
    "comma_sync_job",
    selection=[comma_sync],
    # A handful of HTTP calls. See tests/test_dagster_job_runtime_caps.py for why
    # a frequent job carries its own cap.
    tags={"dagster/max_runtime": "600"},
)


@sensor(
    job=comma_sync_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=COMMA_SENSOR_TICK_SECONDS,
)
def comma_sync_sensor(context):
    active = skip_if_job_in_progress(context, job_name="comma_sync_job")
    if isinstance(active, SkipReason):
        return active
    if not (os.getenv("COMMA_API_TOKEN") and os.getenv("POSTGRES_DATABASE_URL")):
        return SkipReason("comma is not configured: set COMMA_API_TOKEN.")
    config = load_settings(require_gmail=False).comma
    if config is None or not config.enabled:
        return SkipReason("The comma sync is disabled by COMMA_ENABLED=0.")
    now = time.time()
    if context.cursor and now - float(context.cursor) < config.poll_interval_seconds:
        return SkipReason(f"Waiting for the poll interval ({config.poll_interval_seconds}s).")
    context.update_cursor(str(now))
    return RunRequest(tags={"comma_trigger": "poll"})


@definitions
def defs() -> Definitions:
    return Definitions(assets=[comma_sync], jobs=[comma_sync_job], sensors=[comma_sync_sensor])
