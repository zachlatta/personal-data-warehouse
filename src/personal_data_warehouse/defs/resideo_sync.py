"""Poll the Resideo thermostat every five minutes.

The sensor sits out until RESIDEO_API_SUBSCRIPTION_KEY is set. A refused
credential ends the run without retries, since only a new sign-in fixes it, and
`/pipelines` shows it as action_required from ops.resideo_sync_state.
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

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.resideo_api import ResideoAuthError, ResideoClient, WarehouseTokenSource
from personal_data_warehouse.resideo_sync import ResideoSyncRunner
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

# Unique across every *_POSTGRES_LOCK_ID (tests/test_sync_locks.py).
RESIDEO_SYNC_POSTGRES_LOCK_ID = 8_407_112_947
RESIDEO_SENSOR_TICK_SECONDS = 60


@asset(group_name="resideo", retry_policy=RetryPolicy(max_retries=2, delay=60))
def resideo_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False)
    config = settings.resideo
    if config is None or not config.enabled:
        return MaterializeResult(metadata={"skipped": MetadataValue.text("Resideo is not configured")})
    with exclusive_sync_lock(name="resideo_sync", postgres_lock_id=RESIDEO_SYNC_POSTGRES_LOCK_ID) as acquired:
        if not acquired:
            context.log.warning("Skipping the Resideo sync because another run is active")
            return MaterializeResult(metadata={"skipped": MetadataValue.text("another run active")})
        warehouse = warehouse_from_settings(settings)
        try:
            warehouse.ensure_resideo_tables()
            client = ResideoClient(
                tokens=WarehouseTokenSource(
                    warehouse=warehouse, account=config.account, timeout=config.request_timeout_seconds
                ),
                subscription_key=config.subscription_key,
                timeout=config.request_timeout_seconds,
            )
            summary = ResideoSyncRunner(warehouse=warehouse, client=client, account=config.account).sync()
        except ResideoAuthError as exc:
            raise Failure(description=str(exc), metadata={"action_required": True}, allow_retries=False) from exc
        finally:
            warehouse.close()
    return MaterializeResult(
        metadata={
            "devices": MetadataValue.int(summary.devices),
            "readings_written": MetadataValue.int(summary.readings_written),
        }
    )


resideo_sync_job = define_asset_job(
    "resideo_sync_job",
    selection=[resideo_sync],
    # A few HTTP calls. See tests/test_dagster_job_runtime_caps.py for why a
    # frequent job carries its own cap.
    tags={"dagster/max_runtime": "600"},
)


@sensor(
    job=resideo_sync_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=RESIDEO_SENSOR_TICK_SECONDS,
)
def resideo_sync_sensor(context):
    active = skip_if_job_in_progress(context, job_name="resideo_sync_job")
    if isinstance(active, SkipReason):
        return active
    if not (os.getenv("RESIDEO_API_SUBSCRIPTION_KEY") and os.getenv("POSTGRES_DATABASE_URL")):
        return SkipReason("Resideo is not configured: set RESIDEO_API_SUBSCRIPTION_KEY.")
    config = load_settings(require_gmail=False).resideo
    if config is None or not config.enabled:
        return SkipReason("The Resideo sync is disabled by RESIDEO_ENABLED=0.")
    now = time.time()
    if context.cursor and now - float(context.cursor) < config.poll_interval_seconds:
        return SkipReason(f"Waiting for the poll interval ({config.poll_interval_seconds}s).")
    context.update_cursor(str(now))
    return RunRequest(tags={"resideo_trigger": "poll"})


@definitions
def defs() -> Definitions:
    return Definitions(assets=[resideo_sync], jobs=[resideo_sync_job], sensors=[resideo_sync_sensor])
