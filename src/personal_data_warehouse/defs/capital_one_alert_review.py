"""Dagster wiring for the Capital One alert review agent.

Hourly, after the five-minute finance ledger: every alert still unresolved two
days after its purchase gets one agent review; the next ledger run applies the
trusted decisions.
"""

from __future__ import annotations

import os

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
    define_asset_job,
    definitions,
    schedule,
)

from personal_data_warehouse.agent_resource import AgentResource
from personal_data_warehouse.capital_one_alert_review import AlertReviewRunner
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

CAPITAL_ONE_ALERT_REVIEW_POSTGRES_LOCK_ID = 7_403_111_913
ALERT_REVIEW_ENABLED_ENV = "CAPITAL_ONE_ALERT_REVIEW_ENABLED"
ALERT_REVIEW_MODEL_ENV = "CAPITAL_ONE_ALERT_REVIEW_MODEL"


def alert_review_enabled() -> bool:
    return os.getenv(ALERT_REVIEW_ENABLED_ENV, "1").strip().lower() not in {"0", "false", "no"}


def alert_review_runner(*, settings, warehouse, logger, agent: AgentResource | None = None):
    if settings.agent is None:
        raise RuntimeError("Agent runner is not configured")
    resource = agent if agent is not None and agent.is_configured else AgentResource.from_config(settings.agent)
    model = os.getenv(ALERT_REVIEW_MODEL_ENV, "").strip()
    if model and resource.model != model:
        resource = resource.model_copy(update={"model": model})
    return AlertReviewRunner(
        warehouse=warehouse,
        agent=resource,
        logger=logger,
        provider=settings.agent.provider,
        model=resource.model or "",
    )


@asset(group_name="finance", retry_policy=RetryPolicy(max_retries=1, delay=120))
def capital_one_alert_review(context, agent: AgentResource) -> MaterializeResult:
    if not alert_review_enabled():
        context.log.info(f"Capital One alert review is disabled ({ALERT_REVIEW_ENABLED_ENV}=0)")
        return MaterializeResult(metadata={"enabled": MetadataValue.bool(False)})

    settings = load_settings(require_gmail=False, require_agent=True)
    warehouse = warehouse_from_settings(settings)
    summary = None
    try:
        with exclusive_sync_lock(
            name="capital_one_alert_review",
            postgres_lock_id=CAPITAL_ONE_ALERT_REVIEW_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                context.log.warning("Skipping Capital One alert review; another run is active")
            else:
                summary = alert_review_runner(
                    settings=settings, warehouse=warehouse, logger=context.log, agent=agent
                ).sync()
    finally:
        warehouse.close()

    metadata = {"enabled": MetadataValue.bool(True)}
    if summary is not None:
        metadata.update({key: MetadataValue.int(value) for key, value in summary.as_metadata().items()})
    return MaterializeResult(metadata=metadata)


capital_one_alert_review_job = define_asset_job(
    "capital_one_alert_review_job",
    selection=[capital_one_alert_review],
    # Twenty agent calls finish well inside an hour; a hung run must not hold
    # a queue slot for the global four-hour cap.
    tags={"dagster/max_runtime": "3600"},
)


@schedule(
    cron_schedule="37 * * * *",
    job=capital_one_alert_review_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def capital_one_alert_review_hourly(context):
    return skip_if_job_active(context, job_name="capital_one_alert_review_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[capital_one_alert_review],
        jobs=[capital_one_alert_review_job],
        schedules=[capital_one_alert_review_hourly],
    )
