"""Dagster wiring for transaction-first receipt research.

The worklist is hard-capped to the most recent 30 days of posted ledger
transactions. Configuration cannot widen it into an archive scan.
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
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.receipt_enrichment import (
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_RETRY_AFTER_DAYS,
    DEFAULT_TRANSACTION_LIMIT,
    ReceiptEnrichmentRunner,
)
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

RECEIPT_ENRICHMENT_POSTGRES_LOCK_ID = 7_403_111_907

RECEIPT_ENABLED_ENV = "RECEIPT_ENRICHMENT_ENABLED"
RECEIPT_RETRY_AFTER_DAYS_ENV = "RECEIPT_RETRY_AFTER_DAYS"
RECEIPT_MAX_ATTEMPTS_ENV = "RECEIPT_MAX_ATTEMPTS"
RECEIPT_TRANSACTION_LIMIT_ENV = "RECEIPT_TRANSACTION_LIMIT"
RECEIPT_MODEL_ENV = "RECEIPT_ENRICHMENT_MODEL"

# Benchmarked against gpt-5.6-sol on a 40-receipt sample: identical quality
# (13/13 high-confidence links exact to the cent, zero errors) at a third of
# the cost. Override per-deployment with RECEIPT_ENRICHMENT_MODEL.
DEFAULT_RECEIPT_MODEL = "gpt-5.6-terra"


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def receipt_enrichment_enabled() -> bool:
    return os.getenv(RECEIPT_ENABLED_ENV, "1").strip().lower() not in {"0", "false", "no"}


def receipt_lookback_days() -> int:
    return DEFAULT_LOOKBACK_DAYS


def receipt_retry_after_days() -> int:
    return _int_env(RECEIPT_RETRY_AFTER_DAYS_ENV, DEFAULT_RETRY_AFTER_DAYS)


def receipt_max_attempts() -> int:
    return _int_env(RECEIPT_MAX_ATTEMPTS_ENV, DEFAULT_MAX_ATTEMPTS)


def receipt_transaction_limit() -> int:
    return _int_env(RECEIPT_TRANSACTION_LIMIT_ENV, DEFAULT_TRANSACTION_LIMIT)


def receipt_model() -> str:
    return os.getenv(RECEIPT_MODEL_ENV, "").strip() or DEFAULT_RECEIPT_MODEL


def receipt_enrichment_runner(*, settings, warehouse, logger, agent: AgentResource | None = None):
    if settings.agent is None:
        raise RuntimeError("Agent runner is not configured")
    resource = agent if agent is not None and agent.is_configured else AgentResource.from_config(settings.agent)
    model = receipt_model()
    if model and resource.model != model:
        resource = resource.model_copy(update={"model": model})
    return ReceiptEnrichmentRunner(
        warehouse=warehouse,
        agent=resource,
        logger=logger,
        provider=settings.agent.provider,
        model=model,
        lookback_days=receipt_lookback_days(),
        retry_after_days=receipt_retry_after_days(),
        max_attempts=receipt_max_attempts(),
        transaction_limit=receipt_transaction_limit(),
    )


@asset(group_name="finance", retry_policy=RetryPolicy(max_retries=1, delay=120))
def receipt_enrichment(context, agent: AgentResource) -> MaterializeResult:
    if not receipt_enrichment_enabled():
        context.log.info("Receipt enrichment is disabled (RECEIPT_ENRICHMENT_ENABLED=0)")
        return MaterializeResult(metadata={"enabled": MetadataValue.bool(False)})

    settings = load_settings(require_gmail=False, require_agent=True)
    warehouse = warehouse_from_settings(settings)
    summary = None
    try:
        with exclusive_sync_lock(
            name="receipt_enrichment",
            postgres_lock_id=RECEIPT_ENRICHMENT_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                context.log.warning("Skipping receipt enrichment; another run is active")
            else:
                summary = receipt_enrichment_runner(
                    settings=settings,
                    warehouse=warehouse,
                    logger=context.log,
                    agent=agent,
                ).sync()
    finally:
        warehouse.close()

    metadata = {
        "enabled": MetadataValue.bool(True),
        "lookback_days": MetadataValue.int(receipt_lookback_days()),
        "transaction_limit": MetadataValue.int(receipt_transaction_limit()),
        "model": MetadataValue.text(receipt_model()),
    }
    if summary is not None:
        metadata.update(
            {key: MetadataValue.int(value) if isinstance(value, int) else MetadataValue.text(str(value))
             for key, value in summary.as_metadata().items()}
        )
    return MaterializeResult(metadata=metadata)


receipt_enrichment_job = define_asset_job(
    "receipt_enrichment_job",
    selection=[receipt_enrichment],
)


@schedule(
    # Off the hour, after the finance ledger snapshot at :07/:37 has landed the
    # newest transactions a receipt might match against.
    cron_schedule="17 * * * *",
    job=receipt_enrichment_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def receipt_enrichment_hourly(context):
    return skip_if_job_active(context, job_name="receipt_enrichment_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[receipt_enrichment],
        jobs=[receipt_enrichment_job],
        schedules=[receipt_enrichment_hourly],
    )
