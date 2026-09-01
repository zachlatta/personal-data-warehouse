from __future__ import annotations

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

from personal_data_warehouse.agent_usage import AgentUsageCollector
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

AGENT_USAGE_POSTGRES_LOCK_ID = 8_407_112_479

# Daily: the aggregate walks two weeks of agent events (a few million rows),
# which is a once-a-day amount of work, not a ten-minutely one. 04:23 keeps it
# clear of the other daily collectors.
AGENT_USAGE_CRON = "23 4 * * *"


@asset(
    group_name="warehouse",
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def agent_usage(context) -> MaterializeResult:
    """Measure how agents use PDW from their own transcripts (contract C3)."""
    settings = load_settings(require_gmail=False)
    warehouse = warehouse_from_settings(settings)
    snapshots: list = []
    try:
        with exclusive_sync_lock(
            name="agent_usage",
            postgres_lock_id=AGENT_USAGE_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                context.log.warning("Skipping agent usage collection because another run is already active")
            else:
                warehouse.ensure_pipeline_health_tables()
                snapshots = AgentUsageCollector(warehouse).run()
    finally:
        warehouse.close()
    overall = next((s for s in snapshots if s.source == "all"), None)
    return MaterializeResult(
        metadata={
            "sources": MetadataValue.int(len(snapshots)),
            "sessions": MetadataValue.int(overall.sessions if overall else 0),
            "pdw_sessions": MetadataValue.int(overall.pdw_sessions if overall else 0),
            "first_search": MetadataValue.int(overall.first_search if overall else 0),
            "search_with_priority": MetadataValue.int(overall.search_with_priority if overall else 0),
            "search_attention_only": MetadataValue.int(overall.search_attention_only if overall else 0),
            "search_invalid_or_failed_priority": MetadataValue.int(
                overall.search_invalid_or_failed_priority if overall else 0
            ),
            "bulk_hint_improved_retries": MetadataValue.int(
                overall.bulk_hint_improved_retries if overall else 0
            ),
            "sql_base_only": MetadataValue.int(overall.sql_base_only if overall else 0),
        }
    )


agent_usage_job = define_asset_job("agent_usage_job", selection=[agent_usage])


@schedule(
    cron_schedule=AGENT_USAGE_CRON,
    job=agent_usage_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def agent_usage_daily(context):
    return skip_if_job_active(context, job_name="agent_usage_job")


@definitions
def defs() -> Definitions:
    return Definitions(assets=[agent_usage], jobs=[agent_usage_job], schedules=[agent_usage_daily])
