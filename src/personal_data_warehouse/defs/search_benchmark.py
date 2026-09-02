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

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.search_benchmark_runner import AppSearchClient, SearchBenchmarkRunner
from personal_data_warehouse.search_index import record_search_cache_residency
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

SEARCH_BENCHMARK_POSTGRES_LOCK_ID = 8_407_112_480

# Weekly, Monday 05:17 UTC: ~50 hybrid searches through the app is minutes of
# GPU and Postgres time, which is a weekly amount of work; the row's own
# staleness guard is ten days.
SEARCH_BENCHMARK_CRON = "17 5 * * 1"


def app_credentials() -> tuple[str, str]:
    base_url = (os.getenv("PDW_API_URL") or os.getenv("MCP_BASE_URL") or "").strip()
    secret_token = (os.getenv("PDW_SECRET_TOKEN") or os.getenv("MCP_SECRET_TOKEN") or "").strip()
    return base_url, secret_token


@asset(
    group_name="search",
    retry_policy=RetryPolicy(max_retries=1, delay=600),
)
def search_benchmark(context) -> MaterializeResult:
    """Measure search latency and labeled quality through the app's search tool (C8)."""
    base_url, secret_token = app_credentials()
    if not base_url or not secret_token:
        context.log.warning("Skipping search benchmark: PDW_API_URL / PDW_SECRET_TOKEN are not set")
        return MaterializeResult(metadata={"skipped": MetadataValue.text("app credentials not configured")})
    settings = load_settings(require_gmail=False)
    warehouse = warehouse_from_settings(settings)
    result = None
    residency = None
    try:
        with exclusive_sync_lock(
            name="search_benchmark",
            postgres_lock_id=SEARCH_BENCHMARK_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                context.log.warning("Skipping search benchmark because another run is already active")
            else:
                warehouse.ensure_pipeline_health_tables()
                result = SearchBenchmarkRunner(
                    warehouse=warehouse,
                    client=AppSearchClient(base_url=base_url, secret_token=secret_token),
                    logger_=context.log,
                ).run()
                try:
                    residency = record_search_cache_residency(warehouse)
                except Exception as error:  # health fact, not a benchmark failure
                    context.log.error("Could not measure search cache residency: %s", error)
                    warehouse.write_search_health(
                        "cache_residency", last_error=str(error)[:500]
                    )
    finally:
        warehouse.close()
    return MaterializeResult(
        metadata={
            "latency_p50_ms": MetadataValue.int(result.latency_p50_ms if result else 0),
            "latency_p90_ms": MetadataValue.int(result.latency_p90_ms if result else 0),
            "labeled_cases": MetadataValue.int(result.labeled_cases if result else 0),
            "mrr": MetadataValue.float((result.mrr_milli / 1000) if result else 0.0),
            "errors": MetadataValue.int(result.errors if result else 0),
            "note": MetadataValue.text(result.note if result else ""),
            "shared_buffer_resident_fraction": MetadataValue.float(
                float(residency["resident_fraction"]) if residency else 0.0
            ),
        }
    )


search_benchmark_job = define_asset_job("search_benchmark_job", selection=[search_benchmark])


@schedule(
    cron_schedule=SEARCH_BENCHMARK_CRON,
    job=search_benchmark_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def search_benchmark_weekly(context):
    return skip_if_job_active(context, job_name="search_benchmark_job")


@definitions
def defs() -> Definitions:
    return Definitions(assets=[search_benchmark], jobs=[search_benchmark_job], schedules=[search_benchmark_weekly])
