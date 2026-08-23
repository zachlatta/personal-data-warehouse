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

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.pipeline_health import (
    PROBE_EMPTY,
    PROBE_ERROR,
    PROBE_MISSING,
    PROBE_SKIPPED_UNINDEXED,
    PROBE_TIMEOUT,
    PipelineHealthCollector,
)
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

PIPELINE_HEALTH_POSTGRES_LOCK_ID = 7_403_111_908

# Ten minutes is well inside the collector-stale window the marts_ops views use
# (an hour), so a single missed run never turns the whole dashboard 'unknown',
# and it keeps the measured freshness sharp enough to see a five-minute pipeline
# fall behind.
PIPELINE_HEALTH_CRON = "*/10 * * * *"


@asset(
    group_name="warehouse",
    # Freshness is a measurement, not a mutation: a failed run costs one stale
    # snapshot, so retry once and let the next tick handle the rest.
    retry_policy=RetryPolicy(max_retries=1, delay=60),
)
def pipeline_health(context) -> MaterializeResult:
    """Measure every pipeline's freshness into the ops snapshot tables.

    Read-only against every source table (bounded ``max()`` probes plus catalog
    statistics); the only writes are the two snapshot tables the ``/pipelines``
    dashboard and ``marts_ops.pipeline_health`` read.
    """
    settings = load_settings(require_gmail=False)
    warehouse = warehouse_from_settings(settings)
    pipelines: list = []
    tables: list = []
    marts: list = []
    try:
        with exclusive_sync_lock(
            name="pipeline_health",
            postgres_lock_id=PIPELINE_HEALTH_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                context.log.warning(
                    "Skipping pipeline health collection because another run is already active"
                )
            else:
                warehouse.ensure_pipeline_health_tables()
                pipelines, tables, marts = PipelineHealthCollector(warehouse).run_all()
    finally:
        warehouse.close()

    skipped = [table.table_id for table in tables if table.probe_status == PROBE_SKIPPED_UNINDEXED]
    failed = [
        table.table_id
        for table in tables
        if table.probe_status in {PROBE_TIMEOUT, PROBE_ERROR, PROBE_MISSING}
    ]
    for entry in pipelines:
        if entry.state_error_rows:
            context.log.warning(
                "pipeline %s has %d failing scope(s) in %s: %s",
                entry.pipeline,
                entry.state_error_rows,
                entry.state_table,
                entry.last_error,
            )
    return MaterializeResult(
        metadata={
            "pipelines": MetadataValue.int(len(pipelines)),
            "tables": MetadataValue.int(len(tables)),
            "marts": MetadataValue.int(len(marts)),
            # A mart that cannot answer "is there a row?" is a broken read
            # interface, not a quiet one — worth surfacing on the run itself.
            "marts_failing_probe": MetadataValue.json(
                sorted(
                    view.view_id
                    for view in marts
                    if view.probe_status in {PROBE_ERROR, PROBE_TIMEOUT, PROBE_MISSING}
                )
            ),
            "marts_empty": MetadataValue.json(
                sorted(view.view_id for view in marts if view.probe_status == PROBE_EMPTY)
            ),
            # Cost-guard skips are expected (a 50 GB heap with no timestamp
            # index); a probe error or timeout is not.
            "probes_skipped": MetadataValue.json(sorted(skipped)),
            "probes_failed": MetadataValue.json(sorted(failed)),
            "pipelines_with_errors": MetadataValue.json(
                sorted(entry.pipeline for entry in pipelines if entry.state_error_rows)
            ),
            "pipelines_needing_attention": MetadataValue.json(
                sorted(entry.pipeline for entry in pipelines if entry.state_attention_rows)
            ),
        }
    )


pipeline_health_job = define_asset_job(
    "pipeline_health_job",
    selection=[pipeline_health],
)


@schedule(
    cron_schedule=PIPELINE_HEALTH_CRON,
    job=pipeline_health_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def pipeline_health_every_ten_minutes(context):
    return skip_if_job_active(context, job_name="pipeline_health_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[pipeline_health],
        jobs=[pipeline_health_job],
        schedules=[pipeline_health_every_ten_minutes],
    )
