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

from personal_data_warehouse.collation_health import (
    FINDING_DUPLICATE_KEYS,
    FINDING_NO_BASELINE,
    FINDING_VERSION_CHANGED,
    CollationHealthCollector,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

COLLATION_HEALTH_POSTGRES_LOCK_ID = 7_403_111_909

# Daily, not ten-minutely, and separate from the freshness collector for that
# reason: the corroborating divergence probe costs a sequential scan of every
# unique index's heap under the size ceiling (~2 GB of reads on the production
# shape). 03:41 keeps it clear of the on-the-hour and half-hour schedules.
COLLATION_HEALTH_CRON = "41 3 * * *"


@asset(
    group_name="warehouse",
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def collation_health(context) -> MaterializeResult:
    """Detect collation drift and unique-index divergence.

    Read-only: catalog reads, bounded ``count(*)``/``count(DISTINCT key)``
    probes, and rigorous amcheck checks including large indexes. It issues no
    DDL, creates no extension, and never REINDEXes —
    repair is a human decision with an ordering that matters (dedupe first, or
    the REINDEX fails).

    The run stays green on a finding. This is a *detector*: the warehouse has
    carried an undetectable collation baseline since before it was noticed, and
    turning that into a permanently red asset would train everyone to ignore
    the one signal that matters. The finding lives on ``/pipelines`` and in
    ``marts_ops.collation_health``, and the run log warns.
    """
    settings = load_settings(require_gmail=False)
    warehouse = warehouse_from_settings(settings)
    findings: list = []
    try:
        with exclusive_sync_lock(
            name="collation_health",
            postgres_lock_id=COLLATION_HEALTH_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                context.log.warning(
                    "Skipping collation health collection because another run is already active"
                )
            else:
                warehouse.ensure_pipeline_health_tables()
                findings = CollationHealthCollector(warehouse).run()
    finally:
        warehouse.close()

    duplicates = [f for f in findings if f.finding == FINDING_DUPLICATE_KEYS]
    changed = [f for f in findings if f.finding == FINDING_VERSION_CHANGED]
    unbaselined = [f for f in findings if f.finding == FINDING_NO_BASELINE]
    for finding in duplicates:
        context.log.warning(
            "%s admits %d duplicate row(s) beyond its distinct key count: %s",
            finding.object_name,
            finding.excess_rows,
            finding.detail,
        )
    for finding in changed:
        context.log.warning("%s: %s", finding.object_name, finding.detail)
    for finding in unbaselined:
        context.log.warning("%s: %s", finding.object_name, finding.detail)

    return MaterializeResult(
        metadata={
            "objects_checked": MetadataValue.int(len(findings)),
            "indexes_with_duplicates": MetadataValue.json(
                sorted(f.object_name for f in duplicates)
            ),
            "excess_rows": MetadataValue.int(sum(f.excess_rows for f in duplicates)),
            "collations_version_changed": MetadataValue.json(
                sorted(f.object_name for f in changed)
            ),
            "collations_without_baseline": MetadataValue.json(
                sorted(f.object_name for f in unbaselined)
            ),
            # The observed library version, recorded every run. With no baseline
            # in pg_database this snapshot's own history is the only baseline
            # that will ever exist, so a future change shows up as this value
            # moving.
            "database_collation_actual_version": MetadataValue.text(
                next((f.actual_version for f in findings if f.scope == "database"), "")
            ),
        }
    )


collation_health_job = define_asset_job(
    "collation_health_job",
    selection=[collation_health],
)


@schedule(
    cron_schedule=COLLATION_HEALTH_CRON,
    job=collation_health_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def collation_health_daily(context):
    return skip_if_job_active(context, job_name="collation_health_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[collation_health],
        jobs=[collation_health_job],
        schedules=[collation_health_daily],
    )
