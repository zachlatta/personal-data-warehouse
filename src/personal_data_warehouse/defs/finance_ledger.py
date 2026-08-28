from __future__ import annotations

from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    RunRequest,
    SkipReason,
    asset,
    define_asset_job,
    definitions,
    schedule,
    sensor,
)

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.plaid_sync import plaid_finance_sync
from personal_data_warehouse.finance_ledger import (
    FinanceLedgerRunner,
    has_pending_finance_observations,
)
from personal_data_warehouse.schedule_guards import skip_if_job_active, skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

FINANCE_LEDGER_POSTGRES_LOCK_ID = 7_403_111_851
FINANCE_LEDGER_SENSOR_INTERVAL_SECONDS = 300


@asset(
    group_name="finance",
    deps=[plaid_finance_sync],
    retry_policy=RetryPolicy(max_retries=1, delay=120),
)
def finance_ledger(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False)
    warehouse = warehouse_from_settings(settings)
    try:
        with exclusive_sync_lock(
            name="finance_ledger",
            postgres_lock_id=FINANCE_LEDGER_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                context.log.warning("Skipping finance ledger because another run is already active")
                summary = None
            else:
                summary = FinanceLedgerRunner(warehouse=warehouse, logger=context.log).sync()
    finally:
        warehouse.close()

    return MaterializeResult(
        metadata={
            "accounts_seen": MetadataValue.int(summary.accounts_seen if summary else 0),
            "accounts_created": MetadataValue.int(summary.accounts_created if summary else 0),
            "links_created": MetadataValue.int(summary.links_created if summary else 0),
            "observations_upserted": MetadataValue.int(
                summary.observations_upserted if summary else 0
            ),
            # Nonzero means a re-linked institution's accounts merged back into
            # the logical accounts they duplicated, and the residue was pruned.
            "accounts_merged": MetadataValue.int(summary.accounts_merged if summary else 0),
            "accounts_pruned": MetadataValue.int(summary.accounts_pruned if summary else 0),
            "links_removed": MetadataValue.int(summary.links_removed if summary else 0),
            # Documents the ledger refused to book as the owner's money: an
            # entity's own financial statements, and documents that name an
            # institution but no account within it. Both should be small and
            # stable. A jump means either a fund's books arrived where an
            # investor statement used to, or files are landing at the corpus
            # root instead of in an account folder.
            "documents_withheld_entity": MetadataValue.int(
                summary.documents_withheld_entity if summary else 0
            ),
            "documents_withheld_unidentified": MetadataValue.int(
                summary.documents_withheld_unidentified if summary else 0
            ),
            # Account-days two documents disagreed about, so the ledger booked
            # neither. Any nonzero value is a source document needing a human;
            # it is not self-healing.
            "observation_conflicts": MetadataValue.int(
                summary.observation_conflicts if summary else 0
            ),
            # Security trades and the lots they reduce to. `security_trades_merged`
            # is the statement/Plaid overlap seam: a drop toward zero while both
            # sources are live would mean the overlap is double-counting.
            "security_trades_upserted": MetadataValue.int(
                summary.security_trades_upserted if summary else 0
            ),
            "security_trades_merged": MetadataValue.int(
                summary.security_trades_merged if summary else 0
            ),
            "security_trades_removed": MetadataValue.int(
                summary.security_trades_removed if summary else 0
            ),
            "tax_lots_built": MetadataValue.int(summary.tax_lots_built if summary else 0),
        }
    )


finance_ledger_job = define_asset_job(
    "finance_ledger_job",
    selection=[finance_ledger],
)


@schedule(
    # Shortly after each */30 plaid sync window, so every day gets its balance
    # observations even if the backlog sensor never fires.
    cron_schedule="7,37 * * * *",
    job=finance_ledger_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def finance_ledger_schedule(context):
    return skip_if_job_active(context, job_name="finance_ledger_job")


@sensor(
    job=finance_ledger_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=FINANCE_LEDGER_SENSOR_INTERVAL_SECONDS,
)
def finance_ledger_backlog_sensor(context):
    active = skip_if_job_in_progress(context, job_name="finance_ledger_job")
    if isinstance(active, SkipReason):
        return active

    try:
        settings = load_settings(require_gmail=False)
    except ValueError as exc:
        return SkipReason(f"Warehouse is not configured: {exc}")

    warehouse = warehouse_from_settings(settings)
    try:
        try:
            pending = has_pending_finance_observations(warehouse)
        except Exception as exc:
            # The plaid/finance tables may not exist yet on a brand-new
            # deploy. Skip until a sync creates them; never run DDL from a
            # sensor.
            return SkipReason(f"Finance tables are not ready yet: {exc}")
        if not pending:
            return SkipReason("Every live account already has today's observation.")
    finally:
        warehouse.close()

    return RunRequest(tags={"finance_trigger": "observation_backlog"})


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[finance_ledger],
        jobs=[finance_ledger_job],
        schedules=[finance_ledger_schedule],
        sensors=[finance_ledger_backlog_sensor],
    )
