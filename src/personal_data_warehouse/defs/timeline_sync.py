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
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.timeline import TimelineSyncEngine, TimelineSyncError

TIMELINE_SYNC_POSTGRES_LOCK_ID = 8_407_112_466

# Each run keeps ticking incremental sync for every source, then spends the
# rest of the budget backfilling history round-robin. The budget stays under
# the 5-minute cadence so runs never pile up behind the overlap guard.
TIMELINE_SYNC_RUN_BUDGET_SECONDS = 240

# Operator throttle for the history re-walk, in seconds per run. Unset = the
# backfill may use the whole run budget. Changing a high-volume adapter's SQL
# re-walks every row it owns; at the full budget the 2026-08-26 slack/gmail
# re-walk wrote ~19 GB/h of WAL against an archiver shipping ~2 GB/h, so the
# unarchived backlog grew ~15 GB/h. Set this (e.g. 30) in the Dagster
# deployment to slow the re-walk while the archive catches up, and unset it
# afterwards. Incremental sync is never throttled by it.
TIMELINE_SYNC_BACKFILL_BUDGET_ENV = "TIMELINE_SYNC_BACKFILL_BUDGET_SECONDS"

# Their independent coverage proof necessarily walks a large recent source
# window even when it finds only a handful of gaps.  Restore the HNSW/BM25
# working set after those hourly scans, once at the end of the timeline run;
# otherwise a correctness pass makes interactive search cold until organic
# traffic happens to repopulate it.
CACHE_EVICTING_RECONCILE_ADAPTERS = frozenset(
    {"gmail_email", "slack_message", "slack_file"}
)


def _backfill_budget_seconds() -> float | None:
    raw = os.environ.get(TIMELINE_SYNC_BACKFILL_BUDGET_ENV, "").strip()
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError as error:
        raise RuntimeError(f"{TIMELINE_SYNC_BACKFILL_BUDGET_ENV} must be a number of seconds, got {raw!r}") from error


@asset(
    group_name="timeline",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def timeline_sync(context) -> MaterializeResult:
    settings = load_settings(require_gmail=False)
    stats = []
    with exclusive_sync_lock(
        name="timeline_sync",
        postgres_lock_id=TIMELINE_SYNC_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping timeline sync because another run is already active")
        else:
            engine = TimelineSyncEngine(source_url=settings.postgres_database_url or "")
            try:
                stats = engine.run(
                    max_seconds=TIMELINE_SYNC_RUN_BUDGET_SECONDS,
                    backfill_max_seconds=_backfill_budget_seconds(),
                )
            except TimelineSyncError as error:
                # Persisted per-adapter progress survives; surface the failure
                # loudly so monitoring goes red instead of silently stalling.
                context.log.error("timeline sync finished with adapter failures: %s", error)
                raise
            finally:
                engine.close()

            if any(
                stat.reconcile_ran
                and stat.adapter in CACHE_EVICTING_RECONCILE_ADAPTERS
                for stat in stats
            ):
                warehouse = PostgresWarehouse(settings.postgres_database_url or "")
                try:
                    warm = warehouse.prewarm_search_indexes_if_needed(force=True)
                    context.log.info(
                        "Restored search cache after high-volume timeline reconcile: %s",
                        warm,
                    )
                except Exception as error:  # cache repair must not fail ingestion
                    context.log.error(
                        "Could not restore search cache after timeline reconcile: %s",
                        error,
                    )
                finally:
                    warehouse.close()

    return MaterializeResult(
        metadata={
            "adapters": MetadataValue.int(len(stats)),
            "backfill_rows": MetadataValue.int(sum(s.backfill_rows for s in stats)),
            "incremental_rows": MetadataValue.int(sum(s.incremental_rows for s in stats)),
            "reconciled_adapters": MetadataValue.json(
                sorted(s.adapter for s in stats if s.reconcile_ran)
            ),
            "backfill_pending": MetadataValue.json(
                sorted(s.adapter for s in stats if not s.backfill_done)
            ),
            "summaries": MetadataValue.json(
                [
                    {
                        "adapter": s.adapter,
                        "backfill_rows": s.backfill_rows,
                        "incremental_rows": s.incremental_rows,
                        "backfill_done": s.backfill_done,
                        "error": s.error,
                    }
                    for s in stats
                ]
            ),
        }
    )


timeline_sync_job = define_asset_job(
    "timeline_sync_job",
    selection=[timeline_sync],
)


@schedule(
    cron_schedule="*/5 * * * *",
    job=timeline_sync_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def timeline_sync_every_five_minutes(context):
    return skip_if_job_in_progress(context, job_name="timeline_sync_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[timeline_sync],
        jobs=[timeline_sync_job],
        schedules=[timeline_sync_every_five_minutes],
    )
