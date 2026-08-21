"""Dagster wiring for the derived search-retrieval layer (search_index.py).

Two assets, deliberately independent:

- ``search_chunks`` follows the timeline (its cursor is timeline.events.seq),
  so it runs on the timeline's cadence, offset by a couple of minutes so a
  chunk pass usually sees the sync that just finished.
- ``search_chunk_embeddings`` drains the un-embedded chunk backlog through the
  configured OpenAI-compatible endpoint. Unconfigured or pre-pgvector hosts
  skip WITH the reason in the run metadata — never a silent no-op that looks
  like success, and never a red run for a deliberate not-yet-configured state.
"""

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
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.schedule_guards import skip_if_job_in_progress
from personal_data_warehouse.search_index import SearchChunkBuilder, SearchEmbeddingRunner
from personal_data_warehouse.sync_locks import exclusive_sync_lock

SEARCH_CHUNKS_POSTGRES_LOCK_ID = 8_407_112_467
SEARCH_EMBEDDINGS_POSTGRES_LOCK_ID = 8_407_112_468

SEARCH_CHUNKS_RUN_BUDGET_SECONDS = 240
SEARCH_EMBEDDINGS_RUN_BUDGET_SECONDS = 480
# Sized so the time budget, not the count, is the binding constraint: the mew
# TEI server does ~85 texts/s on Qwen3-Embedding-4B, so 480s ≈ 40k texts. The
# chunk builder was measured outpacing the previous 20k cap during the dense
# Slack backfill region while the GPU idled between runs.
SEARCH_EMBEDDINGS_RUN_LIMIT = 40_000


def _warehouse() -> PostgresWarehouse:
    settings = load_settings(require_gmail=False)
    warehouse = PostgresWarehouse(settings.postgres_database_url or "")
    warehouse.ensure_search_index_tables()
    return warehouse


@asset(
    group_name="search",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
)
def search_chunks(context) -> MaterializeResult:
    stats = None
    with exclusive_sync_lock(
        name="search_chunks",
        postgres_lock_id=SEARCH_CHUNKS_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping chunk build because another run is already active")
        else:
            warehouse = _warehouse()
            try:
                stats = SearchChunkBuilder(warehouse).run(
                    max_seconds=SEARCH_CHUNKS_RUN_BUDGET_SECONDS
                )
            finally:
                warehouse.close()
    return MaterializeResult(
        metadata={
            "processed_events": MetadataValue.int(stats.processed_events if stats else 0),
            "rebuilt_anchors": MetadataValue.int(stats.rebuilt_anchors if stats else 0),
            "chunks_written": MetadataValue.int(stats.chunks_written if stats else 0),
            "last_seq": MetadataValue.int(stats.last_seq if stats else 0),
            "caught_up": MetadataValue.bool(bool(stats.caught_up) if stats else False),
        }
    )


@asset(
    group_name="search",
    retry_policy=RetryPolicy(max_retries=2, delay=120),
)
def search_chunk_embeddings(context) -> MaterializeResult:
    stats = None
    with exclusive_sync_lock(
        name="search_chunk_embeddings",
        postgres_lock_id=SEARCH_EMBEDDINGS_POSTGRES_LOCK_ID,
    ) as acquired:
        if not acquired:
            context.log.warning("Skipping embedding run because another run is already active")
        else:
            warehouse = _warehouse()
            try:
                stats = SearchEmbeddingRunner(warehouse).run(
                    limit=SEARCH_EMBEDDINGS_RUN_LIMIT,
                    max_seconds=SEARCH_EMBEDDINGS_RUN_BUDGET_SECONDS,
                )
            finally:
                warehouse.close()
            if stats.skipped_reason:
                context.log.warning("search embeddings skipped: %s", stats.skipped_reason)
    return MaterializeResult(
        metadata={
            "embedded": MetadataValue.int(stats.embedded if stats else 0),
            "caught_up": MetadataValue.bool(bool(stats.caught_up) if stats else False),
            "skipped_reason": MetadataValue.text(
                (stats.skipped_reason if stats else "") or ""
            ),
        }
    )


search_chunks_job = define_asset_job("search_chunks_job", selection=[search_chunks])
search_chunk_embeddings_job = define_asset_job(
    "search_chunk_embeddings_job", selection=[search_chunk_embeddings]
)


@schedule(
    cron_schedule="2-59/5 * * * *",
    job=search_chunks_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def search_chunks_every_five_minutes(context):
    return skip_if_job_in_progress(context, job_name="search_chunks_job")


@schedule(
    cron_schedule="4-59/10 * * * *",
    job=search_chunk_embeddings_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def search_chunk_embeddings_every_ten_minutes(context):
    return skip_if_job_in_progress(context, job_name="search_chunk_embeddings_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[search_chunks, search_chunk_embeddings],
        jobs=[search_chunks_job, search_chunk_embeddings_job],
        schedules=[
            search_chunks_every_five_minutes,
            search_chunk_embeddings_every_ten_minutes,
        ],
    )
