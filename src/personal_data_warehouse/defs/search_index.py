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

from datetime import UTC, datetime

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
from personal_data_warehouse.search_index import (
    SearchChunkBuilder,
    SearchEmbeddingRunner,
    record_search_cache_residency,
)
from personal_data_warehouse.sync_locks import exclusive_sync_lock

# Keep these globally unique across every Dagster asset. Reusing the WhatsApp
# enrichment and WHOOP lock ids serialized unrelated pipelines and let a long
# embedding run delay health-data ingestion.
SEARCH_CHUNKS_POSTGRES_LOCK_ID = 8_407_112_474
SEARCH_EMBEDDINGS_POSTGRES_LOCK_ID = 8_407_112_475

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
    warehouse.ensure_pipeline_health_tables()
    return warehouse


def _oldest_pending_timeline_write(warehouse, cursor_seq: int) -> datetime:
    """When the oldest timeline row past the chunk cursor was written.

    One index-ordered probe on `seq`, never a count: it is what lets
    marts_ops.search_health say `late` in wall-clock terms while a re-walk is
    pumping millions of re-stamped rows through the chunker. The epoch means
    "nothing pending" (the warehouse's absent sentinel).
    """
    rows = warehouse._query(
        "SELECT updated_at FROM @timeline_events WHERE seq > %s ORDER BY seq ASC LIMIT 1",
        (cursor_seq,),
    )
    if not rows or rows[0][0] is None:
        return datetime.fromtimestamp(0, tz=UTC)
    return rows[0][0]


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
                stats = SearchChunkBuilder(warehouse).run(max_seconds=SEARCH_CHUNKS_RUN_BUDGET_SECONDS)
                timeline_max = int(warehouse._query("SELECT COALESCE(max(seq), 0) FROM @timeline_events")[0][0])
                caught_up = stats.caught_up and stats.last_seq >= timeline_max
                warehouse.write_search_health(
                    "chunks",
                    timeline_max_seq=timeline_max,
                    chunk_cursor_seq=stats.last_seq,
                    caught_up=1 if caught_up else 0,
                    processed_rows=stats.processed_events,
                    pending_count=(0 if caught_up else -1),
                    oldest_pending_at=_oldest_pending_timeline_write(warehouse, stats.last_seq),
                    last_success_at=datetime.now(tz=UTC),
                    last_error="",
                )
                # Level 4 of the health contract for search: are the BM25
                # indexes readable at all. Reported as its own component so a
                # corrupt index is a red row, never a silently empty search.
                probe = warehouse.probe_bm25_indexes()
                broken = {name: err for name, err in probe.items() if err}
                warehouse.write_search_health(
                    "bm25_indexes",
                    caught_up=1,
                    processed_rows=len(probe),
                    pending_count=len(broken),
                    last_success_at=(
                        datetime.now(tz=UTC) if not broken else datetime.fromtimestamp(0, tz=UTC)
                    ),
                    last_error="; ".join(f"{name}: {err}" for name, err in broken.items())[:500],
                )
                if broken:
                    context.log.error("BM25 index probe failed: %s", broken)
                # Cache warmth can change within minutes under the raw-source
                # sync workload.  Publishing it only in the weekly benchmark
                # left C6 pointing at a stale cause, so refresh the inexpensive
                # pg_buffercache gauge on this five-minute health cadence.
                try:
                    record_search_cache_residency(warehouse)
                except Exception as error:  # health fact, not a chunk failure
                    context.log.error(
                        "Could not measure search cache residency: %s", error
                    )
                    warehouse.write_search_health(
                        "cache_residency", last_error=str(error)[:500]
                    )
            except Exception as error:
                warehouse.write_search_health("chunks", last_error=str(error)[:500])
                raise
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
                runner = SearchEmbeddingRunner(warehouse)
                stats = runner.run(
                    limit=SEARCH_EMBEDDINGS_RUN_LIMIT,
                    max_seconds=SEARCH_EMBEDDINGS_RUN_BUDGET_SECONDS,
                )
                # from_env is intentionally repeated only for the small config
                # fact; no request is sent. It keeps the SQL surface honest
                # when this deployment is deliberately keyword-only.
                from personal_data_warehouse.search_index import EmbeddingClient

                client = runner._client or EmbeddingClient.from_env()
                configured = client is not None
                pgvector = runner._embedding_column_exists()
                warehouse.write_search_health(
                    "embeddings",
                    model=(client.model if client else ""),
                    configured=1 if configured else 0,
                    pgvector_available=1 if pgvector else 0,
                    caught_up=1 if stats.caught_up else 0,
                    processed_rows=stats.embedded,
                    pending_count=0 if stats.caught_up else -1,
                    last_success_at=(
                        datetime.now(tz=UTC)
                        if configured and pgvector and not stats.skipped_reason
                        else datetime.fromtimestamp(0, tz=UTC)
                    ),
                    last_error=stats.skipped_reason,
                )
                warehouse.write_search_health(
                    "orphaned_chunks",
                    model=(client.model if client else ""),
                    configured=1 if configured else 0,
                    pgvector_available=1 if pgvector else 0,
                    caught_up=1 if stats.orphans_caught_up else 0,
                    processed_rows=stats.orphaned_repaired,
                    pending_count=0 if stats.orphans_caught_up else -1,
                    last_success_at=(
                        datetime.now(tz=UTC)
                        if configured and pgvector and stats.orphans_caught_up
                        else datetime.fromtimestamp(0, tz=UTC)
                    ),
                    last_error=stats.skipped_reason,
                )
            except Exception as error:
                warehouse.write_search_health("embeddings", last_error=str(error)[:500])
                warehouse.write_search_health(
                    "orphaned_chunks", last_error=str(error)[:500]
                )
                raise
            finally:
                warehouse.close()
            if stats.skipped_reason:
                context.log.warning("search embeddings skipped: %s", stats.skipped_reason)
    return MaterializeResult(
        metadata={
            "embedded": MetadataValue.int(stats.embedded if stats else 0),
            "orphaned_found": MetadataValue.int(stats.orphaned_found if stats else 0),
            "orphaned_repaired": MetadataValue.int(
                stats.orphaned_repaired if stats else 0
            ),
            "caught_up": MetadataValue.bool(bool(stats.caught_up) if stats else False),
            "skipped_reason": MetadataValue.text((stats.skipped_reason if stats else "") or ""),
        }
    )


search_chunks_job = define_asset_job("search_chunks_job", selection=[search_chunks])
search_chunk_embeddings_job = define_asset_job("search_chunk_embeddings_job", selection=[search_chunk_embeddings])


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
