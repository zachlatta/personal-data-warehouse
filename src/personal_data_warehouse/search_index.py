"""Derived search-retrieval layer: chunking and embeddings behind search_hybrid().

The unified timeline is the lexical (BM25) search surface; this module builds
the *semantic* half on top of it:

- ``SearchChunkBuilder`` walks ``timeline.events`` by ``seq`` (the arrival /
  content-change order the timeline upsert maintains) and derives retrieval
  chunks into ``derived_search.chunks``. Chat-shaped sources (Slack, iMessage,
  WhatsApp) are chunked as conversation windows — per-message rows average a
  few dozen characters, which is a useless embedding unit — while every other
  source is chunked per event, with oversized documents split. Every chunk is
  tied to an ``anchor`` so a changed event replaces exactly its own chunks.
- ``SearchEmbeddingRunner`` embeds each distinct chunk text once per model
  into ``derived_search.chunk_embeddings`` (keyed by content sha — repeated
  text is paid for once) through any OpenAI-compatible ``/v1/embeddings``
  endpoint: cloud OpenAI, Gemini's compatibility endpoint, or a self-hosted
  server. Unconfigured hosts skip loudly instead of failing.
- ``timeline.search_hybrid()`` (created in postgres.py where pgvector is
  available) fuses BM25 and ANN ranks with reciprocal rank fusion.

Nothing here assumes pgvector: chunks build everywhere, and the embedding
runner reports *why* it is skipping when the vector column or credentials are
missing, so the backlog drains by itself once the prerequisites appear.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import hashlib
import logging
import os
import time
from typing import Any

from psycopg2.extras import execute_values

from personal_data_warehouse.postgres import (
    SEARCH_EMBEDDING_DEFAULT_MODEL,
    SEARCH_EMBEDDING_DIMENSIONS,
    PostgresWarehouse,
)

logger = logging.getLogger(__name__)

# Chat-shaped adapters whose per-event rows are too small to embed alone; they
# are grouped into per-(context, hour) conversation windows instead.
WINDOW_ADAPTERS = frozenset({"slack_message", "apple_message", "whatsapp_message"})
# The warehouse's own machinery: operational rows nobody semantically searches.
SKIP_ADAPTERS = frozenset({"enrichment_run", "mutation", "mutation_request"})
WINDOW_SECONDS = 3600
# Per-member contribution to a window line and the window/document chunk sizes.
WINDOW_MEMBER_MAX_CHARS = 500
CHUNK_MAX_CHARS = 2000
CHUNK_OVERLAP_CHARS = 200
WINDOW_CHUNK_MAX_CHARS = 6000
# Safety cap per source document (multi-MB Drive extracts): beyond this the
# tail is not chunked. ~100 chunks per document is already deep retrieval.
DOCUMENT_MAX_CHARS = 200_000
CHUNK_MIN_CHARS = 3

# This endpoint also serves interactive hybrid searches. Production timing on
# Qwen3-Embedding-4B (2026-09-02) was 1.2s for 32 texts, 3.6s for 64, and 11.7s
# for 128; two concurrent 128-text backfill requests made an interactive query
# wait 23s. Thirty-two was faster in texts/second as well as latency, so keep
# one bounded background request in flight and leave TEI room for user traffic.
EMBED_BATCH_SIZE = 32
# Candidate rows fetched per keyset page (pre-dedupe); bounds memory while
# amortizing the anti-join probe cost over many embed batches.
EMBED_SLAB_SIZE = 5_000
# Embedding-model input cap safety: ~8k tokens for the OpenAI small models.
EMBED_MAX_CHARS = 20_000

_STATE_ID = "timeline"
_EMBED_STATE_ID = "embeddings"
_EPOCH = datetime.fromtimestamp(0, tz=UTC)
# How far behind its own start a fresh-pass watermark is left, so a chunk
# committed late by a long builder transaction is still offered next run.
EMBED_FRESH_OVERLAP = timedelta(minutes=15)
# Index rows the historical backfill may walk per run. It bounds the heap
# reads a run can cause (~250k rows is well under 1 GB) while still finishing
# a 7.9M-row corpus in a few hours of ten-minute runs.
EMBED_BACKFILL_SCAN_ROWS = 250_000
# Same bound for the fresh pass. Both walks are index-only, so a run's read
# cost is a few hundred MB of index at most, never the chunk heap.
EMBED_FRESH_SCAN_ROWS = 500_000
# Cursors make the routine path cheap; this periodic covering-index anti-join
# is the independent completeness proof. It also repairs rows stranded behind
# a cursor by an old bug or interrupted migration.
EMBED_ORPHAN_RECHECK_INTERVAL = timedelta(hours=24)


def record_search_cache_residency(warehouse: Any) -> dict[str, int | float]:
    """Measure and publish current search-index shared-buffer residency.

    This belongs on the five-minute search health cadence, not only beside the
    weekly benchmark: the whole point of the gauge is to explain a slow search
    while the cache is cold, rather than preserve how warm it was days ago.
    """

    measured = warehouse.measure_search_cache_residency()
    warehouse.write_search_health(
        "cache_residency",
        configured=1,
        pgvector_available=1,
        caught_up=1,
        processed_rows=measured["target_count"],
        pending_count=0,
        resident_bytes=measured["resident_bytes"],
        total_bytes=measured["total_bytes"],
        resident_fraction=measured["resident_fraction"],
        last_success_at=datetime.now(tz=UTC),
        last_error="",
    )
    return measured


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def split_text(text: str, *, max_chars: int = CHUNK_MAX_CHARS, overlap: int = CHUNK_OVERLAP_CHARS) -> list[str]:
    """Split a document into chunks on line boundaries where possible.

    Pure and deterministic: the same document always yields the same chunks
    (and therefore the same content shas, so nothing re-embeds on a no-op
    rebuild).
    """
    text = text[:DOCUMENT_MAX_CHARS]
    if len(text) <= max_chars:
        stripped = text.strip()
        return [stripped] if len(stripped) >= CHUNK_MIN_CHARS else []
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(start + max_chars, len(text))
        if end < len(text):
            # Prefer to break on a newline inside the back half of the window.
            newline = text.rfind("\n", start + max_chars // 2, end)
            if newline > start:
                end = newline
        piece = text[start:end].strip()
        if len(piece) >= CHUNK_MIN_CHARS:
            chunks.append(piece)
        if end >= len(text):
            break
        start = max(end - overlap, start + 1)
    return chunks


def window_start(event_ts: datetime) -> datetime:
    epoch = int(event_ts.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % WINDOW_SECONDS), tz=UTC)


@dataclass
class ChunkBuildStats:
    processed_events: int = 0
    rebuilt_anchors: int = 0
    chunks_written: int = 0
    last_seq: int = 0
    caught_up: bool = False


@dataclass
class EmbedStats:
    embedded: int = 0
    orphaned_found: int = 0
    orphaned_repaired: int = 0
    orphans_caught_up: bool = False
    skipped_reason: str = ""
    caught_up: bool = False


@dataclass(frozen=True)
class _ChunkRow:
    chunk_id: str
    anchor: str
    adapter: str
    event_id: str
    source: str
    context: str
    event_ts: datetime
    chunk_index: int
    text: str


class SearchChunkBuilder:
    """Incrementally derives ``derived_search.chunks`` from ``timeline.events``.

    The cursor is ``timeline.events.seq``: the timeline upsert bumps ``seq``
    whenever a row's content changes, so "chunks for every event with
    ``seq > watermark``" is exactly "chunks for everything new or changed" —
    including re-walks triggered by the adapter-signature reset, which makes
    this layer converge for free whenever the timeline does.
    """

    def __init__(self, warehouse: PostgresWarehouse) -> None:
        self._wh = warehouse

    # -- state ---------------------------------------------------------------

    def _load_watermark(self) -> int:
        rows = self._wh._query(
            "SELECT last_seq FROM @search_chunk_sync_state WHERE id = %s", (_STATE_ID,)
        )
        return int(rows[0][0]) if rows else 0

    def _save_watermark(self, seq: int) -> None:
        self._wh._command(
            "INSERT INTO @search_chunk_sync_state (id, last_seq, updated_at)"
            " VALUES (%s, %s, now())"
            " ON CONFLICT (id) DO UPDATE SET last_seq = EXCLUDED.last_seq, updated_at = now()",
            (_STATE_ID, seq),
        )

    # -- chunk derivation ----------------------------------------------------

    def _plain_chunks(self, row: dict[str, Any]) -> list[_ChunkRow]:
        anchor = f"{row['adapter']}|{row['event_id']}"
        pieces = split_text(row["search_text"] or "")
        return [
            _ChunkRow(
                chunk_id=f"{anchor}#{index}",
                anchor=anchor,
                adapter=row["adapter"],
                event_id=row["event_id"],
                source=row["source"],
                context=row["context"] or "",
                event_ts=row["event_ts"],
                chunk_index=index,
                text=piece,
            )
            for index, piece in enumerate(pieces)
        ]

    def _window_chunks(self, adapter: str, context: str, start: datetime) -> list[_ChunkRow]:
        members = self._wh._query_dicts(
            """
            SELECT event_id, source, actor, title, snippet, event_ts
            FROM @timeline_events
            WHERE adapter = %s AND context = %s
              AND event_ts >= %s AND event_ts < %s
              AND NOT COALESCE((metadata->>'deleted')::boolean, false)
            ORDER BY event_ts ASC, seq ASC
            """,
            (adapter, context, start, start + timedelta(seconds=WINDOW_SECONDS)),
        )
        if not members:
            return []
        lines = []
        for member in members:
            body = (member["snippet"] or member["title"] or "")[:WINDOW_MEMBER_MAX_CHARS]
            if not body.strip():
                continue
            actor = member["actor"] or ""
            lines.append(f"{actor}: {body}" if actor else body)
        if context.strip():
            lines.insert(0, context)
        pieces = split_text("\n".join(lines), max_chars=WINDOW_CHUNK_MAX_CHARS, overlap=0)
        anchor = f"{adapter}|w|{context}|{start.isoformat()}"
        representative = members[-1]
        return [
            _ChunkRow(
                chunk_id=f"{anchor}#{index}",
                anchor=anchor,
                adapter=adapter,
                event_id=representative["event_id"],
                source=representative["source"],
                context=context,
                event_ts=start,
                chunk_index=index,
                text=piece,
            )
            for index, piece in enumerate(pieces)
        ]

    def _replace_anchor_chunks(self, anchors: list[str], rows: list[_ChunkRow]) -> None:
        if anchors:
            self._wh._command(
                "DELETE FROM @search_chunks WHERE anchor = ANY(%s)", (anchors,)
            )
        if not rows:
            return
        table = self._wh.sql_relation("search_chunks")
        with self._wh._connection.cursor() as cursor:
            execute_values(
                cursor,
                f"INSERT INTO {table} (chunk_id, anchor, adapter, event_id, source, context,"
                " event_ts, chunk_index, text, text_sha256, char_count, built_at)"
                " VALUES %s ON CONFLICT (chunk_id) DO UPDATE SET"
                " anchor = EXCLUDED.anchor, adapter = EXCLUDED.adapter,"
                " event_id = EXCLUDED.event_id, source = EXCLUDED.source,"
                " context = EXCLUDED.context, event_ts = EXCLUDED.event_ts,"
                " chunk_index = EXCLUDED.chunk_index, text = EXCLUDED.text,"
                " text_sha256 = EXCLUDED.text_sha256, char_count = EXCLUDED.char_count,"
                " built_at = now()",
                [
                    (
                        row.chunk_id,
                        row.anchor,
                        row.adapter,
                        row.event_id,
                        row.source,
                        row.context,
                        row.event_ts,
                        row.chunk_index,
                        row.text,
                        _sha256(row.text),
                        len(row.text),
                    )
                    for row in rows
                ],
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())",
                page_size=500,
            )

    # -- run -----------------------------------------------------------------

    def run(self, *, max_seconds: float | None = None, batch_size: int = 2000) -> ChunkBuildStats:
        deadline = time.monotonic() + max_seconds if max_seconds else None
        stats = ChunkBuildStats(last_seq=self._load_watermark())
        while True:
            events = self._wh._query_dicts(
                """
                SELECT seq, adapter, event_id, source, context, event_ts, actor,
                       title, snippet, search_text,
                       COALESCE((metadata->>'deleted')::boolean, false) AS deleted
                FROM @timeline_events
                WHERE seq > %s
                ORDER BY seq ASC
                LIMIT %s
                """,
                (stats.last_seq, batch_size),
            )
            if not events:
                stats.caught_up = True
                break
            plain_rows: list[_ChunkRow] = []
            plain_anchors: list[str] = []
            window_keys: dict[tuple[str, str, datetime], None] = {}
            for row in events:
                stats.processed_events += 1
                adapter = row["adapter"]
                if adapter in SKIP_ADAPTERS:
                    continue
                if adapter in WINDOW_ADAPTERS:
                    window_keys[(adapter, row["context"] or "", window_start(row["event_ts"]))] = None
                    continue
                plain_anchors.append(f"{adapter}|{row['event_id']}")
                if not row["deleted"]:
                    plain_rows.extend(self._plain_chunks(row))
            window_rows: list[_ChunkRow] = []
            window_anchors: list[str] = []
            for adapter, context, start in window_keys:
                window_anchors.append(f"{adapter}|w|{context}|{start.isoformat()}")
                window_rows.extend(self._window_chunks(adapter, context, start))
            self._replace_anchor_chunks(plain_anchors + window_anchors, plain_rows + window_rows)
            stats.rebuilt_anchors += len(plain_anchors) + len(window_anchors)
            stats.chunks_written += len(plain_rows) + len(window_rows)
            stats.last_seq = int(events[-1]["seq"])
            self._save_watermark(stats.last_seq)
            if len(events) < batch_size:
                stats.caught_up = True
                break
            if deadline is not None and time.monotonic() >= deadline:
                break
        return stats


class EmbeddingConfigError(RuntimeError):
    pass


class EmbeddingClient:
    """Minimal OpenAI-compatible ``/v1/embeddings`` client.

    Configured entirely from env so the same code path serves cloud OpenAI
    (``SEARCH_EMBEDDINGS_API_KEY``), Gemini's OpenAI-compatibility endpoint,
    or a self-hosted server on the tailnet (``SEARCH_EMBEDDINGS_BASE_URL``
    with no key).
    """

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        dimensions: int,
        timeout_seconds: float = 120.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.dimensions = dimensions
        self.timeout_seconds = timeout_seconds

    @classmethod
    def from_env(cls) -> "EmbeddingClient | None":
        base_url = os.environ.get("SEARCH_EMBEDDINGS_BASE_URL", "").strip()
        api_key = os.environ.get("SEARCH_EMBEDDINGS_API_KEY", "").strip()
        if not base_url and not api_key:
            return None
        return cls(
            base_url=base_url or "https://api.openai.com/v1",
            api_key=api_key,
            model=os.environ.get("SEARCH_EMBEDDINGS_MODEL", "").strip()
            or SEARCH_EMBEDDING_DEFAULT_MODEL,
            dimensions=int(
                os.environ.get("SEARCH_EMBEDDINGS_DIMENSIONS", "").strip()
                or SEARCH_EMBEDDING_DIMENSIONS
            ),
        )

    def _fit_dimensions(self, vector: list[float]) -> list[float]:
        """Coerce a returned vector to the configured dimensionality.

        Servers that ignore the ``dimensions`` request parameter (several
        self-hosted backends) return the model's native width. For
        Matryoshka-trained models (text-embedding-3-*, Qwen3-Embedding,
        nomic-embed v1.5) prefix truncation + L2 renormalization is the
        *defined* way to shorten, so a larger vector is truncated rather than
        rejected — pick an MRL model when self-hosting. A smaller vector can
        never be widened and errors.
        """
        if len(vector) == self.dimensions:
            return vector
        if len(vector) < self.dimensions:
            raise EmbeddingConfigError(
                f"embeddings endpoint returned {len(vector)}-dim vectors; "
                f"expected {self.dimensions} (set SEARCH_EMBEDDINGS_DIMENSIONS "
                "or serve a wider model)"
            )
        prefix = vector[: self.dimensions]
        norm = sum(value * value for value in prefix) ** 0.5 or 1.0
        return [value / norm for value in prefix]

    def embed(self, texts: list[str]) -> list[list[float]]:
        import requests

        payload: dict[str, Any] = {
            "model": self.model,
            "input": [text[:EMBED_MAX_CHARS] for text in texts],
        }
        # Matryoshka truncation; servers that do not support the parameter
        # (some self-hosted backends) must be configured to emit the right
        # dimensionality natively, and the runner validates the result size.
        payload["dimensions"] = self.dimensions
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        last_error: Exception | None = None
        for attempt in range(4):
            try:
                response = requests.post(
                    f"{self.base_url}/embeddings",
                    json=payload,
                    headers=headers,
                    timeout=self.timeout_seconds,
                )
                if response.status_code in (429, 500, 502, 503, 504):
                    raise EmbeddingConfigError(
                        f"embeddings endpoint returned {response.status_code}: {response.text[:300]}"
                    )
                response.raise_for_status()
                data = response.json()["data"]
                vectors = [item["embedding"] for item in sorted(data, key=lambda i: i["index"])]
                if len(vectors) != len(texts):
                    raise EmbeddingConfigError(
                        f"embeddings endpoint returned {len(vectors)} vectors for {len(texts)} inputs"
                    )
                return [self._fit_dimensions(vector) for vector in vectors]
            except Exception as error:  # noqa: BLE001 - retried, re-raised below
                last_error = error
                time.sleep(min(2**attempt, 15))
        raise EmbeddingConfigError(f"embedding request failed after retries: {last_error}")


def vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(f"{value:.6f}" for value in vector) + "]"


class SearchEmbeddingRunner:
    """Embeds every distinct un-embedded chunk text, bounded per run."""

    def __init__(self, warehouse: PostgresWarehouse, client: EmbeddingClient | None = None) -> None:
        self._wh = warehouse
        self._client = client

    def _embedding_column_exists(self) -> bool:
        schema = self._wh._object_schema("search_chunk_embeddings")
        rows = self._wh._query(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = %s AND table_name = 'chunk_embeddings'
              AND column_name = 'embedding'
            """,
            (schema,),
        )
        return bool(rows)

    # -- persisted cursors ---------------------------------------------------

    def _load_state(self) -> dict[str, Any]:
        rows = self._wh._query(
            "SELECT embed_fresh_built_at, embed_fresh_chunk_id, embed_cursor_ts,"
            " embed_cursor_id, embed_backfill_status, embed_orphan_checked_at,"
            " embed_orphan_status"
            " FROM @search_chunk_sync_state WHERE id = %s",
            (_EMBED_STATE_ID,),
        )
        if not rows:
            return {}
        (
            fresh_at,
            fresh_chunk_id,
            cursor_ts,
            cursor_id,
            status,
            orphan_checked_at,
            orphan_status,
        ) = rows[0]
        # Warehouse-wide convention: absence is the epoch, never NULL. A
        # backfill that has not started yet has no keyset, and the walk
        # begins from the newest chunk.
        return {
            "fresh_built_at": fresh_at,
            "fresh_chunk_id": fresh_chunk_id or "",
            "cursor_ts": None if cursor_ts is None or cursor_ts <= _EPOCH else cursor_ts,
            "cursor_id": cursor_id or "",
            "backfill_status": status or "",
            "orphan_checked_at": orphan_checked_at,
            "orphan_status": orphan_status or "",
        }

    def _save_state(self, state: dict[str, Any]) -> None:
        self._wh._command(
            "INSERT INTO @search_chunk_sync_state"
            " (id, last_seq, updated_at, embed_fresh_built_at, embed_fresh_chunk_id,"
            "  embed_cursor_ts, embed_cursor_id, embed_backfill_status,"
            "  embed_orphan_checked_at, embed_orphan_status)"
            " VALUES (%s, 0, now(), %s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (id) DO UPDATE SET updated_at = now(),"
            "  embed_fresh_built_at = EXCLUDED.embed_fresh_built_at,"
            "  embed_fresh_chunk_id = EXCLUDED.embed_fresh_chunk_id,"
            "  embed_cursor_ts = EXCLUDED.embed_cursor_ts,"
            "  embed_cursor_id = EXCLUDED.embed_cursor_id,"
            "  embed_backfill_status = EXCLUDED.embed_backfill_status,"
            "  embed_orphan_checked_at = EXCLUDED.embed_orphan_checked_at,"
            "  embed_orphan_status = EXCLUDED.embed_orphan_status",
            (
                _EMBED_STATE_ID,
                state["fresh_built_at"],
                state["fresh_chunk_id"],
                state["cursor_ts"] if state["cursor_ts"] is not None else _EPOCH,
                state["cursor_id"],
                state["backfill_status"],
                state["orphan_checked_at"],
                state["orphan_status"],
            ),
        )

    def _unembedded(self, model: str, shas: list[str]) -> set[str]:
        """The subset of ``shas`` with no vector yet, by primary-key probe."""

        if not shas:
            return set()
        rows = self._wh._query(
            "SELECT text_sha256 FROM @search_chunk_embeddings"
            " WHERE model = %s AND text_sha256 = ANY(%s)",
            (model, shas),
        )
        return set(shas) - {row[0] for row in rows}

    def _texts_for(self, chunk_ids: list[str]) -> dict[str, str]:
        rows = self._wh._query(
            "SELECT text_sha256, text FROM @search_chunks WHERE chunk_id = ANY(%s)",
            (chunk_ids,),
        )
        return {sha: text for sha, text in rows}

    def run(self, *, limit: int = 5000, max_seconds: float | None = None) -> EmbedStats:
        client = self._client or EmbeddingClient.from_env()
        if client is None:
            return EmbedStats(
                skipped_reason=(
                    "embeddings unconfigured: set SEARCH_EMBEDDINGS_API_KEY (cloud) or "
                    "SEARCH_EMBEDDINGS_BASE_URL (self-hosted OpenAI-compatible endpoint)"
                )
            )
        if not self._embedding_column_exists():
            return EmbedStats(
                skipped_reason=(
                    "pgvector unavailable: the embedding column does not exist yet — "
                    "roll the postgres image to one with pgvector and re-run ensure"
                )
            )
        deadline = time.monotonic() + max_seconds if max_seconds else None
        stats = EmbedStats()
        budget = _EmbedBudget(remaining=limit, deadline=deadline)
        run_started = self._wh._query("SELECT now()")[0][0]
        state = self._load_state()
        if not state:
            # First run on this deployment. The fresh pass takes over from a
            # day ago; everything older is the backfill's job, walked
            # newest-first from the top exactly once.
            state = {
                "fresh_built_at": run_started - timedelta(days=1),
                "fresh_chunk_id": "",
                "cursor_ts": None,
                "cursor_id": "",
                "backfill_status": "",
                "orphan_checked_at": _EPOCH,
                "orphan_status": "",
            }
        # Two passes, both resumable and both bounded, so a run over a
        # caught-up corpus reads a few index pages rather than the 7 GB chunk
        # heap: newly BUILT chunks first (a rebuilt chunk gets a new
        # built_at, so re-chunked text is offered again and the sha probe
        # dedups it), then the one-time historical walk.
        fresh_done = self._drain_fresh(client, state, stats, budget, run_started)
        backfill_done = state["backfill_status"] == "done"
        if fresh_done and not backfill_done and not budget.exhausted():
            backfill_done = self._drain_backfill(client, state, stats, budget)
        orphan_done = self._orphan_check_is_current(state, run_started)
        if fresh_done and backfill_done and not orphan_done and not budget.exhausted():
            orphan_done = self._repair_orphans(client, state, stats, budget, run_started)
        stats.orphans_caught_up = orphan_done
        self._save_state(state)
        stats.caught_up = fresh_done and backfill_done and orphan_done
        return stats

    def _orphan_check_is_current(
        self, state: dict[str, Any], run_started: datetime
    ) -> bool:
        checked_at = state.get("orphan_checked_at") or _EPOCH
        return (
            state.get("orphan_status") == "done"
            and checked_at > _EPOCH
            and run_started - checked_at < EMBED_ORPHAN_RECHECK_INTERVAL
        )

    def _repair_orphans(
        self,
        client: EmbeddingClient,
        state: dict[str, Any],
        stats: EmbedStats,
        budget: "_EmbedBudget",
        run_started: datetime,
    ) -> bool:
        """Repair vectors missing behind otherwise-complete cursors.

        The covering chunk-sha index and the embeddings primary key serve this
        as an index-only anti-join. It runs at most daily after convergence,
        not every ten minutes, so health independently proves completeness
        without becoming another cache-evicting corpus scan.
        """

        if budget.exhausted():
            state["orphan_status"] = "running"
            return False
        probe_limit = budget.remaining + 1
        candidates = self._wh._query(
            "SELECT DISTINCT ON (c.text_sha256) c.text_sha256, c.chunk_id"
            " FROM @search_chunks c"
            " WHERE NOT EXISTS ("
            "   SELECT 1 FROM @search_chunk_embeddings e"
            "   WHERE e.model = %s AND e.text_sha256 = c.text_sha256"
            " )"
            " ORDER BY c.text_sha256, c.chunk_id LIMIT %s",
            (client.model, probe_limit),
        )
        stats.orphaned_found = len(candidates)
        complete_probe = len(candidates) <= budget.remaining
        offered = candidates if complete_probe else candidates[: budget.remaining]
        embedded_before = stats.embedded
        fully_embedded = self._embed_shas(client, offered, stats, budget)
        stats.orphaned_repaired = stats.embedded - embedded_before
        if complete_probe and fully_embedded:
            state["orphan_checked_at"] = run_started
            state["orphan_status"] = "done"
            return True
        state["orphan_status"] = "running"
        return False

    def _drain_fresh(
        self,
        client: EmbeddingClient,
        state: dict[str, Any],
        stats: EmbedStats,
        budget: "_EmbedBudget",
        run_started: datetime,
    ) -> bool:
        """Offer every chunk built since the watermark, oldest-built first.

        Returns True when the pass reached the present. The watermark it
        leaves behind is the last built_at it fully processed, or
        ``run_started - EMBED_FRESH_OVERLAP`` once it has: built_at is the
        writer's transaction start, so a chunk committed by a long transaction
        can carry a built_at older than the newest one already seen, and the
        overlap re-reads that window on the next run. The sha probe makes the
        re-read free.
        """

        floor = state["fresh_built_at"]
        floor_chunk_id = state["fresh_chunk_id"]
        scanned = 0
        while True:
            slab = self._wh._query(
                "SELECT text_sha256, chunk_id, built_at FROM @search_chunks"
                " WHERE (built_at, chunk_id) > (%s, %s)"
                " ORDER BY built_at, chunk_id LIMIT %s",
                (floor, floor_chunk_id, EMBED_SLAB_SIZE),
            )
            if not slab:
                state["fresh_built_at"] = max(floor, run_started - EMBED_FRESH_OVERLAP)
                state["fresh_chunk_id"] = ""
                return True
            scanned += len(slab)
            if not self._embed_shas(client, [(sha, cid) for sha, cid, _ in slab], stats, budget):
                # Budget ran out mid-slab; resume from the same floor.
                return False
            floor_chunk_id = slab[-1][1]
            floor = slab[-1][2]
            state["fresh_built_at"] = floor
            state["fresh_chunk_id"] = floor_chunk_id
            if scanned >= EMBED_FRESH_SCAN_ROWS:
                # A timeline re-walk (an adapter's SQL changed) rebuilds
                # millions of chunks in a day, nearly all with unchanged text.
                # Offer them across runs rather than in one unbounded pass.
                return False
            if len(slab) < EMBED_SLAB_SIZE:
                state["fresh_built_at"] = max(floor, run_started - EMBED_FRESH_OVERLAP)
                state["fresh_chunk_id"] = ""
                return True

    def _drain_backfill(
        self,
        client: EmbeddingClient,
        state: dict[str, Any],
        stats: EmbedStats,
        budget: "_EmbedBudget",
    ) -> bool:
        """Walk the corpus newest-first ONCE, resuming from the saved keyset.

        Recency-first means the searchable period grows backwards from today:
        the year Zach actually queries completes long before 2014's Slack
        does. Each run scans at most EMBED_BACKFILL_SCAN_ROWS rows of the
        (event_ts, chunk_id) index so the walk is amortized across runs
        instead of restarting from the newest chunk every ten minutes.
        Returns True when the walk has reached the oldest chunk.
        """

        scanned = 0
        state["backfill_status"] = state["backfill_status"] or "running"
        while scanned < EMBED_BACKFILL_SCAN_ROWS:
            if state["cursor_ts"] is None:
                slab = self._wh._query(
                    "SELECT text_sha256, chunk_id, event_ts FROM @search_chunks"
                    " ORDER BY event_ts DESC, chunk_id DESC LIMIT %s",
                    (EMBED_SLAB_SIZE,),
                )
            else:
                slab = self._wh._query(
                    "SELECT text_sha256, chunk_id, event_ts FROM @search_chunks"
                    " WHERE (event_ts, chunk_id) < (%s, %s)"
                    " ORDER BY event_ts DESC, chunk_id DESC LIMIT %s",
                    (state["cursor_ts"], state["cursor_id"], EMBED_SLAB_SIZE),
                )
            if not slab:
                state["backfill_status"] = "done"
                return True
            scanned += len(slab)
            if not self._embed_shas(client, [(sha, cid) for sha, cid, _ in slab], stats, budget):
                return False
            state["cursor_ts"], state["cursor_id"] = slab[-1][2], slab[-1][1]
            if len(slab) < EMBED_SLAB_SIZE:
                state["backfill_status"] = "done"
                return True
        return False

    def _embed_shas(
        self,
        client: EmbeddingClient,
        candidates: list[tuple[str, str]],
        stats: EmbedStats,
        budget: "_EmbedBudget",
    ) -> bool:
        """Embed the un-embedded shas among ``candidates``.

        Returns False when the run budget stopped it before the slab was
        fully processed, so the caller leaves its cursor where it was.
        """

        by_sha: dict[str, str] = {}
        for sha, chunk_id in candidates:
            by_sha.setdefault(sha, chunk_id)
        missing = self._unembedded(client.model, list(by_sha))
        if not missing:
            return True
        if budget.exhausted():
            return False
        pending_ids = [by_sha[sha] for sha in by_sha if sha in missing]
        texts = self._texts_for(pending_ids)
        pending = [(sha, texts[sha]) for sha in by_sha if sha in missing and sha in texts]
        if len(pending) > budget.remaining:
            # More than the run may spend: do what fits and report a partial
            # slab so the cursor is not advanced past unembedded rows.
            pending = pending[: budget.remaining]
            partial = True
        else:
            partial = False
        self._embed_batches(client, pending, stats, budget)
        return not partial and not budget.exhausted_by_deadline

    def _embed_batches(
        self,
        client: EmbeddingClient,
        pending: list[tuple[str, str]],
        stats: EmbedStats,
        budget: "_EmbedBudget",
    ) -> None:
        table = self._wh.sql_relation("search_chunk_embeddings")
        batches = [
            pending[i : i + EMBED_BATCH_SIZE]
            for i in range(0, len(pending), EMBED_BATCH_SIZE)
        ]
        # Serial on purpose: TEI is also the latency-critical query embedder.
        # Pre-submitting a whole run to a worker pool leaves its queue occupied
        # continuously, so an interactive request waits behind bulk work. The
        # DB insert between calls creates a natural scheduling gap as well.
        for batch in batches:
            if budget.exhausted():
                break
            vectors = client.embed([text for _, text in batch])
            with self._wh._connection.cursor() as db_cursor:
                execute_values(
                    db_cursor,
                    f"INSERT INTO {table} (text_sha256, model, token_count, embedded_at, embedding)"
                    " VALUES %s ON CONFLICT (text_sha256, model) DO UPDATE SET"
                    " token_count = EXCLUDED.token_count, embedded_at = now(),"
                    " embedding = EXCLUDED.embedding",
                    [
                        (sha, client.model, max(1, len(text) // 4), vector_literal(vector))
                        for (sha, text), vector in zip(batch, vectors, strict=True)
                    ],
                    template=(
                        "(%s, %s, %s, now(), %s::public.halfvec("
                        + str(SEARCH_EMBEDDING_DIMENSIONS)
                        + "))"
                    ),
                    page_size=200,
                )
            stats.embedded += len(batch)
            budget.remaining -= len(batch)
            budget.check_deadline()


@dataclass
class _EmbedBudget:
    """What one drain run may still spend: rows, and wall-clock."""

    remaining: int
    deadline: float | None
    exhausted_by_deadline: bool = False

    def check_deadline(self) -> None:
        if self.deadline is not None and time.monotonic() >= self.deadline:
            self.exhausted_by_deadline = True

    def exhausted(self) -> bool:
        self.check_deadline()
        return self.remaining <= 0 or self.exhausted_by_deadline
