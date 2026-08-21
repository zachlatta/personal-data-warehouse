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

EMBED_BATCH_SIZE = 128
# Candidate rows fetched per keyset page (pre-dedupe); bounds memory while
# amortizing the anti-join probe cost over many embed batches.
EMBED_SLAB_SIZE = 5_000
# Embedding-model input cap safety: ~8k tokens for the OpenAI small models.
EMBED_MAX_CHARS = 20_000

_STATE_ID = "timeline"


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
        remaining = limit
        table = self._wh.sql_relation("search_chunk_embeddings")
        # Newest-first keyset cursor over chunks. Recency-first means the
        # searchable period grows backwards from today — the year Zach
        # actually queries completes long before 2014's Slack does. The
        # keyset (event_ts, chunk_id) pages the anti-join WITHOUT re-scanning
        # the whole chunk table per batch: the previous per-batch
        # GROUP-BY-over-everything candidate query collapsed throughput to
        # ~40k/h once the corpus hit millions of chunks.
        cursor_ts: datetime | None = None
        cursor_id = ""
        while remaining > 0:
            if cursor_ts is None:
                slab = self._wh._query(
                    """
                    SELECT c.text_sha256, c.text, c.event_ts, c.chunk_id
                    FROM @search_chunks c
                    WHERE NOT EXISTS (
                        SELECT 1 FROM @search_chunk_embeddings e
                        WHERE e.text_sha256 = c.text_sha256 AND e.model = %s
                    )
                    ORDER BY c.event_ts DESC, c.chunk_id DESC
                    LIMIT %s
                    """,
                    (client.model, EMBED_SLAB_SIZE),
                )
            else:
                slab = self._wh._query(
                    """
                    SELECT c.text_sha256, c.text, c.event_ts, c.chunk_id
                    FROM @search_chunks c
                    WHERE (c.event_ts, c.chunk_id) < (%s, %s)
                      AND NOT EXISTS (
                        SELECT 1 FROM @search_chunk_embeddings e
                        WHERE e.text_sha256 = c.text_sha256 AND e.model = %s
                    )
                    ORDER BY c.event_ts DESC, c.chunk_id DESC
                    LIMIT %s
                    """,
                    (cursor_ts, cursor_id, client.model, EMBED_SLAB_SIZE),
                )
            if not slab:
                stats.caught_up = True
                break
            cursor_ts, cursor_id = slab[-1][2], slab[-1][3]
            # Dedupe by content sha within the slab (identical text repeats
            # across windows); the anti-join handles cross-slab repeats.
            seen: dict[str, str] = {}
            for sha, text, _ts, _cid in slab:
                if sha not in seen:
                    seen[sha] = text
            pending = list(seen.items())[: max(remaining, 0)]
            if not pending:
                continue
            batches = [
                pending[i : i + EMBED_BATCH_SIZE]
                for i in range(0, len(pending), EMBED_BATCH_SIZE)
            ]
            # Two requests in flight keeps the GPU busy while the previous
            # batch's rows insert; more would only queue inside TEI.
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [
                    pool.submit(client.embed, [text for _, text in batch])
                    for batch in batches
                ]
                for batch, future in zip(batches, futures, strict=True):
                    vectors = future.result()
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
                    remaining -= len(batch)
                    if deadline is not None and time.monotonic() >= deadline:
                        for pending_future in futures:
                            pending_future.cancel()
                        return stats
        return stats
