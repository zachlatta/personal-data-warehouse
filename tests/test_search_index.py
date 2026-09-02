from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.postgres import (
    SEARCH_EMBEDDING_DIMENSIONS,
    PostgresWarehouse,
)
from personal_data_warehouse.search_index import (
    CHUNK_MAX_CHARS,
    EmbeddingClient,
    SearchChunkBuilder,
    SearchEmbeddingRunner,
    split_text,
    vector_literal,
    window_start,
)
from personal_data_warehouse.timeline import TimelineSyncEngine


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    schema = make_test_schema()
    wh = PostgresWarehouse(_postgres_url(), schema=schema)
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def _pgvector_usable(wh: PostgresWarehouse) -> bool:
    return wh.pgvector_available()


def _provision(wh: PostgresWarehouse) -> None:
    # The timeline engine runs every adapter, so every source table must
    # exist; borrow the exhaustive helper the search_text tests use.
    from tests.test_postgres_warehouse import _ensure_all_table_groups

    _ensure_all_table_groups(wh)
    wh.ensure_search_index_tables()


def _sync_timeline(wh: PostgresWarehouse) -> None:
    engine = TimelineSyncEngine(
        source_url=_postgres_url(),
        source_schema=wh._schema,
        dest_schema=wh._schema,
    )
    try:
        engine.run()
    finally:
        engine.close()


# --- pure chunking ------------------------------------------------------------


def test_split_text_small_document_is_one_chunk() -> None:
    assert split_text("hello world") == ["hello world"]
    assert split_text("  ") == []
    assert split_text("ab") == []  # below the minimum


def test_split_text_large_document_splits_on_line_boundaries() -> None:
    lines = [f"line {i} " + "x" * 80 for i in range(100)]
    doc = "\n".join(lines)
    chunks = split_text(doc)
    assert len(chunks) > 1
    assert all(len(chunk) <= CHUNK_MAX_CHARS for chunk in chunks)
    # Nothing is lost: every line's marker appears in some chunk.
    joined = "\n".join(chunks)
    for i in range(100):
        assert f"line {i} " in joined
    # Deterministic: identical input yields identical chunks (stable shas so
    # rebuilds never re-embed unchanged text).
    assert chunks == split_text(doc)


def test_window_start_floors_to_the_hour() -> None:
    ts = datetime(2026, 5, 19, 12, 42, 31, tzinfo=UTC)
    assert window_start(ts) == datetime(2026, 5, 19, 12, tzinfo=UTC)


def test_vector_literal_shape() -> None:
    literal = vector_literal([0.5, -1.0, 0.25])
    assert literal == "[0.500000,-1.000000,0.250000]"


def test_search_health_distinguishes_fresh_work_from_convergence(
    warehouse: PostgresWarehouse,
) -> None:
    """A fresh heartbeat with backlog must never present as green."""
    warehouse.ensure_pipeline_health_tables()
    warehouse.write_search_health(
        "chunks",
        timeline_max_seq=120,
        chunk_cursor_seq=100,
        caught_up=0,
        processed_rows=50,
        pending_count=-1,
        last_success_at=datetime.now(tz=UTC),
    )
    row = warehouse._query_dicts("SELECT * FROM @marts_search_health WHERE component = 'chunks'")[0]
    assert row["status"] == "backfilling"
    assert row["seq_lag"] == 20
    assert row["pending_count"] is None
    assert row["pending_age_seconds"] is None

    # A backlog is only "backfilling" while the oldest unprocessed timeline
    # row is recent. Once it has waited longer than SEARCH_HEALTH_LATE_AFTER
    # the semantic corpus is materially behind the timeline and the row says
    # so, instead of reporting a heartbeat that looks like progress forever.
    warehouse.write_search_health(
        "chunks",
        timeline_max_seq=120,
        chunk_cursor_seq=100,
        caught_up=0,
        processed_rows=50,
        pending_count=-1,
        oldest_pending_at=datetime.now(tz=UTC) - timedelta(hours=2),
        last_success_at=datetime.now(tz=UTC),
    )
    row = warehouse._query_dicts("SELECT * FROM @marts_search_health WHERE component = 'chunks'")[0]
    assert row["status"] == "late"
    assert row["pending_age_seconds"] >= 7000

    warehouse.write_search_health(
        "chunks",
        oldest_pending_at=datetime.now(tz=UTC) - timedelta(minutes=5),
        last_success_at=datetime.now(tz=UTC),
    )
    row = warehouse._query_dicts("SELECT * FROM @marts_search_health WHERE component = 'chunks'")[0]
    assert row["status"] == "backfilling"

    warehouse.write_search_health(
        "chunks",
        timeline_max_seq=120,
        chunk_cursor_seq=120,
        caught_up=1,
        processed_rows=20,
        pending_count=0,
        last_success_at=datetime.now(tz=UTC),
    )
    row = warehouse._query_dicts("SELECT * FROM @marts_search_health WHERE component = 'chunks'")[0]
    assert row["status"] == "ok"
    assert row["pending_count"] == 0

    success_at = row["last_success_at"]
    warehouse.write_search_health("chunks", last_error="worker failed")
    row = warehouse._query_dicts(
        "SELECT * FROM @marts_search_health WHERE component = 'chunks'"
    )[0]
    assert row["status"] == "failing"
    assert row["last_success_at"] == success_at


def test_oldest_pending_timeline_write_is_the_epoch_when_nothing_is_pending(
    warehouse: PostgresWarehouse,
) -> None:
    from personal_data_warehouse.defs.search_index import _oldest_pending_timeline_write

    warehouse.ensure_timeline_tables()
    assert _oldest_pending_timeline_write(warehouse, 0) == datetime.fromtimestamp(0, tz=UTC)


# --- chunk builder (live) -----------------------------------------------------

from tests.test_postgres_warehouse import (  # noqa: E402 - shared fixtures
    _message_row,
    _slack_conversation_row,
    _slack_message_row,
)


def _seed_slack(wh: PostgresWarehouse, texts: list[str], *, start_minute: int = 0) -> None:
    base = datetime(2026, 5, 19, 12, tzinfo=UTC)
    wh.insert_slack_conversations(
        [_slack_conversation_row(conversation_id="C1", conversation_type="private_channel", sync_version=1)]
    )
    wh.insert_slack_messages(
        [
            _slack_message_row(
                conversation_id="C1",
                message_ts=f"{100 + start_minute}.{i}",
                message_datetime=base + timedelta(minutes=start_minute + i),
                text=text,
            )
            for i, text in enumerate(texts)
        ]
    )


def test_chunk_builder_windows_chat_and_chunks_documents(warehouse: PostgresWarehouse) -> None:
    _provision(warehouse)
    warehouse._set_search_path()
    _seed_slack(warehouse, ["morning standup notes", "the quokka budget is approved", "lunch plans"])
    warehouse.insert_messages(
        [_message_row(message_id="m1", subject="quarterly xylophone report", labels=["INBOX"], sync_version=1)]
    )
    _sync_timeline(warehouse)

    stats = SearchChunkBuilder(warehouse).run()
    assert stats.caught_up
    assert stats.chunks_written > 0

    rows = warehouse._query_dicts(
        "SELECT chunk_id, anchor, adapter, source, context, text FROM @search_chunks ORDER BY chunk_id"
    )
    by_adapter: dict[str, list[dict]] = {}
    for row in rows:
        by_adapter.setdefault(row["adapter"], []).append(row)

    # Chat messages collapse into one conversation-window chunk, not three
    # tiny per-message chunks.
    slack_chunks = by_adapter.get("slack_message", [])
    assert len(slack_chunks) == 1
    assert "quokka budget" in slack_chunks[0]["text"]
    assert "standup" in slack_chunks[0]["text"]
    assert slack_chunks[0]["anchor"].startswith("slack_message|w|")

    # Gmail is chunked per event.
    gmail_chunks = by_adapter.get("gmail_email", [])
    assert len(gmail_chunks) == 1
    assert "xylophone" in gmail_chunks[0]["text"]

    # Incremental: a new message in the same window rebuilds that window's
    # chunk in place (same anchor, no duplicate windows).
    _seed_slack(warehouse, ["one more thing: ship the zeppelin"], start_minute=10)
    _sync_timeline(warehouse)
    stats2 = SearchChunkBuilder(warehouse).run()
    assert stats2.caught_up
    slack_rows = warehouse._query_dicts("SELECT anchor, text FROM @search_chunks WHERE adapter = 'slack_message'")
    assert len(slack_rows) == 1
    assert "zeppelin" in slack_rows[0]["text"]
    assert "quokka budget" in slack_rows[0]["text"]

    # A caught-up second run is a no-op.
    stats3 = SearchChunkBuilder(warehouse).run()
    assert stats3.processed_events == 0 and stats3.caught_up


def test_chunk_builder_skips_machinery_adapters(warehouse: PostgresWarehouse) -> None:
    _provision(warehouse)
    warehouse.ensure_agent_tables()
    warehouse._set_search_path()
    from personal_data_warehouse.schema import AGENT_RUN_COLUMNS

    created_at = datetime(2026, 5, 19, 12, tzinfo=UTC)
    run_row = {column: "" for column in AGENT_RUN_COLUMNS}
    run_row.update(
        run_id="agent-zzz",
        task_type="test",
        status="succeeded",
        exit_code=0,
        started_at=created_at,
        completed_at=created_at,
        sync_version=1,
    )
    warehouse.insert_agent_runs([run_row])
    _sync_timeline(warehouse)
    SearchChunkBuilder(warehouse).run()
    rows = warehouse._query("SELECT count(*) FROM @search_chunks WHERE adapter = 'enrichment_run'")
    assert rows[0][0] == 0


# --- embeddings (live, requires pgvector) ------------------------------------


class _FakeEmbeddingClient(EmbeddingClient):
    """Deterministic offline embedder: hash-bucket one-hot-ish vectors."""

    def __init__(self) -> None:
        super().__init__(
            base_url="http://fake", api_key="fake", model="fake-model", dimensions=SEARCH_EMBEDDING_DIMENSIONS
        )
        self.calls = 0

    def embed(self, texts: list[str]) -> list[list[float]]:
        self.calls += 1
        vectors = []
        for text in texts:
            vector = [0.0] * self.dimensions
            for token in text.lower().split():
                vector[hash(token) % self.dimensions] += 1.0
            norm = sum(v * v for v in vector) ** 0.5 or 1.0
            vectors.append([v / norm for v in vector])
        return vectors


def test_embedding_runner_reports_unconfigured(warehouse: PostgresWarehouse) -> None:
    _provision(warehouse)
    for var in ("SEARCH_EMBEDDINGS_API_KEY", "SEARCH_EMBEDDINGS_BASE_URL"):
        assert not os.environ.get(var), f"{var} set in test env; unset to run this test"
    stats = SearchEmbeddingRunner(warehouse).run()
    assert "unconfigured" in stats.skipped_reason


def test_embedding_runner_embeds_each_distinct_text_once(warehouse: PostgresWarehouse) -> None:
    _provision(warehouse)
    if not _pgvector_usable(warehouse):
        pytest.skip("pgvector is not installed on this Postgres host")
    warehouse._set_search_path()
    _seed_slack(warehouse, ["alpha bravo charlie", "delta echo foxtrot"])
    _sync_timeline(warehouse)
    SearchChunkBuilder(warehouse).run()

    client = _FakeEmbeddingClient()
    stats = SearchEmbeddingRunner(warehouse, client).run()
    assert stats.embedded > 0 and stats.caught_up

    rows = warehouse._query(
        "SELECT count(*), count(embedding) FROM @search_chunk_embeddings WHERE model = 'fake-model'"
    )
    total, with_vectors = rows[0]
    assert total == with_vectors == stats.embedded

    # Re-run: nothing new to embed, and the client is not called again.
    calls_before = client.calls
    stats2 = SearchEmbeddingRunner(warehouse, client).run()
    assert stats2.embedded == 0 and stats2.caught_up
    assert client.calls == calls_before


def test_search_hybrid_fuses_semantic_and_keyword_ranks(warehouse: PostgresWarehouse) -> None:
    if not _pgvector_usable(warehouse):
        pytest.skip("pgvector is not installed on this Postgres host")
    rows = warehouse._query(
        "SELECT 1 FROM pg_available_extensions WHERE name = 'pg_textsearch'"
        " AND current_setting('shared_preload_libraries') LIKE '%pg_textsearch%'"
    )
    if not rows:
        pytest.skip("pg_textsearch is not installed/preloaded on this Postgres host")

    _provision(warehouse)
    warehouse._set_search_path()
    _seed_slack(
        warehouse,
        [
            "the marmoset enclosure needs cleaning",
            "totally unrelated coffee chatter",
        ],
    )
    _sync_timeline(warehouse)
    SearchChunkBuilder(warehouse).run()
    client = _FakeEmbeddingClient()
    SearchEmbeddingRunner(warehouse, client).run()
    # ensure again so the signature guard sees chunks+vector and creates
    # search_hybrid (provisioning order in this test built tables first).
    warehouse._command("DELETE FROM @search_schema_state")
    warehouse._ensure_search_views_if_possible()

    # Query vector: embed the query text with the same fake embedder, so the
    # window chunk containing the marmoset line is the nearest neighbor.
    [query_vector] = client.embed(["marmoset enclosure"])
    hits = warehouse._query_dicts(
        "SELECT source, ref, text, score, event_ts, source_table FROM @search_hybrid(%s, %s, 'fake-model', 10)",
        ("marmoset enclosure", vector_literal(query_vector)),
    )
    assert hits, "expected hybrid hits"
    assert all(h["score"] < 0 for h in hits), "hybrid scores are negative RRF (lower = better)"
    top = hits[0]
    assert "marmoset" in top["text"]
    assert top["source"] == "slack"
    assert top["ref"].startswith("slack_message:")
    assert top["event_ts"] is not None

    # sources filter + aliases work on the semantic branch too.
    scoped = warehouse._query_dicts(
        "SELECT ref FROM @search_hybrid(%s, %s, 'fake-model', 10, ARRAY['gmail'])",
        ("marmoset enclosure", vector_literal(query_vector)),
    )
    assert scoped == []

    with pytest.raises(Exception, match="query_embedding is required"):
        warehouse._query("SELECT * FROM @search_hybrid('x', '')")


def _embed_state(wh: PostgresWarehouse) -> tuple:
    rows = wh._query(
        "SELECT embed_fresh_built_at, embed_fresh_chunk_id, embed_cursor_ts,"
        " embed_cursor_id, embed_backfill_status"
        " FROM @search_chunk_sync_state WHERE id = 'embeddings'"
    )
    return rows[0] if rows else None


def test_embedding_runner_persists_its_cursors_and_reads_only_new_chunks(
    warehouse: PostgresWarehouse,
) -> None:
    """A run over a caught-up corpus must not restart the walk from the top.

    The drain used to re-scan the whole chunk heap on every run because its
    keyset cursor lived in a local variable; the state row is what makes the
    second run cheap. Observable contract: after the first run the row
    exists with the backfill marked done; a chunk built afterwards is
    embedded by the next run; a run with nothing new makes no embedding
    calls and stays caught up.
    """

    _provision(warehouse)
    if not _pgvector_usable(warehouse):
        pytest.skip("pgvector is not installed on this Postgres host")
    warehouse._set_search_path()
    _seed_slack(warehouse, ["alpha bravo charlie", "delta echo foxtrot"])
    _sync_timeline(warehouse)
    SearchChunkBuilder(warehouse).run()

    client = _FakeEmbeddingClient()
    stats = SearchEmbeddingRunner(warehouse, client).run()
    assert stats.embedded > 0 and stats.caught_up
    fresh_at, fresh_chunk_id, cursor_ts, cursor_id, status = _embed_state(warehouse)
    assert status == "done"
    assert fresh_at.year >= 2026
    assert isinstance(fresh_chunk_id, str)

    # New chunk after the watermark: only it is embedded next run.
    _seed_slack(warehouse, ["golf hotel india"], start_minute=120)
    _sync_timeline(warehouse)
    SearchChunkBuilder(warehouse).run()
    calls_before = client.calls
    stats2 = SearchEmbeddingRunner(warehouse, client).run()
    assert stats2.embedded == 1 and stats2.caught_up
    assert client.calls == calls_before + 1
    assert _embed_state(warehouse)[4] == "done"

    # Nothing new: no embedding request at all.
    calls_before = client.calls
    stats3 = SearchEmbeddingRunner(warehouse, client).run()
    assert stats3.embedded == 0 and stats3.caught_up
    assert client.calls == calls_before


def test_embedding_fresh_cursor_does_not_skip_chunks_with_the_same_built_at(
    warehouse: PostgresWarehouse,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fresh cursor is the complete ``(built_at, chunk_id)`` keyset.

    A chunk-build transaction gives every row the same ``built_at``. The old
    timestamp-only cursor advanced after the first 5,000-row slab and made the
    rest of that transaction permanently invisible to the embedder.
    """

    _provision(warehouse)
    if not _pgvector_usable(warehouse):
        pytest.skip("pgvector is not installed on this Postgres host")
    warehouse._set_search_path()
    for i in range(3):
        _seed_slack(warehouse, [f"same timestamp chunk {i}"], start_minute=120 * i)
    _sync_timeline(warehouse)
    SearchChunkBuilder(warehouse).run()
    built_at = datetime(2026, 8, 28, 12, tzinfo=UTC)
    warehouse._command("UPDATE @search_chunks SET built_at = %s", (built_at,))
    warehouse._command("DELETE FROM @search_chunk_embeddings")
    warehouse._command(
        "INSERT INTO @search_chunk_sync_state"
        " (id, last_seq, updated_at, embed_fresh_built_at, embed_fresh_chunk_id,"
        "  embed_cursor_ts, embed_cursor_id, embed_backfill_status)"
        " VALUES ('embeddings', 0, now(), %s, '', %s, '', 'done')"
        " ON CONFLICT (id) DO UPDATE SET"
        " embed_fresh_built_at = EXCLUDED.embed_fresh_built_at,"
        " embed_fresh_chunk_id = '', embed_backfill_status = 'done'",
        (built_at - timedelta(seconds=1), datetime(1970, 1, 1, tzinfo=UTC)),
    )
    monkeypatch.setattr("personal_data_warehouse.search_index.EMBED_SLAB_SIZE", 2)

    stats = SearchEmbeddingRunner(warehouse, _FakeEmbeddingClient()).run()

    expected = warehouse._query("SELECT count(DISTINCT text_sha256) FROM @search_chunks")[0][0]
    embedded = warehouse._query(
        "SELECT count(*) FROM @search_chunk_embeddings WHERE model = 'fake-model'"
    )[0][0]
    assert expected >= 3
    assert stats.caught_up
    assert embedded == expected


def test_embedding_runner_repairs_orphans_behind_both_cursors(
    warehouse: PostgresWarehouse,
) -> None:
    """Cursor convergence is not proof that every chunk has a vector."""

    _provision(warehouse)
    if not _pgvector_usable(warehouse):
        pytest.skip("pgvector is not installed on this Postgres host")
    warehouse._set_search_path()
    _seed_slack(warehouse, ["orphan repair alpha", "orphan repair bravo"])
    _sync_timeline(warehouse)
    SearchChunkBuilder(warehouse).run()
    client = _FakeEmbeddingClient()
    assert SearchEmbeddingRunner(warehouse, client).run().caught_up
    warehouse._command(
        "UPDATE @search_chunks SET built_at = now() - interval '3 days'"
    )

    orphan_sha = warehouse._query(
        "SELECT text_sha256 FROM @search_chunk_embeddings"
        " WHERE model = 'fake-model' ORDER BY text_sha256 LIMIT 1"
    )[0][0]
    warehouse._command(
        "DELETE FROM @search_chunk_embeddings WHERE model = 'fake-model' AND text_sha256 = %s",
        (orphan_sha,),
    )
    # Force the periodic completeness proof due without moving either normal
    # cursor. This is the production shape: both cursor passes said done while
    # a timestamp tie had stranded rows behind them.
    warehouse._command(
        "UPDATE @search_chunk_sync_state"
        " SET embed_orphan_checked_at = TIMESTAMPTZ '1970-01-01 00:00:00+00',"
        "     embed_orphan_status = 'done' WHERE id = 'embeddings'"
    )

    stats = SearchEmbeddingRunner(warehouse, client).run()

    assert stats.orphaned_found == 1
    assert stats.orphaned_repaired == 1
    assert stats.orphans_caught_up
    assert stats.caught_up
    assert warehouse._query(
        "SELECT count(*) FROM @search_chunk_embeddings"
        " WHERE model = 'fake-model' AND text_sha256 = %s",
        (orphan_sha,),
    )[0][0] == 1


def test_orphaned_chunks_is_a_first_class_search_health_component(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_pipeline_health_tables()
    warehouse.write_search_health(
        "orphaned_chunks",
        model="fake-model",
        caught_up=0,
        pending_count=-1,
        processed_rows=12,
        last_success_at=datetime.now(tz=UTC),
    )
    row = warehouse._query_dicts(
        "SELECT * FROM @marts_search_health WHERE component = 'orphaned_chunks'"
    )[0]
    assert row["status"] == "backfilling"
    assert row["pending_count"] is None


def test_cache_residency_is_visible_and_zero_is_attention(
    warehouse: PostgresWarehouse,
) -> None:
    warehouse.ensure_pipeline_health_tables()
    warehouse.write_search_health(
        "cache_residency",
        caught_up=1,
        pending_count=0,
        processed_rows=5,
        resident_bytes=0,
        total_bytes=10_000,
        resident_fraction=0.0,
        last_success_at=datetime.now(tz=UTC),
    )
    row = warehouse._query_dicts(
        "SELECT * FROM @marts_search_health WHERE component = 'cache_residency'"
    )[0]
    assert row["status"] == "attention"
    assert row["resident_bytes"] == 0
    assert row["total_bytes"] == 10_000
    assert float(row["resident_fraction"]) == 0.0

    warehouse.write_search_health(
        "cache_residency",
        resident_bytes=2_500,
        resident_fraction=0.25,
        last_success_at=datetime.now(tz=UTC),
        last_error="",
    )
    row = warehouse._query_dicts(
        "SELECT * FROM @marts_search_health WHERE component = 'cache_residency'"
    )[0]
    assert row["status"] == "ok"
    assert float(row["resident_fraction"]) == 0.25


def test_embedding_runner_resumes_a_bounded_backfill_across_runs(
    warehouse: PostgresWarehouse,
) -> None:
    """A run that stops at its limit leaves a cursor the next run continues from."""

    _provision(warehouse)
    if not _pgvector_usable(warehouse):
        pytest.skip("pgvector is not installed on this Postgres host")
    warehouse._set_search_path()
    # One chat window per hour, so four texts two hours apart are four
    # distinct chunks rather than one window holding all of them.
    for i in range(4):
        _seed_slack(warehouse, [f"token{i} unique text number {i}"], start_minute=120 * i)
    _sync_timeline(warehouse)
    SearchChunkBuilder(warehouse).run()
    # Age every chunk out of the fresh window so the backfill has to do it.
    warehouse._command(
        "UPDATE @search_chunks SET built_at = now() - interval '3 days'"
    )
    client = _FakeEmbeddingClient()
    runner = SearchEmbeddingRunner(warehouse, client)
    total = warehouse._query("SELECT count(DISTINCT text_sha256) FROM @search_chunks")[0][0]
    assert total >= 2

    first = runner.run(limit=1)
    assert first.embedded == 1 and not first.caught_up
    assert _embed_state(warehouse)[4] == "running"

    embedded = first.embedded
    for _ in range(total + 2):
        stats = runner.run(limit=1)
        embedded += stats.embedded
        if stats.caught_up:
            break
    assert stats.caught_up
    assert embedded == total
    assert _embed_state(warehouse)[4] == "done"
    rows = warehouse._query(
        "SELECT count(*) FROM @search_chunk_embeddings WHERE model = 'fake-model'"
    )
    assert rows[0][0] == total


def test_bm25_index_probe_covers_every_timeline_bm25_index(warehouse: PostgresWarehouse) -> None:
    """A corrupt BM25 index must show up as a health fact, so the probe has
    to read every index the search functions pin by name."""

    from personal_data_warehouse.postgres import POSTGRES_INDEXES

    declared = {
        spec.name for spec in POSTGRES_INDEXES
        if spec.table == "timeline_events" and spec.requires_pg_textsearch
    }
    assert set(warehouse.bm25_timeline_index_names()) == declared
    assert len(declared) >= 4

    _provision(warehouse)
    warehouse.ensure_pipeline_health_tables()
    warehouse._set_search_path()
    _seed_slack(warehouse, ["alpha bravo charlie"])
    _sync_timeline(warehouse)
    probe = warehouse.probe_bm25_indexes()
    assert probe, "no BM25 index existed to probe"
    assert all(err == "" for err in probe.values()), probe
    warehouse.write_search_health("bm25_indexes", caught_up=1, processed_rows=len(probe))
    rows = warehouse._query("SELECT status FROM @marts_search_health WHERE component = 'bm25_indexes'")
    assert rows and rows[0][0] == "ok"
