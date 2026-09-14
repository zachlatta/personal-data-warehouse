"""Warehouse contract for the Hacker News source.

Same two halves as tests/test_whoop_private_warehouse.py: registry checks that
need no database (the "Adding a warehouse source" checklist's SILENT steps), and
real-Postgres checks for the DDL, the upserts, the walk frontier and the
credential handling.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
import os

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.pipeline_health import PIPELINES, TABLE_PIPELINES
from personal_data_warehouse.postgres import (
    POSTGRES_INDEXES,
    POSTGRES_TABLES,
    SEARCH_SOURCE_DEFS,
    PostgresWarehouse,
    _postgres_type,
)
from personal_data_warehouse.relations import CATALOG, relation
from personal_data_warehouse.schema import (
    HACKER_NEWS_ITEM_COLUMNS,
    HACKER_NEWS_PROFILE_COLUMNS,
    HACKER_NEWS_SESSION_COLUMNS,
    HACKER_NEWS_SYNC_STATE_COLUMNS,
    HACKER_NEWS_USER_ITEM_COLUMNS,
)
from personal_data_warehouse.timeline import (
    TIMELINE_ADAPTERS,
    TIMELINE_CONTEXT_GENERIC_ADAPTERS,
    TIMELINE_TABLE_COVERAGE,
)

NOW = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

EXPECTED_TABLES = {
    "hacker_news_items": ("base_hacker_news", "items", ("account", "item_id")),
    "hacker_news_user_items": (
        "base_hacker_news",
        "user_items",
        ("account", "item_id", "relation"),
    ),
    "hacker_news_profile": ("base_hacker_news", "profile", ("account", "user_id")),
    "hacker_news_sync_state": ("ops", "hacker_news_sync_state", ("account", "list_name")),
    "hacker_news_sessions": ("private", "hacker_news_sessions", ("account", "session_key")),
}


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


# ---------------------------------------------------------------------------
# no database required
# ---------------------------------------------------------------------------


def test_every_table_is_cataloged_at_the_contracted_location() -> None:
    for logical, (schema, name, pk) in EXPECTED_TABLES.items():
        rel = relation(logical)
        assert (rel.schema, rel.name) == (schema, name), logical
        if logical == "hacker_news_sessions":
            # Raw DDL (nullable expired_at), like chatgpt_sessions; its key is
            # pinned against the app's Go store below.
            assert logical not in POSTGRES_TABLES
            continue
        assert POSTGRES_TABLES[logical].primary_key == pk, logical


def test_the_source_owns_its_own_base_schema() -> None:
    schema = CATALOG.schema("base_hacker_news")
    assert schema.layer == "base"
    assert schema.domain == "hacker_news"
    assert schema.discoverable is True


def test_the_session_credential_is_secret_and_denied_to_the_query_role() -> None:
    obj = CATALOG.object("hacker_news_sessions")
    assert obj.secret is True
    assert obj.query_access == "denied"
    assert obj.discoverable is False
    assert obj.schema == "private"
    assert "private" in CATALOG.denied_schemas()


def test_the_sync_state_is_hidden_ops_not_a_query_surface() -> None:
    obj = CATALOG.object("hacker_news_sync_state")
    assert obj.schema == "ops"
    assert obj.discoverable is False
    assert obj.query_access == "denied"


def test_the_columns_that_carry_time_and_counts_are_typed() -> None:
    """A column typed text by default is the silent failure here: ORDER BY on
    a text timestamp and sum() over a text count both run and both lie."""
    for column in (
        "posted_at",
        "fetched_at",
        "first_seen_at",
        "synced_at",
        "discovered_at",
        "removed_at",
        "created_at",
        "last_success_at",
        "full_walk_completed_at",
        "published_at",
        "updated_at",
    ):
        assert _postgres_type(column, table="hacker_news_items") == "timestamptz", column
    for column in ("descendants", "is_dead", "is_deleted", "karma", "submitted_count", "pages_seen", "items_seen"):
        assert _postgres_type(column, table="hacker_news_items") == "bigint", column
    assert _postgres_type("score", table="hacker_news_items") in {"bigint", "double precision"}
    assert _postgres_type("raw_json", table="hacker_news_items") == "jsonb"
    assert _postgres_type("kids_json", table="hacker_news_items") == "jsonb"
    assert _postgres_type("raw_json", table="hacker_news_profile") == "jsonb"
    # The primary key stays text like every provider id in the warehouse.
    assert _postgres_type("item_id", table="hacker_news_items") == "text"


def test_every_data_table_has_an_index_leading_with_the_freshness_column() -> None:
    by_table: dict[str, list[str]] = {}
    for spec in POSTGRES_INDEXES:
        by_table.setdefault(spec.table, []).append(spec.sql)
    for table in ("hacker_news_items", "hacker_news_user_items", "hacker_news_profile"):
        column = TABLE_PIPELINES[table].written_at
        assert any(f"({column}" in sql or f"({column})" in sql for sql in by_table.get(table, [])), (
            table,
            column,
        )


def test_the_walk_frontier_is_served_by_an_index() -> None:
    """The frontier query joins kids_json elements against item_id, and the
    refresh pass selects by root story recency; both need their indexes."""
    sql = " ".join(spec.sql for spec in POSTGRES_INDEXES if spec.table == "hacker_news_items")
    assert "root_story_id" in sql
    assert "posted_at" in sql


def test_every_table_is_registered_in_both_registries() -> None:
    for table in EXPECTED_TABLES:
        assert table in TABLE_PIPELINES, table
        assert table in TIMELINE_TABLE_COVERAGE, table
    assert "hacker_news" in {p.id for p in PIPELINES}
    assert TIMELINE_TABLE_COVERAGE["hacker_news_items"].role == "events"
    assert TIMELINE_TABLE_COVERAGE["hacker_news_user_items"].role == "detail"
    assert TIMELINE_TABLE_COVERAGE["hacker_news_profile"].role == "entity"


def test_one_adapter_covers_the_source_and_uses_the_generic_context_walk() -> None:
    adapters = [a for a in TIMELINE_ADAPTERS if a.source == "hacker_news"]
    assert [a.name for a in adapters] == ["hacker_news_item"]
    assert adapters[0].source_table == "hacker_news_items"
    assert "hacker_news_item" in TIMELINE_CONTEXT_GENERIC_ADAPTERS
    tokens = {source: adapters for source, adapters, _ in SEARCH_SOURCE_DEFS}
    assert tokens["hacker_news"] == ("hacker_news_item",)


def test_the_session_table_matches_the_apps_own_definition() -> None:
    store = os.path.join(
        os.path.dirname(__file__), os.pardir, "app", "internal", "hackernewssession", "store.go"
    )
    with open(store, encoding="utf-8") as handle:
        text = handle.read()
    for column in HACKER_NEWS_SESSION_COLUMNS:
        assert f"\n    {column} " in text, column
    assert "PRIMARY KEY (account, session_key)" in text


def test_column_tuples_carry_provenance() -> None:
    for columns in (
        HACKER_NEWS_ITEM_COLUMNS,
        HACKER_NEWS_USER_ITEM_COLUMNS,
        HACKER_NEWS_PROFILE_COLUMNS,
    ):
        assert "account" in columns
        assert "synced_at" in columns
        assert "sync_version" in columns
    assert HACKER_NEWS_SYNC_STATE_COLUMNS[:2] == ("account", "list_name")


# ---------------------------------------------------------------------------
# real Postgres
# ---------------------------------------------------------------------------


def _item(item_id: str, **overrides):
    row = {
        "account": "zachlatta",
        "item_id": item_id,
        "item_type": "comment",
        "author": "someone",
        "posted_at": NOW - timedelta(days=1),
        "title": "",
        "url": "",
        "text": "<p>hello &amp; bye</p>",
        "body_text": "hello & bye",
        "parent_id": "",
        "root_story_id": item_id,
        "score": 0,
        "descendants": 0,
        "is_dead": 0,
        "is_deleted": 0,
        "kids_json": [],
        "raw_json": {"id": int(item_id)},
        "fetched_at": NOW,
        "first_seen_at": NOW,
        "synced_at": NOW,
    }
    row.update(overrides)
    return row


def test_ensure_creates_every_contracted_relation(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_hacker_news_tables()
    for logical, (schema, name, _pk) in EXPECTED_TABLES.items():
        rows = warehouse._query(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (warehouse.physical_schema_name(schema), name),
        )
        assert rows == [(1,)], logical


def test_items_upsert_by_id_and_keep_first_seen(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_hacker_news_tables()
    first_seen = NOW - timedelta(days=3)
    warehouse.insert_hacker_news_items([_item("1", score=1, first_seen_at=first_seen, synced_at=first_seen)])
    warehouse.insert_hacker_news_items([_item("1", score=7, first_seen_at=NOW, synced_at=NOW)])
    rows = warehouse._query_dicts("SELECT score, first_seen_at, fetched_at FROM @hacker_news_items")
    assert len(rows) == 1
    assert rows[0]["score"] == 7
    # The refresh re-stamps fetched_at but must not move first_seen_at forward.
    assert rows[0]["first_seen_at"] == first_seen


def test_the_frontier_is_every_kid_not_yet_fetched(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_hacker_news_tables()
    warehouse.insert_hacker_news_items(
        [
            _item("10", item_type="story", kids_json=["11", "12"]),
            _item("11", parent_id="10", root_story_id="10", kids_json=["13"]),
        ]
    )
    frontier = warehouse.hacker_news_walk_frontier(account="zachlatta", limit=10)
    assert sorted(frontier) == [("12", "10"), ("13", "10")]


def test_items_needing_refresh_are_the_live_threads_read_longest_ago(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_hacker_news_tables()
    stale = NOW - timedelta(hours=12)
    warehouse.insert_hacker_news_items(
        [
            # a live story, last read 12h ago: refresh
            _item("20", item_type="story", posted_at=NOW - timedelta(days=1), fetched_at=stale),
            _item("21", parent_id="20", root_story_id="20", fetched_at=stale),
            # a live story read recently: leave alone
            _item("22", item_type="story", posted_at=NOW - timedelta(days=1), fetched_at=NOW),
            # an old story: settled, never refreshed by this pass
            _item("23", item_type="story", posted_at=NOW - timedelta(days=30), fetched_at=stale),
        ]
    )
    due = warehouse.hacker_news_items_due_for_refresh(
        account="zachlatta",
        now=NOW,
        live_window=timedelta(days=3),
        min_age=timedelta(hours=6),
        limit=10,
    )
    assert sorted(due) == ["20", "21"]


def test_user_items_record_why_and_a_full_walk_can_retire_them(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_hacker_news_tables()
    warehouse.upsert_hacker_news_user_items(
        account="zachlatta", relation="upvoted", item_ids=["1", "2"], now=NOW
    )
    warehouse.upsert_hacker_news_user_items(
        account="zachlatta", relation="upvoted", item_ids=["2"], now=NOW + timedelta(hours=1)
    )
    retired = warehouse.retire_hacker_news_user_items(
        account="zachlatta", relation="upvoted", live_item_ids=["2"], now=NOW + timedelta(hours=2)
    )
    assert retired == 1
    rows = {
        r["item_id"]: r
        for r in warehouse._query_dicts(
            "SELECT item_id, discovered_at, removed_at FROM @hacker_news_user_items"
        )
    }
    assert rows["1"]["removed_at"] == NOW + timedelta(hours=2)
    assert rows["2"]["removed_at"] == EPOCH
    # Re-listing the same item never moves discovered_at.
    assert rows["2"]["discovered_at"] == NOW
    # Re-listing a retired item revives it.
    warehouse.upsert_hacker_news_user_items(
        account="zachlatta", relation="upvoted", item_ids=["1"], now=NOW + timedelta(hours=3)
    )
    (revived,) = warehouse._query_dicts(
        "SELECT removed_at FROM @hacker_news_user_items WHERE item_id = '1'"
    )
    assert revived["removed_at"] == EPOCH


def test_known_item_ids_answers_in_bulk(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_hacker_news_tables()
    warehouse.insert_hacker_news_items([_item("5"), _item("6")])
    assert warehouse.hacker_news_known_item_ids(account="zachlatta", item_ids=["5", "6", "7"]) == {"5", "6"}


def test_session_round_trip_marks_rejection_and_clears_on_success(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_hacker_news_tables()
    warehouse.upsert_hacker_news_session(
        account="zachlatta", session_key="default", session_token="user=abc", source_browser="Chrome"
    )
    row = warehouse.get_hacker_news_session(account="zachlatta", session_key="default")
    sha = hashlib.sha256(b"user=abc").hexdigest()
    assert row["token_sha256"] == sha
    assert row["status"] == "ok"

    warehouse.mark_hacker_news_session_expired(account="zachlatta", session_key="default", token_sha256=sha)
    row = warehouse.get_hacker_news_session(account="zachlatta", session_key="default")
    assert row["status"] == "action_required"
    assert row["expired_token_sha256"] == sha
    assert "pdw hn publish-session" in row["error"]

    # A stale poll's verdict on a token that is no longer installed is ignored.
    warehouse.upsert_hacker_news_session(
        account="zachlatta", session_key="default", session_token="user=def", source_browser="Chrome"
    )
    warehouse.mark_hacker_news_session_expired(account="zachlatta", session_key="default", token_sha256=sha)
    row = warehouse.get_hacker_news_session(account="zachlatta", session_key="default")
    assert row["status"] == "ok"

    new_sha = hashlib.sha256(b"user=def").hexdigest()
    warehouse.mark_hacker_news_session_expired(account="zachlatta", session_key="default", token_sha256=new_sha)
    warehouse.record_hacker_news_session_success(account="zachlatta", session_key="default", token_sha256=new_sha)
    row = warehouse.get_hacker_news_session(account="zachlatta", session_key="default")
    assert row["status"] == "ok"
    assert row["expired_at"] is None


def test_sync_state_round_trips_per_list(warehouse: PostgresWarehouse) -> None:
    warehouse.ensure_hacker_news_tables()
    warehouse.record_hacker_news_sync_state(
        account="zachlatta",
        list_name="upvoted",
        status="ok",
        error="",
        now=NOW,
        success=True,
        full_walk_completed_at=NOW,
        pages_seen=3,
        items_seen=90,
    )
    warehouse.record_hacker_news_sync_state(
        account="zachlatta",
        list_name="hidden",
        status="action_required",
        error="login page",
        now=NOW,
        success=False,
        credential_sha256="abc",
    )
    state = warehouse.load_hacker_news_sync_state(account="zachlatta")
    assert state["upvoted"]["last_success_at"] == NOW
    assert state["upvoted"]["items_seen"] == 90
    assert state["hidden"]["status"] == "action_required"
    assert state["hidden"]["last_success_at"] == EPOCH
    assert state["hidden"]["credential_sha256"] == "abc"
    # A later failure keeps the earlier success time; a later success clears the error.
    warehouse.record_hacker_news_sync_state(
        account="zachlatta", list_name="upvoted", status="error", error="boom", now=NOW + timedelta(hours=1), success=False
    )
    state = warehouse.load_hacker_news_sync_state(account="zachlatta")
    assert state["upvoted"]["last_success_at"] == NOW
    assert state["upvoted"]["full_walk_completed_at"] == NOW
