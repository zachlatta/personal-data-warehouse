"""The Feed tab's storage, marts views, and the Go module against the same schema."""
import json
import os
import re
import subprocess
from pathlib import Path

import pytest

from personal_data_warehouse.feed import EVENT_TYPES, FUNCTIONS, TABLES, TIMELINE_EVENT_TYPES, VIEWS
from personal_data_warehouse.postgres import PostgresWarehouse
from tests.conftest import cleanup_test_warehouse, make_test_schema

REPO = Path(__file__).resolve().parents[1]


def test_the_go_validator_accepts_exactly_the_python_event_types():
    source = (REPO / "app/internal/feed/feed.go").read_text()
    block = source[source.index("var EventTypes = []string{"):]
    block = block[: block.index("}")]
    assert tuple(re.findall(r'"([a-z_]+)"', block)) == EVENT_TYPES
    assert set(TIMELINE_EVENT_TYPES) <= set(EVENT_TYPES)


def test_the_removal_doc_names_every_registration_point():
    doc = (REPO / "docs/feed.md").read_text()
    for path in (
        "app/internal/feed/", "app/cmd/pdw-cli/feed.go", "mobile/src/features/feed/",
        "src/personal_data_warehouse/feed.py", "tests/test_feed.py",
        "app/internal/guide/topics/feed.md",
    ):
        assert path in doc, path
        assert (REPO / path).exists(), path


@pytest.fixture
def warehouse():
    wh = PostgresWarehouse(os.environ["POSTGRES_DATABASE_URL"], schema=make_test_schema())
    try:
        wh.ensure_timeline_tables()
        yield wh
    finally:
        cleanup_test_warehouse(wh)


pytestmark_db = pytest.mark.local_integration


def _item(wh, item_id="hn:1", source="hn", **extra):
    columns = {"item_id": item_id, "source": source, "item_key": item_id.split(":", 1)[1], "title": "T", **extra}
    names = ", ".join(columns)
    marks = ", ".join(["%s"] * len(columns))
    wh._command(f"INSERT INTO @feed_items ({names}) VALUES ({marks})", tuple(columns.values()))


def _event(wh, event_id, event_type, item_id="hn:1", minutes=0, dwell_ms=0, fraction=0.0, metadata="{}"):
    wh._command(
        "INSERT INTO @feed_events (event_id, event_type, item_id, session_id, occurred_at, dwell_ms, "
        "visible_fraction, metadata) VALUES (%s, %s, %s, 's', now() - make_interval(mins => %s), %s, %s, %s::jsonb)",
        (event_id, event_type, item_id, 60 - minutes, dwell_ms, fraction, metadata),
    )


@pytestmark_db
def test_item_engagement_rolls_up_the_phone_events(warehouse):
    wh = warehouse
    _item(wh)
    _item(wh, "hn:2")
    _item(wh, "hn:3")
    _event(wh, "a", "impression", minutes=1, dwell_ms=400, fraction=0.6)
    _event(wh, "b", "impression", minutes=2, dwell_ms=5000, fraction=1.0)
    _event(wh, "c", "open", minutes=3, metadata='{"mode": "reader", "url": "https://a.test/1"}')
    _event(wh, "d", "close", minutes=4, dwell_ms=90000)
    _event(wh, "e", "open", minutes=5, metadata='{"mode": "browser", "url": "https://b.test/2", "link_index": 1}')
    _event(wh, "f", "expand", minutes=6)
    _event(wh, "h", "dismiss", item_id="hn:2", minutes=8, metadata='{"reason": "less_like_this"}')
    # Never tapped, but he sat on the card for six seconds: that is engagement.
    _event(wh, "j", "impression", item_id="hn:3", minutes=9, dwell_ms=6000, fraction=1.0)
    rows = wh._query(
        "SELECT item_id, impressions, dwell_ms, max_visible_fraction, scroll_pasts, opens, links_opened, read_ms, "
        "dismissed, dismiss_reason, engaged, engagement_score FROM @marts_feed_item_engagement ORDER BY item_id"
    )
    assert rows == [
        # 2 opens x2 + expand 1 + 5.4 s / 10 + 1.5 min read = 7.04
        ("hn:1", 2, 5400, 1.0, 1, 2, 2, 90000, 0, "", 1, 7.04),
        ("hn:2", 0, 0, 0.0, 0, 0, 0, 0, 1, "less_like_this", 0, -5.0),
        ("hn:3", 1, 6000, 1.0, 0, 0, 0, 0, 0, "", 1, 0.6),
    ]
    _event(wh, "i", "undismiss", item_id="hn:2", minutes=9)
    assert wh._query("SELECT dismissed, engagement_score FROM @marts_feed_item_engagement WHERE item_id = 'hn:2'") == [
        (0, 0.0)
    ]
    [(items, seen, engaged, open_rate, engaged_rate)] = wh._query(
        "SELECT items, items_seen, items_engaged, open_rate, engaged_rate FROM @marts_feed_source_engagement "
        "WHERE source = 'hn'"
    )
    assert (items, seen, engaged, float(open_rate), float(engaged_rate)) == (3, 2, 2, 0.5, 1.0)


@pytestmark_db
def test_a_card_carries_more_links_and_the_view_counts_them(warehouse):
    wh = warehouse
    _item(wh, links='[{"url": "https://a.test/1", "title": "Paper"}, {"url": "https://b.test/2", "title": "HN"}]')
    _item(wh, "hn:2")
    assert wh._query("SELECT item_id, link_count FROM @marts_feed_item_engagement ORDER BY item_id") == [
        ("hn:1", 2),
        ("hn:2", 0),
    ]


@pytestmark_db
def test_ensure_upgrades_the_views_that_still_carry_likes(warehouse):
    wh = warehouse
    # The first shape of the views had a liked column, which CREATE OR REPLACE
    # cannot drop; ensure must recreate both views rather than wedge.
    wh._command("DROP VIEW @marts_feed_source_engagement")
    wh._command("DROP VIEW @marts_feed_item_engagement")
    wh._command("CREATE VIEW @marts_feed_item_engagement AS SELECT item_id, 0 AS liked FROM @feed_items")
    wh._command("CREATE VIEW @marts_feed_source_engagement AS SELECT count(liked) AS items_liked "
                "FROM @marts_feed_item_engagement")
    wh.ensure_timeline_tables()
    _item(wh)
    assert wh._query("SELECT engaged, engagement_score FROM @marts_feed_item_engagement") == [(0, 0.0)]


@pytestmark_db
@pytest.mark.parametrize("column", ["published_at", "created_at", "updated_at", "expires_at", "removed_at"])
def test_item_engagement_reports_an_absent_timestamp_as_null_not_the_epoch(warehouse, column):
    wh = warehouse
    _item(wh, **{column: "1970-01-01 00:00:00+00"})
    assert wh._query(f"SELECT {column} FROM @marts_feed_item_engagement") == [(None,)]


@pytestmark_db
def test_go_feed_module_against_the_same_provisioned_schema(warehouse):
    wh = warehouse
    names = (*TABLES, *VIEWS, *FUNCTIONS)
    env = dict(
        os.environ,
        PDW_FEED_TEST_URL=os.environ["POSTGRES_DATABASE_URL"],
        PDW_FEED_TEST_RELATIONS=json.dumps({name: wh.sql_relation(name) for name in names}),
    )
    result = subprocess.run(
        ["go", "test", "./internal/feed", "-run", "TestPostgres", "-count=1", "-v"],
        cwd=REPO / "app", env=env, text=True, capture_output=True, timeout=300,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--- SKIP" not in result.stdout, result.stdout


def _versions(wh, item_id="hn:1"):
    return wh._query(
        "SELECT version, action, actor, agent, reason, restored_from, changed_fields, diff, snapshot "
        "FROM @feed_item_versions WHERE item_id = %s ORDER BY version", (item_id,))


@pytestmark_db
def test_every_change_to_a_card_writes_an_immutable_version(warehouse):
    wh = warehouse
    _item(wh, title="First")
    wh._command("UPDATE @feed_items SET title = 'Second', updated_at = now() WHERE item_id = 'hn:1'")
    wh._command("UPDATE @feed_items SET updated_at = now() + interval '1 minute' WHERE item_id = 'hn:1'")
    wh._command("UPDATE @feed_items SET score = 3 WHERE item_id = 'hn:1'")
    wh._command("UPDATE @feed_items SET removed_at = now(), removed_reason = 'stale' WHERE item_id = 'hn:1'")
    wh._command("UPDATE @feed_items SET removed_at = 'epoch', removed_reason = '' WHERE item_id = 'hn:1'")
    wh._command("DELETE FROM @feed_items WHERE item_id = 'hn:1'")
    versions = _versions(wh)
    # The updated_at-only write changed nothing a reader can see: no version.
    assert [(v, a) for v, a, *_ in versions] == [
        (1, "create"), (2, "update"), (3, "rank"), (4, "remove"), (5, "restore"), (6, "delete")]
    _, _, _, _, _, _, fields, diff, snapshot = versions[1]
    assert fields == ["title"]
    assert diff == {"title": {"from": "First", "to": "Second"}}
    assert snapshot["title"] == "Second"
    # The tombstone keeps the card as it was, so it can be restored.
    assert versions[-1][8]["title"] == "Second" and versions[-1][8]["score"] == 3
    with pytest.raises(Exception, match="append-only"):
        wh._command("UPDATE @feed_item_versions SET action = 'x'")
    with pytest.raises(Exception, match="append-only"):
        wh._command("DELETE FROM @feed_item_versions")
    with pytest.raises(Exception, match="append-only"):
        wh._command("TRUNCATE @feed_item_versions")
    assert len(_versions(wh)) == 6


@pytestmark_db
def test_a_version_records_who_which_agent_and_why_from_the_transaction(warehouse):
    wh = warehouse
    _item(wh)
    wh._command(
        "DO $$ BEGIN "
        "PERFORM set_config('pdw.feed_actor', 'porygon', true); "
        "PERFORM set_config('pdw.feed_agent', 'muse-picks', true); "
        "PERFORM set_config('pdw.feed_reason', 'fix typo', true); "
        "PERFORM set_config('pdw.feed_action', 'restore', true); "
        "PERFORM set_config('pdw.feed_restored_from', '1', true); "
        "UPDATE @feed_items SET title = 'Fixed' WHERE item_id = 'hn:1'; END $$")
    _, action, actor, agent, reason, restored_from, *_ = _versions(wh)[-1]
    assert (action, actor, agent, reason, restored_from) == ("restore", "porygon", "muse-picks", "fix typo", 1)
    # Settings are transaction-local: the next write is classified afresh.
    wh._command("UPDATE @feed_items SET body = 'b' WHERE item_id = 'hn:1'")
    _, action, _, agent, reason, restored_from, *_ = _versions(wh)[-1]
    assert (action, agent, reason, restored_from) == ("update", "", "", 0)


@pytestmark_db
def test_cards_from_before_versioning_get_a_baseline_version(warehouse):
    wh = warehouse
    wh._command("ALTER TABLE @feed_items DISABLE TRIGGER feed_items_versioned")
    _item(wh, title="Old")
    wh._command("ALTER TABLE @feed_items ENABLE TRIGGER feed_items_versioned")
    assert _versions(wh) == []
    from personal_data_warehouse.feed import ensure_feed_tables
    ensure_feed_tables(wh)
    ensure_feed_tables(wh)
    [(version, action, actor, *_rest)] = _versions(wh)
    assert (version, action, actor) == (1, "baseline", "versioning")
    assert _rest[-1]["title"] == "Old"
