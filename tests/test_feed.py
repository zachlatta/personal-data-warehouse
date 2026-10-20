"""The Feed tab's storage, marts views, and the Go module against the same schema."""
import json
import os
import re
import subprocess
from pathlib import Path

import pytest

from personal_data_warehouse.feed import EVENT_TYPES, TABLES, TIMELINE_EVENT_TYPES, VIEWS
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
    _event(wh, "a", "impression", minutes=1, dwell_ms=400, fraction=0.6)
    _event(wh, "b", "impression", minutes=2, dwell_ms=5000, fraction=1.0)
    _event(wh, "c", "open", minutes=3, metadata='{"mode": "reader"}')
    _event(wh, "d", "close", minutes=4, dwell_ms=90000)
    _event(wh, "e", "like", minutes=5)
    _event(wh, "f", "unlike", minutes=6)
    _event(wh, "g", "like", minutes=7)
    _event(wh, "h", "dismiss", item_id="hn:2", minutes=8, metadata='{"reason": "less_like_this"}')
    rows = wh._query(
        "SELECT item_id, impressions, dwell_ms, max_visible_fraction, scroll_pasts, opens, read_ms, "
        "liked, dismissed, dismiss_reason, engaged FROM @marts_feed_item_engagement ORDER BY item_id"
    )
    assert rows == [
        ("hn:1", 2, 5400, 1.0, 1, 1, 90000, 1, 0, "", 1),
        ("hn:2", 0, 0, 0.0, 0, 0, 0, 0, 1, "less_like_this", 0),
    ]
    _event(wh, "i", "undismiss", item_id="hn:2", minutes=9)
    assert wh._query("SELECT dismissed FROM @marts_feed_item_engagement WHERE item_id = 'hn:2'") == [(0,)]
    [(items, seen, liked, open_rate)] = wh._query(
        "SELECT items, items_seen, items_liked, open_rate FROM @marts_feed_source_engagement WHERE source = 'hn'"
    )
    assert (items, seen, liked, float(open_rate)) == (2, 1, 1, 1.0)


@pytestmark_db
@pytest.mark.parametrize("column", ["published_at", "created_at", "updated_at", "expires_at", "removed_at"])
def test_item_engagement_reports_an_absent_timestamp_as_null_not_the_epoch(warehouse, column):
    wh = warehouse
    _item(wh, **{column: "1970-01-01 00:00:00+00"})
    assert wh._query(f"SELECT {column} FROM @marts_feed_item_engagement") == [(None,)]


@pytestmark_db
def test_go_feed_module_against_the_same_provisioned_schema(warehouse):
    wh = warehouse
    names = (*TABLES, *VIEWS)
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
