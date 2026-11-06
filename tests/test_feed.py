"""The Feed tab's storage, marts views, and the Go module against the same schema."""
import json
import os
import re
import subprocess
from pathlib import Path

import pytest

from personal_data_warehouse import feed
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


def _event(
    wh, event_id, event_type, item_id="hn:1", minutes=0, dwell_ms=0, fraction=0.0, metadata="{}",
    session_id="s", client_name="", device="",
):
    wh._command(
        "INSERT INTO @feed_events (event_id, event_type, item_id, session_id, occurred_at, dwell_ms, "
        "visible_fraction, metadata, client_name, device) "
        "VALUES (%s, %s, %s, %s, now() - make_interval(secs => %s), %s, %s, %s::jsonb, %s, %s)",
        (event_id, event_type, item_id, session_id, (60 - minutes) * 60, dwell_ms, fraction, metadata, client_name, device),
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
    # Swiped to email it to someone: a share counts once, for 3.
    _event(wh, "k", "share", item_id="hn:3", minutes=10, metadata='{"via": "superhuman"}')
    _event(wh, "l", "share", item_id="hn:3", minutes=11, metadata='{"via": "superhuman"}')
    rows = wh._query(
        "SELECT item_id, impressions, dwell_ms, max_visible_fraction, scroll_pasts, opens, links_opened, read_ms, "
        "dismissed, dismiss_reason, engaged, engagement_score, shares FROM @marts_feed_item_engagement ORDER BY item_id"
    )
    assert rows == [
        # 2 opens x2 + expand 1 + 5.4 s / 10 + 1.5 min read = 7.04
        ("hn:1", 2, 5400, 1.0, 1, 2, 2, 90000, 0, "", 1, 7.04, 0),
        ("hn:2", 0, 0, 0.0, 0, 0, 0, 0, 1, "less_like_this", 0, -5.0, 0),
        ("hn:3", 1, 6000, 1.0, 0, 0, 0, 0, 0, "", 1, 3.6, 2),
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
def test_sessions_roll_up_one_sitting_and_ui_scroll_load_need_no_card(warehouse):
    wh = warehouse
    _item(wh)
    # The sitting's own events and the feed-level ones (a load, a scroll,
    # a tap on the source rail) carry no card.
    _event(wh, "s0", "session_start", item_id="", metadata='{"trigger": "launch", "app_version": "1.0.0", "update_id": "u1"}')
    _event(wh, "s1", "load", item_id="", minutes=0, dwell_ms=300, metadata='{"trigger": "first", "cards": 2}')
    _event(wh, "s2", "impression", minutes=1, dwell_ms=2000, fraction=1.0)
    _event(wh, "s3", "scroll", item_id="", minutes=2, dwell_ms=900, metadata='{"distance": 1200}')
    _event(wh, "s4", "scroll", item_id="", minutes=3, dwell_ms=400, metadata='{"distance": -300}')
    _event(wh, "s5", "ui", item_id="", minutes=4, metadata='{"action": "filter", "to": "saved"}')
    _event(wh, "s6", "open", minutes=5, metadata='{"mode": "browser"}')
    _event(wh, "s7", "session_end", item_id="", minutes=6, dwell_ms=360000, metadata='{"reason": "background"}')
    rows = wh._query(
        "SELECT trigger, end_reason, duration_ms, app_version, update_id, cards_seen, impressions, card_dwell_ms, "
        "scrolls, scroll_distance, opens, taps, loads, events, started_at < ended_at FROM @marts_feed_sessions"
    )
    assert rows == [("launch", "background", 360000, "1.0.0", "u1", 1, 1, 2000, 2, 1500, 1, 1, 1, 8, True)]
    # Feed-level events do not make a card row of their own.
    assert wh._query("SELECT count(*) FROM @marts_feed_item_engagement") == [(1,)]


@pytestmark_db
def test_an_emulator_testing_the_app_is_not_zach(warehouse):
    wh = warehouse
    _item(wh)
    _item(wh, "hn:2")
    # An agent checks a phone build on the Android emulator: it scrolls the
    # real feed and lingers on cards. None of that is Zach, and a card the
    # emulator saw must stay unseen for him.
    emulator = {"session_id": "emu", "client_name": "android-emulator", "device": "sdk_gphone64_arm64"}
    _event(wh, "e0", "session_start", item_id="", **emulator)
    _event(wh, "e1", "impression", minutes=1, dwell_ms=30000, fraction=1.0, **emulator)
    _event(wh, "e2", "open", minutes=2, metadata='{"mode": "detail"}', **emulator)
    _event(wh, "e3", "session_end", item_id="", minutes=3, dwell_ms=70000, **emulator)
    _event(wh, "e4", "impression", item_id="hn:2", minutes=1, dwell_ms=9000, fraction=1.0,
           session_id="sim", client_name="iphone-simulator", device="iPhone 16 Pro")
    # His phone, and a save an agent made for him when he asked, still count.
    phone = {"session_id": "p", "client_name": "pixel11pro", "device": "Pixel 11 Pro"}
    _event(wh, "p0", "session_start", item_id="", minutes=4, **phone)
    _event(wh, "p1", "impression", item_id="hn:2", minutes=5, dwell_ms=2000, fraction=1.0, **phone)
    _event(wh, "p2", "session_end", item_id="", minutes=6, dwell_ms=4000, **phone)
    _event(wh, "a1", "save", minutes=7, session_id="", client_name="porygon", device="agent", metadata='{"via": "agent"}')
    rows = wh._query(
        "SELECT item_id, impressions, dwell_ms, opens, saved, first_seen_at IS NULL FROM @marts_feed_item_engagement "
        "ORDER BY item_id"
    )
    assert rows == [("hn:1", 0, 0, 0, 1, True), ("hn:2", 1, 2000, 0, 0, False)]
    assert wh._query("SELECT session_id, device FROM @marts_feed_sessions") == [("p", "Pixel 11 Pro")]


@pytestmark_db
def test_the_laptop_page_counts_as_zach_and_a_simulator_client_does_not(warehouse):
    wh = warehouse
    _item(wh)
    _item(wh, "hn:2")
    laptop = {"session_id": "l", "client_name": "crobat-web", "device": "crobat"}
    _event(wh, "l0", "session_start", item_id="", metadata='{"trigger": "launch", "update_id": "web"}', **laptop)
    _event(wh, "l1", "impression", minutes=1, dwell_ms=4000, fraction=1.0, **laptop)
    _event(wh, "l2", "open", minutes=2, metadata='{"mode": "browser", "url": "https://a.test", "link_index": 0}', **laptop)
    _event(wh, "l3", "session_end", item_id="", minutes=3, dwell_ms=5000, metadata='{"reason": "background"}', **laptop)
    # A check of the page under a simulator client marks nothing seen.
    test_client = {"session_id": "t", "client_name": "crobat-web-simulator", "device": "crobat"}
    _event(wh, "t0", "session_start", item_id="", minutes=4, **test_client)
    _event(wh, "t1", "impression", item_id="hn:2", minutes=5, dwell_ms=9000, fraction=1.0, **test_client)
    assert wh._query("SELECT session_id, device, trigger, end_reason, duration_ms, impressions, opens FROM @marts_feed_sessions") == [
        ("l", "crobat", "launch", "background", 5000, 1, 1)
    ]
    assert wh._query("SELECT item_id, impressions, opens FROM @marts_feed_item_engagement ORDER BY item_id") == [
        ("hn:1", 1, 1), ("hn:2", 0, 0)
    ]


@pytestmark_db
def test_saved_is_the_latest_of_save_and_unsave_and_scores_like_a_share(warehouse):
    wh = warehouse
    _item(wh)
    _item(wh, "hn:2")
    _event(wh, "s1", "save", minutes=1)
    _event(wh, "s2", "save", item_id="hn:2", minutes=2)
    _event(wh, "s3", "unsave", item_id="hn:2", minutes=3)
    rows = wh._query(
        "SELECT item_id, saved, saved_at IS NOT NULL, engaged, engagement_score FROM @marts_feed_item_engagement "
        "ORDER BY item_id"
    )
    assert rows == [("hn:1", 1, True, 1, 3.0), ("hn:2", 0, False, 0, 0.0)]
    assert wh._query("SELECT items_saved FROM @marts_feed_source_engagement") == [(1,)]


@pytestmark_db
def test_liked_is_the_latest_of_like_and_unlike_and_scores_most(warehouse):
    wh = warehouse
    _item(wh)
    _item(wh, "hn:2")
    _event(wh, "l1", "like", minutes=1)
    _event(wh, "l2", "like", item_id="hn:2", minutes=2)
    _event(wh, "l3", "unlike", item_id="hn:2", minutes=3)
    rows = wh._query(
        "SELECT item_id, liked, liked_at IS NOT NULL, engaged, engagement_score FROM @marts_feed_item_engagement "
        "ORDER BY item_id"
    )
    assert rows == [("hn:1", 1, True, 1, 5.0), ("hn:2", 0, False, 0, 0.0)]
    assert wh._query("SELECT items_liked FROM @marts_feed_source_engagement") == [(1,)]


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
def test_a_card_carries_media_and_an_embedded_post_and_media_taps_engage(warehouse):
    wh = warehouse
    _item(wh, media='[{"type": "image", "url": "https://a.test/1.jpg"}]',
          embed='{"type": "x_post", "author_handle": "someone", "text": "hi"}')
    _item(wh, "hn:2")
    _event(wh, "m", "media", metadata='{"action": "swipe", "index": 1}')
    assert wh._query(
        "SELECT item_id, jsonb_array_length(media), embed->>'type' FROM @feed_items ORDER BY item_id"
    ) == [("hn:1", 1, "x_post"), ("hn:2", 0, None)]
    assert wh._query("SELECT item_id, expands, engaged FROM @marts_feed_item_engagement ORDER BY item_id") == [
        ("hn:1", 1, 1),
        ("hn:2", 0, 0),
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


FLAG_COLUMNS = ["dismissed", "engaged", "saved", "liked"]


@pytestmark_db
@pytest.mark.parametrize("column", FLAG_COLUMNS)
def test_item_engagement_flags_are_bigint_0_1_like_every_warehouse_boolean(warehouse, column):
    wh = warehouse
    _item(wh)
    assert wh._query(f"SELECT pg_typeof({column})::text, {column} FROM @marts_feed_item_engagement") == [
        ("bigint", 0)
    ]


@pytestmark_db
def test_ensure_retypes_integer_flags_of_an_existing_view_to_bigint(warehouse):
    wh = warehouse
    # Before 2026-10-25 the flags were CASE ... THEN 1 ELSE 0, an integer, which
    # CREATE OR REPLACE cannot retype; ensure must recreate both views.
    wh._command("DROP VIEW @marts_feed_source_engagement")
    wh._command("DROP VIEW @marts_feed_item_engagement")
    wh._command(feed.ITEM_ENGAGEMENT_VIEW.replace("END::bigint", "END"))
    wh._command(feed.SOURCE_ENGAGEMENT_VIEW)
    _item(wh)
    assert wh._query("SELECT pg_typeof(engaged)::text FROM @marts_feed_item_engagement") == [("integer",)]
    wh.ensure_timeline_tables()
    assert wh._query(
        "SELECT " + ", ".join(f"pg_typeof({c})::text" for c in FLAG_COLUMNS) + " FROM @marts_feed_item_engagement"
    ) == [("bigint",) * len(FLAG_COLUMNS)]


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


@pytestmark_db
def test_sittings_join_detail_gaps_and_recover_only_same_device_events(warehouse):
    wh = warehouse
    _item(wh)
    for sid, start, end, meta in [('a', 0, 1, '{}'), ('b', 2, 3, '{}'), ('c', 9, 10, '{"gap_ms": 1000}')]:
        _event(wh, sid+'0', 'session_start', minutes=start, session_id=sid, device='phone', metadata=meta)
        _event(wh, sid+'1', 'impression', minutes=start, session_id=sid, device='phone')
        _event(wh, sid+'2', 'session_end', minutes=end, session_id=sid, device='phone', dwell_ms=1000)
    for i, kind in enumerate(['open', 'close', 'web', 'save', 'like', 'share']):
        _event(wh, 'detail'+str(i), kind, minutes=1.5, session_id='', device='phone')
    _event(wh, 'other', 'open', minutes=1.5, session_id='', device='other')
    _event(wh, 'late', 'open', minutes=11, session_id='', device='phone')
    assert wh._query("SELECT session_id, sitting_id, duration_ms, cards_seen, opens, saves, likes, shares "
                     "FROM @marts_feed_sessions") == [('a', 'a', 3000, 1, 1, 1, 1, 1)]


@pytestmark_db
@pytest.mark.parametrize('metadata', ['{}', '{"gap_ms": -1}', '{"gap_ms": "oops"}', '{"gap_ms": 120000}'])
def test_sittings_do_not_merge_two_minute_gaps_or_bad_metadata(warehouse, metadata):
    wh = warehouse
    _event(wh, 'a0', 'session_start', minutes=0, session_id='a')
    _event(wh, 'a1', 'session_end', minutes=1, session_id='a', dwell_ms=1000)
    _event(wh, 'b0', 'session_start', minutes=3.1, session_id='b', metadata=metadata)
    _event(wh, 'b1', 'session_end', minutes=4, session_id='b', dwell_ms=2000)
    assert wh._query('SELECT count(*) FROM @marts_feed_sessions') == [(2,)]


@pytestmark_db
def test_unfinished_sitting_is_finite_and_uses_last_session_end(warehouse):
    wh = warehouse
    _event(wh, 'a0', 'session_start', minutes=0, session_id='a')
    _event(wh, 'a1', 'session_end', minutes=1, session_id='a', dwell_ms=1000)
    _event(wh, 'b0', 'session_start', minutes=2, session_id='b')
    _event(wh, 'b1', 'open', minutes=3, session_id='b')
    _event(wh, 'stray', 'open', minutes=20, session_id='')
    assert wh._query('SELECT session_id, ended_at, duration_ms, opens FROM @marts_feed_sessions') == [('a', None, None, 1)]


@pytestmark_db
def test_sitting_two_minute_boundary_and_first_last_metadata(warehouse):
    wh = warehouse
    rows = [
        ('a0', 'session_start', 'a', 0, 0, '{"trigger":"launch","app_version":"first","update_id":"u1"}'),
        ('a1', 'session_end', 'a', 60, 1000, '{"reason":"blur"}'),
        ('b0', 'session_start', 'b', 180, 0, '{"gap_ms":120000,"trigger":"focus"}'),
        ('b1', 'session_end', 'b', 240, 2000, '{"reason":"background"}'),
        ('c0', 'session_start', 'c', 240, 0, '{"gap_ms":"bad"}'),
        ('c1', 'session_end', 'c', 300, 3000, '{"reason":"background"}'),
        ('open', 'open', '', 240, 0, '{}'),
    ]
    for eid, kind, sid, seconds, dwell, meta in rows:
        _event(wh, eid, kind, session_id=sid, metadata=meta, dwell_ms=dwell)
        wh._command("UPDATE @feed_events SET occurred_at = '2026-01-01'::timestamptz + make_interval(secs => %s) "
                    "WHERE event_id = %s", (seconds, eid))
    assert wh._query("SELECT session_id, duration_ms, trigger, end_reason, opens "
                     "FROM @marts_feed_sessions ORDER BY session_id") == [
        ('a', 1000, 'launch', 'blur', 0), ('b', 5000, 'focus', 'background', 1),
    ]
    assert wh._query("SELECT app_version, update_id FROM @marts_feed_sessions WHERE session_id='a'") == [('first', 'u1')]


@pytestmark_db
def test_empty_id_boundary_event_belongs_to_latest_sitting_once(warehouse):
    wh = warehouse
    for eid, kind, sid, seconds in [('a0', 'session_start', 'a', 0),
                                   ('a1', 'impression', 'a', 60),
                                   ('b0', 'session_start', 'b', 60),
                                   ('b1', 'session_end', 'b', 120),
                                   ('open', 'open', '', 60)]:
        _event(wh, eid, kind, session_id=sid)
        wh._command("UPDATE @feed_events SET occurred_at = '2026-01-01'::timestamptz + make_interval(secs => %s) "
                    "WHERE event_id = %s", (seconds, eid))
    assert wh._query('SELECT session_id, opens FROM @marts_feed_sessions ORDER BY session_id') == [('a', 0), ('b', 1)]


@pytestmark_db
def test_nearby_sessions_on_different_devices_stay_separate(warehouse):
    wh = warehouse
    for sid, device, start in [('a', 'phone-a', 0), ('b', 'phone-b', 1)]:
        _event(wh, sid+'0', 'session_start', minutes=start, session_id=sid, device=device)
        _event(wh, sid+'1', 'session_end', minutes=start+1, session_id=sid, device=device)
    assert wh._query('SELECT session_id, device FROM @marts_feed_sessions ORDER BY session_id') == [
        ('a', 'phone-a'), ('b', 'phone-b'),
    ]


@pytestmark_db
def test_blur_gap_over_two_minutes_attributes_reader_events_without_merging(warehouse):
    wh = warehouse
    for eid, kind, sid, minute, meta in [
        ('a0', 'session_start', 'a', 0, '{}'),
        ('a1', 'session_end', 'a', 1, '{"reason":"blur"}'),
        ('open', 'open', '', 2, '{}'),
        ('close', 'close', '', 3, '{}'),
        ('b0', 'session_start', 'b', 3.7, '{}'),
        ('b1', 'session_end', 'b', 4, '{"reason":"background"}'),
        ('late', 'open', '', 5, '{}'),
    ]:
        _event(wh, eid, kind, session_id=sid, minutes=minute, metadata=meta)
    assert wh._query('SELECT session_id, opens FROM @marts_feed_sessions ORDER BY session_id') == [('a', 1), ('b', 0)]


@pytestmark_db
@pytest.mark.parametrize("manual", [False, True])
@pytest.mark.parametrize("flag,expected", [(None, None), (True, False), (False, True)])
def test_inline_watch_is_separate_from_reading(warehouse, manual, flag, expected):
    wh = warehouse
    _item(wh)
    _event(wh, "auto", "media", metadata='{"autoplay": true}')
    _event(wh, "seen", "impression", dwell_ms=265)
    if manual:
        _event(wh, "manual", "media", metadata='{"autoplay": false}')
    metadata = {"mode": "video", "inline": True}
    if flag is not None:
        metadata["autoplay"] = flag
    _event(wh, "close", "close", dwell_ms=270, metadata=json.dumps(metadata))
    read = manual if expected is None else expected
    assert wh._query(
        "SELECT expands, autoplays, read_ms, watch_ms, engaged FROM @marts_feed_item_engagement"
    ) == [(int(manual), 1, 270 if read else 0, 270, int(manual))]
    assert wh._query("SELECT expands FROM @marts_feed_sessions") == [(int(manual),)]
    assert wh._query("SELECT read_ms FROM @marts_feed_source_engagement") == [(270 if read else 0,)]

@pytestmark_db
def test_autoplay_columns_append_without_recreating_dependents(warehouse):
    wh = warehouse
    wh._command("DROP VIEW @marts_feed_source_engagement")
    wh._command("DROP VIEW @marts_feed_item_engagement")
    legacy = feed.ITEM_ENGAGEMENT_VIEW.replace(
        ",\n           COALESCE(e.autoplays, 0) AS autoplays,\n           COALESCE(e.watch_ms, 0) AS watch_ms", ""
    )
    wh._command(legacy)
    wh._command(feed.SOURCE_ENGAGEMENT_VIEW)
    before = wh._query("SELECT %s::regclass::oid", (wh._expand_relations("@marts_feed_source_engagement"),))
    wh.ensure_timeline_tables()
    assert wh._query("SELECT %s::regclass::oid", (wh._expand_relations("@marts_feed_source_engagement"),)) == before
    _item(wh)
    _event(wh, "reading", "close", dwell_ms=800)
    assert wh._query("SELECT read_ms, watch_ms, autoplays FROM @marts_feed_item_engagement") == [(800, 0, 0)]
