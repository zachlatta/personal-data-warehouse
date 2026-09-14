"""The sync runner against a fake warehouse and a fake client (no database)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from personal_data_warehouse.hacker_news_api import (
    HackerNewsAuthError,
    HackerNewsRateLimitError,
    ListPage,
    item_record,
)
from personal_data_warehouse.hacker_news_sync import (
    HackerNewsSyncRunner,
    hacker_news_reauthorization_skip_reason,
    public_summary,
)

NOW = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _item(item_id, item_type="comment", parent=None, kids=(), by="someone", **extra):
    payload = {"id": int(item_id), "type": item_type, "by": by, "time": 1_757_000_000, "kids": [int(k) for k in kids]}
    if parent is not None:
        payload["parent"] = int(parent)
    payload.update(extra)
    return payload


class FakeWarehouse:
    """Just enough of PostgresWarehouse for the runner: rows kept in dicts."""

    def __init__(self):
        self.items: dict[str, dict] = {}
        self.user_items: dict[tuple[str, str], dict] = {}
        self.profile = None
        self.state: dict[str, dict] = {}
        self.expired: list[str] = []
        self.succeeded: list[str] = []
        self.ensured = 0

    def ensure_hacker_news_tables(self):
        self.ensured += 1

    def load_hacker_news_sync_state(self, *, account):
        return dict(self.state)

    def insert_hacker_news_profile(self, row):
        self.profile = dict(row)

    def insert_hacker_news_items(self, rows):
        for row in rows:
            existing = self.items.get(row["item_id"])
            row = dict(row)
            if existing:
                row["first_seen_at"] = min(existing["first_seen_at"], row["first_seen_at"])
            self.items[row["item_id"]] = row

    def hacker_news_known_item_ids(self, *, account, item_ids):
        return {i for i in item_ids if i in self.items}

    def hacker_news_item_roots(self, *, account, item_ids):
        return {i: self.items[i]["root_story_id"] for i in item_ids if i in self.items}

    def hacker_news_user_item_ids(self, *, account, relation, live_only=True):
        return {
            item_id
            for (item_id, rel), row in self.user_items.items()
            if rel == relation and (not live_only or row["removed_at"] == EPOCH)
        }

    def upsert_hacker_news_user_items(self, *, account, relation, item_ids, now):
        for item_id in item_ids:
            row = self.user_items.setdefault((item_id, relation), {"discovered_at": now, "removed_at": EPOCH})
            row["removed_at"] = EPOCH

    def retire_hacker_news_user_items(self, *, account, relation, live_item_ids, now):
        live = set(live_item_ids)
        count = 0
        for (item_id, rel), row in self.user_items.items():
            if rel == relation and item_id not in live and row["removed_at"] == EPOCH:
                row["removed_at"] = now
                count += 1
        return count

    def hacker_news_walk_frontier(self, *, account, limit):
        out = []
        for row in sorted(self.items.values(), key=lambda r: r["posted_at"], reverse=True):
            for kid in row["kids_json"]:
                if kid not in self.items and all(kid != o[0] for o in out):
                    out.append((kid, row["root_story_id"]))
        return out[:limit]

    def hacker_news_items_due_for_refresh(self, *, account, now, live_window, min_age, limit):
        due = []
        for row in self.items.values():
            root = self.items.get(row["root_story_id"])
            if root and root["posted_at"] >= now - live_window and row["fetched_at"] <= now - min_age:
                due.append(row["item_id"])
        return due[:limit]

    def mark_hacker_news_session_expired(self, *, account, session_key, token_sha256):
        self.expired.append(token_sha256)

    def record_hacker_news_session_success(self, *, account, session_key, token_sha256):
        self.succeeded.append(token_sha256)

    def record_hacker_news_sync_state(self, *, account, list_name, status, error, now, success, **extra):
        row = self.state.setdefault(list_name, {"full_walk_completed_at": EPOCH, "last_success_at": EPOCH})
        row.update({"status": status, "error": error, "updated_at": now})
        if success:
            row["last_success_at"] = now
        if extra.get("full_walk_completed_at"):
            row["full_walk_completed_at"] = extra["full_walk_completed_at"]
        for key in ("pages_seen", "items_seen"):
            if extra.get(key) is not None:
                row[key] = extra[key]
        row["credential_sha256"] = extra.get("credential_sha256", "")


class FakeClient:
    def __init__(self, *, user, items, lists, auth_fail=(), throttle_after=None):
        self.user = user
        self.items = {str(k): v for k, v in items.items()}
        self.lists = lists  # (list_name, comments) -> [page item id lists]
        self.auth_fail = set(auth_fail)
        self.fetched: list[str] = []
        self.throttle_after = throttle_after

    def get_user(self, user_id):
        return self.user

    def get_item(self, item_id):
        if self.throttle_after is not None and len(self.fetched) >= self.throttle_after:
            raise HackerNewsRateLimitError("429")
        self.fetched.append(item_id)
        return item_record(self.items.get(str(item_id)))

    def iter_list_pages(self, list_name, *, user_id, comments=False, max_pages):
        if list_name in self.auth_fail:
            raise HackerNewsAuthError(f"/{list_name} login page")
        pages = self.lists.get((list_name, comments), [[]])
        for index, ids in enumerate(pages[:max_pages]):
            last = index == len(pages) - 1
            yield ListPage(
                list_name=list_name,
                item_ids=tuple(ids),
                next_cursor=None if last else {"id": user_id, "next": ids[-1] if ids else "0"},
                is_comments=comments,
            )


def _runner(wh, client, **kw):
    return HackerNewsSyncRunner(
        warehouse=wh, client=client, account="zachlatta", session_token_sha256=kw.pop("sha", "abc"), now=lambda: NOW, **kw
    )


STORY = _item(100, "story", kids=[101, 103], by="op", title="Show HN", url="https://e.test", score=42, descendants=3)
MY_COMMENT = _item(101, parent=100, kids=[102], by="zachlatta", text="<p>nice")
REPLY = _item(102, parent=101, by="replier", text="thanks")
OTHER = _item(103, parent=100, by="stranger", text="meh")
FAV_COMMENT = _item(202, parent=201, by="wise", text="deep")
FAV_PARENT = _item(201, parent=200, by="mid")
FAV_STORY = _item(200, "story", kids=[201], by="op2", title="Fav story")


def test_a_full_run_archives_every_touched_story_and_its_whole_thread():
    wh = FakeWarehouse()
    client = FakeClient(
        user={"id": "zachlatta", "karma": 4049, "created": 1363601181, "submitted": [101]},
        items={i["id"]: i for i in (STORY, MY_COMMENT, REPLY, OTHER, FAV_COMMENT, FAV_PARENT, FAV_STORY)},
        lists={("favorites", False): [[]], ("favorites", True): [["202"]], ("upvoted", False): [["100"]], ("upvoted", True): [[]], ("hidden", False): [[]]},
    )
    summary = _runner(wh, client).sync()

    # Every item of both discussions is archived, each with its real root.
    assert set(wh.items) == {"100", "101", "102", "103", "200", "201", "202"}
    assert {i: r["root_story_id"] for i, r in wh.items.items()} == {
        "100": "100", "101": "100", "102": "100", "103": "100", "200": "200", "201": "200", "202": "200",
    }
    # Why-rows: his comment is submitted, the story upvoted, the deep comment favorited.
    assert wh.user_items[("101", "submitted")]["removed_at"] == EPOCH
    assert wh.user_items[("100", "upvoted")]["removed_at"] == EPOCH
    assert wh.user_items[("202", "favorited")]["removed_at"] == EPOCH
    assert wh.profile["karma"] == 4049 and wh.profile["submitted_count"] == 1
    assert "submitted" not in wh.profile["raw_json"]
    # Each item was fetched exactly once.
    assert sorted(client.fetched) == sorted(set(client.fetched))
    assert summary.items_fetched == 7
    assert summary.frontier_remaining == 0
    assert summary.session_rejected is False
    assert wh.state["upvoted"]["status"] == "ok"
    assert wh.state["items"]["status"] == "ok"
    # The cookie worked on a private list, so its health is cleared.
    assert wh.succeeded == ["abc", "abc"]  # upvoted and hidden
    assert public_summary(summary)["items_fetched"] == 7


def test_a_login_page_marks_the_session_and_keeps_the_public_work_going():
    wh = FakeWarehouse()
    client = FakeClient(
        user={"id": "zachlatta", "karma": 1, "submitted": [101]},
        items={i["id"]: i for i in (STORY, MY_COMMENT, REPLY, OTHER)},
        lists={("favorites", False): [[]], ("favorites", True): [[]]},
        auth_fail={"upvoted", "hidden"},
    )
    summary = _runner(wh, client).sync()
    assert summary.session_rejected is True
    assert wh.expired == ["abc", "abc"]
    assert wh.state["upvoted"]["status"] == "action_required"
    assert wh.state["upvoted"]["credential_sha256"] == "abc"
    assert "pdw hn publish-session" in wh.state["upvoted"]["error"]
    assert wh.state["favorites"]["status"] == "ok"
    # The public half still archived his comment's whole thread.
    assert set(wh.items) == {"100", "101", "102", "103"}
    reason = hacker_news_reauthorization_skip_reason(wh.state, credential="cookie-whose-sha-is-abc")
    assert reason is None  # different fingerprint
    for name in ("upvoted", "hidden"):
        wh.state[name]["credential_sha256"] = __import__("hashlib").sha256(b"dead").hexdigest()
    assert "publish-session" in hacker_news_reauthorization_skip_reason(wh.state, credential="dead")


def test_the_budget_bounds_a_run_and_the_next_run_resumes_from_the_table():
    wh = FakeWarehouse()
    client = FakeClient(
        user={"id": "zachlatta", "submitted": []},
        items={i["id"]: i for i in (STORY, MY_COMMENT, REPLY, OTHER)},
        lists={("upvoted", False): [["100"]], ("upvoted", True): [[]], ("favorites", False): [[]], ("favorites", True): [[]], ("hidden", False): [[]]},
    )
    first = _runner(wh, client, max_item_fetches=2).sync()
    assert first.budget_exhausted is True
    assert first.frontier_remaining >= 1
    assert len(wh.items) == 2
    second = _runner(wh, client, max_item_fetches=10).sync()
    assert set(wh.items) == {"100", "101", "102", "103"}
    assert second.frontier_remaining == 0


def test_an_incremental_list_walk_stops_at_the_first_page_of_known_items():
    wh = FakeWarehouse()
    wh.state["upvoted"] = {"full_walk_completed_at": NOW - timedelta(hours=1), "last_success_at": NOW}
    wh.user_items[("100", "upvoted")] = {"discovered_at": NOW, "removed_at": EPOCH}
    wh.items["100"] = {"item_id": "100", "root_story_id": "100", "kids_json": [], "posted_at": NOW, "fetched_at": NOW}
    pages_served = []

    class CountingClient(FakeClient):
        def iter_list_pages(self, list_name, *, user_id, comments=False, max_pages):
            for page in super().iter_list_pages(list_name, user_id=user_id, comments=comments, max_pages=max_pages):
                pages_served.append((list_name, comments, page.item_ids))
                yield page

    client = CountingClient(
        user={"id": "zachlatta", "submitted": []},
        items={},
        lists={("upvoted", False): [["100"], ["99"]], ("upvoted", True): [[]], ("favorites", False): [[]], ("favorites", True): [[]], ("hidden", False): [[]]},
    )
    _runner(wh, client).sync()
    served = [p for p in pages_served if p[0] == "upvoted" and p[1] is False]
    assert served == [("upvoted", False, ("100",))]  # page 2 was never requested
    assert ("99", "upvoted") not in wh.user_items


def test_a_full_walk_retires_what_the_list_no_longer_names():
    wh = FakeWarehouse()
    wh.user_items[("7", "favorited")] = {"discovered_at": NOW - timedelta(days=30), "removed_at": EPOCH}
    client = FakeClient(
        user={"id": "zachlatta", "submitted": []},
        items={FAV_STORY["id"]: FAV_STORY, FAV_PARENT["id"]: FAV_PARENT},
        lists={("favorites", False): [["200"]], ("favorites", True): [[]], ("upvoted", False): [[]], ("upvoted", True): [[]], ("hidden", False): [[]]},
    )
    _runner(wh, client).sync()
    assert wh.user_items[("7", "favorited")]["removed_at"] == NOW
    assert wh.user_items[("200", "favorited")]["removed_at"] == EPOCH
    assert wh.state["favorites"]["full_walk_completed_at"] == NOW


def test_a_full_walk_cut_short_by_the_page_budget_retires_nothing():
    wh = FakeWarehouse()
    wh.user_items[("7", "favorited")] = {"discovered_at": NOW - timedelta(days=30), "removed_at": EPOCH}
    client = FakeClient(
        user={"id": "zachlatta", "submitted": []},
        items={},
        lists={("favorites", False): [["1"], ["2"], ["3"]], ("favorites", True): [[]], ("upvoted", False): [[]], ("upvoted", True): [[]], ("hidden", False): [[]]},
    )
    _runner(wh, client, max_list_pages=2).sync()
    assert wh.user_items[("7", "favorited")]["removed_at"] == EPOCH
    assert wh.state["favorites"]["full_walk_completed_at"] == EPOCH
    assert "cut short" in wh.state["favorites"]["error"]


def test_throttling_ends_the_run_cleanly_with_what_was_written():
    wh = FakeWarehouse()
    client = FakeClient(
        user={"id": "zachlatta", "submitted": [101]},
        items={i["id"]: i for i in (STORY, MY_COMMENT, REPLY, OTHER)},
        lists={("favorites", False): [[]], ("favorites", True): [[]], ("upvoted", False): [[]], ("upvoted", True): [[]], ("hidden", False): [[]]},
        throttle_after=1,
    )
    summary = _runner(wh, client).sync()
    assert summary.rate_limited is True
    assert wh.state["items"]["status"] == "ok"


def test_live_threads_are_re_read_and_first_seen_is_kept():
    wh = FakeWarehouse()
    stale = NOW - timedelta(hours=12)
    wh.items["100"] = {"item_id": "100", "root_story_id": "100", "kids_json": ["101"], "posted_at": NOW - timedelta(days=1), "fetched_at": stale, "first_seen_at": stale}
    wh.items["101"] = {"item_id": "101", "root_story_id": "100", "kids_json": [], "posted_at": NOW - timedelta(days=1), "fetched_at": stale, "first_seen_at": stale}
    refreshed_story = dict(STORY, score=99, kids=[101, 103])
    client = FakeClient(
        user={"id": "zachlatta", "submitted": []},
        items={100: refreshed_story, 101: MY_COMMENT, 103: OTHER},
        lists={("favorites", False): [[]], ("favorites", True): [[]], ("upvoted", False): [[]], ("upvoted", True): [[]], ("hidden", False): [[]]},
    )
    summary = _runner(wh, client).sync()
    assert summary.items_refreshed == 2
    assert wh.items["100"]["score"] == 99
    assert wh.items["100"]["first_seen_at"] == stale
    # The refreshed kids list revealed a new comment; the frontier fetched it.
    assert "103" in wh.items
