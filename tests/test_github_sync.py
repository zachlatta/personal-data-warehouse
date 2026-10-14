"""The GitHub sync end to end: a fake GitHub behind the real client, a real warehouse."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import os
import re
from urllib.parse import urlsplit

import pytest

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse import github_sync
from personal_data_warehouse.github_api import GitHubClient, GitHubCredential, RequestBudget
from personal_data_warehouse.github_sync import GitHubSyncRunner
from personal_data_warehouse.postgres import PostgresWarehouse

pytestmark = pytest.mark.local_integration

ACCOUNT = "zachlatta"
NOW = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
ME = {"login": ACCOUNT, "type": "User"}
FRIEND = {"login": "a-friend", "type": "User"}


def ts(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


class Response:
    def __init__(self, status, body):
        self.status_code = status
        self._body = body
        self.headers = {}
        self.text = ""

    def json(self):
        return self._body


class FakeGitHub:
    """Answers the handful of endpoints the sync reads, honoring date qualifiers."""

    def __init__(self) -> None:
        self.issues: list[dict] = []  # each carries a "_streams" set
        self.commits: list[dict] = []
        self.events: list[dict] = []
        self.threads: dict[str, list[dict]] = {}
        self.hidden_repos: set[str] = set()
        self.status = 200
        self.searches: list[tuple[str, int]] = []

    def session(self):
        fake = self

        class Session:
            def get(self, url, params=None, headers=None, timeout=None):
                return fake.route(urlsplit(url).path, dict(params or {}))

        return Session()

    def route(self, path, params):
        if self.status != 200:
            return Response(self.status, {"message": "Bad credentials"})
        path = path.split("/gh", 1)[-1]
        page = int(params.get("page", 1))
        if path == f"/users/{ACCOUNT}/events":
            return Response(200, self.events[(page - 1) * 100 : page * 100])
        if path in ("/search/issues", "/search/commits"):
            return self.search(path, params["q"], page)
        match = re.match(r"/repos/([^/]+/[^/]+)/(issues|pulls)/(\d+)/(comments|reviews)", path)
        if match:
            repo = match.group(1)
            if repo in self.hidden_repos:
                return Response(404, {"message": "Not Found"})
            key = f"{repo}#{match.group(3)}:{match.group(2)}/{match.group(4)}"
            items = self.threads.get(key, [])
            return Response(200, items[(page - 1) * 100 : page * 100])
        return Response(404, {"message": "Not Found"})

    def search(self, path, query, page):
        commits = path.endswith("commits")
        pool = self.commits if commits else self.issues
        if not commits:
            stream = "reviewed" if "reviewed-by:" in query else ("pr" if "is:pr" in query else "issue")
            pool = [i for i in pool if stream in i["_streams"]]

        def when(item, qualifier):
            if commits:
                field = "author" if qualifier == "author-date" else "committer"
                return item["commit"][field]["date"]
            return item["created_at" if qualifier == "created" else "updated_at"]

        for qualifier, lo, hi in re.findall(r"([a-z-]+):(\S+?)\.\.(\S+)", query):
            pool = [i for i in pool if lo <= when(i, qualifier) <= hi]
        for qualifier, lo in re.findall(r"([a-z-]+):>=(\S+)", query):
            pool = [i for i in pool if when(i, qualifier) >= lo]
        self.searches.append((query, len(pool)))
        items = [{k: v for k, v in i.items() if k != "_streams"} for i in pool]
        return Response(200, {"total_count": len(items), "items": items[(page - 1) * 100 : page * 100]})


def issue(issue_id, number, *, user, created, updated, streams, repo="org/app", pr=False, body=""):
    item = {
        "id": issue_id, "number": number, "repository_url": f"https://api.github.com/repos/{repo}",
        "title": f"Issue {number}", "body": body, "state": "open", "user": user,
        "created_at": ts(created), "updated_at": ts(updated), "_streams": set(streams),
    }
    if pr:
        item["pull_request"] = {"merged_at": None}
    return item


def commit(sha, when, repo="zachlatta/demo"):
    return {
        "sha": sha, "repository": {"full_name": repo, "private": False}, "author": ME, "committer": ME,
        "commit": {"message": f"commit {sha}", "author": {"date": ts(when)}, "committer": {"date": ts(when)}},
    }


@pytest.fixture
def warehouse():
    wh = PostgresWarehouse(os.environ["POSTGRES_DATABASE_URL"], schema=make_test_schema())
    try:
        wh.ensure_github_tables()
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def run(wh, fake, *, now=NOW, max_requests=2000, credentials=None):
    budget = RequestBudget(max_requests=max_requests, max_seconds=600)
    credentials = credentials or [GitHubCredential.proxy("k", base_url="https://proxy.example.test/gh")]
    clients = [GitHubClient(c, budget=budget, session=fake.session(), sleep=lambda _s: None) for c in credentials]
    return GitHubSyncRunner(warehouse=wh, clients=clients, account=ACCOUNT, budget=budget, now=lambda: now).sync()


def seeded() -> FakeGitHub:
    fake = FakeGitHub()
    fake.issues = [
        issue(1, 7, user=ME, created=NOW - timedelta(days=3000), updated=NOW - timedelta(days=2000),
              streams={"pr"}, pr=True),
        issue(2, 8, user=FRIEND, created=NOW - timedelta(days=10), updated=NOW - timedelta(days=1),
              streams={"issue"}, body="@zachlatta ping"),
        issue(3, 9, user=FRIEND, created=NOW - timedelta(days=400), updated=NOW - timedelta(days=300),
              streams={"pr", "reviewed"}, pr=True, repo="other/lib"),
    ]
    fake.commits = [commit("a1", NOW - timedelta(days=5)), commit("a2", NOW - timedelta(days=4000))]
    fake.events = [
        {"id": "e1", "type": "WatchEvent", "actor": ME, "repo": {"name": "o/cool"}, "created_at": ts(NOW), "public": True},
    ]
    fake.threads = {
        "org/app#7:issues/comments": [
            {"id": 100, "user": FRIEND, "body": "looks good", "created_at": ts(NOW - timedelta(days=2999))},
        ],
        "org/app#7:pulls/reviews": [
            {"id": 200, "user": FRIEND, "body": "", "state": "APPROVED", "submitted_at": ts(NOW - timedelta(days=2999))},
        ],
        "org/app#7:pulls/comments": [],
        "org/app#8:issues/comments": [
            {"id": 101, "user": ME, "body": "on it", "created_at": ts(NOW - timedelta(days=9))},
        ],
        "other/lib#9:issues/comments": [],
        "other/lib#9:pulls/reviews": [
            {"id": 201, "user": ME, "body": "nit", "state": "CHANGES_REQUESTED", "submitted_at": ts(NOW - timedelta(days=399))},
        ],
        "other/lib#9:pulls/comments": [
            {"id": 300, "user": ME, "body": "rename", "path": "a.py", "created_at": ts(NOW - timedelta(days=399))},
        ],
    }
    return fake


def test_a_first_run_backfills_all_history_and_every_thread(warehouse) -> None:
    fake = seeded()
    summary = run(warehouse, fake)
    assert not summary.failures and not summary.budget_exhausted

    issues = warehouse._query("SELECT issue_id, is_pull_request, thread_status FROM @github_issues ORDER BY issue_id")
    assert issues == [("1", 1, "ok"), ("2", 0, "ok"), ("3", 1, "ok")]
    commits = warehouse._query("SELECT sha FROM @github_commits ORDER BY sha")
    assert commits == [("a1",), ("a2",)]  # the 11-year-old commit came through the backfill
    comments = warehouse._query("SELECT comment_kind, comment_id, author_login FROM @github_comments ORDER BY comment_id")
    assert comments == [
        ("issue_comment", "100", "a-friend"),
        ("issue_comment", "101", ACCOUNT),
        ("review", "200", "a-friend"),
        ("review", "201", ACCOUNT),
        ("review_comment", "300", ACCOUNT),
    ]
    assert warehouse._query("SELECT event_id FROM @github_events") == [("e1",)]

    state = warehouse.load_github_sync_state(account=ACCOUNT)
    for stream in ("issues_involved", "prs_involved", "prs_reviewed", "commits_authored"):
        row = state[f"{stream}@proxy"]
        assert row["status"] == "ok"
        assert row["backfill_completed_at"] > datetime(1971, 1, 1, tzinfo=UTC), stream
        assert row["incremental_watermark_at"] == NOW
    assert state["threads@all"]["status"] == "ok"


def test_a_later_run_reads_only_what_moved(warehouse) -> None:
    fake = seeded()
    run(warehouse, fake)
    later = NOW + timedelta(minutes=15)
    fake.searches.clear()
    # A friend replies on his old PR: the issue's updated_at moves and the
    # reply is the only new comment.
    fake.issues[0]["updated_at"] = ts(later - timedelta(minutes=5))
    fake.threads["org/app#7:issues/comments"].append(
        {"id": 102, "user": FRIEND, "body": "any update?", "created_at": ts(later - timedelta(minutes=5))}
    )
    summary = run(warehouse, fake, now=later)
    assert summary.threads_read == 1
    # No backfill search ran again: every stream is complete.
    assert all(":>=" in q for q, _ in fake.searches)
    assert warehouse._query("SELECT count(*) FROM @github_comments WHERE comment_id = '102'") == [(1,)]


def test_a_window_over_the_result_cap_is_split_until_every_item_is_reachable(warehouse, monkeypatch) -> None:
    monkeypatch.setattr(github_sync, "SEARCH_RESULT_CAP", 2)
    fake = FakeGitHub()
    fake.commits = [commit(f"c{i}", NOW - timedelta(days=1, hours=i * 3)) for i in range(7)]
    run(warehouse, fake)
    assert warehouse._query("SELECT count(*) FROM @github_commits") == [(7,)]
    backfill = [(q, n) for q, n in fake.searches if "author-date:" in q]
    assert any(n > 2 for _q, n in backfill)  # it did hit the cap...
    assert max(n for _q, n in backfill[-5:]) <= 2  # ...and split until it held


def test_a_rejected_token_is_action_required_and_another_credential_carries_on(warehouse) -> None:
    fake = seeded()
    good = GitHubCredential.proxy("k", base_url="https://proxy.example.test/gh")
    bad = GitHubCredential.token("dead", base_url="https://api.example.test")

    budget = RequestBudget(max_requests=2000, max_seconds=600)
    rejecting = FakeGitHub()
    rejecting.status = 401
    clients = [
        GitHubClient(bad, budget=budget, session=rejecting.session(), sleep=lambda _s: None),
        GitHubClient(good, budget=budget, session=fake.session(), sleep=lambda _s: None),
    ]
    summary = GitHubSyncRunner(warehouse=warehouse, clients=clients, account=ACCOUNT, budget=budget, now=lambda: NOW).sync()
    state = warehouse.load_github_sync_state(account=ACCOUNT)
    assert state[f"events@{bad.scope}"]["status"] == "action_required"
    assert "rejected" in state[f"events@{bad.scope}"]["error"]
    assert state["events@proxy"]["status"] == "ok"
    assert summary.comments == 5
    assert warehouse._query("SELECT count(*) FROM @github_issues") == [(3,)]


def test_a_spent_budget_stops_cleanly_and_the_next_run_resumes(warehouse) -> None:
    fake = seeded()
    run(warehouse, fake, max_requests=8)
    # Eight requests cannot hold the backfill: part of history waits.
    assert warehouse._query("SELECT count(*) FROM @github_comments")[0][0] < 5
    for _ in range(20):
        summary = run(warehouse, fake, max_requests=40)
        done = summary.backfill_complete and all(summary.backfill_complete.values())
        if done and not summary.budget_exhausted and summary.threads_remaining == 0:
            break
    assert warehouse._query("SELECT count(*) FROM @github_comments") == [(5,)]
    assert warehouse._query("SELECT count(*) FROM @github_commits") == [(2,)]


def test_a_thread_no_credential_can_see_is_marked_not_retried(warehouse) -> None:
    fake = seeded()
    fake.hidden_repos = {"other/lib"}
    run(warehouse, fake)
    assert warehouse._query("SELECT thread_status FROM @github_issues WHERE issue_id = '3'") == [("not_found",)]
    assert warehouse.github_threads_due(account=ACCOUNT, limit=10) == []
