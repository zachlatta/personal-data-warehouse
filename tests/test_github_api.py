"""The GitHub client's transport rules and the row normalizers."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.github_api import (
    EPOCH,
    GitHubAuthError,
    GitHubBudgetExhausted,
    GitHubClient,
    GitHubCredential,
    GitHubNotFoundError,
    GitHubRateLimitError,
    RequestBudget,
    comment_row,
    commit_row,
    event_row,
    issue_row,
    repo_from_api_url,
)

NOW = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)


class FakeResponse:
    def __init__(self, status: int, body, headers: dict[str, str] | None = None) -> None:
        self.status_code = status
        self._body = body
        self.headers = headers or {}
        self.text = str(body)

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, *responses: FakeResponse) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict, dict]] = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, dict(params or {}), dict(headers or {})))
        return self.responses.pop(0)


def _client(session, credential=None, *, max_requests=100, sleeps=None, clock=None):
    return GitHubClient(
        credential or GitHubCredential.proxy("k", base_url="https://proxy.example.test/gh"),
        budget=RequestBudget(max_requests=max_requests, max_seconds=60),
        session=session,
        sleep=(sleeps.append if sleeps is not None else (lambda _s: None)),
        clock=clock or (lambda: 0.0),
    )


def test_a_proxy_credential_sends_its_key_header_to_the_proxy_path() -> None:
    session = FakeSession(FakeResponse(200, {"total_count": 0, "items": []}))
    _client(session).search("issues", "involves:someone is:pr")
    url, params, headers = session.calls[0]
    assert url == "https://proxy.example.test/gh/search/issues"
    assert params["q"] == "involves:someone is:pr"
    assert headers["X-API-Key"] == "k"
    assert "Authorization" not in headers


def test_a_token_scope_names_the_token_without_containing_it() -> None:
    credential = GitHubCredential.token("fake-token-value")
    assert credential.scope.startswith("token:")
    assert "token-value" not in credential.scope
    assert credential.headers["Authorization"] == "Bearer fake-token-value"
    assert "token-value" not in repr(GitHubCredential.token("fake-token-value").scope)


def test_status_codes_map_to_their_meaning() -> None:
    for status, body, headers, error in (
        (404, {"message": "Not Found"}, {}, GitHubNotFoundError),
        (401, {"message": "Bad credentials"}, {}, GitHubAuthError),
        (403, {"message": "API rate limit exceeded"}, {"X-RateLimit-Remaining": "0"}, GitHubRateLimitError),
        (403, {"message": "You have exceeded a secondary rate limit"}, {}, GitHubRateLimitError),
        (429, {"message": "slow down"}, {"Retry-After": "30"}, GitHubRateLimitError),
    ):
        with pytest.raises(error):
            _client(FakeSession(FakeResponse(status, body, headers))).get_json("users/x")


def test_search_is_paced_per_credential() -> None:
    sleeps: list[float] = []
    session = FakeSession(*(FakeResponse(200, {"total_count": 0, "items": []}) for _ in range(2)))
    client = _client(session, GitHubCredential.token("t"), sleeps=sleeps)
    client.search("issues", "a")
    client.search("issues", "b")
    assert sleeps == [pytest.approx(2.1)]


def test_the_budget_stops_the_run_before_the_request() -> None:
    session = FakeSession(FakeResponse(200, []))
    client = _client(session, max_requests=1)
    client.get_json("users/x/events")
    with pytest.raises(GitHubBudgetExhausted):
        client.get_json("users/x/events")
    assert len(session.calls) == 1


def test_paginate_stops_at_a_short_page() -> None:
    full = [{"id": i} for i in range(100)]
    session = FakeSession(FakeResponse(200, full), FakeResponse(200, [{"id": 100}]))
    items = list(_client(session).paginate("repos/o/r/issues/1/comments"))
    assert len(items) == 101
    assert [c[1]["page"] for c in session.calls] == [1, 2]


def test_repo_names_parse_through_a_proxy_prefix() -> None:
    assert repo_from_api_url("https://api.github.com/repos/hackclub/site") == "hackclub/site"
    assert repo_from_api_url("https://proxy.example.test/gh/repos/o/r/issues/3") == "o/r"
    assert repo_from_api_url("https://example.test/nothing") == ""


def test_issue_row_tells_a_pull_request_from_an_issue() -> None:
    pr = issue_row(
        "me",
        {
            "id": 9, "number": 4, "repository_url": "https://api.github.com/repos/o/r", "title": "T",
            "body": None, "state": "closed", "user": {"login": "me", "type": "User"},
            "pull_request": {"merged_at": "2026-01-02T03:04:05Z"}, "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-02T03:04:05Z", "closed_at": "2026-01-02T03:04:05Z",
            "assignees": [{"login": "me"}], "labels": [{"name": "bug"}], "comments": 3,
        },
        now=NOW,
    )
    assert pr["is_pull_request"] == 1
    assert pr["repo"] == "o/r" and pr["issue_number"] == 4 and pr["issue_id"] == "9"
    assert pr["merged_at"] == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert pr["body"] == ""
    assert pr["assignee_logins"] == ["me"] and pr["label_names"] == ["bug"]
    issue = issue_row("me", {"id": 10, "number": 5, "repository_url": "https://api.github.com/repos/o/r"}, now=NOW)
    assert issue["is_pull_request"] == 0
    assert issue["merged_at"] == EPOCH and issue["created_at"] == EPOCH


def test_commit_comment_and_event_rows() -> None:
    commit = commit_row(
        "me",
        {
            "sha": "s1", "repository": {"full_name": "o/r", "private": True}, "author": {"login": "me"},
            "commit": {"message": "m", "author": {"name": "N", "email": "e@example.test", "date": "2020-05-01T00:00:00Z"},
                       "committer": {"date": "2020-05-02T00:00:00Z"}},
        },
        now=NOW,
    )
    assert (commit["repo"], commit["sha"], commit["is_private"]) == ("o/r", "s1", 1)
    assert commit["authored_at"] == datetime(2020, 5, 1, tzinfo=UTC)
    review = comment_row(
        "me", "review", {"id": 3, "state": "APPROVED", "submitted_at": "2026-02-01T00:00:00Z", "user": {"login": "x", "type": "User"}},
        repo="o/r", issue_number=4, issue_id="9", now=NOW,
    )
    assert review["review_state"] == "APPROVED"
    assert review["created_at"] == datetime(2026, 2, 1, tzinfo=UTC)
    assert review["updated_at"] == review["created_at"]
    event = event_row("me", {"id": "77", "type": "WatchEvent", "repo": {"name": "o/r"}, "actor": {"login": "me"},
                              "created_at": "2026-03-01T00:00:00Z", "public": True}, now=NOW)
    assert (event["event_id"], event["event_type"], event["repo"], event["is_public"]) == ("77", "WatchEvent", "o/r", 1)


def test_config_needs_the_login_and_keeps_tokens_out_of_its_repr(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_ACCOUNT", "someone")
    monkeypatch.setenv("GITHUB_SOURCE_TOKENS", "tok-a, tok-b")
    monkeypatch.delenv("GITHUB_PROXY_BASE_URL", raising=False)
    monkeypatch.delenv("GITHUB_PROXY_API_KEY", raising=False)
    config = load_settings(require_gmail=False, require_github=True).github
    assert config is not None
    assert config.account == "someone"
    assert config.tokens == ("tok-a", "tok-b")
    assert "tok-a" not in repr(config)
    monkeypatch.setenv("GITHUB_PROXY_BASE_URL", "https://proxy.example.test/gh")
    with pytest.raises(ValueError, match="together"):
        load_settings(require_gmail=False, require_github=True)
    monkeypatch.delenv("GITHUB_ACCOUNT")
    monkeypatch.delenv("GITHUB_PROXY_BASE_URL")
    with pytest.raises(ValueError, match="GITHUB_ACCOUNT"):
        load_settings(require_gmail=False, require_github=True)


def test_fraction_left_is_the_scarcer_of_requests_and_time() -> None:
    now = [0.0]
    budget = RequestBudget(max_requests=10, max_seconds=100, clock=lambda: now[0])
    assert budget.fraction_left() == 1.0
    budget.take()
    assert budget.fraction_left() == pytest.approx(0.9)
    now[0] = 70.0
    assert budget.fraction_left() == pytest.approx(0.3)
    now[0] = 200.0
    assert budget.fraction_left() == 0.0
