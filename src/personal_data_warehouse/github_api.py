"""GitHub REST client and the row normalizers for the GitHub source.

The source reads three surfaces of the REST API, all JSON:

* **Search** (``/search/issues``, ``/search/commits``) is the only way to
  find "everything this login touched" across every repository, and the only
  way back further than 90 days. It returns at most 1,000 results per query
  and is throttled separately (30 a minute for a token), so the sync walks it
  in date windows small enough to stay under the cap.
* **Thread lists** (issue comments, PR reviews, PR review comments) give the
  whole conversation under an issue, whoever wrote it.
* **The events feed** (``/users/<login>/events``) is the low-latency record of
  acts search cannot find (stars, forks, releases). GitHub keeps 90 days.

A credential is a base URL plus headers, so the same client talks to
api.github.com with a token, to a read-only proxy with its own key header, or
anonymously. Which repositories a credential can see is the credential's
business; the sync keys its walk state by :attr:`GitHubCredential.scope` so a
new token that can see private repositories walks history from scratch.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
import hashlib
import time
from typing import Any

import requests

DEFAULT_API_BASE_URL = "https://api.github.com"
DEFAULT_USER_AGENT = "personal-data-warehouse github_sync (+https://github.com/zachlatta/personal-data-warehouse)"
DEFAULT_TIMEOUT_SECONDS = 30
PER_PAGE = 100
#: GitHub serves at most this many results for one search query.
SEARCH_RESULT_CAP = 1000
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class GitHubError(RuntimeError):
    """GitHub answered in a way the sync cannot use."""


class GitHubNotFoundError(GitHubError):
    """404: the resource is gone or this credential cannot see it."""


class GitHubAuthError(GitHubError):
    """401: the credential itself was rejected."""


class GitHubRateLimitError(GitHubError):
    """403/429 with a rate-limit signal; back off rather than fail the run."""


class GitHubBudgetExhausted(GitHubError):
    """The run's request or time budget is spent; stop cleanly and resume next run."""


@dataclass(frozen=True)
class GitHubCredential:
    """Where to send requests and how to authenticate them.

    ``scope`` is a stable, non-secret label (``token:<sha256 prefix>``,
    ``proxy``, ``anonymous``) that keys walk state; it never contains the
    secret itself.
    """

    scope: str
    base_url: str
    headers: Mapping[str, str] = field(default_factory=dict)
    #: Seconds between search requests: GitHub allows 30/min with a token and
    #: 10/min anonymously; a pooled proxy needs none.
    search_interval_seconds: float = 0.0

    @classmethod
    def token(cls, token: str, *, base_url: str = DEFAULT_API_BASE_URL) -> GitHubCredential:
        digest = hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]
        return cls(
            scope=f"token:{digest}",
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}"},
            search_interval_seconds=2.1,
        )

    @classmethod
    def proxy(cls, api_key: str, *, base_url: str) -> GitHubCredential:
        return cls(scope="proxy", base_url=base_url, headers={"X-API-Key": api_key})

    @classmethod
    def anonymous(cls, *, base_url: str = DEFAULT_API_BASE_URL) -> GitHubCredential:
        return cls(scope="anonymous", base_url=base_url, search_interval_seconds=6.5)


class RequestBudget:
    """A per-run cap on requests and wall time, shared by every client in the run."""

    def __init__(self, *, max_requests: int, max_seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._max_requests = max(int(max_requests), 0)
        self._max_seconds = max(float(max_seconds), 0.0)
        self._clock = clock
        self._deadline = clock() + self._max_seconds
        self.used = 0

    def remaining(self) -> int:
        return max(self._max_requests - self.used, 0)

    def fraction_left(self) -> float:
        """The scarcer of requests and time left, as a share of the run's budget."""
        requests = self.remaining() / self._max_requests if self._max_requests else 0.0
        seconds = (self._deadline - self._clock()) / self._max_seconds if self._max_seconds else 0.0
        return max(min(requests, seconds), 0.0)

    def take(self) -> None:
        if self.used >= self._max_requests or self._clock() >= self._deadline:
            raise GitHubBudgetExhausted(f"run budget spent after {self.used} requests")
        self.used += 1


@dataclass(frozen=True)
class SearchPage:
    total_count: int
    items: tuple[Mapping[str, Any], ...]


class GitHubClient:
    def __init__(
        self,
        credential: GitHubCredential,
        *,
        budget: RequestBudget,
        session: Any | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.credential = credential
        self._budget = budget
        self._session = session or requests.Session()
        self._timeout = timeout
        self._sleep = sleep
        self._clock = clock
        self._last_search = float("-inf")

    @property
    def scope(self) -> str:
        return self.credential.scope

    # --- transport ------------------------------------------------------------

    def get_json(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        self._budget.take()
        url = self.credential.base_url.rstrip("/") + "/" + path.lstrip("/")
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": DEFAULT_USER_AGENT,
            **dict(self.credential.headers),
        }
        try:
            response = self._session.get(url, params=dict(params or {}), headers=headers, timeout=self._timeout)
        except requests.RequestException as exc:
            raise GitHubError(f"GitHub request failed for {path}: {type(exc).__name__}") from exc
        status = int(response.status_code)
        if status == 200:
            return response.json()
        message = _error_message(response)
        if status == 404:
            raise GitHubNotFoundError(f"GitHub 404 for {path}")
        if status == 401:
            raise GitHubAuthError(f"GitHub rejected the {self.scope} credential (401): {message}")
        if status == 429 or (status == 403 and _is_rate_limited(response, message)):
            raise GitHubRateLimitError(f"GitHub rate limit for {self.scope} on {path}: {message}")
        raise GitHubError(f"GitHub {status} for {path}: {message}")

    def paginate(self, path: str, params: Mapping[str, Any] | None = None, *, max_pages: int = 50) -> Iterator[Mapping[str, Any]]:
        """Every item of a list endpoint, by page number (a proxy may rewrite Link headers)."""
        for page in range(1, max_pages + 1):
            batch = self.get_json(path, {**dict(params or {}), "per_page": PER_PAGE, "page": page})
            if not isinstance(batch, list):
                raise GitHubError(f"GitHub returned a non-list for {path}")
            yield from batch
            if len(batch) < PER_PAGE:
                return

    # --- endpoints ------------------------------------------------------------

    def search(self, kind: str, query: str, *, page: int = 1, sort: str = "", order: str = "desc") -> SearchPage:
        """One page of ``/search/<kind>`` (``issues`` or ``commits``)."""
        wait = self.credential.search_interval_seconds - (self._clock() - self._last_search)
        if wait > 0:
            self._sleep(wait)
        params: dict[str, Any] = {"q": query, "per_page": PER_PAGE, "page": page}
        if sort:
            params["sort"] = sort
            params["order"] = order
        try:
            body = self.get_json(f"search/{kind}", params)
        finally:
            self._last_search = self._clock()
        return SearchPage(
            total_count=int(body.get("total_count") or 0),
            items=tuple(body.get("items") or ()),
        )

    def user_events(self, login: str, *, page: int = 1) -> list[Mapping[str, Any]]:
        body = self.get_json(f"users/{login}/events", {"per_page": PER_PAGE, "page": page})
        return list(body or [])

    def issue_comments(self, repo: str, number: int, *, since: datetime | None = None) -> Iterator[Mapping[str, Any]]:
        params = {"since": iso(since)} if since else {}
        return self.paginate(f"repos/{repo}/issues/{int(number)}/comments", params)

    def pull_reviews(self, repo: str, number: int) -> Iterator[Mapping[str, Any]]:
        return self.paginate(f"repos/{repo}/pulls/{int(number)}/reviews")

    def pull_review_comments(self, repo: str, number: int, *, since: datetime | None = None) -> Iterator[Mapping[str, Any]]:
        params = {"since": iso(since)} if since else {}
        return self.paginate(f"repos/{repo}/pulls/{int(number)}/comments", params)


def _error_message(response: Any) -> str:
    try:
        body = response.json()
    except ValueError:
        return str(getattr(response, "text", "") or "")[:200]
    if isinstance(body, Mapping):
        return str(body.get("message") or "")[:200]
    return ""


def _is_rate_limited(response: Any, message: str) -> bool:
    headers = {k.lower(): v for k, v in dict(getattr(response, "headers", {}) or {}).items()}
    if headers.get("x-ratelimit-remaining") == "0" or "retry-after" in headers:
        return True
    return "rate limit" in message.lower()


# --- time ---------------------------------------------------------------------


def parse_ts(value: Any) -> datetime:
    if not value:
        return EPOCH
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return EPOCH
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- normalizers ----------------------------------------------------------------


def repo_from_api_url(url: str) -> str:
    """``.../repos/<owner>/<name>[/...]`` -> ``owner/name`` (works through a proxy prefix)."""
    marker = "/repos/"
    if marker not in url:
        return ""
    parts = url.split(marker, 1)[1].split("/")
    return "/".join(parts[:2]) if len(parts) >= 2 else ""


def _login(user: Any) -> str:
    return str((user or {}).get("login") or "") if isinstance(user, Mapping) else ""


def _user_type(user: Any) -> str:
    return str((user or {}).get("type") or "") if isinstance(user, Mapping) else ""


def _version(now: datetime) -> int:
    return int(now.astimezone(UTC).timestamp() * 1_000_000)


def issue_row(account: str, item: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    """A ``/search/issues`` item (issue or PR) -> a ``github_issues`` row."""
    pull = item.get("pull_request") if isinstance(item.get("pull_request"), Mapping) else None
    return {
        "account": account,
        "issue_id": str(item.get("id") or ""),
        "repo": repo_from_api_url(str(item.get("repository_url") or "")),
        "issue_number": int(item.get("number") or 0),
        "is_pull_request": 1 if pull is not None else 0,
        "title": str(item.get("title") or ""),
        "body": str(item.get("body") or ""),
        "issue_state": str(item.get("state") or ""),
        "state_reason": str(item.get("state_reason") or ""),
        "author_login": _login(item.get("user")),
        "author_type": _user_type(item.get("user")),
        "assignee_logins": [_login(a) for a in (item.get("assignees") or []) if _login(a)],
        "label_names": [str(lbl.get("name") or "") for lbl in (item.get("labels") or []) if isinstance(lbl, Mapping)],
        "comments_count": int(item.get("comments") or 0),
        "created_at": parse_ts(item.get("created_at")),
        "updated_at": parse_ts(item.get("updated_at")),
        "closed_at": parse_ts(item.get("closed_at")),
        "merged_at": parse_ts((pull or {}).get("merged_at")),
        "html_url": str(item.get("html_url") or ""),
        "raw_json": dict(item),
        "synced_at": now,
        "sync_version": _version(now),
    }


def commit_row(account: str, item: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    """A ``/search/commits`` item -> a ``github_commits`` row."""
    commit = item.get("commit") or {}
    author = commit.get("author") or {}
    committer = commit.get("committer") or {}
    repository = item.get("repository") or {}
    return {
        "account": account,
        "repo": str(repository.get("full_name") or repo_from_api_url(str(item.get("url") or ""))),
        "sha": str(item.get("sha") or ""),
        "author_login": _login(item.get("author")),
        "author_name": str(author.get("name") or ""),
        "author_email": str(author.get("email") or ""),
        "committer_login": _login(item.get("committer")),
        "authored_at": parse_ts(author.get("date")),
        "committed_at": parse_ts(committer.get("date")),
        "message": str(commit.get("message") or ""),
        "html_url": str(item.get("html_url") or ""),
        "is_private": 1 if repository.get("private") else 0,
        "raw_json": dict(item),
        "synced_at": now,
        "sync_version": _version(now),
    }


def comment_row(
    account: str,
    kind: str,
    item: Mapping[str, Any],
    *,
    repo: str,
    issue_number: int,
    issue_id: str,
    now: datetime,
) -> dict[str, Any]:
    """An issue comment, PR review or PR review comment -> a ``github_comments`` row."""
    created = item.get("submitted_at") if kind == "review" else item.get("created_at")
    return {
        "account": account,
        "comment_kind": kind,
        "comment_id": str(item.get("id") or ""),
        "repo": repo,
        "issue_number": int(issue_number),
        "issue_id": issue_id,
        "author_login": _login(item.get("user")),
        "author_type": _user_type(item.get("user")),
        "body": str(item.get("body") or ""),
        "review_state": str(item.get("state") or "") if kind == "review" else "",
        "path": str(item.get("path") or ""),
        "in_reply_to_id": str(item.get("in_reply_to_id") or ""),
        "created_at": parse_ts(created),
        "updated_at": parse_ts(item.get("updated_at") or created),
        "html_url": str(item.get("html_url") or ""),
        "raw_json": dict(item),
        "synced_at": now,
        "sync_version": _version(now),
    }


def event_row(account: str, event: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    """A ``/users/<login>/events`` entry -> a ``github_events`` row."""
    return {
        "account": account,
        "event_id": str(event.get("id") or ""),
        "event_type": str(event.get("type") or ""),
        "repo": str((event.get("repo") or {}).get("name") or ""),
        "actor_login": _login(event.get("actor")),
        "created_at": parse_ts(event.get("created_at")),
        "is_public": 1 if event.get("public") else 0,
        "payload_json": dict(event),
        "synced_at": now,
        "sync_version": _version(now),
    }


def dedupe(rows: Sequence[Mapping[str, Any]], key: Callable[[Mapping[str, Any]], tuple[Any, ...]]) -> list[dict[str, Any]]:
    """Last-wins dedupe by key, so one upsert batch never names a row twice."""
    out: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        out[key(row)] = dict(row)
    return list(out.values())
