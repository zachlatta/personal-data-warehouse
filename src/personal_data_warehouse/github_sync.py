"""The GitHub sync: search finds what the login touched, thread lists fill in the talk.

One run, in order, under a shared request/time budget:

1. **Events feed** per credential: ``/users/<login>/events`` newest first,
   stopping at the first page holding an event already archived. GitHub
   keeps 90 days, so this is only the low-latency path; nothing depends on it
   for history.
2. **Incremental search** per credential and stream: each search re-run with
   ``updated:>=`` (commits: ``committer-date:>=``) the stream's watermark, an
   hour of overlap for search-index lag. Ids dedupe the overlap.
3. **Backfill search** per credential and stream: the same search walked
   newest to oldest in ``created:`` (commits: ``author-date:``) windows that
   halve until GitHub's 1,000-result cap holds and double again when they
   come back thin. The cursor is persisted after every window, so a run cut
   short resumes where it stopped; it ends at the floor (GitHub's launch).
4. **Threads**: every archived issue whose comment walk is unread, or older
   than the issue's ``updated_at``, has its comments (and, for a PR, its
   reviews and review comments) read, by anyone. That is what puts other
   people's replies to Zach's PRs on the timeline.

Walk state is per (stream, credential scope), so adding a token that can see
private repositories walks history again for that scope only. A rejected
credential is ``action_required`` on its own rows and the other credentials
keep working; throttling ends that credential's run cleanly.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import logging
from typing import Any

from personal_data_warehouse.github_api import (
    EPOCH,
    SEARCH_RESULT_CAP,
    GitHubAuthError,
    GitHubBudgetExhausted,
    GitHubClient,
    GitHubError,
    GitHubNotFoundError,
    GitHubRateLimitError,
    comment_row,
    commit_row,
    dedupe,
    event_row,
    iso,
    issue_row,
    parse_ts,
)

STATUS_OK = "ok"
STATUS_ERROR = "error"
STATUS_ACTION_REQUIRED = "action_required"
#: GitHub launched in 2008; nothing a login did is older.
BACKFILL_FLOOR = datetime(2008, 1, 1, tzinfo=UTC)
INITIAL_SPAN = timedelta(days=90)
MIN_SPAN = timedelta(hours=1)
MAX_SPAN = timedelta(days=4 * 365)
#: A window this thin doubles the next one.
THIN_WINDOW = 300
INCREMENTAL_OVERLAP = timedelta(hours=1)
INITIAL_INCREMENTAL_LOOKBACK = timedelta(days=2)
MAX_INCREMENTAL_PAGES = SEARCH_RESULT_CAP // 100
MAX_EVENT_PAGES = 3
THREADS_STREAM = "threads"
THREADS_SCOPE = "all"


@dataclass(frozen=True)
class SearchStream:
    name: str
    kind: str  # issues | commits
    query: str  # formatted with {account}
    created_qualifier: str
    updated_qualifier: str

    def render(self, account: str) -> str:
        return self.query.format(account=account)


STREAMS: tuple[SearchStream, ...] = (
    # involves: = author OR assignee OR mentions OR commenter.
    SearchStream("issues_involved", "issues", "involves:{account} is:issue", "created", "updated"),
    SearchStream("prs_involved", "issues", "involves:{account} is:pr", "created", "updated"),
    SearchStream("prs_reviewed", "issues", "reviewed-by:{account} is:pr", "created", "updated"),
    SearchStream("commits_authored", "commits", "author:{account}", "author-date", "committer-date"),
)


@dataclass
class GitHubSyncSummary:
    account: str
    events: int = 0
    issues: int = 0
    commits: int = 0
    comments: int = 0
    threads_read: int = 0
    threads_remaining: int = 0
    backfill_complete: dict[str, bool] = field(default_factory=dict)
    failures: dict[str, str] = field(default_factory=dict)
    rate_limited: list[str] = field(default_factory=list)
    budget_exhausted: bool = False
    requests: int = 0


class _StopClient(Exception):
    """This credential is done for the run (rejected or throttled)."""


class GitHubSyncRunner:
    def __init__(
        self,
        *,
        warehouse: Any,
        clients: Sequence[GitHubClient],
        account: str,
        budget: Any,
        backfill_share: float = 0.6,
        now: Callable[[], datetime] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if not clients:
            raise ValueError("GitHub sync needs at least one credential")
        self._wh = warehouse
        self._clients = list(clients)
        self._account = account
        self._budget = budget
        # Backfill may spend until this share of the run (requests OR time,
        # whichever is scarcer) remains; the rest is the thread walk's, so a
        # long history never starves conversations. Time is usually the
        # scarcer: the first production run spent its ten minutes on 385
        # requests and read no thread.
        self._thread_reserve = 1.0 - backfill_share
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._log = logger or logging.getLogger(__name__)
        self._summary = GitHubSyncSummary(account=account)
        self._stopped: set[str] = set()
        self._state: dict[str, dict[str, Any]] = {}

    # --- run ------------------------------------------------------------------

    def sync(self) -> GitHubSyncSummary:
        self._wh.ensure_github_tables()
        self._state = self._wh.load_github_sync_state(account=self._account)
        self._run_start = self._now()
        try:
            for client in self._clients:
                self._guarded(client, "events", lambda c=client: self._sync_events(c))
            for client in self._clients:
                for stream in STREAMS:
                    self._guarded(client, stream.name, lambda c=client, s=stream: self._incremental(c, s))
            for client in self._clients:
                for stream in STREAMS:
                    self._guarded(client, stream.name, lambda c=client, s=stream: self._backfill(c, s))
            self._sync_threads()
        except GitHubBudgetExhausted as exc:
            self._summary.budget_exhausted = True
            self._log.info("GitHub run budget spent; resuming next run: %s", exc)
        self._summary.requests = int(getattr(self._budget, "used", 0))
        return self._summary

    def _guarded(self, client: GitHubClient, stream: str, work: Callable[[], None]) -> None:
        if client.scope in self._stopped:
            return
        try:
            work()
        except GitHubAuthError as exc:
            self._stopped.add(client.scope)
            self._summary.failures[f"{stream}@{client.scope}"] = str(exc)
            self._mark_scope(client.scope, STATUS_ACTION_REQUIRED, str(exc))
        except GitHubRateLimitError as exc:
            self._stopped.add(client.scope)
            self._summary.rate_limited.append(client.scope)
            self._log.warning("GitHub throttled %s; stopping it for this run: %s", client.scope, exc)
        except GitHubBudgetExhausted:
            raise
        except GitHubError as exc:
            self._summary.failures[f"{stream}@{client.scope}"] = str(exc)
            self._save(stream, client.scope, status=STATUS_ERROR, error=str(exc))

    # --- state ----------------------------------------------------------------

    def _row(self, stream: str, scope: str) -> dict[str, Any]:
        key = f"{stream}@{scope}"
        row = self._state.get(key)
        if row is None:
            row = {
                "account": self._account,
                "stream": key,
                "credential_scope": scope,
                "status": STATUS_OK,
                "error": "",
                "last_success_at": EPOCH,
                "backfill_cursor_at": EPOCH,
                "backfill_span_seconds": 0,
                "backfill_completed_at": EPOCH,
                "incremental_watermark_at": EPOCH,
                "items_seen": 0,
            }
            self._state[key] = row
        return row

    def _save(self, stream: str, scope: str, *, status: str = STATUS_OK, error: str = "", **changes: Any) -> None:
        row = self._row(stream, scope)
        row.update(changes)
        row["status"] = status
        row["error"] = error
        now = self._now()
        if status == STATUS_OK:
            row["last_success_at"] = now
        row["updated_at"] = now
        row["sync_version"] = int(now.timestamp() * 1_000_000)
        self._wh.save_github_sync_state(row)

    def _mark_scope(self, scope: str, status: str, error: str) -> None:
        for stream in ("events", *(s.name for s in STREAMS)):
            self._save(stream, scope, status=status, error=error)

    # --- stage 1: events --------------------------------------------------------

    def _sync_events(self, client: GitHubClient) -> None:
        now = self._now()
        stored = 0
        for page in range(1, MAX_EVENT_PAGES + 1):
            events = client.user_events(self._account, page=page)
            if not events:
                break
            rows = [event_row(self._account, e, now=now) for e in events]
            known = self._wh.github_known_event_ids(account=self._account, event_ids=[r["event_id"] for r in rows])
            self._wh.upsert_github_events(dedupe(rows, lambda r: (r["event_id"],)))
            stored += len(rows) - len(known)
            if known or len(events) < 100:
                break
        self._summary.events += stored
        row = self._row("events", client.scope)
        self._save("events", client.scope, items_seen=int(row.get("items_seen") or 0) + stored)

    # --- stage 2: incremental search --------------------------------------------

    def _incremental(self, client: GitHubClient, stream: SearchStream) -> None:
        row = self._row(stream.name, client.scope)
        watermark = row.get("incremental_watermark_at") or EPOCH
        if watermark <= EPOCH:
            watermark = self._run_start - INITIAL_INCREMENTAL_LOOKBACK
        query = f"{stream.render(self._account)} {stream.updated_qualifier}:>={iso(watermark - INCREMENTAL_OVERLAP)}"
        fetched = 0
        last_seen = None
        total = 0
        for page in range(1, MAX_INCREMENTAL_PAGES + 1):
            result = client.search(stream.kind, query, page=page, sort=stream.updated_qualifier, order="asc")
            total = result.total_count
            if not result.items:
                break
            self._store(stream, result.items)
            fetched += len(result.items)
            last_seen = self._updated_ts(stream, result.items[-1])
            if fetched >= min(total, SEARCH_RESULT_CAP) or len(result.items) < 100:
                break
        # More than one search can return means a long outage: advance only as
        # far as what was actually read, sorted ascending, and finish next run.
        new_watermark = last_seen if (total > fetched and last_seen) else self._run_start
        self._save(
            stream.name,
            client.scope,
            incremental_watermark_at=max(new_watermark, watermark),
            items_seen=int(row.get("items_seen") or 0) + fetched,
        )

    # --- stage 3: backfill search -----------------------------------------------

    def _backfill(self, client: GitHubClient, stream: SearchStream) -> None:
        row = self._row(stream.name, client.scope)
        key = f"{stream.name}@{client.scope}"
        if (row.get("backfill_completed_at") or EPOCH) > EPOCH:
            self._summary.backfill_complete[key] = True
            return
        self._summary.backfill_complete[key] = False
        cursor = row.get("backfill_cursor_at") or EPOCH
        if cursor <= EPOCH:
            cursor = self._run_start
        span = timedelta(seconds=int(row.get("backfill_span_seconds") or 0)) or INITIAL_SPAN
        base_query = stream.render(self._account)
        while cursor > BACKFILL_FLOOR and self._budget.fraction_left() > self._thread_reserve:
            start = max(BACKFILL_FLOOR, cursor - span)
            end = cursor - timedelta(seconds=1)
            query = f"{base_query} {stream.created_qualifier}:{iso(start)}..{iso(end)}"
            first = client.search(stream.kind, query, page=1, sort=stream.created_qualifier, order="desc")
            if first.total_count > SEARCH_RESULT_CAP and span > MIN_SPAN:
                span = max(span / 2, MIN_SPAN)
                continue
            if first.total_count > SEARCH_RESULT_CAP:
                self._log.warning(
                    "GitHub %s window %s..%s holds %d results; only the first %d are reachable",
                    key, iso(start), iso(end), first.total_count, SEARCH_RESULT_CAP,
                )
            items = list(first.items)
            want = min(first.total_count, SEARCH_RESULT_CAP)
            page = 2
            while len(items) < want and first.items:
                more = client.search(stream.kind, query, page=page, sort=stream.created_qualifier, order="desc")
                if not more.items:
                    break
                items.extend(more.items)
                page += 1
            self._store(stream, items)
            cursor = start
            if first.total_count < THIN_WINDOW:
                span = min(span * 2, MAX_SPAN)
            done = cursor <= BACKFILL_FLOOR
            self._save(
                stream.name,
                client.scope,
                backfill_cursor_at=cursor,
                backfill_span_seconds=int(span.total_seconds()),
                backfill_completed_at=self._now() if done else EPOCH,
                items_seen=int(row.get("items_seen") or 0) + len(items),
            )
            if done:
                self._summary.backfill_complete[key] = True

    # --- storage helpers ----------------------------------------------------------

    def _updated_ts(self, stream: SearchStream, item: Mapping[str, Any]) -> datetime:
        if stream.kind == "commits":
            return parse_ts(((item.get("commit") or {}).get("committer") or {}).get("date"))
        return parse_ts(item.get("updated_at"))

    def _store(self, stream: SearchStream, items: Sequence[Mapping[str, Any]]) -> None:
        if not items:
            return
        now = self._now()
        if stream.kind == "commits":
            rows = [commit_row(self._account, i, now=now) for i in items]
            rows = [r for r in rows if r["repo"] and r["sha"]]
            self._wh.upsert_github_commits(dedupe(rows, lambda r: (r["repo"], r["sha"])))
            self._summary.commits += len(rows)
        else:
            rows = [issue_row(self._account, i, now=now) for i in items]
            rows = [r for r in rows if r["issue_id"] and r["repo"]]
            self._wh.upsert_github_issues(dedupe(rows, lambda r: (r["issue_id"],)))
            self._summary.issues += len(rows)

    # --- stage 4: threads ------------------------------------------------------------

    def _sync_threads(self) -> None:
        clients = [c for c in self._clients if c.scope not in self._stopped]
        if not clients:
            return
        due = self._wh.github_threads_due(account=self._account, limit=max(self._budget.remaining(), 1))
        read = 0
        for issue in due:
            if not clients:
                break
            try:
                status = self._read_thread(clients, issue)
            except GitHubRateLimitError as exc:
                self._log.warning("GitHub throttled the thread walk; resuming next run: %s", exc)
                self._summary.rate_limited.append(THREADS_STREAM)
                break
            except GitHubAuthError as exc:
                self._summary.failures[THREADS_STREAM] = str(exc)
                break
            except GitHubBudgetExhausted:
                self._summary.threads_remaining = len(due) - read
                self._save(THREADS_STREAM, THREADS_SCOPE)
                raise
            except GitHubError as exc:
                self._log.warning("GitHub thread %s#%s failed: %s", issue["repo"], issue["issue_number"], exc)
                status = STATUS_ERROR
            self._wh.mark_github_thread(
                account=self._account,
                issue_id=str(issue["issue_id"]),
                thread_updated_at=issue["updated_at"],
                status=status,
                now=self._now(),
            )
            read += 1
        self._summary.threads_read += read
        self._summary.threads_remaining = max(len(due) - read, 0)
        self._save(THREADS_STREAM, THREADS_SCOPE)

    def _read_thread(self, clients: Sequence[GitHubClient], issue: Mapping[str, Any]) -> str:
        """Read one thread with the first credential that can see it."""
        repo = str(issue["repo"])
        number = int(issue["issue_number"])
        issue_id = str(issue["issue_id"])
        previously = (issue.get("thread_synced_at") or EPOCH) > EPOCH
        since = (issue.get("thread_updated_at") or EPOCH) - INCREMENTAL_OVERLAP if previously else None
        for client in clients:
            try:
                rows = self._thread_rows(client, repo, number, issue_id, bool(issue["is_pull_request"]), since)
            except GitHubNotFoundError:
                continue
            self._wh.upsert_github_comments(dedupe(rows, lambda r: (r["comment_kind"], r["comment_id"])))
            self._summary.comments += len(rows)
            return STATUS_OK
        return "not_found"

    def _thread_rows(
        self,
        client: GitHubClient,
        repo: str,
        number: int,
        issue_id: str,
        is_pull: bool,
        since: datetime | None,
    ) -> list[dict[str, Any]]:
        now = self._now()

        def rows(kind: str, items: Any) -> list[dict[str, Any]]:
            return [
                comment_row(self._account, kind, i, repo=repo, issue_number=number, issue_id=issue_id, now=now)
                for i in items
            ]

        out = rows("issue_comment", client.issue_comments(repo, number, since=since))
        if is_pull:
            out += rows("review", client.pull_reviews(repo, number))
            out += rows("review_comment", client.pull_review_comments(repo, number, since=since))
        return [r for r in out if r["comment_id"]]


def public_summary(summary: GitHubSyncSummary) -> dict[str, Any]:
    return {
        "account": summary.account,
        "events": summary.events,
        "issues": summary.issues,
        "commits": summary.commits,
        "comments": summary.comments,
        "threads_read": summary.threads_read,
        "threads_remaining": summary.threads_remaining,
        "backfill_complete": dict(summary.backfill_complete),
        "failures": dict(summary.failures),
        "rate_limited": list(summary.rate_limited),
        "budget_exhausted": summary.budget_exhausted,
        "requests": summary.requests,
    }
