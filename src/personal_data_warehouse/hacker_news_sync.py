"""The Hacker News sync: lists say WHY an item matters, the API says WHAT it is.

One run, in order:

1. **Profile + submissions.** ``/v0/user/<account>`` carries karma and the
   complete list of everything the account ever submitted (stories AND
   comments), so ``relation = 'submitted'`` is a full walk on every run, in
   one request, and a deleted submission is retired the same run.
2. **Lists.** ``favorites`` is public; ``upvoted`` and ``hidden`` need the
   published cookie. Each list is walked newest-first: an incremental walk
   stops at the first page holding nothing new, and once per
   ``full_walk_interval`` the walk runs to the end so an upvote or favorite
   that was taken back can be stamped ``removed_at``. A login page on a
   private list marks the session rejected and records ``action_required``
   on that list's own state row; the public work is unaffected, so a dead
   cookie degrades to "public only", never to silence.
3. **Items.** Every id a list named that the archive lacks is fetched, then
   its parent chain up to the story (a favorited comment needs its story to
   be a discussion at all), then the **frontier**: every ``kids`` id of an
   archived item that is not itself archived. That is what makes "the full
   post and all the comments" true: the table is the walk state, a run cut
   short by ``max_item_fetches_per_run`` resumes by asking the same question.
   Last, items in still-live discussions (root story younger than
   ``live_window``) are re-read on a ``refresh_min_age`` cadence, because a
   comment's ``kids`` list is the only way to learn about a new reply.

Failures are per stage and recorded per list in ``ops.hacker_news_sync_state``;
HN throttling ends the run cleanly rather than failing it.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import hashlib
import logging
from typing import Any

from personal_data_warehouse.hacker_news_api import (
    HackerNewsAuthError,
    HackerNewsClient,
    HackerNewsError,
    HackerNewsRateLimitError,
    ItemRecord,
    PRIVATE_LISTS,
)

STORY_TYPES = frozenset({"story", "job", "poll"})
#: Lists walked as HTML pages, and whether each has a separate comments half.
LIST_SPECS: tuple[tuple[str, bool], ...] = (
    ("favorites", True),
    ("upvoted", True),
    ("hidden", False),
)
RELATION_BY_LIST = {"favorites": "favorited", "upvoted": "upvoted", "hidden": "hidden", "submitted": "submitted"}
#: State rows that are not lists.
ITEMS_STATE_ROW = "items"
PUBLISH_SESSION_HINT = "run `pdw hn publish-session` on the Mac signed in to news.ycombinator.com"

STATUS_OK = "ok"
STATUS_ERROR = "error"
STATUS_ACTION_REQUIRED = "action_required"


class HackerNewsActionRequiredError(RuntimeError):
    """Every private list failed on the same dead cookie; only a human can fix it."""


@dataclass
class HackerNewsSyncSummary:
    account: str
    lists_walked: dict[str, int] = field(default_factory=dict)
    lists_failed: dict[str, str] = field(default_factory=dict)
    items_fetched: int = 0
    items_refreshed: int = 0
    frontier_remaining: int = 0
    rate_limited: bool = False
    session_rejected: bool = False
    budget_exhausted: bool = False


def credential_sha256(cookie_header: str) -> str:
    return hashlib.sha256(cookie_header.encode("utf-8")).hexdigest() if cookie_header else ""


def hacker_news_reauthorization_skip_reason(
    state_by_list: Mapping[str, Mapping[str, Any]], *, credential: str
) -> str | None:
    """Skip the private lists only while the exact rejected cookie is still installed.

    The public work still runs every tick; this only says whether the sensor
    should bother launching for a session known to be dead. It returns a
    reason when EVERY private list recorded action_required for this
    fingerprint, mirroring ``whoop_private_reauthorization_skip_reason``.
    """
    fingerprint = credential_sha256(credential)
    if not fingerprint:
        return None
    rows = [state_by_list.get(name) for name in PRIVATE_LISTS]
    if not all(
        row
        and str(row.get("status") or "") == STATUS_ACTION_REQUIRED
        and str(row.get("credential_sha256") or "") == fingerprint
        for row in rows
    ):
        return None
    return (
        "the published news.ycombinator.com session was rejected; the login-only lists are "
        f"skipped until a different session is published ({PUBLISH_SESSION_HINT}). Public lists "
        "and the item walk keep running."
    )


class HackerNewsSyncRunner:
    def __init__(
        self,
        *,
        warehouse: Any,
        client: HackerNewsClient,
        account: str,
        session_key: str = "default",
        session_token_sha256: str = "",
        max_item_fetches: int = 3000,
        max_list_pages: int = 40,
        full_walk_interval: timedelta = timedelta(days=7),
        live_window: timedelta = timedelta(days=3),
        refresh_min_age: timedelta = timedelta(hours=6),
        now: Callable[[], datetime] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._wh = warehouse
        self._client = client
        self._account = account
        self._session_key = session_key
        self._token_sha = session_token_sha256
        self._max_fetches = max(int(max_item_fetches), 0)
        self._max_pages = max(int(max_list_pages), 1)
        self._full_walk_interval = full_walk_interval
        self._live_window = live_window
        self._refresh_min_age = refresh_min_age
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._log = logger or logging.getLogger(__name__)
        self._fetches = 0
        self._summary = HackerNewsSyncSummary(account=account)
        self._wanted: list[str] = []

    # --- run ------------------------------------------------------------------

    def sync(self) -> HackerNewsSyncSummary:
        self._wh.ensure_hacker_news_tables()
        state = self._wh.load_hacker_news_sync_state(account=self._account)
        try:
            self._sync_profile_and_submissions()
            for list_name, has_comments in LIST_SPECS:
                self._sync_list(list_name, has_comments=has_comments, state=state.get(list_name))
            self._fetch_wanted()
            self._walk_frontier()
            self._refresh_live()
            # A refresh that revealed a new reply put it on the frontier; take
            # it now rather than a poll later, so a live thread is complete
            # the same run it was re-read.
            self._walk_frontier()
        except HackerNewsRateLimitError as exc:
            self._summary.rate_limited = True
            self._log.warning("Hacker News throttled the run; stopping cleanly: %s", exc)
        self._record(ITEMS_STATE_ROW, STATUS_OK, "", success=True)
        return self._summary

    # --- stage 1: profile + submissions ---------------------------------------

    def _sync_profile_and_submissions(self) -> None:
        now = self._now()
        try:
            user = self._client.get_user(self._account)
        except HackerNewsRateLimitError:
            raise
        except HackerNewsError as exc:
            self._record("submitted", STATUS_ERROR, str(exc), success=False)
            self._summary.lists_failed["submitted"] = str(exc)
            return
        if not user:
            message = f"Hacker News has no user {self._account!r}"
            self._record("submitted", STATUS_ERROR, message, success=False)
            self._summary.lists_failed["submitted"] = message
            return
        submitted = [str(i) for i in (user.get("submitted") or [])]
        created = int(user.get("created") or 0)
        self._wh.insert_hacker_news_profile(
            {
                "account": self._account,
                "user_id": str(user.get("id") or self._account),
                "karma": int(user.get("karma") or 0),
                "about": str(user.get("about") or ""),
                "submitted_count": len(submitted),
                "created_at": datetime.fromtimestamp(created, tz=UTC) if created else datetime.fromtimestamp(0, tz=UTC),
                "raw_json": {k: v for k, v in user.items() if k != "submitted"},
                "synced_at": now,
                "sync_version": int(now.timestamp() * 1_000_000),
            }
        )
        self._wh.upsert_hacker_news_user_items(
            account=self._account, relation="submitted", item_ids=submitted, now=now
        )
        # The API list is complete, so every run is a full walk.
        self._wh.retire_hacker_news_user_items(
            account=self._account, relation="submitted", live_item_ids=submitted, now=now
        )
        self._want(submitted)
        self._record(
            "submitted", STATUS_OK, "", success=True, full_walk_completed_at=now, pages_seen=1, items_seen=len(submitted)
        )
        self._summary.lists_walked["submitted"] = len(submitted)

    # --- stage 2: HTML lists ----------------------------------------------------

    def _sync_list(self, list_name: str, *, has_comments: bool, state: Mapping[str, Any] | None) -> None:
        now = self._now()
        relation = RELATION_BY_LIST[list_name]
        last_full = state.get("full_walk_completed_at") if state else None
        full_walk_due = (
            last_full is None
            or last_full <= datetime.fromtimestamp(0, tz=UTC)
            or now - last_full >= self._full_walk_interval
        )
        known = self._wh.hacker_news_user_item_ids(account=self._account, relation=relation, live_only=True)
        seen: list[str] = []
        pages = 0
        complete = True
        halves = (False, True) if has_comments else (False,)
        try:
            for comments in halves:
                half_complete = False
                for page in self._client.iter_list_pages(
                    list_name, user_id=self._account, comments=comments, max_pages=self._max_pages
                ):
                    pages += 1
                    ids = list(page.item_ids)
                    seen.extend(ids)
                    if ids:
                        self._wh.upsert_hacker_news_user_items(
                            account=self._account, relation=relation, item_ids=ids, now=now
                        )
                        self._want(ids)
                    if page.next_cursor is None:
                        half_complete = True
                        break
                    if not full_walk_due and all(i in known for i in ids):
                        # Incremental: newest-first, so a page of nothing new
                        # means everything older is already known too.
                        break
                if full_walk_due and not half_complete:
                    complete = False
        except HackerNewsAuthError as exc:
            self._summary.session_rejected = True
            self._summary.lists_failed[list_name] = str(exc)
            if self._token_sha:
                self._wh.mark_hacker_news_session_expired(
                    account=self._account, session_key=self._session_key, token_sha256=self._token_sha
                )
            self._record(
                list_name,
                STATUS_ACTION_REQUIRED,
                f"{exc}; {PUBLISH_SESSION_HINT}",
                success=False,
                credential_sha256=self._token_sha,
            )
            return
        except HackerNewsRateLimitError:
            # Keep what was written; the run ends after recording partial progress.
            self._record(list_name, STATUS_OK, "throttled mid-walk; resuming next run", success=True, pages_seen=pages, items_seen=len(seen))
            raise
        except HackerNewsError as exc:
            self._summary.lists_failed[list_name] = str(exc)
            self._record(list_name, STATUS_ERROR, str(exc), success=False, pages_seen=pages, items_seen=len(seen))
            return

        full_walk_completed_at = None
        if full_walk_due and complete:
            self._wh.retire_hacker_news_user_items(
                account=self._account, relation=relation, live_item_ids=seen, now=now
            )
            full_walk_completed_at = now
        if list_name in PRIVATE_LISTS and self._token_sha:
            self._wh.record_hacker_news_session_success(
                account=self._account, session_key=self._session_key, token_sha256=self._token_sha
            )
        self._record(
            list_name,
            STATUS_OK,
            "" if (complete or not full_walk_due) else "full walk cut short by the page budget; retiring deferred",
            success=True,
            full_walk_completed_at=full_walk_completed_at,
            pages_seen=pages,
            items_seen=len(seen),
        )
        self._summary.lists_walked[list_name] = len(seen)

    # --- stage 3: items ---------------------------------------------------------

    def _want(self, ids: Iterable[str]) -> None:
        self._wanted.extend(str(i) for i in ids)

    def _fetch_wanted(self) -> None:
        if not self._wanted:
            return
        unique = list(dict.fromkeys(self._wanted))
        known = self._wh.hacker_news_known_item_ids(account=self._account, item_ids=unique)
        missing = [i for i in unique if i not in known]
        self._wanted = []
        for item_id in missing:
            if not self._budget_left():
                return
            self._fetch_with_ancestors(item_id)

    def _walk_frontier(self) -> None:
        while self._budget_left():
            frontier = self._wh.hacker_news_walk_frontier(
                account=self._account, limit=min(500, self._max_fetches - self._fetches)
            )
            if not frontier:
                self._summary.frontier_remaining = 0
                return
            for item_id, root_story_id in frontier:
                if not self._budget_left():
                    break
                self._fetch_one(item_id, root_story_id=root_story_id)
        if self._budget_left():
            return
        self._summary.budget_exhausted = True
        remaining = self._wh.hacker_news_walk_frontier(account=self._account, limit=1)
        self._summary.frontier_remaining = len(remaining)

    def _refresh_live(self) -> None:
        if not self._budget_left():
            return
        due = self._wh.hacker_news_items_due_for_refresh(
            account=self._account,
            now=self._now(),
            live_window=self._live_window,
            min_age=self._refresh_min_age,
            limit=self._max_fetches - self._fetches,
        )
        roots = self._wh.hacker_news_item_roots(account=self._account, item_ids=due)
        for item_id in due:
            if not self._budget_left():
                self._summary.budget_exhausted = True
                return
            record = self._fetch_one(item_id, root_story_id=roots.get(item_id, ""))
            if record is not None:
                self._summary.items_refreshed += 1

    def _budget_left(self) -> bool:
        return self._fetches < self._max_fetches

    def _fetch_with_ancestors(self, item_id: str) -> None:
        """Fetch an item and, for a comment, its parents up to the story.

        The chain is fetched top-down so every row is written with its real
        root_story_id; a comment written before its parent would have to guess.
        """
        chain: list[ItemRecord] = []
        current = item_id
        known_root = ""
        while current and self._budget_left():
            roots = self._wh.hacker_news_item_roots(account=self._account, item_ids=[current])
            if current in roots:
                known_root = roots[current]
                break
            self._fetches += 1
            record = self._client.get_item(current)
            if record is None:
                break
            chain.append(record)
            if record.item_type in STORY_TYPES or not record.parent_id:
                known_root = record.item_id
                break
            current = record.parent_id
        if not chain:
            return
        if not known_root:
            # Budget ran out mid-chain: hang what we have off the topmost
            # fetched item so nothing is written with an empty root, and the
            # next run's frontier/ancestor pass corrects it via the parent.
            known_root = chain[-1].item_id
        now = self._now()
        rows = [self._row(record, root_story_id=known_root, now=now) for record in reversed(chain)]
        self._wh.insert_hacker_news_items(rows)
        self._summary.items_fetched += len(rows)

    def _fetch_one(self, item_id: str, *, root_story_id: str) -> ItemRecord | None:
        self._fetches += 1
        record = self._client.get_item(item_id)
        if record is None:
            return None
        root = root_story_id
        if record.item_type in STORY_TYPES or not record.parent_id:
            root = record.item_id
        if not root:
            roots = self._wh.hacker_news_item_roots(account=self._account, item_ids=[record.parent_id])
            root = roots.get(record.parent_id, record.parent_id)
        self._wh.insert_hacker_news_items([self._row(record, root_story_id=root, now=self._now())])
        self._summary.items_fetched += 1
        return record

    def _row(self, record: ItemRecord, *, root_story_id: str, now: datetime) -> dict[str, Any]:
        return {
            "account": self._account,
            "item_id": record.item_id,
            "item_type": record.item_type,
            "author": record.author,
            "posted_at": record.posted_at,
            "title": record.title,
            "url": record.url,
            "text": record.text,
            "body_text": record.body_text,
            "parent_id": record.parent_id,
            "root_story_id": root_story_id,
            "score": record.score,
            "descendants": record.descendants,
            "is_dead": record.is_dead,
            "is_deleted": record.is_deleted,
            "kids_json": list(record.kids),
            "raw_json": dict(record.raw),
            "fetched_at": now,
            "first_seen_at": now,
            "synced_at": now,
            "sync_version": int(now.timestamp() * 1_000_000),
        }

    # --- state ------------------------------------------------------------------

    def _record(
        self,
        list_name: str,
        status: str,
        error: str,
        *,
        success: bool,
        full_walk_completed_at: datetime | None = None,
        pages_seen: int | None = None,
        items_seen: int | None = None,
        credential_sha256: str = "",
    ) -> None:
        self._wh.record_hacker_news_sync_state(
            account=self._account,
            list_name=list_name,
            status=status,
            error=error,
            now=self._now(),
            success=success,
            full_walk_completed_at=full_walk_completed_at,
            pages_seen=pages_seen,
            items_seen=items_seen,
            credential_sha256=credential_sha256,
        )


def public_summary(summary: HackerNewsSyncSummary) -> dict[str, Any]:
    return {
        "account": summary.account,
        "lists_walked": dict(summary.lists_walked),
        "lists_failed": dict(summary.lists_failed),
        "items_fetched": summary.items_fetched,
        "items_refreshed": summary.items_refreshed,
        "frontier_remaining": summary.frontier_remaining,
        "rate_limited": summary.rate_limited,
        "session_rejected": summary.session_rejected,
        "budget_exhausted": summary.budget_exhausted,
    }
