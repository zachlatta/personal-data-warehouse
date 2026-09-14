"""Hacker News clients: the public Firebase item API and the HTML list pages.

Two very different surfaces, deliberately kept in one module because the sync
needs both and the boundary between them is what the source is about:

* **Items** come from the official API (``hacker-news.firebaseio.com/v0``):
  one JSON document per id, no auth, no rate limit worth planning around
  (~120 ms a call measured 2026-09-13). Everything the warehouse archives is
  an item fetched this way, so the archive is byte-faithful to the API.
* **Lists** come from ``news.ycombinator.com`` HTML, because the API has no
  endpoint for "what did this user upvote / favorite / hide". ``submitted``
  and ``favorites`` are public; ``upvoted`` and ``hidden`` exist only for the
  logged-in user, so those need the browser's ``user`` cookie, published by
  ``pdw hn publish-session``. A page that answers with a login form is an
  auth failure and raises :class:`HackerNewsAuthError`; it is never an empty
  list, because "nothing here" and "we could not look" must not look alike.

The HTML parser is regex over a stable, tiny grammar (``<tr class="athing
submission" id="N">`` / ``<tr class="athing comtr" id="N">`` rows and one
``class='morelink'`` anchor), verified against live pages the day it was
written; the fixtures in tests/test_hacker_news_api.py are copies of those.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
import html
import json
import re
import time
from typing import Any
from urllib.parse import parse_qs, urlsplit

import requests

DEFAULT_API_BASE_URL = "https://hacker-news.firebaseio.com/v0"
DEFAULT_SITE_BASE_URL = "https://news.ycombinator.com"
#: Cloudflare in front of the PDW app rejects Python's default agent; HN does
#: not, but a descriptive agent is the polite default for both surfaces.
DEFAULT_USER_AGENT = "personal-data-warehouse hacker_news_sync (+https://github.com/zachlatta/personal-data-warehouse)"
DEFAULT_TIMEOUT_SECONDS = 30
#: Rows per HTML list page, fixed by HN.
LIST_PAGE_SIZE = 30
#: The login-only lists. Everything else is public.
PRIVATE_LISTS = ("upvoted", "hidden")
PUBLIC_LISTS = ("favorites",)

_ROW_RE = re.compile(r'<tr class="athing (submission|comtr)" id="(\d+)">')
_MORE_RE = re.compile(r"""<a href=['"]([^'"]+)['"][^>]*class=['"]morelink['"]""")
_TAG_RE = re.compile(r"<[^>]+>")


class HackerNewsError(RuntimeError):
    """The site or API answered in a way the sync cannot use."""


class HackerNewsAuthError(HackerNewsError):
    """A login-only page answered with the login form: the stored cookie is dead."""


class HackerNewsRateLimitError(HackerNewsError):
    """HN throttled the request (HTTP 429/503); back off rather than fail the run."""


@dataclass(frozen=True)
class ListPage:
    """One HTML list page: the item ids in page order and the link to the next page."""

    list_name: str
    item_ids: tuple[str, ...]
    next_cursor: Mapping[str, str] | None
    is_comments: bool = False


@dataclass(frozen=True)
class ItemRecord:
    """One API item, normalized to the warehouse row shape."""

    item_id: str
    item_type: str
    author: str
    posted_at: datetime
    title: str
    url: str
    text: str
    body_text: str
    parent_id: str
    score: int
    descendants: int
    is_dead: int
    is_deleted: int
    kids: tuple[str, ...]
    raw: Mapping[str, Any] = field(default_factory=dict)


def html_to_text(fragment: str) -> str:
    """Decode an HN body fragment (``<p>``, ``<a>``, ``<i>``, entities) to plain text.

    HN separates paragraphs with ``<p>`` and never closes them; links carry
    the URL as both href and text, so dropping tags loses nothing.
    """
    if not fragment:
        return ""
    text = fragment.replace("<p>", "\n\n").replace("</p>", "")
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = _TAG_RE.sub("", text)
    text = html.unescape(text)
    return "\n".join(line.rstrip() for line in text.strip().splitlines()).strip()


def parse_list_page(body: str, *, list_name: str, is_comments: bool = False) -> ListPage:
    """Parse one list page, raising :class:`HackerNewsAuthError` on the login form."""
    if _looks_like_login_page(body):
        raise HackerNewsAuthError(
            f"news.ycombinator.com answered the login page for /{list_name}; the stored session is dead"
        )
    ids = tuple(item_id for _kind, item_id in _ROW_RE.findall(body))
    more = _MORE_RE.search(body)
    cursor: dict[str, str] | None = None
    if more:
        href = html.unescape(more.group(1))
        query = parse_qs(urlsplit(href).query)
        cursor = {key: values[0] for key, values in query.items() if values}
    return ListPage(list_name=list_name, item_ids=ids, next_cursor=cursor, is_comments=is_comments)


def _looks_like_login_page(body: str) -> bool:
    head = body[:2000]
    return "Please log in" in head or ('<form method="post"' in head and "acct" in head and "pw" in head)


def item_record(payload: Mapping[str, Any]) -> ItemRecord | None:
    """Normalize an API item; ``None`` for the API's ``null`` (an id that never existed)."""
    if not payload or not isinstance(payload, Mapping):
        return None
    item_id = str(payload.get("id") or "")
    if not item_id:
        return None
    posted = int(payload.get("time") or 0)
    text = str(payload.get("text") or "")
    return ItemRecord(
        item_id=item_id,
        item_type=str(payload.get("type") or "unknown"),
        author=str(payload.get("by") or ""),
        posted_at=datetime.fromtimestamp(posted, tz=UTC) if posted > 0 else datetime.fromtimestamp(0, tz=UTC),
        title=str(payload.get("title") or ""),
        url=str(payload.get("url") or ""),
        text=text,
        body_text=html_to_text(text),
        parent_id=str(payload.get("parent") or ""),
        score=int(payload.get("score") or 0),
        descendants=int(payload.get("descendants") or 0),
        is_dead=1 if payload.get("dead") else 0,
        is_deleted=1 if payload.get("deleted") else 0,
        kids=tuple(str(kid) for kid in (payload.get("kids") or [])),
        raw=dict(payload),
    )


class HackerNewsClient:
    """Both surfaces behind one object, so the runner has one thing to fake."""

    def __init__(
        self,
        *,
        session_cookie: str = "",
        api_base_url: str = DEFAULT_API_BASE_URL,
        site_base_url: str = DEFAULT_SITE_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        user_agent: str = DEFAULT_USER_AGENT,
        page_delay_seconds: float = 0.5,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._cookie = session_cookie.strip()
        self._api = api_base_url.rstrip("/")
        self._site = site_base_url.rstrip("/")
        self._timeout = timeout
        self._delay = page_delay_seconds
        self._sleep = sleep
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": user_agent})

    # --- items (public API) --------------------------------------------------

    def get_item(self, item_id: str) -> ItemRecord | None:
        response = self._session.get(f"{self._api}/item/{item_id}.json", timeout=self._timeout)
        _raise_for_throttle(response)
        if response.status_code != 200:
            raise HackerNewsError(f"item {item_id}: HTTP {response.status_code}")
        return item_record(response.json())

    def get_user(self, user_id: str) -> Mapping[str, Any] | None:
        response = self._session.get(f"{self._api}/user/{user_id}.json", timeout=self._timeout)
        _raise_for_throttle(response)
        if response.status_code != 200:
            raise HackerNewsError(f"user {user_id}: HTTP {response.status_code}")
        payload = response.json()
        return dict(payload) if isinstance(payload, Mapping) else None

    # --- lists (HTML) --------------------------------------------------------

    def iter_list_pages(
        self,
        list_name: str,
        *,
        user_id: str,
        comments: bool = False,
        max_pages: int,
    ) -> Iterable[ListPage]:
        """Walk a list newest-first, one page per yield, at most ``max_pages`` pages.

        The private lists send the cookie; a page that answers with the login
        form raises :class:`HackerNewsAuthError` before anything is yielded
        for it. ``comments=True`` asks for the list's comment half
        (``&comments=t``), which HN paginates separately.
        """
        if list_name in PRIVATE_LISTS and not self._cookie:
            raise HackerNewsAuthError(f"/{list_name} needs a published news.ycombinator.com session")
        params: dict[str, str] = {"id": user_id}
        if comments:
            params["comments"] = "t"
        for page_index in range(max(int(max_pages), 0)):
            if page_index and self._delay > 0:
                self._sleep(self._delay)
            page = self._fetch_list_page(list_name, params, comments=comments)
            yield page
            if not page.next_cursor:
                return
            params = {"id": user_id, **page.next_cursor}
            if comments:
                params["comments"] = "t"

    def _fetch_list_page(self, list_name: str, params: Mapping[str, str], *, comments: bool) -> ListPage:
        headers = {"Cookie": self._cookie} if self._cookie else {}
        response = self._session.get(
            f"{self._site}/{list_name}", params=dict(params), headers=headers, timeout=self._timeout
        )
        _raise_for_throttle(response)
        if response.status_code != 200:
            raise HackerNewsError(f"/{list_name}: HTTP {response.status_code}")
        return parse_list_page(response.text, list_name=list_name, is_comments=comments)

    def check_login(self, *, user_id: str) -> bool:
        """True when the cookie is accepted by a login-only page."""
        try:
            next(iter(self.iter_list_pages("upvoted", user_id=user_id, max_pages=1)))
        except HackerNewsAuthError:
            return False
        return True


def _raise_for_throttle(response: requests.Response) -> None:
    if response.status_code in (429, 503):
        raise HackerNewsRateLimitError(f"HTTP {response.status_code} from {response.url}")


def public_item_json(record: ItemRecord) -> str:
    """The API document as stored: canonical key order, no secrets to redact."""
    return json.dumps(record.raw, sort_keys=True, separators=(",", ":"))
