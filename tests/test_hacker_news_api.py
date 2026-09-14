"""The HN API normalizer and the HTML list parser.

The HTML fixtures are trimmed copies of live news.ycombinator.com pages
fetched 2026-09-13: the row and "More" markup is the whole grammar.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from personal_data_warehouse.hacker_news_api import (
    HackerNewsAuthError,
    HackerNewsClient,
    HackerNewsRateLimitError,
    html_to_text,
    item_record,
    parse_list_page,
)

STORY_PAGE = """<html lang="en" op="submitted"><head><title>zachlatta's submissions | Hacker News</title></head>
<body><center><table id="hnmain"><tr><td><table border="0" class="itemlist">
<tr class="athing submission" id="48347649"><td align="right" valign="top" class="title"><span class="rank">1.</span></td><td valign="top" class="votelinks"><center><a id='up_48347649' href='vote?id=48347649&amp;how=up&amp;goto=submitted%3Fid%3Dzachlatta'><div class='votearrow' title='upvote'></div></a></center></td><td class="title"><span class="titleline"><a href="https://example.test/a" rel="nofollow">A story</a></span></td></tr>
<tr><td colspan="2"></td><td class="subtext"><span class="score" id="score_48347649">3 points</span></td></tr>
<tr class="spacer" style="height:5px"></tr>
<tr class="athing submission" id="47712716"><td align="right" valign="top" class="title"><span class="rank">2.</span></td><td class="title"><span class="titleline"><a href="https://example.test/b">Another</a></span></td></tr>
<tr class="morespace" style="height:10px"></tr><tr><td colspan="2"></td><td class='title'><a href='submitted?id=zachlatta&amp;next=35060359&amp;n=31' class='morelink' rel='next'>More</a></td></tr>
</table></td></tr></table></center></body></html>"""

COMMENT_PAGE = """<html lang="en" op="threads"><head><title>zachlatta's comments | Hacker News</title></head><body>
<tr class="athing comtr" id="49633635"><td><table border="0"><tr><td class="ind" indent="0"></td><td class="default"><span class="comhead"><a href="user?id=zachlatta" class="hnuser">zachlatta</a></span></td></tr></table></td></tr>
<tr class="athing comtr" id="49633793"><td></td></tr>
<a href='threads?id=zachlatta&amp;next=47712716' class='morelink' rel='next'>More</a>
</body></html>"""

LAST_PAGE = """<html><body><tr class="athing submission" id="5445826"><td></td></tr></body></html>"""

LOGIN_PAGE = """<html lang="en"><head><meta name="referrer" content="origin"><meta name="viewport" content="width=device-width, initial-scale=1.0"><link rel="icon" href="y18.svg"><meta name="robots" content="noindex"></head><body>Please log in.<br><br> <b>Login</b><br><br> <form method="post"><table border="0"><tr><td>username:</td><td><input type="text" name="acct" size="20" autocorrect="off" spellcheck="false" autocapitalize="off" autofocus="true"></td></tr><tr><td>password:</td><td><input type="password" name="pw" size="20"></td></tr></table></form></body></html>"""


def test_story_rows_and_the_more_link_parse() -> None:
    page = parse_list_page(STORY_PAGE, list_name="submitted")
    assert page.item_ids == ("48347649", "47712716")
    assert page.next_cursor == {"id": "zachlatta", "next": "35060359", "n": "31"}
    assert page.is_comments is False


def test_comment_rows_parse_and_a_cursor_without_n_is_fine() -> None:
    page = parse_list_page(COMMENT_PAGE, list_name="threads", is_comments=True)
    assert page.item_ids == ("49633635", "49633793")
    assert page.next_cursor == {"id": "zachlatta", "next": "47712716"}


def test_the_last_page_has_no_cursor() -> None:
    page = parse_list_page(LAST_PAGE, list_name="favorites")
    assert page.item_ids == ("5445826",)
    assert page.next_cursor is None


def test_the_login_form_is_an_auth_error_not_an_empty_list() -> None:
    with pytest.raises(HackerNewsAuthError, match="/upvoted"):
        parse_list_page(LOGIN_PAGE, list_name="upvoted")


def test_html_bodies_decode_to_plain_text() -> None:
    body = 'first para<p>second &amp; <i>third</i> <a href="https://x.test">https://x.test</a>&#x27;s'
    assert html_to_text(body) == "first para\n\nsecond & third https://x.test's"
    assert html_to_text("") == ""


def test_item_record_normalizes_the_api_document() -> None:
    record = item_record(
        {
            "id": 45941596,
            "type": "story",
            "by": "somebody",
            "time": 1763424000,
            "title": "Show HN: thing",
            "url": "https://example.test",
            "score": 462,
            "descendants": 12,
            "kids": [45941605, 45943174],
        }
    )
    assert record is not None
    assert record.item_id == "45941596"
    assert record.posted_at == datetime.fromtimestamp(1763424000, tz=UTC)
    assert record.kids == ("45941605", "45943174")
    assert record.parent_id == ""
    assert record.is_dead == 0 and record.is_deleted == 0
    assert record.body_text == ""


def test_item_record_marks_dead_and_deleted_and_keeps_raw() -> None:
    record = item_record({"id": 7, "type": "comment", "parent": 6, "dead": True, "deleted": True, "text": "<p>x"})
    assert record is not None
    assert (record.is_dead, record.is_deleted, record.parent_id) == (1, 1, "6")
    assert record.body_text == "x"
    assert record.raw["dead"] is True


def test_an_item_that_never_existed_is_none() -> None:
    assert item_record(None) is None
    assert item_record({}) is None


class FakeResponse:
    def __init__(self, status_code=200, text="", payload=None, url="https://x"):
        self.status_code = status_code
        self.text = text
        self._payload = payload
        self.url = url

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {}), "headers": dict(headers or {})})
        return self.responses.pop(0)


def test_private_lists_send_the_cookie_and_follow_the_cursor() -> None:
    session = FakeSession([FakeResponse(text=STORY_PAGE), FakeResponse(text=LAST_PAGE)])
    client = HackerNewsClient(session_cookie="user=zachlatta&tok", session=session, sleep=lambda _s: None)
    pages = list(client.iter_list_pages("upvoted", user_id="zachlatta", max_pages=5))
    assert [p.item_ids for p in pages] == [("48347649", "47712716"), ("5445826",)]
    assert session.calls[0]["headers"]["Cookie"] == "user=zachlatta&tok"
    assert session.calls[0]["params"] == {"id": "zachlatta"}
    assert session.calls[1]["params"] == {"id": "zachlatta", "next": "35060359", "n": "31"}
    assert session.headers["User-Agent"].startswith("personal-data-warehouse")


def test_the_comments_half_keeps_its_flag_across_pages() -> None:
    session = FakeSession([FakeResponse(text=COMMENT_PAGE), FakeResponse(text=LAST_PAGE)])
    client = HackerNewsClient(session=session, sleep=lambda _s: None)
    list(client.iter_list_pages("favorites", user_id="zachlatta", comments=True, max_pages=5))
    assert session.calls[0]["params"]["comments"] == "t"
    assert session.calls[1]["params"]["comments"] == "t"
    assert "Cookie" not in session.calls[0]["headers"]


def test_a_private_list_without_a_cookie_is_an_auth_error_before_any_request() -> None:
    session = FakeSession([])
    client = HackerNewsClient(session=session)
    with pytest.raises(HackerNewsAuthError):
        list(client.iter_list_pages("hidden", user_id="zachlatta", max_pages=1))
    assert session.calls == []


def test_max_pages_bounds_the_walk() -> None:
    session = FakeSession([FakeResponse(text=STORY_PAGE), FakeResponse(text=STORY_PAGE)])
    client = HackerNewsClient(session=session, sleep=lambda _s: None)
    pages = list(client.iter_list_pages("favorites", user_id="zachlatta", max_pages=2))
    assert len(pages) == 2
    assert pages[-1].next_cursor is not None  # there was more; the caller knows the walk is partial


def test_throttling_is_its_own_error() -> None:
    session = FakeSession([FakeResponse(status_code=429)])
    client = HackerNewsClient(session=session)
    with pytest.raises(HackerNewsRateLimitError):
        client.get_item("1")


def test_check_login_reads_the_first_private_page() -> None:
    ok = HackerNewsClient(session_cookie="user=a&b", session=FakeSession([FakeResponse(text=STORY_PAGE)]))
    assert ok.check_login(user_id="a") is True
    dead = HackerNewsClient(session_cookie="user=a&b", session=FakeSession([FakeResponse(text=LOGIN_PAGE)]))
    assert dead.check_login(user_id="a") is False
