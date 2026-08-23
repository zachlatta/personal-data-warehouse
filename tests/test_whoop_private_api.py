"""Unit tests for the WHOOP private-API client.

The private API is undocumented, so several of these tests exist to pin down
behaviour that was expensive to discover and is easy to "simplify" back into a
broken state. Each such test says which discovery it protects.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
import json

import pytest

from personal_data_warehouse.whoop_private_api import (
    WHOOP_PRIVATE_VALID_STEPS,
    WhoopPrivateApiError,
    WhoopPrivateAuthError,
    WhoopPrivateClient,
    WhoopPrivateRateLimitedError,
    WhoopPrivateSession,
)

NOW = datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC)


class FakeResponse:
    def __init__(self, status_code: int, payload: Any = None, *, headers: dict[str, str] | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {"content-type": "application/json"}
        self.text = text or json.dumps(payload if payload is not None else {})

    def json(self) -> Any:
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class FakeHttp:
    """Records requests and replays queued responses."""

    def __init__(self, responses: list[FakeResponse]):
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        if not self._responses:
            raise AssertionError(f"unexpected extra request: {method} {url}")
        return self._responses.pop(0)


def make_session(*, access_expires_in: int = 86_400) -> WhoopPrivateSession:
    return WhoopPrivateSession(
        account="zach@example.com",
        access_token="access-1",
        refresh_token="refresh-1",
        access_expires_at=NOW + timedelta(seconds=access_expires_in),
        refresh_expires_at=NOW + timedelta(days=30),
    )


def make_client(responses: list[FakeResponse], **kwargs: Any) -> tuple[WhoopPrivateClient, FakeHttp]:
    http = FakeHttp(responses)
    client = WhoopPrivateClient(
        session=kwargs.pop("session", make_session()),
        http=http,
        now=lambda: kwargs.pop("now_value", NOW),
        **kwargs,
    )
    return client, http


REFRESH_PAYLOAD = {
    "access_token": "access-2",
    "access_token_expires_in": 86_400,
    "refresh_token": "refresh-2",
    "refresh_token_expires_in": 2_592_000,
}


def test_refresh_sends_the_refresh_token_as_the_bearer_with_an_empty_body() -> None:
    """The discovery that made unattended sync possible.

    Passing the refresh token in the body (the obvious shape) returns 401. It
    must go in the Authorization header, and the body must be empty.
    """
    client, http = make_client([FakeResponse(200, REFRESH_PAYLOAD)])

    client.refresh()

    call = http.calls[0]
    assert call["method"] == "POST"
    assert call["url"].endswith("/auth-service/v2/whoop/refresh")
    assert call["headers"]["authorization"] == "bearer refresh-1"
    assert call["json"] == {}


def test_refresh_returns_a_rotated_session_with_both_expiries() -> None:
    client, _ = make_client([FakeResponse(200, REFRESH_PAYLOAD)])

    rotated = client.refresh()

    assert rotated.access_token == "access-2"
    assert rotated.refresh_token == "refresh-2"
    assert rotated.access_expires_at == NOW + timedelta(seconds=86_400)
    assert rotated.refresh_expires_at == NOW + timedelta(seconds=2_592_000)
    assert client.session == rotated


def test_a_rejected_refresh_is_an_auth_error_needing_a_new_browser_login() -> None:
    for status in (400, 401, 403):
        client, _ = make_client([FakeResponse(status, {"error": "nope"})])
        with pytest.raises(WhoopPrivateAuthError):
            client.refresh()


def test_requests_carry_a_lowercase_bearer_and_the_api_version() -> None:
    """WHOOP's app sends `bearer`, not `Bearer`, and every call carries apiVersion=7."""
    client, http = make_client([FakeResponse(200, {"ok": True})])

    client.get_json("/users-service/v2/bootstrap/")

    call = http.calls[0]
    assert call["headers"]["authorization"] == "bearer access-1"
    assert call["params"]["apiVersion"] == "7"


def test_an_expired_access_token_refreshes_once_and_retries() -> None:
    client, http = make_client(
        [FakeResponse(401, None, text="unauthorized"), FakeResponse(200, REFRESH_PAYLOAD), FakeResponse(200, {"ok": True})]
    )

    assert client.get_json("/users-service/v2/bootstrap/") == {"ok": True}
    assert [c["url"].rsplit("/", 1)[-1] for c in http.calls][1] == "refresh"
    assert http.calls[2]["headers"]["authorization"] == "bearer access-2"


def test_a_second_401_after_refreshing_stops_rather_than_looping() -> None:
    client, _ = make_client(
        [FakeResponse(401, None, text="unauthorized"), FakeResponse(200, REFRESH_PAYLOAD), FakeResponse(401, None, text="still no")]
    )

    with pytest.raises(WhoopPrivateAuthError):
        client.get_json("/users-service/v2/bootstrap/")


def test_a_near_expiry_token_refreshes_before_the_call_rather_than_after_a_401() -> None:
    client, http = make_client(
        [FakeResponse(200, REFRESH_PAYLOAD), FakeResponse(200, {"ok": True})],
        session=make_session(access_expires_in=30),
    )

    client.get_json("/users-service/v2/bootstrap/")

    assert http.calls[0]["url"].endswith("/auth-service/v2/whoop/refresh")


def test_rate_limiting_surfaces_retry_after() -> None:
    client, _ = make_client([FakeResponse(429, None, headers={"retry-after": "42", "content-type": "text/plain"})])

    with pytest.raises(WhoopPrivateRateLimitedError) as excinfo:
        client.get_json("/users-service/v2/bootstrap/")
    assert excinfo.value.retry_after == 42


def test_heart_rate_rejects_a_step_the_api_does_not_support() -> None:
    """metrics-service accepts only 6, 60 and 600; everything else is a 400.

    Failing client-side turns a silent empty series into a loud programming
    error, and documents the constraint where the caller can see it.
    """
    client, _ = make_client([])

    assert WHOOP_PRIVATE_VALID_STEPS == (6, 60, 600)
    with pytest.raises(ValueError, match="step"):
        client.heart_rate(user_id="1", start=NOW - timedelta(hours=1), end=NOW, step=300)


def test_heart_rate_requests_the_documented_shape() -> None:
    client, http = make_client([FakeResponse(200, {"name": "heart_rate", "start": 0, "values": []})])

    client.heart_rate(user_id="42", start=NOW - timedelta(hours=1), end=NOW, step=60)

    call = http.calls[0]
    assert call["url"].endswith("/metrics-service/v1/metrics/user/42")
    assert call["params"]["name"] == "heart_rate"
    assert call["params"]["step"] == 60
    assert call["params"]["order"] == "t"
    assert call["params"]["start"] == "2026-08-23T11:00:00.000Z"


def test_cycles_details_pages_by_time_range() -> None:
    client, http = make_client([FakeResponse(200, {"records": []})])

    client.cycles_details(user_id="42", start=NOW - timedelta(days=7), end=NOW, limit=10)

    call = http.calls[0]
    assert call["url"].endswith("/core-details-bff/v0/cycles/details")
    assert call["params"]["id"] == "42"
    assert call["params"]["limit"] == 10


def test_sleep_events_and_journal_use_their_discovered_paths() -> None:
    client, http = make_client([FakeResponse(200, []), FakeResponse(200, {"journal": {}})])

    client.sleep_events(activity_id="abc-123")
    client.journal_entries(day="2026-08-23")

    assert http.calls[0]["params"]["activityId"] == "abc-123"
    assert http.calls[0]["url"].endswith("/sleep-service/v1/sleep-events")
    # v3 drafts is the day's entries; the v2 "behaviors" path is only the catalog.
    assert http.calls[1]["url"].endswith("/journal-service/v3/journals/drafts/mobile/2026-08-23")


def test_a_server_error_is_reported_not_swallowed() -> None:
    client, _ = make_client([FakeResponse(500, None, headers={"content-type": "text/plain"}, text="boom")])

    with pytest.raises(WhoopPrivateApiError):
        client.get_json("/users-service/v2/bootstrap/")


def test_bootstrap_extracts_the_user_id_and_timezone_offset() -> None:
    payload = {"profile": {"user_id": 7654321, "timezone_offset": "-04:00"}, "user": {"first_name": "Z"}}
    client, _ = make_client([FakeResponse(200, payload)])

    identity = client.bootstrap()

    assert identity.user_id == "7654321"
    assert identity.timezone_offset == "-04:00"
    assert identity.raw == payload
