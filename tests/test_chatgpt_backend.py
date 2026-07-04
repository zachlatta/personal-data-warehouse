from __future__ import annotations

import base64
import json

import pytest

from personal_data_warehouse.chatgpt_backend import (
    ChatGPTAuthError,
    ChatGPTBackendClient,
    ChatGPTBackendError,
    ChatGPTRateLimitError,
)


def _jwt(exp: float) -> str:
    """Build a JWT-shaped access token whose payload carries ``exp``."""

    def _b64(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    return f"{_b64({'alg': 'RS256'})}.{_b64({'exp': exp})}.sig"


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}

    def json(self):
        if self._payload is _NOT_JSON:
            raise ValueError("not json")
        return self._payload


_NOT_JSON = object()


class FakeSession:
    """Routes GETs by URL path; each path maps to a queue of responses."""

    def __init__(self, routes):
        self.routes = {path: list(responses) for path, responses in routes.items()}
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append((url, headers or {}))
        path = url.split("chatgpt.com", 1)[-1]
        # Match by path prefix up to the query string for conversations listing.
        for key, queue in self.routes.items():
            if path == key or path.startswith(key):
                if not queue:
                    return FakeResponse(200, {"items": []})
                return queue.pop(0) if len(queue) > 1 else queue[0]
        raise AssertionError(f"unexpected path {path!r}")


AUTH = "/api/auth/session"


def client(routes, *, credential="session-token-abc"):
    return ChatGPTBackendClient(
        session_credential=credential,
        session=FakeSession(routes),
        now=lambda: 1_000.0,
    )


def test_auth_exchange_sets_bearer_on_backend_calls():
    c = client(
        {
            AUTH: [FakeResponse(200, {"accessToken": "tok-123", "expires": "2999-01-01T00:00:00Z"})],
            "/backend-api/conversation/abc": [FakeResponse(200, {"title": "t", "mapping": {}})],
        }
    )
    convo = c.get_conversation("abc")
    assert convo["conversation_id"] == "abc"
    # The backend call carried the bearer token.
    backend_call = [call for call in c._session.calls if "/backend-api/" in call[0]][0]
    assert backend_call[1]["Authorization"] == "Bearer tok-123"


def test_logged_out_session_raises_auth_error():
    c = client({AUTH: [FakeResponse(200, {})]})
    with pytest.raises(ChatGPTAuthError):
        c.get_conversation("abc")


def test_auth_401_raises_auth_error():
    c = client({AUTH: [FakeResponse(401, {})]})
    with pytest.raises(ChatGPTAuthError):
        list(c.iter_conversation_refs())


def test_backend_403_raises_auth_error():
    c = client(
        {
            AUTH: [FakeResponse(200, {"accessToken": "tok", "expires": "2999-01-01T00:00:00Z"})],
            "/backend-api/conversations": [FakeResponse(403, {})],
        }
    )
    with pytest.raises(ChatGPTAuthError):
        list(c.iter_conversation_refs())


def test_backend_500_raises_backend_error():
    c = client(
        {
            AUTH: [FakeResponse(200, {"accessToken": "tok", "expires": "2999-01-01T00:00:00Z"})],
            "/backend-api/conversations": [FakeResponse(500, {})],
        }
    )
    with pytest.raises(ChatGPTBackendError):
        list(c.iter_conversation_refs())


def test_backend_429_raises_rate_limit_error_with_retry_after():
    c = client(
        {
            AUTH: [FakeResponse(200, {"accessToken": "tok", "expires": "2999-01-01T00:00:00Z"})],
            "/backend-api/conversation/abc": [FakeResponse(429, {}, headers={"Retry-After": "12"})],
        }
    )
    with pytest.raises(ChatGPTRateLimitError) as exc_info:
        c.get_conversation("abc")
    assert exc_info.value.retry_after_seconds == 12


def test_iter_conversation_refs_paginates_until_total():
    page1 = {
        "items": [
            {"id": "c1", "title": "one", "create_time": 1.0, "update_time": 10.0},
            {"id": "c2", "title": "two", "create_time": 2.0, "update_time": 9.0},
        ],
        "total": 3,
    }
    page2 = {"items": [{"id": "c3", "title": "three", "create_time": 3.0, "update_time": 8.0}], "total": 3}
    c = client(
        {
            AUTH: [FakeResponse(200, {"accessToken": "tok", "expires": "2999-01-01T00:00:00Z"})],
            "/backend-api/conversations": [FakeResponse(200, page1), FakeResponse(200, page2)],
        }
    )
    refs = list(c.iter_conversation_refs(page_size=2))
    assert [r.id for r in refs] == ["c1", "c2", "c3"]
    assert refs[0].update_time == 10.0


def test_cookie_header_passthrough_for_full_header():
    c = client({AUTH: [FakeResponse(200, {"accessToken": "tok", "expires": "2999-01-01T00:00:00Z"})]},
               credential="__Secure-next-auth.session-token=xyz; cf_clearance=cf123")
    c._ensure_access_token()
    auth_call = c._session.calls[0]
    assert auth_call[1]["Cookie"] == "__Secure-next-auth.session-token=xyz; cf_clearance=cf123"


def test_bare_token_is_wrapped_into_cookie():
    c = client({AUTH: [FakeResponse(200, {"accessToken": "tok", "expires": "2999-01-01T00:00:00Z"})]})
    c._ensure_access_token()
    assert c._session.calls[0][1]["Cookie"] == "__Secure-next-auth.session-token=session-token-abc"


def test_missing_credential_raises():
    with pytest.raises(ChatGPTAuthError):
        ChatGPTBackendClient(session_credential="", session=FakeSession({}))


def test_expired_jwt_access_token_raises_auth_error():
    # /api/auth/session can return 200 with a *cached, already-expired* access
    # token when the browser session can no longer refresh it (the JWT lasts
    # ~10 days; the session cookie ~90). Treat that as an auth failure rather
    # than sending a dead bearer and getting an opaque backend 401.
    c = client({AUTH: [FakeResponse(200, {"accessToken": _jwt(500.0), "expires": "2999-01-01T00:00:00Z"})]})
    with pytest.raises(ChatGPTAuthError):
        c.fetch_auth_session()


def test_fresh_jwt_access_token_expiry_tracks_jwt_not_session():
    # A healthy session: the JWT exp (9000) is authoritative for the bearer, so
    # cache freshness keys off it, not the far-future session ``expires``.
    c = client(
        {AUTH: [FakeResponse(200, {"accessToken": _jwt(9000.0), "expires": "2999-01-01T00:00:00Z"})]}
    )
    c.fetch_auth_session()
    assert c._access_expiry == 9000.0


def test_opaque_access_token_falls_back_to_session_expiry():
    # A non-JWT (opaque) token can't be introspected, so we fall back to the
    # session ``expires`` and accept the token as before.
    c = client({AUTH: [FakeResponse(200, {"accessToken": "opaque-tok", "expires": "2999-01-01T00:00:00Z"})]})
    c.fetch_auth_session()
    assert c._access_token == "opaque-tok"
    assert c._access_expiry > 1_000.0
