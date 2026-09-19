from __future__ import annotations

import hashlib
from urllib.parse import parse_qs, urlsplit

import pytest

from personal_data_warehouse.ingest_client import (
    IngestClient,
    ingest_client_from_env,
    ingest_upload_config_problem,
    resolve_direct_ingest_origin,
    sign_object_upload,
)


# --- signing contract -------------------------------------------------------


def test_known_answer_signature_matches_go() -> None:
    # Pinned to the identical constant asserted in the Go test
    # (app/internal/auth/objectupload_test.go TestObjectUploadKnownAnswer).
    sig = sign_object_upload(
        b"0123456789abcdef0123456789abcdef",
        "/ingest/agent-sessions/batch",
        "abc123",
        1700003600,
    )
    assert sig == "vmGZqtDVzN69EfjqxTokbJxrbFgrzknfRCyTZQVbjxk"


# --- request shaping --------------------------------------------------------


class _FakeResponse:
    def __init__(
        self,
        payload: dict,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def post(self, url, *, data, headers, timeout):
        self.calls.append({"url": url, "data": data, "headers": headers, "timeout": timeout})
        return _FakeResponse(
            {
                "storage_backend": "google_drive",
                "storage_key": "agent-sessions/inbox/batches/x.jsonl.gz",
                "storage_file_id": "fid-1",
                "storage_url": "https://drive/x",
            }
        )


def test_post_signs_body_and_sends_expected_query() -> None:
    session = _FakeSession()
    client = IngestClient(
        base_url="https://app.example.test/",
        signing_key=b"0123456789abcdef0123456789abcdef",
        session=session,
        now=lambda: 1700000000.0,
        link_ttl_seconds=900,
    )
    body = b"gzipped-batch-bytes"
    stored = client.upload_agent_sessions_batch(body, exported_at="2026-06-19T12:34:56+00:00")

    assert stored["storage_file_id"] == "fid-1"
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["data"] == body
    assert call["headers"]["Content-Type"] == "application/gzip"

    parts = urlsplit(call["url"])
    assert parts.path == "/ingest/agent-sessions/batch"
    q = {k: v[0] for k, v in parse_qs(parts.query).items()}
    expected_sha = hashlib.sha256(body).hexdigest()
    assert q["content_sha256"] == expected_sha
    assert q["exported_at"] == "2026-06-19T12:34:56+00:00"
    assert q["exp"] == str(1700000000 + 900)
    # The signature it sent must verify under the same scheme.
    expected_sig = sign_object_upload(
        b"0123456789abcdef0123456789abcdef",
        "/ingest/agent-sessions/batch",
        expected_sha,
        1700000000 + 900,
    )
    assert q["sig"] == expected_sig


class _PhotoResumableSession(_FakeSession):
    def __init__(self, *, content_sha256: str) -> None:
        super().__init__()
        self.content_sha256 = content_sha256
        self.put_calls: list[dict] = []

    def post(self, url, *, data, headers, timeout):
        parts = urlsplit(url)
        if parts.path == "/ingest/photos/file/resumable":
            self.calls.append({"url": url, "data": data, "headers": headers, "timeout": timeout})
            return _FakeResponse(
                {
                    "complete": False,
                    "upload_url": "https://uploads.example.test/session-secret",
                    "storage_backend": "google_drive",
                    "storage_key": "photos/inbox/2026/06/photo.heic",
                    "chunk_size_bytes": 4,
                }
            )
        return super().post(url, data=data, headers=headers, timeout=timeout)

    def put(self, url, *, data, headers, timeout):
        self.put_calls.append({"url": url, "data": data, "headers": headers, "timeout": timeout})
        end = int(headers["Content-Range"].split(" ")[1].split("-")[1].split("/")[0])
        if end < 9:
            return _FakeResponse({}, status_code=308, headers={"Range": f"bytes=0-{end}"})
        return _FakeResponse(
            {
                "id": "fid-photo",
                "webViewLink": "https://drive/photo",
                "sha256Checksum": self.content_sha256,
                "size": "10",
            },
            status_code=200,
        )


class _FakeJSONSession:
    def __init__(self, payload: dict) -> None:
        self.calls: list[dict] = []
        self._payload = payload

    def post(self, url, *, data, headers, timeout):
        self.calls.append({"url": url, "data": data, "headers": headers, "timeout": timeout})
        return _FakeResponse(self._payload)


# --- runner uses the http_app batch uploader --------------------------------


class _FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


# --- ingest_client_from_env: warehouse URL/token resolution -----------------

_INGEST_ENV_VARS = (
    "PDW_API_URL",
    "MCP_BASE_URL",
    "PDW_SECRET_TOKEN",
    "MCP_SECRET_TOKEN",
)


def _clear_ingest_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _INGEST_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_from_env_uses_main_api_url(monkeypatch: pytest.MonkeyPatch) -> None:
    # The uploader's base URL is the warehouse's main API URL; no separate
    # ingest URL is required.
    _clear_ingest_env(monkeypatch)
    monkeypatch.setenv("PDW_API_URL", "https://warehouse.example")
    monkeypatch.setenv("PDW_SECRET_TOKEN", "tok")
    client = ingest_client_from_env()
    assert client._base_url == "https://warehouse.example"
    assert client._signing_key == b"tok"


def test_from_env_accepts_mcp_base_url_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ingest_env(monkeypatch)
    monkeypatch.setenv("MCP_BASE_URL", "https://legacy.example")
    monkeypatch.setenv("PDW_SECRET_TOKEN", "tok")
    client = ingest_client_from_env()
    assert client._base_url == "https://legacy.example"


def test_from_env_requires_a_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ingest_env(monkeypatch)
    monkeypatch.setenv("PDW_SECRET_TOKEN", "tok")
    with pytest.raises(ValueError, match="PDW_API_URL"):
        ingest_client_from_env()


def test_config_problem_none_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ingest_env(monkeypatch)
    monkeypatch.setenv("PDW_API_URL", "https://warehouse.example")
    monkeypatch.setenv("PDW_SECRET_TOKEN", "tok")
    assert ingest_upload_config_problem() is None


def test_config_problem_reports_missing_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ingest_env(monkeypatch)
    monkeypatch.setenv("PDW_SECRET_TOKEN", "tok")
    problem = ingest_upload_config_problem()
    assert problem is not None
    assert "PDW_API_URL" in problem


def test_config_problem_reports_missing_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ingest_env(monkeypatch)
    monkeypatch.setenv("PDW_API_URL", "https://warehouse.example")
    problem = ingest_upload_config_problem()
    assert problem is not None
    assert "PDW_SECRET_TOKEN" in problem


# --- Tailscale-direct upload routing ---------------------------------------


def test_direct_upload_uses_direct_origin_with_public_host_header() -> None:
    session = _FakeSession()
    client = IngestClient(
        base_url="https://pdw.example.com/",
        signing_key=b"0123456789abcdef0123456789abcdef",
        session=session,
        now=lambda: 1700000000.0,
        upload_base_url="http://100.115.245.118",
    )
    body = b"audio-bytes"
    client.upload_agent_sessions_batch(body, exported_at="2026-06-19T12:34:56+00:00")
    call = session.calls[0]
    # Connects to the direct origin...
    assert call["url"].startswith("http://100.115.245.118/ingest/agent-sessions/batch")
    # ...but presents the public Host so Traefik still routes to the app,
    # and the signature (host-independent) is unchanged.
    assert call["headers"]["Host"] == "pdw.example.com"
    parts = urlsplit(call["url"])
    q = {k: v[0] for k, v in parse_qs(parts.query).items()}
    assert q["sig"] == sign_object_upload(
        b"0123456789abcdef0123456789abcdef",
        "/ingest/agent-sessions/batch",
        hashlib.sha256(body).hexdigest(),
        1700000000 + 900,
    )


def test_no_host_header_without_direct_origin() -> None:
    session = _FakeSession()
    client = IngestClient(
        base_url="https://pdw.example.com/",
        signing_key=b"k" * 32,
        session=session,
        now=lambda: 1700000000.0,
    )
    client.upload_agent_sessions_batch(b"x", exported_at="2026-06-19T12:34:56+00:00")
    call = session.calls[0]
    assert call["url"].startswith("https://pdw.example.com/")
    assert "Host" not in call["headers"]


def test_effective_max_upload_bytes_reflects_route() -> None:
    direct = IngestClient(
        base_url="https://pdw.example.com",
        signing_key=b"k" * 32,
        upload_base_url="http://100.64.0.1",
        max_object_bytes=512 * 1024 * 1024,
    )
    # Direct to the app: the app's own cap applies.
    assert direct.effective_max_upload_bytes == 512 * 1024 * 1024
    public = IngestClient(
        base_url="https://pdw.example.com",
        signing_key=b"k" * 32,
        max_object_bytes=512 * 1024 * 1024,
    )
    # Via Cloudflare: clamped to the 100 MiB edge limit.
    assert public.effective_max_upload_bytes == 100 * 1024 * 1024


def test_resolve_direct_prefers_explicit_url_when_reachable() -> None:
    seen = {}

    def probe(url, host):
        seen["url"], seen["host"] = url, host
        return True

    out = resolve_direct_ingest_origin(
        "https://pdw.example.com",
        explicit_direct_url="http://10.0.0.5",
        tailscale_host="rotom",
        ipv4_resolver=lambda h: (_ for _ in ()).throw(AssertionError("should not resolve")),
        probe=probe,
    )
    assert out == "http://10.0.0.5"
    assert seen == {"url": "http://10.0.0.5", "host": "pdw.example.com"}


def test_resolve_direct_uses_tailscale_ip_when_no_explicit() -> None:
    out = resolve_direct_ingest_origin(
        "https://pdw.example.com",
        explicit_direct_url=None,
        tailscale_host="rotom",
        ipv4_resolver=lambda h: "100.115.245.118" if h == "rotom" else None,
        probe=lambda url, host: True,
    )
    assert out == "http://100.115.245.118"


def test_resolve_direct_falls_back_when_unreachable() -> None:
    out = resolve_direct_ingest_origin(
        "https://pdw.example.com",
        explicit_direct_url=None,
        tailscale_host="rotom",
        ipv4_resolver=lambda h: "100.115.245.118",
        probe=lambda url, host: False,
    )
    assert out is None


def test_resolve_direct_none_when_unconfigured() -> None:
    out = resolve_direct_ingest_origin(
        "https://pdw.example.com",
        explicit_direct_url=None,
        tailscale_host=None,
        ipv4_resolver=lambda h: (_ for _ in ()).throw(AssertionError("should not resolve")),
        probe=lambda url, host: (_ for _ in ()).throw(AssertionError("should not probe")),
    )
    assert out is None


def test_tailscale_ipv4_parses_first_ipv4(monkeypatch) -> None:
    import personal_data_warehouse.ingest_client as ic

    monkeypatch.setattr(ic, "_tailscale_binary", lambda: "/usr/bin/tailscale")
    ip = ic._tailscale_ipv4("rotom", runner=lambda argv: "fd7a::1\n100.115.245.118\n")
    assert ip == "100.115.245.118"


def test_tailscale_ipv4_none_when_binary_missing(monkeypatch) -> None:
    import personal_data_warehouse.ingest_client as ic

    monkeypatch.setattr(ic, "_tailscale_binary", lambda: None)
    assert ic._tailscale_ipv4("rotom") is None


# --- size-aware upload timeout ---------------------------------------------


def test_upload_timeout_scales_with_body_size() -> None:
    client = IngestClient(
        base_url="https://pdw.example.com",
        signing_key=b"k" * 32,
        timeout=120.0,
    )
    # Small bodies keep the base timeout...
    assert client._upload_timeout(1024) == 120.0
    # ...large ones get at least ~1 MiB/s of headroom so a multi-hundred-MiB
    # upload to object storage does not trip the fixed 120 s default.
    assert client._upload_timeout(300 * 1024 * 1024) == float(300 * 1024 * 1024) / (1024 * 1024)


def test_upload_timeout_applied_to_post() -> None:
    session = _FakeSession()
    client = IngestClient(
        base_url="https://pdw.example.com",
        signing_key=b"k" * 32,
        session=session,
        now=lambda: 1700000000.0,
        timeout=120.0,
    )
    big = b"x" * (200 * 1024 * 1024)
    client.upload_agent_sessions_batch(big, exported_at="2026-06-19T12:34:56+00:00")
    assert session.calls[0]["timeout"] == 200.0


# --- transient upload retries ----------------------------------------------


class _HTTPStatusResponse:
    """Response whose raise_for_status raises like requests does for a status."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def raise_for_status(self) -> None:
        import requests

        raise requests.HTTPError(f"{self.status_code} Server Error", response=self)

    def json(self) -> dict:
        return {}


class _ScriptedSession:
    """Session that replays a scripted sequence of outcomes."""

    def __init__(self, outcomes: list) -> None:
        self._outcomes = list(outcomes)
        self.calls: list[dict] = []

    def post(self, url, *, data, headers, timeout):
        self.calls.append({"url": url, "data": data, "headers": headers, "timeout": timeout})
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _retry_client(session) -> IngestClient:
    return IngestClient(
        base_url="https://pdw.example.com",
        signing_key=b"k" * 32,
        session=session,
        now=lambda: 1700000000.0,
    )


def test_upload_retries_proxy_499_and_succeeds(monkeypatch) -> None:
    monkeypatch.setattr("personal_data_warehouse.ingest_client.time.sleep", lambda _s: None)
    ok = _FakeResponse(
        {
            "storage_backend": "google_drive",
            "storage_key": "apple-photos/inbox/files/x.mov",
            "storage_file_id": "fid-1",
            "storage_url": "https://drive/x",
        }
    )
    session = _ScriptedSession([_HTTPStatusResponse(499), ok])
    stored = _retry_client(session).upload_agent_sessions_batch(
        b"batch",
        exported_at="2026-07-22T12:00:00+00:00",
    )

    assert stored["storage_file_id"] == "fid-1"
    assert len(session.calls) == 2


def test_upload_retries_dropped_connection(monkeypatch) -> None:
    import requests

    monkeypatch.setattr("personal_data_warehouse.ingest_client.time.sleep", lambda _s: None)
    ok = _FakeResponse(
        {"storage_backend": "google_drive", "storage_key": "k", "storage_file_id": "f", "storage_url": "u"}
    )
    session = _ScriptedSession([requests.ConnectionError("tailnet reset"), ok])

    stored = _retry_client(session).upload_agent_sessions_batch(
        b"batch", exported_at="2026-07-22T12:00:00+00:00"
    )

    assert stored["storage_file_id"] == "f"
    assert len(session.calls) == 2


def test_upload_does_not_retry_client_rejections(monkeypatch) -> None:
    import requests

    monkeypatch.setattr("personal_data_warehouse.ingest_client.time.sleep", lambda _s: None)
    # 413 means the body is over the route's cap: retrying cannot help and the
    # caller (e.g. voice memos) needs the error to defer the file instead.
    session = _ScriptedSession([_HTTPStatusResponse(413)])

    with pytest.raises(requests.HTTPError):
        _retry_client(session).upload_agent_sessions_batch(
            b"batch", exported_at="2026-07-22T12:00:00+00:00"
        )

    assert len(session.calls) == 1


def test_upload_gives_up_after_the_attempt_budget(monkeypatch) -> None:
    import requests

    from personal_data_warehouse.ingest_client import UPLOAD_RETRY_ATTEMPTS

    monkeypatch.setattr("personal_data_warehouse.ingest_client.time.sleep", lambda _s: None)
    session = _ScriptedSession([_HTTPStatusResponse(502)] * UPLOAD_RETRY_ATTEMPTS)

    with pytest.raises(requests.HTTPError):
        _retry_client(session).upload_agent_sessions_batch(
            b"batch", exported_at="2026-07-22T12:00:00+00:00"
        )

    assert len(session.calls) == UPLOAD_RETRY_ATTEMPTS


def test_upload_retry_resigns_each_attempt(monkeypatch) -> None:
    """Each attempt must carry a fresh expiry, or a slow retry ships a dead signature."""
    monkeypatch.setattr("personal_data_warehouse.ingest_client.time.sleep", lambda _s: None)
    clock = iter([1700000000.0, 1700000900.0])
    ok = _FakeResponse(
        {"storage_backend": "google_drive", "storage_key": "k", "storage_file_id": "f", "storage_url": "u"}
    )
    session = _ScriptedSession([_HTTPStatusResponse(503), ok])
    client = IngestClient(
        base_url="https://pdw.example.com",
        signing_key=b"k" * 32,
        session=session,
        now=lambda: next(clock),
        link_ttl_seconds=900,
    )

    client.upload_agent_sessions_batch(b"batch", exported_at="2026-07-22T12:00:00+00:00")

    exps = [
        {k: v[0] for k, v in parse_qs(urlsplit(call["url"]).query).items()}["exp"]
        for call in session.calls
    ]
    assert exps == [str(1700000000 + 900), str(1700000900 + 900)]
