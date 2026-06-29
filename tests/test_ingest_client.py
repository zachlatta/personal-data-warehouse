from __future__ import annotations

from datetime import UTC, datetime
import gzip
import hashlib
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from personal_data_warehouse.ingest_client import (
    IngestClient,
    ingest_client_from_env,
    ingest_upload_config_problem,
    resolve_direct_ingest_origin,
    sign_object_upload,
)
from personal_data_warehouse_agent_sessions.state import AgentSessionsUploadState
from personal_data_warehouse_agent_sessions.sync import AgentSessionsUploadRunner


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
    def __init__(self, payload: dict) -> None:
        self._payload = payload

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


class _FakeJSONSession:
    def __init__(self, payload: dict) -> None:
        self.calls: list[dict] = []
        self._payload = payload

    def post(self, url, *, data, headers, timeout):
        self.calls.append({"url": url, "data": data, "headers": headers, "timeout": timeout})
        return _FakeResponse(self._payload)


def test_publish_chatgpt_session_signs_json_body() -> None:
    session = _FakeJSONSession({"account": "user@example.com", "session_key": "default", "token_sha256": "abc"})
    client = IngestClient(
        base_url="https://app.example.test/",
        signing_key=b"0123456789abcdef0123456789abcdef",
        session=session,
        now=lambda: 1700000000.0,
        link_ttl_seconds=900,
    )
    ack = client.publish_chatgpt_session(
        account="user@example.com",
        session_token="__Secure-next-auth.session-token=tok; cf_clearance=cf",
        source_browser="Google Chrome",
    )
    assert ack["token_sha256"] == "abc"
    call = session.calls[0]
    assert call["headers"]["Content-Type"] == "application/json"
    parts = urlsplit(call["url"])
    assert parts.path == "/ingest/chatgpt/session"
    q = {k: v[0] for k, v in parse_qs(parts.query).items()}
    expected_sha = hashlib.sha256(call["data"]).hexdigest()
    assert q["content_sha256"] == expected_sha
    assert q["sig"] == sign_object_upload(
        b"0123456789abcdef0123456789abcdef", "/ingest/chatgpt/session", expected_sha, 1700000000 + 900
    )
    # The body is canonical JSON carrying the credential.
    assert b'"account":"user@example.com"' in call["data"]
    assert b"__Secure-next-auth.session-token=tok" in call["data"]


# Object keys are now built only by the app (Go); see
# app/internal/server/ingest_test.go for the key/tag assertions.


# --- runner uses the http_app batch uploader --------------------------------


class _FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


def test_runner_uploads_via_batch_uploader(tmp_path: Path) -> None:
    proj = tmp_path / "claude" / "-proj"
    proj.mkdir(parents=True)
    path = proj / "sess-1.jsonl"
    with path.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps({"type": "user", "uuid": "u1"}) + "\n")
        handle.write(json.dumps({"type": "user", "uuid": "u2"}) + "\n")

    uploads: list[tuple[bytes, datetime]] = []

    def uploader(encoded: bytes, exported_at: datetime) -> dict:
        uploads.append((encoded, exported_at))
        return {"storage_backend": "google_drive", "storage_key": "k", "storage_file_id": "fid", "storage_url": ""}

    state = AgentSessionsUploadState.open(tmp_path / "state.sqlite", account="zach@example.com")
    now = datetime(2026, 6, 14, 18, tzinfo=UTC)
    summary = AgentSessionsUploadRunner(
        account="zach@example.com",
        device="porygon",
        claude_projects_dir=tmp_path / "claude",
        codex_sessions_dir=None,
        batch_uploader=uploader,
        logger=_FakeLogger(),
        upload_state=state,
        now=lambda: now,
    ).sync()
    state.close()

    assert summary.batches_uploaded == 1
    assert len(uploads) == 1
    encoded, exported_at = uploads[0]
    assert exported_at == now
    # The uploader receives ready-to-store gzipped JSONL envelopes.
    records = [json.loads(line) for line in gzip.decompress(encoded).decode("utf-8").splitlines() if line.strip()]
    assert [r["record"]["line"]["uuid"] for r in records] == ["u1", "u2"]


def test_runner_requires_batch_uploader(tmp_path: Path) -> None:
    state = AgentSessionsUploadState.open(tmp_path / "state.sqlite", account="zach@example.com")
    try:
        raised = False
        try:
            AgentSessionsUploadRunner(
                account="z@example.com",
                device="d",
                claude_projects_dir=tmp_path,
                codex_sessions_dir=None,
                batch_uploader=None,
                logger=_FakeLogger(),
                upload_state=state,
            )
        except ValueError:
            raised = True
        assert raised
    finally:
        state.close()


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
