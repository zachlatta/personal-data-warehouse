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
    "PDW_INGEST_BASE_URL",
    "PDW_API_URL",
    "MCP_BASE_URL",
    "PDW_INGEST_SIGNING_KEY",
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


def test_from_env_prefers_explicit_ingest_url_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ingest_env(monkeypatch)
    monkeypatch.setenv("PDW_INGEST_BASE_URL", "https://explicit.example")
    monkeypatch.setenv("PDW_API_URL", "https://warehouse.example")
    monkeypatch.setenv("PDW_SECRET_TOKEN", "tok")
    client = ingest_client_from_env()
    assert client._base_url == "https://explicit.example"


def test_from_env_requires_a_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ingest_env(monkeypatch)
    monkeypatch.setenv("PDW_SECRET_TOKEN", "tok")
    with pytest.raises(ValueError, match="PDW_API_URL"):
        ingest_client_from_env()
