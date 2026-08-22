from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import pytest
from dagster import (
    DagsterInstance,
    MaterializeResult,
    RunRequest,
    SkipReason,
    build_asset_context,
    build_sensor_context,
)

from personal_data_warehouse.chatgpt_backend import ChatGPTAuthError
from personal_data_warehouse.chatgpt_backend_ingest import ChatGPTBackendIngestSummary
from personal_data_warehouse.definitions import defs
from personal_data_warehouse.defs import chatgpt_backend_ingest as chatgpt_defs


class FakeConfig:
    def __init__(self, *, enabled=True, poll_interval_seconds=300):
        self.account = "user@example.com"
        self.session_key = "default"
        self.client_enabled = enabled
        self.poll_interval_seconds = poll_interval_seconds
        self.page_size = 28
        self.max_conversations_per_run = 0
        self.base_url = "https://chatgpt.com"
        self.request_timeout_seconds = 30


class FakeSettings:
    def __init__(self, config):
        self.chatgpt = config


class FakeWarehouse:
    def __init__(self, session_row):
        self._session_row = session_row
        self.closed = False
        self.marked_expired: list[dict] = []
        self.recorded_success: list[dict] = []

    def get_chatgpt_session(self, *, account, session_key):
        return self._session_row

    def mark_chatgpt_session_expired(self, *, account, session_key, token_sha256):
        self.marked_expired.append(
            {"account": account, "session_key": session_key, "token_sha256": token_sha256}
        )

    def record_chatgpt_session_success(
        self, *, account, session_key, token_sha256, token_expires_at=None
    ):
        self.recorded_success.append(
            {
                "account": account,
                "session_key": session_key,
                "token_sha256": token_sha256,
                "token_expires_at": token_expires_at,
            }
        )

    def close(self):
        self.closed = True


def _patch(monkeypatch, *, config, session_row):
    monkeypatch.setattr(chatgpt_defs, "load_settings", lambda **_k: FakeSettings(config))
    monkeypatch.setattr(chatgpt_defs, "warehouse_from_settings", lambda _s: FakeWarehouse(session_row))


def test_repository_includes_chatgpt_definitions() -> None:
    repository = defs().get_repository_def()
    sensor_names = {s.name for s in repository.sensor_defs}
    job_names = {j.name for j in repository.get_all_jobs()}
    assert "chatgpt_backend_ingest_sensor" in sensor_names
    assert "chatgpt_backend_ingest_job" in job_names


def test_sensor_skips_when_disabled(monkeypatch) -> None:
    _patch(monkeypatch, config=FakeConfig(enabled=False), session_row={"session_token": "t"})
    with DagsterInstance.ephemeral() as instance:
        result = chatgpt_defs.chatgpt_backend_ingest_sensor(build_sensor_context(instance=instance))
    assert isinstance(result, SkipReason)
    assert "disabled" in result.skip_message


def test_sensor_skips_until_session_published(monkeypatch) -> None:
    _patch(monkeypatch, config=FakeConfig(), session_row=None)
    with DagsterInstance.ephemeral() as instance:
        result = chatgpt_defs.chatgpt_backend_ingest_sensor(build_sensor_context(instance=instance))
    assert isinstance(result, SkipReason)
    assert "publish-session" in result.skip_message


def test_sensor_fires_when_due(monkeypatch) -> None:
    _patch(monkeypatch, config=FakeConfig(poll_interval_seconds=0), session_row={"session_token": "t"})
    with DagsterInstance.ephemeral() as instance:
        result = chatgpt_defs.chatgpt_backend_ingest_sensor(
            build_sensor_context(instance=instance, cursor=None)
        )
    assert isinstance(result, RunRequest)
    assert result.tags == {"chatgpt_trigger": "poll"}


def test_sensor_waits_for_poll_interval(monkeypatch) -> None:
    import time

    _patch(monkeypatch, config=FakeConfig(poll_interval_seconds=10_000), session_row={"session_token": "t"})
    with DagsterInstance.ephemeral() as instance:
        result = chatgpt_defs.chatgpt_backend_ingest_sensor(
            build_sensor_context(instance=instance, cursor=str(time.time()))
        )
    assert isinstance(result, SkipReason)
    assert "poll interval" in result.skip_message


def _expired_row(*, seconds_ago, token_sha="abc", expired_sha=None):
    return {
        "session_token": "cookie",
        "token_sha256": token_sha,
        "expired_token_sha256": token_sha if expired_sha is None else expired_sha,
        "expired_at": datetime.now(tz=UTC) - timedelta(seconds=seconds_ago),
    }


def test_sensor_skips_when_session_marked_expired(monkeypatch) -> None:
    # Marked expired recently (within the re-probe window): skip instead of firing a
    # doomed run, but say why and how to fix it.
    _patch(
        monkeypatch,
        config=FakeConfig(poll_interval_seconds=0),
        session_row=_expired_row(seconds_ago=60),
    )
    with DagsterInstance.ephemeral() as instance:
        result = chatgpt_defs.chatgpt_backend_ingest_sensor(
            build_sensor_context(instance=instance, cursor=None)
        )
    assert isinstance(result, SkipReason)
    assert "expired" in result.skip_message
    assert "publish-session" in result.skip_message


def test_sensor_does_not_reprobe_a_token_already_rejected(monkeypatch) -> None:
    # Only publishing a different token can change a permanent credential failure.
    # Time passing must not produce an hourly flood of identical red runs.
    _patch(
        monkeypatch,
        config=FakeConfig(poll_interval_seconds=0),
        session_row=_expired_row(seconds_ago=30 * 24 * 60 * 60),
    )
    with DagsterInstance.ephemeral() as instance:
        result = chatgpt_defs.chatgpt_backend_ingest_sensor(
            build_sensor_context(instance=instance, cursor=None)
        )
    assert isinstance(result, SkipReason)
    assert "publish-session" in result.skip_message


def test_sensor_fires_after_republish_despite_stale_mark(monkeypatch) -> None:
    # A fresh publish rotates token_sha256, so a leftover mark on the old token no
    # longer applies and polling resumes immediately (not waiting for the re-probe).
    _patch(
        monkeypatch,
        config=FakeConfig(poll_interval_seconds=0),
        session_row=_expired_row(seconds_ago=30, token_sha="new-token", expired_sha="old-token"),
    )
    with DagsterInstance.ephemeral() as instance:
        result = chatgpt_defs.chatgpt_backend_ingest_sensor(
            build_sensor_context(instance=instance, cursor=None)
        )
    assert isinstance(result, RunRequest)


# --- asset marks/clears the expiry state -----------------------------------


@contextmanager
def _fake_lock(*args, **kwargs):
    yield True


class _DummyClient:
    #: No parseable JWT expiry, the opaque-token case: the heartbeat records None.
    access_token_expiry = 0.0

    def __init__(self, **kwargs):
        pass


def _patch_asset(monkeypatch, *, session_row, sync):
    """Wire the asset's collaborators so only the mark/clear logic is exercised."""
    warehouse = FakeWarehouse(session_row)
    monkeypatch.setattr(chatgpt_defs, "load_settings", lambda **_k: FakeSettings(FakeConfig()))
    monkeypatch.setattr(chatgpt_defs, "warehouse_from_settings", lambda _s: warehouse)
    monkeypatch.setattr(chatgpt_defs, "exclusive_sync_lock", _fake_lock)
    monkeypatch.setattr(chatgpt_defs, "ChatGPTBackendClient", _DummyClient)

    class _FakeRunner:
        def __init__(self, **kwargs):
            pass

        def sync(self):
            return sync()

    monkeypatch.setattr(chatgpt_defs, "ChatGPTBackendIngestRunner", _FakeRunner)
    return warehouse


def _healthy_summary():
    return ChatGPTBackendIngestSummary(
        conversations_seen=0,
        conversations_fetched=0,
        events_written=0,
        reached_run_limit=False,
        rate_limited=False,
        stopped_at_high_water=True,
    )


def test_asset_marks_session_expired_on_auth_error(monkeypatch) -> None:
    def _raise():
        raise ChatGPTAuthError("session expired")

    warehouse = _patch_asset(
        monkeypatch,
        session_row={"session_token": "cookie", "token_sha256": "abc", "expired_token_sha256": ""},
        sync=_raise,
    )
    with pytest.raises(ChatGPTAuthError):
        chatgpt_defs.chatgpt_backend_ingest(build_asset_context())

    assert warehouse.marked_expired == [
        {"account": "user@example.com", "session_key": "default", "token_sha256": "abc"}
    ]
    assert warehouse.recorded_success == []
    assert warehouse.closed is True


def test_asset_records_credential_health_after_successful_poll(monkeypatch) -> None:
    warehouse = _patch_asset(
        monkeypatch,
        session_row={
            "session_token": "cookie",
            "token_sha256": "abc",
            "expired_token_sha256": "abc",
            "expired_at": datetime.now(tz=UTC),
        },
        sync=_healthy_summary,
    )
    result = chatgpt_defs.chatgpt_backend_ingest(build_asset_context())

    assert isinstance(result, MaterializeResult)
    assert warehouse.recorded_success == [
        {
            "account": "user@example.com",
            "session_key": "default",
            "token_sha256": "abc",
            "token_expires_at": None,
        }
    ]
    assert warehouse.marked_expired == []


def test_asset_records_every_healthy_poll_as_the_pipeline_heartbeat(monkeypatch) -> None:
    warehouse = _patch_asset(
        monkeypatch,
        session_row={"session_token": "cookie", "token_sha256": "abc", "expired_token_sha256": ""},
        sync=_healthy_summary,
    )
    result = chatgpt_defs.chatgpt_backend_ingest(build_asset_context())

    assert isinstance(result, MaterializeResult)
    assert warehouse.recorded_success == [
        {
            "account": "user@example.com",
            "session_key": "default",
            "token_sha256": "abc",
            "token_expires_at": None,
        }
    ]
    assert warehouse.marked_expired == []


def test_successful_poll_hands_the_token_expiry_to_the_credential_heartbeat(monkeypatch) -> None:
    """The asset is the only place that learns the token's real expiry.

    It authenticates every poll, so it holds the JWT ``exp`` that says when the
    published session dies. Passing it through is what lets the dashboard ask for
    a re-publish before ingest stops instead of after.
    """
    expiry = datetime(2026, 8, 24, 20, 52, 47, tzinfo=UTC)

    class _ExpiringClient:
        access_token_expiry = expiry.timestamp()

        def __init__(self, **kwargs):
            pass

    warehouse = _patch_asset(
        monkeypatch,
        session_row={"session_token": "cookie", "token_sha256": "abc", "expired_token_sha256": ""},
        sync=_healthy_summary,
    )
    monkeypatch.setattr(chatgpt_defs, "ChatGPTBackendClient", _ExpiringClient)

    chatgpt_defs.chatgpt_backend_ingest(build_asset_context())

    assert warehouse.recorded_success == [
        {
            "account": "user@example.com",
            "session_key": "default",
            "token_sha256": "abc",
            "token_expires_at": expiry,
        }
    ]
