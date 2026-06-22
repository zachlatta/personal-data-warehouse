from __future__ import annotations

from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

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

    def get_chatgpt_session(self, *, account, session_key):
        return self._session_row

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
