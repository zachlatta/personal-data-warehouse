from __future__ import annotations

from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.definitions import defs
from personal_data_warehouse.defs import claude_desktop_client as claude_defs


def test_repository_includes_claude_desktop_definitions() -> None:
    repository = defs().get_repository_def()

    sensor_names = {sensor.name for sensor in repository.sensor_defs}
    job_names = {job.name for job in repository.get_all_jobs()}
    assert "claude_desktop_client_keepalive_sensor" in sensor_names
    assert "claude_desktop_client_job" in job_names


def test_claude_desktop_sensor_runs_on_five_minute_cadence() -> None:
    sensor = claude_defs.claude_desktop_client_keepalive_sensor
    assert sensor.minimum_interval_seconds == 300
    assert sensor.default_status.value == "RUNNING"


def test_claude_desktop_sensor_launches_when_enabled(monkeypatch) -> None:
    monkeypatch.setattr(claude_defs, "skip_if_job_in_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(claude_defs, "load_settings", lambda **_kwargs: FakeSettings(enabled=True))
    monkeypatch.setattr(claude_defs, "ingest_upload_config_problem", lambda: None)
    monkeypatch.setattr(claude_defs, "warehouse_from_settings", lambda _settings: FakeWarehouse(None))

    with DagsterInstance.ephemeral() as instance:
        result = claude_defs.claude_desktop_client_keepalive_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, RunRequest)
    assert result.tags == {"claude_desktop_trigger": "keepalive"}


def test_claude_desktop_sensor_skips_when_disabled(monkeypatch) -> None:
    monkeypatch.setattr(claude_defs, "skip_if_job_in_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(claude_defs, "load_settings", lambda **_kwargs: FakeSettings(enabled=False))
    monkeypatch.setattr(claude_defs, "ingest_upload_config_problem", lambda: None)

    with DagsterInstance.ephemeral() as instance:
        result = claude_defs.claude_desktop_client_keepalive_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert "disabled" in result.skip_message


def test_claude_desktop_sensor_sits_out_a_key_claude_ai_rejected(monkeypatch) -> None:
    """3,450 identical red runs in twelve days, behind a green health row.

    A key the poller has recorded as rejected is not relaunched every five
    minutes; the verdict is on the credential row for /pipelines and the
    sensor re-probes hourly or when a different key is pushed.
    """
    from datetime import UTC, datetime

    from personal_data_warehouse_claude_desktop.state import session_key_sha256

    monkeypatch.setattr(claude_defs, "skip_if_job_in_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(claude_defs, "load_settings", lambda **_kwargs: FakeSettings(enabled=True))
    monkeypatch.setattr(claude_defs, "ingest_upload_config_problem", lambda: None)
    rejected = {
        "account": "a@example.com",
        "session_key": "sk-dead",
        "status": "action_required",
        "error": "claude.ai returned 403 for /x",
        "rejected_session_sha256": session_key_sha256("sk-dead"),
        "rejected_at": datetime.now(tz=UTC),
    }
    monkeypatch.setattr(claude_defs, "warehouse_from_settings", lambda _settings: FakeWarehouse(rejected))

    with DagsterInstance.ephemeral() as instance:
        result = claude_defs.claude_desktop_client_keepalive_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert "403" in result.skip_message and "pdw ingest claude-desktop" in result.skip_message


class FakeWarehouse:
    def __init__(self, credential) -> None:
        self._credential = credential
        self.closed = False

    def read_latest_claude_desktop_credential(self):
        return self._credential

    def close(self) -> None:
        self.closed = True


class FakeSettings:
    def __init__(self, *, enabled: bool) -> None:
        self.claude_desktop = type(
            "FakeClaudeDesktopConfig",
            (),
            {"enabled": enabled},
        )()
