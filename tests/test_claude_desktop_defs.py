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


class FakeSettings:
    def __init__(self, *, enabled: bool) -> None:
        self.claude_desktop = type(
            "FakeClaudeDesktopConfig",
            (),
            {"enabled": enabled},
        )()
