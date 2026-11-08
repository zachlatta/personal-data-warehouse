from __future__ import annotations

from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.definitions import defs
from personal_data_warehouse.defs import agent_sessions_drive_ingest as agent_defs


def test_repository_includes_agent_sessions_definitions() -> None:
    repository = defs().get_repository_def()

    sensor_names = {sensor.name for sensor in repository.sensor_defs}
    job_names = {job.name for job in repository.get_all_jobs()}
    assert "agent_sessions_drive_inbox_sensor" in sensor_names
    assert "agent_sessions_drive_ingest_job" in job_names


def test_drive_inbox_sensor_runs_every_minute() -> None:
    sensor = agent_defs.agent_sessions_drive_inbox_sensor
    assert sensor.minimum_interval_seconds == 60
    assert sensor.default_status.value == "RUNNING"


def test_drive_ingest_does_not_take_postgres_advisory_lock() -> None:
    assert not hasattr(agent_defs, "exclusive_sync_lock")
    assert not hasattr(agent_defs, "AGENT_SESSIONS_DRIVE_INGEST_POSTGRES_LOCK_ID")


def test_drive_inbox_sensor_skips_when_inbox_is_empty(monkeypatch) -> None:
    monkeypatch.setattr(agent_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(agent_defs, "_agent_sessions_object_store", lambda _settings: object())
    monkeypatch.setattr(agent_defs, "has_batch_payloads", lambda **_kwargs: False)
    monkeypatch.setenv("AGENT_SESSIONS_LANDING_WORKER_ENABLED", "0")

    with DagsterInstance.ephemeral() as instance:
        result = agent_defs.agent_sessions_drive_inbox_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert "No agent session inbox batches" in result.skip_message


def test_drive_inbox_sensor_launches_when_inbox_has_batch(monkeypatch) -> None:
    monkeypatch.setattr(agent_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(agent_defs, "_agent_sessions_object_store", lambda _settings: object())
    monkeypatch.setattr(agent_defs, "has_batch_payloads", lambda **_kwargs: True)
    monkeypatch.setenv("AGENT_SESSIONS_LANDING_WORKER_ENABLED", "0")

    with DagsterInstance.ephemeral() as instance:
        result = agent_defs.agent_sessions_drive_inbox_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, RunRequest)
    assert result.tags == {"agent_sessions_trigger": "drive_inbox"}


def test_drive_inbox_sensor_skips_when_unconfigured(monkeypatch) -> None:
    def raise_value_error(**_kwargs):
        raise ValueError("AGENT_SESSIONS_ACCOUNT must be set")

    monkeypatch.setattr(agent_defs, "load_settings", raise_value_error)

    with DagsterInstance.ephemeral() as instance:
        result = agent_defs.agent_sessions_drive_inbox_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert "not configured" in result.skip_message


class FakeSettings:
    def __init__(self) -> None:
        self.agent_sessions = type(
            "FakeAgentSessionsConfig",
            (),
            {
                "google_drive_account": "zach@example.com",
                "google_drive_folder_id": "drive-folder",
            },
        )()


class _Listing:
    def __init__(self, exported_at: str) -> None:
        self.app_properties = {"exported_at": exported_at}


class _Store:
    def __init__(self, *exported_at: str) -> None:
        self._listings = [_Listing(value) for value in exported_at]

    def list_objects(self, **_kwargs):
        return iter(self._listings)


def test_the_sensor_leaves_a_fresh_batch_to_the_landing_worker(monkeypatch) -> None:
    """The resident worker lands a batch seconds after the app NOTIFYs it; a
    sensor run on the same batch would only race it. So with the worker on,
    the sensor is the floor: it launches for a batch the worker has left in
    the inbox past the grace, which means the worker is down or wedged."""
    from datetime import UTC, datetime, timedelta

    now = datetime(2026, 11, 8, 16, 0, tzinfo=UTC)
    monkeypatch.setattr(agent_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(agent_defs, "_utcnow", lambda: now)
    monkeypatch.delenv("AGENT_SESSIONS_LANDING_WORKER_ENABLED", raising=False)

    fresh = (now - timedelta(seconds=30)).isoformat()
    stale = (now - timedelta(seconds=agent_defs.AGENT_SESSIONS_WORKER_GRACE_SECONDS + 1)).isoformat()
    for store, launches in (
        (_Store(), False),
        (_Store(fresh), False),
        (_Store(fresh, stale), True),
        (_Store("not a timestamp"), True),
    ):
        monkeypatch.setattr(agent_defs, "_agent_sessions_object_store", lambda _settings, store=store: store)
        with DagsterInstance.ephemeral() as instance:
            result = agent_defs.agent_sessions_drive_inbox_sensor(build_sensor_context(instance=instance))
        assert isinstance(result, RunRequest) is launches, result
