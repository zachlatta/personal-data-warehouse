from __future__ import annotations

from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.definitions import defs
from personal_data_warehouse.defs import whatsapp_client as whatsapp_client_defs
from personal_data_warehouse.defs import whatsapp_drive_ingest as whatsapp_defs


def test_whatsapp_repository_includes_client_and_ingest_definitions() -> None:
    repository = defs().get_repository_def()

    sensor_names = {sensor.name for sensor in repository.sensor_defs}
    job_names = {job.name for job in repository.get_all_jobs()}
    assert "whatsapp_drive_inbox_sensor" in sensor_names
    assert "whatsapp_client_keepalive_sensor" in sensor_names
    assert "whatsapp_drive_ingest_job" in job_names
    assert "whatsapp_client_job" in job_names


def test_whatsapp_drive_inbox_sensor_runs_every_minute() -> None:
    sensor = whatsapp_defs.whatsapp_drive_inbox_sensor

    assert sensor.minimum_interval_seconds == 60
    assert sensor.default_status.value == "RUNNING"


def test_whatsapp_drive_inbox_sensor_skips_when_inbox_is_empty(monkeypatch) -> None:
    monkeypatch.setattr(whatsapp_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(whatsapp_defs, "_whatsapp_object_store", lambda _settings: object())
    monkeypatch.setattr(whatsapp_defs, "has_batch_payloads", lambda **_kwargs: False)

    with DagsterInstance.ephemeral() as instance:
        result = whatsapp_defs.whatsapp_drive_inbox_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert "No WhatsApp inbox batches" in result.skip_message


def test_whatsapp_drive_inbox_sensor_launches_when_inbox_has_batch(monkeypatch) -> None:
    monkeypatch.setattr(whatsapp_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(whatsapp_defs, "_whatsapp_object_store", lambda _settings: object())
    monkeypatch.setattr(whatsapp_defs, "has_batch_payloads", lambda **_kwargs: True)

    with DagsterInstance.ephemeral() as instance:
        result = whatsapp_defs.whatsapp_drive_inbox_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, RunRequest)
    assert result.tags == {"whatsapp_trigger": "drive_inbox"}


def test_whatsapp_client_keepalive_sensor_skips_when_disabled(monkeypatch) -> None:
    monkeypatch.setattr(whatsapp_client_defs, "load_settings", lambda **_kwargs: FakeSettings(client_enabled=False))

    with DagsterInstance.ephemeral() as instance:
        result = whatsapp_client_defs.whatsapp_client_keepalive_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert "WHATSAPP_CLIENT_ENABLED" in result.skip_message


def test_whatsapp_client_keepalive_sensor_skips_when_unconfigured(monkeypatch) -> None:
    def raise_value_error(**_kwargs):
        raise ValueError("WHATSAPP_ACCOUNT must be set")

    monkeypatch.setattr(whatsapp_client_defs, "load_settings", raise_value_error)

    with DagsterInstance.ephemeral() as instance:
        result = whatsapp_client_defs.whatsapp_client_keepalive_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert "not configured" in result.skip_message


def _set_ingest_env(monkeypatch) -> None:
    monkeypatch.setenv("PDW_API_URL", "https://app.example.test")
    monkeypatch.setenv("PDW_SECRET_TOKEN", "tok")


def test_whatsapp_client_keepalive_sensor_launches_when_enabled(monkeypatch) -> None:
    monkeypatch.setattr(whatsapp_client_defs, "load_settings", lambda **_kwargs: FakeSettings(client_enabled=True))
    _set_ingest_env(monkeypatch)

    with DagsterInstance.ephemeral() as instance:
        result = whatsapp_client_defs.whatsapp_client_keepalive_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, RunRequest)
    assert result.tags == {"whatsapp_trigger": "keepalive"}


def test_whatsapp_client_keepalive_sensor_skips_when_ingest_unconfigured(monkeypatch) -> None:
    # Enabled + WhatsApp-configured, but the http_app upload env is missing.
    # The keepalive sensor must skip rather than launch a run that would only
    # crash-loop (the 2026-06-20 prod incident: ~1000 failed runs/day).
    monkeypatch.setattr(whatsapp_client_defs, "load_settings", lambda **_kwargs: FakeSettings(client_enabled=True))
    for name in ("PDW_API_URL", "MCP_BASE_URL", "PDW_SECRET_TOKEN", "MCP_SECRET_TOKEN"):
        monkeypatch.delenv(name, raising=False)

    with DagsterInstance.ephemeral() as instance:
        result = whatsapp_client_defs.whatsapp_client_keepalive_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert "http_app ingest is not configured" in result.skip_message
    assert "PDW_API_URL" in result.skip_message


class FakeSettings:
    def __init__(self, *, client_enabled: bool = False) -> None:
        self.whatsapp = type(
            "FakeWhatsAppConfig",
            (),
            {
                "google_drive_account": "zach@example.com",
                "google_drive_folder_id": "drive-folder",
                "client_enabled": client_enabled,
            },
        )()
