from __future__ import annotations

from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.definitions import defs
from personal_data_warehouse.defs import apple_notes_drive_ingest as apple_notes_defs


def test_apple_notes_repository_includes_drive_inbox_sensor() -> None:
    repository = defs().get_repository_def()

    assert "apple_notes_drive_inbox_sensor" in {sensor.name for sensor in repository.sensor_defs}
    assert "apple_notes_drive_ingest_job" in {job.name for job in repository.get_all_jobs()}


def test_apple_notes_drive_inbox_sensor_runs_every_minute() -> None:
    sensor = apple_notes_defs.apple_notes_drive_inbox_sensor

    assert sensor.minimum_interval_seconds == 60
    assert sensor.default_status.value == "RUNNING"


def test_apple_notes_drive_inbox_sensor_skips_when_inbox_is_empty(monkeypatch) -> None:
    monkeypatch.setattr(apple_notes_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(apple_notes_defs, "_apple_notes_object_store", lambda _settings: object())
    monkeypatch.setattr(apple_notes_defs, "has_metadata_payloads", lambda **_kwargs: False)

    with DagsterInstance.ephemeral() as instance:
        result = apple_notes_defs.apple_notes_drive_inbox_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert "No Apple Notes inbox metadata" in result.skip_message


def test_apple_notes_drive_inbox_sensor_launches_when_inbox_has_metadata(monkeypatch) -> None:
    monkeypatch.setattr(apple_notes_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(apple_notes_defs, "_apple_notes_object_store", lambda _settings: object())
    monkeypatch.setattr(apple_notes_defs, "has_metadata_payloads", lambda **_kwargs: True)

    with DagsterInstance.ephemeral() as instance:
        result = apple_notes_defs.apple_notes_drive_inbox_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, RunRequest)
    assert result.tags == {"apple_notes_trigger": "drive_inbox"}


class FakeSettings:
    apple_notes = type(
        "FakeAppleNotesConfig",
        (),
        {
            "google_drive_account": "zach@example.com",
            "google_drive_folder_id": "drive-folder",
        },
    )()
