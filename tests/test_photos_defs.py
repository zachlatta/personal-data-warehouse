"""Dagster wiring for the photos drive ingest (sensor-driven, like voice memos)."""

from __future__ import annotations

from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.defs import photos_drive_ingest as photos_defs
from personal_data_warehouse.definitions import defs


class FakeSettings:
    photos = type(
        "FakePhotosConfig",
        (),
        {"google_drive_folder_id": "folder", "google_drive_account": "z@x.test", "account": "z@x.test"},
    )()


def test_photos_repository_registers_drive_ingest() -> None:
    repository = defs().get_repository_def()
    assert "photos_drive_inbox_sensor" in {sensor.name for sensor in repository.sensor_defs}
    assert repository.has_job("photos_drive_ingest_job")


def test_photos_repository_registers_identity_pipeline() -> None:
    repository = defs().get_repository_def()
    assert "photo_identity_backlog_sensor" in {sensor.name for sensor in repository.sensor_defs}
    assert "photo_identity_hourly" in {schedule.name for schedule in repository.schedule_defs}
    assert repository.has_job("photo_identity_job")


def test_photo_identity_backlog_sensor_skips_when_tables_missing(monkeypatch) -> None:
    from personal_data_warehouse.defs import photo_identity as identity_defs

    monkeypatch.setattr(identity_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(
        identity_defs, "warehouse_from_settings", lambda _settings: _FakeWarehouse()
    )

    def boom(_warehouse):
        raise RuntimeError("relation does not exist")

    monkeypatch.setattr(identity_defs, "has_unresolved_photo_files", boom)

    with DagsterInstance.ephemeral() as instance:
        result = identity_defs.photo_identity_backlog_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert "not ready yet" in result.skip_message


def test_photo_identity_backlog_sensor_launches_on_backlog(monkeypatch) -> None:
    from personal_data_warehouse.defs import photo_identity as identity_defs

    monkeypatch.setattr(identity_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(
        identity_defs, "warehouse_from_settings", lambda _settings: _FakeWarehouse()
    )
    monkeypatch.setattr(identity_defs, "has_unresolved_photo_files", lambda _warehouse: True)

    with DagsterInstance.ephemeral() as instance:
        result = identity_defs.photo_identity_backlog_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, RunRequest)
    assert result.tags == {"photos_trigger": "identity_backlog"}


class _FakeWarehouse:
    def close(self) -> None:
        pass


def test_photos_drive_inbox_sensor_runs_every_minute() -> None:
    sensor = photos_defs.photos_drive_inbox_sensor
    assert sensor.minimum_interval_seconds == 60
    assert sensor.default_status.value == "RUNNING"


def test_photos_drive_inbox_sensor_skips_when_inbox_is_empty(monkeypatch) -> None:
    monkeypatch.setattr(photos_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(photos_defs, "photos_object_store", lambda _settings: object())
    monkeypatch.setattr(photos_defs, "has_metadata_payloads", lambda **_kwargs: False)

    with DagsterInstance.ephemeral() as instance:
        result = photos_defs.photos_drive_inbox_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert "No photo inbox metadata" in result.skip_message


def test_photos_drive_inbox_sensor_launches_when_inbox_has_metadata(monkeypatch) -> None:
    monkeypatch.setattr(photos_defs, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(photos_defs, "photos_object_store", lambda _settings: object())
    monkeypatch.setattr(photos_defs, "has_metadata_payloads", lambda **_kwargs: True)

    with DagsterInstance.ephemeral() as instance:
        result = photos_defs.photos_drive_inbox_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, RunRequest)
    assert result.tags == {"photos_trigger": "drive_inbox"}
