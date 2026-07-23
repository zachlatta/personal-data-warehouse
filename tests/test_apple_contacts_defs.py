from __future__ import annotations

from dagster import DagsterInstance, SkipReason, build_sensor_context

from personal_data_warehouse.defs import apple_contacts_drive_ingest as defs_module


class FakeSettings:
    apple_contacts = type(
        "AppleContacts",
        (),
        {
            "google_drive_folder_id": "folder",
            "google_drive_account": "owner@example.test",
        },
    )()


def test_apple_contacts_sensor_runs_every_minute() -> None:
    assert defs_module.apple_contacts_drive_inbox_sensor.minimum_interval_seconds == 60


def test_apple_contacts_sensor_skips_empty_inbox(monkeypatch) -> None:
    monkeypatch.setattr(defs_module, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(defs_module, "_apple_contacts_object_store", lambda _settings: object())
    monkeypatch.setattr(defs_module, "has_batch_payloads", lambda **_kwargs: False)
    with DagsterInstance.ephemeral() as instance:
        result = defs_module.apple_contacts_drive_inbox_sensor(
            build_sensor_context(instance=instance)
        )
    assert isinstance(result, SkipReason)
