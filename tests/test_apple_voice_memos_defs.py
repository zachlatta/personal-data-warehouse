from __future__ import annotations

from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.defs import apple_voice_memos_drive_ingest as apple_voice_memos_drive_ingest_defs
from personal_data_warehouse.definitions import defs


def test_apple_voice_memos_repository_uses_sensors_for_frequent_work() -> None:
    repository = defs().get_repository_def()

    assert {
        "apple_voice_memos_drive_inbox_sensor",
        "apple_voice_memos_transcription_backlog_sensor",
        "apple_voice_memos_enrichment_backlog_sensor",
    }.issubset({sensor.name for sensor in repository.sensor_defs})
    assert {
        "apple_voice_memos_drive_ingest_every_five_minutes",
        "apple_voice_memos_transcription_every_fifteen_minutes",
    }.isdisjoint({schedule.name for schedule in repository.schedule_defs})
    assert "apple_voice_memos_enrichment_hourly" in {schedule.name for schedule in repository.schedule_defs}


def test_apple_voice_memos_drive_inbox_sensor_runs_every_minute() -> None:
    sensor = apple_voice_memos_drive_ingest_defs.apple_voice_memos_drive_inbox_sensor

    assert sensor.minimum_interval_seconds == 60
    assert sensor.default_status.value == "RUNNING"


def test_apple_voice_memos_drive_inbox_sensor_skips_when_inbox_is_empty(monkeypatch) -> None:
    monkeypatch.setattr(
        apple_voice_memos_drive_ingest_defs,
        "load_settings",
        lambda **_kwargs: FakeSettings(),
    )
    monkeypatch.setattr(
        apple_voice_memos_drive_ingest_defs,
        "build_google_drive_service",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        apple_voice_memos_drive_ingest_defs,
        "has_drive_metadata_payloads",
        lambda **_kwargs: False,
    )

    with DagsterInstance.ephemeral() as instance:
        result = apple_voice_memos_drive_ingest_defs.apple_voice_memos_drive_inbox_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert "No Voice Memos inbox metadata" in result.skip_message


def test_apple_voice_memos_drive_inbox_sensor_launches_when_inbox_has_metadata(monkeypatch) -> None:
    monkeypatch.setattr(
        apple_voice_memos_drive_ingest_defs,
        "load_settings",
        lambda **_kwargs: FakeSettings(),
    )
    monkeypatch.setattr(
        apple_voice_memos_drive_ingest_defs,
        "build_google_drive_service",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        apple_voice_memos_drive_ingest_defs,
        "has_drive_metadata_payloads",
        lambda **_kwargs: True,
    )

    with DagsterInstance.ephemeral() as instance:
        result = apple_voice_memos_drive_ingest_defs.apple_voice_memos_drive_inbox_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, RunRequest)
    assert result.tags == {"apple_voice_memos_trigger": "drive_inbox"}


class FakeSettings:
    voice_memos = type(
        "FakeVoiceMemosConfig",
        (),
        {
            "account": "zach@example.com",
            "google_drive_folder_id": "drive-folder",
        },
    )()
