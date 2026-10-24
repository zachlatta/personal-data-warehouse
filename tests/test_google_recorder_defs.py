from __future__ import annotations

from types import SimpleNamespace

from dagster import DagsterInstance, RunRequest, SkipReason, build_asset_context, build_sensor_context

from personal_data_warehouse.defs import google_recorder_drive_ingest as recorder_defs
from personal_data_warehouse.definitions import defs


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        postgres_database_url="postgresql://example/warehouse",
        voice_memos=SimpleNamespace(google_drive_folder_id="folder", account="zach@example.com"),
    )


def test_the_recorder_inbox_sensor_and_asset_are_registered() -> None:
    repository = defs().get_repository_def()
    assert "google_recorder_drive_inbox_sensor" in {sensor.name for sensor in repository.sensor_defs}
    assert "google_recorder_drive_ingest_job" in {job.name for job in repository.get_all_jobs()}


def test_the_inbox_sensor_runs_every_minute_by_default() -> None:
    sensor = recorder_defs.google_recorder_drive_inbox_sensor
    assert sensor.minimum_interval_seconds == 60
    assert sensor.default_status.value == "RUNNING"


def test_the_sensor_skips_an_empty_inbox(monkeypatch) -> None:
    monkeypatch.setattr(recorder_defs, "load_settings", lambda **_k: _settings())
    monkeypatch.setattr(recorder_defs, "google_recorder_object_store", lambda _settings: object())
    monkeypatch.setattr(recorder_defs, "has_metadata_payloads", lambda **_k: False)

    with DagsterInstance.ephemeral() as instance:
        result = recorder_defs.google_recorder_drive_inbox_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SkipReason)
    assert "No Google Recorder inbox metadata" in result.skip_message


def test_the_sensor_launches_when_the_inbox_has_a_sidecar(monkeypatch) -> None:
    monkeypatch.setattr(recorder_defs, "load_settings", lambda **_k: _settings())
    monkeypatch.setattr(recorder_defs, "google_recorder_object_store", lambda _settings: object())
    monkeypatch.setattr(recorder_defs, "has_metadata_payloads", lambda **_k: True)

    with DagsterInstance.ephemeral() as instance:
        result = recorder_defs.google_recorder_drive_inbox_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, RunRequest)
    assert result.tags == {"google_recorder_trigger": "drive_inbox"}


def test_the_store_shares_the_voice_memos_drive_folder_under_its_own_source_tag(monkeypatch) -> None:
    """The app files Recorder objects in the same folder as every other upload,
    tagged pdw_source=google_recorder; the reader must list that tag or the
    inbox is invisible to it."""
    seen = {}

    def fake_build(spec, *, settings):
        seen["spec"] = spec
        return object()

    monkeypatch.setattr(recorder_defs, "build_object_store", fake_build)
    recorder_defs.google_recorder_object_store(_settings())
    spec = seen["spec"]
    assert spec.namespace == "folder"
    assert spec.google_drive.account == "zach@example.com"
    assert spec.source == "google_recorder"
    assert spec.blob_kind == "google_recorder_audio"
    assert spec.metadata_kind == "google_recorder_metadata"


def test_the_asset_lands_recordings_on_the_timeline_only_when_it_wrote(monkeypatch) -> None:
    monkeypatch.setattr(recorder_defs, "load_settings", lambda **_k: _settings())
    landed: list[list[str]] = []
    monkeypatch.setattr(
        recorder_defs,
        "land_sources_on_timeline",
        lambda *, postgres_url, sources, logger: landed.append(list(sources)) or {"rows": 1},
    )

    monkeypatch.setattr(
        recorder_defs,
        "ingest_google_recorder_inbox",
        lambda **_k: SimpleNamespace(metadata_seen=0, rows_written=0, recordings_promoted=0),
    )
    recorder_defs.google_recorder_drive_ingest(build_asset_context())
    assert landed == []

    monkeypatch.setattr(
        recorder_defs,
        "ingest_google_recorder_inbox",
        lambda **_k: SimpleNamespace(metadata_seen=1, rows_written=1, recordings_promoted=1),
    )
    recorder_defs.google_recorder_drive_ingest(build_asset_context())
    assert landed == [["voice_memos"]]
