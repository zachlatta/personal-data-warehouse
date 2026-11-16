"""One factory preserves each source's Dagster names and landing behavior."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from dagster import DagsterInstance, RunRequest, SkipReason, build_asset_context, build_sensor_context

from personal_data_warehouse import paired_audio_defs as shared
from personal_data_warehouse.definitions import defs
from personal_data_warehouse.defs import android_call_recordings_drive_ingest as calls
from personal_data_warehouse.defs import google_recorder_drive_ingest as recorder


@pytest.fixture(params=[recorder, calls], ids=["google_recorder", "android_call_recordings"])
def source(request):
    return request.param


def settings():
    return SimpleNamespace(
        postgres_database_url="postgresql://example/warehouse",
        voice_memos=SimpleNamespace(google_drive_folder_id="folder", account="owner@example.com"),
    )


def test_source_names_and_sensor_cadence_are_preserved(source):
    name = source.INBOX.source
    repository = defs().get_repository_def()
    assert f"{name}_drive_inbox_sensor" in {s.name for s in repository.sensor_defs}
    assert f"{name}_drive_ingest_job" in {j.name for j in repository.get_all_jobs()}
    assert source.runtime.asset.key.path == [f"{name}_drive_ingest"]
    assert source.runtime.sensor.minimum_interval_seconds == 60
    assert source.runtime.sensor.default_status.value == "RUNNING"


@pytest.mark.parametrize("pending", [False, True])
def test_sensor_checks_its_source_inbox(monkeypatch, source, pending):
    monkeypatch.setattr(shared, "load_settings", lambda **_: settings())
    monkeypatch.setattr(source.runtime, "has_metadata", lambda _: pending)
    with DagsterInstance.ephemeral() as instance:
        result = source.runtime.sensor(build_sensor_context(instance=instance))
    if pending:
        assert isinstance(result, RunRequest)
        assert result.tags == {f"{source.INBOX.source}_trigger": "drive_inbox"}
    else:
        assert isinstance(result, SkipReason)


def test_source_store_uses_shared_folder_and_source_tags(monkeypatch, source):
    seen = {}

    def build(spec, **_):
        seen["spec"] = spec
        return object()

    monkeypatch.setattr(shared, "build_object_store", build)
    source.runtime.store(settings())
    spec = seen["spec"]
    assert spec.namespace == "folder" and spec.google_drive.account == "owner@example.com"
    assert (spec.source, spec.blob_kind, spec.metadata_kind) == (
        source.INBOX.source,
        source.INBOX.audio_kind,
        source.INBOX.metadata_kind,
    )


@pytest.mark.parametrize("rows", [0, 1])
def test_asset_uses_shared_ingest_and_lands_written_rows(monkeypatch, source, rows):
    monkeypatch.setattr(shared, "load_settings", lambda **_: settings())
    landed = []
    monkeypatch.setattr(shared, "land_sources_on_timeline", lambda **kw: landed.append(kw["sources"]) or {"rows": 1})
    monkeypatch.setattr(
        source.runtime,
        "ingest",
        lambda **_: SimpleNamespace(metadata_seen=rows, rows_written=rows, recordings_promoted=rows),
    )
    source.runtime.asset(build_asset_context())
    assert landed == ([["voice_memos"]] if rows else [])


def test_ingest_lock_identity_and_close_are_shared(monkeypatch, source):
    seen = {}
    warehouse = SimpleNamespace(close=lambda: seen.update(closed=True))
    monkeypatch.setattr(shared, "warehouse_from_settings", lambda _: warehouse)

    @contextmanager
    def lock(**kw):
        seen.update(kw)
        yield True

    monkeypatch.setattr(shared, "exclusive_sync_lock", lock)
    monkeypatch.setattr(source.runtime, "store", lambda _: object())

    def runner(**kw):
        seen["inbox"] = kw["inbox"]
        return SimpleNamespace(sync=lambda: "done")

    monkeypatch.setattr(shared, "PairedAudioIngestRunner", runner)
    assert source.runtime.ingest(settings=settings(), logger=object()) == "done"
    assert seen["name"] == f"{source.INBOX.source}_drive_ingest" and seen["closed"]
    assert seen["inbox"] is source.INBOX
