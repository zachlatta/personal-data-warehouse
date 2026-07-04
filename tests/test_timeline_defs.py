from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from dagster import build_asset_context, build_schedule_context

import personal_data_warehouse.defs.timeline_sync as timeline_defs
from personal_data_warehouse.timeline import AdapterSyncStats, TimelineSyncError


class _FakeEngine:
    instances: list["_FakeEngine"] = []

    def __init__(self, *, source_url: str) -> None:
        self.source_url = source_url
        self.ran_with: float | None = None
        self.closed = False
        self.raise_error: TimelineSyncError | None = None
        _FakeEngine.instances.append(self)

    def run(self, *, max_seconds: float | None = None) -> list[AdapterSyncStats]:
        self.ran_with = max_seconds
        if self.raise_error is not None:
            raise self.raise_error
        return [
            AdapterSyncStats(adapter="gmail_email", backfill_rows=5, backfill_done=True),
            AdapterSyncStats(adapter="slack_message", incremental_rows=2, backfill_done=False),
        ]

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _reset_fake_engine():
    _FakeEngine.instances = []
    yield
    _FakeEngine.instances = []


@contextmanager
def _acquired_lock(**_kwargs):
    yield True


@contextmanager
def _busy_lock(**_kwargs):
    yield False


def _patch_common(monkeypatch):
    monkeypatch.setattr(
        timeline_defs,
        "load_settings",
        lambda **_: SimpleNamespace(postgres_database_url="postgresql://example/warehouse"),
    )
    monkeypatch.setattr(timeline_defs, "TimelineSyncEngine", _FakeEngine)


def test_timeline_sync_asset_runs_engine_within_budget(monkeypatch):
    _patch_common(monkeypatch)
    monkeypatch.setattr(timeline_defs, "exclusive_sync_lock", _acquired_lock)

    result = timeline_defs.timeline_sync(build_asset_context())

    engine = _FakeEngine.instances[0]
    assert engine.source_url == "postgresql://example/warehouse"
    assert engine.ran_with == timeline_defs.TIMELINE_SYNC_RUN_BUDGET_SECONDS
    assert engine.closed
    assert result.metadata["backfill_rows"].value == 5
    assert result.metadata["incremental_rows"].value == 2
    assert result.metadata["backfill_pending"].value == ["slack_message"]


def test_timeline_sync_asset_skips_when_lock_busy(monkeypatch):
    _patch_common(monkeypatch)
    monkeypatch.setattr(timeline_defs, "exclusive_sync_lock", _busy_lock)

    result = timeline_defs.timeline_sync(build_asset_context())

    assert _FakeEngine.instances == []
    assert result.metadata["adapters"].value == 0


def test_timeline_sync_asset_raises_on_adapter_failures(monkeypatch):
    _patch_common(monkeypatch)
    monkeypatch.setattr(timeline_defs, "exclusive_sync_lock", _acquired_lock)

    def _failing_engine(**kwargs):
        engine = _FakeEngine(**kwargs)
        engine.raise_error = TimelineSyncError("adapter blew up", stats=[])
        return engine

    monkeypatch.setattr(timeline_defs, "TimelineSyncEngine", _failing_engine)
    with pytest.raises(TimelineSyncError):
        timeline_defs.timeline_sync(build_asset_context())
    assert _FakeEngine.instances[0].closed


def test_schedule_skips_when_job_in_progress(monkeypatch):
    calls = {}

    def _fake_guard(context, *, job_name):
        calls["job_name"] = job_name
        return {"skipped": True}

    monkeypatch.setattr(timeline_defs, "skip_if_job_in_progress", _fake_guard)
    result = timeline_defs.timeline_sync_every_five_minutes._execution_fn.decorated_fn(
        build_schedule_context()
    )
    assert result == {"skipped": True}
    assert calls["job_name"] == "timeline_sync_job"
