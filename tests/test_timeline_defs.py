from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
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

    def run(
        self, *, max_seconds: float | None = None, backfill_max_seconds: float | None = None
    ) -> list[AdapterSyncStats]:
        self.ran_with = max_seconds
        self.backfill_budget = backfill_max_seconds
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
    assert engine.backfill_budget is None
    assert engine.closed
    assert result.metadata["backfill_rows"].value == 5
    assert result.metadata["incremental_rows"].value == 2
    assert result.metadata["backfill_pending"].value == ["slack_message"]


def test_timeline_sync_asset_passes_the_backfill_throttle_from_the_environment(monkeypatch):
    _patch_common(monkeypatch)
    monkeypatch.setattr(timeline_defs, "exclusive_sync_lock", _acquired_lock)
    monkeypatch.setenv("TIMELINE_SYNC_BACKFILL_BUDGET_SECONDS", "45")

    timeline_defs.timeline_sync(build_asset_context())

    engine = _FakeEngine.instances[0]
    assert engine.ran_with == timeline_defs.TIMELINE_SYNC_RUN_BUDGET_SECONDS
    assert engine.backfill_budget == 45.0


def _reconcile_stats(self, **_kwargs):
    return [AdapterSyncStats(adapter="slack_message", backfill_done=True, reconcile_ran=True)]


class _FakeCacheWarehouse:
    """Enough of PostgresWarehouse for the post-reconcile cache repair."""

    instances: list["_FakeCacheWarehouse"] = []

    def __init__(self, url, *, resident_fraction=0.10, prewarmed_at=None):
        self.url = url
        self.resident_fraction = resident_fraction
        self.prewarmed_at = prewarmed_at or datetime(2020, 1, 1, tzinfo=UTC)
        self.force_calls = []
        self.closed = False
        _FakeCacheWarehouse.instances.append(self)

    def measure_search_cache_residency(self):
        return {"resident_fraction": self.resident_fraction, "resident_bytes": 1, "total_bytes": 10, "target_count": 2}

    def write_search_health(self, component, **facts):
        self.health = (component, facts)

    def search_prewarmed_at(self):
        return self.prewarmed_at

    def prewarm_search_indexes_if_needed(self, *, force=False):
        self.force_calls.append(force)
        return {"warmed": True, "reason": "forced", "blocks": 10}

    def close(self):
        self.closed = True


def test_timeline_sync_rewarms_search_after_a_high_volume_reconcile_only_when_cold(monkeypatch):
    """The post-reconcile warm is gated by residency and the daily floor.

    Until 2026-09-20 it was ``force=True``: three adapters reconcile on
    independent hourly clocks, so production issued 63 full BM25 warms and a
    10 GB HNSW ``buffer`` load in 24 hours -- ~1.5 TB through an 18 GB cache,
    the single largest block reader on the host, and the reason residency sat
    at 19.6% with cold searches at 5-8s. The warmer was the evictor.
    """
    _patch_common(monkeypatch)
    monkeypatch.setattr(timeline_defs, "exclusive_sync_lock", _acquired_lock)
    monkeypatch.setattr(_FakeEngine, "run", _reconcile_stats)
    _FakeCacheWarehouse.instances.clear()
    monkeypatch.setattr(timeline_defs, "PostgresWarehouse", _FakeCacheWarehouse)

    timeline_defs.timeline_sync(build_asset_context())

    [warehouse] = _FakeCacheWarehouse.instances
    assert warehouse.url == "postgresql://example/warehouse"
    assert warehouse.force_calls == [True]  # cold and never warmed: repair it
    assert warehouse.health[0] == "cache_residency"  # and the measurement is published
    assert warehouse.closed


def test_timeline_sync_leaves_a_warm_or_recently_warmed_cache_alone(monkeypatch):
    _patch_common(monkeypatch)
    monkeypatch.setattr(timeline_defs, "exclusive_sync_lock", _acquired_lock)
    monkeypatch.setattr(_FakeEngine, "run", _reconcile_stats)

    for kwargs in (
        {"resident_fraction": 0.45},  # still warm after the reconcile
        {"resident_fraction": 0.10, "prewarmed_at": datetime.now(tz=UTC) - timedelta(hours=1)},  # cold, warmed an hour ago
    ):
        _FakeCacheWarehouse.instances.clear()
        monkeypatch.setattr(
            timeline_defs, "PostgresWarehouse", lambda url, _k=kwargs: _FakeCacheWarehouse(url, **_k)
        )
        timeline_defs.timeline_sync(build_asset_context())
        [warehouse] = _FakeCacheWarehouse.instances
        assert warehouse.force_calls == [], kwargs
        assert warehouse.closed


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
