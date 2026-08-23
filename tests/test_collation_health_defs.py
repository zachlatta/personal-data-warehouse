from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from dagster import build_asset_context, build_schedule_context

import personal_data_warehouse.defs.collation_health as collation_health_defs
from personal_data_warehouse.collation_health import (
    FINDING_DUPLICATE_KEYS,
    FINDING_NO_BASELINE,
    FINDING_OK,
    CollationFinding,
)


class _FakeWarehouse:
    def __init__(self) -> None:
        self.ensured = False
        self.closed = False

    def ensure_pipeline_health_tables(self) -> None:
        self.ensured = True

    def close(self) -> None:
        self.closed = True


class _FakeCollector:
    instances: list["_FakeCollector"] = []

    def __init__(self, warehouse) -> None:
        self.warehouse = warehouse
        self.ran = False
        _FakeCollector.instances.append(self)

    def run(self):
        self.ran = True
        return _findings()


def _findings() -> list[CollationFinding]:
    return [
        CollationFinding(
            object_id="database",
            scope="database",
            object_name="postgres",
            provider="database default",
            recorded_version="",
            actual_version="2.36",
            dependent_indexes=188,
            finding=FINDING_NO_BASELINE,
            detail="this database cannot detect collation drift; text index ordering is unverified",
        ),
        CollationFinding(
            object_id="index:base_slack.message_reactions_pkey",
            scope="index",
            object_name="base_slack.message_reactions_pkey",
            provider="",
            recorded_version="",
            actual_version="",
            dependent_indexes=0,
            finding=FINDING_DUPLICATE_KEYS,
            detail="6622 row(s) beyond the distinct key count",
            excess_rows=6622,
        ),
        CollationFinding(
            object_id="index:base_gmail.messages_pkey",
            scope="index",
            object_name="base_gmail.messages_pkey",
            provider="",
            recorded_version="",
            actual_version="",
            dependent_indexes=0,
            finding=FINDING_OK,
            detail="",
        ),
    ]


@contextmanager
def _acquired_lock(**_kwargs):
    yield True


@contextmanager
def _busy_lock(**_kwargs):
    yield False


def _patch_common(monkeypatch) -> _FakeWarehouse:
    warehouse = _FakeWarehouse()
    _FakeCollector.instances = []
    monkeypatch.setattr(
        collation_health_defs,
        "load_settings",
        lambda **_: SimpleNamespace(postgres_database_url="postgresql://example/warehouse"),
    )
    monkeypatch.setattr(
        collation_health_defs, "warehouse_from_settings", lambda _settings: warehouse
    )
    monkeypatch.setattr(collation_health_defs, "CollationHealthCollector", _FakeCollector)
    return warehouse


def test_asset_reports_findings_without_failing_the_run(monkeypatch):
    """A finding is a detector result, not a run failure.

    The warehouse has carried an undetectable collation baseline since before
    anyone noticed. Turning that into a permanently red asset would train
    everyone to ignore the one signal that matters -- the same reasoning that
    made Plaid's action_required a green run with a visible dashboard state.
    """
    warehouse = _patch_common(monkeypatch)
    monkeypatch.setattr(collation_health_defs, "exclusive_sync_lock", _acquired_lock)

    result = collation_health_defs.collation_health(build_asset_context())

    assert warehouse.ensured
    assert warehouse.closed
    assert _FakeCollector.instances[0].ran
    assert result.metadata["objects_checked"].value == 3
    assert result.metadata["indexes_with_duplicates"].value == [
        "base_slack.message_reactions_pkey"
    ]
    assert result.metadata["excess_rows"].value == 6622
    assert result.metadata["collations_without_baseline"].value == ["postgres"]
    # The observed library version is recorded every run: with no baseline in
    # pg_database, this asset's own history is the only baseline that will ever
    # exist, so a future glibc change shows up as this value moving.
    assert result.metadata["database_collation_actual_version"].value == "2.36"


def test_asset_skips_when_another_collection_is_active(monkeypatch):
    warehouse = _patch_common(monkeypatch)
    monkeypatch.setattr(collation_health_defs, "exclusive_sync_lock", _busy_lock)

    result = collation_health_defs.collation_health(build_asset_context())

    assert _FakeCollector.instances == []
    assert not warehouse.ensured
    assert warehouse.closed
    assert result.metadata["objects_checked"].value == 0


def test_schedule_is_daily_and_skips_overlap(monkeypatch):
    """Daily, not ten-minutely.

    The divergence probe costs a sequential scan of every unique index's heap
    under the size ceiling (~2 GB of reads on the production shape). Folding it
    into the ten-minute freshness collector would either make that collector
    expensive or make this check useless.
    """
    assert collation_health_defs.COLLATION_HEALTH_CRON == "41 3 * * *"
    calls = {}

    def _fake_guard(context, *, job_name):
        calls["job_name"] = job_name
        return {"skipped": True}

    monkeypatch.setattr(collation_health_defs, "skip_if_job_active", _fake_guard)
    result = collation_health_defs.collation_health_daily._execution_fn.decorated_fn(
        build_schedule_context()
    )
    assert result == {"skipped": True}
    assert calls["job_name"] == "collation_health_job"


def test_defs_expose_the_asset_job_and_schedule():
    defs = collation_health_defs.defs()
    assert [spec.key.to_user_string() for spec in defs.resolve_all_asset_specs()] == [
        "collation_health"
    ]
    assert [job.name for job in defs.jobs] == ["collation_health_job"]
    assert [schedule.name for schedule in defs.schedules] == ["collation_health_daily"]


def test_the_collector_lock_is_distinct_from_the_freshness_collectors():
    """Two independent collectors must not serialize on one lock."""
    import personal_data_warehouse.defs.pipeline_health as pipeline_health_defs

    assert (
        collation_health_defs.COLLATION_HEALTH_POSTGRES_LOCK_ID
        != pipeline_health_defs.PIPELINE_HEALTH_POSTGRES_LOCK_ID
    )
