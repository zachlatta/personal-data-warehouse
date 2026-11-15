"""Approve now, run later: the worker's side of not_before, not_after,
cancelled steps and preconditions (app/internal/mutations/schedule.go
writes the rows)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import os
import uuid

import pytest

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.defs.upstream_mutations import process_upstream_mutation_batch
from personal_data_warehouse.postgres import PostgresWarehouse, _jsonb_param
from personal_data_warehouse.upstream_mutation_schedule import precondition_failure

CLAIMER = "resident:test:upstream_mutation_worker"
HOME = {"PDW_HOME_LATITUDE": "40.0", "PDW_HOME_LONGITUDE": "-75.0", "PDW_HOME_RADIUS_METERS": "1000"}


@pytest.fixture()
def warehouse():
    wh = PostgresWarehouse(os.environ["POSTGRES_DATABASE_URL"], schema=make_test_schema("mutsched"))
    try:
        wh.ensure_upstream_mutation_tables()
        wh.ensure_resideo_tables()
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def _seed(wh: PostgresWarehouse, *, not_before=None, not_after=None, precondition=None, status="approved") -> str:
    request_id = "req_" + uuid.uuid4().hex[:12]
    now = datetime.now(tz=UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    wh._command(
        """
        INSERT INTO @upstream_mutation_requests (id, status, title, reason, idempotency_key, requested_by, approved_by,
                                                 created_at, updated_at, approved_at, not_before, not_after)
        VALUES (%s, %s, 'Trip', 'trip plan', %s, 'agent', 'app:test', %s, %s, %s, %s, %s)
        """,
        (request_id, status, request_id, now, now, now, not_before or epoch, not_after or epoch),
    )
    wh._command(
        """
        INSERT INTO @upstream_mutations (id, request_id, request_index, provider, operation, account, status, title, reason,
                                         payload_json, idempotency_key, requested_by, approved_by, created_at, updated_at,
                                         approved_at, not_before, not_after, precondition_json)
        VALUES (%s, %s, 0, 'resideo', 'resideo.set_thermostat', 'home', %s, 'Away', 'leaving', %s, '', 'agent', 'app:test',
                %s, %s, %s, %s, %s, %s)
        """,
        (
            request_id + "_m0", request_id, status,
            _jsonb_param({"device_id": "DEV1", "heat_setpoint": 55.0, "hold": "permanent"}),
            now, now, now, not_before or epoch, not_after or epoch, _jsonb_param(precondition or {}),
        ),
    )
    return request_id


def _status(wh: PostgresWarehouse, request_id: str) -> tuple[str, str, str]:
    mutation = wh._query_dicts("SELECT status, error FROM @upstream_mutations WHERE request_id = %s", (request_id,))[0]
    request = wh._query_dicts("SELECT status FROM @upstream_mutation_requests WHERE id = %s", (request_id,))[0]
    return str(mutation["status"]), str(mutation["error"]), str(request["status"])


def _claim(wh: PostgresWarehouse) -> list[str]:
    return [str(row["request_id"]) for row in wh.claim_approved_upstream_mutations(limit=10, claimed_by=CLAIMER, providers=["resideo"])]


@pytest.mark.local_integration
def test_a_step_is_not_claimed_before_its_time_and_is_claimed_after(warehouse) -> None:
    later = _seed(warehouse, not_before=datetime.now(tz=UTC) + timedelta(hours=2))
    due = _seed(warehouse, not_before=datetime.now(tz=UTC) - timedelta(minutes=1))
    assert _claim(warehouse) == [due]
    assert _status(warehouse, later)[0] == "approved"


@pytest.mark.local_integration
def test_a_step_past_its_not_after_fails_instead_of_running_late(warehouse) -> None:
    request_id = _seed(warehouse, not_after=datetime.now(tz=UTC) - timedelta(minutes=5))
    assert _claim(warehouse) == []
    status, error, request_status = _status(warehouse, request_id)
    assert status == "failed_terminal" and "expired" in error
    assert request_status == "failed_terminal"


@pytest.mark.local_integration
def test_a_cancelled_step_is_never_claimed_and_its_request_reads_cancelled(warehouse) -> None:
    request_id = _seed(warehouse)
    warehouse._command("UPDATE @upstream_mutations SET status = 'cancelled' WHERE request_id = %s", (request_id,))
    warehouse._refresh_upstream_mutation_request_status(request_id)
    assert _claim(warehouse) == []
    assert _status(warehouse, request_id)[2] == "cancelled"


class _NoWrites:
    def execute(self, mutation):  # pragma: no cover - a write here is the failure
        raise AssertionError(f"the executor ran {mutation['id']}")


@pytest.mark.local_integration
def test_a_failed_precondition_skips_the_step_without_a_write(warehouse, monkeypatch) -> None:
    for key, value in HOME.items():
        monkeypatch.setenv(key, value)
    warehouse.ensure_android_location_tables()
    request_id = _seed(warehouse, precondition={"kind": "not_home", "within_minutes": 120})
    summary = process_upstream_mutation_batch(
        warehouse=warehouse,
        gmail_executor=_NoWrites(),
        contact_executor=_NoWrites(),
        calendar_executor=_NoWrites(),
        slack_executor=_NoWrites(),
        screen_timelapse_executor=_NoWrites(),
        resideo_executor=_NoWrites(),
        limit=10,
        claimed_by=CLAIMER,
        ensure_tables=False,
        on_gmail_settings_changed=None,
    )
    assert summary.claimed == 1
    status, error, request_status = _status(warehouse, request_id)
    assert status == "skipped_precondition", error
    assert request_status == "skipped_precondition"
    events = warehouse._query_dicts("SELECT event_type FROM @upstream_mutation_request_events WHERE request_id = %s", (request_id,))
    assert "precondition_skipped" in {row["event_type"] for row in events}


def _query_with(samples):
    def query(sql, params):
        return samples
    return query


def test_not_home_runs_only_when_recent_location_is_away_from_home() -> None:
    now = datetime(2026, 12, 4, 12, 0, tzinfo=UTC)
    mutation = {"precondition_json": {"kind": "not_home", "within_minutes": 120}}
    exists = lambda name: True  # noqa: E731
    away = [{"latitude": 40.5, "longitude": -75.0, "measured_at": now - timedelta(minutes=30)}]
    home = [{"latitude": 40.001, "longitude": -75.0, "measured_at": now - timedelta(minutes=30)}]
    assert precondition_failure(_query_with(away), mutation, now=now, relation_exists=exists, environ=HOME) is None
    assert "at home" in precondition_failure(_query_with(home), mutation, now=now, relation_exists=exists, environ=HOME)
    # Missing data never sends the house to away.
    assert "no phone location" in precondition_failure(_query_with([]), mutation, now=now, relation_exists=exists, environ=HOME)
    assert "not configured" in precondition_failure(_query_with(away), mutation, now=now, relation_exists=exists, environ={})
    assert precondition_failure(_query_with(home), {"precondition_json": {}}, now=now, relation_exists=exists, environ=HOME) is None


class _KeepOpen:
    """The sensor closes its warehouse; the test's must stay open."""

    def __init__(self, warehouse: PostgresWarehouse) -> None:
        self._warehouse = warehouse

    def __getattr__(self, name):
        return getattr(self._warehouse, name)

    def close(self) -> None:
        pass


def _sensor(warehouse: PostgresWarehouse, monkeypatch):
    from dagster import DagsterInstance, build_sensor_context

    from personal_data_warehouse.defs import upstream_mutations as defs

    monkeypatch.setattr(defs, "load_settings", lambda **_kwargs: object())
    monkeypatch.setattr(defs, "warehouse_from_settings", lambda _settings: _KeepOpen(warehouse))
    with DagsterInstance.ephemeral() as instance:
        return defs.upstream_mutation_sensor(build_sensor_context(instance=instance))


@pytest.mark.local_integration
def test_the_sensor_waits_for_a_scheduled_step_instead_of_starting_empty_runs(warehouse, monkeypatch) -> None:
    """A step approved days ahead is not work yet: counting it made the
    sensor start an empty Dagster run every 10 seconds until its time."""
    from dagster import RunRequest, SkipReason

    request_id = _seed(warehouse, not_before=datetime.now(tz=UTC) + timedelta(days=3))
    assert warehouse.approved_upstream_mutation_count(ensure_tables=False) == 0
    assert isinstance(_sensor(warehouse, monkeypatch), SkipReason)

    warehouse._command("UPDATE @upstream_mutations SET not_before = now() - interval '1 minute' WHERE request_id = %s", (request_id,))
    assert isinstance(_sensor(warehouse, monkeypatch), RunRequest)

    # A step whose deadline passed is work too: the claim must fail it.
    warehouse._command(
        "UPDATE @upstream_mutations SET not_before = now() + interval '3 days', not_after = now() - interval '1 minute' WHERE request_id = %s",
        (request_id,),
    )
    assert isinstance(_sensor(warehouse, monkeypatch), RunRequest)
