"""The worker's claim-time re-check of policy-approved mutations.

The Go app approves a thermostat change under a policy in the proposal's own
transaction (app/internal/mutations/policy_store.go); these tests seed the rows
it writes and drive the Python claim, which must re-check the kill switch, the
policy's mode and version, the grant, the limits and the manual-change pause
before a write, and send a failing request back to Zach's review.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import os
import threading
import uuid

import pytest

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.postgres import PostgresWarehouse, _jsonb_param
from personal_data_warehouse.upstream_mutation_policy import (
    POLICY_LOCK_PREFIX,
    SETTINGS_LOCK_ID,
    auto_approve_forced_off,
    parse_policy_actor,
)

POLICY = "heating-winter"
TOKEN = "tok_heating"
DEVICE = "DEV1"
CLAIMER = "resident:test:upstream_mutation_worker"


@pytest.fixture()
def warehouse():
    wh = PostgresWarehouse(os.environ["POSTGRES_DATABASE_URL"], schema=make_test_schema("mutpolicy"))
    try:
        wh.ensure_upstream_mutation_tables()
        wh.ensure_resideo_tables()
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def _seed_policy(wh: PostgresWarehouse, *, version: int = 1, mode: str = "enforce") -> None:
    now = datetime.now(tz=UTC)
    wh._command(
        """
        INSERT INTO @mutation_policies (id, version, operation, account, match_json, deny_json, limits_json, mode,
                                        valid_from, valid_until, created_by, approved_by)
        VALUES (%s, %s, 'resideo.set_thermostat', 'home', %s, %s, %s, %s, %s, %s, 'test', 'app:test')
        """,
        (
            POLICY,
            version,
            _jsonb_param({"heat_setpoint": {"min": 55, "max": 72}}),
            _jsonb_param({"heat_setpoint": {"below": 50}}),
            _jsonb_param({"max_per_day": 8, "max_per_hour": 1, "min_interval_minutes": 30,
                          "input_max_age_minutes": 30, "pause_after_manual_change_hours": 4}),
            mode,
            now - timedelta(days=1),
            now + timedelta(days=30),
        ),
    )


def _seed_grant(wh: PostgresWarehouse) -> None:
    wh._command(
        """
        INSERT INTO @mutation_policy_grants (id, policy_id, token_id, label, expires_at, created_by)
        VALUES ('grant-1', %s, %s, 'wf:heating-winter', %s, 'test')
        """,
        (POLICY, TOKEN, datetime.now(tz=UTC) + timedelta(days=30)),
    )


def _seed_policy_approved_request(wh: PostgresWarehouse, *, heat: float = 55.0, version: int = 1, approved_by: str | None = None) -> str:
    request_id = "req_" + uuid.uuid4().hex[:12]
    now = datetime.now(tz=UTC)
    actor = approved_by or f"policy:{POLICY}@{version}"
    wh._command(
        """
        INSERT INTO @upstream_mutation_requests (id, status, title, reason, idempotency_key, requested_by,
                                                 requested_by_token_id, approved_by, created_at, updated_at, approved_at)
        VALUES (%s, 'approved', 'Heat to away', 'left home', %s, 'wf:heating-winter', %s, %s, %s, %s, %s)
        """,
        (request_id, request_id, TOKEN, actor, now, now, now),
    )
    wh._command(
        """
        INSERT INTO @upstream_mutations (id, request_id, request_index, provider, operation, account, status, title, reason,
                                         payload_json, idempotency_key, requested_by, approved_by, created_at, updated_at, approved_at)
        VALUES (%s, %s, 0, 'resideo', 'resideo.set_thermostat', 'home', 'approved', 'Heat to away', 'left home',
                %s, '', 'wf:heating-winter', %s, %s, %s, %s)
        """,
        (request_id + "_m0", request_id, _jsonb_param({"device_id": DEVICE, "heat_setpoint": heat, "hold": "permanent"}), actor, now, now, now),
    )
    return request_id


def _claim(wh: PostgresWarehouse) -> list[dict]:
    return wh.claim_approved_upstream_mutations(limit=10, claimed_by=CLAIMER, providers=["resideo"])


def _request(wh: PostgresWarehouse, request_id: str) -> dict:
    return wh._query_dicts("SELECT * FROM @upstream_mutation_requests WHERE id = %s", (request_id,))[0]


def _mutation_status(wh: PostgresWarehouse, request_id: str) -> str:
    return str(wh._query_dicts("SELECT status FROM @upstream_mutations WHERE request_id = %s", (request_id,))[0]["status"])


def _sent_back(wh: PostgresWarehouse, request_id: str) -> dict:
    request = _request(wh, request_id)
    assert request["status"] == "pending_review", request
    assert request["approved_by"] == ""
    assert _mutation_status(wh, request_id) == "pending_review"
    events = wh._query_dicts(
        "SELECT event_type FROM @upstream_mutation_request_events WHERE request_id = %s", (request_id,)
    )
    assert "policy_recheck_failed" in {row["event_type"] for row in events}
    return request


def test_parse_policy_actor_and_the_env_switch() -> None:
    assert parse_policy_actor("policy:heating-winter@3") == ("heating-winter", 3)
    assert parse_policy_actor("app:test-phone") is None
    assert parse_policy_actor("policy:x@y") is None
    assert auto_approve_forced_off({"PDW_MUTATION_AUTO_APPROVE": "0"})
    assert not auto_approve_forced_off({})


@pytest.mark.local_integration
def test_a_policy_approved_change_whose_checks_still_hold_is_claimed(warehouse) -> None:
    _seed_policy(warehouse)
    _seed_grant(warehouse)
    request_id = _seed_policy_approved_request(warehouse)
    claimed = _claim(warehouse)
    assert [row["request_id"] for row in claimed] == [request_id]
    assert _mutation_status(warehouse, request_id) == "executing"


@pytest.mark.local_integration
def test_a_kill_switch_committed_before_the_claim_sends_the_change_back_to_review(warehouse) -> None:
    _seed_policy(warehouse)
    _seed_grant(warehouse)
    request_id = _seed_policy_approved_request(warehouse)
    warehouse._command(
        "INSERT INTO @mutation_policy_settings (key, value, updated_by) VALUES ('auto_approve', %s, 'app:test')",
        (_jsonb_param({"enabled": False}),),
    )
    assert _claim(warehouse) == []
    assert "switched off" in _sent_back(warehouse, request_id)["error"]


@pytest.mark.local_integration
def test_a_kill_switch_committed_after_the_claim_does_not_stop_that_write(warehouse) -> None:
    """The other order: the claim won, so this one write proceeds; Undo and
    the next run correct it."""
    _seed_policy(warehouse)
    _seed_grant(warehouse)
    request_id = _seed_policy_approved_request(warehouse)
    assert [row["request_id"] for row in _claim(warehouse)] == [request_id]
    warehouse._command(
        "INSERT INTO @mutation_policy_settings (key, value, updated_by) VALUES ('auto_approve', %s, 'app:test')",
        (_jsonb_param({"enabled": False}),),
    )
    assert _mutation_status(warehouse, request_id) == "executing"
    # And a later policy-approved change is now held back.
    later = _seed_policy_approved_request(warehouse, heat=68.0)
    assert _claim(warehouse) == []
    _sent_back(warehouse, later)


@pytest.mark.local_integration
def test_a_revoke_committed_before_the_claim_sends_the_change_back_to_review(warehouse) -> None:
    _seed_policy(warehouse)
    _seed_grant(warehouse)
    request_id = _seed_policy_approved_request(warehouse)
    warehouse._command("UPDATE @mutation_policy_grants SET revoked_at = now(), revoked_by = 'app:test'")
    assert _claim(warehouse) == []
    assert "revoked" in _sent_back(warehouse, request_id)["error"]


@pytest.mark.local_integration
def test_a_revoke_committed_after_the_claim_does_not_stop_that_write(warehouse) -> None:
    _seed_policy(warehouse)
    _seed_grant(warehouse)
    request_id = _seed_policy_approved_request(warehouse)
    assert [row["request_id"] for row in _claim(warehouse)] == [request_id]
    warehouse._command("UPDATE @mutation_policy_grants SET revoked_at = now(), revoked_by = 'app:test'")
    assert _mutation_status(warehouse, request_id) == "executing"


@pytest.mark.local_integration
def test_the_env_override_wins_over_the_settings_row(warehouse, monkeypatch) -> None:
    _seed_policy(warehouse)
    _seed_grant(warehouse)
    warehouse._command(
        "INSERT INTO @mutation_policy_settings (key, value, updated_by) VALUES ('auto_approve', %s, 'app:test')",
        (_jsonb_param({"enabled": True}),),
    )
    request_id = _seed_policy_approved_request(warehouse)
    monkeypatch.setenv("PDW_MUTATION_AUTO_APPROVE", "0")
    assert _claim(warehouse) == []
    assert "PDW_MUTATION_AUTO_APPROVE" in _sent_back(warehouse, request_id)["error"]


@pytest.mark.local_integration
def test_a_policy_changed_or_paused_since_approval_sends_the_change_back(warehouse) -> None:
    _seed_policy(warehouse)
    _seed_grant(warehouse)
    first = _seed_policy_approved_request(warehouse)
    _seed_policy(warehouse, version=2, mode="dry_run")
    assert _claim(warehouse) == []
    assert "dry_run" in _sent_back(warehouse, first)["error"]


@pytest.mark.local_integration
def test_a_hand_change_inside_the_pause_window_sends_the_change_back(warehouse) -> None:
    _seed_policy(warehouse)
    _seed_grant(warehouse)
    now = datetime.now(tz=UTC)
    for observed_at, heat in ((now - timedelta(hours=3), 68.0), (now - timedelta(hours=1), 70.0)):
        warehouse._command(
            """
            INSERT INTO @resideo_thermostat_readings (account, device_id, observed_at, heat_setpoint, cool_setpoint)
            VALUES ('home', %s, %s, %s, 78)
            """,
            (DEVICE, observed_at, heat),
        )
    request_id = _seed_policy_approved_request(warehouse)
    assert _claim(warehouse) == []
    assert "pauses it" in _sent_back(warehouse, request_id)["error"]


@pytest.mark.local_integration
def test_a_rate_limit_already_used_by_another_claimed_change_sends_the_change_back(warehouse) -> None:
    _seed_policy(warehouse)
    _seed_grant(warehouse)
    first = _seed_policy_approved_request(warehouse)
    assert [row["request_id"] for row in _claim(warehouse)] == [first]
    second = _seed_policy_approved_request(warehouse, heat=68.0)
    assert _claim(warehouse) == []
    assert "limit" in _sent_back(warehouse, second)["error"] or "minutes between" in _request(warehouse, second)["error"]


@pytest.mark.local_integration
def test_a_change_zach_approved_by_hand_is_never_rechecked(warehouse) -> None:
    warehouse._command(
        "INSERT INTO @mutation_policy_settings (key, value, updated_by) VALUES ('auto_approve', %s, 'app:test')",
        (_jsonb_param({"enabled": False}),),
    )
    request_id = _seed_policy_approved_request(warehouse, approved_by="app:test-phone")
    assert [row["request_id"] for row in _claim(warehouse)] == [request_id]


@pytest.mark.local_integration
def test_a_delayed_queue_of_several_changes_runs_one_per_the_limits(warehouse) -> None:
    """Three policy-approved changes waiting together (the worker was down):
    one claim pass runs one, and the hourly limit and spacing send the rest
    back, because each claim is recorded before the next is checked."""
    _seed_policy(warehouse)
    _seed_grant(warehouse)
    queued = [_seed_policy_approved_request(warehouse, heat=heat) for heat in (55.0, 60.0, 68.0)]
    claimed = _claim(warehouse)
    assert len(claimed) == 1
    for request_id in queued:
        if request_id != claimed[0]["request_id"]:
            assert _request(warehouse, request_id)["status"] == "pending_review"


@pytest.mark.local_integration
def test_a_policy_approved_batch_is_never_claimed(warehouse) -> None:
    _seed_policy(warehouse)
    _seed_grant(warehouse)
    request_id = _seed_policy_approved_request(warehouse)
    now = datetime.now(tz=UTC)
    warehouse._command(
        """
        INSERT INTO @upstream_mutations (id, request_id, request_index, provider, operation, account, status, title, reason,
                                         payload_json, idempotency_key, requested_by, approved_by, created_at, updated_at, approved_at)
        SELECT id || '_second', request_id, 1, provider, operation, account, status, title, reason, payload_json, '',
               requested_by, approved_by, created_at, updated_at, approved_at
        FROM @upstream_mutations WHERE request_id = %s
        """,
        (request_id,),
    )
    assert now
    assert _claim(warehouse) == []
    assert "changes is never run under a policy" in _sent_back(warehouse, request_id)["error"]


def _writer(warehouse: PostgresWarehouse) -> PostgresWarehouse:
    """A second connection on the same test schema, as the Go app would be."""
    return PostgresWarehouse(os.environ["POSTGRES_DATABASE_URL"], schema=warehouse._schema)


def _policy_off_holding_the_lock(writer: PostgresWarehouse) -> None:
    """What the Go app does to turn the policy off (setPolicyMode under
    lockPolicies), left uncommitted."""
    writer._command("BEGIN")
    writer._query_dicts("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0)) AS locked", (POLICY_LOCK_PREFIX + POLICY,))
    writer._command(
        """
        INSERT INTO @mutation_policies (id, version, operation, account, match_json, deny_json, limits_json, mode,
                                        valid_from, valid_until, created_by, approved_by)
        SELECT id, version + 1, operation, account, match_json, deny_json, limits_json, 'off',
               valid_from, valid_until, 'app:test', 'app:test'
        FROM @mutation_policies WHERE id = %s ORDER BY version DESC LIMIT 1
        """,
        (POLICY,),
    )


@pytest.mark.local_integration
def test_a_policy_off_committing_while_the_claim_waits_sends_the_change_back(warehouse) -> None:
    """Overlapping, writer first: the claim blocks on the policy's lock until
    the switch-off commits, then sees it."""
    _seed_policy(warehouse)
    _seed_grant(warehouse)
    request_id = _seed_policy_approved_request(warehouse)
    writer = _writer(warehouse)
    try:
        _policy_off_holding_the_lock(writer)
        result: dict[str, object] = {}
        claimer = threading.Thread(target=lambda: result.setdefault("claimed", _claim(warehouse)))
        claimer.start()
        claimer.join(timeout=0.5)
        assert claimer.is_alive(), "the claim did not wait for the policy's lock"
        writer._command("COMMIT")
        claimer.join(timeout=10)
        assert result["claimed"] == []
        assert "is off now" in _sent_back(warehouse, request_id)["error"]
    finally:
        writer.close()


@pytest.mark.local_integration
def test_a_policy_off_started_during_the_claim_waits_and_does_not_stop_that_write(warehouse, monkeypatch) -> None:
    """Overlapping, claim first: the switch-off blocks on the lock the claim
    holds, the claimed write proceeds, and the switch-off lands after."""
    import personal_data_warehouse.postgres as postgres_module

    _seed_policy(warehouse)
    _seed_grant(warehouse)
    request_id = _seed_policy_approved_request(warehouse)
    checked, release = threading.Event(), threading.Event()
    original = postgres_module.policy_recheck_failure

    def paused(*args, **kwargs):
        reason = original(*args, **kwargs)
        checked.set()
        release.wait(timeout=10)
        return reason

    monkeypatch.setattr(postgres_module, "policy_recheck_failure", paused)
    writer = _writer(warehouse)
    try:
        result: dict[str, object] = {}
        claimer = threading.Thread(target=lambda: result.setdefault("claimed", _claim(warehouse)))
        claimer.start()
        assert checked.wait(timeout=10)
        off = threading.Thread(target=lambda: (_policy_off_holding_the_lock(writer), writer._command("COMMIT")))
        off.start()
        off.join(timeout=0.5)
        assert off.is_alive(), "the switch-off did not wait for the claim's lock"
        release.set()
        claimer.join(timeout=10)
        off.join(timeout=10)
        assert not off.is_alive()
        assert [row["request_id"] for row in result["claimed"]] == [request_id]
        assert _mutation_status(warehouse, request_id) == "executing"
        latest = warehouse._query_dicts("SELECT mode FROM @mutation_policies WHERE id = %s ORDER BY version DESC LIMIT 1", (POLICY,))
        assert latest[0]["mode"] == "off"
    finally:
        release.set()
        writer.close()


@pytest.mark.local_integration
def test_a_first_kill_switch_row_committing_while_the_claim_waits_wins(warehouse) -> None:
    """No settings row exists yet, so FOR SHARE has nothing to lock: the
    claim and the switch serialize on the settings lock instead."""
    _seed_policy(warehouse)
    _seed_grant(warehouse)
    request_id = _seed_policy_approved_request(warehouse)
    writer = _writer(warehouse)
    try:
        writer._command("BEGIN")
        writer._query_dicts("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0)) AS locked", (POLICY_LOCK_PREFIX + SETTINGS_LOCK_ID,))
        writer._command(
            "INSERT INTO @mutation_policy_settings (key, value, updated_by) VALUES ('auto_approve', %s, 'app:test')",
            (_jsonb_param({"enabled": False}),),
        )
        result: dict[str, object] = {}
        claimer = threading.Thread(target=lambda: result.setdefault("claimed", _claim(warehouse)))
        claimer.start()
        claimer.join(timeout=0.5)
        assert claimer.is_alive(), "the claim did not wait for the kill switch's lock"
        writer._command("COMMIT")
        claimer.join(timeout=10)
        assert result["claimed"] == []
        assert "switched off" in _sent_back(warehouse, request_id)["error"]
    finally:
        writer.close()


@pytest.mark.local_integration
def test_a_policy_counts_only_its_own_claims_not_a_lookalike_id(warehouse, monkeypatch) -> None:
    """approved_by is matched as an exact 'policy:<id>@' prefix, never with
    LIKE, where '_' in an id would match any character."""
    import sys

    monkeypatch.setattr(sys.modules[__name__], "POLICY", "heating_winter")
    _seed_policy(warehouse)
    warehouse._command(
        "INSERT INTO @mutation_policy_grants (id, policy_id, token_id, label, expires_at, created_by) VALUES ('grant-u', %s, %s, 'x', %s, 'test')",
        (POLICY, TOKEN, datetime.now(tz=UTC) + timedelta(days=30)),
    )
    lookalike = "heating-winter"
    now = datetime.now(tz=UTC)
    warehouse._command(
        """
        INSERT INTO @upstream_mutations (id, request_id, request_index, provider, operation, account, status, title, reason,
                                         payload_json, idempotency_key, requested_by, approved_by, claimed_at, created_at, updated_at, approved_at)
        VALUES ('other_m0', 'other', 0, 'resideo', 'resideo.set_thermostat', 'home', 'succeeded', 't', 'r', '{}', '', 'x', %s, %s, %s, %s, %s)
        """,
        (f"policy:{lookalike}@1", now, now, now, now),
    )
    request_id = _seed_policy_approved_request(warehouse)
    assert [row["request_id"] for row in _claim(warehouse)] == [request_id]
