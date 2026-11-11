"""Resideo thermostat source: parsing, the changes-only readings, the executor."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from urllib.parse import parse_qs, urlparse

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn
import pytest

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.defs.upstream_mutations import (
    LOCAL_ONLY_MUTATION_PROVIDERS,
    RECLAIMABLE_IDEMPOTENT_OPERATIONS,
    process_upstream_mutation_batch,
)
from personal_data_warehouse.postgres import PostgresWarehouse

from personal_data_warehouse.resideo_api import (
    OAUTH_REDIRECT_URI,
    ResideoAuthError,
    ResideoClient,
    ResideoRequestError,
    ResideoTransientError,
    WarehouseTokenSource,
    authorization_code_from_redirect,
    build_authorize_request,
    login_with_password,
)
from personal_data_warehouse.resideo_mutations import (
    RESIDEO_PROVIDER,
    RESIDEO_SET_THERMOSTAT_OPERATION,
    ResideoMutationExecutor,
)
from personal_data_warehouse.resideo_sync import ResideoSyncRunner, thermostat_state

NOW = datetime(2026, 11, 6, 20, 0, tzinfo=UTC)
DEVICE = "AABBCCDDEEFF"


def shadow(*, indoor=70.0, heat=66.0, cool=80.0, status="NoHold", mode="Heat", online=True, outdoor=41.0, schedule=True):
    reported = {
        "DeviceName": "Thermostat",
        "SystemSwitch": mode,
        "Setpoint": {"SetpointStatus": status, "HeatSetpoint": heat, "CoolSetpoint": cool},
        "DisplayedIndoorTemperature": indoor,
        "DisplayedIndoorHumidity": 40,
        "FanSwitch": {"Position": "Auto"},
        "OperationStatus": {"Mode": "Off"},
        "Online": online,
        "ScheduleEnabled": schedule,
        "BackLightState": "Active",
    }
    if outdoor is not None:
        reported["DisplayedOutdoorTemperature"] = outdoor
    return {"DeviceId": DEVICE, "Reported": reported}


CONFIGURATION = {
    "DeviceId": DEVICE,
    "Reported": {
        "MinimumHeatSetpointAllowed": 50.0,
        "MaximumHeatSetpointAllowed": 90.0,
        "MinimumCoolSetpointAllowed": 50.0,
        "MaximumCoolSetpointAllowed": 90.0,
        "TemperatureUnits": "F",
    },
}


class FakeClient:
    def __init__(self, shadows):
        self.shadows = list(shadows)
        self.writes = []

    def list_thermostats(self):
        return [{"device_id": DEVICE, "name": "Thermostat", "location_name": "Home", "device_type": "T5"}]

    def get_device(self, device_id):
        return self.shadows.pop(0) if len(self.shadows) > 1 else self.shadows[0]

    def get_configuration(self, device_id):
        return CONFIGURATION

    def set_setpoint(self, device_id, *, kind, value, setpoint_status):
        self.writes.append((kind, value, setpoint_status))
        return {"TransactionId": "t"}


class FakeWarehouse:
    def __init__(self):
        self.thermostats = []
        self.readings = []
        self.state = []

    def latest_resideo_reading_sha256(self, *, account, device_id):
        return self.readings[-1]["state_sha256"] if self.readings else ""

    def insert_resideo_thermostats(self, rows):
        self.thermostats.extend(rows)

    def insert_resideo_thermostat_readings(self, rows):
        self.readings.extend(rows)

    def insert_resideo_sync_state(self, **row):
        self.state.append(row)


def run_sync(warehouse, client, now=NOW):
    return ResideoSyncRunner(warehouse=warehouse, client=client, account="home", now=lambda: now).sync()


def test_thermostat_state_translates_the_reported_shadow():
    state = thermostat_state(shadow(status="TemporaryHold"), CONFIGURATION)
    assert state["indoor_temperature"] == 70.0
    assert state["heat_setpoint"] == 66.0
    assert state["cool_setpoint"] == 80.0
    assert state["system_mode"] == "Heat"
    assert state["operation_mode"] == "Off"
    assert state["fan_mode"] == "Auto"
    assert state["setpoint_status"] == "TemporaryHold"
    assert state["is_online"] == 1
    assert state["temperature_units"] == "F"
    assert state["min_heat_setpoint"] == 50.0
    assert state["max_cool_setpoint"] == 90.0


def test_an_absent_outdoor_reading_says_so_instead_of_reading_as_zero_degrees():
    assert thermostat_state(shadow(outdoor=None), CONFIGURATION)["has_outdoor_reading"] == 0
    present = thermostat_state(shadow(outdoor=0.0), CONFIGURATION)
    assert (present["has_outdoor_reading"], present["outdoor_temperature"]) == (1, 0.0)


def test_a_shadow_without_an_indoor_temperature_is_refused_not_stored_as_zero():
    broken = shadow()
    del broken["Reported"]["DisplayedIndoorTemperature"]
    with pytest.raises(ResideoRequestError, match="DisplayedIndoorTemperature"):
        thermostat_state(broken, CONFIGURATION)


def test_only_a_changed_state_writes_a_reading_while_every_poll_refreshes_the_device():
    warehouse = FakeWarehouse()
    run_sync(warehouse, FakeClient([shadow()]))
    run_sync(warehouse, FakeClient([shadow()]), now=NOW + timedelta(minutes=5))
    summary = run_sync(warehouse, FakeClient([shadow(indoor=71.0)]), now=NOW + timedelta(minutes=10))

    assert [row["indoor_temperature"] for row in warehouse.readings] == [70.0, 71.0]
    assert [row["observed_at"] for row in warehouse.readings] == [NOW, NOW + timedelta(minutes=10)]
    assert len(warehouse.thermostats) == 3
    assert warehouse.thermostats[-1]["last_polled_at"] == NOW + timedelta(minutes=10)
    assert summary.readings_written == 1
    assert warehouse.state[-1]["status"] == "ok"


def test_a_field_outside_the_tracked_state_does_not_write_a_reading():
    warehouse = FakeWarehouse()
    run_sync(warehouse, FakeClient([shadow()]))
    noisy = shadow()
    noisy["Reported"]["BackLightState"] = "Dim"
    run_sync(warehouse, FakeClient([noisy]), now=NOW + timedelta(minutes=5))
    assert len(warehouse.readings) == 1


def test_an_account_with_no_thermostat_is_a_failure_not_a_quiet_green_run():
    class Empty(FakeClient):
        def list_thermostats(self):
            return []

    warehouse = FakeWarehouse()
    with pytest.raises(ResideoRequestError, match="no devices"):
        run_sync(warehouse, Empty([shadow()]))
    assert warehouse.state[-1]["status"] == "failed"
    assert "no devices" in warehouse.state[-1]["error"]


def test_a_refused_credential_records_action_required_and_reraises():
    class Refused(FakeClient):
        def list_thermostats(self):
            raise ResideoAuthError("refused")

    warehouse = FakeWarehouse()
    with pytest.raises(ResideoAuthError):
        run_sync(warehouse, Refused([shadow()]))
    assert warehouse.state[-1]["status"] == "action_required"


def mutation(**payload):
    defaults = {"device_id": DEVICE, "heat_setpoint": 68.0, "hold": "temporary"}
    return {
        "id": "m1",
        "provider": RESIDEO_PROVIDER,
        "operation": RESIDEO_SET_THERMOSTAT_OPERATION,
        "approved_at": NOW - timedelta(seconds=10),
        "payload_json": {**defaults, **payload},
    }


def executor(client, warehouse=None):
    return ResideoMutationExecutor(
        warehouse=warehouse or FakeWarehouse(),
        account="home",
        client_factory=lambda: client,
        now=lambda: NOW,
        sleep=lambda seconds: None,
    )


def test_set_thermostat_writes_then_verifies_by_reading_the_device_back():
    client = FakeClient([shadow(), shadow(), shadow(heat=68.0, status="TemporaryHold")])
    warehouse = FakeWarehouse()
    result = executor(client, warehouse).execute(mutation())

    assert client.writes == [("heat", 68.0, "TemporaryHold")]
    assert result.status == "succeeded"
    assert result.result_json["verified"] is True
    assert result.result_json["previous"]["heat_setpoint"] == 66.0
    assert result.result_json["current"]["heat_setpoint"] == 68.0
    assert warehouse.readings[-1]["heat_setpoint"] == 68.0


def test_a_permanent_hold_is_sent_as_one():
    client = FakeClient([shadow(), shadow(cool=76.0, status="PermanentHold")])
    executor(client).execute(mutation(heat_setpoint=None, cool_setpoint=76.0, hold="permanent"))
    assert client.writes == [("cool", 76.0, "PermanentHold")]


def test_with_no_schedule_on_a_temporary_hold_is_sent_as_permanent():
    """The real T5 runs this way: there is no next period for it to end at."""

    client = FakeClient([shadow(schedule=False), shadow(heat=68.0, status="PermanentHold", schedule=False)])
    result = executor(client).execute(mutation())
    assert client.writes == [("heat", 68.0, "PermanentHold")]
    assert result.result_json["verified"] is True


def test_a_thermostat_with_no_humidity_sensor_stores_zero_and_stays_on_the_state():
    t5 = shadow()
    t5["Reported"].pop("DisplayedIndoorHumidity")
    assert thermostat_state(t5, CONFIGURATION)["indoor_humidity"] == 0


def test_a_write_the_device_has_not_shown_yet_succeeds_unverified():
    client = FakeClient([shadow()])
    result = executor(client).execute(mutation())
    assert result.status == "succeeded"
    assert result.result_json["verified"] is False
    assert len(client.writes) == 1


def test_a_setpoint_already_held_writes_nothing():
    client = FakeClient([shadow(heat=68.0, status="TemporaryHold")])
    result = executor(client).execute(mutation())
    assert client.writes == []
    assert result.status == "succeeded"
    assert result.result_json["already_set"] is True


def test_a_setpoint_outside_the_devices_limits_is_refused_without_a_write():
    client = FakeClient([shadow()])
    result = executor(client).execute(mutation(heat_setpoint=45.0))
    assert client.writes == []
    assert result.status == "failed_terminal"
    assert "50" in result.error


def test_an_offline_thermostat_is_retried_without_a_write():
    client = FakeClient([shadow(online=False)])
    result = executor(client).execute(mutation())
    assert client.writes == []
    assert result.status == "failed_retryable"


def test_an_approval_too_old_to_mean_now_is_never_applied():
    """A retry that outlives the moment must not change the heat a day later."""

    client = FakeClient([shadow()])
    old = mutation()
    old["approved_at"] = NOW - timedelta(hours=2)
    result = executor(client).execute(old)
    assert client.writes == []
    assert result.status == "failed_terminal"
    assert "propose" in result.error


def test_a_policy_approved_setpoint_outside_the_fixed_cap_is_refused_without_a_write():
    """The executor's own cap (heat 50 to 72) holds whatever a policy says."""

    for payload in ({"heat_setpoint": 74.0}, {"heat_setpoint": 49.0}, {"heat_setpoint": None, "cool_setpoint": 76.0}):
        client = FakeClient([shadow()])
        capped = mutation(**payload)
        capped["approved_by"] = "policy:heating-winter@3"
        result = executor(client).execute(capped)
        assert client.writes == [], payload
        assert result.status == "failed_terminal", payload
        assert "Zach's own approval" in result.error

    # The same value approved by Zach is applied: the cap is only on policies.
    client = FakeClient([shadow(), shadow(heat=74.0, status="TemporaryHold")])
    by_hand = mutation(heat_setpoint=74.0)
    by_hand["approved_by"] = "app:test-phone"
    assert executor(client).execute(by_hand).status == "succeeded"
    assert client.writes == [("heat", 74.0, "TemporaryHold")]

    client = FakeClient([shadow(), shadow(heat=55.0, status="TemporaryHold")])
    inside = mutation(heat_setpoint=55.0)
    inside["approved_by"] = "policy:heating-winter@3"
    assert executor(client).execute(inside).status == "succeeded"


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (ResideoAuthError("refused"), "blocked_missing_credentials"),
        (ResideoTransientError("503"), "failed_retryable"),
        (ResideoRequestError("400"), "failed_terminal"),
    ],
)
def test_each_upstream_failure_maps_to_its_mutation_status(error, status):
    class Failing(FakeClient):
        def set_setpoint(self, device_id, **kwargs):
            raise error

    result = executor(Failing([shadow()])).execute(mutation())
    assert result.status == status


def test_a_missing_api_key_blocks_the_mutation_instead_of_failing_it():
    def no_client():
        raise ResideoAuthError("RESIDEO_API_SUBSCRIPTION_KEY is not set")

    result = ResideoMutationExecutor(
        warehouse=FakeWarehouse(), account="home", client_factory=no_client, now=lambda: NOW, sleep=lambda s: None
    ).execute(mutation())
    assert result.status == "blocked_missing_credentials"


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.content = json.dumps(self._payload).encode()

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


class FakeTokens:
    def __init__(self):
        self.forced = 0

    def access_token(self, *, force_refresh=False):
        self.forced += int(force_refresh)
        return "fresh" if force_refresh else "stale"


def test_a_setpoint_command_always_carries_its_value_status_and_channel():
    session = FakeSession([FakeResponse(202, {"TransactionId": "t"})])
    client = ResideoClient(tokens=FakeTokens(), subscription_key="key", session=session)
    client.set_setpoint(DEVICE, kind="heat", value=68, setpoint_status="TemporaryHold")

    method, url, kwargs = session.calls[0]
    assert method == "PUT"
    assert url.endswith(f"/v1/devices/thermostats/{DEVICE}/heatSetpoint")
    assert kwargs["json"] == {
        "SetpointStatus": "TemporaryHold",
        "SetpointValue": 68.0,
        "ChannelId": "ds-notification-service",
    }
    assert kwargs["headers"]["Ocp-Apim-Subscription-Key"] == "key"


def test_a_401_refreshes_once_and_a_second_refusal_is_an_auth_error():
    tokens = FakeTokens()
    session = FakeSession([FakeResponse(401), FakeResponse(200, {"DeviceId": DEVICE})])
    assert ResideoClient(tokens=tokens, subscription_key="k", session=session).get_device(DEVICE) == {"DeviceId": DEVICE}
    assert tokens.forced == 1
    assert session.calls[1][2]["headers"]["Authorization"] == "Bearer fresh"

    session = FakeSession([FakeResponse(401), FakeResponse(401)])
    with pytest.raises(ResideoAuthError):
        ResideoClient(tokens=FakeTokens(), subscription_key="k", session=session).get_device(DEVICE)


def test_the_account_graph_flattens_to_devices():
    graph = {
        "data": {
            "consumerUsers": [
                {
                    "consumerAccount": {
                        "locations": [
                            {
                                "name": "Home",
                                "consumerDevices": [
                                    {"name": "Thermostat", "device": {"deviceId": DEVICE, "globalDeviceType": "T5"}},
                                    {"name": "No id", "device": {}},
                                ],
                            }
                        ]
                    }
                }
            ]
        }
    }
    client = ResideoClient(tokens=FakeTokens(), subscription_key="k", session=FakeSession([FakeResponse(200, graph)]))
    assert client.list_thermostats() == [
        {"device_id": DEVICE, "name": "Thermostat", "location_name": "Home", "device_type": "T5"}
    ]


def test_a_fresh_token_is_reused_and_a_stale_one_rotates_through_the_warehouse():
    class Store:
        def __init__(self, expires_at):
            self.token = json.dumps({"access_token": "a", "refresh_token": "r", "expires_at": expires_at})
            self.rotations = 0

        def load_resideo_oauth_token(self, *, account):
            return self.token

        def rotate_resideo_oauth_token(self, *, account, expected_token_json, rotate, updated_at):
            self.rotations += 1
            return json.dumps({"access_token": "b", "refresh_token": "r2", "expires_at": 9e9})

    fresh = Store(expires_at=10_000)
    assert WarehouseTokenSource(warehouse=fresh, account="home", now=lambda: 1000).access_token() == "a"
    assert fresh.rotations == 0
    stale = Store(expires_at=1100)
    assert WarehouseTokenSource(warehouse=stale, account="home", now=lambda: 1000).access_token() == "b"
    assert stale.rotations == 1


def test_a_missing_credential_names_the_command_that_installs_one():
    class Empty:
        def load_resideo_oauth_token(self, *, account):
            return ""

    with pytest.raises(ResideoAuthError, match="personal-data-warehouse-resideo-auth"):
        WarehouseTokenSource(warehouse=Empty(), account="home").access_token()


def test_the_browser_sign_in_checks_the_state_of_the_pasted_redirect():
    request = build_authorize_request()
    assert "code_challenge=" in request.url
    redirect = f"{OAUTH_REDIRECT_URI}?code=abc&state={request.state}"
    assert authorization_code_from_redirect(redirect, expected_state=request.state) == "abc"
    with pytest.raises(ResideoAuthError, match="different sign-in"):
        authorization_code_from_redirect(f"{OAUTH_REDIRECT_URI}?code=abc&state=other", expected_state=request.state)
    with pytest.raises(ResideoAuthError, match="no code"):
        authorization_code_from_redirect(OAUTH_REDIRECT_URI, expected_state=request.state)


# --- the real warehouse ---------------------------------------------------


def _ensure(wh: PostgresWarehouse) -> None:
    wh.ensure_upstream_mutation_tables()
    wh.ensure_resideo_tables()


@pytest.fixture()
def warehouse():
    wh = PostgresWarehouse(os.environ["POSTGRES_DATABASE_URL"], schema=make_test_schema("resideo"))
    try:
        _ensure(wh)
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def test_readings_round_trip_with_only_changes_stored(warehouse) -> None:
    run_sync(warehouse, FakeClient([shadow()]))
    run_sync(warehouse, FakeClient([shadow()]), now=NOW + timedelta(minutes=5))
    run_sync(warehouse, FakeClient([shadow(indoor=71.0, outdoor=None)]), now=NOW + timedelta(minutes=10))

    rows = warehouse._query(
        "SELECT indoor_temperature, outdoor_temperature, has_outdoor_reading, heat_setpoint, system_mode "
        "FROM @resideo_thermostat_readings ORDER BY observed_at"
    )
    assert [tuple(row) for row in rows] == [(70.0, 41.0, 1, 66.0, "Heat"), (71.0, 0.0, 0, 66.0, "Heat")]
    device = warehouse._query(
        "SELECT name, location_name, indoor_temperature, min_heat_setpoint, last_polled_at FROM @resideo_thermostats"
    )
    assert [tuple(row) for row in device] == [("Thermostat", "Home", 71.0, 50.0, NOW + timedelta(minutes=10))]


def test_a_failed_poll_keeps_the_last_success(warehouse) -> None:
    class Refused(FakeClient):
        def list_thermostats(self):
            raise ResideoAuthError("refused")

    run_sync(warehouse, FakeClient([shadow()]))
    with pytest.raises(ResideoAuthError):
        run_sync(warehouse, Refused([shadow()]), now=NOW + timedelta(minutes=5))
    rows = warehouse._query("SELECT status, error, last_success_at, updated_at FROM @resideo_sync_state")
    assert [tuple(row) for row in rows] == [("action_required", "refused", NOW, NOW + timedelta(minutes=5))]


def _token(access: str, refresh: str) -> str:
    return json.dumps({"access_token": access, "refresh_token": refresh, "expires_at": 1})


def test_a_failed_refresh_leaves_the_stored_token_untouched(warehouse) -> None:
    warehouse.replace_resideo_oauth_token(account="home", token_json=_token("a", "r1"), updated_at=NOW)

    def refuse(current):
        raise ResideoAuthError("invalid_grant")

    with pytest.raises(ResideoAuthError):
        warehouse.rotate_resideo_oauth_token(
            account="home", expected_token_json=_token("a", "r1"), rotate=refuse, updated_at=NOW
        )
    assert warehouse.load_resideo_oauth_token(account="home") == _token("a", "r1")


def test_two_racing_refreshes_spend_the_refresh_token_once(warehouse) -> None:
    """Auth0 revokes the whole grant when a refresh token is used twice."""

    warehouse.replace_resideo_oauth_token(account="home", token_json=_token("a", "r1"), updated_at=NOW)
    calls = []
    entered = threading.Event()
    release = threading.Event()

    def slow_rotate(current):
        calls.append(current)
        entered.set()
        release.wait(5)
        return _token("b", "r2")

    results = []

    def race(rotate):
        results.append(
            warehouse.rotate_resideo_oauth_token(
                account="home", expected_token_json=_token("a", "r1"), rotate=rotate, updated_at=NOW
            )
        )

    first = threading.Thread(target=race, args=(slow_rotate,))
    first.start()
    assert entered.wait(5)
    second = threading.Thread(target=race, args=(slow_rotate,))
    second.start()
    # The second caller is parked on the row lock, not calling Auth0.
    second.join(0.3)
    assert second.is_alive()
    release.set()
    first.join(5)
    second.join(5)

    assert len(calls) == 1
    assert results == [_token("b", "r2"), _token("b", "r2")]
    assert warehouse.load_resideo_oauth_token(account="home") == _token("b", "r2")


def test_the_worker_routes_a_thermostat_mutation_to_its_executor_and_can_reclaim_it() -> None:
    assert (RESIDEO_PROVIDER, RESIDEO_SET_THERMOSTAT_OPERATION) in RECLAIMABLE_IDEMPOTENT_OPERATIONS
    assert RESIDEO_PROVIDER not in LOCAL_ONLY_MUTATION_PROVIDERS


@contextmanager
def _isolated_database():
    """A database of its own at the default layout, which the Go store reads."""
    admin = psycopg2.connect(os.environ["POSTGRES_DATABASE_URL"])
    admin.autocommit = True
    database = make_test_schema("resideo")
    wh = None
    with admin.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    try:
        url = make_dsn(os.environ["POSTGRES_DATABASE_URL"], dbname=database)
        wh = PostgresWarehouse(url)
        _ensure(wh)
        yield wh, url
    finally:
        if wh is not None:
            wh.close()
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
        admin.close()


@pytest.mark.local_integration
def test_a_proposal_is_reviewed_approved_and_applied_by_the_worker() -> None:
    with _isolated_database() as (wh, url):
        run_sync(wh, FakeClient([shadow()]), now=datetime.now(tz=UTC))
        name = "TestThermostatProposalReviewApproval"
        result = subprocess.run(
            ["go", "test", "./internal/mutations", "-run", f"^{name}$", "-count=1", "-v"],
            cwd=Path(__file__).resolve().parents[1] / "app",
            env={**os.environ, "PDW_RESIDEO_TEST_URL": url},
            capture_output=True,
            text=True,
            timeout=180,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        # A skipped Go test exits 0 too; the proposal half must actually run.
        assert f"--- PASS: {name}" in result.stdout, result.stdout + result.stderr

        client = FakeClient([shadow(), shadow(heat=68.0, status="TemporaryHold")])
        summary = process_upstream_mutation_batch(
            warehouse=wh,
            gmail_executor=None,  # type: ignore[arg-type]
            contact_executor=None,  # type: ignore[arg-type]
            calendar_executor=None,  # type: ignore[arg-type]
            slack_executor=None,  # type: ignore[arg-type]
            screen_timelapse_executor=None,  # type: ignore[arg-type]
            resideo_executor=ResideoMutationExecutor(
                warehouse=wh, account="home", client_factory=lambda: client, sleep=lambda seconds: None
            ),
            limit=10,
            claimed_by="resideo-test",
            ensure_tables=False,
            on_gmail_settings_changed=None,
        )
        assert summary.succeeded == 1
        assert client.writes == [("heat", 68.0, "TemporaryHold")]
        status, stored, approved_at = wh._query(
            "SELECT status, result_json, approved_at FROM @upstream_mutations WHERE operation = %s",
            (RESIDEO_SET_THERMOSTAT_OPERATION,),
        )[0]
        assert status == "succeeded"
        # What the worker's approval-age check reads, on the row the API approved.
        assert datetime.now(tz=UTC) - approved_at < timedelta(minutes=5)
        assert stored["verified"] is True
        newest = wh._query(
            "SELECT heat_setpoint, setpoint_status FROM @resideo_thermostat_readings ORDER BY observed_at DESC LIMIT 1"
        )
        assert tuple(newest[0]) == (68.0, "TemporaryHold")


def test_the_sensor_sits_out_until_the_api_key_is_set_and_then_polls_on_its_interval(monkeypatch) -> None:
    from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

    from personal_data_warehouse.defs import resideo_sync as defs_module

    monkeypatch.setattr(defs_module, "skip_if_job_in_progress", lambda context, job_name: None)
    monkeypatch.delenv("RESIDEO_API_SUBSCRIPTION_KEY", raising=False)
    with DagsterInstance.ephemeral() as instance:
        skipped = defs_module.resideo_sync_sensor(build_sensor_context(instance=instance))
        assert isinstance(skipped, SkipReason)
        assert "RESIDEO_API_SUBSCRIPTION_KEY" in skipped.skip_message

        monkeypatch.setenv("RESIDEO_API_SUBSCRIPTION_KEY", "key")
        monkeypatch.setenv("POSTGRES_DATABASE_URL", "postgres://unused")

        class Settings:
            class resideo:
                enabled = True
                poll_interval_seconds = 300

        monkeypatch.setattr(defs_module, "load_settings", lambda **kwargs: Settings)
        assert isinstance(defs_module.resideo_sync_sensor(build_sensor_context(instance=instance)), RunRequest)
        recent = build_sensor_context(instance=instance, cursor=str(time.time()))
        assert isinstance(defs_module.resideo_sync_sensor(recent), SkipReason)


class LoginResponse:
    def __init__(self, status_code=200, text="", location=""):
        self.status_code = status_code
        self.text = text
        self.headers = {"Location": location} if location else {}

    def json(self):
        return json.loads(self.text)


class LoginSession:
    """Answers each step of Resideo's sign-in, keeping what the page would send."""

    def __init__(self, *, login_response):
        self.cookies = {"_csrf": "csrf-token"}
        self.login_response = login_response
        self.login_body = None
        self.callback_form = None

    def request(self, method, url, **kwargs):
        if "/authorize?" in url:
            self.client_state = parse_qs(urlparse(url).query)["state"][0]
            return LoginResponse(302, location="/login?state=auth-state")
        if url.endswith("/login?state=auth-state"):
            return LoginResponse(200)
        if url.endswith("/usernamepassword/login"):
            self.login_body = kwargs["json"]
            return self.login_response
        if url.endswith("/login/callback"):
            self.callback_form = kwargs["data"]
            return LoginResponse(302, location="/authorize/resume?state=r")
        if "/authorize/resume" in url:
            return LoginResponse(302, location=f"{OAUTH_REDIRECT_URI}?code=the-code&state={self.client_state}")
        raise AssertionError(url)


def test_a_password_sign_in_walks_the_page_flow_and_exchanges_the_code(monkeypatch) -> None:
    exchanged = {}

    def exchange(code, *, code_verifier, timeout):
        exchanged.update(code=code, verifier=code_verifier)
        return {"access_token": "a", "refresh_token": "r", "expires_at": 1}

    monkeypatch.setattr("personal_data_warehouse.resideo_api.exchange_authorization_code", exchange)
    session = LoginSession(
        login_response=LoginResponse(text='<input name="wresult" value="a&amp;b"><input name="wctx" value="c">')
    )
    token = login_with_password("me@example.test", "hunter2", session=session)

    assert token["refresh_token"] == "r"
    assert exchanged["code"] == "the-code" and exchanged["verifier"]
    assert session.login_body["_csrf"] == "csrf-token"
    assert session.login_body["state"] == "auth-state"
    assert (session.login_body["username"], session.login_body["password"]) == ("me@example.test", "hunter2")
    assert session.callback_form == {"wa": "wsignin1.0", "wresult": "a&b", "wctx": "c"}


def test_a_refused_password_names_auth0s_code_and_never_repeats_the_body() -> None:
    body = json.dumps({"code": "invalid_captcha", "description": "hunter2 was here"})
    session = LoginSession(login_response=LoginResponse(400, text=body))
    with pytest.raises(ResideoAuthError) as refused:
        login_with_password("me@example.test", "hunter2", session=session)
    assert "invalid_captcha" in str(refused.value)
    assert "hunter2" not in str(refused.value)


def test_a_table_created_before_a_column_was_added_gains_it(warehouse) -> None:
    """Production's table predates schedule_enabled, which no fresh database can show."""

    warehouse._command("ALTER TABLE @resideo_thermostats DROP COLUMN schedule_enabled")
    warehouse.ensure_resideo_tables()
    run_sync(warehouse, FakeClient([shadow()]))
    assert warehouse._query("SELECT schedule_enabled FROM @resideo_thermostats") == [(1,)]
