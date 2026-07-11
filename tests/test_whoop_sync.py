from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta

import pytest
import requests

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.whoop_auth import encode_whoop_token_env, whoop_token_json_from_env
from personal_data_warehouse.whoop_sync import (
    WhoopApiClient,
    WhoopApiError,
    WhoopAuthError,
    WhoopRateLimitedError,
    WhoopSyncRunner,
    cycle_to_row,
    iter_collection_pages,
    newer_whoop_token_json,
    parse_rfc3339,
    public_whoop_sync_summary,
    recovery_to_row,
    sleep_to_row,
    workout_to_row,
)


class NullLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass


class FakeWhoopWarehouse:
    def __init__(self, state=None) -> None:
        self.state = state or {}
        self.ensure_called = False
        self.profiles = []
        self.body_measurements = []
        self.cycles = []
        self.recoveries = []
        self.sleeps = []
        self.workouts = []
        self.state_rows = []
        self.oauth_token = ""
        self.oauth_token_updates = []

    def ensure_whoop_tables(self) -> None:
        self.ensure_called = True

    def load_whoop_sync_state(self):
        return self.state

    def load_whoop_oauth_token(self, *, account):
        return self.oauth_token

    def upsert_whoop_oauth_token(self, *, account, token_json, updated_at):
        self.oauth_token = token_json
        self.oauth_token_updates.append({"account": account, "token_json": token_json, "updated_at": updated_at})

    def insert_whoop_profiles(self, rows):
        self.profiles.extend(rows)

    def insert_whoop_body_measurements(self, rows):
        self.body_measurements.extend(rows)

    def insert_whoop_cycles(self, rows):
        self.cycles.extend(rows)

    def insert_whoop_recoveries(self, rows):
        self.recoveries.extend(rows)

    def insert_whoop_sleeps(self, rows):
        self.sleeps.extend(rows)

    def insert_whoop_workouts(self, rows):
        self.workouts.extend(rows)

    def insert_whoop_sync_state(self, **row):
        self.state_rows.append(row)


class FakeWhoopClient:
    def __init__(self, *, profile=None, body=None, pages=None, fail_path=None) -> None:
        self.profile = profile or {"user_id": 101, "email": "z@example.com", "first_name": "Z", "last_name": "L"}
        self.body = body or {"height_meter": 1.8, "weight_kilogram": 80.0, "max_heart_rate": 190}
        self.pages = {path: list(path_pages) for path, path_pages in (pages or {}).items()}
        self.calls = []
        self.fail_path = fail_path

    def get_json(self, path, *, params=None):
        self.calls.append((path, dict(params or {})))
        if path == self.fail_path:
            raise RuntimeError("boom")
        if path == "/v2/user/profile/basic":
            return self.profile
        if path == "/v2/user/measurement/body":
            return self.body
        values = self.pages.get(path)
        if not values:
            raise AssertionError(f"Unexpected WHOOP path {path}")
        return values.pop(0)


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None, text="") -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses) -> None:
        self.responses = list(responses)
        self.requests = []

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeTokenRefresher:
    def __init__(self) -> None:
        self.calls = []

    def __call__(self, token):
        self.calls.append(token)
        updated = dict(token)
        updated["access_token"] = "fresh-token"
        updated["expires_at"] = int((datetime.now(tz=UTC) + timedelta(hours=1)).timestamp())
        return updated


@pytest.fixture(autouse=True)
def _whoop_client_credentials(monkeypatch) -> None:
    monkeypatch.setenv("WHOOP_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("WHOOP_CLIENT_SECRET", "test-client-secret")


def _token_json(**overrides) -> str:
    token = {
        "access_token": "access-token",
        "refresh_token": "refresh-token",
        "expires_at": int((datetime.now(tz=UTC) + timedelta(hours=1)).timestamp()),
        "scope": "offline read:profile read:body_measurement read:cycles read:recovery read:sleep read:workout",
    }
    token.update(overrides)
    return json.dumps(token)


def test_load_settings_accepts_whoop_config(monkeypatch) -> None:
    monkeypatch.setenv("WHOOP_ACCOUNT", "zach@example.com")
    monkeypatch.setenv("WHOOP_CLIENT_ID", "client-id")
    monkeypatch.setenv("WHOOP_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("WHOOP_TOKEN_JSON_B64", encode_whoop_token_env(_token_json()))
    monkeypatch.setenv("WHOOP_PAGE_SIZE", "25")
    monkeypatch.setenv("WHOOP_INCREMENTAL_LOOKBACK_DAYS", "21")
    monkeypatch.setenv("WHOOP_FULL_SYNC_START", "2024-01-01T00:00:00Z")

    settings = load_settings(require_postgres=False, require_gmail=False, require_whoop=True)

    assert settings.whoop is not None
    assert settings.whoop.account == "zach@example.com"
    assert settings.whoop.page_size == 25
    assert settings.whoop.incremental_lookback_days == 21
    assert settings.whoop.max_rate_limit_sleep_seconds == 120
    assert settings.whoop.full_sync_start == "2024-01-01T00:00:00Z"
    assert "read:workout" in settings.whoop.scopes
    assert "offline" in settings.whoop.scopes


def test_load_settings_requires_whoop_token_for_sync(monkeypatch) -> None:
    monkeypatch.setenv("WHOOP_ACCOUNT", "zach@example.com")
    monkeypatch.delenv("WHOOP_TOKEN_JSON", raising=False)
    monkeypatch.delenv("WHOOP_TOKEN_JSON_B64", raising=False)

    with pytest.raises(ValueError, match="WHOOP_TOKEN_JSON_B64"):
        load_settings(require_postgres=False, require_gmail=False, require_whoop=True)


def test_load_settings_requires_whoop_client_credentials_for_refresh(monkeypatch) -> None:
    monkeypatch.setenv("WHOOP_ACCOUNT", "zach@example.com")
    monkeypatch.setenv("WHOOP_TOKEN_JSON_B64", encode_whoop_token_env(_token_json()))
    monkeypatch.delenv("WHOOP_CLIENT_ID")
    monkeypatch.delenv("WHOOP_CLIENT_SECRET")

    with pytest.raises(ValueError, match="token refresh"):
        load_settings(require_postgres=False, require_gmail=False, require_whoop=True)


def test_whoop_token_json_from_env_decodes_base64(monkeypatch) -> None:
    token_json = _token_json(access_token="secret-access-token")
    monkeypatch.setenv("WHOOP_TOKEN_JSON_B64", base64.b64encode(token_json.encode()).decode("ascii"))

    assert json.loads(whoop_token_json_from_env())["access_token"] == "secret-access-token"


def test_api_client_refreshes_expired_token_before_request() -> None:
    session = FakeSession([FakeResponse(payload={"ok": True})])
    refresher = FakeTokenRefresher()
    expired = _token_json(expires_at=int((datetime.now(tz=UTC) - timedelta(minutes=5)).timestamp()))

    client = WhoopApiClient(
        token_json=expired,
        client_id="client-id",
        client_secret="client-secret",
        session=session,
        token_refresher=refresher,
    )

    assert client.get_json("/v2/user/profile/basic") == {"ok": True}
    assert refresher.calls
    assert session.requests[0][2]["headers"]["Authorization"] == "Bearer fresh-token"


def test_api_client_can_disable_refresh_for_non_persistent_validation() -> None:
    session = FakeSession([])
    expired = _token_json(expires_at=int((datetime.now(tz=UTC) - timedelta(minutes=5)).timestamp()))
    client = WhoopApiClient(token_json=expired, session=session, refresh_enabled=False)

    with pytest.raises(WhoopAuthError, match="rotated tokens can be persisted"):
        client.get_json("/v2/user/profile/basic")

    assert session.requests == []


def test_api_client_persists_rotated_token_after_refresh() -> None:
    session = FakeSession([FakeResponse(payload={"ok": True})])
    refresher = FakeTokenRefresher()
    persisted = []
    expired = _token_json(expires_at=int((datetime.now(tz=UTC) - timedelta(minutes=5)).timestamp()))

    client = WhoopApiClient(
        token_json=expired,
        session=session,
        token_refresher=refresher,
        token_updated=persisted.append,
    )

    client.get_json("/v2/user/profile/basic")

    assert persisted[0]["access_token"] == "fresh-token"
    assert persisted[0]["refresh_token"] == "refresh-token"


def test_api_client_waits_and_retries_short_rate_limit() -> None:
    session = FakeSession(
        [
            FakeResponse(status_code=429, headers={"X-RateLimit-Reset": "2"}),
            FakeResponse(payload={"ok": True}),
        ]
    )
    sleeps = []
    client = WhoopApiClient(token_json=_token_json(), session=session, sleep=sleeps.append)

    assert client.get_json("/v2/cycle") == {"ok": True}
    assert sleeps == [2]
    assert len(session.requests) == 2


def test_api_client_raises_rate_limited_error_with_reset_seconds() -> None:
    session = FakeSession([FakeResponse(status_code=429, headers={"X-RateLimit-Reset": "7"}, text="slow down")])
    client = WhoopApiClient(token_json=_token_json(), session=session, max_rate_limit_retries=0)

    with pytest.raises(WhoopRateLimitedError) as excinfo:
        client.get_json("/v2/cycle")

    assert excinfo.value.retry_after == 7


def test_api_client_redacts_http_and_transport_error_details() -> None:
    http_client = WhoopApiClient(
        token_json=_token_json(access_token="do-not-leak"),
        session=FakeSession([FakeResponse(status_code=500, text="do-not-leak")]),
    )
    with pytest.raises(WhoopApiError, match="HTTP 500") as http_error:
        http_client.get_json("/v2/cycle")
    assert "do-not-leak" not in str(http_error.value)

    transport_client = WhoopApiClient(
        token_json=_token_json(),
        session=FakeSession([requests.Timeout("sensitive transport detail")]),
    )
    with pytest.raises(WhoopApiError, match="transport error") as transport_error:
        transport_client.get_json("/v2/cycle")
    assert "sensitive transport detail" not in str(transport_error.value)


def test_api_client_raises_auth_error_without_refresh_token_on_401() -> None:
    session = FakeSession([FakeResponse(status_code=401, text="bad token")])
    token_json = _token_json(refresh_token="")
    client = WhoopApiClient(token_json=token_json, session=session)

    with pytest.raises(WhoopAuthError, match="WHOOP access token was rejected"):
        client.get_json("/v2/user/profile/basic")


def test_iter_collection_pages_sends_next_token_and_date_window() -> None:
    client = FakeWhoopClient(
        pages={
            "/v2/activity/sleep": [
                {"records": [{"id": "sleep-1"}], "next_token": "token-2"},
                {"records": [{"id": "sleep-2"}], "next_token": ""},
            ]
        }
    )

    pages = list(
        iter_collection_pages(
            client,
            "/v2/activity/sleep",
            limit=25,
            start="2024-01-01T00:00:00Z",
            end="2024-02-01T00:00:00Z",
        )
    )

    assert pages == [[{"id": "sleep-1"}], [{"id": "sleep-2"}]]
    assert client.calls[0] == (
        "/v2/activity/sleep",
        {"limit": 25, "start": "2024-01-01T00:00:00Z", "end": "2024-02-01T00:00:00Z"},
    )
    assert client.calls[1][1]["nextToken"] == "token-2"


def test_iter_collection_pages_rejects_repeated_next_token() -> None:
    client = FakeWhoopClient(
        pages={
            "/v2/cycle": [
                {"records": [], "next_token": "same-token"},
                {"records": [], "next_token": "same-token"},
            ]
        }
    )

    with pytest.raises(WhoopApiError, match="repeated a pagination token"):
        list(iter_collection_pages(client, "/v2/cycle", limit=25))


def test_rows_extract_common_fields_and_keep_raw_payload() -> None:
    synced_at = datetime(2026, 7, 9, 12, tzinfo=UTC)
    cycle = {
        "id": 93845,
        "user_id": 10129,
        "created_at": "2022-04-24T11:25:44.774Z",
        "updated_at": "2022-04-24T14:25:44.774Z",
        "start": "2022-04-24T02:25:44.774Z",
        "end": "2022-04-24T10:25:44.774Z",
        "timezone_offset": "-05:00",
        "score_state": "SCORED",
        "score": {"strain": 5.2, "kilojoule": 8288.2, "average_heart_rate": 68, "max_heart_rate": 141},
    }
    recovery = {
        "cycle_id": 93845,
        "sleep_id": "sleep-1",
        "user_id": 10129,
        "created_at": "2022-04-24T11:25:44.774Z",
        "updated_at": "2022-04-24T14:25:44.774Z",
        "score_state": "SCORED",
        "score": {"user_calibrating": False, "recovery_score": 44, "hrv_rmssd_milli": 31.8},
    }
    sleep = {
        "id": "sleep-1",
        "cycle_id": 93845,
        "v1_id": 123,
        "user_id": 10129,
        "created_at": "2022-04-24T11:25:44.774Z",
        "updated_at": "2022-04-24T14:25:44.774Z",
        "start": "2022-04-24T02:25:44.774Z",
        "end": "2022-04-24T10:25:44.774Z",
        "timezone_offset": "-05:00",
        "nap": False,
        "score_state": "SCORED",
        "score": {
            "stage_summary": {"total_in_bed_time_milli": 1000, "sleep_cycle_count": 3},
            "sleep_needed": {"baseline_milli": 2000},
            "respiratory_rate": 16.1,
            "sleep_performance_percentage": 98,
        },
    }
    workout = {
        "id": "workout-1",
        "v1_id": 456,
        "user_id": 10129,
        "created_at": "2022-04-24T11:25:44.774Z",
        "updated_at": "2022-04-24T14:25:44.774Z",
        "start": "2022-04-24T02:25:44.774Z",
        "end": "2022-04-24T10:25:44.774Z",
        "timezone_offset": "-05:00",
        "sport_name": "running",
        "sport_id": 1,
        "score_state": "SCORED",
        "score": {"strain": 8.2, "zone_durations": {"zone_one_milli": 600000}},
    }

    cycle_row = cycle_to_row(account="zach", payload=cycle, synced_at=synced_at)
    recovery_row = recovery_to_row(account="zach", payload=recovery, synced_at=synced_at)
    sleep_row = sleep_to_row(account="zach", payload=sleep, synced_at=synced_at)
    workout_row = workout_to_row(account="zach", payload=workout, synced_at=synced_at)

    assert cycle_row["cycle_id"] == "93845"
    assert cycle_row["whoop_user_id"] == 10129
    assert cycle_row["strain"] == 5.2
    assert cycle_row["start_at"] == parse_rfc3339("2022-04-24T02:25:44.774Z")
    assert cycle_row["raw_json"]["score_state"] == "SCORED"
    assert recovery_row["user_calibrating"] == 0
    assert recovery_row["hrv_rmssd_milli"] == 31.8
    assert sleep_row["stage_summary_json"]["sleep_cycle_count"] == 3
    assert sleep_row["nap"] == 0
    assert workout_row["sport_name"] == "running"
    assert workout_row["zone_durations_json"]["zone_one_milli"] == 600000


def test_validate_all_makes_live_shape_checks_without_returning_personal_data(monkeypatch) -> None:
    monkeypatch.setenv("WHOOP_ACCOUNT", "zach@example.com")
    monkeypatch.setenv("WHOOP_TOKEN_JSON_B64", encode_whoop_token_env(_token_json()))
    settings = load_settings(require_postgres=False, require_gmail=False, require_whoop=True)
    client = FakeWhoopClient(
        pages={
            "/v2/cycle": [{"records": [{"id": 1, "user_id": 101}], "next_token": "ignored"}],
            "/v2/recovery": [{"records": [{"cycle_id": 1, "user_id": 101}], "next_token": "ignored"}],
            "/v2/activity/sleep": [{"records": [{"id": "sleep-1", "user_id": 101}], "next_token": "ignored"}],
            "/v2/activity/workout": [{"records": [{"id": "workout-1", "user_id": 101}], "next_token": "ignored"}],
        }
    )

    result = WhoopSyncRunner(
        settings=settings,
        warehouse=FakeWhoopWarehouse(),
        logger=NullLogger(),
        client_factory=lambda _config: client,
    ).validate_all()

    assert result == {
        "account_configured": True,
        "profile_endpoint": "ok",
        "profile_fields": ["email", "first_name", "last_name", "user_id"],
        "body_measurement_endpoint": "ok",
        "body_measurement_fields": ["height_meter", "max_heart_rate", "weight_kilogram"],
        "collection_endpoints": {
            "cycles": {"status": "ok", "record_count": 1, "record_fields": ["id", "user_id"]},
            "recovery": {"status": "ok", "record_count": 1, "record_fields": ["cycle_id", "user_id"]},
            "sleep": {"status": "ok", "record_count": 1, "record_fields": ["id", "user_id"]},
            "workout": {"status": "ok", "record_count": 1, "record_fields": ["id", "user_id"]},
        },
    }
    assert "z@example.com" not in json.dumps(result)
    assert "101" not in json.dumps(result)


def test_newer_token_selection_allows_reauth_to_replace_stale_private_token() -> None:
    bootstrap = _token_json(access_token="reauthorized", expires_at=300)
    stored = _token_json(access_token="stale-private", expires_at=200)

    assert newer_whoop_token_json(
        bootstrap_token_json=bootstrap,
        stored_token_json=stored,
    ) == bootstrap


def test_sync_runner_uses_private_stored_token_over_bootstrap_env_token(monkeypatch) -> None:
    monkeypatch.setenv("WHOOP_ACCOUNT", "zach@example.com")
    monkeypatch.setenv(
        "WHOOP_TOKEN_JSON_B64",
        encode_whoop_token_env(_token_json(access_token="bootstrap-token", expires_at=100)),
    )
    settings = load_settings(require_postgres=False, require_gmail=False, require_whoop=True)
    warehouse = FakeWhoopWarehouse()
    warehouse.oauth_token = _token_json(access_token="rotated-stored-token", expires_at=200)
    captured = {}
    client = FakeWhoopClient(
        pages={
            "/v2/cycle": [{"records": [], "next_token": ""}],
            "/v2/recovery": [{"records": [], "next_token": ""}],
            "/v2/activity/sleep": [{"records": [], "next_token": ""}],
            "/v2/activity/workout": [{"records": [], "next_token": ""}],
        }
    )

    def client_factory(config):
        captured["token"] = json.loads(config.token_json)["access_token"]
        return client

    WhoopSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=client_factory,
    ).sync_all()

    assert captured["token"] == "rotated-stored-token"


def test_sync_runner_fetches_profile_body_and_collections_with_state(monkeypatch) -> None:
    monkeypatch.setenv("WHOOP_ACCOUNT", "zach@example.com")
    monkeypatch.setenv("WHOOP_TOKEN_JSON_B64", encode_whoop_token_env(_token_json()))
    monkeypatch.setenv("WHOOP_PAGE_SIZE", "25")
    monkeypatch.setenv("WHOOP_FULL_SYNC_START", "2024-01-01T00:00:00Z")
    settings = load_settings(require_postgres=False, require_gmail=False, require_whoop=True)
    now = datetime(2026, 7, 9, 12, tzinfo=UTC)
    client = FakeWhoopClient(
        pages={
            "/v2/cycle": [{"records": [{"id": 1, "updated_at": "2026-07-08T00:00:00Z"}], "next_token": ""}],
            "/v2/recovery": [{"records": [{"cycle_id": 1, "updated_at": "2026-07-08T01:00:00Z"}], "next_token": ""}],
            "/v2/activity/sleep": [{"records": [{"id": "sleep-1", "updated_at": "2026-07-08T02:00:00Z"}], "next_token": ""}],
            "/v2/activity/workout": [{"records": [{"id": "workout-1", "updated_at": "2026-07-08T03:00:00Z"}], "next_token": ""}],
        }
    )
    warehouse = FakeWhoopWarehouse()

    summaries = WhoopSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=NullLogger(),
        client_factory=lambda _config: client,
        now=lambda: now,
    ).sync_all()

    assert warehouse.ensure_called
    assert len(warehouse.profiles) == 1
    assert len(warehouse.body_measurements) == 1
    assert len(warehouse.cycles) == 1
    assert len(warehouse.recoveries) == 1
    assert len(warehouse.sleeps) == 1
    assert len(warehouse.workouts) == 1
    assert {row["collection"] for row in warehouse.state_rows} == {
        "profile",
        "body_measurement",
        "cycles",
        "recovery",
        "sleep",
        "workout",
    }
    assert all(row["status"] == "ok" for row in warehouse.state_rows)
    collection_calls = [call for call in client.calls if call[0] in {"/v2/cycle", "/v2/recovery", "/v2/activity/sleep", "/v2/activity/workout"}]
    assert all(call[1]["limit"] == 25 for call in collection_calls)
    assert all(call[1]["start"] == "2024-01-01T00:00:00Z" for call in collection_calls)
    assert summaries[0].records_written == 6
    assert public_whoop_sync_summary(summaries[0])["has_token"] is False


def test_sync_runner_uses_incremental_lookback_from_prior_success(monkeypatch) -> None:
    monkeypatch.setenv("WHOOP_ACCOUNT", "zach@example.com")
    monkeypatch.setenv("WHOOP_TOKEN_JSON_B64", encode_whoop_token_env(_token_json()))
    monkeypatch.setenv("WHOOP_INCREMENTAL_LOOKBACK_DAYS", "7")
    settings = load_settings(require_postgres=False, require_gmail=False, require_whoop=True)
    last_success = datetime(2026, 7, 9, 12, tzinfo=UTC)
    state = {
        ("zach@example.com", "cycles"): {
            "last_sync_type": "full",
            "status": "ok",
            "updated_at": last_success,
            "watermark_updated_at": last_success,
        }
    }
    client = FakeWhoopClient(
        pages={
            "/v2/cycle": [{"records": [], "next_token": ""}],
            "/v2/recovery": [{"records": [], "next_token": ""}],
            "/v2/activity/sleep": [{"records": [], "next_token": ""}],
            "/v2/activity/workout": [{"records": [], "next_token": ""}],
        }
    )

    WhoopSyncRunner(
        settings=settings,
        warehouse=FakeWhoopWarehouse(state=state),
        logger=NullLogger(),
        client_factory=lambda _config: client,
        now=lambda: datetime(2026, 7, 10, 12, tzinfo=UTC),
    ).sync_all()

    cycle_call = next(call for call in client.calls if call[0] == "/v2/cycle")
    assert cycle_call[1]["start"] == "2026-07-02T12:00:00Z"


def test_sync_runner_records_failure_state_without_token_leak(monkeypatch) -> None:
    monkeypatch.setenv("WHOOP_ACCOUNT", "zach@example.com")
    monkeypatch.setenv("WHOOP_TOKEN_JSON_B64", encode_whoop_token_env(_token_json(access_token="secret-token")))
    settings = load_settings(require_postgres=False, require_gmail=False, require_whoop=True)
    warehouse = FakeWhoopWarehouse()
    client = FakeWhoopClient(fail_path="/v2/user/profile/basic", pages={})

    with pytest.raises(RuntimeError, match="WHOOP sync failed"):
        WhoopSyncRunner(
            settings=settings,
            warehouse=warehouse,
            logger=NullLogger(),
            client_factory=lambda _config: client,
            now=lambda: datetime(2026, 7, 9, 12, tzinfo=UTC),
        ).sync_all()

    assert warehouse.state_rows[0]["collection"] == "profile"
    assert warehouse.state_rows[0]["status"] == "failed"
    assert "secret-token" not in warehouse.state_rows[0]["error"]
