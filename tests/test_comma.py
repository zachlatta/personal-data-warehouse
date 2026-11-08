"""comma.ai source: the API client, the drive poller, the warehouse round trip, the sensor."""

from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta
import json
import os
import time

import pytest

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.comma_api import (
    CommaAuthError,
    CommaClient,
    CommaRequestError,
    token_expires_at,
)
from personal_data_warehouse.comma_sync import CommaSyncRunner, route_row
from personal_data_warehouse.postgres import PostgresWarehouse

NOW = datetime(2026, 11, 8, 15, 0, tzinfo=UTC)
USER = "00aa11bb22cc33dd"
DONGLE = "feedfacecafe0001"
START_MS = int(datetime(2026, 11, 8, 13, 53, 34, tzinfo=UTC).timestamp() * 1000)


def jwt(exp: datetime) -> str:
    def part(value: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")

    return f"{part({'alg': 'HS256'})}.{part({'exp': int(exp.timestamp()), 'identity': USER})}.signature"


TOKEN = jwt(NOW + timedelta(days=90))


def api_route(*, name="00000131--5a0a6a29f3", start_ms=START_MS, minutes=26, distance=19.4727, lat=40.01):
    end_ms = start_ms + minutes * 60_000
    return {
        "fullname": f"{DONGLE}|{name}",
        "dongle_id": DONGLE,
        "user_id": USER,
        "start_time_utc_millis": start_ms,
        "end_time_utc_millis": end_ms,
        "start_time": "2026-11-08T13:53:34",
        "distance": distance,
        "start_lat": lat,
        "start_lng": -75.2,
        "end_lat": 40.3,
        "end_lng": -75.1,
        "make": "examplemake",
        "platform": "EXAMPLE_PLATFORM",
        "vin": "TESTVIN0000000000",
        "version": "0.11.2",
        "git_remote": "github.com/commaai/openpilot",
        "git_branch": "release3",
        "git_commit": "abc123",
        "segment_numbers": list(range(minutes + 1)),
        "maxqlog": minutes,
        "procqlog": minutes,
        "is_preserved": False,
        "is_public": False,
        # A share signature lets anyone holding the link watch the drive.
        "share_sig": "secret-share-signature",
        "share_exp": "1794243510",
        "url": "https://example.invalid/blob/route",
    }


DEVICE = {
    "dongle_id": DONGLE,
    "alias": None,
    "device_type": "threex",
    "serial": "0000test",
    "openpilot_version": "0.11.2",
    "is_owner": True,
    "is_paired": True,
    "prime": False,
    "last_athena_ping": int(NOW.timestamp()) - 60,
    "public_key": "-----BEGIN PUBLIC KEY-----\n...",
}
STATS = {
    "all": {"distance": 18787.3, "minutes": 39637, "routes": 2372},
    "week": {"distance": 319.4, "minutes": 692, "routes": 40},
}
LOCATION = {"dongle_id": DONGLE, "lat": 40.3, "lng": -75.1, "time": START_MS + 26 * 60_000}


class FakeClient:
    def __init__(self, routes=None, *, location=LOCATION, token=TOKEN):
        self.routes_by_call = [routes if routes is not None else [api_route()]]
        self.location = location
        self.token = token
        self.route_windows = []

    def me(self):
        return {"id": USER, "username": "someone"}

    def devices(self):
        return [DEVICE]

    def device_stats(self, dongle_id):
        return STATS

    def device_location(self, dongle_id):
        return self.location

    def routes(self, dongle_id, *, start_ms, end_ms):
        self.route_windows.append((start_ms, end_ms))
        return self.routes_by_call[0]

    #: Segments comma has not processed yet answer None, as a 404 does.
    unprocessed: set = set()
    coords_calls: list

    def segment_coords(self, route_url, segment):
        self.__dict__.setdefault("coords_calls", []).append((route_url, segment))
        if segment in self.unprocessed:
            return None
        return coords(segment)


def coords(segment, *, points=3):
    """comma's per-segment track: t counts seconds from the drive's start, dist resets per segment."""

    return [
        {"t": segment * 60 + i, "lat": 40.0 + segment / 100 + i / 1000, "lng": -75.0, "speed": 10.0 + i,
         "dist": i * 0.01}
        for i in range(points)
    ]


class FakeWarehouse:
    def __init__(self):
        self.devices = []
        self.routes = []
        self.state = []
        self.segments = []
        self.points = []

    def comma_tracked_segments(self, *, account, dongle_id, route_names):
        return {(row["route_name"], row["segment_number"]) for row in self.segments}

    def insert_comma_track(self, *, segments, points):
        self.segments.extend(segments)
        self.points.extend(points)

    def insert_comma_devices(self, rows):
        self.devices.extend(rows)

    def upsert_comma_routes(self, rows):
        self.routes.extend(rows)
        return len(rows)

    def insert_comma_sync_state(self, **row):
        self.state.append(row)


def run_sync(warehouse, client, now=NOW):
    return CommaSyncRunner(warehouse=warehouse, client=client, token=client.token, now=lambda: now).sync()


# --- the token and the client ---------------------------------------------


def test_the_token_expiry_is_read_from_the_jwt_without_verifying_it():
    assert token_expires_at(TOKEN) == NOW + timedelta(days=90)


def test_a_token_that_is_not_a_jwt_is_refused_by_name():
    with pytest.raises(CommaAuthError, match="not a JWT"):
        token_expires_at("not-a-token")


class Response:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body)

    def json(self):
        return self._body


class Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


def test_every_call_carries_the_jwt_and_hits_the_trailing_slash_path():
    session = Session([Response(200, {"id": USER})])
    client = CommaClient(token=TOKEN, session=session)
    assert client.me() == {"id": USER}
    method, url, kwargs = session.calls[0]
    assert (method, url) == ("GET", "https://api.commadotai.com/v1/me/")
    assert kwargs["headers"]["Authorization"] == f"JWT {TOKEN}"


@pytest.mark.parametrize("status", [401, 403])
def test_a_refused_token_is_an_auth_error(status):
    client = CommaClient(token=TOKEN, session=Session([Response(status, {"error": "unauthorized"})]))
    with pytest.raises(CommaAuthError, match="COMMA_API_TOKEN"):
        client.devices()


def test_a_server_error_is_a_request_error_not_an_auth_error():
    client = CommaClient(token=TOKEN, session=Session([Response(502, {"error": "bad gateway"})]))
    with pytest.raises(CommaRequestError) as raised:
        client.devices()
    assert not isinstance(raised.value, CommaAuthError)


def test_routes_page_backwards_until_a_short_page():
    first = [api_route(name=f"0000000{i}--a", start_ms=START_MS - i * 3_600_000) for i in range(2)]
    second = [api_route(name="00000009--a", start_ms=START_MS - 9 * 3_600_000)]
    session = Session([Response(200, first), Response(200, second)])
    client = CommaClient(token=TOKEN, session=session, route_page_size=2)
    routes = client.routes(DONGLE, start_ms=0, end_ms=START_MS + 1)
    assert [r["fullname"].split("|")[1] for r in routes] == ["00000000--a", "00000001--a", "00000009--a"]
    second_url = session.calls[1][1]
    assert f"end={START_MS - 3_600_000 - 1}" in second_url


def test_a_device_with_no_known_location_reads_as_none():
    client = CommaClient(token=TOKEN, session=Session([Response(404, {"error": "no location"})]))
    assert client.device_location(DONGLE) is None


# --- the runner -----------------------------------------------------------


def test_a_route_row_takes_utc_times_miles_and_drops_the_share_signature():
    row = route_row(api_route(), account=USER, synced_at=NOW)
    assert row["route_name"] == "00000131--5a0a6a29f3"
    assert row["start_at"] == datetime(2026, 11, 8, 13, 53, 34, tzinfo=UTC)
    assert row["end_at"] == row["start_at"] + timedelta(minutes=26)
    assert row["distance_miles"] == 19.4727
    assert row["segment_count"] == 27
    assert row["has_gps"] == 1
    raw = row["raw_json"]
    assert "share_sig" not in raw and "share_exp" not in raw
    assert raw["vin"] == "TESTVIN0000000000"


def test_a_route_without_gps_says_so_instead_of_reading_as_null_island():
    route = api_route()
    route["start_lat"] = route["start_lng"] = route["end_lat"] = route["end_lng"] = None
    row = route_row(route, account=USER, synced_at=NOW)
    assert (row["has_gps"], row["start_lat"], row["end_lng"]) == (0, 0.0, 0.0)


def test_a_route_without_utc_millis_is_refused_rather_than_guessed():
    route = api_route()
    del route["start_time_utc_millis"]
    with pytest.raises(CommaRequestError, match="start_time_utc_millis"):
        route_row(route, account=USER, synced_at=NOW)


def test_a_poll_stores_the_device_its_totals_and_every_route():
    warehouse = FakeWarehouse()
    client = FakeClient()
    summary = run_sync(warehouse, client)
    assert (summary.devices, summary.routes_seen, summary.routes_written) == (1, 1, 1)
    device = warehouse.devices[0]
    assert device["account"] == USER
    assert device["total_routes"] == 2372 and device["week_distance_miles"] == 319.4
    assert device["location_at"] == datetime.fromtimestamp(LOCATION["time"] / 1000, tz=UTC)
    assert device["last_polled_at"] == NOW
    assert warehouse.routes[0]["dongle_id"] == DONGLE
    assert warehouse.state[-1]["status"] == "ok"
    assert warehouse.state[-1]["token_expires_at"] == NOW + timedelta(days=90)
    # The whole retained window: comma keeps a free account's drives for days.
    assert client.route_windows == [(0, int(NOW.timestamp() * 1000))]


def test_a_device_without_a_location_keeps_the_epoch():
    warehouse = FakeWarehouse()
    run_sync(warehouse, FakeClient(location=None))
    device = warehouse.devices[0]
    assert (device["has_location"], device["location_at"]) == (0, datetime(1970, 1, 1, tzinfo=UTC))


def test_a_token_close_to_expiry_still_polls_but_asks_for_a_new_one():
    warehouse = FakeWarehouse()
    client = FakeClient(token=jwt(NOW + timedelta(days=5)))
    run_sync(warehouse, client)
    assert warehouse.routes, "an expiring token is still a working one"
    state = warehouse.state[-1]
    assert state["status"] == "action_required"
    assert "expires" in state["error"] and "2026-11-13" in state["error"]
    assert state["last_success_at"] == NOW


def test_a_refused_token_records_action_required_and_reraises():
    class Refused(FakeClient):
        def me(self):
            raise CommaAuthError("refused; set a new COMMA_API_TOKEN")

    warehouse = FakeWarehouse()
    with pytest.raises(CommaAuthError):
        run_sync(warehouse, Refused())
    assert warehouse.state[-1]["status"] == "action_required"
    assert warehouse.state[-1]["last_success_at"] is None


def test_an_account_with_no_device_is_a_failure_not_a_quiet_green_run():
    class Empty(FakeClient):
        def devices(self):
            return []

    warehouse = FakeWarehouse()
    with pytest.raises(CommaRequestError, match="no device"):
        run_sync(warehouse, Empty())
    assert warehouse.state[-1]["status"] == "failed"


# --- the real warehouse ---------------------------------------------------


@pytest.fixture()
def warehouse():
    wh = PostgresWarehouse(os.environ["POSTGRES_DATABASE_URL"], schema=make_test_schema("comma"))
    try:
        wh.ensure_comma_tables()
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def test_an_unchanged_route_is_not_rewritten_and_a_grown_one_is(warehouse) -> None:
    run_sync(warehouse, FakeClient([api_route(minutes=10, distance=5.0)]))
    run_sync(warehouse, FakeClient([api_route(minutes=10, distance=5.0)]), now=NOW + timedelta(minutes=15))
    rows = warehouse._query("SELECT distance_miles, synced_at FROM @comma_routes")
    assert [tuple(row) for row in rows] == [(5.0, NOW)]

    # More segments uploaded: the drive grows, and the row (and its timeline event) follow.
    summary = run_sync(
        warehouse, FakeClient([api_route(minutes=26, distance=19.5)]), now=NOW + timedelta(minutes=30)
    )
    assert summary.routes_written == 1
    rows = warehouse._query("SELECT distance_miles, segment_count, synced_at FROM @comma_routes")
    assert [tuple(row) for row in rows] == [(19.5, 27, NOW + timedelta(minutes=30))]


def test_a_route_comma_has_aged_out_stays_in_the_warehouse(warehouse) -> None:
    run_sync(warehouse, FakeClient([api_route(name="00000001--a")]))
    run_sync(warehouse, FakeClient([api_route(name="00000002--b", start_ms=START_MS + 3_600_000)]),
             now=NOW + timedelta(hours=1))
    names = [row[0] for row in warehouse._query("SELECT route_name FROM @comma_routes ORDER BY start_at")]
    assert names == ["00000001--a", "00000002--b"]


def test_devices_and_state_round_trip(warehouse) -> None:
    run_sync(warehouse, FakeClient())
    run_sync(warehouse, FakeClient(location=None), now=NOW + timedelta(minutes=15))
    device = warehouse._query(
        "SELECT device_type, total_routes, has_location, location_lat, last_polled_at FROM @comma_devices"
    )
    # A poll that cannot place the device keeps the last position it had.
    assert [tuple(row) for row in device] == [("threex", 2372, 1, 40.3, NOW + timedelta(minutes=15))]
    state = warehouse._query("SELECT status, routes_seen, last_success_at, token_expires_at FROM @comma_sync_state")
    assert [tuple(row) for row in state] == [("ok", 1, NOW + timedelta(minutes=15), NOW + timedelta(days=90))]


def test_a_failed_poll_keeps_the_last_success(warehouse) -> None:
    class Refused(FakeClient):
        def me(self):
            raise CommaAuthError("refused")

    run_sync(warehouse, FakeClient())
    with pytest.raises(CommaAuthError):
        run_sync(warehouse, Refused(), now=NOW + timedelta(minutes=15))
    rows = warehouse._query("SELECT status, error, last_success_at FROM @comma_sync_state")
    assert [tuple(row) for row in rows] == [("action_required", "refused", NOW)]


# --- the sensor -----------------------------------------------------------


def test_the_sensor_sits_out_until_the_token_is_set_and_then_polls_on_its_interval(monkeypatch) -> None:
    from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

    from personal_data_warehouse.defs import comma_sync as defs_module

    monkeypatch.setattr(defs_module, "skip_if_job_in_progress", lambda context, job_name: None)
    monkeypatch.delenv("COMMA_API_TOKEN", raising=False)
    with DagsterInstance.ephemeral() as instance:
        skipped = defs_module.comma_sync_sensor(build_sensor_context(instance=instance))
        assert isinstance(skipped, SkipReason)
        assert "COMMA_API_TOKEN" in skipped.skip_message

        monkeypatch.setenv("COMMA_API_TOKEN", TOKEN)
        monkeypatch.setenv("POSTGRES_DATABASE_URL", "postgres://unused")
        assert isinstance(defs_module.comma_sync_sensor(build_sensor_context(instance=instance)), RunRequest)
        recent = build_sensor_context(instance=instance, cursor=str(time.time()))
        assert isinstance(defs_module.comma_sync_sensor(recent), SkipReason)


def test_settings_read_the_token_and_clamp_the_interval(monkeypatch) -> None:
    from personal_data_warehouse.config import load_settings

    monkeypatch.setenv("COMMA_API_TOKEN", f"  {TOKEN}\n")
    monkeypatch.setenv("COMMA_POLL_INTERVAL_SECONDS", "5")
    config = load_settings(require_gmail=False).comma
    assert config is not None
    assert config.token == TOKEN
    assert config.poll_interval_seconds == 60
    monkeypatch.delenv("COMMA_API_TOKEN")
    assert load_settings(require_gmail=False).comma is None


# --- the GPS track ----------------------------------------------------------


def test_every_processed_segment_lands_its_track_with_absolute_times():
    warehouse = FakeWarehouse()
    route = api_route(minutes=2)  # segments 0, 1, 2
    summary = run_sync(warehouse, FakeClient([route]))
    assert summary.track_points_written == 9
    assert [(row["segment_number"], row["point_count"]) for row in warehouse.segments] == [(0, 3), (1, 3), (2, 3)]
    first = warehouse.points[0]
    assert first["route_name"] == "00000131--5a0a6a29f3"
    assert first["offset_seconds"] == 0
    assert first["recorded_at"] == datetime(2026, 11, 8, 13, 53, 34, tzinfo=UTC)
    assert (first["latitude"], first["longitude"], first["speed_mps"]) == (40.0, -75.0, 10.0)
    later = warehouse.points[4]
    assert later["recorded_at"] == first["recorded_at"] + timedelta(seconds=61)
    assert warehouse.segments[0]["distance_miles"] == 0.02


def test_a_segment_already_tracked_is_not_fetched_again():
    warehouse = FakeWarehouse()
    route = api_route(minutes=2)
    run_sync(warehouse, FakeClient([route]))
    client = FakeClient([route])
    summary = run_sync(warehouse, client, now=NOW + timedelta(minutes=15))
    assert getattr(client, "coords_calls", []) == []
    assert summary.track_points_written == 0


def test_a_segment_comma_has_not_processed_waits_for_a_later_poll():
    warehouse = FakeWarehouse()
    route = api_route(minutes=3)
    route["procqlog"] = 1  # comma has processed segments 0 and 1 only
    client = FakeClient([route])
    run_sync(warehouse, client)
    assert [segment for _, segment in client.coords_calls] == [0, 1]

    route["procqlog"] = 3
    late = FakeClient([route])
    late.unprocessed = {3}  # listed as processed, but its coords.json is not there yet
    run_sync(warehouse, late, now=NOW + timedelta(minutes=15))
    assert [segment for _, segment in late.coords_calls] == [2, 3]
    assert sorted(row["segment_number"] for row in warehouse.segments) == [0, 1, 2]


def test_a_segment_without_a_gps_fix_is_recorded_so_it_is_not_fetched_forever():
    class NoFix(FakeClient):
        def segment_coords(self, route_url, segment):
            super().segment_coords(route_url, segment)
            return []

    warehouse = FakeWarehouse()
    run_sync(warehouse, NoFix([api_route(minutes=0)]))
    assert [(row["segment_number"], row["point_count"]) for row in warehouse.segments] == [(0, 0)]
    assert warehouse.points == []


def test_the_oldest_drive_is_tracked_first_because_comma_deletes_it_first():
    warehouse = FakeWarehouse()
    newer = api_route(name="00000002--b", start_ms=START_MS + 3_600_000, minutes=0)
    older = api_route(name="00000001--a", minutes=0)
    newer["url"], older["url"] = "https://example.invalid/newer", "https://example.invalid/older"
    client = FakeClient([newer, older])  # comma lists newest first
    run_sync(warehouse, client)
    assert [url for url, _ in client.coords_calls] == ["https://example.invalid/older", "https://example.invalid/newer"]


def test_segment_coords_reads_the_derived_file_without_the_jwt_and_a_404_is_none():
    session = Session([Response(200, coords(0)), Response(404, {})])
    client = CommaClient(token=TOKEN, session=session)
    assert client.segment_coords("https://blob.example.invalid/route", 0) == coords(0)
    assert client.segment_coords("https://blob.example.invalid/route", 1) is None
    method, url, kwargs = session.calls[0]
    assert (method, url) == ("GET", "https://blob.example.invalid/route/0/coords.json")
    # The blob store rejects an Authorization scheme it does not know.
    assert "Authorization" not in kwargs["headers"]


def test_the_track_round_trips_and_a_rerun_does_not_duplicate_it(warehouse) -> None:
    route = api_route(minutes=1)
    run_sync(warehouse, FakeClient([route]))
    run_sync(warehouse, FakeClient([route]), now=NOW + timedelta(minutes=15))
    points = warehouse._query(
        "SELECT segment_number, offset_seconds, recorded_at, latitude, speed_mps, segment_distance_miles "
        "FROM @comma_route_points ORDER BY recorded_at"
    )
    assert len(points) == 6
    assert tuple(points[3]) == (1, 60, datetime(2026, 11, 8, 13, 54, 34, tzinfo=UTC), 40.01, 10.0, 0.0)
    segments = warehouse._query(
        "SELECT segment_number, point_count, distance_miles FROM @comma_route_segments ORDER BY segment_number"
    )
    assert [tuple(row) for row in segments] == [(0, 3, 0.02), (1, 3, 0.02)]
