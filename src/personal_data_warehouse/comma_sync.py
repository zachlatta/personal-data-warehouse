"""Poll comma.ai: each device, its lifetime totals, and every drive it uploaded.

comma keeps a free account's drives for about three days (measured
2026-11-08: the API listed Nov 5-8 plus one preserved route, against 2,372
lifetime routes), so the warehouse is the only lasting copy and a route comma
has aged out is never deleted here. A route grows while its segments upload, so
each poll re-reads the whole retained window and the upsert rewrites a route
only when something other than ``synced_at`` changed.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from personal_data_warehouse.comma_api import (
    CommaAuthError,
    CommaRequestError,
    token_expires_at,
    token_identity,
)

#: The warehouse's "never" (AGENTS.md: absence is the epoch, not NULL).
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
#: Ask for a new token this long before the 90-day one lapses.
TOKEN_RENEWAL_WARNING = timedelta(days=14)
#: Response fields that grant access to a drive: never stored.
_SECRET_ROUTE_FIELDS = ("share_sig", "share_exp")


@dataclass(frozen=True)
class CommaSyncSummary:
    devices: int
    routes_seen: int
    routes_written: int


def _utc_from_millis(value: Any, *, field: str, route: str) -> datetime:
    if value in (None, ""):
        raise CommaRequestError(f"route {route} has no {field}")
    return datetime.fromtimestamp(int(value) / 1000, tz=UTC)


def _float(value: Any) -> float:
    return float(value) if value is not None else 0.0


def route_row(route: dict[str, Any], *, account: str, synced_at: datetime) -> dict[str, Any]:
    """Translate one ``routes_segments`` entry to warehouse columns."""

    fullname = str(route.get("fullname") or "")
    dongle_id, _, route_name = fullname.partition("|")
    if not route_name:
        raise CommaRequestError(f"route {fullname!r} is not dongle|name")
    start_at = _utc_from_millis(route.get("start_time_utc_millis"), field="start_time_utc_millis", route=fullname)
    end_at = _utc_from_millis(route.get("end_time_utc_millis"), field="end_time_utc_millis", route=fullname)
    coordinates = [route.get(key) for key in ("start_lat", "start_lng", "end_lat", "end_lng")]
    raw = {key: value for key, value in route.items() if key not in _SECRET_ROUTE_FIELDS}
    return {
        "account": account,
        "dongle_id": dongle_id,
        "route_name": route_name,
        "start_at": start_at,
        "end_at": end_at,
        # comma reports distance in miles.
        "distance_miles": _float(route.get("distance")),
        "has_gps": 0 if any(value is None for value in coordinates) else 1,
        "start_lat": _float(coordinates[0]),
        "start_lng": _float(coordinates[1]),
        "end_lat": _float(coordinates[2]),
        "end_lng": _float(coordinates[3]),
        "car_make": str(route.get("make") or ""),
        "car_platform": str(route.get("platform") or ""),
        "vin": str(route.get("vin") or ""),
        "openpilot_version": str(route.get("version") or ""),
        "git_remote": str(route.get("git_remote") or ""),
        "git_branch": str(route.get("git_branch") or ""),
        "git_commit": str(route.get("git_commit") or ""),
        "segment_count": len(route.get("segment_numbers") or []),
        "is_preserved": 1 if route.get("is_preserved") else 0,
        "is_public": 1 if route.get("is_public") else 0,
        "raw_json": raw,
        "synced_at": synced_at,
    }


def device_row(
    device: dict[str, Any],
    *,
    account: str,
    stats: dict[str, Any],
    location: dict[str, Any] | None,
    polled_at: datetime,
) -> dict[str, Any]:
    total = stats.get("all") or {}
    week = stats.get("week") or {}
    ping = device.get("last_athena_ping")
    has_location = bool(location) and location.get("lat") is not None and location.get("lng") is not None
    return {
        "account": account,
        "dongle_id": str(device["dongle_id"]),
        "alias": str(device.get("alias") or ""),
        "device_type": str(device.get("device_type") or ""),
        "serial": str(device.get("serial") or ""),
        "openpilot_version": str(device.get("openpilot_version") or ""),
        "is_owner": 1 if device.get("is_owner") else 0,
        "is_paired": 1 if device.get("is_paired") else 0,
        "prime": 1 if device.get("prime") else 0,
        "last_athena_ping_at": datetime.fromtimestamp(int(ping), tz=UTC) if ping else EPOCH,
        "has_location": 1 if has_location else 0,
        "location_lat": float(location["lat"]) if has_location else 0.0,
        "location_lng": float(location["lng"]) if has_location else 0.0,
        "location_at": (
            datetime.fromtimestamp(int(location["time"]) / 1000, tz=UTC)
            if has_location and location.get("time")
            else EPOCH
        ),
        "total_distance_miles": _float(total.get("distance")),
        "total_minutes": int(total.get("minutes") or 0),
        "total_routes": int(total.get("routes") or 0),
        "week_distance_miles": _float(week.get("distance")),
        "week_minutes": int(week.get("minutes") or 0),
        "week_routes": int(week.get("routes") or 0),
        "last_polled_at": polled_at,
        "raw_json": device,
        "synced_at": polled_at,
    }


class CommaSyncRunner:
    def __init__(
        self, *, warehouse: Any, client: Any, token: str, now: Callable[[], datetime] | None = None
    ) -> None:
        self._warehouse = warehouse
        self._client = client
        self._token = token
        self._now = now or (lambda: datetime.now(tz=UTC))
        # The token names its user, so even a refused poll lands on the right state row.
        try:
            self._account = token_identity(token)
        except CommaAuthError:
            self._account = "comma"

    def sync(self) -> CommaSyncSummary:
        try:
            expires_at = token_expires_at(self._token)
            summary = self._poll()
        except CommaAuthError as exc:
            self._record("action_required", str(exc))
            raise
        except Exception as exc:
            self._record("failed", str(exc))
            raise
        now = self._now()
        status, error = "ok", ""
        if expires_at - now <= TOKEN_RENEWAL_WARNING:
            # Still a working token: keep polling, but say a new one is due.
            status = "action_required"
            error = (
                f"COMMA_API_TOKEN expires {expires_at.date().isoformat()}; sign in at connect.comma.ai "
                "and set a new one"
            )
        self._record(status, error, summary=summary, succeeded=True, token_expires_at=expires_at)
        return summary

    def _poll(self) -> CommaSyncSummary:
        me = self._client.me()
        if str(me.get("id")) != self._account:
            raise CommaAuthError(f"COMMA_API_TOKEN names user {self._account} but comma answers for {me.get('id')}")
        devices = self._client.devices()
        if not devices:
            # A device moved off the account must read as an outage.
            raise CommaRequestError("the comma account lists no device")
        now = self._now()
        seen = written = 0
        for device in devices:
            dongle_id = str(device["dongle_id"])
            self._warehouse.insert_comma_devices(
                [
                    device_row(
                        device,
                        account=self._account,
                        stats=self._client.device_stats(dongle_id),
                        location=self._client.device_location(dongle_id),
                        polled_at=now,
                    )
                ]
            )
            routes = self._client.routes(dongle_id, start_ms=0, end_ms=int(now.timestamp() * 1000))
            rows = [route_row(route, account=self._account, synced_at=now) for route in routes]
            seen += len(rows)
            written += self._warehouse.upsert_comma_routes(rows)
        return CommaSyncSummary(devices=len(devices), routes_seen=seen, routes_written=written)

    def _record(
        self,
        status: str,
        error: str,
        *,
        summary: CommaSyncSummary | None = None,
        succeeded: bool = False,
        token_expires_at: datetime | None = None,
    ) -> None:
        now = self._now()
        self._warehouse.insert_comma_sync_state(
            account=self._account,
            status=status,
            error=error,
            last_success_at=now if succeeded else None,
            token_expires_at=token_expires_at,
            devices_seen=summary.devices if summary else 0,
            routes_seen=summary.routes_seen if summary else 0,
            routes_written=summary.routes_written if summary else 0,
            updated_at=now,
        )
