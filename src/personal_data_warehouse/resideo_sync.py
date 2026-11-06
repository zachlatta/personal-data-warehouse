"""Poll the Resideo thermostat: current state, and a row for each change.

``base_resideo.thermostats`` is refreshed on every poll, so its
``last_polled_at`` proves the poller is alive when nothing changed.
``base_resideo.thermostat_readings`` gains a row only when the tracked state
differs from the newest row: read the value at a moment as the newest row at or
before it, not as a regular series.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from typing import Any

from personal_data_warehouse.resideo_api import ResideoAuthError, ResideoRequestError

#: What counts as the thermostat changing. The shadow's other fields (backlight,
#: firmware, sync transaction ids) move without anything having happened.
_TRACKED_STATE_COLUMNS = (
    "is_online",
    "indoor_temperature",
    "outdoor_temperature",
    "has_outdoor_reading",
    "indoor_humidity",
    "heat_setpoint",
    "cool_setpoint",
    "system_mode",
    "operation_mode",
    "fan_mode",
    "setpoint_status",
)


@dataclass(frozen=True)
class ResideoSyncSummary:
    devices: int
    readings_written: int


def thermostat_state(device: dict[str, Any], configuration: dict[str, Any]) -> dict[str, Any]:
    """Translate one device shadow and its configuration to warehouse columns."""

    reported = device.get("Reported") or {}
    limits = configuration.get("Reported") or {}
    indoor = reported.get("DisplayedIndoorTemperature")
    if indoor is None:
        # Zero degrees indoors would be a false reading on every chart.
        raise ResideoRequestError("the device shadow has no DisplayedIndoorTemperature")
    setpoint = reported.get("Setpoint") or {}
    outdoor = reported.get("DisplayedOutdoorTemperature")
    return {
        "is_online": 1 if reported.get("Online") else 0,
        "indoor_temperature": float(indoor),
        "outdoor_temperature": float(outdoor) if outdoor is not None else 0.0,
        "has_outdoor_reading": 0 if outdoor is None else 1,
        "indoor_humidity": int(reported.get("DisplayedIndoorHumidity") or 0),
        "heat_setpoint": float(setpoint.get("HeatSetpoint") or 0),
        "cool_setpoint": float(setpoint.get("CoolSetpoint") or 0),
        "system_mode": str(reported.get("SystemSwitch") or ""),
        "operation_mode": str((reported.get("OperationStatus") or {}).get("Mode") or ""),
        "fan_mode": str((reported.get("FanSwitch") or {}).get("Position") or ""),
        "setpoint_status": str(setpoint.get("SetpointStatus") or ""),
        "temperature_units": str(limits.get("TemperatureUnits") or ""),
        "min_heat_setpoint": float(limits.get("MinimumHeatSetpointAllowed") or 0),
        "max_heat_setpoint": float(limits.get("MaximumHeatSetpointAllowed") or 0),
        "min_cool_setpoint": float(limits.get("MinimumCoolSetpointAllowed") or 0),
        "max_cool_setpoint": float(limits.get("MaximumCoolSetpointAllowed") or 0),
        "schedule_enabled": 1 if reported.get("ScheduleEnabled") else 0,
    }


def record_thermostat_state(
    *,
    warehouse: Any,
    account: str,
    device_id: str,
    name: str,
    location_name: str,
    device_type: str,
    device: dict[str, Any],
    state: dict[str, Any],
    observed_at: datetime,
) -> bool:
    """Store one observation; returns whether it was a change."""

    fingerprint = hashlib.sha256(
        json.dumps({column: state[column] for column in _TRACKED_STATE_COLUMNS}, sort_keys=True).encode("utf-8")
    ).hexdigest()
    changed = warehouse.latest_resideo_reading_sha256(account=account, device_id=device_id) != fingerprint
    identity = {"account": account, "device_id": device_id, "name": name}
    raw_json = json.dumps(device, sort_keys=True)
    if changed:
        warehouse.insert_resideo_thermostat_readings(
            [
                {
                    **identity,
                    **{column: state[column] for column in (*_TRACKED_STATE_COLUMNS, "temperature_units")},
                    "observed_at": observed_at,
                    "state_sha256": fingerprint,
                    "raw_json": raw_json,
                    "synced_at": observed_at,
                }
            ]
        )
    warehouse.insert_resideo_thermostats(
        [
            {
                **identity,
                **state,
                "location_name": location_name,
                "device_type": device_type,
                "last_polled_at": observed_at,
                "raw_json": raw_json,
                "synced_at": observed_at,
            }
        ]
    )
    return changed


class ResideoSyncRunner:
    def __init__(self, *, warehouse: Any, client: Any, account: str, now: Callable[[], datetime] | None = None) -> None:
        self._warehouse = warehouse
        self._client = client
        self._account = account
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync(self) -> ResideoSyncSummary:
        try:
            summary = self._poll()
        except ResideoAuthError as exc:
            self._record("action_required", str(exc))
            raise
        except Exception as exc:
            self._record("failed", str(exc))
            raise
        self._record("ok", "", summary=summary, succeeded=True)
        return summary

    def _poll(self) -> ResideoSyncSummary:
        devices = self._client.list_thermostats()
        if not devices:
            # A thermostat moved off the account must read as an outage.
            raise ResideoRequestError("the Resideo account lists no devices")
        written = 0
        for listed in devices:
            device_id = listed["device_id"]
            device = self._client.get_device(device_id)
            state = thermostat_state(device, self._client.get_configuration(device_id))
            written += record_thermostat_state(
                warehouse=self._warehouse,
                account=self._account,
                device_id=device_id,
                name=listed["name"],
                location_name=listed["location_name"],
                device_type=listed["device_type"],
                device=device,
                state=state,
                observed_at=self._now(),
            )
        return ResideoSyncSummary(devices=len(devices), readings_written=written)

    def _record(
        self, status: str, error: str, *, summary: ResideoSyncSummary | None = None, succeeded: bool = False
    ) -> None:
        now = self._now()
        self._warehouse.insert_resideo_sync_state(
            account=self._account,
            status=status,
            error=error,
            last_success_at=now if succeeded else None,
            devices_seen=summary.devices if summary else 0,
            readings_written=summary.readings_written if summary else 0,
            updated_at=now,
        )
