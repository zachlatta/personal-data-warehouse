"""Execute `resideo.set_thermostat`: change a setpoint on the thermostat.

The proposal half is Go (app/internal/mutations/resideo.go). The worker reads
the device first, because the review may be old: it refuses a value outside the
device's own limits, skips a setpoint already held (so a replay writes nothing),
and refuses an approval old enough that a retry would change the heat at a time
nobody chose. A 202 only means Resideo queued the command, so the worker then
reads the device back and reports whether the change showed up.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import time
from typing import Any

from personal_data_warehouse.config import RESIDEO_ACCOUNT
from personal_data_warehouse.resideo_api import (
    COOL,
    HEAT,
    ResideoAuthError,
    ResideoClient,
    ResideoError,
    ResideoRequestError,
    ResideoTransientError,
    WarehouseTokenSource,
)
from personal_data_warehouse.resideo_sync import record_thermostat_state, thermostat_state

RESIDEO_PROVIDER = "resideo"
RESIDEO_SET_THERMOSTAT_OPERATION = "resideo.set_thermostat"

_SETPOINT_STATUS = {"temporary": "TemporaryHold", "permanent": "PermanentHold"}
_MAX_APPROVAL_AGE = timedelta(hours=1)
_READ_BACK_ATTEMPTS = 10
_READ_BACK_INTERVAL_SECONDS = 3
_RESULT_COLUMNS = ("indoor_temperature", "heat_setpoint", "cool_setpoint", "system_mode", "setpoint_status")


@dataclass(frozen=True)
class ResideoMutationResult:
    status: str
    result_json: dict[str, Any]
    error: str = ""


class ResideoMutationExecutor:
    def __init__(
        self,
        *,
        warehouse: Any,
        account: str,
        client_factory: Callable[[], Any],
        now: Callable[[], datetime] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._warehouse = warehouse
        self._account = account
        self._client_factory = client_factory
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._sleep = sleep

    def execute(self, mutation: Mapping[str, Any]) -> ResideoMutationResult:
        try:
            return self._execute(mutation)
        except ResideoAuthError as exc:
            return ResideoMutationResult("blocked_missing_credentials", {}, str(exc))
        except ResideoTransientError as exc:
            return ResideoMutationResult("failed_retryable", {}, str(exc))
        except ResideoError as exc:
            return ResideoMutationResult("failed_terminal", {}, str(exc))

    def _execute(self, mutation: Mapping[str, Any]) -> ResideoMutationResult:
        if mutation.get("operation") != RESIDEO_SET_THERMOSTAT_OPERATION:
            raise ResideoRequestError(f"unsupported Resideo operation {mutation.get('operation')!r}")
        approved_at = mutation.get("approved_at")
        if isinstance(approved_at, datetime) and self._now() - approved_at > _MAX_APPROVAL_AGE:
            raise ResideoRequestError(
                "the approval is over an hour old, so this was not applied; propose it again if it is still wanted"
            )
        payload = mutation.get("payload_json") or {}
        device_id = str(payload.get("device_id") or "")
        status = _SETPOINT_STATUS.get(str(payload.get("hold") or ""))
        if not device_id or status is None:
            raise ResideoRequestError("the mutation has no device_id or a hold other than temporary or permanent")
        targets = {
            kind: float(payload[key])
            for kind, key in ((HEAT, "heat_setpoint"), (COOL, "cool_setpoint"))
            if payload.get(key) is not None
        }
        if not targets:
            raise ResideoRequestError("the mutation names no setpoint")

        client = self._client_factory()
        configuration = client.get_configuration(device_id)
        before = thermostat_state(client.get_device(device_id), configuration)
        for kind, value in targets.items():
            low, high = before[f"min_{kind}_setpoint"], before[f"max_{kind}_setpoint"]
            if not low <= value <= high:
                raise ResideoRequestError(f"{kind} setpoint {value:g} is outside this thermostat's {low:g} to {high:g}")
        if not before["is_online"]:
            raise ResideoTransientError("the thermostat is offline")

        pending = {kind: value for kind, value in targets.items() if not _holds(before, kind, value, status)}
        if not pending:
            return ResideoMutationResult("succeeded", {"already_set": True, "current": _summary(before)})
        for kind, value in pending.items():
            client.set_setpoint(device_id, kind=kind, value=value, setpoint_status=status)

        device, after, verified = self._read_back(client, device_id, configuration, targets, status)
        listed = next((row for row in client.list_thermostats() if row["device_id"] == device_id), {})
        record_thermostat_state(
            warehouse=self._warehouse,
            account=self._account,
            device_id=device_id,
            name=str(listed.get("name") or ""),
            location_name=str(listed.get("location_name") or ""),
            device_type=str(listed.get("device_type") or ""),
            device=device,
            state=after,
            observed_at=self._now(),
        )
        return ResideoMutationResult(
            "succeeded",
            {"verified": verified, "previous": _summary(before), "current": _summary(after)},
        )

    def _read_back(
        self, client: Any, device_id: str, configuration: dict[str, Any], targets: dict[str, float], status: str
    ) -> tuple[dict[str, Any], dict[str, Any], bool]:
        for attempt in range(_READ_BACK_ATTEMPTS):
            device = client.get_device(device_id)
            state = thermostat_state(device, configuration)
            if all(_holds(state, kind, value, status) for kind, value in targets.items()):
                return device, state, True
            if attempt < _READ_BACK_ATTEMPTS - 1:
                self._sleep(_READ_BACK_INTERVAL_SECONDS)
        return device, state, False


def _holds(state: Mapping[str, Any], kind: str, value: float, status: str) -> bool:
    return state[f"{kind}_setpoint"] == value and state["setpoint_status"] == status


def _summary(state: Mapping[str, Any]) -> dict[str, Any]:
    return {column: state[column] for column in _RESULT_COLUMNS}


def resideo_mutation_executor(*, settings: Any, warehouse: Any) -> ResideoMutationExecutor:
    config = settings.resideo

    def client_factory() -> ResideoClient:
        if config is None:
            raise ResideoAuthError("RESIDEO_API_SUBSCRIPTION_KEY is not set on the Dagster deployment")
        return ResideoClient(
            tokens=WarehouseTokenSource(
                warehouse=warehouse, account=config.account, timeout=config.request_timeout_seconds
            ),
            subscription_key=config.subscription_key,
            timeout=config.request_timeout_seconds,
        )

    return ResideoMutationExecutor(
        warehouse=warehouse, account=RESIDEO_ACCOUNT if config is None else config.account, client_factory=client_factory
    )
