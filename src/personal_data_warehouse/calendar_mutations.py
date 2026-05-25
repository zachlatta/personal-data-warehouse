from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import ssl
from typing import Any

from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from personal_data_warehouse.calendar_sync import execute_calendar_request
from personal_data_warehouse.config import CALENDAR_MUTATION_SCOPE, Settings
from personal_data_warehouse.google_auth import load_google_credentials


CALENDAR_PROVIDER = "google_calendar"
CALENDAR_CREATE_EVENT_OPERATION = "calendar.create_event"
CALENDAR_UPDATE_EVENT_OPERATION = "calendar.update_event"
CALENDAR_DELETE_EVENT_OPERATION = "calendar.delete_event"

CALENDAR_MUTATION_SCOPES: tuple[str, ...] = (CALENDAR_MUTATION_SCOPE,)

_SEND_UPDATES_VALUES = {"all", "externalOnly", "none"}


@dataclass(frozen=True)
class CalendarMutationResult:
    status: str
    result_json: dict[str, Any]
    error: str = ""


class CalendarMutationExecutor:
    def __init__(
        self,
        *,
        settings: Settings,
        service_factory: Callable[[str], Any] | None = None,
    ) -> None:
        self._settings = settings
        self._service_factory = service_factory

    def execute(self, mutation: Mapping[str, Any]) -> CalendarMutationResult:
        operation = str(mutation.get("operation") or "")
        if mutation.get("provider") != CALENDAR_PROVIDER or operation not in {
            CALENDAR_CREATE_EVENT_OPERATION,
            CALENDAR_UPDATE_EVENT_OPERATION,
            CALENDAR_DELETE_EVENT_OPERATION,
        }:
            return CalendarMutationResult(
                status="failed_terminal",
                result_json={},
                error=f"unsupported mutation operation: {mutation.get('provider')}.{mutation.get('operation')}",
            )
        account = str(mutation.get("account") or "")
        payload = _mapping(mutation.get("payload_json"))
        calendar_id = str(payload.get("calendar_id") or "primary").strip() or "primary"

        try:
            send_updates = _send_updates(payload.get("send_updates"))
            service = self._service(account=account)
            if operation == CALENDAR_CREATE_EVENT_OPERATION:
                return self._create_event(
                    service=service,
                    calendar_id=calendar_id,
                    send_updates=send_updates,
                    event=_mapping(payload.get("event")),
                )
            if operation == CALENDAR_UPDATE_EVENT_OPERATION:
                return self._update_event(
                    service=service,
                    calendar_id=calendar_id,
                    send_updates=send_updates,
                    event_id=str(payload.get("event_id") or "").strip(),
                    expected_etag=str(payload.get("expected_etag") or "").strip(),
                    patch=_mapping(payload.get("patch")),
                )
            return self._delete_event(
                service=service,
                calendar_id=calendar_id,
                send_updates=send_updates,
                event_id=str(payload.get("event_id") or "").strip(),
                expected_etag=str(payload.get("expected_etag") or "").strip(),
            )
        except Exception as exc:
            return CalendarMutationResult(
                status=calendar_mutation_failure_status(exc),
                result_json={"calendar_id": calendar_id},
                error=str(exc),
            )

    def _create_event(
        self,
        *,
        service,
        calendar_id: str,
        send_updates: str,
        event: Mapping[str, Any],
    ) -> CalendarMutationResult:
        if not event:
            return CalendarMutationResult(
                status="failed_terminal",
                result_json={"calendar_id": calendar_id},
                error="event must not be empty",
            )
        response = execute_calendar_request(
            lambda: service.events()
            .insert(calendarId=calendar_id, body=dict(event), sendUpdates=send_updates)
            .execute()
        )
        return CalendarMutationResult(
            status="succeeded",
            result_json={
                "calendar_id": calendar_id,
                "event_id": str(response.get("id") or ""),
                "etag": str(response.get("etag") or ""),
                "html_link": str(response.get("htmlLink") or ""),
                "send_updates": send_updates,
                "response": response,
            },
        )

    def _update_event(
        self,
        *,
        service,
        calendar_id: str,
        send_updates: str,
        event_id: str,
        expected_etag: str,
        patch: Mapping[str, Any],
    ) -> CalendarMutationResult:
        if not event_id:
            return CalendarMutationResult(
                status="failed_terminal",
                result_json={"calendar_id": calendar_id},
                error="event_id must not be blank",
            )
        if not patch:
            return CalendarMutationResult(
                status="failed_terminal",
                result_json={"calendar_id": calendar_id, "event_id": event_id},
                error="patch must include at least one field to update",
            )
        live = self._get_event(service=service, calendar_id=calendar_id, event_id=event_id)
        _raise_if_stale(event_id=event_id, expected_etag=expected_etag, live=live)
        response = execute_calendar_request(
            lambda: service.events()
            .patch(
                calendarId=calendar_id,
                eventId=event_id,
                body=dict(patch),
                sendUpdates=send_updates,
            )
            .execute()
        )
        return CalendarMutationResult(
            status="succeeded",
            result_json={
                "calendar_id": calendar_id,
                "event_id": str(response.get("id") or event_id),
                "etag": str(response.get("etag") or ""),
                "html_link": str(response.get("htmlLink") or ""),
                "send_updates": send_updates,
                "response": response,
            },
        )

    def _delete_event(
        self,
        *,
        service,
        calendar_id: str,
        send_updates: str,
        event_id: str,
        expected_etag: str,
    ) -> CalendarMutationResult:
        if not event_id:
            return CalendarMutationResult(
                status="failed_terminal",
                result_json={"calendar_id": calendar_id},
                error="event_id must not be blank",
            )
        live = self._get_event(service=service, calendar_id=calendar_id, event_id=event_id)
        _raise_if_stale(event_id=event_id, expected_etag=expected_etag, live=live)
        execute_calendar_request(
            lambda: service.events()
            .delete(calendarId=calendar_id, eventId=event_id, sendUpdates=send_updates)
            .execute()
        )
        return CalendarMutationResult(
            status="succeeded",
            result_json={
                "calendar_id": calendar_id,
                "event_id": event_id,
                "send_updates": send_updates,
            },
        )

    def _get_event(self, *, service, calendar_id: str, event_id: str) -> dict[str, Any]:
        return dict(
            execute_calendar_request(
                lambda: service.events().get(calendarId=calendar_id, eventId=event_id).execute()
            )
        )

    def _service(self, *, account: str):
        if self._service_factory is not None:
            return self._service_factory(account)
        return build_calendar_mutation_service(account=account, settings=self._settings)


def build_calendar_mutation_service(*, account: str, settings: Settings):
    credentials = load_google_credentials(
        email_address=settings.calendar_account_for_email(account).email_address,
        settings=settings,
        scopes=settings.calendar_mutation_scopes,
        service_name="Google Calendar mutation",
    )
    return build("calendar", "v3", credentials=credentials, cache_discovery=False)


def calendar_mutation_failure_status(exc: Exception) -> str:
    if isinstance(exc, HttpError):
        status = getattr(exc.resp, "status", None)
        if status in {401, 403}:
            return "blocked_missing_credentials"
        if status == 412:
            return "failed_terminal"
        if status in {429, 500, 502, 503, 504}:
            return "failed_retryable"
        return "failed_terminal"
    if isinstance(exc, RefreshError):
        return "blocked_missing_credentials"
    if isinstance(exc, (ConnectionError, TimeoutError, OSError, ssl.SSLError)):
        return "failed_retryable"
    if isinstance(exc, RuntimeError) and ("OAuth token" in str(exc) or "cannot be refreshed" in str(exc)):
        return "blocked_missing_credentials"
    return "failed_terminal"


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _send_updates(value: Any) -> str:
    raw = str(value or "all").strip()
    if raw not in _SEND_UPDATES_VALUES:
        raise ValueError(f"send_updates must be one of {sorted(_SEND_UPDATES_VALUES)}; got {raw!r}")
    return raw


def _raise_if_stale(*, event_id: str, expected_etag: str, live: Mapping[str, Any]) -> None:
    if not expected_etag:
        return
    live_etag = str(live.get("etag") or "")
    if live_etag != expected_etag:
        raise ValueError(
            f"calendar event {event_id} changed since proposal: expected etag {expected_etag}, got {live_etag}"
        )
