from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import json
import time
from typing import Any

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from personal_data_warehouse.config import CalendarAccount, Settings
from personal_data_warehouse.google_auth import load_google_credentials

EPOCH_UTC = datetime.fromtimestamp(0, tz=UTC)


@dataclass(frozen=True)
class CalendarSyncSummary:
    account: str
    calendar_id: str
    sync_type: str
    next_sync_token: str
    events_written: int
    deleted_events: int
    expanded_events_written: int = 0
    expanded_deleted_events: int = 0
    expanded_synced_at: datetime | None = None
    expanded_window_start: datetime | None = None
    expanded_window_end: datetime | None = None
    recurring_changes_seen: bool = False


class CalendarSyncRunner:
    def __init__(
        self,
        *,
        settings: Settings,
        warehouse,
        logger,
        service_factory: Callable[[CalendarAccount], Any] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._settings = settings
        self._warehouse = warehouse
        self._logger = logger
        self._service_factory = service_factory or (
            lambda account: build_calendar_service(account=account, settings=settings)
        )
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync_all(self) -> list[CalendarSyncSummary]:
        self._warehouse.ensure_calendar_tables()
        state_by_key = self._warehouse.load_calendar_sync_state()
        summaries: list[CalendarSyncSummary] = []
        failures: list[str] = []

        for account in self._settings.calendar_accounts:
            service = self._service_factory(account)
            for calendar_id in account.calendar_ids:
                state = state_by_key.get((account.email_address, calendar_id))
                try:
                    summary = self._sync_calendar(
                        account=account,
                        calendar_id=calendar_id,
                        service=service,
                        state=state,
                    )
                except Exception as exc:
                    self._warehouse.insert_calendar_sync_state(
                        account=account.email_address,
                        calendar_id=calendar_id,
                        sync_token=str((state or {}).get("sync_token", "")),
                        last_sync_type=str((state or {}).get("last_sync_type", "unknown")),
                        status="failed",
                        error=str(exc),
                        expanded_synced_at=state_datetime(state, "expanded_synced_at"),
                        expanded_window_start=state_datetime(state, "expanded_window_start"),
                        expanded_window_end=state_datetime(state, "expanded_window_end"),
                        updated_at=self._now(),
                    )
                    failures.append(f"{account.email_address}/{calendar_id}: {exc}")
                    continue

                self._warehouse.insert_calendar_sync_state(
                    account=summary.account,
                    calendar_id=summary.calendar_id,
                    sync_token=summary.next_sync_token,
                    last_sync_type=summary.sync_type,
                    status="ok",
                    error="",
                    expanded_synced_at=summary.expanded_synced_at or state_datetime(state, "expanded_synced_at"),
                    expanded_window_start=summary.expanded_window_start
                    or state_datetime(state, "expanded_window_start"),
                    expanded_window_end=summary.expanded_window_end or state_datetime(state, "expanded_window_end"),
                    updated_at=self._now(),
                )
                summaries.append(summary)

        if failures:
            raise RuntimeError("Calendar sync failed for: " + "; ".join(failures))
        return summaries

    def _sync_calendar(
        self,
        *,
        account: CalendarAccount,
        calendar_id: str,
        service,
        state: Mapping[str, Any] | None,
    ) -> CalendarSyncSummary:
        sync_token = str((state or {}).get("sync_token", ""))
        if self._settings.calendar_force_full_sync or not sync_token:
            return self._sync_calendar_with_expanded_instances(
                account=account,
                calendar_id=calendar_id,
                service=service,
                state=state,
                sync_type="full",
                sync_token=None,
                force_expanded=True,
            )

        try:
            return self._sync_calendar_with_expanded_instances(
                account=account,
                calendar_id=calendar_id,
                service=service,
                state=state,
                sync_type="partial",
                sync_token=sync_token,
                force_expanded=False,
            )
        except HttpError as exc:
            if _http_status(exc) != 410:
                raise
            self._logger.warning(
                "Calendar sync token for %s/%s is stale, falling back to full sync",
                account.email_address,
                calendar_id,
            )
            return self._sync_calendar_with_expanded_instances(
                account=account,
                calendar_id=calendar_id,
                service=service,
                state=state,
                sync_type="full",
                sync_token=None,
                force_expanded=True,
            )

    def _sync_calendar_with_expanded_instances(
        self,
        *,
        account: CalendarAccount,
        calendar_id: str,
        service,
        state: Mapping[str, Any] | None,
        sync_type: str,
        sync_token: str | None,
        force_expanded: bool,
    ) -> CalendarSyncSummary:
        summary = self._sync_events(
            account=account,
            calendar_id=calendar_id,
            service=service,
            sync_type=sync_type,
            sync_token=sync_token,
        )
        if not self._should_sync_expanded_instances(
            state=state,
            force=force_expanded or summary.recurring_changes_seen,
        ):
            return summary
        expanded_summary = self._sync_expanded_instances(
            account=account,
            calendar_id=calendar_id,
            service=service,
        )
        return CalendarSyncSummary(
            account=summary.account,
            calendar_id=summary.calendar_id,
            sync_type=summary.sync_type,
            next_sync_token=summary.next_sync_token,
            events_written=summary.events_written + expanded_summary.events_written,
            deleted_events=summary.deleted_events + expanded_summary.deleted_events,
            expanded_events_written=expanded_summary.events_written,
            expanded_deleted_events=expanded_summary.deleted_events,
            expanded_synced_at=expanded_summary.expanded_synced_at,
            expanded_window_start=expanded_summary.expanded_window_start,
            expanded_window_end=expanded_summary.expanded_window_end,
            recurring_changes_seen=summary.recurring_changes_seen,
        )

    def _should_sync_expanded_instances(
        self,
        *,
        state: Mapping[str, Any] | None,
        force: bool,
    ) -> bool:
        if force:
            return True
        expanded_synced_at = state_datetime(state, "expanded_synced_at")
        if expanded_synced_at == EPOCH_UTC:
            return True
        interval = timedelta(minutes=self._settings.calendar_expanded_sync_interval_minutes)
        return self._now() - expanded_synced_at >= interval

    def _sync_events(
        self,
        *,
        account: CalendarAccount,
        calendar_id: str,
        service,
        sync_type: str,
        sync_token: str | None,
    ) -> CalendarSyncSummary:
        synced_at = self._now()
        events_written = 0
        deleted_events = 0
        next_sync_token = sync_token or ""
        recurring_changes_seen = False

        self._logger.info(
            "Starting %s Google Calendar sync for %s/%s",
            sync_type,
            account.email_address,
            calendar_id,
        )
        for events, page_sync_token in iter_event_pages(
            service=service,
            calendar_id=calendar_id,
            page_size=self._settings.calendar_page_size,
            sync_token=sync_token,
        ):
            rows = [
                event_to_row(
                    account=account.email_address,
                    calendar_id=calendar_id,
                    event=event,
                    synced_at=synced_at,
                )
                for event in events
            ]
            recurring_changes_seen = recurring_changes_seen or any(is_recurring_related_event(event) for event in events)
            self._warehouse.insert_calendar_events(rows)
            events_written += len(rows)
            deleted_events += sum(1 for row in rows if row["is_deleted"])
            if page_sync_token:
                next_sync_token = page_sync_token
            self._logger.info(
                "Synced %s Google Calendar events for %s/%s so far",
                events_written,
                account.email_address,
                calendar_id,
            )

        return CalendarSyncSummary(
            account=account.email_address,
            calendar_id=calendar_id,
            sync_type=sync_type,
            next_sync_token=next_sync_token,
            events_written=events_written,
            deleted_events=deleted_events,
            recurring_changes_seen=recurring_changes_seen,
        )

    def _sync_expanded_instances(
        self,
        *,
        account: CalendarAccount,
        calendar_id: str,
        service,
    ) -> CalendarSyncSummary:
        synced_at = self._now()
        window_start = synced_at - timedelta(days=self._settings.calendar_expanded_sync_lookback_days)
        window_end = synced_at + timedelta(days=self._settings.calendar_expanded_sync_lookahead_days)
        active_recurring_event_ids = set(
            self._warehouse.load_active_recurring_calendar_event_ids(
                account=account.email_address,
                calendar_id=calendar_id,
                window_start=window_start,
                window_end=window_end,
            )
        )
        seen_recurring_event_ids: set[str] = set()
        events_written = 0
        deleted_events = 0

        self._logger.info(
            "Starting expanded Google Calendar instance sync for %s/%s from %s to %s",
            account.email_address,
            calendar_id,
            window_start.isoformat(),
            window_end.isoformat(),
        )
        for events in iter_expanded_event_pages(
            service=service,
            calendar_id=calendar_id,
            page_size=self._settings.calendar_page_size,
            time_min=window_start,
            time_max=window_end,
        ):
            rows = [
                event_to_row(
                    account=account.email_address,
                    calendar_id=calendar_id,
                    event=event,
                    synced_at=synced_at,
                )
                for event in events
                if event.get("recurringEventId")
            ]
            for row in rows:
                if row["recurring_event_id"]:
                    seen_recurring_event_ids.add(row["event_id"])
            self._warehouse.insert_calendar_events(rows)
            events_written += len(rows)
            deleted_events += sum(1 for row in rows if row["is_deleted"])
            self._logger.info(
                "Synced %s expanded Google Calendar instances for %s/%s so far",
                events_written,
                account.email_address,
                calendar_id,
            )

        stale_event_ids = sorted(active_recurring_event_ids - seen_recurring_event_ids)
        stale_deleted_events = self._warehouse.mark_calendar_events_deleted(
            account=account.email_address,
            calendar_id=calendar_id,
            event_ids=stale_event_ids,
            synced_at=synced_at,
        )
        if stale_deleted_events:
            self._logger.info(
                "Marked %s stale expanded Google Calendar instances deleted for %s/%s",
                stale_deleted_events,
                account.email_address,
                calendar_id,
            )
        return CalendarSyncSummary(
            account=account.email_address,
            calendar_id=calendar_id,
            sync_type="expanded",
            next_sync_token="",
            events_written=events_written + stale_deleted_events,
            deleted_events=deleted_events + stale_deleted_events,
            expanded_events_written=events_written + stale_deleted_events,
            expanded_deleted_events=deleted_events + stale_deleted_events,
            expanded_synced_at=synced_at,
            expanded_window_start=window_start,
            expanded_window_end=window_end,
        )


def build_calendar_service(*, account: CalendarAccount, settings: Settings):
    credentials = load_google_credentials(
        email_address=account.email_address,
        settings=settings,
        scopes=settings.calendar_scopes,
        service_name="Google Calendar",
    )
    return build("calendar", "v3", credentials=credentials, cache_discovery=False)


def iter_event_pages(
    *,
    service,
    calendar_id: str,
    page_size: int,
    sync_token: str | None = None,
) -> Iterator[tuple[list[Mapping[str, Any]], str | None]]:
    page_token: str | None = None
    while True:
        list_kwargs: dict[str, Any] = {
            "calendarId": calendar_id,
            "maxResults": page_size,
            "showDeleted": True,
        }
        if page_token:
            list_kwargs["pageToken"] = page_token
        if sync_token:
            list_kwargs["syncToken"] = sync_token

        response = execute_calendar_request(lambda: service.events().list(**list_kwargs).execute())
        items = [item for item in response.get("items", []) if isinstance(item, Mapping)]
        next_sync_token = response.get("nextSyncToken")
        yield items, str(next_sync_token) if next_sync_token else None

        page_token = response.get("nextPageToken")
        if not page_token:
            return


def iter_expanded_event_pages(
    *,
    service,
    calendar_id: str,
    page_size: int,
    time_min: datetime,
    time_max: datetime,
) -> Iterator[list[Mapping[str, Any]]]:
    page_token: str | None = None
    while True:
        list_kwargs: dict[str, Any] = {
            "calendarId": calendar_id,
            "maxResults": page_size,
            "showDeleted": True,
            "singleEvents": True,
            "orderBy": "startTime",
            "timeMin": calendar_api_datetime(time_min),
            "timeMax": calendar_api_datetime(time_max),
        }
        if page_token:
            list_kwargs["pageToken"] = page_token

        response = execute_calendar_request(lambda: service.events().list(**list_kwargs).execute())
        yield [item for item in response.get("items", []) if isinstance(item, Mapping)]

        page_token = response.get("nextPageToken")
        if not page_token:
            return


def execute_calendar_request(operation: Callable[[], Any], *, max_attempts: int = 5) -> Any:
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except HttpError as exc:
            status = _http_status(exc)
            if status not in {429, 500, 502, 503, 504} or attempt == max_attempts:
                raise
            time.sleep(min(60, attempt))
    raise RuntimeError("unreachable Calendar retry state")


def event_to_row(
    *,
    account: str,
    calendar_id: str,
    event: Mapping[str, Any],
    synced_at: datetime,
) -> dict[str, Any]:
    start = event.get("start") if isinstance(event.get("start"), Mapping) else {}
    end = event.get("end") if isinstance(event.get("end"), Mapping) else {}
    updated_at = parse_rfc3339(str(event.get("updated", ""))) or EPOCH_UTC
    status = str(event.get("status", ""))
    return {
        "account": account,
        "calendar_id": calendar_id,
        "event_id": str(event.get("id", "")),
        "recurring_event_id": str(event.get("recurringEventId", "")),
        "i_cal_uid": str(event.get("iCalUID", "")),
        "status": status,
        "is_deleted": 1 if status == "cancelled" else 0,
        "summary": str(event.get("summary", "")),
        "description": str(event.get("description", "")),
        "location": str(event.get("location", "")),
        "creator_email": nested_string(event, "creator", "email"),
        "organizer_email": nested_string(event, "organizer", "email"),
        "start_at": parse_calendar_event_time(start) or EPOCH_UTC,
        "end_at": parse_calendar_event_time(end) or EPOCH_UTC,
        "start_date": str(start.get("date", "")),
        "end_date": str(end.get("date", "")),
        "is_all_day": 1 if start.get("date") else 0,
        "html_link": str(event.get("htmlLink", "")),
        "attendees_json": json_dumps(event.get("attendees", [])),
        "reminders_json": json_dumps(event.get("reminders", {})),
        "recurrence": [str(item) for item in event.get("recurrence", [])]
        if isinstance(event.get("recurrence", []), list)
        else [],
        "event_type": str(event.get("eventType", "")),
        "raw_json": json_dumps(event),
        "updated_at": updated_at,
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def parse_calendar_event_time(value: Mapping[str, Any]) -> datetime | None:
    date_time = value.get("dateTime")
    if date_time:
        return parse_rfc3339(str(date_time))
    date_value = value.get("date")
    if date_value:
        parsed_date = date.fromisoformat(str(date_value))
        return datetime(parsed_date.year, parsed_date.month, parsed_date.day, tzinfo=UTC)
    return None


def parse_rfc3339(value: str) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def nested_string(value: Mapping[str, Any], key: str, nested_key: str) -> str:
    nested = value.get(key)
    if not isinstance(nested, Mapping):
        return ""
    return str(nested.get(nested_key, ""))


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def sync_version_from_datetime(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000)


def state_datetime(state: Mapping[str, Any] | None, key: str) -> datetime:
    value = (state or {}).get(key)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str) and value:
        return parse_rfc3339(value) or EPOCH_UTC
    return EPOCH_UTC


def is_recurring_related_event(event: Mapping[str, Any]) -> bool:
    return bool(event.get("recurringEventId") or event.get("recurrence") or event.get("status") == "cancelled")


def calendar_api_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _http_status(exc: HttpError) -> int | None:
    try:
        return int(exc.resp.status)
    except (TypeError, ValueError, AttributeError):
        return None
