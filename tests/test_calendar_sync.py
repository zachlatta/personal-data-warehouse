from __future__ import annotations

from datetime import UTC, datetime
import base64
import json

from googleapiclient.errors import HttpError
from httplib2 import Response

from personal_data_warehouse.calendar_sync import (
    CalendarSyncRunner,
    event_to_row,
    iter_event_pages,
    parse_calendar_event_time,
)
from personal_data_warehouse.clickhouse import ClickHouseWarehouse
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.calendar_sync import calendar_event_sync_every_minute
from personal_data_warehouse.google_auth import google_token_json_from_env


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeCalendarRequest:
    def __init__(self, service, response=None, error=None) -> None:
        self._service = service
        self._response = response
        self._error = error

    def execute(self):
        if self._error is not None:
            raise self._error
        return self._response


class FakeCalendarEventsResource:
    def __init__(self, service) -> None:
        self._service = service

    def list(self, **kwargs):
        self._service.list_calls.append(kwargs)
        if self._service.errors:
            return FakeCalendarRequest(self._service, error=self._service.errors.pop(0))
        return FakeCalendarRequest(self._service, response=self._service.responses.pop(0))


class FakeCalendarService:
    def __init__(self, responses, errors=None) -> None:
        self.responses = list(responses)
        self.errors = list(errors or [])
        self.list_calls = []

    def events(self):
        return FakeCalendarEventsResource(self)


class FakeCalendarWarehouse:
    def __init__(self, state=None) -> None:
        self.state = state or {}
        self.events = []
        self.state_rows = []
        self.ensure_calendar_tables_called = False

    def ensure_calendar_tables(self) -> None:
        self.ensure_calendar_tables_called = True

    def load_calendar_sync_state(self):
        return self.state

    def insert_calendar_events(self, rows) -> None:
        self.events.extend(rows)

    def insert_calendar_sync_state(self, **row) -> None:
        self.state_rows.append(row)


def test_load_settings_defaults_calendar_accounts_to_gmail_accounts(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")

    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)

    assert settings.calendar_accounts[0].email_address == "zach@example.com"
    assert settings.calendar_accounts[0].calendar_ids == ("primary",)
    assert "https://www.googleapis.com/auth/calendar.readonly" in settings.google_scopes
    assert "https://www.googleapis.com/auth/gmail.readonly" in settings.google_scopes


def test_google_token_json_from_env_accepts_shared_google_token(monkeypatch) -> None:
    token_json = json.dumps({"token": "access-token"})
    encoded_token = base64.b64encode(token_json.encode("utf-8")).decode("ascii")
    monkeypatch.setenv("GOOGLE_ZACH_EXAMPLE_COM_TOKEN_JSON_B64", encoded_token)

    assert google_token_json_from_env("zach@example.com") == token_json


def test_calendar_sync_schedule_runs_every_minute_by_default() -> None:
    assert calendar_event_sync_every_minute.cron_schedule == "* * * * *"
    assert calendar_event_sync_every_minute.default_status.value == "RUNNING"


def test_load_settings_accepts_calendar_specific_accounts_and_ids(monkeypatch) -> None:
    monkeypatch.setenv("CALENDAR_ACCOUNTS", "zach@example.com")
    monkeypatch.setenv("CALENDAR_ZACH_EXAMPLE_COM_CALENDAR_IDS", "primary,team@example.com")
    monkeypatch.setenv("CALENDAR_PAGE_SIZE", "100")

    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)

    assert settings.calendar_accounts[0].email_address == "zach@example.com"
    assert settings.calendar_accounts[0].calendar_ids == ("primary", "team@example.com")
    assert settings.calendar_page_size == 100


def test_parse_calendar_event_time_handles_datetime_and_all_day() -> None:
    assert parse_calendar_event_time({"dateTime": "2026-04-23T12:30:00-05:00"}) == datetime(
        2026,
        4,
        23,
        17,
        30,
        tzinfo=UTC,
    )
    assert parse_calendar_event_time({"date": "2026-04-23"}) == datetime(2026, 4, 23, tzinfo=UTC)


def test_event_to_row_preserves_searchable_fields_and_raw_payload() -> None:
    event = {
        "id": "event-1",
        "iCalUID": "uid-1",
        "status": "confirmed",
        "summary": "Board meeting",
        "description": "Discuss roadmap",
        "location": "HQ",
        "creator": {"email": "creator@example.com"},
        "organizer": {"email": "organizer@example.com"},
        "start": {"dateTime": "2026-04-23T12:30:00-05:00"},
        "end": {"dateTime": "2026-04-23T13:00:00-05:00"},
        "attendees": [{"email": "friend@example.com"}],
        "reminders": {"useDefault": True},
        "recurrence": ["RRULE:FREQ=WEEKLY"],
        "updated": "2026-04-23T18:00:00Z",
        "htmlLink": "https://calendar.google.com/event",
    }

    row = event_to_row(
        account="zach@example.com",
        calendar_id="primary",
        event=event,
        synced_at=datetime(2026, 4, 23, 19, tzinfo=UTC),
    )

    assert row["account"] == "zach@example.com"
    assert row["calendar_id"] == "primary"
    assert row["event_id"] == "event-1"
    assert row["summary"] == "Board meeting"
    assert row["start_at"] == datetime(2026, 4, 23, 17, 30, tzinfo=UTC)
    assert row["is_all_day"] == 0
    assert json.loads(row["attendees_json"]) == [{"email": "friend@example.com"}]
    assert json.loads(row["raw_json"])["description"] == "Discuss roadmap"


def test_iter_event_pages_collects_next_sync_token() -> None:
    service = FakeCalendarService(
        [
            {"items": [{"id": "1"}], "nextPageToken": "page-2"},
            {"items": [{"id": "2"}], "nextSyncToken": "sync-token"},
        ]
    )

    pages = list(iter_event_pages(service=service, calendar_id="primary", page_size=50))

    assert pages == [([{"id": "1"}], None), ([{"id": "2"}], "sync-token")]
    assert service.list_calls[0]["calendarId"] == "primary"
    assert service.list_calls[0]["showDeleted"] is True
    assert service.list_calls[1]["pageToken"] == "page-2"


def test_runner_full_sync_writes_events_and_state(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)
    service = FakeCalendarService([{"items": [{"id": "event-1", "summary": "Sync"}], "nextSyncToken": "next"}])
    warehouse = FakeCalendarWarehouse()

    runner = CalendarSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
    )
    summaries = runner.sync_all()

    assert warehouse.ensure_calendar_tables_called
    assert summaries[0].sync_type == "full"
    assert summaries[0].events_written == 1
    assert summaries[0].deleted_events == 0
    assert warehouse.events[0]["event_id"] == "event-1"
    assert warehouse.state_rows[0]["sync_token"] == "next"
    assert warehouse.state_rows[0]["status"] == "ok"


def test_runner_incremental_sync_uses_sync_token_and_counts_cancelled(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)
    service = FakeCalendarService(
        [{"items": [{"id": "event-1", "status": "cancelled"}], "nextSyncToken": "new-token"}]
    )
    warehouse = FakeCalendarWarehouse(
        state={
            ("zach@example.com", "primary"): {
                "sync_token": "old-token",
                "last_sync_type": "full",
            }
        }
    )

    runner = CalendarSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
    )
    summaries = runner.sync_all()

    assert service.list_calls[0]["syncToken"] == "old-token"
    assert summaries[0].sync_type == "partial"
    assert summaries[0].deleted_events == 1
    assert warehouse.events[0]["is_deleted"] == 1
    assert warehouse.state_rows[0]["sync_token"] == "new-token"


def test_runner_falls_back_to_full_sync_when_sync_token_is_stale(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)
    stale_token_error = HttpError(Response({"status": "410"}), b"sync token expired")
    service = FakeCalendarService(
        responses=[{"items": [{"id": "event-1"}], "nextSyncToken": "fresh-token"}],
        errors=[stale_token_error],
    )
    warehouse = FakeCalendarWarehouse(
        state={
            ("zach@example.com", "primary"): {
                "sync_token": "stale-token",
                "last_sync_type": "partial",
            }
        }
    )

    runner = CalendarSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
    )
    summaries = runner.sync_all()

    assert summaries[0].sync_type == "full"
    assert "syncToken" not in service.list_calls[1]
    assert warehouse.state_rows[0]["sync_token"] == "fresh-token"


def test_clickhouse_insert_calendar_events_batches_by_event_month() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    inserted_batches = []

    def fake_insert_rows(table, rows, columns):
        inserted_batches.append((table, rows, columns))

    warehouse._insert_rows = fake_insert_rows

    warehouse.insert_calendar_events(
        [
            {"start_at": datetime(2026, 4, 1, tzinfo=UTC)},
            {"start_at": datetime(2026, 5, 1, tzinfo=UTC)},
            {"start_at": datetime(2026, 4, 2, tzinfo=UTC)},
        ]
    )

    assert [len(batch[1]) for batch in inserted_batches] == [2, 1]
    assert all(batch[0] == "calendar_events" for batch in inserted_batches)
