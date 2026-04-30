from __future__ import annotations

from datetime import UTC, datetime
import base64
import json

from googleapiclient.errors import HttpError
from httplib2 import Response

from personal_data_warehouse.calendar_sync import (
    CalendarSyncRunner,
    calendar_api_datetime,
    event_to_row,
    iter_expanded_event_pages,
    iter_event_pages,
    parse_calendar_event_time,
)
from personal_data_warehouse.clickhouse import CALENDAR_EVENT_COLUMNS, CALENDAR_SYNC_STATE_COLUMNS, ClickHouseWarehouse
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
        self.active_recurring_event_ids = []
        self.active_recurring_event_rows = []
        self.active_calendar_events_by_id = {}
        self.deleted_event_ids = []
        self.state_rows = []
        self.ensure_calendar_tables_called = False

    def ensure_calendar_tables(self) -> None:
        self.ensure_calendar_tables_called = True

    def load_calendar_sync_state(self):
        return self.state

    def insert_calendar_events(self, rows) -> None:
        self.events.extend(rows)

    def load_active_recurring_calendar_event_ids(self, **kwargs):
        if self.active_recurring_event_rows:
            return [
                row["event_id"]
                for row in self.active_recurring_event_rows
                if row["start_at"] < kwargs["window_end"] and row["end_at"] > kwargs["window_start"]
            ]
        return self.active_recurring_event_ids

    def load_active_calendar_events_by_id(self, **kwargs):
        return {
            event_id: self.active_calendar_events_by_id[event_id]
            for event_id in kwargs["event_ids"]
            if event_id in self.active_calendar_events_by_id
        }

    def mark_calendar_events_deleted(self, **kwargs) -> int:
        self.deleted_event_ids.extend(kwargs["event_ids"])
        return len(kwargs["event_ids"])

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
    monkeypatch.setenv("CALENDAR_EXPANDED_SYNC_LOOKBACK_DAYS", "30")
    monkeypatch.setenv("CALENDAR_EXPANDED_SYNC_LOOKAHEAD_DAYS", "60")
    monkeypatch.setenv("CALENDAR_EXPANDED_SYNC_INTERVAL_MINUTES", "120")

    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)

    assert settings.calendar_accounts[0].email_address == "zach@example.com"
    assert settings.calendar_accounts[0].calendar_ids == ("primary", "team@example.com")
    assert settings.calendar_page_size == 100
    assert settings.calendar_expanded_sync_lookback_days == 30
    assert settings.calendar_expanded_sync_lookahead_days == 60
    assert settings.calendar_expanded_sync_interval_minutes == 120


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


def test_iter_expanded_event_pages_requests_single_events_window() -> None:
    service = FakeCalendarService(
        [
            {"items": [{"id": "1"}], "nextPageToken": "page-2"},
            {"items": [{"id": "2"}]},
        ]
    )

    pages = list(
        iter_expanded_event_pages(
            service=service,
            calendar_id="primary",
            page_size=50,
            time_min=datetime(2026, 4, 1, tzinfo=UTC),
            time_max=datetime(2026, 5, 1, tzinfo=UTC),
        )
    )

    assert pages == [[{"id": "1"}], [{"id": "2"}]]
    assert service.list_calls[0]["singleEvents"] is True
    assert service.list_calls[0]["orderBy"] == "startTime"
    assert service.list_calls[0]["showDeleted"] is True
    assert service.list_calls[0]["timeMin"] == "2026-04-01T00:00:00Z"
    assert service.list_calls[0]["timeMax"] == "2026-05-01T00:00:00Z"
    assert "syncToken" not in service.list_calls[0]
    assert service.list_calls[1]["pageToken"] == "page-2"


def test_runner_full_sync_writes_events_and_state(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)
    service = FakeCalendarService(
        [
            {"items": [{"id": "event-1", "summary": "Sync"}], "nextSyncToken": "next"},
            {"items": [{"id": "event-1", "summary": "Sync"}]},
        ]
    )
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
    assert summaries[0].expanded_events_written == 0
    assert summaries[0].deleted_events == 0
    assert warehouse.events[0]["event_id"] == "event-1"
    assert warehouse.state_rows[0]["sync_token"] == "next"
    assert warehouse.state_rows[0]["status"] == "ok"
    assert service.list_calls[1]["singleEvents"] is True


def test_runner_incremental_sync_uses_sync_token_and_counts_cancelled(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)
    service = FakeCalendarService(
        [
            {"items": [{"id": "event-1", "status": "cancelled"}], "nextSyncToken": "new-token"},
            {"items": []},
        ]
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
    assert "syncToken" not in service.list_calls[1]
    assert service.list_calls[1]["singleEvents"] is True
    assert summaries[0].sync_type == "partial"
    assert summaries[0].deleted_events == 1
    assert warehouse.events[0]["is_deleted"] == 1
    assert warehouse.state_rows[0]["sync_token"] == "new-token"


def test_runner_falls_back_to_full_sync_when_sync_token_is_stale(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)
    stale_token_error = HttpError(Response({"status": "410"}), b"sync token expired")
    service = FakeCalendarService(
        responses=[
            {"items": [{"id": "event-1"}], "nextSyncToken": "fresh-token"},
            {"items": []},
        ],
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
    assert service.list_calls[2]["singleEvents"] is True
    assert warehouse.state_rows[0]["sync_token"] == "fresh-token"


def test_runner_expanded_sync_writes_recurring_instances_and_deletes_stale(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)
    service = FakeCalendarService(
        [
            {"items": [], "nextSyncToken": "new-token"},
            {
                "items": [
                    {
                        "id": "series-1_20260402T130000Z",
                        "recurringEventId": "series-1",
                        "status": "confirmed",
                        "summary": "JEA / Hack Club Quarterly Check-In",
                        "start": {"dateTime": "2026-04-02T09:00:00-04:00"},
                        "end": {"dateTime": "2026-04-02T10:00:00-04:00"},
                    }
                ]
            },
        ]
    )
    warehouse = FakeCalendarWarehouse(
        state={
            ("zach@example.com", "primary"): {
                "sync_token": "old-token",
                "last_sync_type": "partial",
            }
        }
    )
    warehouse.active_recurring_event_ids = [
        "series-1_20260402T130000Z",
        "series-1_20260702T130000Z",
    ]

    runner = CalendarSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
        now=lambda: datetime(2026, 4, 29, tzinfo=UTC),
    )
    summaries = runner.sync_all()

    assert warehouse.events[0]["event_id"] == "series-1_20260402T130000Z"
    assert warehouse.events[0]["recurring_event_id"] == "series-1"
    assert warehouse.deleted_event_ids == ["series-1_20260702T130000Z"]
    assert summaries[0].events_written == 2
    assert summaries[0].deleted_events == 1
    assert summaries[0].expanded_events_written == 2
    assert summaries[0].expanded_deleted_events == 1
    assert summaries[0].expanded_window_start == datetime(2025, 4, 29, tzinfo=UTC)
    assert summaries[0].expanded_window_end == datetime(2027, 4, 29, tzinfo=UTC)


def test_runner_skips_expanded_sync_when_recent(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)
    service = FakeCalendarService(
        [
            {"items": [{"id": "event-1", "status": "confirmed"}], "nextSyncToken": "new-token"},
        ]
    )
    warehouse = FakeCalendarWarehouse(
        state={
            ("zach@example.com", "primary"): {
                "sync_token": "old-token",
                "last_sync_type": "partial",
                "expanded_synced_at": datetime(2026, 4, 29, 12, 30, tzinfo=UTC),
            }
        }
    )

    runner = CalendarSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
        now=lambda: datetime(2026, 4, 29, 13, 0, tzinfo=UTC),
    )
    summaries = runner.sync_all()

    assert len(service.list_calls) == 1
    assert summaries[0].events_written == 1
    assert summaries[0].expanded_events_written == 0
    assert warehouse.state_rows[0]["expanded_synced_at"] == datetime(2026, 4, 29, 12, 30, tzinfo=UTC)


def test_runner_forces_expanded_sync_on_recurring_change(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)
    service = FakeCalendarService(
        [
            {
                "items": [
                    {
                        "id": "series-1",
                        "status": "confirmed",
                        "recurrence": ["RRULE:FREQ=MONTHLY"],
                    }
                ],
                "nextSyncToken": "new-token",
            },
            {
                "items": [
                    {
                        "id": "series-1_20260402T130000Z",
                        "recurringEventId": "series-1",
                        "status": "confirmed",
                    }
                ]
            },
        ]
    )
    warehouse = FakeCalendarWarehouse(
        state={
            ("zach@example.com", "primary"): {
                "sync_token": "old-token",
                "last_sync_type": "partial",
                "expanded_synced_at": datetime(2026, 4, 29, 12, 30, tzinfo=UTC),
            }
        }
    )

    runner = CalendarSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
        now=lambda: datetime(2026, 4, 29, 13, 0, tzinfo=UTC),
    )
    summaries = runner.sync_all()

    assert len(service.list_calls) == 2
    assert service.list_calls[1]["singleEvents"] is True
    assert summaries[0].recurring_changes_seen is True
    assert summaries[0].expanded_events_written == 1
    assert warehouse.state_rows[0]["expanded_synced_at"] == datetime(2026, 4, 29, 13, 0, tzinfo=UTC)


def test_runner_forces_expanded_sync_on_cancelled_event(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)
    service = FakeCalendarService(
        [
            {
                "items": [{"id": "maybe-series-1", "status": "cancelled"}],
                "nextSyncToken": "new-token",
            },
            {"items": []},
        ]
    )
    warehouse = FakeCalendarWarehouse(
        state={
            ("zach@example.com", "primary"): {
                "sync_token": "old-token",
                "last_sync_type": "partial",
                "expanded_synced_at": datetime(2026, 4, 29, 12, 30, tzinfo=UTC),
            }
        }
    )

    runner = CalendarSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
        now=lambda: datetime(2026, 4, 29, 13, 0, tzinfo=UTC),
    )
    summaries = runner.sync_all()

    assert len(service.list_calls) == 2
    assert service.list_calls[1]["singleEvents"] is True
    assert summaries[0].recurring_changes_seen is True


def test_runner_preserves_started_event_when_google_moves_it(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)
    service = FakeCalendarService(
        [
            {
                "items": [
                    {
                        "id": "event-1",
                        "status": "confirmed",
                        "summary": "Kennedy Space Center / Hack Club",
                        "start": {"dateTime": "2026-04-06T12:30:00-04:00"},
                        "end": {"dateTime": "2026-04-06T13:00:00-04:00"},
                    }
                ],
                "nextSyncToken": "new-token",
            }
        ]
    )
    warehouse = FakeCalendarWarehouse(
        state={
            ("zach@example.com", "primary"): {
                "sync_token": "old-token",
                "last_sync_type": "partial",
                "expanded_synced_at": datetime(2026, 3, 30, 16, 45, tzinfo=UTC),
            }
        }
    )
    warehouse.active_calendar_events_by_id["event-1"] = event_to_row(
        account="zach@example.com",
        calendar_id="primary",
        event={
            "id": "event-1",
            "status": "confirmed",
            "summary": "KSC VC / Hack Club",
            "start": {"dateTime": "2026-03-30T12:30:00-04:00"},
            "end": {"dateTime": "2026-03-30T13:00:00-04:00"},
        },
        synced_at=datetime(2026, 3, 26, tzinfo=UTC),
    )

    runner = CalendarSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
        now=lambda: datetime(2026, 3, 30, 16, 59, tzinfo=UTC),
    )
    summaries = runner.sync_all()

    historical_row = warehouse.events[0]
    updated_row = warehouse.events[1]
    assert historical_row["event_id"] == "event-1_20260330T163000Z_historical"
    assert historical_row["summary"] == "KSC VC / Hack Club"
    assert historical_row["start_at"] == datetime(2026, 3, 30, 16, 30, tzinfo=UTC)
    assert json.loads(historical_row["raw_json"])["pdw_original_event_id"] == "event-1"
    assert updated_row["event_id"] == "event-1"
    assert updated_row["start_at"] == datetime(2026, 4, 6, 16, 30, tzinfo=UTC)
    assert summaries[0].events_written == 2


def test_runner_does_not_preserve_future_event_when_google_moves_it(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)
    service = FakeCalendarService(
        [
            {
                "items": [
                    {
                        "id": "event-1",
                        "status": "confirmed",
                        "summary": "Planning",
                        "start": {"dateTime": "2026-05-08T12:30:00-04:00"},
                        "end": {"dateTime": "2026-05-08T13:00:00-04:00"},
                    }
                ],
                "nextSyncToken": "new-token",
            }
        ]
    )
    warehouse = FakeCalendarWarehouse(
        state={
            ("zach@example.com", "primary"): {
                "sync_token": "old-token",
                "last_sync_type": "partial",
                "expanded_synced_at": datetime(2026, 4, 30, 12, 30, tzinfo=UTC),
            }
        }
    )
    warehouse.active_calendar_events_by_id["event-1"] = event_to_row(
        account="zach@example.com",
        calendar_id="primary",
        event={
            "id": "event-1",
            "status": "confirmed",
            "summary": "Planning",
            "start": {"dateTime": "2026-05-01T12:30:00-04:00"},
            "end": {"dateTime": "2026-05-01T13:00:00-04:00"},
        },
        synced_at=datetime(2026, 4, 29, tzinfo=UTC),
    )

    runner = CalendarSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
        now=lambda: datetime(2026, 4, 30, 13, 0, tzinfo=UTC),
    )
    summaries = runner.sync_all()

    assert [row["event_id"] for row in warehouse.events] == ["event-1"]
    assert warehouse.events[0]["start_at"] == datetime(2026, 5, 8, 16, 30, tzinfo=UTC)
    assert summaries[0].events_written == 1


def test_runner_keeps_started_recurring_instance_when_google_late_cancels_it(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)
    service = FakeCalendarService(
        [
            {"items": [], "nextSyncToken": "new-token"},
            {
                "items": [
                    {
                        "id": "series-1_20260428T193000Z",
                        "recurringEventId": "series-1",
                        "status": "cancelled",
                        "summary": "Stardance Challenge Activation",
                        "start": {"dateTime": "2026-04-28T14:30:00-05:00"},
                        "end": {"dateTime": "2026-04-28T15:30:00-05:00"},
                    }
                ]
            },
        ]
    )
    warehouse = FakeCalendarWarehouse()
    warehouse.active_recurring_event_rows = [
        {
            "event_id": "series-1_20260428T193000Z",
            "start_at": datetime(2026, 4, 28, 19, 30, tzinfo=UTC),
            "end_at": datetime(2026, 4, 28, 20, 30, tzinfo=UTC),
        }
    ]

    runner = CalendarSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
        now=lambda: datetime(2026, 4, 30, 13, 0, tzinfo=UTC),
    )
    summaries = runner.sync_all()

    assert warehouse.events == []
    assert warehouse.deleted_event_ids == []
    assert summaries[0].expanded_events_written == 0
    assert summaries[0].expanded_deleted_events == 0


def test_runner_only_deletes_future_stale_recurring_instances(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_calendar=True)
    service = FakeCalendarService(
        [
            {"items": [], "nextSyncToken": "new-token"},
            {"items": []},
        ]
    )
    warehouse = FakeCalendarWarehouse()
    warehouse.active_recurring_event_rows = [
        {
            "event_id": "series-1_20260428T193000Z",
            "start_at": datetime(2026, 4, 28, 19, 30, tzinfo=UTC),
            "end_at": datetime(2026, 4, 28, 20, 30, tzinfo=UTC),
        },
        {
            "event_id": "series-1_20260505T193000Z",
            "start_at": datetime(2026, 5, 5, 19, 30, tzinfo=UTC),
            "end_at": datetime(2026, 5, 5, 20, 30, tzinfo=UTC),
        },
    ]

    runner = CalendarSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
        now=lambda: datetime(2026, 4, 30, 13, 0, tzinfo=UTC),
    )
    summaries = runner.sync_all()

    assert warehouse.deleted_event_ids == ["series-1_20260505T193000Z"]
    assert summaries[0].expanded_events_written == 1
    assert summaries[0].expanded_deleted_events == 1


def test_calendar_api_datetime_normalizes_utc() -> None:
    assert calendar_api_datetime(datetime(2026, 4, 2, 9)) == "2026-04-02T09:00:00Z"


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


def test_clickhouse_load_active_calendar_events_by_id_returns_rows_by_event_id() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    existing_row = {
        "account": "zach@example.com",
        "calendar_id": "primary",
        "event_id": "event-1",
        "recurring_event_id": "",
        "i_cal_uid": "uid-1",
        "status": "confirmed",
        "is_deleted": 0,
        "summary": "Planning",
        "description": "",
        "location": "",
        "creator_email": "",
        "organizer_email": "",
        "start_at": datetime(2026, 4, 1, tzinfo=UTC),
        "end_at": datetime(2026, 4, 1, 1, tzinfo=UTC),
        "start_date": "",
        "end_date": "",
        "is_all_day": 0,
        "html_link": "",
        "attendees_json": "[]",
        "reminders_json": "{}",
        "recurrence": [],
        "event_type": "",
        "raw_json": "{}",
        "updated_at": datetime(2026, 3, 1, tzinfo=UTC),
        "synced_at": datetime(2026, 3, 1, tzinfo=UTC),
        "sync_version": 1,
    }
    queries = []

    def fake_query(sql):
        queries.append(sql)
        return [tuple(existing_row[column] for column in CALENDAR_EVENT_COLUMNS)]

    warehouse._query = fake_query

    rows = warehouse.load_active_calendar_events_by_id(
        account="zach@example.com",
        calendar_id="primary",
        event_ids=["event-1", "event-2"],
    )

    assert rows["event-1"]["summary"] == "Planning"
    assert "has(['event-1', 'event-2'], event_id)" in queries[0]
    assert "AND is_deleted = 0" in queries[0]


def test_clickhouse_ensure_calendar_tables_migrates_expanded_sync_state_columns() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    commands = []

    def fake_command(sql):
        commands.append(sql)

    warehouse._command = fake_command
    warehouse._drop_obsolete_views = lambda: None

    warehouse.ensure_calendar_tables()

    joined = "\n".join(commands)
    assert "expanded_synced_at DateTime64(3, 'UTC')" in joined
    assert "expanded_window_start DateTime64(3, 'UTC')" in joined
    assert "expanded_window_end DateTime64(3, 'UTC')" in joined
    assert "ALTER TABLE calendar_sync_state ADD COLUMN IF NOT EXISTS expanded_synced_at" in joined
    assert "ALTER TABLE calendar_sync_state ADD COLUMN IF NOT EXISTS expanded_window_start" in joined
    assert "ALTER TABLE calendar_sync_state ADD COLUMN IF NOT EXISTS expanded_window_end" in joined


def test_clickhouse_sync_state_round_trips_expanded_sync_state() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    inserted = []
    expanded_synced_at = datetime(2026, 4, 29, 13, tzinfo=UTC)
    expanded_window_start = datetime(2025, 4, 29, 13, tzinfo=UTC)
    expanded_window_end = datetime(2027, 4, 29, 13, tzinfo=UTC)
    updated_at = datetime(2026, 4, 29, 13, 1, tzinfo=UTC)

    def fake_insert(table, rows, columns):
        inserted.append((table, rows, columns))

    warehouse._insert = fake_insert

    warehouse.insert_calendar_sync_state(
        account="zach@example.com",
        calendar_id="primary",
        sync_token="sync-token",
        last_sync_type="partial",
        status="ok",
        error="",
        expanded_synced_at=expanded_synced_at,
        expanded_window_start=expanded_window_start,
        expanded_window_end=expanded_window_end,
        updated_at=updated_at,
    )

    assert inserted[0][0] == "calendar_sync_state"
    assert inserted[0][2] == CALENDAR_SYNC_STATE_COLUMNS
    assert inserted[0][1][0] == (
        "zach@example.com",
        "primary",
        "sync-token",
        "partial",
        "ok",
        "",
        expanded_synced_at,
        expanded_window_start,
        expanded_window_end,
        updated_at,
        int(updated_at.timestamp() * 1_000_000),
    )

    def fake_query(sql):
        return [
            (
                "zach@example.com",
                "primary",
                "sync-token",
                "partial",
                "ok",
                "",
                expanded_synced_at,
                expanded_window_start,
                expanded_window_end,
                updated_at,
            )
        ]

    warehouse._query = fake_query

    state = warehouse.load_calendar_sync_state()

    assert state[("zach@example.com", "primary")]["expanded_synced_at"] == expanded_synced_at
    assert state[("zach@example.com", "primary")]["expanded_window_start"] == expanded_window_start
    assert state[("zach@example.com", "primary")]["expanded_window_end"] == expanded_window_end


def test_clickhouse_mark_calendar_events_deleted_writes_tombstones() -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    existing_row = {
        "account": "zach@example.com",
        "calendar_id": "primary",
        "event_id": "event-1",
        "recurring_event_id": "series-1",
        "i_cal_uid": "uid-1",
        "status": "confirmed",
        "is_deleted": 0,
        "summary": "Recurring meeting",
        "description": "",
        "location": "",
        "creator_email": "",
        "organizer_email": "",
        "start_at": datetime(2026, 4, 1, tzinfo=UTC),
        "end_at": datetime(2026, 4, 1, 1, tzinfo=UTC),
        "start_date": "",
        "end_date": "",
        "is_all_day": 0,
        "html_link": "",
        "attendees_json": "[]",
        "reminders_json": "{}",
        "recurrence": [],
        "event_type": "",
        "raw_json": "{}",
        "updated_at": datetime(2026, 3, 1, tzinfo=UTC),
        "synced_at": datetime(2026, 3, 1, tzinfo=UTC),
        "sync_version": 1,
    }
    inserted_rows = []

    def fake_query(sql):
        return [tuple(existing_row[column] for column in CALENDAR_EVENT_COLUMNS)]

    def fake_insert_calendar_events(rows):
        inserted_rows.extend(rows)

    warehouse._query = fake_query
    warehouse.insert_calendar_events = fake_insert_calendar_events
    synced_at = datetime(2026, 4, 29, tzinfo=UTC)

    count = warehouse.mark_calendar_events_deleted(
        account="zach@example.com",
        calendar_id="primary",
        event_ids=["event-1"],
        synced_at=synced_at,
    )

    assert count == 1
    assert inserted_rows[0]["event_id"] == "event-1"
    assert inserted_rows[0]["status"] == "cancelled"
    assert inserted_rows[0]["is_deleted"] == 1
    assert inserted_rows[0]["synced_at"] == synced_at
    assert inserted_rows[0]["sync_version"] == int(synced_at.timestamp() * 1_000_000)


def test_clickhouse_retries_disconnect_before_retry(monkeypatch) -> None:
    warehouse = object.__new__(ClickHouseWarehouse)
    calls = []
    monkeypatch.setattr("personal_data_warehouse.clickhouse.time.sleep", lambda seconds: calls.append(f"sleep:{seconds}"))

    class FakeClient:
        def disconnect(self):
            calls.append("disconnect")

    warehouse._client = FakeClient()

    def flaky_operation():
        calls.append("operation")
        if calls.count("operation") == 1:
            raise TimeoutError("stale connection")
        return "ok"

    assert warehouse._with_clickhouse_retries(flaky_operation) == "ok"
    assert calls == ["operation", "disconnect", "sleep:5", "operation"]
