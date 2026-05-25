from __future__ import annotations

from personal_data_warehouse import calendar_mutations
from personal_data_warehouse.calendar_mutations import (
    CALENDAR_CREATE_EVENT_OPERATION,
    CALENDAR_DELETE_EVENT_OPERATION,
    CALENDAR_UPDATE_EVENT_OPERATION,
    CalendarMutationExecutor,
    calendar_mutation_failure_status,
)


class FakeRequest:
    def __init__(self, response=None, error: Exception | None = None) -> None:
        self._response = response if response is not None else {}
        self._error = error

    def execute(self):
        if self._error is not None:
            raise self._error
        return self._response


class FakeEventsResource:
    def __init__(self, service) -> None:
        self.service = service

    def insert(self, **kwargs):
        self.service.insert_calls.append(kwargs)
        body = dict(kwargs.get("body") or {})
        body.setdefault("id", "event-new-1")
        body.setdefault("etag", '"created-etag"')
        body.setdefault("htmlLink", "https://calendar.google.com/event?eid=event-new-1")
        return FakeRequest(body)

    def get(self, **kwargs):
        self.service.get_calls.append(kwargs)
        event_id = kwargs["eventId"]
        if event_id not in self.service.events_by_id:
            raise KeyError(f"unknown event {event_id}")
        return FakeRequest(dict(self.service.events_by_id[event_id]))

    def patch(self, **kwargs):
        self.service.patch_calls.append(kwargs)
        event_id = kwargs["eventId"]
        existing = dict(self.service.events_by_id.get(event_id, {}))
        existing.update(kwargs.get("body") or {})
        existing["etag"] = '"updated-etag"'
        existing["id"] = event_id
        self.service.events_by_id[event_id] = existing
        return FakeRequest(existing)

    def delete(self, **kwargs):
        self.service.delete_calls.append(kwargs)
        return FakeRequest("")


class FakeCalendarService:
    def __init__(self, events: dict[str, dict] | None = None) -> None:
        self.events_by_id = dict(events or {})
        self.insert_calls: list[dict] = []
        self.get_calls: list[dict] = []
        self.patch_calls: list[dict] = []
        self.delete_calls: list[dict] = []

    def events(self):
        return FakeEventsResource(self)


def _executor(service: FakeCalendarService) -> CalendarMutationExecutor:
    return CalendarMutationExecutor(
        settings=object(),
        service_factory=lambda account: service,
    )


def test_create_event_executor_inserts_event_with_attendees(monkeypatch) -> None:
    monkeypatch.setattr(calendar_mutations, "execute_calendar_request", lambda fn: fn())
    service = FakeCalendarService()
    executor = _executor(service)

    result = executor.execute(
        {
            "provider": "google_calendar",
            "operation": CALENDAR_CREATE_EVENT_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "calendar_id": "primary",
                "send_updates": "all",
                "event": {
                    "summary": "PDW calendar mutation test",
                    "description": "Created by automated test",
                    "start": {"dateTime": "2030-01-01T10:00:00", "timeZone": "America/Los_Angeles"},
                    "end": {"dateTime": "2030-01-01T10:30:00", "timeZone": "America/Los_Angeles"},
                    "attendees": [
                        {"email": "one@example.test"},
                        {"email": "two@example.test"},
                    ],
                },
            },
        }
    )

    assert result.status == "succeeded"
    assert result.result_json["event_id"] == "event-new-1"
    assert result.result_json["etag"] == '"created-etag"'
    assert result.result_json["html_link"] == "https://calendar.google.com/event?eid=event-new-1"
    assert len(service.insert_calls) == 1
    call = service.insert_calls[0]
    assert call["calendarId"] == "primary"
    assert call["sendUpdates"] == "all"
    body = call["body"]
    assert body["summary"] == "PDW calendar mutation test"
    assert body["start"] == {"dateTime": "2030-01-01T10:00:00", "timeZone": "America/Los_Angeles"}
    assert body["attendees"] == [
        {"email": "one@example.test"},
        {"email": "two@example.test"},
    ]


def test_create_event_executor_supports_recurrence(monkeypatch) -> None:
    monkeypatch.setattr(calendar_mutations, "execute_calendar_request", lambda fn: fn())
    service = FakeCalendarService()
    executor = _executor(service)

    result = executor.execute(
        {
            "provider": "google_calendar",
            "operation": CALENDAR_CREATE_EVENT_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "calendar_id": "primary",
                "event": {
                    "summary": "Weekly sync",
                    "start": {"dateTime": "2030-01-01T10:00:00", "timeZone": "UTC"},
                    "end": {"dateTime": "2030-01-01T10:30:00", "timeZone": "UTC"},
                    "recurrence": ["RRULE:FREQ=WEEKLY;COUNT=4"],
                },
            },
        }
    )

    assert result.status == "succeeded"
    assert service.insert_calls[0]["body"]["recurrence"] == ["RRULE:FREQ=WEEKLY;COUNT=4"]


def test_update_event_executor_patches_only_changed_fields(monkeypatch) -> None:
    monkeypatch.setattr(calendar_mutations, "execute_calendar_request", lambda fn: fn())
    service = FakeCalendarService(
        events={
            "event-abc": {
                "id": "event-abc",
                "etag": '"current-etag"',
                "summary": "Old summary",
                "start": {"dateTime": "2030-01-01T10:00:00Z"},
                "end": {"dateTime": "2030-01-01T10:30:00Z"},
            }
        }
    )
    executor = _executor(service)

    result = executor.execute(
        {
            "provider": "google_calendar",
            "operation": CALENDAR_UPDATE_EVENT_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "calendar_id": "primary",
                "event_id": "event-abc",
                "expected_etag": '"current-etag"',
                "send_updates": "all",
                "patch": {"summary": "New summary"},
            },
        }
    )

    assert result.status == "succeeded"
    assert result.result_json["event_id"] == "event-abc"
    assert result.result_json["etag"] == '"updated-etag"'
    assert service.get_calls == [{"calendarId": "primary", "eventId": "event-abc"}]
    patch_call = service.patch_calls[0]
    assert patch_call["calendarId"] == "primary"
    assert patch_call["eventId"] == "event-abc"
    assert patch_call["sendUpdates"] == "all"
    assert patch_call["body"] == {"summary": "New summary"}


def test_update_event_executor_blocks_stale_etag(monkeypatch) -> None:
    monkeypatch.setattr(calendar_mutations, "execute_calendar_request", lambda fn: fn())
    service = FakeCalendarService(
        events={"event-abc": {"id": "event-abc", "etag": '"current-etag"'}}
    )
    executor = _executor(service)

    result = executor.execute(
        {
            "provider": "google_calendar",
            "operation": CALENDAR_UPDATE_EVENT_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "calendar_id": "primary",
                "event_id": "event-abc",
                "expected_etag": '"stale-etag"',
                "patch": {"summary": "New"},
            },
        }
    )

    assert result.status == "failed_terminal"
    assert "changed since proposal" in result.error
    assert service.patch_calls == []


def test_delete_event_executor_deletes_with_expected_etag(monkeypatch) -> None:
    monkeypatch.setattr(calendar_mutations, "execute_calendar_request", lambda fn: fn())
    service = FakeCalendarService(
        events={"event-abc": {"id": "event-abc", "etag": '"current-etag"'}}
    )
    executor = _executor(service)

    result = executor.execute(
        {
            "provider": "google_calendar",
            "operation": CALENDAR_DELETE_EVENT_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "calendar_id": "primary",
                "event_id": "event-abc",
                "expected_etag": '"current-etag"',
                "send_updates": "all",
            },
        }
    )

    assert result.status == "succeeded"
    assert result.result_json["event_id"] == "event-abc"
    assert service.delete_calls == [
        {"calendarId": "primary", "eventId": "event-abc", "sendUpdates": "all"}
    ]


def test_delete_event_executor_blocks_stale_etag(monkeypatch) -> None:
    monkeypatch.setattr(calendar_mutations, "execute_calendar_request", lambda fn: fn())
    service = FakeCalendarService(
        events={"event-abc": {"id": "event-abc", "etag": '"current-etag"'}}
    )
    executor = _executor(service)

    result = executor.execute(
        {
            "provider": "google_calendar",
            "operation": CALENDAR_DELETE_EVENT_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "calendar_id": "primary",
                "event_id": "event-abc",
                "expected_etag": '"stale-etag"',
            },
        }
    )

    assert result.status == "failed_terminal"
    assert "changed since proposal" in result.error
    assert service.delete_calls == []


def test_executor_rejects_unsupported_operation() -> None:
    service = FakeCalendarService()
    executor = _executor(service)

    result = executor.execute(
        {
            "provider": "google_calendar",
            "operation": "calendar.unsupported",
            "account": "zach@example.test",
            "payload_json": {},
        }
    )

    assert result.status == "failed_terminal"
    assert "unsupported" in result.error


def test_calendar_mutation_failure_status_maps_network_to_retryable() -> None:
    assert calendar_mutation_failure_status(ConnectionError("down")) == "failed_retryable"


def test_create_event_executor_accepts_external_only_send_updates(monkeypatch) -> None:
    monkeypatch.setattr(calendar_mutations, "execute_calendar_request", lambda fn: fn())
    service = FakeCalendarService()
    executor = _executor(service)

    result = executor.execute(
        {
            "provider": "google_calendar",
            "operation": CALENDAR_CREATE_EVENT_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "calendar_id": "primary",
                "send_updates": "externalOnly",
                "event": {
                    "summary": "External",
                    "start": {"dateTime": "2030-01-01T10:00:00", "timeZone": "UTC"},
                    "end": {"dateTime": "2030-01-01T10:30:00", "timeZone": "UTC"},
                },
            },
        }
    )

    assert result.status == "succeeded"
    assert service.insert_calls[0]["sendUpdates"] == "externalOnly"


def test_create_event_executor_rejects_invalid_send_updates(monkeypatch) -> None:
    monkeypatch.setattr(calendar_mutations, "execute_calendar_request", lambda fn: fn())
    service = FakeCalendarService()
    executor = _executor(service)

    result = executor.execute(
        {
            "provider": "google_calendar",
            "operation": CALENDAR_CREATE_EVENT_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "calendar_id": "primary",
                "send_updates": "yes-please",
                "event": {
                    "summary": "X",
                    "start": {"dateTime": "2030-01-01T10:00:00", "timeZone": "UTC"},
                    "end": {"dateTime": "2030-01-01T10:30:00", "timeZone": "UTC"},
                },
            },
        }
    )

    assert result.status == "failed_terminal"
    assert "send_updates" in result.error
    assert service.insert_calls == []


def test_update_event_executor_rejects_missing_event_id(monkeypatch) -> None:
    monkeypatch.setattr(calendar_mutations, "execute_calendar_request", lambda fn: fn())
    service = FakeCalendarService()
    executor = _executor(service)

    result = executor.execute(
        {
            "provider": "google_calendar",
            "operation": CALENDAR_UPDATE_EVENT_OPERATION,
            "account": "zach@example.test",
            "payload_json": {"calendar_id": "primary", "patch": {"summary": "x"}},
        }
    )

    assert result.status == "failed_terminal"
    assert "event_id" in result.error


def test_delete_event_executor_rejects_missing_event_id(monkeypatch) -> None:
    monkeypatch.setattr(calendar_mutations, "execute_calendar_request", lambda fn: fn())
    service = FakeCalendarService()
    executor = _executor(service)

    result = executor.execute(
        {
            "provider": "google_calendar",
            "operation": CALENDAR_DELETE_EVENT_OPERATION,
            "account": "zach@example.test",
            "payload_json": {"calendar_id": "primary"},
        }
    )

    assert result.status == "failed_terminal"
    assert "event_id" in result.error


def test_create_event_executor_rejects_empty_event_body(monkeypatch) -> None:
    monkeypatch.setattr(calendar_mutations, "execute_calendar_request", lambda fn: fn())
    service = FakeCalendarService()
    executor = _executor(service)

    result = executor.execute(
        {
            "provider": "google_calendar",
            "operation": CALENDAR_CREATE_EVENT_OPERATION,
            "account": "zach@example.test",
            "payload_json": {"calendar_id": "primary", "event": {}},
        }
    )

    assert result.status == "failed_terminal"
    assert "event" in result.error
    assert service.insert_calls == []


def test_calendar_mutation_failure_status_maps_missing_oauth_to_blocked() -> None:
    err = RuntimeError("OAuth token for zach@example.test cannot be refreshed")
    assert calendar_mutation_failure_status(err) == "blocked_missing_credentials"


def test_calendar_mutation_failure_status_treats_refresh_error_as_blocked() -> None:
    from google.auth.exceptions import RefreshError

    err = RefreshError("invalid_scope: Bad Request", {"error": "invalid_scope"})
    assert calendar_mutation_failure_status(err) == "blocked_missing_credentials"
