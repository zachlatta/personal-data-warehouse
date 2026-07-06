from __future__ import annotations

from datetime import UTC, datetime
import json

import pytest
from googleapiclient.errors import HttpError
from httplib2 import Response

from personal_data_warehouse.config import CONTACTS_READONLY_SCOPE, load_settings
from personal_data_warehouse.contacts_sync import (
    ADDRESS_BOOK_ID,
    ContactsSyncRunner,
    CONTACT_PERSON_FIELDS,
    SOURCE,
    SOURCE_KIND,
    iter_google_contact_pages,
    person_to_contact_card_row,
    public_contacts_sync_summary,
)


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeContactsRequest:
    def __init__(self, service, response=None, error=None) -> None:
        self._service = service
        self._response = response
        self._error = error

    def execute(self):
        if self._error is not None:
            raise self._error
        return self._response


class FakeConnectionsResource:
    def __init__(self, service) -> None:
        self._service = service

    def list(self, **kwargs):
        self._service.list_calls.append(kwargs)
        if self._service.errors and len(self._service.list_calls) > self._service.errors_after_responses:
            return FakeContactsRequest(self._service, error=self._service.errors.pop(0))
        return FakeContactsRequest(self._service, response=self._service.responses.pop(0))


class FakePeopleResource:
    def __init__(self, service) -> None:
        self._service = service

    def connections(self):
        return FakeConnectionsResource(self._service)


class FakeContactsService:
    def __init__(self, responses, errors=None, errors_after_responses: int = 0) -> None:
        self.responses = list(responses)
        self.errors = list(errors or [])
        self.errors_after_responses = errors_after_responses
        self.list_calls = []

    def people(self):
        return FakePeopleResource(self)


class FakeContactsWarehouse:
    def __init__(self, state=None) -> None:
        self.state = state or {}
        self.cards = []
        self.state_rows = []
        self.ensure_contacts_tables_called = False
        self.missing_tombstone_calls = []
        self.missing_tombstone_count = 0

    def ensure_contacts_tables(self) -> None:
        self.ensure_contacts_tables_called = True

    def load_contact_sync_state(self):
        return self.state

    def insert_contact_cards(self, rows) -> None:
        self.cards.extend(rows)

    def insert_contact_sync_state(self, **row) -> None:
        self.state_rows.append(row)

    def mark_missing_contact_cards_deleted(self, **kwargs) -> int:
        self.missing_tombstone_calls.append(kwargs)
        return self.missing_tombstone_count


def google_person(resource_name: str = "people/c1") -> dict:
    return {
        "resourceName": resource_name,
        "etag": "etag-1",
        "metadata": {
            "sources": [
                {
                    "type": "CONTACT",
                    "id": "source-1",
                    "updateTime": "2026-05-20T12:30:00Z",
                }
            ]
        },
        "names": [
            {"displayName": "Backup Name", "givenName": "Backup"},
            {
                "displayName": "Alex Example",
                "givenName": "Alex",
                "familyName": "Example",
                "metadata": {"primary": True},
            },
        ],
        "emailAddresses": [
            {"value": "old@example.com"},
            {"value": "alex@example.com", "metadata": {"primary": True}},
        ],
        "phoneNumbers": [{"value": "(555) 0100", "canonicalForm": "+15550100", "metadata": {"primary": True}}],
        "organizations": [{"name": "Example Org", "title": "Founder", "metadata": {"primary": True}}],
        "biographies": [{"value": "met at HQ", "metadata": {"primary": True}}],
        "nicknames": [{"value": "Ace", "type": "DEFAULT"}],
        "memberships": [{"contactGroupMembership": {"contactGroupResourceName": "contactGroups/myContacts"}}],
        "birthdays": [{"date": {"month": 5, "day": 20}}],
        "events": [{"date": {"year": 2020, "month": 1, "day": 2}, "type": "anniversary"}],
        "photos": [{"url": "https://example.com/photo.jpg"}],
        "urls": [{"value": "https://example.com/profile"}],
    }


def test_load_settings_contacts_config_and_scope(monkeypatch) -> None:
    monkeypatch.setenv("CONTACT_GOOGLE_ACCOUNTS", "work@example.com,personal@example.com")
    monkeypatch.setenv("CONTACT_PAGE_SIZE", "500")

    settings = load_settings(require_postgres=False, require_gmail=False, require_contacts=True)

    assert [account.email_address for account in settings.contact_google_accounts] == [
        "work@example.com",
        "personal@example.com",
    ]
    assert settings.contact_page_size == 500
    assert CONTACTS_READONLY_SCOPE in settings.google_scopes


def test_person_to_contact_card_row_maps_queryable_fields_and_raw_json() -> None:
    person = google_person()

    row = person_to_contact_card_row(
        account="account@example.com",
        person=person,
        synced_at=datetime(2026, 5, 20, 13, tzinfo=UTC),
    )

    assert row["source"] == SOURCE
    assert row["source_kind"] == SOURCE_KIND
    assert row["address_book_id"] == ADDRESS_BOOK_ID
    assert row["card_id"] == "people/c1"
    assert row["source_uid"] == "source-1"
    assert row["display_name"] == "Alex Example"
    assert row["given_name"] == "Alex"
    assert row["family_name"] == "Example"
    assert row["organization"] == "Example Org"
    assert row["job_title"] == "Founder"
    assert row["primary_email"] == "alex@example.com"
    assert row["primary_phone"] == "+15550100"
    assert row["notes"] == "met at HQ"
    assert row["nicknames"] == person["nicknames"]
    assert row["groups"] == person["memberships"]
    assert row["dates"]["birthdays"] == person["birthdays"]
    assert row["raw_json"]["resourceName"] == "people/c1"
    assert row["source_updated_at"] == datetime(2026, 5, 20, 12, 30, tzinfo=UTC)


def test_deleted_person_maps_to_tombstone_row() -> None:
    row = person_to_contact_card_row(
        account="account@example.com",
        person={"resourceName": "people/deleted", "metadata": {"deleted": True}},
        synced_at=datetime(2026, 5, 20, 13, tzinfo=UTC),
    )

    assert row["is_deleted"] == 1
    assert row["card_id"] == "people/deleted"


def test_iter_google_contact_pages_requests_contacts_source_and_sync_token() -> None:
    service = FakeContactsService(
        [
            {"connections": [{"resourceName": "people/1"}], "nextPageToken": "page-2"},
            {"connections": [{"resourceName": "people/2"}], "nextSyncToken": "sync-token"},
        ]
    )

    pages = list(iter_google_contact_pages(service=service, page_size=1000))

    assert pages == [([{"resourceName": "people/1"}], None), ([{"resourceName": "people/2"}], "sync-token")]
    assert service.list_calls[0]["resourceName"] == "people/me"
    assert service.list_calls[0]["requestSyncToken"] is True
    assert service.list_calls[0]["sources"] == ["READ_SOURCE_TYPE_CONTACT"]
    assert service.list_calls[0]["personFields"] == CONTACT_PERSON_FIELDS
    assert service.list_calls[1]["pageToken"] == "page-2"


def test_runner_full_sync_writes_cards_tombstones_and_state(monkeypatch) -> None:
    monkeypatch.setenv("CONTACT_GOOGLE_ACCOUNTS", "account@example.com")
    settings = load_settings(require_postgres=False, require_gmail=False, require_contacts=True)
    service = FakeContactsService([{"connections": [google_person("people/c1")], "nextSyncToken": "next"}])
    warehouse = FakeContactsWarehouse()
    warehouse.missing_tombstone_count = 1

    summaries = ContactsSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
        now=lambda: datetime(2026, 5, 20, 13, tzinfo=UTC),
    ).sync_all()

    assert warehouse.ensure_contacts_tables_called
    assert summaries[0].sync_type == "full"
    assert summaries[0].cards_written == 1
    assert summaries[0].deleted_cards == 1
    assert summaries[0].tombstones_written == 1
    assert warehouse.cards[0]["card_id"] == "people/c1"
    assert warehouse.missing_tombstone_calls[0]["active_card_ids"] == {"people/c1"}
    assert warehouse.state_rows[0]["sync_token"] == "next"
    assert warehouse.state_rows[0]["status"] == "ok"


def test_runner_incremental_sync_uses_existing_sync_token(monkeypatch) -> None:
    monkeypatch.setenv("CONTACT_GOOGLE_ACCOUNTS", "account@example.com")
    settings = load_settings(require_postgres=False, require_gmail=False, require_contacts=True)
    state = {
        (SOURCE, "account@example.com", SOURCE_KIND, ADDRESS_BOOK_ID): {
            "sync_token": "old-token",
            "last_sync_type": "full",
            "full_synced_at": datetime(2026, 5, 19, 12, tzinfo=UTC),
        }
    }
    service = FakeContactsService([{"connections": [google_person("people/c2")], "nextSyncToken": "new-token"}])
    warehouse = FakeContactsWarehouse(state=state)

    summaries = ContactsSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
        now=lambda: datetime(2026, 5, 20, 13, tzinfo=UTC),
    ).sync_all()

    assert summaries[0].sync_type == "partial"
    assert service.list_calls[0]["syncToken"] == "old-token"
    assert warehouse.missing_tombstone_calls == []
    assert warehouse.state_rows[0]["sync_token"] == "new-token"
    assert warehouse.state_rows[0]["full_synced_at"] == datetime(2026, 5, 19, 12, tzinfo=UTC)


def test_runner_expired_sync_token_falls_back_to_full(monkeypatch) -> None:
    monkeypatch.setenv("CONTACT_GOOGLE_ACCOUNTS", "account@example.com")
    settings = load_settings(require_postgres=False, require_gmail=False, require_contacts=True)
    state = {
        (SOURCE, "account@example.com", SOURCE_KIND, ADDRESS_BOOK_ID): {
            "sync_token": "old-token",
            "last_sync_type": "partial",
            "full_synced_at": datetime(2026, 5, 19, 12, tzinfo=UTC),
        }
    }
    error = HttpError(Response({"status": "400"}), b"Sync token is expired")
    service = FakeContactsService(
        [{"connections": [google_person("people/c1")], "nextSyncToken": "fresh"}],
        errors=[error],
    )
    warehouse = FakeContactsWarehouse(state=state)

    summaries = ContactsSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
        now=lambda: datetime(2026, 5, 20, 13, tzinfo=UTC),
    ).sync_all()

    assert summaries[0].sync_type == "full"
    assert service.list_calls[0]["syncToken"] == "old-token"
    assert "syncToken" not in service.list_calls[1]
    assert warehouse.state_rows[0]["sync_token"] == "fresh"


def test_runner_failure_preserves_previous_state_without_tombstoning(monkeypatch) -> None:
    monkeypatch.setenv("CONTACT_GOOGLE_ACCOUNTS", "account@example.com")
    settings = load_settings(require_postgres=False, require_gmail=False, require_contacts=True)
    state = {
        (SOURCE, "account@example.com", SOURCE_KIND, ADDRESS_BOOK_ID): {
            "sync_token": "old-token",
            "last_sync_type": "partial",
            "full_synced_at": datetime(2026, 5, 19, 12, tzinfo=UTC),
        }
    }
    service = FakeContactsService([], errors=[HttpError(Response({"status": "403"}), b"denied")])
    warehouse = FakeContactsWarehouse(state=state)

    with pytest.raises(RuntimeError, match="Contacts sync failed"):
        ContactsSyncRunner(
            settings=settings,
            warehouse=warehouse,
            logger=FakeLogger(),
            service_factory=lambda account: service,
            now=lambda: datetime(2026, 5, 20, 13, tzinfo=UTC),
        ).sync_all()

    assert warehouse.cards == []
    assert warehouse.missing_tombstone_calls == []
    assert warehouse.state_rows[0]["status"] == "failed"
    assert warehouse.state_rows[0]["sync_token"] == "old-token"


def test_runner_failure_after_page_does_not_write_partial_cards(monkeypatch) -> None:
    monkeypatch.setenv("CONTACT_GOOGLE_ACCOUNTS", "account@example.com")
    settings = load_settings(require_postgres=False, require_gmail=False, require_contacts=True)
    service = FakeContactsService(
        [{"connections": [google_person("people/c1")], "nextPageToken": "page-2"}],
        errors=[HttpError(Response({"status": "403"}), b"denied")],
        errors_after_responses=1,
    )
    warehouse = FakeContactsWarehouse()

    with pytest.raises(RuntimeError, match="Contacts sync failed"):
        ContactsSyncRunner(
            settings=settings,
            warehouse=warehouse,
            logger=FakeLogger(),
            service_factory=lambda account: service,
            now=lambda: datetime(2026, 5, 20, 13, tzinfo=UTC),
        ).sync_all()

    assert warehouse.cards == []
    assert warehouse.missing_tombstone_calls == []
    assert warehouse.state_rows[0]["status"] == "failed"


def test_contact_rows_are_json_serializable_for_asset_metadata() -> None:
    row = person_to_contact_card_row(
        account="account@example.com",
        person=google_person(),
        synced_at=datetime(2026, 5, 20, 13, tzinfo=UTC),
    )

    assert json.loads(json.dumps(row["raw_json"]))["resourceName"] == "people/c1"


def test_public_contacts_sync_summary_does_not_expose_sync_token(monkeypatch) -> None:
    monkeypatch.setenv("CONTACT_GOOGLE_ACCOUNTS", "account@example.com")
    settings = load_settings(require_postgres=False, require_gmail=False, require_contacts=True)
    service = FakeContactsService([{"connections": [], "nextSyncToken": "sync-token-value"}])
    warehouse = FakeContactsWarehouse()

    summaries = ContactsSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        service_factory=lambda account: service,
        now=lambda: datetime(2026, 5, 20, 13, tzinfo=UTC),
    ).sync_all()

    public_summary = public_contacts_sync_summary(summaries[0])

    assert public_summary["has_next_sync_token"] is True
    assert "next_sync_token" not in public_summary
    assert "sync-token-value" not in json.dumps(public_summary)
