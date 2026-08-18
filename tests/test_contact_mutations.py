from __future__ import annotations

import json
from pathlib import Path

import pytest

from personal_data_warehouse import contact_mutations
from personal_data_warehouse.contact_mutations import (
    GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
    GoogleContactMutationExecutor,
    contact_mutation_failure_status,
)


class FakeRequest:
    def __init__(self, response=None, error: Exception | None = None) -> None:
        self._response = response or {}
        self._error = error

    def execute(self):
        if self._error:
            raise self._error
        return self._response


class FakePeopleResource:
    def __init__(self, service) -> None:
        self.service = service

    def createContact(self, **kwargs):
        self.service.create_calls.append(kwargs)
        return FakeRequest({"resourceName": "people/new", "etag": "new-etag", **kwargs["body"]})

    def get(self, **kwargs):
        self.service.get_calls.append(kwargs)
        return FakeRequest(self.service.people_by_resource[kwargs["resourceName"]])

    def updateContact(self, **kwargs):
        self.service.update_calls.append(kwargs)
        return FakeRequest({"resourceName": kwargs["resourceName"], "etag": "updated-etag", **kwargs["body"]})

    def deleteContact(self, **kwargs):
        self.service.delete_calls.append(kwargs)
        return FakeRequest({})


class FakePeopleService:
    def __init__(self) -> None:
        self.people_by_resource = {
            "people/update": {"resourceName": "people/update", "etag": "etag-update"},
            "people/delete": {"resourceName": "people/delete", "etag": "etag-delete"},
        }
        self.create_calls = []
        self.get_calls = []
        self.update_calls = []
        self.delete_calls = []

    def people(self):
        return FakePeopleResource(self)


def test_contact_mutation_executor_runs_create_update_delete_sequentially(monkeypatch) -> None:
    monkeypatch.setattr(contact_mutations, "execute_contacts_request", lambda operation: operation())
    service = FakePeopleService()
    executor = GoogleContactMutationExecutor(settings=object(), service_factory=lambda account: service)

    result = executor.execute(
        {
            "provider": "google_people",
            "operation": GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "operations": [
                    {
                        "op": "create_contact",
                        "client_op_id": "create-1",
                        "person": {"names": [{"givenName": "New"}]},
                    },
                    {
                        "op": "update_contact",
                        "client_op_id": "update-1",
                        "resource_name": "people/update",
                        "expected_etag": "etag-update",
                        "update_person_fields": ["names", "emailAddresses"],
                        "person": {"resourceName": "people/update", "etag": "etag-update", "names": [{"givenName": "Edited"}]},
                    },
                    {
                        "op": "delete_contact",
                        "client_op_id": "delete-1",
                        "resource_name": "people/delete",
                        "expected_etag": "etag-delete",
                    },
                ]
            },
        }
    )

    assert result.status == "succeeded"
    assert [item["op"] for item in result.result_json["operation_results"]] == [
        "create_contact",
        "update_contact",
        "delete_contact",
    ]
    assert service.create_calls[0]["personFields"]
    assert service.get_calls == [
        {
            "resourceName": "people/update",
            "personFields": contact_mutations.CONTACT_PERSON_FIELDS,
            "sources": ["READ_SOURCE_TYPE_CONTACT"],
        },
        {
            "resourceName": "people/delete",
            "personFields": contact_mutations.CONTACT_PERSON_FIELDS,
            "sources": ["READ_SOURCE_TYPE_CONTACT"],
        },
    ]
    assert service.update_calls[0]["updatePersonFields"] == "names,emailAddresses"
    assert service.delete_calls == [{"resourceName": "people/delete"}]


def test_contact_mutation_executor_blocks_stale_etag(monkeypatch) -> None:
    monkeypatch.setattr(contact_mutations, "execute_contacts_request", lambda operation: operation())
    service = FakePeopleService()
    executor = GoogleContactMutationExecutor(settings=object(), service_factory=lambda account: service)

    result = executor.execute(
        {
            "provider": "google_people",
            "operation": GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "operations": [
                    {
                        "op": "delete_contact",
                        "client_op_id": "delete-1",
                        "resource_name": "people/delete",
                        "expected_etag": "old-etag",
                    },
                ]
            },
        }
    )

    assert result.status == "failed_terminal"
    assert "changed since proposal" in result.error
    assert service.delete_calls == []


def test_contact_mutation_failure_status_marks_network_retryable() -> None:
    assert contact_mutation_failure_status(ConnectionError("down")) == "failed_retryable"


def test_contact_mutation_failure_status_treats_refresh_error_as_blocked() -> None:
    from google.auth.exceptions import RefreshError

    err = RefreshError("invalid_scope: Bad Request", {"error": "invalid_scope"})
    assert contact_mutation_failure_status(err) == "blocked_missing_credentials"


def _execute_update(monkeypatch, operation: dict, *, service: FakePeopleService | None = None):
    monkeypatch.setattr(contact_mutations, "execute_contacts_request", lambda call: call())
    service = service or FakePeopleService()
    executor = GoogleContactMutationExecutor(settings=object(), service_factory=lambda account: service)
    result = executor.execute(
        {
            "provider": "google_people",
            "operation": GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
            "account": "zach@example.test",
            "payload_json": {"operations": [operation]},
        }
    )
    return result, service


# The People API refuses an update whose body carries no etag. The executor
# never set one, so every update_contact mutation this warehouse has ever
# proposed failed with HTTP 400 -- most recently the 2026-08-15 gap-year cohort
# request.
def test_update_contact_sends_the_live_etag_in_the_request_body(monkeypatch) -> None:
    result, service = _execute_update(
        monkeypatch,
        {
            "op": "update_contact",
            "resource_name": "people/update",
            "expected_etag": "etag-update",
            "update_person_fields": ["names"],
            "person": {"names": [{"givenName": "Edited"}]},
        },
    )

    assert result.status == "succeeded", result.error
    body = service.update_calls[0]["body"]
    assert body["etag"] == "etag-update"
    assert body["resourceName"] == "people/update"
    assert service.update_calls[0]["updatePersonFields"] == "names"


# The Go proposer writes `expected_etag`, but agent-authored payloads (and the
# already-stored failed one) carry a bare `etag`. Reading only the first is how
# the etag got dropped on the way to Google.
def test_update_contact_accepts_a_bare_etag_key(monkeypatch) -> None:
    result, service = _execute_update(
        monkeypatch,
        {
            "op": "update_contact",
            "resource_name": "people/update",
            "etag": "etag-update",
            "update_person_fields": ["names"],
            "person": {"names": [{"givenName": "Edited"}]},
        },
    )

    assert result.status == "succeeded", result.error
    assert service.update_calls[0]["body"]["etag"] == "etag-update"


def test_update_contact_uses_person_etag_when_no_operation_etag(monkeypatch) -> None:
    result, service = _execute_update(
        monkeypatch,
        {
            "op": "update_contact",
            "resource_name": "people/update",
            "update_person_fields": ["names"],
            "person": {"names": [{"givenName": "Edited"}], "etag": "etag-update"},
        },
    )

    assert result.status == "succeeded", result.error
    assert service.update_calls[0]["body"]["etag"] == "etag-update"


# An empty field mask is what produced `updatePersonFields=` in the failed
# request. Fail with a sentence a human can act on instead of forwarding a
# request Google can only reject.
def test_update_contact_without_field_mask_fails_before_calling_google(monkeypatch) -> None:
    result, service = _execute_update(
        monkeypatch,
        {
            "op": "update_contact",
            "resource_name": "people/update",
            "expected_etag": "etag-update",
            "person": {"names": [{"givenName": "Edited"}]},
        },
    )

    assert result.status == "failed_terminal"
    assert "update_person_fields" in result.error
    assert service.update_calls == []


def test_update_contact_without_person_fails_before_calling_google(monkeypatch) -> None:
    result, service = _execute_update(
        monkeypatch,
        {
            "op": "update_contact",
            "resource_name": "people/update",
            "expected_etag": "etag-update",
            "update_person_fields": ["names"],
        },
    )

    assert result.status == "failed_terminal"
    assert "person" in result.error
    assert service.update_calls == []


def test_update_contact_without_expected_etag_fails_before_calling_google(monkeypatch) -> None:
    result, service = _execute_update(
        monkeypatch,
        {
            "op": "update_contact",
            "resource_name": "people/update",
            "update_person_fields": ["names"],
            "person": {"names": [{"givenName": "Edited"}]},
        },
    )

    assert result.status == "failed_terminal"
    assert "etag" in result.error
    assert service.update_calls == []


def test_update_contact_refuses_a_stale_etag(monkeypatch) -> None:
    result, service = _execute_update(
        monkeypatch,
        {
            "op": "update_contact",
            "resource_name": "people/update",
            "expected_etag": "etag-from-a-week-ago",
            "update_person_fields": ["names"],
            "person": {"names": [{"givenName": "Edited"}]},
        },
    )

    assert result.status == "failed_terminal"
    assert "changed since proposal" in result.error
    assert service.update_calls == []


def test_create_contact_without_person_fails_before_calling_google(monkeypatch) -> None:
    monkeypatch.setattr(contact_mutations, "execute_contacts_request", lambda call: call())
    service = FakePeopleService()
    executor = GoogleContactMutationExecutor(settings=object(), service_factory=lambda account: service)

    result = executor.execute(
        {
            "provider": "google_people",
            "operation": GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
            "account": "zach@example.test",
            "payload_json": {"operations": [{"op": "create_contact"}]},
        }
    )

    assert result.status == "failed_terminal"
    assert "person" in result.error
    assert service.create_calls == []


def test_delete_contact_accepts_a_bare_etag_key(monkeypatch) -> None:
    monkeypatch.setattr(contact_mutations, "execute_contacts_request", lambda call: call())
    service = FakePeopleService()
    executor = GoogleContactMutationExecutor(settings=object(), service_factory=lambda account: service)

    result = executor.execute(
        {
            "provider": "google_people",
            "operation": GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
            "account": "zach@example.test",
            "payload_json": {"operations": [{"op": "delete_contact", "resource_name": "people/delete", "etag": "wrong-etag"}]},
        }
    )

    assert result.status == "failed_terminal"
    assert "changed since proposal" in result.error
    assert service.delete_calls == []


CONTRACT_PATH = Path(__file__).resolve().parent / "contracts" / "google_contacts_mutation_operations.json"


def _contract_cases() -> list[dict]:
    contract = json.loads(CONTRACT_PATH.read_text())
    assert contract["cases"], "contract has no cases"
    return contract["cases"]


# The Go proposer produces these operations and this executor consumes them.
# Nothing in either language forces the two to agree, so both test suites read
# this one file. They disagreed once, silently, for months.
@pytest.mark.parametrize("case", _contract_cases(), ids=lambda case: case["name"])
def test_executor_runs_every_operation_shape_the_proposer_emits(monkeypatch, case) -> None:
    operation = case["normalized"]
    resource_name = operation.get("resource_name", "")
    monkeypatch.setattr(contact_mutations, "execute_contacts_request", lambda call: call())
    service = FakePeopleService()
    if resource_name:
        service.people_by_resource[resource_name] = {
            "resourceName": resource_name,
            "etag": operation["expected_etag"],
        }
    executor = GoogleContactMutationExecutor(settings=object(), service_factory=lambda account: service)

    result = executor.execute(
        {
            "provider": "google_people",
            "operation": GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
            "account": "zach@example.test",
            "payload_json": {"operations": [operation]},
        }
    )

    assert result.status == "succeeded", result.error
    if operation["op"] == "update_contact":
        call = service.update_calls[0]
        assert call["updatePersonFields"] == ",".join(operation["update_person_fields"])
        assert call["body"]["etag"] == operation["expected_etag"]
        assert call["body"]["resourceName"] == resource_name
    elif operation["op"] == "create_contact":
        assert service.create_calls[0]["body"] == operation["person"]
    elif operation["op"] == "delete_contact":
        assert service.delete_calls == [{"resourceName": resource_name}]


def test_delete_contact_without_expected_etag_fails_before_calling_google(monkeypatch) -> None:
    monkeypatch.setattr(contact_mutations, "execute_contacts_request", lambda call: call())
    service = FakePeopleService()
    executor = GoogleContactMutationExecutor(settings=object(), service_factory=lambda account: service)

    result = executor.execute(
        {
            "provider": "google_people",
            "operation": GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION,
            "account": "zach@example.test",
            "payload_json": {"operations": [{"op": "delete_contact", "resource_name": "people/delete"}]},
        }
    )

    assert result.status == "failed_terminal"
    assert "etag" in result.error
    assert service.delete_calls == []
