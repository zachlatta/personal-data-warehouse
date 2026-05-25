from __future__ import annotations

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
