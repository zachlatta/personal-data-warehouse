from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import ssl
from typing import Any

from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from personal_data_warehouse.config import ContactGoogleAccount, Settings
from personal_data_warehouse.contacts_sync import CONTACT_PERSON_FIELDS, execute_contacts_request
from personal_data_warehouse.google_auth import load_google_credentials


GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION = "contacts.batch_mutation"


@dataclass(frozen=True)
class ContactMutationResult:
    status: str
    result_json: dict[str, Any]
    error: str = ""


class GoogleContactMutationExecutor:
    def __init__(
        self,
        *,
        settings: Settings,
        service_factory: Callable[[str], Any] | None = None,
    ) -> None:
        self._settings = settings
        self._service_factory = service_factory or (
            lambda account: build_google_people_mutation_service(account=account, settings=settings)
        )

    def execute(self, mutation: Mapping[str, Any]) -> ContactMutationResult:
        if mutation.get("provider") != "google_people" or mutation.get("operation") != GOOGLE_CONTACTS_BATCH_MUTATION_OPERATION:
            return ContactMutationResult(
                status="failed_terminal",
                result_json={},
                error=f"unsupported mutation operation: {mutation.get('provider')}.{mutation.get('operation')}",
            )
        account = str(mutation.get("account") or "")
        payload = _mapping(mutation.get("payload_json"))
        operations = _operations(payload.get("operations"))
        if not operations:
            return ContactMutationResult(status="failed_terminal", result_json={}, error="missing operations")

        operation_results: list[dict[str, Any]] = []
        try:
            service = self._service_factory(account)
            for index, operation in enumerate(operations):
                result = self._execute_operation(service=service, operation=operation)
                result["op_index"] = index
                operation_results.append(result)
            return ContactMutationResult(
                status="succeeded",
                result_json={"operation_results": operation_results},
            )
        except Exception as exc:
            return ContactMutationResult(
                status=contact_mutation_failure_status(exc),
                result_json={"operation_results": operation_results},
                error=str(exc),
            )

    def _execute_operation(self, *, service, operation: Mapping[str, Any]) -> dict[str, Any]:
        op = str(operation.get("op") or "")
        client_op_id = str(operation.get("client_op_id") or "")
        if op == "create_contact":
            response = execute_contacts_request(
                lambda: service.people()
                .createContact(body=_mapping(operation.get("person")), personFields=CONTACT_PERSON_FIELDS)
                .execute()
            )
            return {
                "op": op,
                "client_op_id": client_op_id,
                "resource_name": str(response.get("resourceName") or ""),
                "etag": str(response.get("etag") or ""),
                "response": response,
            }
        if op == "update_contact":
            resource_name = str(operation.get("resource_name") or "")
            expected_etag = str(operation.get("expected_etag") or "")
            live_person = self._live_contact(service=service, resource_name=resource_name)
            _raise_if_stale(resource_name=resource_name, expected_etag=expected_etag, live_person=live_person)
            fields = operation.get("update_person_fields") or []
            response = execute_contacts_request(
                lambda: service.people()
                .updateContact(
                    resourceName=resource_name,
                    updatePersonFields=",".join(str(field) for field in fields),
                    personFields=CONTACT_PERSON_FIELDS,
                    body=_mapping(operation.get("person")),
                )
                .execute()
            )
            return {
                "op": op,
                "client_op_id": client_op_id,
                "resource_name": resource_name,
                "etag": str(response.get("etag") or ""),
                "response": response,
            }
        if op == "delete_contact":
            resource_name = str(operation.get("resource_name") or "")
            expected_etag = str(operation.get("expected_etag") or "")
            live_person = self._live_contact(service=service, resource_name=resource_name)
            _raise_if_stale(resource_name=resource_name, expected_etag=expected_etag, live_person=live_person)
            response = execute_contacts_request(
                lambda: service.people().deleteContact(resourceName=resource_name).execute()
            )
            return {
                "op": op,
                "client_op_id": client_op_id,
                "resource_name": resource_name,
                "etag": expected_etag,
                "response": response or {},
            }
        raise ValueError(f"unsupported contact mutation op: {op}")

    def _live_contact(self, *, service, resource_name: str) -> dict[str, Any]:
        response = execute_contacts_request(
            lambda: service.people()
            .get(
                resourceName=resource_name,
                personFields=CONTACT_PERSON_FIELDS,
                sources=["READ_SOURCE_TYPE_CONTACT"],
            )
            .execute()
        )
        return dict(response)


def build_google_people_mutation_service(*, account: str, settings: Settings):
    credentials = load_google_credentials(
        email_address=settings.contact_account_for_email(account).email_address,
        settings=settings,
        scopes=settings.contact_mutation_scopes,
        service_name="Google Contacts mutation",
    )
    return build("people", "v1", credentials=credentials, cache_discovery=False)


def contact_mutation_failure_status(exc: Exception) -> str:
    if isinstance(exc, HttpError):
        status = getattr(exc.resp, "status", None)
        if status in {401, 403}:
            return "blocked_missing_credentials"
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


def _operations(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _raise_if_stale(*, resource_name: str, expected_etag: str, live_person: Mapping[str, Any]) -> None:
    if not expected_etag:
        return
    live_etag = str(live_person.get("etag") or "")
    if live_etag != expected_etag:
        raise ValueError(f"contact {resource_name} changed since proposal: expected etag {expected_etag}, got {live_etag}")
