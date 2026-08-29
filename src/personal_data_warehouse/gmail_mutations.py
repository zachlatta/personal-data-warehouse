from __future__ import annotations

import base64
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from email.message import EmailMessage
import ssl
from typing import Any

from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from personal_data_warehouse.config import Settings
from personal_data_warehouse.gmail_sync import execute_gmail_request
from personal_data_warehouse.google_auth import load_google_credentials


GMAIL_ARCHIVE_OPERATION = "gmail.archive_threads"
GMAIL_UNARCHIVE_OPERATION = "gmail.unarchive_threads"
GMAIL_MODIFY_THREAD_LABELS_OPERATION = "gmail.modify_thread_labels"
GMAIL_SEND_EMAIL_OPERATION = "gmail.send_email"
GMAIL_BATCH_MODIFY_MESSAGE_LIMIT = 1000


@dataclass(frozen=True)
class GmailMutationResult:
    status: str
    result_json: dict[str, Any]
    error: str = ""


class GmailMutationExecutor:
    def __init__(
        self,
        *,
        settings: Settings,
        service_factory: Callable[[str], Any] | None = None,
    ) -> None:
        self._settings = settings
        self._service_factory = service_factory

    def execute(self, mutation: Mapping[str, Any]) -> GmailMutationResult:
        operation = str(mutation.get("operation") or "")
        if mutation.get("provider") != "gmail" or operation not in {
            GMAIL_ARCHIVE_OPERATION,
            GMAIL_UNARCHIVE_OPERATION,
            GMAIL_MODIFY_THREAD_LABELS_OPERATION,
            GMAIL_SEND_EMAIL_OPERATION,
        }:
            return GmailMutationResult(
                status="failed_terminal",
                result_json={},
                error=f"unsupported mutation operation: {mutation.get('provider')}.{mutation.get('operation')}",
            )
        if operation == GMAIL_SEND_EMAIL_OPERATION:
            return self._execute_send_email(mutation)

        account = str(mutation.get("account") or "")
        payload = _mapping(mutation.get("payload_json"))
        thread_ids = _thread_ids(payload.get("thread_ids"))
        if not thread_ids:
            return GmailMutationResult(status="failed_terminal", result_json={}, error="missing thread_ids")

        changed_thread_ids: list[str] = []
        progress_key = _thread_progress_key(operation)
        add_labels: list[str] = []
        create_and_add_labels: list[str] = []
        remove_labels: list[str] = []
        add_label_ids: list[str] = []
        remove_label_ids: list[str] = []
        created_labels: list[dict[str, str]] = []
        try:
            add_labels, create_and_add_labels, remove_labels = _gmail_label_changes(
                operation=operation,
                payload=payload,
            )
            service = self._service(account=account, operation=operation)
            add_label_ids, remove_label_ids, created_labels = _resolved_gmail_label_changes(
                service=service,
                operation=operation,
                add_labels=add_labels,
                create_and_add_labels=create_and_add_labels,
                remove_labels=remove_labels,
            )
            modify_body = _gmail_modify_body(
                add_label_ids=add_label_ids,
                remove_label_ids=remove_label_ids,
            )
            thread_results: list[dict[str, Any]] = []
            for thread_id in thread_ids:
                response = execute_gmail_request(
                    lambda thread_id=thread_id: service.users()
                    .threads()
                    .modify(
                        userId="me",
                        id=thread_id,
                        body=modify_body,
                    )
                    .execute()
                )
                changed_thread_ids.append(thread_id)
                thread_results.append({"thread_id": thread_id, "response": response})
            result_json: dict[str, Any] = {
                progress_key: changed_thread_ids,
                "thread_results": thread_results,
            }
            if operation == GMAIL_MODIFY_THREAD_LABELS_OPERATION:
                result_json.update(
                    {
                        "add_label_ids": add_label_ids,
                        "remove_label_ids": remove_label_ids,
                        "created_labels": created_labels,
                    }
                )
            return GmailMutationResult(status="succeeded", result_json=result_json)
        except Exception as exc:
            result_json = {progress_key: changed_thread_ids}
            if operation == GMAIL_MODIFY_THREAD_LABELS_OPERATION:
                result_json.update(
                    {
                        "add_label_ids": add_label_ids,
                        "remove_label_ids": remove_label_ids,
                        "created_labels": created_labels,
                    }
                )
            return GmailMutationResult(
                status=gmail_mutation_failure_status(exc),
                result_json=result_json,
                error=str(exc),
            )

    def execute_message_batch_modify(
        self,
        *,
        account: str,
        operation: str,
        message_ids: list[str],
        add_labels: list[str] | None = None,
        create_and_add_labels: list[str] | None = None,
        remove_labels: list[str] | None = None,
    ) -> GmailMutationResult:
        if operation not in {
            GMAIL_ARCHIVE_OPERATION,
            GMAIL_UNARCHIVE_OPERATION,
            GMAIL_MODIFY_THREAD_LABELS_OPERATION,
        }:
            return GmailMutationResult(
                status="failed_terminal",
                result_json={},
                error=f"unsupported Gmail batch modify operation: {operation}",
            )
        normalized_message_ids = _message_ids(message_ids)
        progress_key = _message_progress_key(operation)
        if not normalized_message_ids:
            return GmailMutationResult(status="succeeded", result_json={progress_key: []})

        requested_add_labels: list[str] = []
        requested_create_and_add_labels: list[str] = []
        requested_remove_labels: list[str] = []
        add_label_ids: list[str] = []
        remove_label_ids: list[str] = []
        created_labels: list[dict[str, str]] = []
        changed_message_ids: list[str] = []
        batch_responses: list[dict[str, Any]] = []
        try:
            (
                requested_add_labels,
                requested_create_and_add_labels,
                requested_remove_labels,
            ) = _gmail_label_changes(
                operation=operation,
                payload={
                    "add_labels": add_labels or [],
                    "create_and_add_labels": create_and_add_labels or [],
                    "remove_labels": remove_labels or [],
                },
            )
            service = self._service(account=account, operation=operation)
            add_label_ids, remove_label_ids, created_labels = _resolved_gmail_label_changes(
                service=service,
                operation=operation,
                add_labels=requested_add_labels,
                create_and_add_labels=requested_create_and_add_labels,
                remove_labels=requested_remove_labels,
            )
            modify_body = _gmail_modify_body(
                add_label_ids=add_label_ids,
                remove_label_ids=remove_label_ids,
            )
            for chunk in _chunks(normalized_message_ids, GMAIL_BATCH_MODIFY_MESSAGE_LIMIT):
                response = execute_gmail_request(
                    lambda chunk=chunk: service.users()
                    .messages()
                    .batchModify(
                        userId="me",
                        body={**modify_body, "ids": chunk},
                    )
                    .execute()
                )
                changed_message_ids.extend(chunk)
                batch_responses.append(response or {})
            result_json: dict[str, Any] = {
                progress_key: changed_message_ids,
                "batch_modified_message_ids": changed_message_ids,
                "batch_modify_responses": batch_responses,
            }
            if operation == GMAIL_MODIFY_THREAD_LABELS_OPERATION:
                result_json.update(
                    {
                        "add_label_ids": add_label_ids,
                        "remove_label_ids": remove_label_ids,
                        "created_labels": created_labels,
                    }
                )
            return GmailMutationResult(status="succeeded", result_json=result_json)
        except Exception as exc:
            result_json = {
                progress_key: changed_message_ids,
                "batch_modified_message_ids": changed_message_ids,
                "batch_modify_responses": batch_responses,
            }
            if operation == GMAIL_MODIFY_THREAD_LABELS_OPERATION:
                result_json.update(
                    {
                        "add_label_ids": add_label_ids,
                        "remove_label_ids": remove_label_ids,
                        "created_labels": created_labels,
                    }
                )
            return GmailMutationResult(
                status=gmail_mutation_failure_status(exc),
                result_json=result_json,
                error=str(exc),
            )

    def _execute_send_email(self, mutation: Mapping[str, Any]) -> GmailMutationResult:
        account = str(mutation.get("account") or "")
        payload = _mapping(mutation.get("payload_json"))
        message = _mapping(payload.get("message"))
        delivery_mode = _delivery_mode(payload.get("delivery_mode"))
        try:
            reply_to_thread_id = str(message.get("reply_to_thread_id") or "").strip()
            if reply_to_thread_id and not str(message.get("in_reply_to") or "").strip():
                return GmailMutationResult(
                    status="failed_terminal",
                    result_json={"delivery_mode": delivery_mode, "thread_id": reply_to_thread_id},
                    error="reply email is missing In-Reply-To metadata; recreate the mutation after Gmail thread enrichment is available",
                )
            message = _message_with_reply_references(message)
            raw = build_email_raw(account=account, message=message)
            gmail_message: dict[str, Any] = {"raw": raw}
            if reply_to_thread_id:
                gmail_message["threadId"] = reply_to_thread_id

            service = self._service(account=account, operation=GMAIL_SEND_EMAIL_OPERATION)
            if delivery_mode == "draft":
                response = execute_gmail_request(
                    lambda: service.users().drafts().create(userId="me", body={"message": gmail_message}).execute()
                )
                draft_message = _mapping(response.get("message"))
                return GmailMutationResult(
                    status="succeeded",
                    result_json={
                        "delivery_mode": "draft",
                        "draft_id": str(response.get("id") or ""),
                        "draft_message_id": str(draft_message.get("id") or ""),
                        "thread_id": str(draft_message.get("threadId") or reply_to_thread_id),
                        "response": response,
                    },
                )
            response = execute_gmail_request(
                lambda: service.users().messages().send(userId="me", body=gmail_message).execute()
            )
            return GmailMutationResult(
                status="succeeded",
                result_json={
                    "delivery_mode": "send",
                    "sent_message_id": str(response.get("id") or ""),
                    "thread_id": str(response.get("threadId") or reply_to_thread_id),
                    "response": response,
                },
            )
        except Exception as exc:
            return GmailMutationResult(
                status=gmail_mutation_failure_status(exc),
                result_json={"delivery_mode": delivery_mode},
                error=str(exc),
            )

    def _service(self, *, account: str, operation: str):
        if self._service_factory is not None:
            return self._service_factory(account)
        scopes = (
            self._settings.gmail_compose_scopes
            if operation == GMAIL_SEND_EMAIL_OPERATION
            else self._settings.gmail_mutation_scopes
        )
        return build_gmail_mutation_service(account=account, settings=self._settings, scopes=scopes)


GmailArchiveMutationExecutor = GmailMutationExecutor


def build_gmail_mutation_service(*, account: str, settings: Settings, scopes: tuple[str, ...] | None = None):
    credentials = load_google_credentials(
        email_address=settings.account_for_email(account).email_address,
        settings=settings,
        scopes=scopes or settings.gmail_mutation_scopes,
        service_name="Gmail mutation",
    )
    return build("gmail", "v1", credentials=credentials, cache_discovery=False)


def gmail_mutation_failure_status(exc: Exception) -> str:
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


def _thread_ids(value: Any) -> list[str]:
    if isinstance(value, str) or not isinstance(value, list):
        return []
    return _unique_strings(value)


def _gmail_label_changes(*, operation: str, payload: Mapping[str, Any]) -> tuple[list[str], list[str], list[str]]:
    if operation == GMAIL_ARCHIVE_OPERATION:
        return [], [], ["INBOX"]
    if operation == GMAIL_UNARCHIVE_OPERATION:
        return ["INBOX"], [], []
    if operation != GMAIL_MODIFY_THREAD_LABELS_OPERATION:
        raise ValueError(f"unsupported Gmail label operation: {operation}")
    add_labels = _label_tokens(payload.get("add_labels"))
    create_and_add_labels = _casefold_unique_strings(_label_tokens(payload.get("create_and_add_labels")))
    remove_labels = _label_tokens(payload.get("remove_labels"))
    if not add_labels and not create_and_add_labels and not remove_labels:
        raise ValueError("missing add_labels, create_and_add_labels, or remove_labels")
    if len(add_labels) + len(create_and_add_labels) > 100:
        raise ValueError("add_labels and create_and_add_labels must include at most 100 labels combined")
    if len(remove_labels) > 100:
        raise ValueError("remove_labels must include at most 100 labels")
    adding = {label.casefold() for label in add_labels}
    overlap = next((label for label in create_and_add_labels if label.casefold() in adding), "")
    if overlap:
        raise ValueError(f"Gmail label {overlap!r} cannot be in both add_labels and create_and_add_labels")
    adding.update(label.casefold() for label in create_and_add_labels)
    overlap = next((label for label in remove_labels if label.casefold() in adding), "")
    if overlap:
        raise ValueError(f"Gmail label {overlap!r} cannot be both added and removed")
    return add_labels, create_and_add_labels, remove_labels


def _resolved_gmail_label_changes(
    *,
    service,
    operation: str,
    add_labels: list[str],
    create_and_add_labels: list[str],
    remove_labels: list[str],
) -> tuple[list[str], list[str], list[dict[str, str]]]:
    if operation in {GMAIL_ARCHIVE_OPERATION, GMAIL_UNARCHIVE_OPERATION}:
        return add_labels, remove_labels, []
    labels = _list_gmail_labels(service)
    # Resolve every strict reference before creating anything. A typo in add_labels or
    # remove_labels must fail without leaving a new user label behind.
    add_label_ids = _resolve_gmail_label_ids(add_labels, labels)
    remove_label_ids = _resolve_gmail_label_ids(remove_labels, labels)
    create_and_add_label_ids, created_labels = _resolve_or_create_gmail_label_ids(
        service=service,
        names=create_and_add_labels,
        labels=labels,
    )
    add_label_ids = _unique_strings([*add_label_ids, *create_and_add_label_ids])
    adding = set(add_label_ids)
    overlap = next((label_id for label_id in remove_label_ids if label_id in adding), "")
    if overlap:
        raise ValueError(f"Gmail label ID {overlap!r} cannot be both added and removed")
    return add_label_ids, remove_label_ids, created_labels


def _list_gmail_labels(service) -> list[dict[str, Any]]:
    response = execute_gmail_request(lambda: service.users().labels().list(userId="me").execute())
    return [_mapping(label) for label in response.get("labels", [])]


def _resolve_or_create_gmail_label_ids(
    *, service, names: list[str], labels: list[dict[str, Any]]
) -> tuple[list[str], list[dict[str, str]]]:
    resolved: list[str] = []
    created: list[dict[str, str]] = []
    available = list(labels)
    for name in names:
        label_id = _resolve_gmail_label_name_id(name, available, required=False)
        if not label_id:
            created_now = True
            try:
                response = execute_gmail_request(
                    lambda name=name: service.users().labels().create(userId="me", body={"name": name}).execute()
                )
            except HttpError as exc:
                # A successful create whose response was lost can be retried as a 409.
                # Re-listing makes that path, and concurrent same-name creates, idempotent.
                if getattr(exc.resp, "status", None) != 409:
                    raise
                created_now = False
                available = _list_gmail_labels(service)
                existing_id = _resolve_gmail_label_name_id(name, available, required=True)
                response = {"id": existing_id, "name": name}
            label = _mapping(response)
            label_id = str(label.get("id") or "").strip()
            if label_id:
                available.append({"id": label_id, "name": str(label.get("name") or name)})
            else:
                available = _list_gmail_labels(service)
                label_id = _resolve_gmail_label_name_id(name, available, required=True)
            if created_now:
                created.append({"id": label_id, "name": name})
        if label_id not in resolved:
            resolved.append(label_id)
    return resolved, created


def _resolve_gmail_label_name_id(
    name: str,
    labels: list[dict[str, Any]],
    *,
    required: bool,
) -> str:
    exact = [str(label.get("id") or "") for label in labels if name == str(label.get("name") or "")]
    candidates = exact or [
        str(label.get("id") or "") for label in labels if name.casefold() == str(label.get("name") or "").casefold()
    ]
    candidates = _unique_strings(candidates)
    if not candidates:
        if required:
            raise ValueError(f"created Gmail label {name!r} could not be resolved")
        return ""
    if len(candidates) > 1:
        raise ValueError(f"ambiguous Gmail label name {name!r}")
    return candidates[0]


def _resolve_gmail_label_ids(tokens: list[str], labels: list[dict[str, Any]]) -> list[str]:
    resolved: list[str] = []
    for token in tokens:
        exact = [
            str(label.get("id") or "")
            for label in labels
            if token in {str(label.get("id") or ""), str(label.get("name") or "")}
        ]
        candidates = exact or [
            str(label.get("id") or "")
            for label in labels
            if token.casefold()
            in {
                str(label.get("id") or "").casefold(),
                str(label.get("name") or "").casefold(),
            }
        ]
        candidates = _unique_strings(candidates)
        if not candidates:
            raise ValueError(f"unknown Gmail label {token!r}")
        if len(candidates) > 1:
            raise ValueError(f"ambiguous Gmail label {token!r}; use its immutable label ID")
        if candidates[0] not in resolved:
            resolved.append(candidates[0])
    return resolved


def _gmail_modify_body(*, add_label_ids: list[str], remove_label_ids: list[str]) -> dict[str, list[str]]:
    body: dict[str, list[str]] = {}
    if add_label_ids:
        body["addLabelIds"] = add_label_ids
    if remove_label_ids:
        body["removeLabelIds"] = remove_label_ids
    if not body:
        raise ValueError("Gmail label mutation resolved to no label changes")
    return body


def _thread_progress_key(operation: str) -> str:
    if operation == GMAIL_ARCHIVE_OPERATION:
        return "archived_thread_ids"
    if operation == GMAIL_UNARCHIVE_OPERATION:
        return "unarchived_thread_ids"
    return "modified_thread_ids"


def _message_progress_key(operation: str) -> str:
    if operation == GMAIL_ARCHIVE_OPERATION:
        return "archived_message_ids"
    if operation == GMAIL_UNARCHIVE_OPERATION:
        return "unarchived_message_ids"
    return "modified_message_ids"


def _label_tokens(value: Any) -> list[str]:
    if isinstance(value, str) or not isinstance(value, list):
        return []
    return _unique_strings(value)


def _unique_strings(values) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in values:
        value = str(item).strip()
        if value and value not in seen:
            normalized.append(value)
            seen.add(value)
    return normalized


def _casefold_unique_strings(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.casefold()
        if key not in seen:
            normalized.append(value)
            seen.add(key)
    return normalized


def _message_ids(value: Any) -> list[str]:
    if isinstance(value, str) or not isinstance(value, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        message_id = str(item).strip()
        if message_id and message_id not in seen:
            normalized.append(message_id)
            seen.add(message_id)
    return normalized


def _chunks(values: list[str], size: int):
    for index in range(0, len(values), size):
        yield values[index : index + size]


def build_email_raw(*, account: str, message: Mapping[str, Any]) -> str:
    recipients = {
        "To": _string_list(message.get("to")),
        "Cc": _string_list(message.get("cc")),
        "Bcc": _string_list(message.get("bcc")),
    }
    if not any(recipients.values()):
        raise ValueError("email must include at least one recipient")
    subject = str(message.get("subject") or "").strip()
    if not subject:
        raise ValueError("email subject must not be blank")
    body_text = str(message.get("body_text") or "")
    body_html = str(message.get("body_html") or "")
    if not body_text.strip() and not body_html.strip():
        raise ValueError("email body must not be blank")

    email = EmailMessage()
    email["From"] = account
    for header, values in recipients.items():
        if values:
            email[header] = ", ".join(values)
    email["Subject"] = subject
    in_reply_to = str(message.get("in_reply_to") or "").strip()
    if in_reply_to:
        email["In-Reply-To"] = in_reply_to
    references = _string_list(message.get("references"))
    if references:
        email["References"] = " ".join(references)

    if body_text and body_html:
        email.set_content(body_text)
        email.add_alternative(body_html, subtype="html")
    elif body_html:
        email.set_content(body_html, subtype="html")
    else:
        email.set_content(body_text)
    return base64.urlsafe_b64encode(email.as_bytes()).decode("ascii")


def _delivery_mode(value: Any) -> str:
    mode = str(value or "send").strip().lower()
    if mode not in {"send", "draft"}:
        raise ValueError("delivery_mode must be send or draft")
    return mode


def _message_with_reply_references(message: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(message)
    in_reply_to = str(out.get("in_reply_to") or "").strip()
    if not str(out.get("reply_to_thread_id") or "").strip() or not in_reply_to:
        return out
    references = _string_list(out.get("references"))
    if in_reply_to not in references:
        references.append(in_reply_to)
    out["references"] = references
    return out


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_values = value.split(",")
    elif isinstance(value, list):
        raw_values = value
    else:
        return []
    return [str(item).strip() for item in raw_values if str(item).strip()]
