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
GMAIL_SEND_EMAIL_OPERATION = "gmail.send_email"


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
        progress_key = "archived_thread_ids" if operation == GMAIL_ARCHIVE_OPERATION else "unarchived_thread_ids"
        modify_body = (
            {"removeLabelIds": ["INBOX"]}
            if operation == GMAIL_ARCHIVE_OPERATION
            else {"addLabelIds": ["INBOX"]}
        )
        try:
            service = self._service(account=account, operation=operation)
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
            return GmailMutationResult(
                status="succeeded",
                result_json={
                    progress_key: changed_thread_ids,
                    "thread_results": thread_results,
                },
            )
        except Exception as exc:
            return GmailMutationResult(
                status=gmail_mutation_failure_status(exc),
                result_json={progress_key: changed_thread_ids},
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
                    lambda: service.users()
                    .drafts()
                    .create(userId="me", body={"message": gmail_message})
                    .execute()
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
                lambda: service.users()
                .messages()
                .send(userId="me", body=gmail_message)
                .execute()
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
        scopes = self._settings.gmail_compose_scopes if operation == GMAIL_SEND_EMAIL_OPERATION else self._settings.gmail_mutation_scopes
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
    return [str(item).strip() for item in value if str(item).strip()]


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
