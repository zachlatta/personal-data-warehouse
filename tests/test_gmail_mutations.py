from __future__ import annotations

import base64
from email import policy
from email.parser import BytesParser

from personal_data_warehouse import gmail_mutations
from personal_data_warehouse.gmail_mutations import (
    GMAIL_ARCHIVE_OPERATION,
    GMAIL_SEND_EMAIL_OPERATION,
    GMAIL_UNARCHIVE_OPERATION,
    GmailMutationExecutor,
    gmail_mutation_failure_status,
)


class FakeGmailRequest:
    def __init__(self, response=None, error: Exception | None = None) -> None:
        self._response = response or {}
        self._error = error

    def execute(self):
        if self._error is not None:
            raise self._error
        return self._response


class FakeThreadsResource:
    def __init__(self, service) -> None:
        self._service = service

    def modify(self, **kwargs):
        self._service.modify_calls.append(kwargs)
        if self._service.errors:
            return FakeGmailRequest(response={"id": kwargs["id"], "ok": True}, error=self._service.errors.pop(0))
        return FakeGmailRequest(response={"id": kwargs["id"], "ok": True})


class FakeMessagesResource:
    def __init__(self, service) -> None:
        self._service = service

    def send(self, **kwargs):
        self._service.send_calls.append(kwargs)
        return FakeGmailRequest(response={"id": "sent-message-1", "threadId": kwargs["body"].get("threadId", "thread-new")})


class FakeDraftsResource:
    def __init__(self, service) -> None:
        self._service = service

    def create(self, **kwargs):
        self._service.draft_create_calls.append(kwargs)
        message = kwargs["body"]["message"]
        return FakeGmailRequest(
            response={
                "id": "draft-1",
                "message": {"id": "draft-message-1", "threadId": message.get("threadId", "thread-new")},
            }
        )


class FakeUsersResource:
    def __init__(self, service) -> None:
        self._service = service

    def threads(self):
        return FakeThreadsResource(self._service)

    def messages(self):
        return FakeMessagesResource(self._service)

    def drafts(self):
        return FakeDraftsResource(self._service)


class FakeGmailService:
    def __init__(self, *, errors=None) -> None:
        self.errors = list(errors or [])
        self.modify_calls = []
        self.send_calls = []
        self.draft_create_calls = []

    def users(self):
        return FakeUsersResource(self)


def test_gmail_archive_executor_removes_inbox_from_threads() -> None:
    service = FakeGmailService()
    executor = GmailMutationExecutor(
        settings=object(),
        service_factory=lambda account: service,
    )

    result = executor.execute(
        {
            "provider": "gmail",
            "operation": GMAIL_ARCHIVE_OPERATION,
            "account": "zach@example.test",
            "payload_json": {"thread_ids": ["thread-1", "thread-2"]},
        }
    )

    assert result.status == "succeeded"
    assert result.result_json["archived_thread_ids"] == ["thread-1", "thread-2"]
    assert service.modify_calls == [
        {"userId": "me", "id": "thread-1", "body": {"removeLabelIds": ["INBOX"]}},
        {"userId": "me", "id": "thread-2", "body": {"removeLabelIds": ["INBOX"]}},
    ]


def test_gmail_unarchive_executor_adds_inbox_to_threads() -> None:
    service = FakeGmailService()
    executor = GmailMutationExecutor(
        settings=object(),
        service_factory=lambda account: service,
    )

    result = executor.execute(
        {
            "provider": "gmail",
            "operation": GMAIL_UNARCHIVE_OPERATION,
            "account": "zach@example.test",
            "payload_json": {"thread_ids": ["thread-1"]},
        }
    )

    assert result.status == "succeeded"
    assert result.result_json["unarchived_thread_ids"] == ["thread-1"]
    assert service.modify_calls == [
        {"userId": "me", "id": "thread-1", "body": {"addLabelIds": ["INBOX"]}},
    ]


def test_gmail_archive_executor_records_partial_progress_for_retryable_failure(monkeypatch) -> None:
    monkeypatch.setattr(gmail_mutations, "execute_gmail_request", lambda request_fn: request_fn())
    service = FakeGmailService(errors=[None, ConnectionError("network down")])
    executor = GmailMutationExecutor(
        settings=object(),
        service_factory=lambda account: service,
    )

    result = executor.execute(
        {
            "provider": "gmail",
            "operation": GMAIL_ARCHIVE_OPERATION,
            "account": "zach@example.test",
            "payload_json": {"thread_ids": ["thread-1", "thread-2"]},
        }
    )

    assert result.status == "failed_retryable"
    assert result.result_json["archived_thread_ids"] == ["thread-1"]
    assert service.modify_calls == [
        {"userId": "me", "id": "thread-1", "body": {"removeLabelIds": ["INBOX"]}},
        {"userId": "me", "id": "thread-2", "body": {"removeLabelIds": ["INBOX"]}},
    ]


def test_gmail_send_email_executor_sends_new_message_with_recipients() -> None:
    service = FakeGmailService()
    executor = GmailMutationExecutor(
        settings=object(),
        service_factory=lambda account: service,
    )

    result = executor.execute(
        {
            "provider": "gmail",
            "operation": GMAIL_SEND_EMAIL_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "delivery_mode": "send",
                "message": {
                    "to": ["one@example.test"],
                    "cc": ["two@example.test", "three@example.test"],
                    "bcc": ["secret@example.test"],
                    "subject": "Hello",
                    "body_text": "Plain body",
                },
            },
        }
    )

    assert result.status == "succeeded"
    assert result.result_json["delivery_mode"] == "send"
    assert result.result_json["sent_message_id"] == "sent-message-1"
    assert len(service.send_calls) == 1
    body = service.send_calls[0]["body"]
    assert set(body) == {"raw"}
    message = _decode_raw_message(body["raw"])
    assert message["From"] == "zach@example.test"
    assert message["To"] == "one@example.test"
    assert message["Cc"] == "two@example.test, three@example.test"
    assert message["Bcc"] == "secret@example.test"
    assert message["Subject"] == "Hello"
    assert message.get_body(preferencelist=("plain",)).get_content() == "Plain body\n"


def test_gmail_send_email_executor_creates_reply_draft_with_thread_headers() -> None:
    service = FakeGmailService()
    executor = GmailMutationExecutor(
        settings=object(),
        service_factory=lambda account: service,
    )

    result = executor.execute(
        {
            "provider": "gmail",
            "operation": GMAIL_SEND_EMAIL_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "delivery_mode": "draft",
                "message": {
                    "to": ["sender@example.test"],
                    "subject": "Re: Existing thread",
                    "body_text": "Reply body",
                    "reply_to_thread_id": "thread-1",
                    "in_reply_to": "<message-1@example.test>",
                    "references": ["<message-0@example.test>", "<message-1@example.test>"],
                },
            },
        }
    )

    assert result.status == "succeeded"
    assert result.result_json["delivery_mode"] == "draft"
    assert result.result_json["draft_id"] == "draft-1"
    assert len(service.draft_create_calls) == 1
    body = service.draft_create_calls[0]["body"]
    assert body["message"]["threadId"] == "thread-1"
    message = _decode_raw_message(body["message"]["raw"])
    assert message["In-Reply-To"] == "<message-1@example.test>"
    assert message["References"] == "<message-0@example.test> <message-1@example.test>"


def test_gmail_mutation_failure_status_marks_missing_scope_as_blocked() -> None:
    error = RuntimeError("OAuth token for zach@example.test cannot be refreshed")

    assert gmail_mutation_failure_status(error) == "blocked_missing_credentials"


def _decode_raw_message(raw: str):
    padded = raw + ("=" * (-len(raw) % 4))
    return BytesParser(policy=policy.default).parsebytes(base64.urlsafe_b64decode(padded.encode("ascii")))
