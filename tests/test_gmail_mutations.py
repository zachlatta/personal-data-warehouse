from __future__ import annotations

import base64
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser

from personal_data_warehouse import gmail_mutations
from personal_data_warehouse.gmail_mutations import (
    GMAIL_ARCHIVE_OPERATION,
    GMAIL_MODIFY_THREAD_LABELS_OPERATION,
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

    def batchModify(self, **kwargs):
        self._service.batch_modify_calls.append(kwargs)
        if self._service.batch_modify_errors:
            return FakeGmailRequest(response={}, error=self._service.batch_modify_errors.pop(0))
        return FakeGmailRequest(response={})

    def get(self, **kwargs):
        self._service.get_calls.append(kwargs)
        original = self._service.originals.get(kwargs["id"])
        if original is None:
            return FakeGmailRequest(error=_http_error(404))
        return FakeGmailRequest(response=original)

    def send(self, **kwargs):
        self._service.send_calls.append(kwargs)
        return FakeGmailRequest(
            response={"id": "sent-message-1", "threadId": kwargs["body"].get("threadId", "thread-new")}
        )


class FakeLabelsResource:
    def __init__(self, service) -> None:
        self._service = service

    def list(self, **kwargs):
        self._service.label_list_calls.append(kwargs)
        return FakeGmailRequest(response={"labels": self._service.labels})

    def create(self, **kwargs):
        self._service.label_create_calls.append(kwargs)
        if self._service.label_create_errors:
            error = self._service.label_create_errors.pop(0)
            if self._service.label_create_error_labels:
                self._service.labels.append(self._service.label_create_error_labels.pop(0))
            return FakeGmailRequest(error=error)
        response = self._service.label_create_responses.pop(0)
        self._service.labels.append(response)
        return FakeGmailRequest(response=response)


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

    def labels(self):
        return FakeLabelsResource(self._service)


class FakeGmailService:
    def __init__(
        self,
        *,
        errors=None,
        batch_modify_errors=None,
        labels=None,
        label_create_responses=None,
        label_create_errors=None,
        label_create_error_labels=None,
        originals=None,
    ) -> None:
        self.originals = dict(originals or {})
        self.get_calls = []
        self.errors = list(errors or [])
        self.batch_modify_errors = list(batch_modify_errors or [])
        self.modify_calls = []
        self.batch_modify_calls = []
        self.send_calls = []
        self.draft_create_calls = []
        self.labels = list(labels or [])
        self.label_list_calls = []
        self.label_create_calls = []
        self.label_create_responses = list(label_create_responses or [])
        self.label_create_errors = list(label_create_errors or [])
        self.label_create_error_labels = list(label_create_error_labels or [])

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


def test_gmail_modify_thread_labels_executor_resolves_names_and_ids() -> None:
    service = FakeGmailService(
        labels=[
            {"id": "Label_42", "name": "Receipts", "type": "user"},
            {"id": "STARRED", "name": "STARRED", "type": "system"},
            {"id": "UNREAD", "name": "UNREAD", "type": "system"},
        ]
    )
    executor = GmailMutationExecutor(settings=object(), service_factory=lambda account: service)

    result = executor.execute(
        {
            "provider": "gmail",
            "operation": GMAIL_MODIFY_THREAD_LABELS_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "thread_ids": ["thread-1"],
                "add_labels": ["Receipts", "starred"],
                "remove_labels": ["UNREAD"],
            },
        }
    )

    assert result.status == "succeeded"
    assert result.result_json["modified_thread_ids"] == ["thread-1"]
    assert result.result_json["add_label_ids"] == ["Label_42", "STARRED"]
    assert result.result_json["remove_label_ids"] == ["UNREAD"]
    assert service.label_list_calls == [{"userId": "me"}]
    assert service.modify_calls == [
        {
            "userId": "me",
            "id": "thread-1",
            "body": {
                "addLabelIds": ["Label_42", "STARRED"],
                "removeLabelIds": ["UNREAD"],
            },
        }
    ]


def test_gmail_modify_thread_labels_executor_rejects_unknown_label_before_writing() -> None:
    service = FakeGmailService(labels=[{"id": "Label_42", "name": "Receipts", "type": "user"}])
    executor = GmailMutationExecutor(settings=object(), service_factory=lambda account: service)

    result = executor.execute(
        {
            "provider": "gmail",
            "operation": GMAIL_MODIFY_THREAD_LABELS_OPERATION,
            "account": "zach@example.test",
            "payload_json": {"thread_ids": ["thread-1"], "add_labels": ["Missing label"]},
        }
    )

    assert result.status == "failed_terminal"
    assert "unknown Gmail label" in result.error
    assert service.modify_calls == []


def test_gmail_modify_thread_labels_executor_explicitly_creates_and_adds_missing_label() -> None:
    service = FakeGmailService(
        labels=[
            {"id": "Label_42", "name": "Receipts", "type": "user"},
            {"id": "UNREAD", "name": "UNREAD", "type": "system"},
        ],
        label_create_responses=[
            {"id": "Label_99", "name": "Projects/Launch", "type": "user"},
        ],
    )
    executor = GmailMutationExecutor(settings=object(), service_factory=lambda account: service)

    result = executor.execute(
        {
            "provider": "gmail",
            "operation": GMAIL_MODIFY_THREAD_LABELS_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "thread_ids": ["thread-1"],
                "add_labels": ["Receipts"],
                "create_and_add_labels": ["Projects/Launch"],
                "remove_labels": ["UNREAD"],
            },
        }
    )

    assert result.status == "succeeded"
    assert result.result_json["add_label_ids"] == ["Label_42", "Label_99"]
    assert result.result_json["created_labels"] == [{"id": "Label_99", "name": "Projects/Launch"}]
    assert service.label_create_calls == [{"userId": "me", "body": {"name": "Projects/Launch"}}]
    assert service.modify_calls == [
        {
            "userId": "me",
            "id": "thread-1",
            "body": {
                "addLabelIds": ["Label_42", "Label_99"],
                "removeLabelIds": ["UNREAD"],
            },
        }
    ]


def test_gmail_modify_thread_labels_executor_reuses_create_if_missing_label_on_retry() -> None:
    service = FakeGmailService(labels=[{"id": "Label_99", "name": "Projects/Launch", "type": "user"}])
    executor = GmailMutationExecutor(settings=object(), service_factory=lambda account: service)

    result = executor.execute(
        {
            "provider": "gmail",
            "operation": GMAIL_MODIFY_THREAD_LABELS_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "thread_ids": ["thread-1"],
                "create_and_add_labels": ["projects/launch"],
            },
        }
    )

    assert result.status == "succeeded"
    assert result.result_json["add_label_ids"] == ["Label_99"]
    assert result.result_json["created_labels"] == []
    assert service.label_create_calls == []
    assert service.modify_calls[0]["body"] == {"addLabelIds": ["Label_99"]}


def test_gmail_modify_thread_labels_executor_recovers_create_conflict_by_relisting() -> None:
    class ConflictResponse:
        status = 409
        reason = "Conflict"

    service = FakeGmailService(
        label_create_errors=[gmail_mutations.HttpError(ConflictResponse(), b'{"error":"already exists"}')],
        label_create_error_labels=[{"id": "Label_99", "name": "Projects/Launch", "type": "user"}],
    )
    executor = GmailMutationExecutor(settings=object(), service_factory=lambda account: service)

    result = executor.execute(
        {
            "provider": "gmail",
            "operation": GMAIL_MODIFY_THREAD_LABELS_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "thread_ids": ["thread-1"],
                "create_and_add_labels": ["Projects/Launch"],
            },
        }
    )

    assert result.status == "succeeded"
    assert result.result_json["add_label_ids"] == ["Label_99"]
    assert result.result_json["created_labels"] == []
    assert service.label_list_calls == [{"userId": "me"}, {"userId": "me"}]
    assert service.modify_calls[0]["body"] == {"addLabelIds": ["Label_99"]}


def test_gmail_modify_thread_labels_executor_validates_strict_labels_before_creating() -> None:
    service = FakeGmailService(label_create_responses=[{"id": "Label_99", "name": "Projects/Launch", "type": "user"}])
    executor = GmailMutationExecutor(settings=object(), service_factory=lambda account: service)

    result = executor.execute(
        {
            "provider": "gmail",
            "operation": GMAIL_MODIFY_THREAD_LABELS_OPERATION,
            "account": "zach@example.test",
            "payload_json": {
                "thread_ids": ["thread-1"],
                "add_labels": ["Misspelled existing label"],
                "create_and_add_labels": ["Projects/Launch"],
            },
        }
    )

    assert result.status == "failed_terminal"
    assert "unknown Gmail label" in result.error
    assert service.label_create_calls == []
    assert service.modify_calls == []


def test_gmail_archive_executor_batch_modifies_messages() -> None:
    service = FakeGmailService()
    executor = GmailMutationExecutor(
        settings=object(),
        service_factory=lambda account: service,
    )

    result = executor.execute_message_batch_modify(
        account="zach@example.test",
        operation=GMAIL_ARCHIVE_OPERATION,
        message_ids=["message-1", "message-2", "message-1"],
    )

    assert result.status == "succeeded"
    assert result.result_json["archived_message_ids"] == ["message-1", "message-2"]
    assert result.result_json["batch_modified_message_ids"] == ["message-1", "message-2"]
    assert service.batch_modify_calls == [
        {
            "userId": "me",
            "body": {"removeLabelIds": ["INBOX"], "ids": ["message-1", "message-2"]},
        }
    ]
    assert service.modify_calls == []


def test_gmail_unarchive_executor_batch_modifies_messages() -> None:
    service = FakeGmailService()
    executor = GmailMutationExecutor(
        settings=object(),
        service_factory=lambda account: service,
    )

    result = executor.execute_message_batch_modify(
        account="zach@example.test",
        operation=GMAIL_UNARCHIVE_OPERATION,
        message_ids=["message-1"],
    )

    assert result.status == "succeeded"
    assert result.result_json["unarchived_message_ids"] == ["message-1"]
    assert service.batch_modify_calls == [{"userId": "me", "body": {"addLabelIds": ["INBOX"], "ids": ["message-1"]}}]


def test_gmail_modify_thread_labels_executor_batch_modifies_messages() -> None:
    service = FakeGmailService(
        labels=[
            {"id": "Label_42", "name": "Receipts", "type": "user"},
            {"id": "UNREAD", "name": "UNREAD", "type": "system"},
        ],
        label_create_responses=[
            {"id": "Label_99", "name": "Projects/Launch", "type": "user"},
        ],
    )
    executor = GmailMutationExecutor(settings=object(), service_factory=lambda account: service)

    result = executor.execute_message_batch_modify(
        account="zach@example.test",
        operation=GMAIL_MODIFY_THREAD_LABELS_OPERATION,
        message_ids=["message-1", "message-2"],
        add_labels=["Receipts"],
        create_and_add_labels=["Projects/Launch"],
        remove_labels=["UNREAD"],
    )

    assert result.status == "succeeded"
    assert result.result_json["add_label_ids"] == ["Label_42", "Label_99"]
    assert result.result_json["created_labels"] == [{"id": "Label_99", "name": "Projects/Launch"}]
    assert result.result_json["remove_label_ids"] == ["UNREAD"]
    assert result.result_json["batch_modified_message_ids"] == ["message-1", "message-2"]
    assert service.batch_modify_calls == [
        {
            "userId": "me",
            "body": {
                "addLabelIds": ["Label_42", "Label_99"],
                "removeLabelIds": ["UNREAD"],
                "ids": ["message-1", "message-2"],
            },
        }
    ]


def test_gmail_archive_executor_batch_records_partial_progress_for_retryable_failure(monkeypatch) -> None:
    monkeypatch.setattr(gmail_mutations, "GMAIL_BATCH_MODIFY_MESSAGE_LIMIT", 2)
    monkeypatch.setattr(gmail_mutations, "execute_gmail_request", lambda request_fn: request_fn())
    service = FakeGmailService(batch_modify_errors=[None, ConnectionError("network down")])
    executor = GmailMutationExecutor(
        settings=object(),
        service_factory=lambda account: service,
    )

    result = executor.execute_message_batch_modify(
        account="zach@example.test",
        operation=GMAIL_ARCHIVE_OPERATION,
        message_ids=["message-1", "message-2", "message-3"],
    )

    assert result.status == "failed_retryable"
    assert result.result_json["archived_message_ids"] == ["message-1", "message-2"]
    assert result.result_json["batch_modified_message_ids"] == ["message-1", "message-2"]
    assert service.batch_modify_calls == [
        {
            "userId": "me",
            "body": {"removeLabelIds": ["INBOX"], "ids": ["message-1", "message-2"]},
        },
        {
            "userId": "me",
            "body": {"removeLabelIds": ["INBOX"], "ids": ["message-3"]},
        },
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


def test_gmail_send_email_executor_blocks_reply_without_message_headers() -> None:
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
                },
            },
        }
    )

    assert result.status == "failed_terminal"
    assert "In-Reply-To" in result.error
    assert service.draft_create_calls == []
    assert service.send_calls == []


def test_build_email_raw_preserves_gmail_style_html_body() -> None:
    body_html = (
        "<div>Hello from PDW.</div><div><br></div>"
        '<div class="gmail_signature"><div dir="ltr"><span style="color:rgb(0,0,0);font-family:arial,sans-serif">--</span><br/>'
        '<font face="arial, sans-serif" color="#000000" style="background-color:rgb(255,255,255)">Zach Latta</font>'
        "</div></div>"
    )

    raw = gmail_mutations.build_email_raw(
        account="zach@example.test",
        message={
            "to": ["one@example.test"],
            "subject": "HTML body",
            "body_text": "Hello from PDW.\n\n--\nZach Latta\n",
            "body_html": body_html,
        },
    )

    message = _decode_raw_message(raw)
    html_part = message.get_body(preferencelist=("html",))
    assert html_part is not None
    assert html_part.get_content().strip() == body_html


def test_gmail_mutation_failure_status_marks_missing_scope_as_blocked() -> None:
    error = RuntimeError("OAuth token for zach@example.test cannot be refreshed")

    assert gmail_mutation_failure_status(error) == "blocked_missing_credentials"


def test_gmail_mutation_failure_status_treats_refresh_error_as_blocked() -> None:
    from google.auth.exceptions import RefreshError

    error = RefreshError("invalid_scope: Bad Request", {"error": "invalid_scope"})

    assert gmail_mutation_failure_status(error) == "blocked_missing_credentials"


def _http_error(status: int):
    class Response:
        reason = "error"

    response = Response()
    response.status = status
    return gmail_mutations.HttpError(response, b'{"error":"error"}')


def _original_message_raw() -> str:
    original = EmailMessage()
    original["From"] = "Vendor <billing@vendor.test>"
    original["To"] = "zach@example.test"
    original["Subject"] = "Invoice 4831"
    original["Message-ID"] = "<invoice-4831@vendor.test>"
    original.set_content("Your invoice is attached.")
    original.add_alternative('<p>Your invoice is attached.</p><img src="cid:logo@vendor.test">', subtype="html")
    original.get_payload()[1].add_related(
        b"\x89PNG-logo", maintype="image", subtype="png", cid="<logo@vendor.test>"
    )
    original.add_attachment(b"%PDF-invoice", maintype="application", subtype="pdf", filename="invoice-4831.pdf")
    return base64.urlsafe_b64encode(original.as_bytes()).decode("ascii")


def _forward_payload(mode: str) -> dict:
    return {
        "delivery_mode": mode,
        "message": {
            "to": ["accountant@example.test"],
            "subject": "Fwd: Invoice 4831",
            "body_text": "Can you file this?\n\n---------- Forwarded message ---------\nFrom: Vendor <billing@vendor.test>\n",
            "body_html": "<div>Can you file this?</div>",
            "forward_message_id": "orig-1",
            "in_reply_to": "<invoice-4831@vendor.test>",
            "references": ["<invoice-4831@vendor.test>"],
            "attachments": [
                {"filename": "note.txt", "content_type": "text/plain", "data_base64": base64.b64encode(b"note").decode()},
            ],
        },
    }


def test_gmail_forward_carries_the_originals_attachments_into_its_thread() -> None:
    for mode in ("send", "draft"):
        service = FakeGmailService(
            originals={"orig-1": {"id": "orig-1", "threadId": "thread-orig", "raw": _original_message_raw()}}
        )
        result = GmailMutationExecutor(settings=object(), service_factory=lambda account: service).execute(
            {
                "provider": "gmail",
                "operation": GMAIL_SEND_EMAIL_OPERATION,
                "account": "zach@example.test",
                "payload_json": _forward_payload(mode),
            }
        )

        assert result.status == "succeeded", result.error
        assert service.get_calls == [{"userId": "me", "id": "orig-1", "format": "raw"}]
        assert result.result_json["forwarded_message_id"] == "orig-1"
        assert result.result_json["thread_id"] == "thread-orig"
        body = service.send_calls[0]["body"] if mode == "send" else service.draft_create_calls[0]["body"]["message"]
        # Gmail files its own forward in the original's conversation.
        assert body["threadId"] == "thread-orig"
        email = _decode_raw_message(body["raw"])
        assert email["Subject"] == "Fwd: Invoice 4831"
        assert email["In-Reply-To"] == "<invoice-4831@vendor.test>"
        assert email["References"] == "<invoice-4831@vendor.test>"
        # The reviewed words are the body; the original's text is not re-sent as a part.
        assert email.get_body(preferencelist=("plain",)).get_content().startswith("Can you file this?")
        attachments = {part.get_filename() or part["Content-ID"]: part for part in email.iter_attachments()}
        assert set(attachments) == {"note.txt", "invoice-4831.pdf", "<logo@vendor.test>"}
        assert attachments["invoice-4831.pdf"].get_payload(decode=True) == b"%PDF-invoice"
        assert attachments["invoice-4831.pdf"].get_content_type() == "application/pdf"
        assert attachments["<logo@vendor.test>"].get_payload(decode=True) == b"\x89PNG-logo"
        assert attachments["note.txt"].get_payload(decode=True) == b"note"


def test_gmail_forward_of_a_message_gone_from_gmail_sends_nothing() -> None:
    service = FakeGmailService()
    result = GmailMutationExecutor(settings=object(), service_factory=lambda account: service).execute(
        {
            "provider": "gmail",
            "operation": GMAIL_SEND_EMAIL_OPERATION,
            "account": "zach@example.test",
            "payload_json": _forward_payload("send"),
        }
    )

    assert result.status == "failed_terminal"
    assert service.send_calls == []
    assert service.draft_create_calls == []


def test_gmail_forward_cannot_also_be_a_reply() -> None:
    service = FakeGmailService(
        originals={"orig-1": {"id": "orig-1", "threadId": "thread-orig", "raw": _original_message_raw()}}
    )
    payload = _forward_payload("send")
    payload["message"]["reply_to_thread_id"] = "thread-other"
    result = GmailMutationExecutor(settings=object(), service_factory=lambda account: service).execute(
        {"provider": "gmail", "operation": GMAIL_SEND_EMAIL_OPERATION, "account": "zach@example.test", "payload_json": payload}
    )

    assert result.status == "failed_terminal"
    assert "forward" in result.error
    assert service.get_calls == []
    assert service.send_calls == []


def _decode_raw_message(raw: str):
    padded = raw + ("=" * (-len(raw) % 4))
    return BytesParser(policy=policy.default).parsebytes(base64.urlsafe_b64decode(padded.encode("ascii")))


def test_email_attachments_survive_send_and_reply_draft() -> None:
    for mode in ("send", "draft"):
        service = FakeGmailService()
        data = bytes(range(256))
        result = GmailMutationExecutor(
            settings=object(), service_factory=lambda account: service
        ).execute({
            "provider": "gmail", "operation": GMAIL_SEND_EMAIL_OPERATION,
            "account": "sender@example.test",
            "payload_json": {"delivery_mode": mode, "message": {
                "to": ["recipient@example.test"], "subject": "Attachments",
                "body_text": "Plain", "body_html": "<p>HTML</p>",
                "reply_to_thread_id": "thread-1", "in_reply_to": "<id@example.test>",
                "attachments": [
                    {"filename": "résumé.bin", "content_type": "application/octet-stream",
                     "data_base64": base64.b64encode(data).decode()},
                    {"filename": "empty.txt", "content_type": "text/plain", "data_base64": ""},
                ],
            }},
        })
        assert result.status == "succeeded"
        body = (service.send_calls[0]["body"] if mode == "send"
                else service.draft_create_calls[0]["body"]["message"])
        assert body["threadId"] == "thread-1"
        email = _decode_raw_message(body["raw"])
        assert email.get_content_type() == "multipart/mixed"
        assert email.get_body(preferencelist=("plain",)).get_content().strip() == "Plain"
        assert email.get_body(preferencelist=("html",)).get_content().strip() == "<p>HTML</p>"
        parts = list(email.iter_attachments())
        assert [p.get_filename() for p in parts] == ["résumé.bin", "empty.txt"]
        assert [p.get_payload(decode=True) for p in parts] == [data, b""]
        assert all(p.get_content_disposition() == "attachment" for p in parts)


def test_invalid_attachments_fail_before_contacting_gmail() -> None:
    for attachments in (
        "bad", [{}], [{"filename": "../secret", "content_type": "text/plain", "data_base64": ""}],
        [{"filename": "ok", "content_type": "text/plain\r\nX: injected", "data_base64": ""}],
        [{"filename": "ok", "content_type": "text/plain", "data_base64": "!!"}],
        [{"filename": "ok", "content_type": "text/plain"}],
    ):
        service = FakeGmailService()
        result = GmailMutationExecutor(settings=object(), service_factory=lambda _: service).execute({
            "provider": "gmail", "operation": GMAIL_SEND_EMAIL_OPERATION,
            "account": "sender@example.test",
            "payload_json": {"message": {"to": ["a@example.test"], "subject": "Test",
                                         "body_text": "Test", "attachments": attachments}},
        })
        assert result.status == "failed_terminal"
        assert not service.send_calls and not service.draft_create_calls


def test_attachment_limits_and_canonical_base64() -> None:
    import pytest

    attachment = {"filename": "file.bin", "content_type": "application/octet-stream", "data_base64": ""}
    assert len(gmail_mutations._email_attachments([attachment] * 100)) == 100
    with pytest.raises(ValueError):
        gmail_mutations._email_attachments([attachment] * 101)
    with pytest.raises(ValueError):
        gmail_mutations._email_attachments([{**attachment, "content_type": "multipart/mixed"}])
    for encoded in ("YQ", "YR==", "YQ==\n", "_w=="):
        with pytest.raises(ValueError):
            gmail_mutations._email_attachments([{**attachment, "data_base64": encoded}])
    large = {**attachment, "data_base64": base64.b64encode(bytes(20 * 1024 * 1024)).decode()}
    assert len(gmail_mutations._email_attachments([large])[0][1]) == 20 * 1024 * 1024
    with pytest.raises(ValueError):
        gmail_mutations._email_attachments([large, {**attachment, "data_base64": "AA=="}])
