from __future__ import annotations

import base64
from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
from io import BytesIO
import json
from unittest.mock import patch
import zipfile

from googleapiclient.errors import HttpError
from httplib2 import Response
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.gmail_sync import gmail_mailbox_sync_every_minute
from personal_data_warehouse.gmail_auth import update_env_file
from personal_data_warehouse.gmail_sync import (
    attachment_parts_from_message,
    attachment_rows_for_message,
    collapsed_message_body_to_markdown,
    decode_base64url,
    deleted_attachment_rows,
    execute_gmail_request,
    extract_attachment_text,
    exclusive_gmail_sync_lock,
    exclusive_process_lock,
    extract_message_bodies,
    GMAIL_SYNC_POSTGRES_LOCK_ID,
    GmailSyncRunner,
    gmail_token_json_from_env,
    history_message_ids,
    message_body_to_markdown,
    message_to_row,
    strip_quoted_history,
)


def _gmail_data(content: bytes) -> str:
    return base64.urlsafe_b64encode(content).decode("ascii").rstrip("=")


class FakeGmailAttachmentService:
    def __init__(self, attachments: dict[str, bytes]) -> None:
        self.attachment_bytes = attachments
        self.requests: list[tuple[str, str, str]] = []
        self._current_attachment_id = ""

    def users(self):
        return self

    def messages(self):
        return self

    def attachments(self):
        return self

    def get(self, *, userId: str, messageId: str, id: str):
        self.requests.append((userId, messageId, id))
        self._current_attachment_id = id
        return self

    def execute(self):
        return {"data": _gmail_data(self.attachment_bytes[self._current_attachment_id])}


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeAttachmentBackfillWarehouse:
    def __init__(self, messages: list[dict[str, object]], existing_keys=None) -> None:
        self.messages = messages
        self.existing_keys = set(existing_keys or set())
        self.attachment_rows: list[dict[str, object]] = []
        self.state_rows: list[dict[str, object]] = []

    def load_attachment_backfill_candidate_messages(self, *, account: str, limit: int):
        return self.messages[:limit]

    def existing_attachment_keys(self, *, account: str, message_ids: list[str]):
        return self.existing_keys

    def insert_attachments(self, rows: list[dict[str, object]]) -> None:
        self.attachment_rows.extend(rows)

    def insert_attachment_backfill_state(self, rows: list[dict[str, object]]) -> None:
        self.state_rows.extend(rows)


def test_load_settings_accepts_oauth_client_secrets_json_env(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_SECRETS_JSON", '{"installed":{}}')

    settings = load_settings(require_clickhouse=False)

    assert settings.gmail_oauth_client_secrets_json == '{"installed":{}}'


def test_load_settings_can_skip_oauth_client_secrets_for_sync_runtime(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_SECRETS_JSON", "")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_SECRETS_JSON_B64", "")

    settings = load_settings(require_clickhouse=False, require_gmail_client_secrets=False)

    assert settings.gmail_accounts[0].email_address == "zach@hackclub.com"
    assert settings.gmail_oauth_client_secrets_json is None


def test_load_settings_accepts_gmail_attachment_limits(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("GMAIL_ATTACHMENT_MAX_BYTES", "1024")
    monkeypatch.setenv("GMAIL_ATTACHMENT_TEXT_MAX_CHARS", "2048")
    monkeypatch.setenv("GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE", "25")

    settings = load_settings(require_clickhouse=False, require_gmail_client_secrets=False)

    assert settings.gmail_attachment_max_bytes == 1024
    assert settings.gmail_attachment_text_max_chars == 2048
    assert settings.gmail_attachment_backfill_batch_size == 25


def test_gmail_token_json_from_env_accepts_base64(monkeypatch) -> None:
    token_json = json.dumps({"token": "access-token"})
    encoded_token = base64.b64encode(token_json.encode("utf-8")).decode("ascii")
    monkeypatch.setenv("GMAIL_ZACH_HACKCLUB_COM_TOKEN_JSON_B64", encoded_token)

    assert gmail_token_json_from_env("zach@hackclub.com") == token_json


def test_gmail_sync_schedule_runs_every_minute_by_default() -> None:
    assert gmail_mailbox_sync_every_minute.cron_schedule == "* * * * *"
    assert gmail_mailbox_sync_every_minute.default_status.value == "RUNNING"


def test_google_auth_update_env_file_replaces_and_appends_values(tmp_path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("CLICKHOUSE_URL=clickhouse://example\nGMAIL_ZACH_EXAMPLE_COM_TOKEN_JSON_B64=old\n")

    update_env_file(
        env_path,
        {
            "GOOGLE_ZACH_EXAMPLE_COM_TOKEN_JSON_B64": "new",
            "GMAIL_ZACH_EXAMPLE_COM_TOKEN_JSON_B64": "new",
        },
    )

    assert env_path.read_text().splitlines() == [
        "CLICKHOUSE_URL=clickhouse://example",
        "GMAIL_ZACH_EXAMPLE_COM_TOKEN_JSON_B64=new",
        "",
        "GOOGLE_ZACH_EXAMPLE_COM_TOKEN_JSON_B64=new",
    ]


def test_exclusive_process_lock_is_non_blocking(tmp_path) -> None:
    lock_path = tmp_path / "gmail-sync.lock"

    with exclusive_process_lock(lock_path) as first_acquired:
        assert first_acquired
        with exclusive_process_lock(lock_path) as second_acquired:
            assert not second_acquired


def test_exclusive_gmail_sync_lock_uses_postgres_when_configured(monkeypatch) -> None:
    calls: list[tuple[str, int]] = []

    @contextmanager
    def fake_postgres_lock(postgres_url: str, lock_id: int):
        calls.append((postgres_url, lock_id))
        yield False

    monkeypatch.setenv("DAGSTER_POSTGRES_URL", "postgresql://postgres/dagster")
    monkeypatch.setattr(
        "personal_data_warehouse.gmail_sync.exclusive_postgres_advisory_lock",
        fake_postgres_lock,
    )

    with exclusive_gmail_sync_lock() as acquired:
        assert not acquired

    assert calls == [("postgresql://postgres/dagster", GMAIL_SYNC_POSTGRES_LOCK_ID)]


def test_decode_base64url_handles_missing_padding() -> None:
    assert decode_base64url("SGVsbG8") == "Hello"


def test_execute_gmail_request_retries_transient_backend_errors() -> None:
    attempts = 0

    def flaky_request() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise HttpError(Response({"status": "500"}), b"backend error")
        return "ok"

    with patch("personal_data_warehouse.gmail_sync.time.sleep") as sleep:
        assert execute_gmail_request(flaky_request) == "ok"

    assert attempts == 2
    sleep.assert_called_once_with(1)


def test_extract_message_bodies_walks_nested_parts() -> None:
    payload = {
        "mimeType": "multipart/alternative",
        "parts": [
            {
                "mimeType": "text/plain",
                "body": {"data": "cGxhaW4gdGV4dA"},
            },
            {
                "mimeType": "multipart/related",
                "parts": [
                    {
                        "mimeType": "text/html",
                        "body": {"data": "PGI-aHRtbDwvYj4"},
                    }
                ],
            },
        ],
    }

    text_body, html_body = extract_message_bodies(payload)

    assert text_body == "plain text"
    assert html_body == "<b>html</b>"


def test_extract_message_bodies_handles_deeply_nested_parts() -> None:
    payload = {"mimeType": "text/plain", "body": {"data": "ZGVlcA"}}
    for _ in range(1100):
        payload = {"mimeType": "multipart/mixed", "parts": [payload]}

    text_body, html_body = extract_message_bodies(payload)

    assert text_body == "deep"
    assert html_body == ""


def test_history_message_ids_collects_all_message_shapes() -> None:
    history_record = {
        "messages": [{"id": "1"}],
        "messagesAdded": [{"message": {"id": "2"}}],
        "messagesDeleted": [{"message": {"id": "3"}}],
        "labelsAdded": [{"message": {"id": "4"}}],
        "labelsRemoved": [{"message": {"id": "5"}}],
    }

    assert history_message_ids(history_record) == {"1", "2", "3", "4", "5"}


def test_attachment_parts_from_message_finds_real_attachment_parts() -> None:
    message = {
        "payload": {
            "mimeType": "multipart/mixed",
            "parts": [
                {"partId": "1", "mimeType": "text/plain", "body": {"data": "aGVsbG8"}},
                {
                    "partId": "2",
                    "filename": "report.txt",
                    "mimeType": "text/plain",
                    "headers": [{"name": "Content-Disposition", "value": "attachment; filename=report.txt"}],
                    "body": {"attachmentId": "att-1", "size": 12},
                },
                {
                    "partId": "3",
                    "filename": "",
                    "mimeType": "image/png",
                    "headers": [{"name": "Content-Disposition", "value": "inline"}],
                    "body": {"attachmentId": "att-2", "size": 10},
                },
            ],
        }
    }

    parts = attachment_parts_from_message(message)

    assert [part["partId"] for part in parts] == ["2", "3"]


def test_attachment_rows_for_message_extracts_inline_text_attachment() -> None:
    content = b"attachment body"
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "2",
                    "filename": "notes.txt",
                    "mimeType": "text/plain",
                    "body": {"data": _gmail_data(content), "size": len(content)},
                    "headers": [{"name": "Content-ID", "value": "<notes@example.com>"}],
                }
            ]
        },
    }

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=100,
        text_max_chars=100,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["message_id"] == "gmail-id"
    assert row["filename"] == "notes.txt"
    assert row["mime_type"] == "text/plain"
    assert row["content_id"] == "<notes@example.com>"
    assert row["size"] == len(content)
    assert row["content_sha256"] == hashlib.sha256(content).hexdigest()
    assert row["text"] == "attachment body"
    assert row["text_extraction_status"] == "ok"
    assert row["is_deleted"] == 0
    assert json.loads(row["part_json"])["body"]["data"] == "<redacted>"


def test_attachment_rows_for_message_fetches_attachment_id_content() -> None:
    service = FakeGmailAttachmentService({"att-1": b"<p>Attachment <b>HTML</b></p>"})
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "2",
                    "filename": "notes.html",
                    "mimeType": "text/html",
                    "body": {"attachmentId": "att-1", "size": 30},
                }
            ]
        },
    }

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=service,
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=100,
        text_max_chars=100,
    )

    assert service.requests == [("me", "gmail-id", "att-1")]
    assert rows[0]["text_extraction_status"] == "ok"
    assert "Attachment" in rows[0]["text"]
    assert "HTML" in rows[0]["text"]


def test_attachment_rows_for_message_skips_existing_and_tombstones_missing_attachment() -> None:
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "2",
                    "filename": "current.txt",
                    "mimeType": "text/plain",
                    "body": {"attachmentId": "att-current", "size": 5},
                }
            ]
        },
    }
    existing_keys = {
        ("gmail-id", "2", "current.txt"),
        ("gmail-id", "3", "old.txt"),
        ("other-message", "2", "other.txt"),
    }

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=existing_keys,
        max_bytes=100,
        text_max_chars=100,
    )

    assert len(rows) == 1
    assert rows[0]["message_id"] == "gmail-id"
    assert rows[0]["part_id"] == "3"
    assert rows[0]["attachment_id"] == ""
    assert rows[0]["filename"] == "old.txt"
    assert rows[0]["text_extraction_status"] == "deleted"
    assert rows[0]["is_deleted"] == 1


def test_deleted_attachment_rows_only_marks_deleted_message_attachments() -> None:
    rows = deleted_attachment_rows(
        account="zach@example.com",
        message_id="gmail-id",
        existing_keys={
            ("gmail-id", "2", "one.txt"),
            ("other-message", "3", "two.txt"),
        },
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        history_id=43,
    )

    assert len(rows) == 1
    assert rows[0]["message_id"] == "gmail-id"
    assert rows[0]["filename"] == "one.txt"
    assert rows[0]["history_id"] == 43
    assert rows[0]["is_deleted"] == 1


def test_runner_backfills_attachment_candidates_and_marks_state(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    monkeypatch.setenv("GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE", "10")
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "2",
                    "filename": "notes.txt",
                    "mimeType": "text/plain",
                    "body": {"data": _gmail_data(b"backfill text"), "size": 13},
                }
            ]
        },
    }
    warehouse = FakeAttachmentBackfillWarehouse(
        [message],
        existing_keys={("gmail-id", "2", "notes.txt")},
    )
    settings = load_settings(require_clickhouse=False, require_gmail_client_secrets=False)
    runner = GmailSyncRunner(settings=settings, warehouse=warehouse, logger=FakeLogger())

    candidates, rows_written, text_chars = runner._backfill_attachment_candidates(
        account=settings.gmail_accounts[0],
        service=FakeGmailAttachmentService({}),
    )

    assert candidates == 1
    assert rows_written == 1
    assert text_chars == len("backfill text")
    assert warehouse.attachment_rows[0]["filename"] == "notes.txt"
    assert warehouse.attachment_rows[0]["text"] == "backfill text"
    assert warehouse.state_rows[0]["message_id"] == "gmail-id"
    assert warehouse.state_rows[0]["status"] == "ok"
    assert warehouse.state_rows[0]["attachment_rows_written"] == 1


def test_runner_marks_false_positive_attachment_candidates_as_processed(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    monkeypatch.setenv("GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE", "10")
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {"mimeType": "text/plain", "body": {"data": _gmail_data(b"plain body")}},
    }
    warehouse = FakeAttachmentBackfillWarehouse([message])
    settings = load_settings(require_clickhouse=False, require_gmail_client_secrets=False)
    runner = GmailSyncRunner(settings=settings, warehouse=warehouse, logger=FakeLogger())

    candidates, rows_written, text_chars = runner._backfill_attachment_candidates(
        account=settings.gmail_accounts[0],
        service=FakeGmailAttachmentService({}),
    )

    assert candidates == 1
    assert rows_written == 0
    assert text_chars == 0
    assert warehouse.attachment_rows == []
    assert warehouse.state_rows[0]["status"] == "ok"
    assert warehouse.state_rows[0]["attachment_rows_written"] == 0


def test_extract_attachment_text_supports_office_open_xml() -> None:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "word/document.xml",
            "<document><body><p><r><t>Quarterly plan</t></r></p><p><r><t>Launch notes</t></r></p></body></document>",
        )

    extraction = extract_attachment_text(
        content=buffer.getvalue(),
        mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename="plan.docx",
        max_chars=100,
    )

    assert extraction.status == "ok"
    assert extraction.text == "Quarterly plan\nLaunch notes"


def test_extract_attachment_text_supports_zip_contents() -> None:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("notes/readme.txt", "Flight confirmation ABC123")
        archive.writestr("summary.html", "<p>Hotel address</p>")
        archive.writestr("image.png", b"\x89PNG\r\n\x1a\n")

    extraction = extract_attachment_text(
        content=buffer.getvalue(),
        mime_type="application/x-zip-compressed",
        filename="travel.zip",
        max_chars=1000,
    )

    assert extraction.status == "ok"
    assert "## notes/readme.txt" in extraction.text
    assert "Flight confirmation ABC123" in extraction.text
    assert "## summary.html" in extraction.text
    assert "Hotel address" in extraction.text
    assert "image.png" not in extraction.text


def test_extract_attachment_text_supports_nested_zip_contents() -> None:
    nested = BytesIO()
    with zipfile.ZipFile(nested, "w") as archive:
        archive.writestr("nested.txt", "Nested contract text")
    outer = BytesIO()
    with zipfile.ZipFile(outer, "w") as archive:
        archive.writestr("inner.zip", nested.getvalue())

    extraction = extract_attachment_text(
        content=outer.getvalue(),
        mime_type="application/zip",
        filename="outer.zip",
        max_chars=1000,
    )

    assert extraction.status == "ok"
    assert "## inner.zip" in extraction.text
    assert "## nested.txt" in extraction.text
    assert "Nested contract text" in extraction.text


def test_extract_attachment_text_truncates_large_text() -> None:
    extraction = extract_attachment_text(
        content=b"abcdef",
        mime_type="text/plain",
        filename="notes.txt",
        max_chars=3,
    )

    assert extraction.status == "truncated"
    assert extraction.text == "abc"


def test_message_to_row_extracts_headers_and_bodies() -> None:
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "labelIds": ["INBOX", "UNREAD"],
        "snippet": "hello world",
        "sizeEstimate": 1234,
        "payload": {
            "headers": [
                {"name": "Subject", "value": "Test subject"},
                {"name": "From", "value": "Zach <zach@example.com>"},
                {"name": "To", "value": "friend@example.com"},
                {"name": "Message-ID", "value": "<abc@example.com>"},
            ],
            "mimeType": "multipart/alternative",
            "parts": [
                {"mimeType": "text/plain", "body": {"data": "aGVsbG8"}},
                {"mimeType": "text/html", "body": {"data": "PHA-aGVsbG88L3A-"}},
            ],
        },
    }

    row = message_to_row(
        account="zach@example.com",
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
    )

    assert row["account"] == "zach@example.com"
    assert row["message_id"] == "gmail-id"
    assert row["history_id"] == 42
    assert row["subject"] == "Test subject"
    assert row["from_address"] == "zach@example.com"
    assert row["to_addresses"] == ["friend@example.com"]
    assert row["body_text"] == "hello"
    assert row["body_html"] == "<p>hello</p>"
    assert row["body_markdown"] == "hello"
    assert row["body_markdown_full"] == "hello"
    assert row["body_markdown_clean"] == "hello"


def test_message_to_row_tolerates_recursive_html_conversion(monkeypatch) -> None:
    def raise_recursion_error(value: str) -> str:
        raise RecursionError("too deep")

    monkeypatch.setattr("personal_data_warehouse.gmail_sync.html_fragment_to_markdown", raise_recursion_error)
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "mimeType": "multipart/alternative",
            "parts": [
                {"mimeType": "text/plain", "body": {"data": "bGF0ZXN0Cg0KT24gVGh1LCBTb21lb25lIHdyb3RlOg0Kb2xk"}},
                {"mimeType": "text/html", "body": {"data": "PGRpdj5sYXRlc3Q8L2Rpdj4"}},
            ],
        },
    }

    row = message_to_row(
        account="zach@example.com",
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
    )

    assert row["body_text"].replace("\r\n", "\n") == "latest\n\nOn Thu, Someone wrote:\nold"
    assert row["body_markdown"] == "latest"
    assert row["body_markdown_full"] == "latest\n\nOn Thu, Someone wrote:\nold"


def test_message_to_row_uses_collapsed_markdown_when_full_is_empty(monkeypatch) -> None:
    monkeypatch.setattr("personal_data_warehouse.gmail_sync.safe_message_body_to_markdown", lambda **_: "")
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {"mimeType": "text/plain", "body": {"data": "bGF0ZXN0"}},
    }

    row = message_to_row(
        account="zach@example.com",
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
    )

    assert row["body_markdown"] == "latest"
    assert row["body_markdown_full"] == "latest"


def test_strip_quoted_history_keeps_latest_reply_only() -> None:
    markdown = """Latest reply.

> quoted previous reply

On Thu, Apr 23, 2026 at 10:00 AM Someone <a@example.com> wrote:
Older body
"""

    assert strip_quoted_history(markdown) == "Latest reply."


def test_message_body_to_markdown_prefers_html() -> None:
    assert message_body_to_markdown(body_text="plain", body_html="<p><strong>html</strong></p>") == "**html**"


def test_collapsed_message_body_to_markdown_removes_gmail_quote() -> None:
    html = """
    <div>Latest reply</div>
    <div class="gmail_quote">
      <div>On Thu, Someone wrote:</div>
      <blockquote>Older full thread</blockquote>
    </div>
    """

    assert collapsed_message_body_to_markdown(body_text="", body_html=html) == "Latest reply"


def test_collapsed_message_body_to_markdown_removes_plain_text_history() -> None:
    text = """Latest reply

On Thu, Apr 23, 2026 at 10:00 AM Someone <a@example.com> wrote:
Older full thread
"""

    assert collapsed_message_body_to_markdown(body_text=text, body_html="") == "Latest reply"
