from __future__ import annotations

import base64
from contextlib import contextmanager
from datetime import UTC, datetime
import json
from unittest.mock import patch

from googleapiclient.errors import HttpError
from httplib2 import Response
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.gmail_sync import gmail_mailbox_sync_every_minute
from personal_data_warehouse.gmail_sync import (
    collapsed_message_body_to_markdown,
    decode_base64url,
    execute_gmail_request,
    exclusive_gmail_sync_lock,
    exclusive_process_lock,
    extract_message_bodies,
    GMAIL_SYNC_POSTGRES_LOCK_ID,
    gmail_token_json_from_env,
    history_message_ids,
    message_body_to_markdown,
    message_to_row,
    strip_quoted_history,
)


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


def test_gmail_token_json_from_env_accepts_base64(monkeypatch) -> None:
    token_json = json.dumps({"token": "access-token"})
    encoded_token = base64.b64encode(token_json.encode("utf-8")).decode("ascii")
    monkeypatch.setenv("GMAIL_ZACH_HACKCLUB_COM_TOKEN_JSON_B64", encoded_token)

    assert gmail_token_json_from_env("zach@hackclub.com") == token_json


def test_gmail_sync_schedule_runs_every_minute_by_default() -> None:
    assert gmail_mailbox_sync_every_minute.cron_schedule == "* * * * *"
    assert gmail_mailbox_sync_every_minute.default_status.value == "RUNNING"


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
