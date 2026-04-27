from __future__ import annotations

import base64
from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
from io import BytesIO
import json
import subprocess
import time
from unittest.mock import patch
import zipfile

from googleapiclient.errors import HttpError
from httplib2 import Response
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.gmail_sync import (
    gmail_mailbox_sync_every_minute,
    ollama_resource_from_env,
    prepare_attachment_ai_fallback,
)
from personal_data_warehouse.gmail_auth import update_env_file
from personal_data_warehouse.gmail_sync import (
    ATTACHMENT_AI_PROMPT,
    ATTACHMENT_AI_PROMPT_VERSION,
    AttachmentAiFallbackConfig,
    attachment_ai_supporting_ocr_text,
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
    format_attachment_ai_response,
    GMAIL_SYNC_POSTGRES_LOCK_ID,
    GmailSyncRunner,
    gmail_token_json_from_env,
    history_message_ids,
    message_body_to_markdown,
    message_to_row,
    run_attachment_ai_call_with_timeout,
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


class FakeOllamaResource:
    def __init__(self, response: str) -> None:
        self.response = response
        self.generate_calls = []
        self.ensure_model_calls = []
        self.ensure_model_error: Exception | None = None

    def ensure_model(self, model: str, *, pull: bool = True) -> None:
        self.ensure_model_calls.append((model, pull))
        if self.ensure_model_error:
            raise self.ensure_model_error

    def generate(self, **kwargs):
        self.generate_calls.append(kwargs)
        return self.response


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


def test_load_settings_accepts_domain_specific_oauth_client_secrets(monkeypatch) -> None:
    hackclub_secrets_json = '{"installed":{"client_id":"hackclub-client"}}'
    zachlatta_secrets_json = '{"installed":{"client_id":"zachlatta-client"}}'
    encoded_hackclub_secrets = base64.b64encode(hackclub_secrets_json.encode("utf-8")).decode("ascii")
    encoded_zachlatta_secrets = base64.b64encode(zachlatta_secrets_json.encode("utf-8")).decode("ascii")
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com,zach@zachlatta.com")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_SECRETS_JSON", '{"installed":{"client_id":"default-client"}}')
    monkeypatch.setenv("GMAIL_DOMAIN_HACKCLUB_COM_OAUTH_CLIENT_SECRETS_JSON_B64", encoded_hackclub_secrets)
    monkeypatch.setenv("GMAIL_DOMAIN_ZACHLATTA_COM_OAUTH_CLIENT_SECRETS_JSON_B64", encoded_zachlatta_secrets)

    settings = load_settings(require_clickhouse=False, require_gmail_client_secrets=True)

    assert (
        settings.google_oauth_client_secrets_json_for_email("zach@zachlatta.com")
        == zachlatta_secrets_json
    )
    assert settings.google_oauth_client_secrets_json_for_email("zach@hackclub.com") == hackclub_secrets_json


def test_load_settings_does_not_use_global_oauth_client_for_multiple_domains(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com,zach@zachlatta.com")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_SECRETS_JSON", '{"installed":{"client_id":"default-client"}}')
    for name in (
        "GOOGLE_ZACH_HACKCLUB_COM_OAUTH_CLIENT_SECRETS_JSON",
        "GOOGLE_ZACH_HACKCLUB_COM_OAUTH_CLIENT_SECRETS_JSON_B64",
        "GMAIL_ZACH_HACKCLUB_COM_OAUTH_CLIENT_SECRETS_JSON",
        "GMAIL_ZACH_HACKCLUB_COM_OAUTH_CLIENT_SECRETS_JSON_B64",
        "GOOGLE_ZACH_ZACHLATTA_COM_OAUTH_CLIENT_SECRETS_JSON",
        "GOOGLE_ZACH_ZACHLATTA_COM_OAUTH_CLIENT_SECRETS_JSON_B64",
        "GMAIL_ZACH_ZACHLATTA_COM_OAUTH_CLIENT_SECRETS_JSON",
        "GMAIL_ZACH_ZACHLATTA_COM_OAUTH_CLIENT_SECRETS_JSON_B64",
        "GOOGLE_DOMAIN_HACKCLUB_COM_OAUTH_CLIENT_SECRETS_JSON",
        "GOOGLE_DOMAIN_HACKCLUB_COM_OAUTH_CLIENT_SECRETS_JSON_B64",
        "GMAIL_DOMAIN_HACKCLUB_COM_OAUTH_CLIENT_SECRETS_JSON",
        "GMAIL_DOMAIN_HACKCLUB_COM_OAUTH_CLIENT_SECRETS_JSON_B64",
        "GOOGLE_DOMAIN_ZACHLATTA_COM_OAUTH_CLIENT_SECRETS_JSON",
        "GOOGLE_DOMAIN_ZACHLATTA_COM_OAUTH_CLIENT_SECRETS_JSON_B64",
        "GMAIL_DOMAIN_ZACHLATTA_COM_OAUTH_CLIENT_SECRETS_JSON",
        "GMAIL_DOMAIN_ZACHLATTA_COM_OAUTH_CLIENT_SECRETS_JSON_B64",
    ):
        monkeypatch.setenv(name, "")

    settings = load_settings(require_clickhouse=False, require_gmail_client_secrets=True)

    assert settings.google_oauth_client_secrets_json_for_email("zach@zachlatta.com") is None


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


def test_load_settings_enables_gmail_attachment_ai_fallback_by_default(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")

    settings = load_settings(require_clickhouse=False, require_gmail_client_secrets=False)

    assert settings.gmail_attachment_ai_fallback_enabled is True
    assert settings.gmail_attachment_ai_fallback_base_url == "http://127.0.0.1:11435"
    assert settings.gmail_attachment_ai_fallback_model == "gemma4:e2b"
    assert settings.gmail_attachment_ai_fallback_pull_model is True


def test_load_settings_accepts_gmail_attachment_ai_fallback(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("GMAIL_ATTACHMENT_AI_FALLBACK_ENABLED", "true")
    monkeypatch.setenv("GMAIL_ATTACHMENT_AI_FALLBACK_BASE_URL", "http://rotom.local:11435")
    monkeypatch.setenv("GMAIL_ATTACHMENT_AI_FALLBACK_MODEL", "gemma4:e2b")
    monkeypatch.setenv("GMAIL_ATTACHMENT_AI_FALLBACK_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("GMAIL_ATTACHMENT_AI_FALLBACK_PDF_MAX_PAGES", "2")
    monkeypatch.setenv("GMAIL_ATTACHMENT_AI_FALLBACK_PULL_MODEL", "false")

    settings = load_settings(require_clickhouse=False, require_gmail_client_secrets=False)

    assert settings.gmail_attachment_ai_fallback_enabled is True
    assert settings.gmail_attachment_ai_fallback_base_url == "http://rotom.local:11435"
    assert settings.gmail_attachment_ai_fallback_model == "gemma4:e2b"
    assert settings.gmail_attachment_ai_fallback_timeout_seconds == 45
    assert settings.gmail_attachment_ai_fallback_pdf_max_pages == 2
    assert settings.gmail_attachment_ai_fallback_pull_model is False


def test_gmail_token_json_from_env_accepts_base64(monkeypatch) -> None:
    token_json = json.dumps({"token": "access-token"})
    encoded_token = base64.b64encode(token_json.encode("utf-8")).decode("ascii")
    monkeypatch.setenv("GMAIL_ZACH_HACKCLUB_COM_TOKEN_JSON_B64", encoded_token)

    assert gmail_token_json_from_env("zach@hackclub.com") == token_json


def test_gmail_sync_schedule_runs_every_minute_by_default() -> None:
    assert gmail_mailbox_sync_every_minute.cron_schedule == "* * * * *"
    assert gmail_mailbox_sync_every_minute.default_status.value == "RUNNING"


def test_ollama_resource_from_env_reads_only_ai_connection_settings(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ATTACHMENT_AI_FALLBACK_BASE_URL", "http://ollama.local:11435")
    monkeypatch.setenv("GMAIL_ATTACHMENT_AI_FALLBACK_TIMEOUT_SECONDS", "67")

    resource = ollama_resource_from_env()

    assert resource.base_url == "http://ollama.local:11435"
    assert resource.request_timeout_seconds == 67


def test_prepare_attachment_ai_fallback_returns_ready_config(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("GMAIL_ATTACHMENT_AI_FALLBACK_MODEL", "gemma4:e2b")
    settings = load_settings(require_clickhouse=False, require_gmail_client_secrets=False)
    ollama = FakeOllamaResource("{}")

    config = prepare_attachment_ai_fallback(settings=settings, ollama=ollama, logger=FakeLogger())

    assert config is not None
    assert config.client is ollama
    assert ollama.ensure_model_calls == [("gemma4:e2b", True)]


def test_prepare_attachment_ai_fallback_disables_run_when_model_setup_fails(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    settings = load_settings(require_clickhouse=False, require_gmail_client_secrets=False)
    ollama = FakeOllamaResource("{}")
    ollama.ensure_model_error = RuntimeError("down")

    config = prepare_attachment_ai_fallback(settings=settings, ollama=ollama, logger=FakeLogger())

    assert config is None


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


def test_execute_gmail_request_retries_transient_network_errors() -> None:
    attempts = 0

    def flaky_request() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise BrokenPipeError("broken pipe")
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


def test_attachment_rows_for_message_uses_ai_fallback_for_images(monkeypatch) -> None:
    image_content = b"\x89PNG\r\n\x1a\n" + (b"fake screenshot bytes" * 1000)
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "2",
                    "filename": "receipt.png",
                    "mimeType": "image/png",
                    "body": {"data": _gmail_data(image_content), "size": len(image_content)},
                }
            ]
        },
    }

    ollama = FakeOllamaResource(
        json.dumps(
            {
                "summary": "Payment receipt.",
                "visible_text": ["Total $42.00", "Paid Apr 23, 2026"],
                "likely_document_type": "receipt",
                "useful_for_search": "payment receipt total",
            }
        )
    )

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=len(image_content) + 1,
        text_max_chars=1000,
        ai_fallback=AttachmentAiFallbackConfig(
            provider="ollama",
            base_url="http://127.0.0.1:11435",
            model="gemma4:e2b",
            timeout_seconds=30,
            pdf_max_pages=1,
            pull_model=True,
            client=ollama,
        ),
    )

    row = rows[0]
    assert row["text_extraction_status"] == "ai_ok"
    assert "Payment receipt" in row["text"]
    assert "Total $42.00" in row["text"]
    assert row["ai_provider"] == "ollama"
    assert row["ai_model"] == "gemma4:e2b"
    assert row["ai_prompt_version"] == ATTACHMENT_AI_PROMPT_VERSION
    assert row["ai_prompt"] == ATTACHMENT_AI_PROMPT
    assert row["ai_source_status"] == "unsupported"
    assert row["ai_elapsed_ms"] >= 0
    assert row["ai_processed_at"] > datetime(2026, 4, 23, tzinfo=UTC)
    assert ollama.generate_calls == [
        {
            "model": "gemma4:e2b",
            "prompt": ATTACHMENT_AI_PROMPT,
            "images": [image_content],
            "format": "json",
            "options": {"temperature": 0, "num_predict": 512},
            "think": False,
            "timeout_seconds": 30,
        }
    ]
    metadata = json.loads(row["text_extraction_error"])
    assert metadata["source_status"] == "unsupported"
    assert metadata["model"] == "gemma4:e2b"
    assert metadata["prompt_sha256"] == row["ai_prompt_sha256"]


def test_attachment_rows_for_message_ai_fallback_keeps_scene_and_visible_text() -> None:
    image_content = b"\x89PNG\r\n\x1a\n" + (b"fake classroom presentation photo bytes" * 1000)
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "2",
                    "filename": "classroom-presentation.png",
                    "mimeType": "image/png",
                    "body": {"data": _gmail_data(image_content), "size": len(image_content)},
                }
            ]
        },
    }

    ollama = FakeOllamaResource(
        json.dumps(
            {
                "is_useful": True,
                "scene_summary": "A woman presenting to children in a classroom in front of a projected slide.",
                "visible_text": ["TO SHARE OR NOT TO SHARE?", "Social media privacy"],
                "likely_document_type": "photo",
                "document_context": "classroom presentation photo",
                "search_keywords": "woman presenting children classroom TO SHARE OR NOT TO SHARE social media privacy",
            }
        )
    )

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=len(image_content) + 1,
        text_max_chars=1000,
        ai_fallback=AttachmentAiFallbackConfig(
            provider="ollama",
            base_url="http://127.0.0.1:11435",
            model="gemma4:e2b",
            timeout_seconds=30,
            pdf_max_pages=1,
            pull_model=True,
            client=ollama,
        ),
    )

    row = rows[0]
    assert row["text_extraction_status"] == "ai_ok"
    assert "Summary: A woman presenting to children in a classroom" in row["text"]
    assert "Visible text:" in row["text"]
    assert "TO SHARE OR NOT TO SHARE?" in row["text"]
    assert "Document context: classroom presentation photo" in row["text"]
    assert row["ai_prompt_version"] == ATTACHMENT_AI_PROMPT_VERSION


def test_attachment_rows_for_message_ai_fallback_formats_ocr_first_schema() -> None:
    image_content = b"\x89PNG\r\n\x1a\n" + (b"fake logo image bytes" * 1000)
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "2",
                    "filename": "ATT00001.png",
                    "mimeType": "image/png",
                    "body": {"data": _gmail_data(image_content), "size": len(image_content)},
                }
            ]
        },
    }

    ollama = FakeOllamaResource(
        json.dumps(
            {
                "is_useful": True,
                "document_type": "logo",
                "summary": "Common Sense Media logo with a green checkmark icon.",
                "visible_text": ["common sense media"],
                "entities": ["Common Sense Media"],
                "search_keywords": ["Common Sense Media", "green checkmark logo"],
                "uncertainties": [],
            }
        )
    )

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=len(image_content) + 1,
        text_max_chars=1000,
        ai_fallback=AttachmentAiFallbackConfig(
            provider="ollama",
            base_url="http://127.0.0.1:11435",
            model="gemma4:e2b",
            timeout_seconds=30,
            pdf_max_pages=1,
            pull_model=True,
            client=ollama,
        ),
    )

    row = rows[0]
    assert row["text_extraction_status"] == "ai_ok"
    assert "Document type: logo" in row["text"]
    assert "Summary: Common Sense Media logo" in row["text"]
    assert "Visible text:\ncommon sense media" in row["text"]
    assert "Entities: Common Sense Media" in row["text"]
    assert "Search keywords: Common Sense Media, green checkmark logo" in row["text"]


def test_format_attachment_ai_response_appends_supporting_ocr_text() -> None:
    formatted = format_attachment_ai_response(
        json.dumps(
            {
                "is_useful": True,
                "document_type": "logo",
                "summary": "A stylized Hack Club logo.",
                "visible_text": ["HACK", "THE"],
                "entities": ["Hack Club"],
                "search_keywords": ["Hack Club"],
                "uncertainties": [],
            }
        ),
        supporting_ocr_text="THE HACK\nSTRIKES BACK",
    )

    assert "Visible text:\nHACK\nTHE" in formatted
    assert "Deterministic OCR text:\nTHE HACK\nSTRIKES BACK" in formatted


def test_format_attachment_ai_response_keeps_supporting_ocr_for_wrong_non_useful_flag() -> None:
    formatted = format_attachment_ai_response(
        json.dumps(
            {
                "is_useful": False,
                "document_type": "unknown",
                "summary": "No useful visible text.",
                "visible_text": [],
                "entities": [],
                "search_keywords": [],
                "uncertainties": [],
            }
        ),
        supporting_ocr_text="THE HACK STRIKES BACK",
    )

    assert formatted == "AI attachment extraction\n\nDeterministic OCR text:\nTHE HACK STRIKES BACK"


def test_attachment_ai_supporting_ocr_filters_short_garbage(monkeypatch) -> None:
    def fake_run(*_args, **_kwargs):
        return subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=b"\xe2\x80\x9c(YQ +\n\xe2\x80\x94a\nGS\nTHE HACK\n81-2908499\n",
            stderr=b"",
        )

    monkeypatch.setattr("personal_data_warehouse.gmail_sync.shutil.which", lambda _name: "/usr/bin/tesseract")
    monkeypatch.setattr("personal_data_warehouse.gmail_sync.subprocess.run", fake_run)

    assert (
        attachment_ai_supporting_ocr_text(images=[b"not really an image"], timeout_seconds=30)
        == "THE HACK\n81-2908499"
    )


def test_attachment_rows_for_message_records_ai_empty_for_non_useful_images() -> None:
    image_content = b"\x89PNG\r\n\x1a\n" + (b"fake blank image bytes" * 1000)
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "2",
                    "filename": "placeholder.png",
                    "mimeType": "image/png",
                    "body": {"data": _gmail_data(image_content), "size": len(image_content)},
                }
            ]
        },
    }

    ollama = FakeOllamaResource(
        json.dumps(
            {
                "is_useful": False,
                "summary": "",
                "visible_text": "",
                "likely_document_type": "",
                "useful_for_search": "",
            }
        )
    )

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=len(image_content) + 1,
        text_max_chars=1000,
        ai_fallback=AttachmentAiFallbackConfig(
            provider="ollama",
            base_url="http://127.0.0.1:11435",
            model="gemma4:e2b",
            timeout_seconds=30,
            pdf_max_pages=1,
            pull_model=True,
            client=ollama,
        ),
    )

    row = rows[0]
    assert row["text_extraction_status"] == "ai_empty"
    assert row["text"] == ""
    assert row["ai_provider"] == "ollama"
    assert row["ai_model"] == "gemma4:e2b"
    assert row["ai_prompt_version"] == ATTACHMENT_AI_PROMPT_VERSION
    assert row["ai_prompt"] == ATTACHMENT_AI_PROMPT
    assert row["ai_source_status"] == "unsupported"
    metadata = json.loads(row["text_extraction_error"])
    assert metadata["prompt_version"] == ATTACHMENT_AI_PROMPT_VERSION
    assert metadata["prompt_sha256"] == row["ai_prompt_sha256"]


def test_attachment_rows_for_message_drops_non_useful_visual_summary_without_text() -> None:
    image_content = b"\x89PNG\r\n\x1a\n" + (b"fake decorative image bytes" * 1000)
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "2",
                    "filename": "decorative.png",
                    "mimeType": "image/png",
                    "body": {"data": _gmail_data(image_content), "size": len(image_content)},
                }
            ]
        },
    }

    ollama = FakeOllamaResource(
        json.dumps(
            {
                "is_useful": False,
                "document_type": "unknown",
                "summary": "A decorative gift box illustration.",
                "visible_text": ["unknown"],
                "entities": ["gift", "ribbon"],
                "search_keywords": ["gift box", "ribbon graphic"],
                "uncertainties": ["No readable text is visible."],
            }
        )
    )

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=len(image_content) + 1,
        text_max_chars=1000,
        ai_fallback=AttachmentAiFallbackConfig(
            provider="ollama",
            base_url="http://127.0.0.1:11435",
            model="gemma4:e2b",
            timeout_seconds=30,
            pdf_max_pages=1,
            pull_model=True,
            client=ollama,
        ),
    )

    assert rows[0]["text_extraction_status"] == "ai_empty"
    assert rows[0]["text"] == ""


def test_attachment_rows_for_message_keeps_content_when_ai_usefulness_flag_is_wrong() -> None:
    image_content = b"\x89PNG\r\n\x1a\n" + (b"fake receipt image bytes" * 1000)
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "2",
                    "filename": "receipt.png",
                    "mimeType": "image/png",
                    "body": {"data": _gmail_data(image_content), "size": len(image_content)},
                }
            ]
        },
    }

    ollama = FakeOllamaResource(
        json.dumps(
            {
                "is_useful": False,
                "summary": "Coffee receipt.",
                "visible_text": "Fairgrounds Coffee & Tea",
                "likely_document_type": "receipt",
                "useful_for_search": "coffee receipt",
            }
        )
    )

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=len(image_content) + 1,
        text_max_chars=1000,
        ai_fallback=AttachmentAiFallbackConfig(
            provider="ollama",
            base_url="http://127.0.0.1:11435",
            model="gemma4:e2b",
            timeout_seconds=30,
            pdf_max_pages=1,
            pull_model=True,
            client=ollama,
        ),
    )

    row = rows[0]
    assert row["text_extraction_status"] == "ai_ok"
    assert "Coffee receipt" in row["text"]
    assert "Fairgrounds Coffee & Tea" in row["text"]
    assert row["ai_prompt_version"] == ATTACHMENT_AI_PROMPT_VERSION


def test_attachment_rows_for_message_parses_json_string_wrapped_ai_response() -> None:
    image_content = b"\x89PNG\r\n\x1a\n" + (b"fake placeholder image bytes" * 1000)
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "2",
                    "filename": "placeholder.png",
                    "mimeType": "image/png",
                    "body": {"data": _gmail_data(image_content), "size": len(image_content)},
                }
            ]
        },
    }

    ollama = FakeOllamaResource(
        json.dumps(
            "```json\n"
            + json.dumps(
                {
                    "is_useful": False,
                    "summary": "",
                    "visible_text": "",
                    "likely_document_type": "",
                    "useful_for_search": "",
                }
            )
            + "\n```"
        )
    )

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=len(image_content) + 1,
        text_max_chars=1000,
        ai_fallback=AttachmentAiFallbackConfig(
            provider="ollama",
            base_url="http://127.0.0.1:11435",
            model="gemma4:e2b",
            timeout_seconds=30,
            pdf_max_pages=1,
            pull_model=True,
            client=ollama,
        ),
    )

    assert rows[0]["text_extraction_status"] == "ai_empty"
    assert rows[0]["text"] == ""


def test_attachment_rows_for_message_skips_tiny_images_for_ai_fallback() -> None:
    image_content = b"\x89PNG\r\n\x1a\nsmall"
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "2",
                    "filename": "tracking.png",
                    "mimeType": "image/png",
                    "body": {"data": _gmail_data(image_content), "size": len(image_content)},
                }
            ]
        },
    }
    ollama = FakeOllamaResource("{}")

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=len(image_content) + 1,
        text_max_chars=1000,
        ai_fallback=AttachmentAiFallbackConfig(
            provider="ollama",
            base_url="http://127.0.0.1:11435",
            model="gemma4:e2b",
            timeout_seconds=30,
            pdf_max_pages=1,
            pull_model=True,
            client=ollama,
        ),
    )

    assert rows[0]["text_extraction_status"] == "unsupported"
    assert rows[0]["ai_model"] == ""
    assert ollama.generate_calls == []


def test_attachment_rows_for_message_filters_generic_ai_placeholders() -> None:
    image_content = b"\x89PNG\r\n\x1a\n" + (b"fake placeholder image bytes" * 1000)
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "2",
                    "filename": "placeholder.png",
                    "mimeType": "image/png",
                    "body": {"data": _gmail_data(image_content), "size": len(image_content)},
                }
            ]
        },
    }
    ollama = FakeOllamaResource(
        json.dumps(
            {
                "is_useful": True,
                "summary": "The image appears to be a placeholder or a visual representation of a Gmail attachment without any discernible content.",
                "visible_text": "Gmail attachment",
                "likely_document_type": "unknown",
                "useful_for_search": "Gmail attachment",
            }
        )
    )

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=len(image_content) + 1,
        text_max_chars=1000,
        ai_fallback=AttachmentAiFallbackConfig(
            provider="ollama",
            base_url="http://127.0.0.1:11435",
            model="gemma4:e2b",
            timeout_seconds=30,
            pdf_max_pages=1,
            pull_model=True,
            client=ollama,
        ),
    )

    assert rows[0]["text_extraction_status"] == "ai_empty"
    assert rows[0]["text"] == ""
    assert rows[0]["ai_prompt_version"] == ATTACHMENT_AI_PROMPT_VERSION


def test_attachment_ai_call_timeout_returns_control() -> None:
    started_at = time.monotonic()

    def slow_call() -> str:
        time.sleep(1)
        return "too late"

    try:
        run_attachment_ai_call_with_timeout(slow_call, timeout_seconds=0.01)
    except TimeoutError as exc:
        assert "timed out" in str(exc)
    else:
        raise AssertionError("expected timeout")

    assert time.monotonic() - started_at < 0.5


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


def test_extract_attachment_text_skips_unreadable_zip_members() -> None:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("broken.pdf", b"%PDF-1.7\nnot a complete pdf")
        archive.writestr("notes.txt", "Readable member")

    extraction = extract_attachment_text(
        content=buffer.getvalue(),
        mime_type="application/zip",
        filename="mixed.zip",
        max_chars=1000,
    )

    assert extraction.status == "ok"
    assert "Readable member" in extraction.text
    assert "broken.pdf" not in extraction.text


def test_extract_attachment_text_marks_encrypted_pdf() -> None:
    from pypdf import PdfWriter

    buffer = BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    writer.encrypt("secret")
    writer.write(buffer)

    extraction = extract_attachment_text(
        content=buffer.getvalue(),
        mime_type="application/pdf",
        filename="tax.pdf",
        max_chars=1000,
    )

    assert extraction.status == "encrypted"
    assert "PDF is encrypted" in extraction.error


def test_extract_attachment_text_marks_mislabeled_pdf_as_unsupported() -> None:
    extraction = extract_attachment_text(
        content=b"\x89PNG\r\n\x1a\n",
        mime_type="image/png",
        filename="printFriendly.pdf",
        max_chars=1000,
    )

    assert extraction.status == "unsupported"
    assert "does not contain PDF data" in extraction.error


def test_extract_attachment_text_marks_invalid_office_document() -> None:
    extraction = extract_attachment_text(
        content=b"\xd0\xcf\x11\xe0\x00not-open-xml",
        mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename="form.docx",
        max_chars=1000,
    )

    assert extraction.status == "invalid_office_document"
    assert "not a valid zip container" in extraction.error


def test_extract_attachment_text_marks_invalid_zip_archive() -> None:
    extraction = extract_attachment_text(
        content=b"\x00not-a-zip",
        mime_type="application/zip",
        filename="archive.zip",
        max_chars=1000,
    )

    assert extraction.status == "invalid_archive"
    assert "not a valid archive" in extraction.error


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
