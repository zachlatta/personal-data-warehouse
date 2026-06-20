from __future__ import annotations

import base64
from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
import http.server
from io import BytesIO
import json
import socketserver
import subprocess
from pathlib import Path
import threading
import time
import urllib.request
from unittest.mock import patch
import zipfile

from googleapiclient.errors import HttpError
from httplib2 import Response
from PIL import Image
import pytest
from personal_data_warehouse.config import GOOGLE_DRIVE_SCOPE, load_settings
from personal_data_warehouse.defs.gmail_sync import (
    gmail_mailbox_sync_every_fifteen_minutes,
)
from personal_data_warehouse.gmail_auth import update_env_file
from personal_data_warehouse.gmail_sync import (
    AttachmentTextExtraction,
    WarehouseAttachmentEnrichmentCache,
    attachment_parts_from_message,
    attachment_rows_for_message,
    collapsed_message_body_to_markdown,
    count_attachments_stored,
    decode_base64url,
    deleted_attachment_row,
    deleted_attachment_rows,
    GMAIL_ATTACHMENT_STORAGE_KIND,
    GMAIL_ATTACHMENT_STORAGE_PREFIX,
    gmail_attachment_extension,
    gmail_attachment_object_key,
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


_ALICE_ENV_VARS = (
    "ALICE_VOICE_RECORDINGS_ACCOUNT",
    "ALICE_API_KEY_ID",
    "ALICE_API_SECRET_KEY",
    "ALICE_VOICE_RECORDINGS_GOOGLE_DRIVE_ACCOUNT",
    "ALICE_VOICE_RECORDINGS_GOOGLE_DRIVE_FOLDER_ID",
)


@pytest.fixture(autouse=True)
def _isolate_unrelated_alice_env(monkeypatch):
    for name in _ALICE_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


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


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.put_calls: list[dict] = []

    def put_file(
        self,
        *,
        path,
        object_key: str,
        content_sha256: str,
        content_type: str,
        skip_existing_check: bool = False,
        app_properties: dict | None = None,
        kind: str | None = None,
    ) -> dict:
        self.put_calls.append(
            {
                "object_key": object_key,
                "content_sha256": content_sha256,
                "content_type": content_type,
                "app_properties": dict(app_properties or {}),
                "kind": kind,
                "content": Path(path).read_bytes(),
            }
        )
        if self.fail:
            raise RuntimeError("drive upload boom")
        return {
            "storage_backend": self.backend,
            "storage_key": object_key,
            "storage_file_id": f"file-{content_sha256[:8]}",
            "storage_url": f"https://drive.example/{content_sha256[:8]}",
        }

    def has_object(self, *, kind: str, key: str, value: str) -> bool:
        return False


class FakeAttachmentBackfillWarehouse:
    def __init__(self, messages: list[dict[str, object]], existing_keys=None) -> None:
        self.messages = messages
        self.existing_keys = set(existing_keys or set())
        self.attachment_rows: list[dict[str, object]] = []
        self.state_rows: list[dict[str, object]] = []
        self.candidate_requests: list[dict[str, object]] = []

    def load_attachment_backfill_candidate_messages(
        self,
        *,
        account: str,
        limit: int,
        include_storage_pending: bool = False,
        storage_max_bytes: int = 0,
    ):
        self.candidate_requests.append(
            {
                "account": account,
                "limit": limit,
                "include_storage_pending": include_storage_pending,
                "storage_max_bytes": storage_max_bytes,
            }
        )
        return self.messages[:limit]

    def existing_attachment_keys(self, *, account: str, message_ids: list[str]):
        return self.existing_keys

    def insert_attachments(self, rows: list[dict[str, object]]) -> None:
        self.attachment_rows.extend(rows)

    def insert_attachment_backfill_state(self, rows: list[dict[str, object]]) -> None:
        self.state_rows.extend(rows)


class FakeAttachmentEnrichmentCache:
    def __init__(self, cached: dict[str, AttachmentTextExtraction] | None = None) -> None:
        self.cached = dict(cached or {})
        self.get_calls: list[str] = []
        self.put_calls: list[tuple[str, AttachmentTextExtraction]] = []

    def get(self, content_sha256: str) -> AttachmentTextExtraction | None:
        self.get_calls.append(content_sha256)
        return self.cached.get(content_sha256)

    def put(self, content_sha256: str, extraction: AttachmentTextExtraction) -> None:
        self.put_calls.append((content_sha256, extraction))
        self.cached[content_sha256] = extraction


class FakeWarehouseAttachmentEnrichments:
    def __init__(self, rows: dict[tuple[str, str, str, str], dict[str, object]] | None = None) -> None:
        self.rows = dict(rows or {})
        self.load_calls: list[dict[str, object]] = []
        self.inserted_rows: list[dict[str, object]] = []

    def load_attachment_enrichments(
        self,
        *,
        content_sha256s: list[str],
        ai_provider: str,
        ai_model: str,
        ai_prompt_version: str,
    ) -> dict[str, dict[str, object]]:
        self.load_calls.append(
            {
                "content_sha256s": content_sha256s,
                "ai_provider": ai_provider,
                "ai_model": ai_model,
                "ai_prompt_version": ai_prompt_version,
            }
        )
        return {
            content_sha256: row
            for content_sha256 in content_sha256s
            if (
                row := self.rows.get((content_sha256, ai_provider, ai_model, ai_prompt_version))
            )
            is not None
        }

    def insert_attachment_enrichments(self, rows: list[dict[str, object]]) -> None:
        self.inserted_rows.extend(rows)


def test_load_settings_accepts_oauth_client_secrets_json_env(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_SECRETS_JSON", '{"installed":{}}')

    settings = load_settings(require_postgres=False)

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

    settings = load_settings(require_postgres=False, require_gmail_client_secrets=True)

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

    settings = load_settings(require_postgres=False, require_gmail_client_secrets=True)

    assert settings.google_oauth_client_secrets_json_for_email("zach@zachlatta.com") is None


def test_load_settings_can_skip_oauth_client_secrets_for_sync_runtime(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_SECRETS_JSON", "")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_SECRETS_JSON_B64", "")

    settings = load_settings(require_postgres=False, require_gmail_client_secrets=False)

    assert settings.gmail_accounts[0].email_address == "zach@hackclub.com"
    assert settings.gmail_oauth_client_secrets_json is None


def test_load_settings_accepts_gmail_attachment_limits(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("GMAIL_ATTACHMENT_MAX_BYTES", "1024")
    monkeypatch.setenv("GMAIL_ATTACHMENT_TEXT_MAX_CHARS", "2048")
    monkeypatch.setenv("GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE", "25")

    settings = load_settings(require_postgres=False, require_gmail_client_secrets=False)

    assert settings.gmail_attachment_max_bytes == 1024
    assert settings.gmail_attachment_text_max_chars == 2048
    assert settings.gmail_attachment_backfill_batch_size == 25


def test_load_settings_disables_attachment_storage_without_folder(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.delenv("GMAIL_ATTACHMENT_GOOGLE_DRIVE_FOLDER_ID", raising=False)
    monkeypatch.delenv("VOICE_MEMOS_ACCOUNT", raising=False)
    monkeypatch.delenv("VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID", raising=False)
    monkeypatch.delenv("VOICE_MEMOS_DRIVE_FOLDER_ID", raising=False)
    # Isolate the attachment-storage scope contribution from the (default-on)
    # Google Drive source, which adds the Drive scope independently.
    monkeypatch.setenv("GOOGLE_DRIVE_SOURCE_ENABLED", "0")

    settings = load_settings(require_postgres=False, require_gmail_client_secrets=False)

    assert settings.gmail_attachment_storage_backend == "google_drive"
    assert settings.gmail_attachment_google_drive_folder_id == ""
    assert settings.gmail_attachment_storage_enabled is False
    assert GOOGLE_DRIVE_SCOPE not in settings.google_scopes


def test_load_settings_enables_attachment_storage_with_folder(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("GMAIL_ATTACHMENT_GOOGLE_DRIVE_FOLDER_ID", "folder-123")

    settings = load_settings(require_postgres=False, require_gmail_client_secrets=False)

    assert settings.gmail_attachment_storage_enabled is True
    assert settings.gmail_attachment_google_drive_folder_id == "folder-123"
    assert GOOGLE_DRIVE_SCOPE in settings.google_scopes


def test_load_settings_rejects_unknown_attachment_storage_backend(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("GMAIL_ATTACHMENT_STORAGE_BACKEND", "s3")

    try:
        load_settings(require_postgres=False, require_gmail_client_secrets=False)
    except ValueError as exc:
        assert "GMAIL_ATTACHMENT_STORAGE_BACKEND" in str(exc)
    else:
        raise AssertionError("expected ValueError for unknown storage backend")


def test_load_settings_attachment_storage_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("GMAIL_ATTACHMENT_STORAGE_BACKEND", "none")
    monkeypatch.setenv("GMAIL_ATTACHMENT_GOOGLE_DRIVE_FOLDER_ID", "folder-123")
    monkeypatch.delenv("VOICE_MEMOS_ACCOUNT", raising=False)
    monkeypatch.delenv("VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID", raising=False)
    monkeypatch.delenv("VOICE_MEMOS_DRIVE_FOLDER_ID", raising=False)
    # Isolate from the default-on Google Drive source, which adds the Drive
    # scope independently of attachment storage.
    monkeypatch.setenv("GOOGLE_DRIVE_SOURCE_ENABLED", "0")

    settings = load_settings(require_postgres=False, require_gmail_client_secrets=False)

    assert settings.gmail_attachment_storage_enabled is False
    assert GOOGLE_DRIVE_SCOPE not in settings.google_scopes


def test_load_settings_normalizes_attachment_storage_backend(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("GMAIL_ATTACHMENT_STORAGE_BACKEND", "GOOGLE_DRIVE")
    monkeypatch.setenv("GMAIL_ATTACHMENT_GOOGLE_DRIVE_FOLDER_ID", "folder-123")

    settings = load_settings(require_postgres=False, require_gmail_client_secrets=False)

    assert settings.gmail_attachment_storage_backend == "google_drive"
    assert settings.gmail_attachment_storage_enabled is True


def test_load_settings_reads_attachment_storage_drive_account(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("GMAIL_ATTACHMENT_GOOGLE_DRIVE_FOLDER_ID", "folder-123")
    monkeypatch.setenv("GMAIL_ATTACHMENT_GOOGLE_DRIVE_ACCOUNT", "Zach@ZachLatta.com")

    settings = load_settings(require_postgres=False, require_gmail_client_secrets=False)

    assert settings.gmail_attachment_google_drive_account == "zach@zachlatta.com"


def test_load_settings_shares_voice_memos_object_store_by_default(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com,zach@zachlatta.com")
    monkeypatch.delenv("GMAIL_ATTACHMENT_GOOGLE_DRIVE_FOLDER_ID", raising=False)
    monkeypatch.delenv("GMAIL_ATTACHMENT_GOOGLE_DRIVE_ACCOUNT", raising=False)
    monkeypatch.setenv("VOICE_MEMOS_ACCOUNT", "zach@zachlatta.com")
    monkeypatch.setenv("VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID", "shared-folder-id")

    settings = load_settings(require_postgres=False, require_gmail_client_secrets=False)

    # Gmail attachments land in the same Drive store as voice memos by default.
    assert settings.gmail_attachment_google_drive_folder_id == "shared-folder-id"
    assert settings.gmail_attachment_google_drive_account == "zach@zachlatta.com"
    assert settings.gmail_attachment_storage_enabled is True


def test_load_settings_attachment_folder_overrides_shared_store(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@hackclub.com")
    monkeypatch.setenv("VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID", "shared-folder-id")
    monkeypatch.setenv("GMAIL_ATTACHMENT_GOOGLE_DRIVE_FOLDER_ID", "gmail-only-folder")

    settings = load_settings(require_postgres=False, require_gmail_client_secrets=False)

    assert settings.gmail_attachment_google_drive_folder_id == "gmail-only-folder"


def test_gmail_token_json_from_env_accepts_base64(monkeypatch) -> None:
    token_json = json.dumps({"token": "access-token"})
    encoded_token = base64.b64encode(token_json.encode("utf-8")).decode("ascii")
    monkeypatch.setenv("GMAIL_ZACH_HACKCLUB_COM_TOKEN_JSON_B64", encoded_token)

    assert gmail_token_json_from_env("zach@hackclub.com") == token_json


def test_gmail_sync_schedule_runs_every_fifteen_minutes_by_default() -> None:
    assert gmail_mailbox_sync_every_fifteen_minutes.cron_schedule == "*/15 * * * *"
    assert gmail_mailbox_sync_every_fifteen_minutes.default_status.value == "RUNNING"


def test_google_auth_update_env_file_replaces_and_appends_values(tmp_path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("POSTGRES_DATABASE_URL=postgresql://example\nGMAIL_ZACH_EXAMPLE_COM_TOKEN_JSON_B64=old\n")

    update_env_file(
        env_path,
        {
            "GOOGLE_ZACH_EXAMPLE_COM_TOKEN_JSON_B64": "new",
            "GMAIL_ZACH_EXAMPLE_COM_TOKEN_JSON_B64": "new",
        },
    )

    assert env_path.read_text().splitlines() == [
        "POSTGRES_DATABASE_URL=postgresql://example",
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


def _attachment_message(content: bytes, *, filename: str = "notes.txt", mime_type: str = "text/plain") -> dict:
    return {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "2",
                    "filename": filename,
                    "mimeType": mime_type,
                    "body": {"data": _gmail_data(content), "size": len(content)},
                }
            ]
        },
    }


def test_attachment_part_to_row_uploads_blob_to_object_store() -> None:
    content = b"attachment blob bytes"
    object_store = FakeObjectStore()
    message = _attachment_message(content)

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=100,
        text_max_chars=100,
        object_store=object_store,
    )

    row = rows[0]
    sha = hashlib.sha256(content).hexdigest()
    assert row["storage_backend"] == "google_drive"
    assert row["storage_status"] == "stored"
    assert row["storage_file_id"] == f"file-{sha[:8]}"
    assert row["storage_url"] == f"https://drive.example/{sha[:8]}"
    assert row["storage_key"].startswith(f"{GMAIL_ATTACHMENT_STORAGE_PREFIX}/library/2024/04/2024-04-23-")
    assert row["storage_key"].endswith(".txt")

    assert len(object_store.put_calls) == 1
    call = object_store.put_calls[0]
    assert call["content"] == content
    assert call["content_sha256"] == sha
    assert call["kind"] == GMAIL_ATTACHMENT_STORAGE_KIND
    assert call["content_type"] == "text/plain"
    assert call["app_properties"]["gmail_account"] == "zach@example.com"
    assert call["app_properties"]["gmail_message_id"] == "gmail-id"
    assert call["app_properties"]["gmail_part_id"] == "2"


def test_attachment_part_to_row_records_upload_error() -> None:
    content = b"attachment blob bytes"
    object_store = FakeObjectStore(fail=True)
    message = _attachment_message(content)

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=100,
        text_max_chars=100,
        object_store=object_store,
    )

    row = rows[0]
    assert row["storage_status"] == "upload_error"
    assert row["storage_backend"] == ""
    assert row["storage_key"] == ""
    assert row["storage_file_id"] == ""
    assert row["storage_url"] == ""
    # Text extraction still succeeds even when the blob upload fails.
    assert row["text_extraction_status"] == "ok"
    assert row["content_sha256"] == hashlib.sha256(content).hexdigest()


def test_attachment_part_to_row_skips_upload_without_object_store() -> None:
    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=_attachment_message(b"hello"),
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=100,
        text_max_chars=100,
    )

    row = rows[0]
    assert row["storage_status"] == ""
    assert row["storage_backend"] == ""
    assert row["storage_key"] == ""


def test_attachment_part_to_row_does_not_upload_oversized_attachment() -> None:
    content = b"x" * 200
    object_store = FakeObjectStore()

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=_attachment_message(content),
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=10,
        text_max_chars=100,
        object_store=object_store,
    )

    assert object_store.put_calls == []
    assert rows[0]["storage_status"] == ""
    assert rows[0]["text_extraction_status"] == "too_large"


def test_deleted_attachment_row_has_empty_storage_fields() -> None:
    row = deleted_attachment_row(
        account="zach@example.com",
        message_id="gmail-id",
        part_id="2",
        attachment_id="",
        filename="notes.txt",
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        history_id=7,
    )

    assert row["storage_backend"] == ""
    assert row["storage_key"] == ""
    assert row["storage_file_id"] == ""
    assert row["storage_url"] == ""
    assert row["storage_status"] == ""


def test_count_attachments_stored_counts_only_stored_rows() -> None:
    rows = [
        {"storage_status": "stored"},
        {"storage_status": "upload_error"},
        {"storage_status": ""},
        {"storage_status": "stored"},
    ]
    assert count_attachments_stored(rows) == 2


def test_gmail_attachment_object_key_uses_internal_date_and_sha() -> None:
    key = gmail_attachment_object_key(
        internal_date=datetime(2026, 4, 23, 9, 30, tzinfo=UTC),
        content_sha256="abc123",
        filename="report.PDF",
        mime_type="application/pdf",
    )
    assert key == f"{GMAIL_ATTACHMENT_STORAGE_PREFIX}/library/2026/04/2026-04-23-abc123.pdf"


def test_gmail_attachment_extension_falls_back_to_mime_type() -> None:
    assert gmail_attachment_extension(filename="no-extension", mime_type="application/pdf") == ".pdf"
    assert gmail_attachment_extension(filename="photo.JPG", mime_type="image/jpeg") == ".jpg"
    assert gmail_attachment_extension(filename="blob", mime_type="") == ""


def test_attachment_rows_for_message_reuses_cached_text_enrichment_by_hash_without_ai() -> None:
    content = b"actual attachment text that should not be reparsed"
    content_sha256 = hashlib.sha256(content).hexdigest()
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
                }
            ]
        },
    }
    cached = AttachmentTextExtraction(text="cached text enrichment", status="ok")
    cache = FakeAttachmentEnrichmentCache({content_sha256: cached})

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=len(content) + 1,
        text_max_chars=1000,
        enrichment_cache=cache,
    )

    assert rows[0]["content_sha256"] == content_sha256
    assert rows[0]["text_extraction_status"] == "ok"
    assert rows[0]["text"] == "cached text enrichment"
    assert cache.get_calls == [content_sha256]
    assert cache.put_calls == []


def test_attachment_rows_for_message_caches_new_text_enrichment_for_duplicate_hashes_without_ai() -> None:
    content = b"duplicate plain text attachment"
    content_sha256 = hashlib.sha256(content).hexdigest()
    message = {
        "id": "gmail-id",
        "threadId": "thread-id",
        "historyId": "42",
        "internalDate": "1713875400000",
        "payload": {
            "parts": [
                {
                    "partId": "1",
                    "filename": "one.txt",
                    "mimeType": "text/plain",
                    "body": {"data": _gmail_data(content), "size": len(content)},
                },
                {
                    "partId": "2",
                    "filename": "two.txt",
                    "mimeType": "text/plain",
                    "body": {"data": _gmail_data(content), "size": len(content)},
                },
            ]
        },
    }
    cache = FakeAttachmentEnrichmentCache()

    rows = attachment_rows_for_message(
        account="zach@example.com",
        service=FakeGmailAttachmentService({}),
        message=message,
        synced_at=datetime(2026, 4, 23, tzinfo=UTC),
        existing_keys=set(),
        max_bytes=len(content) + 1,
        text_max_chars=1000,
        enrichment_cache=cache,
    )

    assert [row["content_sha256"] for row in rows] == [content_sha256, content_sha256]
    assert [row["text"] for row in rows] == ["duplicate plain text attachment", "duplicate plain text attachment"]
    assert [row["text_extraction_status"] for row in rows] == ["ok", "ok"]
    assert cache.get_calls == [content_sha256, content_sha256]
    assert len(cache.put_calls) == 1
    assert cache.put_calls[0][0] == content_sha256


def test_warehouse_attachment_enrichment_cache_writes_text_extraction_to_enrichment_table() -> None:
    warehouse = FakeWarehouseAttachmentEnrichments()
    cache = WarehouseAttachmentEnrichmentCache(warehouse=warehouse)
    extraction = AttachmentTextExtraction(text="cached parsed text", status="ok")

    cache.put("hash-1", extraction)

    assert len(warehouse.inserted_rows) == 1
    inserted = warehouse.inserted_rows[0]
    assert inserted["content_sha256"] == "hash-1"
    assert inserted["text"] == "cached parsed text"
    assert inserted["text_extraction_status"] == "ok"
    assert inserted["ai_provider"] == ""
    assert inserted["ai_model"] == ""
    assert inserted["ai_prompt_version"] == ""


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
    settings = load_settings(require_postgres=False, require_gmail_client_secrets=False)
    runner = GmailSyncRunner(settings=settings, warehouse=warehouse, logger=FakeLogger())

    candidates, rows_written, text_chars, stored = runner._backfill_attachment_candidates(
        account=settings.gmail_accounts[0],
        service=FakeGmailAttachmentService({}),
    )

    assert candidates == 1
    assert rows_written == 1
    assert text_chars == len("backfill text")
    assert stored == 0
    assert warehouse.attachment_rows[0]["filename"] == "notes.txt"
    assert warehouse.attachment_rows[0]["text"] == "backfill text"
    assert warehouse.candidate_requests[0]["include_storage_pending"] is False
    assert warehouse.state_rows[0]["message_id"] == "gmail-id"
    assert warehouse.state_rows[0]["status"] == "ok"
    assert warehouse.state_rows[0]["attachment_rows_written"] == 1
    assert warehouse.state_rows[0]["ai_provider"] == ""
    assert warehouse.state_rows[0]["ai_model"] == ""
    assert warehouse.state_rows[0]["ai_prompt_version"] == ""


def test_runner_backfill_stores_blobs_for_storage_pending_candidates(monkeypatch) -> None:
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
    # Already synced/enriched on a prior run, so this message is only a candidate
    # because its blob was never stored to Drive (e.g. processed before Drive
    # storage shipped). force_reprocess must still re-store the blob.
    warehouse = FakeAttachmentBackfillWarehouse(
        [message],
        existing_keys={("gmail-id", "2", "notes.txt")},
    )
    settings = load_settings(require_postgres=False, require_gmail_client_secrets=False)
    runner = GmailSyncRunner(settings=settings, warehouse=warehouse, logger=FakeLogger())
    object_store = FakeObjectStore()

    candidates, rows_written, _text_chars, stored = runner._backfill_attachment_candidates(
        account=settings.gmail_accounts[0],
        service=FakeGmailAttachmentService({}),
        object_store=object_store,
    )

    assert candidates == 1
    assert rows_written == 1
    assert stored == 1
    assert len(object_store.put_calls) == 1
    assert warehouse.attachment_rows[0]["storage_status"] == "stored"
    request = warehouse.candidate_requests[0]
    assert request["include_storage_pending"] is True
    assert request["storage_max_bytes"] == settings.gmail_attachment_max_bytes


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
    settings = load_settings(require_postgres=False, require_gmail_client_secrets=False)
    runner = GmailSyncRunner(settings=settings, warehouse=warehouse, logger=FakeLogger())

    candidates, rows_written, text_chars, stored = runner._backfill_attachment_candidates(
        account=settings.gmail_accounts[0],
        service=FakeGmailAttachmentService({}),
    )

    assert candidates == 1
    assert rows_written == 0
    assert text_chars == 0
    assert stored == 0
    assert warehouse.attachment_rows == []
    assert warehouse.state_rows[0]["status"] == "ok"
    assert warehouse.state_rows[0]["attachment_rows_written"] == 0


def test_runner_backfills_attachment_candidates_in_parallel(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    monkeypatch.setenv("GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE", "10")
    monkeypatch.setenv("GMAIL_ATTACHMENT_BACKFILL_CONCURRENCY", "4")
    messages = [
        {
            "id": f"gmail-id-{index}",
            "threadId": "thread-id",
            "historyId": "42",
            "internalDate": "1713875400000",
            "payload": {
                "parts": [
                    {
                        "partId": "2",
                        "filename": f"notes-{index}.txt",
                        "mimeType": "text/plain",
                        "body": {"data": _gmail_data(f"backfill {index}".encode()), "size": 11},
                    }
                ]
            },
        }
        for index in range(5)
    ]
    existing_keys = {(f"gmail-id-{index}", "2", f"notes-{index}.txt") for index in range(5)}
    warehouse = FakeAttachmentBackfillWarehouse(messages, existing_keys=existing_keys)
    object_store = FakeObjectStore()
    settings = load_settings(require_postgres=False, require_gmail_client_secrets=False)
    runner = GmailSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=FakeLogger(),
        attachment_object_store_factory=lambda account: object_store,
    )

    opened_calls: list[bool] = []
    monkeypatch.setattr(runner, "_supports_parallel_backfill", lambda: True)
    monkeypatch.setattr(runner, "_open_worker_warehouse", lambda: opened_calls.append(True) or warehouse)
    monkeypatch.setattr(
        "personal_data_warehouse.gmail_sync.build_gmail_service",
        lambda *, account, settings: FakeGmailAttachmentService({}),
    )

    candidates, rows_written, _text_chars, stored = runner._backfill_attachment_candidates(
        account=settings.gmail_accounts[0],
        service=FakeGmailAttachmentService({}),
        object_store=object_store,
    )

    assert opened_calls, "parallel path should open per-worker warehouses"
    assert candidates == 5
    assert rows_written == 5
    assert stored == 5
    assert len(object_store.put_calls) == 5
    assert len(warehouse.attachment_rows) == 5
    assert all(row["storage_status"] == "stored" for row in warehouse.attachment_rows)
    assert len(warehouse.state_rows) == 5
    assert {row["message_id"] for row in warehouse.state_rows} == {f"gmail-id-{index}" for index in range(5)}
    assert all(row["status"] == "ok" for row in warehouse.state_rows)


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
                {"name": "From", "value": "Example Sender <sender@example.com>"},
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
    assert row["from_address"] == "sender@example.com"
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
