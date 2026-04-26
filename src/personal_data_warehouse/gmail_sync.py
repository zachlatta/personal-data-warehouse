from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
import base64
import fcntl
import hashlib
import json
import os
import queue
import re
import ssl
from email.utils import getaddresses, parseaddr
from io import BytesIO
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
from typing import Callable, Protocol, TypeVar
import urllib.error
import urllib.request
import zipfile
import xml.etree.ElementTree as ET

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from markdownify import markdownify as html_to_markdown
import warnings
import zlib

from personal_data_warehouse.clickhouse import ClickHouseWarehouse, SyncState
from personal_data_warehouse.config import GmailAccount, Settings, env_slug
from personal_data_warehouse.google_auth import google_token_json_from_env, load_google_credentials

EPOCH_UTC = datetime.fromtimestamp(0, tz=UTC)
DEFAULT_GMAIL_SYNC_LOCK_PATH = Path(tempfile.gettempdir()) / "personal-data-warehouse-gmail-sync.lock"
GMAIL_SYNC_POSTGRES_LOCK_ID = 7_403_111_836
ZIP_MIME_TYPES = {"application/zip", "application/x-zip-compressed"}
ZIP_EXTENSIONS = {".zip"}
ZIP_MAX_MEMBERS = 200
ZIP_MAX_MEMBER_BYTES = 25 * 1024 * 1024
ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES = 50 * 1024 * 1024
ZIP_MAX_RECURSION_DEPTH = 1
ATTACHMENT_AI_PROVIDER = "ollama"
ATTACHMENT_AI_PROMPT_VERSION = "gmail-attachment-ai-v5"
ATTACHMENT_AI_PROMPT = """Analyze this real Gmail attachment image.

Return concise JSON with these keys:
- is_useful: true if the image may contain meaningful user-visible information worth indexing. Use false only when the image is clearly blank, decorative, a tracking pixel, a logo-only image, UI chrome, or an attachment placeholder with no useful content. Receipts, invoices, photos, charts, screenshots of documents, and screenshots containing readable content are useful. Generic words like "Gmail attachment" alone are not useful. If unsure, use true.
- scene_summary: one sentence describing the whole image, including people, setting, activity, important objects, and visual context. Do not summarize only the largest text region.
- visible_text: readable text visible anywhere in the image, preserving important names, dates, amounts, addresses, headings, labels, and identifiers.
- likely_document_type: a short label such as receipt, invoice, chart, photo, official letter, form, screenshot, or unknown.
- document_context: a short explanation of what the attachment appears to be in context, such as classroom presentation photo, event flyer, product screenshot, scanned receipt, handwritten note, or unknown.
- search_keywords: concise search keywords, named entities, topics, settings, activities, and visible text phrases useful for finding this later.

Describe the full scene even when there is prominent text. If text is unclear, say so. Do not invent details."""
ATTACHMENT_AI_FALLBACK_STATUSES = {"unsupported", "empty", "invalid_pdf"}
ATTACHMENT_AI_MIN_IMAGE_BYTES = 16 * 1024
IMAGE_MIME_TYPES = {"image/png", "image/jpeg", "image/jpg", "image/webp"}
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
T = TypeVar("T")
ATTACHMENT_AI_FALLBACK_UNSET = object()


@dataclass(frozen=True)
class MailboxSyncSummary:
    account: str
    sync_type: str
    next_history_id: int
    messages_written: int
    deleted_messages: int
    attachments_written: int
    attachment_text_chars: int
    query: str | None
    attachment_backfill_candidates: int = 0
    attachment_backfill_rows_written: int = 0


@dataclass(frozen=True)
class AttachmentTextExtraction:
    text: str
    status: str
    error: str = ""
    ai_provider: str = ""
    ai_model: str = ""
    ai_base_url: str = ""
    ai_prompt_version: str = ""
    ai_prompt_sha256: str = ""
    ai_prompt: str = ""
    ai_source_status: str = ""
    ai_elapsed_ms: int = 0
    ai_processed_at: datetime = EPOCH_UTC


@dataclass(frozen=True)
class AttachmentAiFallbackConfig:
    provider: str
    base_url: str
    model: str
    timeout_seconds: int
    pdf_max_pages: int
    pull_model: bool = True
    client: AttachmentVisionClient | None = None


class AttachmentVisionClient(Protocol):
    def generate(
        self,
        *,
        model: str,
        prompt: str,
        images: Sequence[bytes],
        format: str | None = None,
        options: Mapping[str, object] | None = None,
        think: bool = False,
        timeout_seconds: int | None = None,
    ) -> str: ...


class AttachmentTextUnavailable(Exception):
    def __init__(self, *, status: str, error: str) -> None:
        super().__init__(error)
        self.status = status
        self.error = error


AttachmentKey = tuple[str, str, str]


def gmail_sync_lock_path() -> Path:
    return Path(os.getenv("GMAIL_SYNC_LOCK_PATH", str(DEFAULT_GMAIL_SYNC_LOCK_PATH))).expanduser()


def gmail_sync_lock_postgres_url() -> str | None:
    return (
        os.getenv("GMAIL_SYNC_LOCK_POSTGRES_URL")
        or os.getenv("DAGSTER_POSTGRES_URL")
        or os.getenv("DATABASE_URL")
        or None
    )


@contextmanager
def exclusive_gmail_sync_lock() -> Iterator[bool]:
    postgres_url = gmail_sync_lock_postgres_url()
    if postgres_url:
        with exclusive_postgres_advisory_lock(postgres_url, GMAIL_SYNC_POSTGRES_LOCK_ID) as acquired:
            yield acquired
        return

    with exclusive_process_lock(gmail_sync_lock_path()) as acquired:
        yield acquired


@contextmanager
def exclusive_postgres_advisory_lock(postgres_url: str, lock_id: int) -> Iterator[bool]:
    import psycopg2

    connection = psycopg2.connect(postgres_url)
    connection.autocommit = True
    cursor = connection.cursor()
    acquired = False
    try:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
        acquired = bool(cursor.fetchone()[0])
        yield acquired
    finally:
        if acquired:
            cursor.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
        cursor.close()
        connection.close()


@contextmanager
def exclusive_process_lock(path: Path) -> Iterator[bool]:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = path.open("a+")
    try:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    finally:
        lock_file.close()


class GmailSyncRunner:
    def __init__(
        self,
        *,
        settings: Settings,
        warehouse: ClickHouseWarehouse,
        logger,
        attachment_ai_client: AttachmentVisionClient | None = None,
        attachment_ai_fallback: AttachmentAiFallbackConfig | None | object = ATTACHMENT_AI_FALLBACK_UNSET,
    ) -> None:
        self._settings = settings
        self._warehouse = warehouse
        self._logger = logger
        if attachment_ai_fallback is ATTACHMENT_AI_FALLBACK_UNSET:
            self._attachment_ai_fallback = attachment_ai_fallback_config_from_settings(
                settings,
                client=attachment_ai_client,
            )
        else:
            self._attachment_ai_fallback = attachment_ai_fallback

    def sync_all(self) -> list[MailboxSyncSummary]:
        with exclusive_gmail_sync_lock() as acquired:
            if not acquired:
                self._logger.warning("Skipping Gmail sync because another Gmail sync is already running")
                return []

            self._warehouse.ensure_tables()
            state_by_account = self._warehouse.load_sync_state()

            summaries: list[MailboxSyncSummary] = []
            failures: list[str] = []

            for account in self._settings.gmail_accounts:
                state = state_by_account.get(account.email_address)
                try:
                    summary = self._sync_account(account, state)
                except Exception as exc:
                    last_history_id = state.last_history_id if state else 0
                    self._warehouse.insert_sync_state(
                        account=account.email_address,
                        last_history_id=last_history_id,
                        last_sync_type=state.last_sync_type if state else "unknown",
                        status="failed",
                        error=str(exc),
                        updated_at=datetime.now(tz=UTC),
                    )
                    failures.append(f"{account.email_address}: {exc}")
                    continue

                self._warehouse.insert_sync_state(
                    account=summary.account,
                    last_history_id=summary.next_history_id,
                    last_sync_type=summary.sync_type,
                    status="ok",
                    error="",
                    updated_at=datetime.now(tz=UTC),
                )
                summaries.append(summary)

            if failures:
                raise RuntimeError("Mailbox sync failed for: " + "; ".join(failures))

            return summaries

    def _sync_account(self, account: GmailAccount, state: SyncState | None) -> MailboxSyncSummary:
        service = build_gmail_service(account=account, settings=self._settings)
        if self._settings.gmail_force_full_sync or not state or state.last_history_id == 0:
            summary = self._full_sync(account=account, service=service)
            return self._with_attachment_backfill(account=account, service=service, summary=summary)

        try:
            summary = self._partial_sync(
                account=account,
                service=service,
                start_history_id=state.last_history_id,
            )
            return self._with_attachment_backfill(account=account, service=service, summary=summary)
        except HttpError as exc:
            if _http_status(exc) != 404:
                raise
            self._logger.warning(
                "History cursor for %s is stale at %s, falling back to full sync",
                account.email_address,
                state.last_history_id,
            )
            summary = self._full_sync(account=account, service=service)
            return self._with_attachment_backfill(account=account, service=service, summary=summary)

    def _full_sync(self, *, account: GmailAccount, service) -> MailboxSyncSummary:
        sync_started_at = datetime.now(tz=UTC)
        next_history_id = current_history_id(service)
        messages_written = 0
        attachments_written = 0
        attachment_text_chars = 0

        self._logger.info(
            "Starting full Gmail sync for %s with query %r",
            account.email_address,
            self._settings.gmail_full_sync_query,
        )
        for message_ids in iter_full_message_id_batches(
            service=service,
            page_size=self._settings.gmail_page_size,
            include_spam_trash=self._settings.gmail_include_spam_trash,
            query=self._settings.gmail_full_sync_query,
        ):
            existing_message_ids = self._warehouse.existing_message_ids(
                account=account.email_address,
                message_ids=message_ids,
            )
            new_message_ids = [
                message_id for message_id in message_ids if message_id not in existing_message_ids
            ]
            fetched_messages = [fetch_message(service, message_id) for message_id in new_message_ids]
            rows = [
                message_to_row(
                    account=account.email_address,
                    message=message,
                    synced_at=sync_started_at,
                )
                for message in fetched_messages
            ]
            self._warehouse.insert_messages(rows)
            messages_written += len(rows)

            existing_messages = list(
                self._warehouse.load_message_payloads(
                    account=account.email_address,
                    message_ids=sorted(existing_message_ids),
                ).values()
            )
            attachment_rows = self._attachment_rows_for_messages(
                account=account,
                service=service,
                messages=[*fetched_messages, *existing_messages],
                message_ids=message_ids,
                synced_at=sync_started_at,
                force_reprocess=self._settings.gmail_force_full_sync,
            )
            self._warehouse.insert_attachments(attachment_rows)
            attachments_written += len(attachment_rows)
            attachment_text_chars += sum(len(str(row["text"])) for row in attachment_rows)

            self._logger.info(
                "Synced %s new Gmail messages and %s Gmail attachment rows for %s so far; skipped %s existing messages in latest page",
                messages_written,
                attachments_written,
                account.email_address,
                len(existing_message_ids),
            )

        return MailboxSyncSummary(
            account=account.email_address,
            sync_type="full",
            next_history_id=next_history_id,
            messages_written=messages_written,
            deleted_messages=0,
            attachments_written=attachments_written,
            attachment_text_chars=attachment_text_chars,
            query=self._settings.gmail_full_sync_query,
        )

    def _partial_sync(self, *, account: GmailAccount, service, start_history_id: int) -> MailboxSyncSummary:
        sync_started_at = datetime.now(tz=UTC)
        changed_message_ids, next_history_id = load_incremental_message_ids(
            service=service,
            start_history_id=start_history_id,
            page_size=self._settings.gmail_page_size,
        )

        messages_written = 0
        deleted_messages = 0
        attachments_written = 0
        attachment_text_chars = 0

        self._logger.info(
            "Starting incremental Gmail sync for %s from history %s (%s messages changed)",
            account.email_address,
            start_history_id,
            len(changed_message_ids),
        )

        for message_ids in chunked(sorted(changed_message_ids), self._settings.gmail_page_size):
            rows = []
            attachment_rows = []
            existing_attachment_keys = self._warehouse.existing_attachment_keys(
                account=account.email_address,
                message_ids=message_ids,
            )
            for message_id in message_ids:
                message = fetch_message_or_none(service, message_id)
                if message is None:
                    rows.append(
                        deleted_message_row(
                            account=account.email_address,
                            message_id=message_id,
                            synced_at=sync_started_at,
                            history_id=next_history_id,
                        )
                    )
                    attachment_rows.extend(
                        deleted_attachment_rows(
                            account=account.email_address,
                            message_id=message_id,
                            existing_keys=existing_attachment_keys,
                            synced_at=sync_started_at,
                            history_id=next_history_id,
                        )
                    )
                    deleted_messages += 1
                else:
                    rows.append(
                        message_to_row(
                            account=account.email_address,
                            message=message,
                            synced_at=sync_started_at,
                        )
                    )
                    attachment_rows.extend(
                        attachment_rows_for_message(
                            account=account.email_address,
                            service=service,
                            message=message,
                            synced_at=sync_started_at,
                            existing_keys=existing_attachment_keys,
                            max_bytes=self._settings.gmail_attachment_max_bytes,
                            text_max_chars=self._settings.gmail_attachment_text_max_chars,
                            ai_fallback=self._attachment_ai_fallback,
                        )
                    )
            self._warehouse.insert_messages(rows)
            self._warehouse.insert_attachments(attachment_rows)
            messages_written += len(rows)
            attachments_written += len(attachment_rows)
            attachment_text_chars += sum(len(str(row["text"])) for row in attachment_rows)
            self._logger.info(
                "Synced %s changed Gmail messages and %s Gmail attachment rows for %s so far",
                messages_written,
                attachments_written,
                account.email_address,
            )

        return MailboxSyncSummary(
            account=account.email_address,
            sync_type="partial",
            next_history_id=next_history_id,
            messages_written=messages_written,
            deleted_messages=deleted_messages,
            attachments_written=attachments_written,
            attachment_text_chars=attachment_text_chars,
            query=None,
        )

    def _attachment_rows_for_messages(
        self,
        *,
        account: GmailAccount,
        service,
        messages: list[Mapping[str, object]],
        message_ids: list[str],
        synced_at: datetime,
        force_reprocess: bool = False,
    ) -> list[dict[str, object]]:
        if not messages:
            return []
        existing_attachment_keys = self._warehouse.existing_attachment_keys(
            account=account.email_address,
            message_ids=message_ids,
        )
        rows: list[dict[str, object]] = []
        for message in messages:
            rows.extend(
                attachment_rows_for_message(
                    account=account.email_address,
                    service=service,
                    message=message,
                    synced_at=synced_at,
                    existing_keys=existing_attachment_keys,
                    max_bytes=self._settings.gmail_attachment_max_bytes,
                    text_max_chars=self._settings.gmail_attachment_text_max_chars,
                    force_reprocess=force_reprocess,
                    ai_fallback=self._attachment_ai_fallback,
                )
            )
        return rows

    def _with_attachment_backfill(
        self,
        *,
        account: GmailAccount,
        service,
        summary: MailboxSyncSummary,
    ) -> MailboxSyncSummary:
        try:
            candidates, rows_written, text_chars = self._backfill_attachment_candidates(
                account=account,
                service=service,
            )
        except Exception as exc:
            self._logger.warning(
                "Skipping Gmail attachment backfill for %s because it failed: %s",
                account.email_address,
                exc,
            )
            return summary
        return replace(
            summary,
            attachments_written=summary.attachments_written + rows_written,
            attachment_text_chars=summary.attachment_text_chars + text_chars,
            attachment_backfill_candidates=candidates,
            attachment_backfill_rows_written=rows_written,
        )

    def _backfill_attachment_candidates(self, *, account: GmailAccount, service) -> tuple[int, int, int]:
        limit = self._settings.gmail_attachment_backfill_batch_size
        if limit <= 0:
            return (0, 0, 0)

        messages = self._warehouse.load_attachment_backfill_candidate_messages(
            account=account.email_address,
            limit=limit,
        )
        if not messages:
            return (0, 0, 0)

        candidates = 0
        rows_written = 0
        text_chars = 0

        for message in messages:
            message_id = str(message.get("id", ""))
            if not message_id:
                continue
            candidates += 1
            synced_at = datetime.now(tz=UTC)
            try:
                existing_keys = self._warehouse.existing_attachment_keys(
                    account=account.email_address,
                    message_ids=[message_id],
                )
                rows = attachment_rows_for_message(
                    account=account.email_address,
                    service=service,
                    message=message,
                    synced_at=synced_at,
                    existing_keys=existing_keys,
                    max_bytes=self._settings.gmail_attachment_max_bytes,
                    text_max_chars=self._settings.gmail_attachment_text_max_chars,
                    force_reprocess=True,
                    ai_fallback=self._attachment_ai_fallback,
                )
                self._warehouse.insert_attachments(rows)
                self._warehouse.insert_attachment_backfill_state(
                    [
                        attachment_backfill_state_row(
                            account=account.email_address,
                            message_id=message_id,
                            status="ok",
                            attachment_rows_written=len(rows),
                            error="",
                            updated_at=synced_at,
                        )
                    ]
                )
            except Exception as exc:
                self._warehouse.insert_attachment_backfill_state(
                    [
                        attachment_backfill_state_row(
                            account=account.email_address,
                            message_id=message_id,
                            status="failed",
                            attachment_rows_written=0,
                            error=truncate_error(str(exc)),
                            updated_at=datetime.now(tz=UTC),
                        )
                    ]
                )
                self._logger.warning(
                    "Failed to backfill Gmail attachments for %s message %s: %s",
                    account.email_address,
                    message_id,
                    exc,
                )
                continue

            rows_written += len(rows)
            text_chars += sum(len(str(row["text"])) for row in rows)

        self._logger.info(
            "Backfilled Gmail attachments for %s: %s candidates, %s rows, %s text chars",
            account.email_address,
            candidates,
            rows_written,
            text_chars,
        )
        return (candidates, rows_written, text_chars)


def attachment_ai_fallback_config_from_settings(
    settings: Settings,
    *,
    client: AttachmentVisionClient | None = None,
) -> AttachmentAiFallbackConfig | None:
    if not settings.gmail_attachment_ai_fallback_enabled:
        return None
    return AttachmentAiFallbackConfig(
        provider=ATTACHMENT_AI_PROVIDER,
        base_url=settings.gmail_attachment_ai_fallback_base_url.rstrip("/"),
        model=settings.gmail_attachment_ai_fallback_model,
        timeout_seconds=settings.gmail_attachment_ai_fallback_timeout_seconds,
        pdf_max_pages=settings.gmail_attachment_ai_fallback_pdf_max_pages,
        pull_model=settings.gmail_attachment_ai_fallback_pull_model,
        client=client,
    )


def build_gmail_service(*, account: GmailAccount, settings: Settings):
    credentials = load_credentials(account=account, settings=settings)
    return build("gmail", "v1", credentials=credentials, cache_discovery=False)


def load_credentials(*, account: GmailAccount, settings: Settings) -> Credentials:
    return load_google_credentials(
        email_address=account.email_address,
        settings=settings,
        scopes=settings.gmail_scopes,
        service_name="Gmail",
    )


def gmail_token_json_from_env(email_address: str) -> str | None:
    slug = env_slug(email_address)
    for name in (
        f"GMAIL_{slug}_TOKEN_JSON",
        f"GMAIL_TOKEN_JSON_{slug}",
    ):
        value = os.getenv(name)
        if value:
            return value
    for name in (
        f"GMAIL_{slug}_TOKEN_JSON_B64",
        f"GMAIL_TOKEN_JSON_B64_{slug}",
    ):
        value = os.getenv(name)
        if value:
            return base64.b64decode(value).decode("utf-8")
    return google_token_json_from_env(email_address)


def current_history_id(service) -> int:
    profile = execute_gmail_request(lambda: service.users().getProfile(userId="me").execute())
    return int(profile["historyId"])


def iter_full_message_id_batches(
    *,
    service,
    page_size: int,
    include_spam_trash: bool,
    query: str | None,
) -> Iterator[list[str]]:
    page_token: str | None = None
    while True:
        list_kwargs = {
            "userId": "me",
            "maxResults": page_size,
            "pageToken": page_token,
            "includeSpamTrash": include_spam_trash,
        }
        if query:
            list_kwargs["q"] = query
        response = execute_gmail_request(lambda: service.users().messages().list(**list_kwargs).execute())
        messages = response.get("messages", [])
        if messages:
            yield [message["id"] for message in messages]
        page_token = response.get("nextPageToken")
        if not page_token:
            return


def load_incremental_message_ids(*, service, start_history_id: int, page_size: int) -> tuple[set[str], int]:
    page_token: str | None = None
    changed_message_ids: set[str] = set()
    next_history_id = start_history_id

    while True:
        response = execute_gmail_request(
            lambda: service.users()
            .history()
            .list(
                userId="me",
                startHistoryId=str(start_history_id),
                maxResults=page_size,
                pageToken=page_token,
            )
            .execute()
        )
        next_history_id = int(response.get("historyId", start_history_id))
        for history_record in response.get("history", []):
            changed_message_ids.update(history_message_ids(history_record))

        page_token = response.get("nextPageToken")
        if not page_token:
            return changed_message_ids, next_history_id


def history_message_ids(history_record: Mapping[str, object]) -> set[str]:
    message_ids: set[str] = set()
    for field_name in ("messages", "messagesAdded", "messagesDeleted", "labelsAdded", "labelsRemoved"):
        value = history_record.get(field_name, [])
        if not isinstance(value, Sequence):
            continue
        for item in value:
            if not isinstance(item, Mapping):
                continue
            message = item.get("message", item)
            if isinstance(message, Mapping) and "id" in message:
                message_ids.add(str(message["id"]))
    return message_ids


def fetch_message(service, message_id: str) -> Mapping[str, object]:
    return execute_gmail_request(
        lambda: service.users().messages().get(userId="me", id=message_id, format="full").execute()
    )


def fetch_message_or_none(service, message_id: str) -> Mapping[str, object] | None:
    try:
        return fetch_message(service, message_id)
    except HttpError as exc:
        if _http_status(exc) == 404:
            return None
        raise


def message_to_row(*, account: str, message: Mapping[str, object], synced_at: datetime) -> dict[str, object]:
    payload = message.get("payload", {})
    headers = header_map(payload.get("headers", []))
    from_header = headers.get("from", "")
    _, from_address = parseaddr(from_header)
    body_text, body_html = extract_message_bodies(payload if isinstance(payload, Mapping) else {})
    body_markdown_full = safe_message_body_to_markdown(body_text=body_text, body_html=body_html)
    body_markdown = safe_collapsed_message_body_to_markdown(body_text=body_text, body_html=body_html)
    if body_markdown and not body_markdown_full:
        body_markdown_full = body_markdown
    sync_version = int(synced_at.timestamp() * 1000)

    return {
        "account": account,
        "message_id": str(message.get("id", "")),
        "thread_id": str(message.get("threadId", "")),
        "history_id": int(message.get("historyId", 0)),
        "internal_date": internal_date_from_message(message),
        "label_ids": [str(label_id) for label_id in message.get("labelIds", [])],
        "is_deleted": 0,
        "snippet": str(message.get("snippet", "")),
        "subject": headers.get("subject", ""),
        "from_address": from_address or from_header,
        "to_addresses": parse_address_list(headers.get("to", "")),
        "cc_addresses": parse_address_list(headers.get("cc", "")),
        "bcc_addresses": parse_address_list(headers.get("bcc", "")),
        "delivered_to": headers.get("delivered-to", ""),
        "rfc822_message_id": headers.get("message-id", ""),
        "date_header": headers.get("date", ""),
        "size_estimate": int(message.get("sizeEstimate", 0)),
        "body_text": body_text,
        "body_html": body_html,
        "body_markdown": body_markdown,
        "body_markdown_full": body_markdown_full,
        "body_markdown_clean": body_markdown,
        "payload_json": json.dumps(message, sort_keys=True, separators=(",", ":")),
        "synced_at": synced_at,
        "sync_version": sync_version,
    }


def deleted_message_row(*, account: str, message_id: str, synced_at: datetime, history_id: int) -> dict[str, object]:
    sync_version = int(synced_at.timestamp() * 1000)
    return {
        "account": account,
        "message_id": message_id,
        "thread_id": "",
        "history_id": history_id,
        "internal_date": EPOCH_UTC,
        "label_ids": [],
        "is_deleted": 1,
        "snippet": "",
        "subject": "",
        "from_address": "",
        "to_addresses": [],
        "cc_addresses": [],
        "bcc_addresses": [],
        "delivered_to": "",
        "rfc822_message_id": "",
        "date_header": "",
        "size_estimate": 0,
        "body_text": "",
        "body_html": "",
        "body_markdown": "",
        "body_markdown_full": "",
        "body_markdown_clean": "",
        "payload_json": "{}",
        "synced_at": synced_at,
        "sync_version": sync_version,
    }


def attachment_rows_for_message(
    *,
    account: str,
    service,
    message: Mapping[str, object],
    synced_at: datetime,
    existing_keys: set[AttachmentKey],
    max_bytes: int,
    text_max_chars: int,
    force_reprocess: bool = False,
    ai_fallback: AttachmentAiFallbackConfig | None = None,
) -> list[dict[str, object]]:
    message_id = str(message.get("id", ""))
    current_parts = attachment_parts_from_message(message)
    current_keys = {attachment_key(message_id=message_id, part=part) for part in current_parts}
    message_existing_keys = {key for key in existing_keys if key[0] == message_id}

    rows: list[dict[str, object]] = []
    for part in current_parts:
        key = attachment_key(message_id=message_id, part=part)
        if key in existing_keys and not force_reprocess:
            continue
        rows.append(
            attachment_part_to_row(
                account=account,
                service=service,
                message=message,
                part=part,
                synced_at=synced_at,
                max_bytes=max_bytes,
                text_max_chars=text_max_chars,
                ai_fallback=ai_fallback,
            )
        )

    for key in sorted(message_existing_keys - current_keys):
        rows.append(
            deleted_attachment_row(
                account=account,
                message_id=key[0],
                part_id=key[1],
                attachment_id="",
                filename=key[2],
                synced_at=synced_at,
                history_id=int(message.get("historyId", 0)),
            )
        )
    return rows


def deleted_attachment_rows(
    *,
    account: str,
    message_id: str,
    existing_keys: set[AttachmentKey],
    synced_at: datetime,
    history_id: int,
) -> list[dict[str, object]]:
    return [
        deleted_attachment_row(
            account=account,
            message_id=key[0],
            part_id=key[1],
            attachment_id="",
            filename=key[2],
            synced_at=synced_at,
            history_id=history_id,
        )
        for key in sorted(existing_keys)
        if key[0] == message_id
    ]


def deleted_attachment_row(
    *,
    account: str,
    message_id: str,
    part_id: str,
    attachment_id: str,
    filename: str,
    synced_at: datetime,
    history_id: int,
) -> dict[str, object]:
    sync_version = int(synced_at.timestamp() * 1000)
    return {
        "account": account,
        "message_id": message_id,
        "thread_id": "",
        "history_id": int(history_id),
        "internal_date": EPOCH_UTC,
        "part_id": part_id,
        "attachment_id": attachment_id,
        "filename": filename,
        "mime_type": "",
        "content_id": "",
        "content_disposition": "",
        "size": 0,
        "content_sha256": "",
        "text": "",
        "text_extraction_status": "deleted",
        "text_extraction_error": "",
        "ai_provider": "",
        "ai_model": "",
        "ai_base_url": "",
        "ai_prompt_version": "",
        "ai_prompt_sha256": "",
        "ai_prompt": "",
        "ai_source_status": "",
        "ai_elapsed_ms": 0,
        "ai_processed_at": EPOCH_UTC,
        "is_deleted": 1,
        "part_json": "{}",
        "synced_at": synced_at,
        "sync_version": sync_version,
    }


def attachment_backfill_state_row(
    *,
    account: str,
    message_id: str,
    status: str,
    attachment_rows_written: int,
    error: str,
    updated_at: datetime,
) -> dict[str, object]:
    return {
        "account": account,
        "message_id": message_id,
        "status": status,
        "attachment_rows_written": int(attachment_rows_written),
        "error": error,
        "updated_at": updated_at,
        "sync_version": int(updated_at.timestamp() * 1000),
    }


def attachment_part_to_row(
    *,
    account: str,
    service,
    message: Mapping[str, object],
    part: Mapping[str, object],
    synced_at: datetime,
    max_bytes: int,
    text_max_chars: int,
    ai_fallback: AttachmentAiFallbackConfig | None = None,
) -> dict[str, object]:
    message_id = str(message.get("id", ""))
    headers = header_map(part.get("headers", []))
    body = part_body(part)
    attachment_id = str(body.get("attachmentId", ""))
    filename = str(part.get("filename", ""))
    mime_type = normalized_mime_type(str(part.get("mimeType", "")))
    declared_size = int(body.get("size", 0) or 0)
    content_sha256 = ""

    if max_bytes == 0:
        extraction = AttachmentTextExtraction(
            text="",
            status="disabled",
            error="attachment downloads disabled by GMAIL_ATTACHMENT_MAX_BYTES=0",
        )
    elif declared_size > max_bytes:
        extraction = AttachmentTextExtraction(
            text="",
            status="too_large",
            error=f"attachment size {declared_size} exceeds GMAIL_ATTACHMENT_MAX_BYTES={max_bytes}",
        )
    else:
        try:
            content = attachment_content_bytes(service=service, message_id=message_id, part=part)
        except Exception as exc:
            content = b""
            extraction = AttachmentTextExtraction(
                text="",
                status="fetch_error",
                error=truncate_error(str(exc)),
            )
        else:
            if not content:
                extraction = AttachmentTextExtraction(text="", status="no_content")
            elif len(content) > max_bytes:
                extraction = AttachmentTextExtraction(
                    text="",
                    status="too_large",
                    error=f"attachment content length {len(content)} exceeds GMAIL_ATTACHMENT_MAX_BYTES={max_bytes}",
                )
            else:
                content_sha256 = hashlib.sha256(content).hexdigest()
                declared_size = declared_size or len(content)
                extraction = extract_attachment_text(
                    content=content,
                    mime_type=mime_type,
                    filename=filename,
                    max_chars=text_max_chars,
                )
                extraction = apply_attachment_ai_fallback(
                    extraction=extraction,
                    content=content,
                    mime_type=mime_type,
                    filename=filename,
                    max_chars=text_max_chars,
                    config=ai_fallback,
                )

    sync_version = int(synced_at.timestamp() * 1000)
    return {
        "account": account,
        "message_id": message_id,
        "thread_id": str(message.get("threadId", "")),
        "history_id": int(message.get("historyId", 0)),
        "internal_date": internal_date_from_message(message),
        "part_id": str(part.get("partId", "")),
        "attachment_id": attachment_id,
        "filename": filename,
        "mime_type": mime_type,
        "content_id": headers.get("content-id", ""),
        "content_disposition": headers.get("content-disposition", ""),
        "size": declared_size,
        "content_sha256": content_sha256,
        "text": extraction.text,
        "text_extraction_status": extraction.status,
        "text_extraction_error": extraction.error,
        "ai_provider": extraction.ai_provider,
        "ai_model": extraction.ai_model,
        "ai_base_url": extraction.ai_base_url,
        "ai_prompt_version": extraction.ai_prompt_version,
        "ai_prompt_sha256": extraction.ai_prompt_sha256,
        "ai_prompt": extraction.ai_prompt,
        "ai_source_status": extraction.ai_source_status,
        "ai_elapsed_ms": extraction.ai_elapsed_ms,
        "ai_processed_at": extraction.ai_processed_at,
        "is_deleted": 0,
        "part_json": attachment_part_json(part),
        "synced_at": synced_at,
        "sync_version": sync_version,
    }


def attachment_parts_from_message(message: Mapping[str, object]) -> list[Mapping[str, object]]:
    payload = message.get("payload", {})
    if not isinstance(payload, Mapping):
        return []
    return [part for part in walk_parts(payload) if is_attachment_part(part)]


def is_attachment_part(part: Mapping[str, object]) -> bool:
    body = part_body(part)
    headers = header_map(part.get("headers", []))
    content_disposition = headers.get("content-disposition", "").lower()
    return bool(
        str(part.get("filename", ""))
        or body.get("attachmentId")
        or content_disposition.startswith("attachment")
        or content_disposition.startswith("inline")
    )


def attachment_key(*, message_id: str, part: Mapping[str, object]) -> AttachmentKey:
    body = part_body(part)
    return (
        message_id,
        str(part.get("partId", "")),
        str(part.get("filename", "")),
    )


def part_body(part: Mapping[str, object]) -> Mapping[str, object]:
    body = part.get("body", {})
    if isinstance(body, Mapping):
        return body
    return {}


def attachment_content_bytes(*, service, message_id: str, part: Mapping[str, object]) -> bytes:
    body = part_body(part)
    data = body.get("data")
    if data:
        return decode_base64url_bytes(data)
    attachment_id = body.get("attachmentId")
    if attachment_id:
        return fetch_attachment_bytes(service=service, message_id=message_id, attachment_id=str(attachment_id))
    return b""


def fetch_attachment_bytes(*, service, message_id: str, attachment_id: str) -> bytes:
    response = execute_gmail_request(
        lambda: service.users()
        .messages()
        .attachments()
        .get(userId="me", messageId=message_id, id=attachment_id)
        .execute()
    )
    return decode_base64url_bytes(response.get("data"))


def extract_attachment_text(
    *,
    content: bytes,
    mime_type: str,
    filename: str,
    max_chars: int,
) -> AttachmentTextExtraction:
    try:
        text = raw_attachment_text(
            content=content,
            mime_type=mime_type,
            filename=filename,
            max_chars=max_chars,
        )
    except AttachmentTextUnavailable as exc:
        return AttachmentTextExtraction(text="", status=exc.status, error=truncate_error(exc.error))
    except Exception as exc:
        return AttachmentTextExtraction(text="", status="error", error=truncate_error(str(exc)))
    if text is None:
        return AttachmentTextExtraction(text="", status="unsupported")

    text = normalize_markdown(text)
    if not text:
        return AttachmentTextExtraction(text="", status="empty")
    if len(text) > max_chars:
        return AttachmentTextExtraction(text=text[:max_chars], status="truncated")
    return AttachmentTextExtraction(text=text, status="ok")


def apply_attachment_ai_fallback(
    *,
    extraction: AttachmentTextExtraction,
    content: bytes,
    mime_type: str,
    filename: str,
    max_chars: int,
    config: AttachmentAiFallbackConfig | None,
) -> AttachmentTextExtraction:
    if config is None or extraction.status not in ATTACHMENT_AI_FALLBACK_STATUSES:
        return extraction
    try:
        images = attachment_ai_fallback_images(
            content=content,
            mime_type=mime_type,
            filename=filename,
            pdf_max_pages=config.pdf_max_pages,
            timeout_seconds=config.timeout_seconds,
        )
        if not images:
            return extraction
        started_at = time.monotonic()
        response_text = call_ollama_attachment_vision_model(
            images=images,
            config=config,
        )
        elapsed_ms = int((time.monotonic() - started_at) * 1000)
    except Exception as exc:
        return replace(
            extraction,
            error=truncate_error(
                "; ".join(
                    part
                    for part in (
                        extraction.error,
                        f"AI fallback failed: {exc}",
                    )
                    if part
                )
            ),
        )

    prompt_sha256 = hashlib.sha256(ATTACHMENT_AI_PROMPT.encode("utf-8")).hexdigest()
    metadata = {
        "source_status": extraction.status,
        "source_error": extraction.error,
        "provider": config.provider,
        "model": config.model,
        "base_url": config.base_url,
        "prompt_version": ATTACHMENT_AI_PROMPT_VERSION,
        "prompt_sha256": prompt_sha256,
    }

    text = normalize_markdown(format_attachment_ai_response(response_text))
    status = "ai_ok"
    if not text:
        status = "ai_empty"
    elif len(text) > max_chars:
        text = text[:max_chars]
        status = "ai_truncated"

    return AttachmentTextExtraction(
        text=text,
        status=status,
        error=truncate_error(json.dumps(metadata, sort_keys=True, separators=(",", ":"))),
        ai_provider=config.provider,
        ai_model=config.model,
        ai_base_url=config.base_url,
        ai_prompt_version=ATTACHMENT_AI_PROMPT_VERSION,
        ai_prompt_sha256=prompt_sha256,
        ai_prompt=ATTACHMENT_AI_PROMPT,
        ai_source_status=extraction.status,
        ai_elapsed_ms=elapsed_ms,
        ai_processed_at=datetime.now(tz=UTC),
    )


def attachment_ai_fallback_images(
    *,
    content: bytes,
    mime_type: str,
    filename: str,
    pdf_max_pages: int,
    timeout_seconds: int,
) -> list[bytes]:
    extension = Path(filename.lower()).suffix
    if is_supported_image_attachment(content=content, mime_type=mime_type, extension=extension):
        return [content]
    if mime_type == "application/pdf" or extension == ".pdf" or looks_like_pdf(content):
        if not looks_like_pdf(content):
            return []
        return render_pdf_attachment_pages(
            content=content,
            max_pages=pdf_max_pages,
            timeout_seconds=timeout_seconds,
        )
    return []


def is_supported_image_attachment(*, content: bytes, mime_type: str, extension: str) -> bool:
    if len(content) < ATTACHMENT_AI_MIN_IMAGE_BYTES:
        return False
    if mime_type in IMAGE_MIME_TYPES or extension in IMAGE_EXTENSIONS:
        return looks_like_supported_image(content)
    return looks_like_supported_image(content)


def looks_like_supported_image(content: bytes) -> bool:
    return (
        content.startswith(b"\x89PNG\r\n\x1a\n")
        or content.startswith(b"\xff\xd8\xff")
        or content.startswith(b"RIFF") and content[8:12] == b"WEBP"
    )


def render_pdf_attachment_pages(*, content: bytes, max_pages: int, timeout_seconds: int) -> list[bytes]:
    pdftoppm = shutil.which("pdftoppm")
    if not pdftoppm:
        raise RuntimeError("pdftoppm is not installed; cannot render image-only PDF for AI fallback")

    with tempfile.TemporaryDirectory() as directory:
        tempdir = Path(directory)
        input_path = tempdir / "attachment.pdf"
        output_prefix = tempdir / "page"
        input_path.write_bytes(content)
        command = [
            pdftoppm,
            "-png",
            "-r",
            "160",
            "-f",
            "1",
            "-l",
            str(max_pages),
            str(input_path),
            str(output_prefix),
        ]
        result = subprocess.run(
            command,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
        if result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"pdftoppm failed: {stderr or result.returncode}")
        return [path.read_bytes() for path in sorted(tempdir.glob("page-*.png"))]


def call_ollama_attachment_vision_model(
    *,
    images: Sequence[bytes],
    config: AttachmentAiFallbackConfig,
) -> str:
    options = {
        "temperature": 0,
        "num_predict": 512,
    }
    if config.client is not None:
        return run_attachment_ai_call_with_timeout(
            lambda: config.client.generate(
                model=config.model,
                prompt=ATTACHMENT_AI_PROMPT,
                images=images,
                format="json",
                options=options,
                think=False,
                timeout_seconds=config.timeout_seconds,
            ),
            timeout_seconds=config.timeout_seconds,
        )

    image_payload = [base64.b64encode(image).decode("ascii") for image in images]
    payload = {
        "model": config.model,
        "prompt": ATTACHMENT_AI_PROMPT,
        "images": image_payload,
        "stream": False,
        "think": False,
        "format": "json",
        "options": options,
    }
    request = urllib.request.Request(
        f"{config.base_url}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )

    def request_ollama_generate() -> dict[str, object]:
        with urllib.request.urlopen(request, timeout=config.timeout_seconds) as response:
            payload = json.loads(response.read())
        if isinstance(payload, dict):
            return payload
        raise RuntimeError("Ollama returned a non-object JSON response")

    try:
        response_data = run_attachment_ai_call_with_timeout(
            request_ollama_generate,
            timeout_seconds=config.timeout_seconds,
        )
    except urllib.error.URLError as exc:
        raise RuntimeError(str(exc)) from exc
    return str(response_data.get("response", ""))


def run_attachment_ai_call_with_timeout[T](call: Callable[[], T], *, timeout_seconds: float) -> T:
    if timeout_seconds <= 0:
        return call()

    results: queue.Queue[tuple[bool, T | BaseException]] = queue.Queue(maxsize=1)

    def target() -> None:
        try:
            results.put((True, call()), block=False)
        except BaseException as exc:
            results.put((False, exc), block=False)

    thread = threading.Thread(target=target, name="gmail-attachment-ai-call", daemon=True)
    thread.start()
    thread.join(timeout_seconds)
    if thread.is_alive():
        raise TimeoutError(f"AI attachment fallback timed out after {timeout_seconds:g} seconds")

    ok, value = results.get_nowait()
    if ok:
        return value  # type: ignore[return-value]
    raise value


def format_attachment_ai_response(response_text: str) -> str:
    payload = parse_attachment_ai_json_response(response_text)
    if not payload:
        return response_text.strip()
    if not attachment_ai_response_is_useful(payload):
        return ""

    scene_summary = attachment_ai_response_field(payload, "scene_summary", "summary")
    visible_text = payload.get("visible_text", "")
    visible_text_value = attachment_ai_response_text(visible_text, separator="\n")
    likely_document_type = attachment_ai_response_field(payload, "likely_document_type")
    document_context = attachment_ai_response_field(payload, "document_context")
    search_keywords = attachment_ai_response_field(payload, "search_keywords", "useful_for_search")

    return "\n\n".join(
        part
        for part in (
            "AI attachment extraction",
            f"Scene: {scene_summary}".strip(),
            f"Likely document type: {likely_document_type}".strip(),
            f"Document context: {document_context}".strip(),
            f"Visible text:\n{visible_text_value}".strip(),
            f"Search keywords: {search_keywords}".strip(),
        )
        if part and not part.endswith(":")
    )


def parse_attachment_ai_json_response(response_text: str) -> dict[str, object] | None:
    text = response_text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    if isinstance(payload, str) and payload.strip() != text:
        return parse_attachment_ai_json_response(payload)
    if isinstance(payload, dict):
        return payload
    return None


def attachment_ai_response_is_useful(payload: Mapping[str, object]) -> bool:
    if attachment_ai_response_is_generic_placeholder(payload):
        return False
    value = payload.get("is_useful")
    if isinstance(value, bool):
        return value or attachment_ai_response_has_indexable_content(payload)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"false", "no", "0"}:
            return attachment_ai_response_has_indexable_content(payload)
        if normalized in {"true", "yes", "1"}:
            return True
    return True


def attachment_ai_response_is_generic_placeholder(payload: Mapping[str, object]) -> bool:
    summary = attachment_ai_response_field(payload, "scene_summary", "summary")
    visible_text = attachment_ai_response_text(payload.get("visible_text", ""))
    likely_document_type = attachment_ai_response_field(payload, "likely_document_type")
    document_context = attachment_ai_response_field(payload, "document_context")
    useful_for_search = attachment_ai_response_field(payload, "search_keywords", "useful_for_search")
    combined = " ".join(
        part
        for part in (summary, visible_text, likely_document_type, document_context, useful_for_search)
        if part
    ).lower()
    if not combined:
        return False
    if not re.search(r"\b(placeholder|no discernible content|no useful content|generic gmail attachment)\b", combined):
        return False

    searchable = " ".join(part for part in (visible_text, useful_for_search) if part).lower()
    searchable_words = set(re.findall(r"[a-z0-9]+", searchable))
    generic_words = {"gmail", "attachment", "image", "placeholder", "unknown", "none", "no", "content"}
    return searchable_words <= generic_words


def attachment_ai_response_field(payload: Mapping[str, object], *names: str) -> str:
    for name in names:
        text = attachment_ai_response_text(payload.get(name, ""))
        if text:
            return text
    return ""


def attachment_ai_response_text(value: object, *, separator: str = " ") -> str:
    if isinstance(value, list):
        return separator.join(str(item).strip() for item in value if str(item).strip())
    return str(value).strip()


def attachment_ai_response_has_indexable_content(payload: Mapping[str, object]) -> bool:
    values = [
        payload.get("scene_summary", ""),
        payload.get("summary", ""),
        payload.get("visible_text", ""),
        payload.get("likely_document_type", ""),
        payload.get("document_context", ""),
        payload.get("search_keywords", ""),
        payload.get("useful_for_search", ""),
    ]
    for value in values:
        text = attachment_ai_response_text(value)
        if re.search(r"[A-Za-z0-9]", text):
            return True
    return False


def raw_attachment_text(
    *,
    content: bytes,
    mime_type: str,
    filename: str,
    max_chars: int,
    archive_depth: int = 0,
) -> str | None:
    extension = Path(filename.lower()).suffix
    if mime_type == "text/html" or extension in {".html", ".htm"}:
        return html_attachment_text(content)
    if is_plain_text_attachment(mime_type=mime_type, extension=extension):
        return decode_text_bytes(content)
    if mime_type == "application/pdf" or extension == ".pdf":
        return pdf_attachment_text(content)
    if extension in {".docx", ".pptx", ".xlsx"} or mime_type in {
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }:
        return office_open_xml_attachment_text(content)
    if is_zip_attachment(mime_type=mime_type, extension=extension):
        return zip_attachment_text(
            content=content,
            max_chars=max_chars,
            archive_depth=archive_depth,
        )
    if looks_like_text(content):
        return decode_text_bytes(content)
    return None


def is_zip_attachment(*, mime_type: str, extension: str) -> bool:
    return mime_type in ZIP_MIME_TYPES or extension in ZIP_EXTENSIONS


def is_plain_text_attachment(*, mime_type: str, extension: str) -> bool:
    if mime_type.startswith("text/"):
        return True
    return mime_type in {
        "application/json",
        "application/ld+json",
        "application/xml",
        "application/yaml",
        "application/x-yaml",
        "application/javascript",
        "application/x-sh",
        "message/rfc822",
    } or extension in {
        ".txt",
        ".text",
        ".md",
        ".markdown",
        ".csv",
        ".tsv",
        ".json",
        ".jsonl",
        ".xml",
        ".yaml",
        ".yml",
        ".ics",
        ".log",
        ".py",
        ".js",
        ".jsx",
        ".ts",
        ".tsx",
        ".go",
        ".rs",
        ".rb",
        ".php",
        ".java",
        ".c",
        ".cc",
        ".cpp",
        ".h",
        ".hpp",
        ".sql",
        ".sh",
        ".toml",
        ".ini",
        ".cfg",
        ".conf",
    }


def html_attachment_text(content: bytes) -> str:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
        soup = BeautifulSoup(decode_text_bytes(content), "html.parser")
    return soup.get_text("\n")


def pdf_attachment_text(content: bytes) -> str:
    if not looks_like_pdf(content):
        if looks_like_text(content):
            return decode_text_bytes(content)
        raise AttachmentTextUnavailable(
            status="unsupported",
            error="attachment is labeled as a PDF but does not contain PDF data",
        )

    from pypdf import PdfReader
    from pypdf.errors import FileNotDecryptedError, PdfReadError, PdfStreamError

    try:
        reader = PdfReader(BytesIO(content))
        if reader.is_encrypted:
            decrypt_result = reader.decrypt("")
            if decrypt_result == 0:
                raise AttachmentTextUnavailable(
                    status="encrypted",
                    error="PDF is encrypted and could not be decrypted with an empty password",
                )
        return "\n\n".join(page.extract_text() or "" for page in reader.pages)
    except AttachmentTextUnavailable:
        raise
    except FileNotDecryptedError as exc:
        raise AttachmentTextUnavailable(status="encrypted", error=str(exc)) from exc
    except PdfReadError as exc:
        raise AttachmentTextUnavailable(status="invalid_pdf", error=str(exc)) from exc
    except PdfStreamError as exc:
        raise AttachmentTextUnavailable(status="invalid_pdf", error=str(exc)) from exc


def office_open_xml_attachment_text(content: bytes) -> str:
    if not zipfile.is_zipfile(BytesIO(content)):
        if looks_like_text(content):
            return decode_text_bytes(content)
        raise AttachmentTextUnavailable(
            status="invalid_office_document",
            error="Office Open XML attachment is not a valid zip container",
        )

    text_parts: list[str] = []
    with zipfile.ZipFile(BytesIO(content)) as archive:
        for name in sorted(archive.namelist()):
            if not should_extract_office_xml(name):
                continue
            try:
                root = ET.fromstring(archive.read(name))
            except ET.ParseError:
                continue
            for element in root.iter():
                if element.text and element.text.strip():
                    text_parts.append(element.text.strip())
    return "\n".join(text_parts)


def zip_attachment_text(*, content: bytes, max_chars: int, archive_depth: int = 0) -> str | None:
    if archive_depth > ZIP_MAX_RECURSION_DEPTH:
        return None
    if not zipfile.is_zipfile(BytesIO(content)):
        if looks_like_text(content):
            return decode_text_bytes(content)
        raise AttachmentTextUnavailable(status="invalid_archive", error="ZIP attachment is not a valid archive")

    text_parts: list[str] = []
    total_uncompressed_bytes = 0
    read_errors: list[str] = []
    encrypted_members = 0
    try:
        archive = zipfile.ZipFile(BytesIO(content))
    except zipfile.BadZipFile as exc:
        raise AttachmentTextUnavailable(status="invalid_archive", error=str(exc)) from exc

    with archive:
        try:
            infos = archive.infolist()
        except (EOFError, RuntimeError, zipfile.BadZipFile, zlib.error) as exc:
            raise AttachmentTextUnavailable(status="invalid_archive", error=str(exc)) from exc

        for member_index, info in enumerate(infos):
            if member_index >= ZIP_MAX_MEMBERS or len("\n\n".join(text_parts)) >= max_chars:
                break
            if info.is_dir():
                continue
            if info.flag_bits & 0x1:
                encrypted_members += 1
                continue
            if info.file_size > ZIP_MAX_MEMBER_BYTES:
                continue
            total_uncompressed_bytes += info.file_size
            if total_uncompressed_bytes > ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES:
                break

            member_name = info.filename
            try:
                member_content = archive.read(info)
            except (EOFError, RuntimeError, zipfile.BadZipFile, zlib.error) as exc:
                read_errors.append(f"{member_name}: {exc}")
                continue

            try:
                member_text = raw_attachment_text(
                    content=member_content,
                    mime_type=mime_type_from_filename(member_name),
                    filename=member_name,
                    max_chars=max_chars,
                    archive_depth=archive_depth + 1,
                )
            except AttachmentTextUnavailable:
                continue
            if not member_text:
                continue
            remaining_chars = max_chars - len("\n\n".join(text_parts))
            if remaining_chars <= 0:
                break
            marker = f"## {member_name}"
            body = normalize_markdown(member_text)
            if not body:
                continue
            section = f"{marker}\n\n{body}"
            text_parts.append(section[:remaining_chars])

    if not text_parts:
        if read_errors:
            raise AttachmentTextUnavailable(
                status="invalid_archive",
                error="Could not read text from ZIP members: " + "; ".join(read_errors[:5]),
            )
        if encrypted_members:
            raise AttachmentTextUnavailable(
                status="encrypted",
                error=f"ZIP contains {encrypted_members} encrypted member(s)",
            )
        return None
    return "\n\n".join(text_parts)


def mime_type_from_filename(filename: str) -> str:
    extension = Path(filename.lower()).suffix
    if extension in {".html", ".htm"}:
        return "text/html"
    if extension == ".pdf":
        return "application/pdf"
    if extension == ".docx":
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if extension == ".pptx":
        return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    if extension == ".xlsx":
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if extension in ZIP_EXTENSIONS:
        return "application/zip"
    return ""


def should_extract_office_xml(name: str) -> bool:
    if not name.endswith(".xml"):
        return False
    return name.startswith(
        (
            "word/",
            "ppt/slides/",
            "ppt/notesSlides/",
            "xl/sharedStrings.xml",
            "xl/worksheets/",
            "docProps/",
        )
    )


def decode_text_bytes(content: bytes) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        pass
    if content.startswith((b"\xff\xfe", b"\xfe\xff")):
        try:
            return content.decode("utf-16")
        except UnicodeDecodeError:
            pass
    return content.decode("latin-1")


def looks_like_text(content: bytes) -> bool:
    sample = content[:4096]
    if b"\x00" in sample:
        return False
    try:
        sample.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


def looks_like_pdf(content: bytes) -> bool:
    return content.lstrip()[:5] == b"%PDF-"


def attachment_part_json(part: Mapping[str, object]) -> str:
    body = part_body(part)
    scrubbed_body = dict(body)
    if "data" in scrubbed_body:
        scrubbed_body["data"] = "<redacted>"
    scrubbed_part = dict(part)
    scrubbed_part["body"] = scrubbed_body
    return json.dumps(scrubbed_part, sort_keys=True, separators=(",", ":"))


def normalized_mime_type(value: str) -> str:
    return value.split(";", 1)[0].strip().lower()


def truncate_error(value: str, limit: int = 2000) -> str:
    if len(value) <= limit:
        return value
    return value[:limit]


def internal_date_from_message(message: Mapping[str, object]) -> datetime:
    value = message.get("internalDate")
    if value is None:
        return EPOCH_UTC
    return datetime.fromtimestamp(int(value) / 1000, tz=UTC)


def header_map(headers: object) -> dict[str, str]:
    result: dict[str, str] = {}
    if not isinstance(headers, Sequence):
        return result
    for header in headers:
        if not isinstance(header, Mapping):
            continue
        name = str(header.get("name", "")).strip().lower()
        if not name:
            continue
        result[name] = str(header.get("value", ""))
    return result


def parse_address_list(value: str) -> list[str]:
    addresses = []
    for _, address in getaddresses([value]):
        addresses.append(address or "")
    return [address for address in addresses if address]


def extract_message_bodies(payload: Mapping[str, object]) -> tuple[str, str]:
    text_parts: list[str] = []
    html_parts: list[str] = []

    for part in walk_parts(payload):
        mime_type = str(part.get("mimeType", ""))
        body = part.get("body", {})
        if not isinstance(body, Mapping):
            continue
        data = decode_base64url(body.get("data"))
        if not data:
            continue
        if mime_type == "text/plain":
            text_parts.append(data)
        elif mime_type == "text/html":
            html_parts.append(data)

    return ("\n\n".join(text_parts), "\n\n".join(html_parts))


def message_body_to_markdown(*, body_text: str, body_html: str) -> str:
    if body_html.strip():
        return html_fragment_to_markdown(body_html)
    return normalize_markdown(body_text)


def collapsed_message_body_to_markdown(*, body_text: str, body_html: str) -> str:
    if body_html.strip():
        collapsed_html = remove_quoted_html(body_html)
        collapsed_markdown = html_fragment_to_markdown(collapsed_html)
        if collapsed_markdown:
            return strip_quoted_history(collapsed_markdown)
    return strip_quoted_history(body_text)


def safe_message_body_to_markdown(*, body_text: str, body_html: str) -> str:
    try:
        return message_body_to_markdown(body_text=body_text, body_html=body_html)
    except RecursionError:
        return normalize_markdown(body_text)


def safe_collapsed_message_body_to_markdown(*, body_text: str, body_html: str) -> str:
    try:
        return collapsed_message_body_to_markdown(body_text=body_text, body_html=body_html)
    except RecursionError:
        return strip_quoted_history(body_text)


def html_fragment_to_markdown(body_html: str) -> str:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
        return normalize_markdown(html_to_markdown(body_html, heading_style="ATX"))


def remove_quoted_html(body_html: str) -> str:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
        soup = BeautifulSoup(body_html, "html.parser")

    selectors = [
        ".gmail_quote",
        ".gmail_attr",
        ".yahoo_quoted",
        ".moz-cite-prefix",
        ".OutlookMessageHeader",
        "#divRplyFwdMsg",
        "blockquote",
    ]
    for selector in selectors:
        for element in soup.select(selector):
            element.decompose()

    for element in soup.find_all(["hr"]):
        element.decompose()

    return str(soup)


def strip_quoted_history(markdown: str) -> str:
    lines: list[str] = []
    quote_start_patterns = (
        re.compile(r"^On .+ wrote:$", re.IGNORECASE),
        re.compile(r"^On .+ at .+, .+ wrote:$", re.IGNORECASE),
        re.compile(r"^Le .+ a écrit\s?:$", re.IGNORECASE),
        re.compile(r"^-+ ?Original Message ?-+$", re.IGNORECASE),
        re.compile(r"^_{2,}$"),
        re.compile(r"^\*{2,}$"),
        re.compile(r"^From:\s.+", re.IGNORECASE),
        re.compile(r"^Sent:\s.+", re.IGNORECASE),
        re.compile(r"^Date:\s.+", re.IGNORECASE),
        re.compile(r"^To:\s.+", re.IGNORECASE),
        re.compile(r"^Cc:\s.+", re.IGNORECASE),
        re.compile(r"^Subject:\s.+", re.IGNORECASE),
        re.compile(r"^Begin forwarded message:$", re.IGNORECASE),
        re.compile(r"^Forwarded message$", re.IGNORECASE),
    )
    for raw_line in markdown.splitlines():
        line = raw_line.rstrip()
        if line.lstrip().startswith(">"):
            continue
        if any(pattern.match(line.strip()) for pattern in quote_start_patterns):
            break
        lines.append(line)
    return normalize_markdown("\n".join(lines))


def normalize_markdown(value: str) -> str:
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def walk_parts(payload: Mapping[str, object]) -> Iterator[Mapping[str, object]]:
    stack: list[Mapping[str, object]] = [payload]
    while stack:
        part = stack.pop()
        yield part
        parts = part.get("parts", [])
        if not isinstance(parts, Sequence):
            continue
        stack.extend(child for child in reversed(parts) if isinstance(child, Mapping))


def decode_base64url(value: object) -> str:
    return decode_base64url_bytes(value).decode("utf-8", errors="replace")


def decode_base64url_bytes(value: object) -> bytes:
    if not value:
        return b""
    encoded = str(value)
    padding = "=" * (-len(encoded) % 4)
    return base64.urlsafe_b64decode(encoded + padding)


def chunked(values: Sequence[str], size: int) -> Iterator[list[str]]:
    for index in range(0, len(values), size):
        yield list(values[index : index + size])


def _http_status(exc: HttpError) -> int | None:
    return getattr(exc.resp, "status", None)


TRANSIENT_GMAIL_EXCEPTIONS = (ConnectionError, TimeoutError, OSError, ssl.SSLError)


def execute_gmail_request(request_fn: Callable[[], T], *, max_attempts: int = 5) -> T:
    for attempt in range(1, max_attempts + 1):
        try:
            return request_fn()
        except HttpError as exc:
            status = _http_status(exc)
            if status not in {429, 500, 502, 503, 504} or attempt == max_attempts:
                raise
            time.sleep(min(2 ** (attempt - 1), 30))
        except TRANSIENT_GMAIL_EXCEPTIONS:
            if attempt == max_attempts:
                raise
            time.sleep(min(2 ** (attempt - 1), 30))
    raise RuntimeError("unreachable Gmail retry state")
