from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
import base64
import fcntl
import hashlib
import json
import logging
import mimetypes
import os
import re
import ssl
from email.utils import getaddresses, parseaddr
from io import BytesIO
from pathlib import Path
import tempfile
import threading
import time
from typing import Callable, Protocol, TypeVar
import zipfile
import xml.etree.ElementTree as ET

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from markdownify import markdownify as html_to_markdown
import warnings
import zlib

from personal_data_warehouse.config import GmailAccount, Settings, env_slug
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.warehouse import warehouse_from_settings
from personal_data_warehouse.schema import SyncState
from personal_data_warehouse.google_auth import google_token_json_from_env, load_google_credentials
from personal_data_warehouse.objectstore import ObjectStore, StoredObject

LOGGER = logging.getLogger(__name__)
EPOCH_UTC = datetime.fromtimestamp(0, tz=UTC)
DEFAULT_GMAIL_SYNC_LOCK_PATH = Path(tempfile.gettempdir()) / "personal-data-warehouse-gmail-sync.lock"
GMAIL_SYNC_POSTGRES_LOCK_ID = 7_403_111_836
ZIP_MIME_TYPES = {"application/zip", "application/x-zip-compressed"}
ZIP_EXTENSIONS = {".zip"}
ZIP_MAX_MEMBERS = 200
ZIP_MAX_MEMBER_BYTES = 25 * 1024 * 1024
ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES = 50 * 1024 * 1024
ZIP_MAX_RECURSION_DEPTH = 1
GMAIL_ATTACHMENT_STORAGE_SOURCE = "gmail_attachments"
GMAIL_ATTACHMENT_STORAGE_KIND = "gmail_attachment"
GMAIL_ATTACHMENT_STORAGE_METADATA_KIND = "gmail_attachment_metadata"
GMAIL_ATTACHMENT_STORAGE_PREFIX = "gmail-attachments"
GMAIL_ATTACHMENT_STORAGE_STAGE = "library"
GMAIL_ATTACHMENT_STORAGE_MAX_EXTENSION_LEN = 16
T = TypeVar("T")


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
    attachments_stored: int = 0


@dataclass(frozen=True)
class AttachmentTextExtraction:
    text: str
    status: str
    error: str = ""


class AttachmentEnrichmentCache(Protocol):
    def get(self, content_sha256: str) -> AttachmentTextExtraction | None: ...

    def put(self, content_sha256: str, extraction: AttachmentTextExtraction) -> None: ...


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
        warehouse: PostgresWarehouse,
        logger,
        attachment_object_store_factory: Callable[[GmailAccount], ObjectStore | None] | None = None,
    ) -> None:
        self._settings = settings
        self._warehouse = warehouse
        self._logger = logger
        self._attachment_object_store_factory = attachment_object_store_factory

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
        object_store = self._attachment_object_store(account)
        if self._settings.gmail_force_full_sync or not state or state.last_history_id == 0:
            summary = self._full_sync(account=account, service=service, object_store=object_store)
            return self._with_attachment_backfill(
                account=account, service=service, summary=summary, object_store=object_store
            )

        try:
            summary = self._partial_sync(
                account=account,
                service=service,
                start_history_id=state.last_history_id,
                object_store=object_store,
            )
            return self._with_attachment_backfill(
                account=account, service=service, summary=summary, object_store=object_store
            )
        except HttpError as exc:
            if _http_status(exc) != 404:
                raise
            self._logger.warning(
                "History cursor for %s is stale at %s, falling back to full sync",
                account.email_address,
                state.last_history_id,
            )
            summary = self._full_sync(account=account, service=service, object_store=object_store)
            return self._with_attachment_backfill(
                account=account, service=service, summary=summary, object_store=object_store
            )

    def _attachment_object_store(self, account: GmailAccount) -> ObjectStore | None:
        if self._attachment_object_store_factory is None:
            return None
        try:
            return self._attachment_object_store_factory(account)
        except Exception as exc:
            self._logger.warning(
                "Skipping Gmail attachment object store for %s because it could not be built: %s",
                account.email_address,
                exc,
            )
            return None

    def _full_sync(
        self, *, account: GmailAccount, service, object_store: ObjectStore | None = None
    ) -> MailboxSyncSummary:
        sync_started_at = datetime.now(tz=UTC)
        next_history_id = current_history_id(service)
        messages_written = 0
        attachments_written = 0
        attachment_text_chars = 0
        attachments_stored = 0

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
                enrichment_cache=self._attachment_enrichment_cache(),
                object_store=object_store,
            )
            self._warehouse.insert_attachments(attachment_rows)
            attachments_written += len(attachment_rows)
            attachment_text_chars += sum(len(str(row["text"])) for row in attachment_rows)
            attachments_stored += count_attachments_stored(attachment_rows)

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
            attachments_stored=attachments_stored,
        )

    def _partial_sync(
        self, *, account: GmailAccount, service, start_history_id: int, object_store: ObjectStore | None = None
    ) -> MailboxSyncSummary:
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
        attachments_stored = 0

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
            enrichment_cache = self._attachment_enrichment_cache()
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
                            enrichment_cache=enrichment_cache,
                            object_store=object_store,
                        )
                    )
            self._warehouse.insert_messages(rows)
            self._warehouse.insert_attachments(attachment_rows)
            messages_written += len(rows)
            attachments_written += len(attachment_rows)
            attachment_text_chars += sum(len(str(row["text"])) for row in attachment_rows)
            attachments_stored += count_attachments_stored(attachment_rows)
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
            attachments_stored=attachments_stored,
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
        enrichment_cache: AttachmentEnrichmentCache | None = None,
        object_store: ObjectStore | None = None,
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
                    enrichment_cache=enrichment_cache,
                    object_store=object_store,
                )
            )
        return rows

    def _with_attachment_backfill(
        self,
        *,
        account: GmailAccount,
        service,
        summary: MailboxSyncSummary,
        object_store: ObjectStore | None = None,
    ) -> MailboxSyncSummary:
        try:
            candidates, rows_written, text_chars, stored = self._backfill_attachment_candidates(
                account=account,
                service=service,
                object_store=object_store,
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
            attachments_stored=summary.attachments_stored + stored,
        )

    def _backfill_attachment_candidates(
        self, *, account: GmailAccount, service, object_store: ObjectStore | None = None
    ) -> tuple[int, int, int, int]:
        limit = self._settings.gmail_attachment_backfill_batch_size
        if limit <= 0:
            return (0, 0, 0, 0)

        messages = self._warehouse.load_attachment_backfill_candidate_messages(
            account=account.email_address,
            limit=limit,
            include_storage_pending=object_store is not None,
            storage_max_bytes=self._settings.gmail_attachment_max_bytes,
        )
        messages = [message for message in messages if str(message.get("id", ""))]
        if not messages:
            return (0, 0, 0, 0)

        candidates = len(messages)
        concurrency = max(1, self._settings.gmail_attachment_backfill_concurrency)
        if concurrency > 1 and len(messages) > 1 and self._supports_parallel_backfill():
            rows_written, text_chars, stored = self._backfill_messages_parallel(
                account=account,
                messages=messages,
                object_store=object_store,
                concurrency=min(concurrency, len(messages)),
            )
        else:
            rows_written, text_chars, stored = self._backfill_messages_serial(
                account=account,
                service=service,
                messages=messages,
                object_store=object_store,
            )

        self._logger.info(
            "Backfilled Gmail attachments for %s: %s candidates, %s rows, %s text chars, %s blobs stored",
            account.email_address,
            candidates,
            rows_written,
            text_chars,
            stored,
        )
        return (candidates, rows_written, text_chars, stored)

    def _supports_parallel_backfill(self) -> bool:
        return isinstance(self._warehouse, PostgresWarehouse)

    def _open_worker_warehouse(self) -> PostgresWarehouse:
        return warehouse_from_settings(self._settings)

    def _backfill_messages_serial(
        self,
        *,
        account: GmailAccount,
        service,
        messages: list[Mapping[str, object]],
        object_store: ObjectStore | None,
    ) -> tuple[int, int, int]:
        enrichment_cache = self._attachment_enrichment_cache()
        rows_written = 0
        text_chars = 0
        stored = 0
        for message in messages:
            written, chars, blobs = self._backfill_message(
                account=account,
                message=message,
                service=service,
                object_store=object_store,
                warehouse=self._warehouse,
                enrichment_cache=enrichment_cache,
            )
            rows_written += written
            text_chars += chars
            stored += blobs
        return (rows_written, text_chars, stored)

    def _backfill_messages_parallel(
        self,
        *,
        account: GmailAccount,
        messages: list[Mapping[str, object]],
        object_store: ObjectStore | None,
        concurrency: int,
    ) -> tuple[int, int, int]:
        # Google API clients and psycopg2 connections are not thread-safe, so each
        # worker thread gets its own warehouse, Gmail service, Drive store, and
        # enrichment cache. wants_object_store mirrors the caller so that
        # storage-pending candidates still get their blobs uploaded.
        thread_local = threading.local()
        opened: list[PostgresWarehouse] = []
        opened_lock = threading.Lock()
        wants_object_store = object_store is not None

        def resources():
            existing = getattr(thread_local, "backfill", None)
            if existing is not None:
                return existing
            warehouse = self._open_worker_warehouse()
            with opened_lock:
                opened.append(warehouse)
            worker_service = build_gmail_service(account=account, settings=self._settings)
            store = self._attachment_object_store(account) if wants_object_store else None
            cache = self._attachment_enrichment_cache(warehouse)
            built = (warehouse, worker_service, store, cache)
            thread_local.backfill = built
            return built

        def process(message: Mapping[str, object]) -> tuple[int, int, int]:
            warehouse, worker_service, store, cache = resources()
            return self._backfill_message(
                account=account,
                message=message,
                service=worker_service,
                object_store=store,
                warehouse=warehouse,
                enrichment_cache=cache,
            )

        try:
            with ThreadPoolExecutor(
                max_workers=concurrency, thread_name_prefix="gmail-backfill"
            ) as pool:
                results = list(pool.map(process, messages))
        finally:
            with opened_lock:
                worker_warehouses = list(opened)
            for warehouse in worker_warehouses:
                try:
                    warehouse.close()
                except Exception:
                    pass

        rows_written = sum(written for written, _chars, _blobs in results)
        text_chars = sum(chars for _written, chars, _blobs in results)
        stored = sum(blobs for _written, _chars, blobs in results)
        return (rows_written, text_chars, stored)

    def _backfill_message(
        self,
        *,
        account: GmailAccount,
        message: Mapping[str, object],
        service,
        object_store: ObjectStore | None,
        warehouse,
        enrichment_cache: AttachmentEnrichmentCache | None,
    ) -> tuple[int, int, int]:
        message_id = str(message.get("id", ""))
        synced_at = datetime.now(tz=UTC)
        try:
            existing_keys = warehouse.existing_attachment_keys(
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
                enrichment_cache=enrichment_cache,
                object_store=object_store,
            )
            warehouse.insert_attachments(rows)
            warehouse.insert_attachment_backfill_state(
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
            warehouse.insert_attachment_backfill_state(
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
            return (0, 0, 0)

        return (
            len(rows),
            sum(len(str(row["text"])) for row in rows),
            count_attachments_stored(rows),
        )

    def _attachment_enrichment_cache(self, warehouse=None) -> AttachmentEnrichmentCache | None:
        warehouse = warehouse if warehouse is not None else self._warehouse
        if not hasattr(warehouse, "load_attachment_enrichments") or not hasattr(
            warehouse, "insert_attachment_enrichments"
        ):
            return None
        return WarehouseAttachmentEnrichmentCache(warehouse=warehouse)


class WarehouseAttachmentEnrichmentCache:
    """Content-addressed cache of deterministic text extraction results.

    Rows are keyed by (content_sha256, ai_provider, ai_model, ai_prompt_version);
    sync-time deterministic extraction always uses the empty AI identity. Agent
    vision enrichment writes its own identity rows separately (see
    gmail_attachment_enrichment.py) and is intentionally not consulted here.
    """

    _MISSING = object()

    def __init__(self, *, warehouse) -> None:
        self._warehouse = warehouse
        self._cache: dict[str, AttachmentTextExtraction | object] = {}

    def get(self, content_sha256: str) -> AttachmentTextExtraction | None:
        if not content_sha256:
            return None
        cached = self._cache.get(content_sha256)
        if cached is self._MISSING:
            return None
        if isinstance(cached, AttachmentTextExtraction):
            return cached

        rows = self._warehouse.load_attachment_enrichments(
            content_sha256s=[content_sha256],
            ai_provider="",
            ai_model="",
            ai_prompt_version="",
        )
        row = rows.get(content_sha256)
        if row is None:
            self._cache[content_sha256] = self._MISSING
            return None
        extraction = attachment_text_extraction_from_enrichment_row(row)
        self._cache[content_sha256] = extraction
        return extraction

    def put(self, content_sha256: str, extraction: AttachmentTextExtraction) -> None:
        if not content_sha256 or not attachment_enrichment_is_cacheable(extraction):
            return
        self._cache[content_sha256] = extraction
        self._warehouse.insert_attachment_enrichments(
            [
                attachment_enrichment_row(
                    content_sha256=content_sha256,
                    extraction=extraction,
                    updated_at=datetime.now(tz=UTC),
                )
            ]
        )


def attachment_enrichment_is_cacheable(extraction: AttachmentTextExtraction) -> bool:
    return extraction.status not in {
        "",
        "disabled",
        "fetch_error",
        "too_large",
        "no_content",
    }


def attachment_text_extraction_from_enrichment_row(row: Mapping[str, object]) -> AttachmentTextExtraction:
    return AttachmentTextExtraction(
        text=str(row.get("text", "")),
        status=str(row.get("text_extraction_status", "")),
        error=str(row.get("text_extraction_error", "")),
    )


def attachment_enrichment_row(
    *,
    content_sha256: str,
    extraction: AttachmentTextExtraction,
    updated_at: datetime,
) -> dict[str, object]:
    return {
        "content_sha256": content_sha256,
        "ai_provider": "",
        "ai_model": "",
        "ai_prompt_version": "",
        "text": extraction.text,
        "text_extraction_status": extraction.status,
        "text_extraction_error": extraction.error,
        "ai_base_url": "",
        "ai_prompt_sha256": "",
        "ai_prompt": "",
        "ai_source_status": "",
        "ai_elapsed_ms": 0,
        "ai_processed_at": EPOCH_UTC,
        "updated_at": updated_at,
        "sync_version": int(updated_at.timestamp() * 1000),
    }


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
    enrichment_cache: AttachmentEnrichmentCache | None = None,
    object_store: ObjectStore | None = None,
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
                enrichment_cache=enrichment_cache,
                object_store=object_store,
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
        "storage_backend": "",
        "storage_key": "",
        "storage_file_id": "",
        "storage_url": "",
        "storage_status": "",
        "text": "",
        "text_extraction_status": "deleted",
        "text_extraction_error": "",
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
        "ai_provider": "",
        "ai_model": "",
        "ai_prompt_version": "",
        "updated_at": updated_at,
        "sync_version": int(updated_at.timestamp() * 1000),
    }


def gmail_attachment_extension(*, filename: str, mime_type: str) -> str:
    suffix = Path(filename).suffix
    if not suffix and mime_type:
        suffix = mimetypes.guess_extension(mime_type.split(";", 1)[0].strip()) or ""
    suffix = "".join(char for char in suffix if char.isalnum() or char == ".")
    if len(suffix) > GMAIL_ATTACHMENT_STORAGE_MAX_EXTENSION_LEN:
        return ""
    return suffix.lower()


def gmail_attachment_object_key(
    *,
    internal_date: datetime,
    content_sha256: str,
    filename: str,
    mime_type: str,
    stage: str = GMAIL_ATTACHMENT_STORAGE_STAGE,
) -> str:
    moment = internal_date.astimezone(UTC)
    date_prefix = moment.strftime("%Y/%m/%Y-%m-%d")
    extension = gmail_attachment_extension(filename=filename, mime_type=mime_type)
    return f"{GMAIL_ATTACHMENT_STORAGE_PREFIX}/{stage}/{date_prefix}-{content_sha256}{extension}"


def store_attachment_blob(
    *,
    object_store: ObjectStore,
    content: bytes,
    content_sha256: str,
    mime_type: str,
    filename: str,
    internal_date: datetime,
    account: str,
    message_id: str,
    part_id: str,
    attachment_id: str,
    stage: str = GMAIL_ATTACHMENT_STORAGE_STAGE,
) -> StoredObject:
    object_key = gmail_attachment_object_key(
        internal_date=internal_date,
        content_sha256=content_sha256,
        filename=filename,
        mime_type=mime_type,
        stage=stage,
    )
    app_properties = {
        "gmail_account": account,
        "gmail_message_id": message_id,
        "gmail_part_id": part_id,
    }
    if attachment_id:
        app_properties["gmail_attachment_id_sha256"] = hashlib.sha256(
            attachment_id.encode("utf-8")
        ).hexdigest()
    with tempfile.NamedTemporaryFile(prefix="pdw-gmail-attachment-") as handle:
        handle.write(content)
        handle.flush()
        return object_store.put_file(
            path=Path(handle.name),
            object_key=object_key,
            content_sha256=content_sha256,
            content_type=mime_type or "application/octet-stream",
            kind=GMAIL_ATTACHMENT_STORAGE_KIND,
            app_properties=app_properties,
        )


def count_attachments_stored(rows: Sequence[Mapping[str, object]]) -> int:
    return sum(1 for row in rows if row.get("storage_status") == "stored")


def attachment_part_to_row(
    *,
    account: str,
    service,
    message: Mapping[str, object],
    part: Mapping[str, object],
    synced_at: datetime,
    max_bytes: int,
    text_max_chars: int,
    enrichment_cache: AttachmentEnrichmentCache | None = None,
    object_store: ObjectStore | None = None,
) -> dict[str, object]:
    message_id = str(message.get("id", ""))
    headers = header_map(part.get("headers", []))
    body = part_body(part)
    attachment_id = str(body.get("attachmentId", ""))
    filename = str(part.get("filename", ""))
    mime_type = normalized_mime_type(str(part.get("mimeType", "")))
    declared_size = int(body.get("size", 0) or 0)
    internal_date = internal_date_from_message(message)
    content_sha256 = ""
    storage_backend = ""
    storage_key = ""
    storage_file_id = ""
    storage_url = ""
    storage_status = ""

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
                if object_store is not None:
                    try:
                        stored = store_attachment_blob(
                            object_store=object_store,
                            content=content,
                            content_sha256=content_sha256,
                            mime_type=mime_type,
                            filename=filename,
                            internal_date=internal_date,
                            account=account,
                            message_id=message_id,
                            part_id=str(part.get("partId", "")),
                            attachment_id=attachment_id,
                        )
                    except Exception as exc:
                        storage_status = "upload_error"
                        LOGGER.warning(
                            "gmail attachment blob upload failed for %s/%s: %s",
                            message_id,
                            part.get("partId", ""),
                            truncate_error(str(exc)),
                        )
                    else:
                        storage_backend = stored["storage_backend"]
                        storage_key = stored["storage_key"]
                        storage_file_id = stored["storage_file_id"]
                        storage_url = stored["storage_url"]
                        storage_status = "stored"
                cached_extraction = enrichment_cache.get(content_sha256) if enrichment_cache is not None else None
                if cached_extraction is not None:
                    extraction = cached_extraction
                else:
                    extraction = extract_attachment_text(
                        content=content,
                        mime_type=mime_type,
                        filename=filename,
                        max_chars=text_max_chars,
                    )
                    if enrichment_cache is not None:
                        enrichment_cache.put(content_sha256, extraction)

    sync_version = int(synced_at.timestamp() * 1000)
    return {
        "account": account,
        "message_id": message_id,
        "thread_id": str(message.get("threadId", "")),
        "history_id": int(message.get("historyId", 0)),
        "internal_date": internal_date,
        "part_id": str(part.get("partId", "")),
        "attachment_id": attachment_id,
        "filename": filename,
        "mime_type": mime_type,
        "content_id": headers.get("content-id", ""),
        "content_disposition": headers.get("content-disposition", ""),
        "size": declared_size,
        "content_sha256": content_sha256,
        "storage_backend": storage_backend,
        "storage_key": storage_key,
        "storage_file_id": storage_file_id,
        "storage_url": storage_url,
        "storage_status": storage_status,
        "text": extraction.text,
        "text_extraction_status": extraction.status,
        "text_extraction_error": extraction.error,
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
