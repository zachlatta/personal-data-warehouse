from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
import base64
import fcntl
import json
import os
import re
from email.utils import getaddresses, parseaddr
from pathlib import Path
import tempfile
import time
from typing import Callable, TypeVar

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from markdownify import markdownify as html_to_markdown
import warnings

from personal_data_warehouse.clickhouse import ClickHouseWarehouse, SyncState
from personal_data_warehouse.config import GmailAccount, Settings, env_slug

EPOCH_UTC = datetime.fromtimestamp(0, tz=UTC)
DEFAULT_GMAIL_SYNC_LOCK_PATH = Path(tempfile.gettempdir()) / "personal-data-warehouse-gmail-sync.lock"
GMAIL_SYNC_POSTGRES_LOCK_ID = 7_403_111_836
T = TypeVar("T")


@dataclass(frozen=True)
class MailboxSyncSummary:
    account: str
    sync_type: str
    next_history_id: int
    messages_written: int
    deleted_messages: int
    query: str | None


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
    def __init__(self, *, settings: Settings, warehouse: ClickHouseWarehouse, logger) -> None:
        self._settings = settings
        self._warehouse = warehouse
        self._logger = logger

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
            return self._full_sync(account=account, service=service)

        try:
            return self._partial_sync(
                account=account,
                service=service,
                start_history_id=state.last_history_id,
            )
        except HttpError as exc:
            if _http_status(exc) != 404:
                raise
            self._logger.warning(
                "History cursor for %s is stale at %s, falling back to full sync",
                account.email_address,
                state.last_history_id,
            )
            return self._full_sync(account=account, service=service)

    def _full_sync(self, *, account: GmailAccount, service) -> MailboxSyncSummary:
        sync_started_at = datetime.now(tz=UTC)
        next_history_id = current_history_id(service)
        messages_written = 0

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
            rows = [
                message_to_row(
                    account=account.email_address,
                    message=fetch_message(service, message_id),
                    synced_at=sync_started_at,
                )
                for message_id in new_message_ids
            ]
            self._warehouse.insert_messages(rows)
            messages_written += len(rows)
            self._logger.info(
                "Synced %s new Gmail messages for %s so far; skipped %s existing messages in latest page",
                messages_written,
                account.email_address,
                len(existing_message_ids),
            )

        return MailboxSyncSummary(
            account=account.email_address,
            sync_type="full",
            next_history_id=next_history_id,
            messages_written=messages_written,
            deleted_messages=0,
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

        self._logger.info(
            "Starting incremental Gmail sync for %s from history %s (%s messages changed)",
            account.email_address,
            start_history_id,
            len(changed_message_ids),
        )

        for message_ids in chunked(sorted(changed_message_ids), self._settings.gmail_page_size):
            rows = []
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
                    deleted_messages += 1
                else:
                    rows.append(
                        message_to_row(
                            account=account.email_address,
                            message=message,
                            synced_at=sync_started_at,
                        )
                    )
            self._warehouse.insert_messages(rows)
            messages_written += len(rows)
            self._logger.info(
                "Synced %s changed Gmail messages for %s so far",
                messages_written,
                account.email_address,
            )

        return MailboxSyncSummary(
            account=account.email_address,
            sync_type="partial",
            next_history_id=next_history_id,
            messages_written=messages_written,
            deleted_messages=deleted_messages,
            query=None,
        )


def build_gmail_service(*, account: GmailAccount, settings: Settings):
    credentials = load_credentials(account=account, settings=settings)
    return build("gmail", "v1", credentials=credentials, cache_discovery=False)


def load_credentials(*, account: GmailAccount, settings: Settings) -> Credentials:
    token_json = gmail_token_json_from_env(account.email_address)
    if not token_json:
        raise RuntimeError(
            f"No Gmail OAuth token for {account.email_address}. "
            f"Set GMAIL_{env_slug(account.email_address)}_TOKEN_JSON_B64."
        )

    credentials = Credentials.from_authorized_user_info(
        json.loads(token_json),
        settings.gmail_scopes,
    )
    if credentials.valid:
        return credentials

    if not credentials.expired or not credentials.refresh_token:
        raise RuntimeError(
            f"Stored OAuth token for {account.email_address} cannot be refreshed. "
            f"Update GMAIL_{env_slug(account.email_address)}_TOKEN_JSON_B64 and authorize again."
        )

    credentials.refresh(Request())
    return credentials


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
    return None


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
    if not value:
        return ""
    encoded = str(value)
    padding = "=" * (-len(encoded) % 4)
    decoded = base64.urlsafe_b64decode(encoded + padding)
    return decoded.decode("utf-8", errors="replace")


def chunked(values: Sequence[str], size: int) -> Iterator[list[str]]:
    for index in range(0, len(values), size):
        yield list(values[index : index + size])


def _http_status(exc: HttpError) -> int | None:
    return getattr(exc.resp, "status", None)


def execute_gmail_request(request_fn: Callable[[], T], *, max_attempts: int = 5) -> T:
    for attempt in range(1, max_attempts + 1):
        try:
            return request_fn()
        except HttpError as exc:
            status = _http_status(exc)
            if status not in {429, 500, 502, 503, 504} or attempt == max_attempts:
                raise
            time.sleep(min(2 ** (attempt - 1), 30))
    raise RuntimeError("unreachable Gmail retry state")
