from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
import hashlib
import mimetypes
import sqlite3
from typing import Any

APPLE_EPOCH = datetime(2001, 1, 1, tzinfo=UTC)
DEFAULT_MESSAGES_STORE_PATH = "~/Library/Messages/chat.db"


class AppleMessagesSchemaError(RuntimeError):
    pass


@dataclass(frozen=True)
class AppleMessageHandle:
    handle_id: str
    handle_rowid: int
    address: str
    country: str
    service: str
    uncanonicalized_id: str
    person_centric_id: str
    raw: dict[str, object]


@dataclass(frozen=True)
class AppleMessageChat:
    chat_id: str
    chat_rowid: int
    guid: str
    chat_identifier: str
    service_name: str
    display_name: str
    room_name: str
    account_login: str
    style: int
    state: int
    is_archived: bool
    is_filtered: bool
    is_recovered: bool
    is_pending_review: bool
    last_read_message_at: datetime
    raw: dict[str, object]


@dataclass(frozen=True)
class AppleMessageChatHandle:
    chat_id: str
    handle_id: str
    raw: dict[str, object]


@dataclass(frozen=True)
class AppleMessageChatMessage:
    chat_id: str
    message_id: str
    message_date: datetime
    message_date_ns: int
    raw: dict[str, object]


@dataclass(frozen=True)
class AppleMessage:
    message_id: str
    message_rowid: int
    handle_id: str
    service: str
    message_account: str
    text: str
    attributed_body: bytes | None
    attributed_body_sha256: str
    subject: str
    country: str
    message_type: int
    item_type: int
    is_from_me: bool
    is_read: bool
    is_sent: bool
    is_delivered: bool
    is_finished: bool
    is_system_message: bool
    is_service_message: bool
    is_forward: bool
    is_empty: bool
    is_audio_message: bool
    is_played: bool
    cache_has_attachments: bool
    has_unseen_mention: bool
    is_spam: bool
    reply_to_guid: str
    associated_message_guid: str
    associated_message_type: int
    associated_message_emoji: str
    balloon_bundle_id: str
    group_title: str
    group_action_type: int
    message_action_type: int
    message_source: int
    expressive_send_style_id: str
    message_at: datetime
    date_ns: int
    date_read: datetime
    date_delivered: datetime
    date_played: datetime
    date_edited: datetime
    date_retracted: datetime
    date_recovered: datetime
    raw: dict[str, object]


@dataclass(frozen=True)
class AppleMessageAttachment:
    attachment_id: str
    attachment_rowid: int
    message_id: str
    guid: str
    original_guid: str
    filename: str
    resolved_path: Path | None
    transfer_name: str
    content_type: str
    uti: str
    mime_type: str
    total_bytes: int
    size_bytes: int
    is_missing: bool
    error: str
    is_outgoing: bool
    is_sticker: bool
    hide_attachment: bool
    transfer_state: int
    created_at: datetime
    start_at: datetime
    raw: dict[str, object]


@dataclass(frozen=True)
class AppleMessageDeletedMessage:
    message_id: str
    deleted_at: datetime
    raw: dict[str, object]


@dataclass(frozen=True)
class AppleMessagesSnapshot:
    handles: tuple[AppleMessageHandle, ...]
    chats: tuple[AppleMessageChat, ...]
    chat_handles: tuple[AppleMessageChatHandle, ...]
    messages: tuple[AppleMessage, ...]
    chat_messages: tuple[AppleMessageChatMessage, ...]
    attachments: tuple[AppleMessageAttachment, ...]
    deleted_messages: tuple[AppleMessageDeletedMessage, ...]


def snapshot_apple_messages_store(store_path: Path | str, destination_dir: Path | str) -> Path:
    source_path = Path(store_path).expanduser()
    destination = Path(destination_dir) / "chat.db"
    source_uri = source_path.resolve().as_uri() + "?mode=ro"
    try:
        source = sqlite3.connect(source_uri, uri=True)
    except sqlite3.Error as exc:
        raise PermissionError(
            f"Could not open Apple Messages store at {source_path}. "
            "Grant Full Disk Access to the launching executable chain and retry."
        ) from exc
    try:
        target = sqlite3.connect(destination)
        try:
            source.backup(target)
        finally:
            target.close()
    finally:
        source.close()
    return destination


def scan_apple_messages_store(
    store_path: Path | str,
    *,
    messages_root: Path | str | None = None,
) -> AppleMessagesSnapshot:
    root = Path(messages_root).expanduser() if messages_root else Path(store_path).expanduser().parent
    connection = sqlite3.connect(Path(store_path))
    connection.row_factory = sqlite3.Row
    try:
        tables = table_names(connection)
        required = {"message", "handle", "chat", "chat_message_join", "chat_handle_join", "attachment"}
        missing = required - tables
        if missing:
            raise AppleMessagesSchemaError(f"Unsupported Apple Messages schema: missing {', '.join(sorted(missing))}")
        handles = tuple(scan_handles(connection))
        handle_ids = {handle.handle_rowid: handle.handle_id for handle in handles}
        chats = tuple(scan_chats(connection))
        chat_ids = {chat.chat_rowid: chat.chat_id for chat in chats}
        messages = tuple(scan_messages(connection, handle_ids=handle_ids))
        message_ids = {message.message_rowid: message.message_id for message in messages}
        return AppleMessagesSnapshot(
            handles=handles,
            chats=chats,
            chat_handles=tuple(scan_chat_handles(connection, chat_ids=chat_ids, handle_ids=handle_ids)),
            messages=messages,
            chat_messages=tuple(scan_chat_messages(connection, chat_ids=chat_ids, message_ids=message_ids)),
            attachments=tuple(scan_attachments(connection, root=root, message_ids=message_ids)),
            deleted_messages=tuple(scan_deleted_messages(connection)),
        )
    finally:
        connection.close()


def scan_handles(connection: sqlite3.Connection) -> list[AppleMessageHandle]:
    rows = select_all(connection, "handle")
    handles: list[AppleMessageHandle] = []
    for row in rows:
        rowid = int_value(row, "ROWID")
        handles.append(
            AppleMessageHandle(
                handle_id=str(rowid),
                handle_rowid=rowid,
                address=string_value(row, "id"),
                country=string_value(row, "country"),
                service=string_value(row, "service"),
                uncanonicalized_id=string_value(row, "uncanonicalized_id"),
                person_centric_id=string_value(row, "person_centric_id"),
                raw=public_row(row),
            )
        )
    return handles


def scan_chats(connection: sqlite3.Connection) -> list[AppleMessageChat]:
    chats: list[AppleMessageChat] = []
    for row in select_all(connection, "chat"):
        rowid = int_value(row, "ROWID")
        guid = string_value(row, "guid") or str(rowid)
        chats.append(
            AppleMessageChat(
                chat_id=guid,
                chat_rowid=rowid,
                guid=guid,
                chat_identifier=string_value(row, "chat_identifier"),
                service_name=string_value(row, "service_name"),
                display_name=string_value(row, "display_name"),
                room_name=string_value(row, "room_name"),
                account_login=string_value(row, "account_login"),
                style=int_value(row, "style"),
                state=int_value(row, "state"),
                is_archived=bool_value(row, "is_archived"),
                is_filtered=bool_value(row, "is_filtered"),
                is_recovered=bool_value(row, "is_recovered"),
                is_pending_review=bool_value(row, "is_pending_review"),
                last_read_message_at=parse_apple_timestamp(int_value(row, "last_read_message_timestamp")),
                raw=public_row(row),
            )
        )
    return chats


def scan_chat_handles(
    connection: sqlite3.Connection,
    *,
    chat_ids: dict[int, str],
    handle_ids: dict[int, str],
) -> list[AppleMessageChatHandle]:
    result: list[AppleMessageChatHandle] = []
    for row in select_all(connection, "chat_handle_join"):
        chat_id = chat_ids.get(int_value(row, "chat_id"), "")
        handle_id = handle_ids.get(int_value(row, "handle_id"), "")
        if chat_id and handle_id:
            result.append(AppleMessageChatHandle(chat_id=chat_id, handle_id=handle_id, raw=public_row(row)))
    return result


def scan_messages(connection: sqlite3.Connection, *, handle_ids: dict[int, str]) -> list[AppleMessage]:
    messages: list[AppleMessage] = []
    for row in select_all(connection, "message"):
        rowid = int_value(row, "ROWID")
        guid = string_value(row, "guid") or str(rowid)
        attributed_body = bytes_value(row, "attributedBody")
        date_ns = int_value(row, "date")
        messages.append(
            AppleMessage(
                message_id=guid,
                message_rowid=rowid,
                handle_id=handle_ids.get(int_value(row, "handle_id"), ""),
                service=string_value(row, "service"),
                message_account=string_value(row, "account"),
                text=string_value(row, "text"),
                attributed_body=attributed_body,
                attributed_body_sha256=file_bytes_sha256(attributed_body) if attributed_body else "",
                subject=string_value(row, "subject"),
                country=string_value(row, "country"),
                message_type=int_value(row, "type"),
                item_type=int_value(row, "item_type"),
                is_from_me=bool_value(row, "is_from_me"),
                is_read=bool_value(row, "is_read"),
                is_sent=bool_value(row, "is_sent"),
                is_delivered=bool_value(row, "is_delivered"),
                is_finished=bool_value(row, "is_finished"),
                is_system_message=bool_value(row, "is_system_message"),
                is_service_message=bool_value(row, "is_service_message"),
                is_forward=bool_value(row, "is_forward"),
                is_empty=bool_value(row, "is_empty"),
                is_audio_message=bool_value(row, "is_audio_message"),
                is_played=bool_value(row, "is_played"),
                cache_has_attachments=bool_value(row, "cache_has_attachments"),
                has_unseen_mention=bool_value(row, "has_unseen_mention"),
                is_spam=bool_value(row, "is_spam"),
                reply_to_guid=string_value(row, "reply_to_guid"),
                associated_message_guid=string_value(row, "associated_message_guid"),
                associated_message_type=int_value(row, "associated_message_type"),
                associated_message_emoji=string_value(row, "associated_message_emoji"),
                balloon_bundle_id=string_value(row, "balloon_bundle_id"),
                group_title=string_value(row, "group_title"),
                group_action_type=int_value(row, "group_action_type"),
                message_action_type=int_value(row, "message_action_type"),
                message_source=int_value(row, "message_source"),
                expressive_send_style_id=string_value(row, "expressive_send_style_id"),
                message_at=parse_apple_timestamp(date_ns),
                date_ns=date_ns,
                date_read=parse_apple_timestamp(int_value(row, "date_read")),
                date_delivered=parse_apple_timestamp(int_value(row, "date_delivered")),
                date_played=parse_apple_timestamp(int_value(row, "date_played")),
                date_edited=parse_apple_timestamp(int_value(row, "date_edited")),
                date_retracted=parse_apple_timestamp(int_value(row, "date_retracted")),
                date_recovered=parse_apple_timestamp(int_value(row, "date_recovered")),
                raw=public_row_without_blobs(row),
            )
        )
    return messages


def scan_chat_messages(
    connection: sqlite3.Connection,
    *,
    chat_ids: dict[int, str],
    message_ids: dict[int, str],
) -> list[AppleMessageChatMessage]:
    result: list[AppleMessageChatMessage] = []
    for row in select_all(connection, "chat_message_join"):
        chat_id = chat_ids.get(int_value(row, "chat_id"), "")
        message_id = message_ids.get(int_value(row, "message_id"), "")
        date_ns = int_value(row, "message_date")
        if chat_id and message_id:
            result.append(
                AppleMessageChatMessage(
                    chat_id=chat_id,
                    message_id=message_id,
                    message_date=parse_apple_timestamp(date_ns),
                    message_date_ns=date_ns,
                    raw=public_row(row),
                )
            )
    return result


def scan_attachments(
    connection: sqlite3.Connection,
    *,
    root: Path,
    message_ids: dict[int, str],
) -> list[AppleMessageAttachment]:
    rows = connection.execute(
        """
        SELECT attachment.ROWID AS ROWID, attachment.*, message_attachment_join.message_id AS pdw_message_rowid
        FROM attachment
        LEFT JOIN message_attachment_join ON message_attachment_join.attachment_id = attachment.ROWID
        """
    ).fetchall()
    result: list[AppleMessageAttachment] = []
    for row in rows:
        rowid = int_value(row, "ROWID")
        guid = string_value(row, "guid") or str(rowid)
        filename = string_value(row, "filename")
        resolved = resolve_attachment_path(filename, root=root)
        exists = bool(resolved and resolved.exists() and resolved.is_file())
        size_bytes = resolved.stat().st_size if exists and resolved is not None else 0
        error = "" if exists else ("attachment file is not locally available" if filename else "attachment filename is empty")
        message_id = message_ids.get(int_value(row, "pdw_message_rowid"), "")
        result.append(
            AppleMessageAttachment(
                attachment_id=guid,
                attachment_rowid=rowid,
                message_id=message_id,
                guid=guid,
                original_guid=string_value(row, "original_guid"),
                filename=filename,
                resolved_path=resolved if exists else None,
                transfer_name=string_value(row, "transfer_name"),
                content_type=normalized_content_type(
                    string_value(row, "mime_type"),
                    uti=string_value(row, "uti"),
                    filename=filename,
                ),
                uti=string_value(row, "uti"),
                mime_type=string_value(row, "mime_type"),
                total_bytes=int_value(row, "total_bytes"),
                size_bytes=size_bytes,
                is_missing=not exists,
                error=error,
                is_outgoing=bool_value(row, "is_outgoing"),
                is_sticker=bool_value(row, "is_sticker"),
                hide_attachment=bool_value(row, "hide_attachment"),
                transfer_state=int_value(row, "transfer_state"),
                created_at=parse_apple_timestamp(int_value(row, "created_date")),
                start_at=parse_apple_timestamp(int_value(row, "start_date")),
                raw=public_row_without_blobs(row),
            )
        )
    return result


def scan_deleted_messages(connection: sqlite3.Connection) -> list[AppleMessageDeletedMessage]:
    deleted: dict[str, AppleMessageDeletedMessage] = {}
    for table in ("deleted_messages", "sync_deleted_messages"):
        if table not in table_names(connection):
            continue
        for row in select_all(connection, table):
            guid = string_value(row, "guid")
            if not guid:
                continue
            deleted[guid] = AppleMessageDeletedMessage(
                message_id=guid,
                deleted_at=datetime.fromtimestamp(0, tz=UTC),
                raw={"source_table": table, **public_row(row)},
            )
    return list(deleted.values())


def table_names(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]) for row in rows}


def select_all(connection: sqlite3.Connection, table: str) -> list[sqlite3.Row]:
    quoted = quote_identifier(table)
    return connection.execute(f"SELECT ROWID AS ROWID, * FROM {quoted}").fetchall()


def quote_identifier(value: str) -> str:
    if not value.replace("_", "").isalnum():
        raise AppleMessagesSchemaError(f"invalid SQLite identifier: {value!r}")
    return f'"{value}"'


def parse_apple_timestamp(value: int | float | str | None) -> datetime:
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        number = 0
    if number <= 0:
        return datetime.fromtimestamp(0, tz=UTC)
    seconds = number / 1_000_000_000 if number > 10_000_000_000 else number
    return APPLE_EPOCH + timedelta(seconds=seconds)


def string_value(row: sqlite3.Row, column: str) -> str:
    if column not in row.keys() or row[column] is None:
        return ""
    value = row[column]
    if isinstance(value, bytes):
        return f"<{len(value)} bytes>"
    return str(value)


def int_value(row: sqlite3.Row, column: str) -> int:
    if column not in row.keys() or row[column] is None:
        return 0
    try:
        return int(row[column])
    except (TypeError, ValueError):
        try:
            return int(float(row[column]))
        except (TypeError, ValueError):
            return 0


def bool_value(row: sqlite3.Row, column: str) -> bool:
    return bool(int_value(row, column))


def bytes_value(row: sqlite3.Row, column: str) -> bytes | None:
    if column not in row.keys():
        return None
    value = row[column]
    return value if isinstance(value, bytes) else None


def public_row(row: sqlite3.Row) -> dict[str, object]:
    return {key: public_value(row[key]) for key in row.keys()}


def public_row_without_blobs(row: sqlite3.Row) -> dict[str, object]:
    return {
        key: public_value(row[key])
        for key in row.keys()
        if not isinstance(row[key], bytes)
    }


def public_value(value) -> object:
    if isinstance(value, bytes):
        return f"<{len(value)} bytes>"
    if value is None or isinstance(value, str | int | float):
        return value
    return str(value)


UTI_CONTENT_TYPES = {
    "com.adobe.pdf": "application/pdf",
    "com.apple.m4a-audio": "audio/x-m4a",
    "com.apple.quicktime-movie": "video/quicktime",
    "com.compuserve.gif": "image/gif",
    "org.openxmlformats.wordprocessingml.document": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "public.heic": "image/heic",
    "public.html": "text/html",
    "public.jpeg": "image/jpeg",
    "public.mpeg-4": "video/mp4",
    "public.mpeg-4-audio": "audio/mp4",
    "public.mp3": "audio/mpeg",
    "public.plain-text": "text/plain",
    "public.png": "image/png",
    "public.svg-image": "image/svg+xml",
    "public.tiff": "image/tiff",
    "public.vcard": "text/vcard",
    "public.zip-archive": "application/zip",
}


def normalized_content_type(value: str, *, uti: str, filename: str) -> str:
    if "/" in value:
        return value
    guessed = mimetypes.guess_type(filename)[0] if filename else None
    return guessed or UTI_CONTENT_TYPES.get(uti, "application/octet-stream")


def resolve_attachment_path(filename: str, *, root: Path) -> Path | None:
    if not filename:
        return None
    if filename.startswith("~/"):
        return Path(filename).expanduser()
    path = Path(filename)
    if path.is_absolute():
        return path
    return root / path


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_bytes_sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()
