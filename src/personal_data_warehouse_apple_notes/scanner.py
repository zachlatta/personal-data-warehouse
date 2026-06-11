from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from html import escape
from pathlib import Path
import gzip
import hashlib
import mimetypes
import re
import sqlite3
import zlib

from markdownify import markdownify as html_to_markdown

APPLE_EPOCH = datetime(2001, 1, 1, tzinfo=UTC)
UTI_CONTENT_TYPES = {
    "com.adobe.pdf": "application/pdf",
    "com.apple.quicktime-movie": "video/quicktime",
    "com.compuserve.gif": "image/gif",
    "public.heic": "image/heic",
    "public.html": "text/html",
    "public.jpeg": "image/jpeg",
    "public.mpeg-4": "video/mp4",
    "public.mpeg-4-audio": "audio/mp4",
    "public.mp3": "audio/mpeg",
    "public.png": "image/png",
    "public.plain-text": "text/plain",
    "public.tiff": "image/tiff",
}
# Attachment types that never have their own file on disk; their content lives
# in the note body, in other attachments, or in the attachment's raw metadata.
NO_FILE_ATTACHMENT_ERRORS = {
    "public.url": "link attachment has no file; the URL is in raw metadata (ZURLSTRING)",
    "com.apple.notes.table": "table attachment has no file; the table is embedded in the note body",
    "com.apple.notes.gallery": "gallery attachment has no file; its pages are separate attachments",
}
INLINE_ATTACHMENT_UTI_PREFIX = "com.apple.notes.inlinetextattachment"
INLINE_ATTACHMENT_ERROR = "inline text attachment has no file; its text is in raw metadata (ZALTTEXT)"


class AppleNotesSchemaError(RuntimeError):
    pass


@dataclass(frozen=True)
class AppleNoteAttachment:
    attachment_id: str
    note_id: str
    filename: str
    content_type: str
    path: Path | None
    size_bytes: int
    content_sha256: str
    is_missing: bool = False
    error: str = ""
    raw: dict[str, object] | None = None


@dataclass(frozen=True)
class AppleNote:
    note_id: str
    title: str
    folder_id: str
    folder_path: str
    apple_account_id: str
    apple_account_name: str
    created_at: datetime
    modified_at: datetime
    body_text: str
    body_html: str
    body_markdown: str
    attachments: tuple[AppleNoteAttachment, ...]
    is_deleted: bool = False
    raw: dict[str, object] | None = None


@dataclass(frozen=True)
class _CoreDataMediaFile:
    path: Path | None
    filename: str
    identifier: str


def snapshot_apple_notes_store(store_path: Path | str, destination_dir: Path | str) -> Path:
    source_path = Path(store_path).expanduser()
    destination = Path(destination_dir) / "NoteStore.sqlite"
    source_uri = source_path.resolve().as_uri() + "?mode=ro"
    try:
        source = sqlite3.connect(source_uri, uri=True)
    except sqlite3.Error as exc:
        raise PermissionError(
            f"Could not open Apple Notes store at {source_path}. "
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


def scan_apple_notes_store(store_path: Path | str, *, attachments_root: Path | str | None = None) -> list[AppleNote]:
    connection = sqlite3.connect(Path(store_path))
    connection.row_factory = sqlite3.Row
    try:
        tables = _table_names(connection)
        if "notes" in tables:
            return _scan_synthetic_store(connection, attachments_root=attachments_root)
        if "ZICCLOUDSYNCINGOBJECT" in tables and "Z_PRIMARYKEY" in tables:
            return _scan_core_data_store(connection, attachments_root=attachments_root)
        raise AppleNotesSchemaError(
            "Unsupported Apple Notes schema: expected a synthetic notes table or Apple's Core Data tables"
        )
    finally:
        connection.close()


def _scan_synthetic_store(
    connection: sqlite3.Connection,
    *,
    attachments_root: Path | str | None,
) -> list[AppleNote]:
    folder_rows = _optional_select_all(connection, "folders")
    account_rows = _optional_select_all(connection, "accounts")
    folders = {
        _string_value(row, ("folder_id", "id")): {
            "name": _string_value(row, ("name", "title")),
            "parent": _string_value(row, ("parent_folder_id", "parent_id")),
            "account": _string_value(row, ("account_id",)),
        }
        for row in folder_rows
    }
    accounts = {
        _string_value(row, ("account_id", "id")): _string_value(row, ("name", "account_name", "identifier"))
        for row in account_rows
    }
    attachments_by_note = _synthetic_attachments_by_note(connection, attachments_root=attachments_root)
    notes: list[AppleNote] = []
    for row in _select_all(connection, "notes"):
        note_id = _string_value(row, ("note_id", "id", "identifier"))
        if not note_id:
            continue
        folder_id = _string_value(row, ("folder_id",))
        apple_account_id = _string_value(row, ("account_id",)) or str(folders.get(folder_id, {}).get("account", ""))
        body_html = _string_value(row, ("body_html", "html"))
        body_text = _string_value(row, ("body_text", "text", "plaintext"))
        if not body_text and body_html:
            body_text = _plain_text_from_html(body_html)
        body_markdown = _string_value(row, ("body_markdown", "markdown")) or _markdown_from_body(
            body_html=body_html,
            body_text=body_text,
        )
        notes.append(
            AppleNote(
                note_id=note_id,
                title=_string_value(row, ("title", "name")),
                folder_id=folder_id,
                folder_path=_folder_path(folder_id, folders),
                apple_account_id=apple_account_id,
                apple_account_name=accounts.get(apple_account_id, apple_account_id),
                created_at=_datetime_value(row, ("created_at", "creation_date")),
                modified_at=_datetime_value(row, ("modified_at", "updated_at", "modification_date")),
                body_text=body_text,
                body_html=body_html or _html_from_text(body_text),
                body_markdown=body_markdown,
                attachments=tuple(attachments_by_note.get(note_id, [])),
                is_deleted=_bool_value(row, ("is_deleted", "deleted")),
                raw=_public_row(row),
            )
        )
    return sorted(notes, key=lambda note: (note.modified_at, note.note_id))


def _scan_core_data_store(
    connection: sqlite3.Connection,
    *,
    attachments_root: Path | str | None,
) -> list[AppleNote]:
    entity_names = {
        int(row["Z_ENT"]): str(row["Z_NAME"])
        for row in connection.execute("SELECT Z_ENT, Z_NAME FROM Z_PRIMARYKEY").fetchall()
        if row["Z_ENT"] is not None and row["Z_NAME"] is not None
    }
    object_rows = _select_all(connection, "ZICCLOUDSYNCINGOBJECT")
    note_entity_ids = {
        entity_id
        for entity_id, name in entity_names.items()
        if "note" in name.lower() and "data" not in name.lower() and "attachment" not in name.lower()
    }
    folder_entity_ids = {entity_id for entity_id, name in entity_names.items() if "folder" in name.lower()}
    account_entity_ids = {entity_id for entity_id, name in entity_names.items() if "account" in name.lower()}
    attachment_entity_ids = {entity_id for entity_id, name in entity_names.items() if "attachment" in name.lower()}
    media_entity_ids = {entity_id for entity_id, name in entity_names.items() if "media" in name.lower()}

    if not note_entity_ids:
        raise AppleNotesSchemaError("Unsupported Apple Notes schema: could not identify note entities")

    folders = _core_data_folders(object_rows, folder_entity_ids)
    accounts = _core_data_accounts(object_rows, account_entity_ids)
    note_data = _core_data_note_data(connection)
    attachments_by_note = _core_data_attachments_by_note(
        object_rows,
        attachment_entity_ids,
        media_entity_ids,
        attachments_root=attachments_root,
    )
    notes: list[AppleNote] = []
    for row in object_rows:
        if int(row["Z_ENT"] or 0) not in note_entity_ids:
            continue
        note_id = _string_value(row, ("ZIDENTIFIER", "ZUNIQUEIDENTIFIER", "ZUUID", "ZGCKEY"))
        if not note_id:
            note_id = str(row["Z_PK"])
        folder_id = _string_value(row, ("ZFOLDER", "ZFOLDER1", "ZPARENT", "ZPARENTFOLDER"))
        apple_account_id = _string_value(row, ("ZACCOUNT", "ZACCOUNT1", "ZACCOUNT2", "ZACCOUNT3", "ZACCOUNT4"))
        body_text, body_html = _core_data_body(row, note_data)
        title = _string_value(row, ("ZTITLE1", "ZTITLE", "ZNAME", "ZSNIPPET"))
        body_markdown = _markdown_from_body(body_html=body_html, body_text=body_text)
        attachments = tuple(
            replace(attachment, note_id=note_id)
            for attachment in attachments_by_note.get(str(row["Z_PK"]), [])
        )
        notes.append(
            AppleNote(
                note_id=note_id,
                title=title,
                folder_id=folder_id,
                folder_path=_folder_path(folder_id, folders),
                apple_account_id=apple_account_id,
                apple_account_name=accounts.get(apple_account_id, apple_account_id),
                created_at=_datetime_value(
                    row,
                    ("ZCREATIONDATE3", "ZCREATIONDATE1", "ZCREATIONDATE", "ZCREATEDDATE"),
                ),
                modified_at=_datetime_value(row, ("ZMODIFICATIONDATE1", "ZMODIFICATIONDATE", "ZUPDATEDDATE")),
                body_text=body_text,
                body_html=body_html or _html_from_text(body_text),
                body_markdown=body_markdown,
                attachments=attachments,
                is_deleted=_bool_value(row, ("ZMARKEDFORDELETION", "ZISDELETED", "ZDELETED")),
                raw=_public_row(row),
            )
        )
    return sorted(notes, key=lambda note: (note.modified_at, note.note_id))


def _synthetic_attachments_by_note(
    connection: sqlite3.Connection,
    *,
    attachments_root: Path | str | None,
) -> dict[str, list[AppleNoteAttachment]]:
    if "attachments" not in _table_names(connection):
        return {}
    result: dict[str, list[AppleNoteAttachment]] = {}
    root = Path(attachments_root).expanduser() if attachments_root else None
    for row in _select_all(connection, "attachments"):
        note_id = _string_value(row, ("note_id",))
        if not note_id:
            continue
        attachment = _attachment_from_row(
            row,
            note_id=note_id,
            root=root,
            id_columns=("attachment_id", "id", "identifier"),
            path_columns=("path", "file_path", "filename"),
            filename_columns=("filename", "name"),
            content_type_columns=("content_type", "mime_type", "uti"),
        )
        result.setdefault(note_id, []).append(attachment)
    return result


def _core_data_attachments_by_note(
    rows: list[sqlite3.Row],
    attachment_entity_ids: set[int],
    media_entity_ids: set[int],
    *,
    attachments_root: Path | str | None,
) -> dict[str, list[AppleNoteAttachment]]:
    if not attachment_entity_ids:
        return {}
    root = Path(attachments_root).expanduser() if attachments_root else None
    media_files = _core_data_media_files(rows, media_entity_ids, attachments_root=attachments_root)
    result: dict[str, list[AppleNoteAttachment]] = {}
    for row in rows:
        if int(row["Z_ENT"] or 0) not in attachment_entity_ids:
            continue
        note_pk = _string_value(row, ("ZNOTE", "ZNOTE1", "ZOWNER", "ZPARENT"))
        if not note_pk:
            continue
        media = media_files.get(_string_value(row, ("ZMEDIA",)))
        path_override = media.path if media else None
        filename_override = media.filename if media else ""
        if path_override is None:
            fallback_path = _resolve_core_data_fallback_path(row, root=root)
            if fallback_path is not None:
                path_override = fallback_path
                filename_override = _fallback_attachment_filename(row, fallback_path)
        attachment = _attachment_from_row(
            row,
            note_id=note_pk,
            root=root,
            id_columns=("ZIDENTIFIER", "ZUUID", "Z_PK"),
            path_columns=("ZFILEURL", "ZURL", "ZPATH", "ZFILENAME", "ZTITLE"),
            filename_columns=("ZFILENAME", "ZTITLE", "ZNAME"),
            content_type_columns=("ZMIMETYPE", "ZTYPEUTI", "ZUTI", "ZCONTENTTYPE"),
            path_override=path_override,
            filename_override=filename_override,
            missing_error=_no_file_attachment_error(row) if path_override is None else "",
            raw_extra={
                "ZMEDIA_IDENTIFIER": media.identifier,
                "ZMEDIA_FILENAME": media.filename,
            }
            if media
            else None,
        )
        result.setdefault(note_pk, []).append(attachment)
    return result


def _core_data_media_files(
    rows: list[sqlite3.Row],
    media_entity_ids: set[int],
    *,
    attachments_root: Path | str | None,
) -> dict[str, _CoreDataMediaFile]:
    if not media_entity_ids:
        return {}
    root = Path(attachments_root).expanduser() if attachments_root else None
    result: dict[str, _CoreDataMediaFile] = {}
    for row in rows:
        if int(row["Z_ENT"] or 0) not in media_entity_ids:
            continue
        media_pk = _string_value(row, ("Z_PK",))
        if not media_pk:
            continue
        identifier = _string_value(row, ("ZIDENTIFIER", "ZUUID"))
        filename = _string_value(row, ("ZFILENAME", "ZTITLE", "ZNAME"))
        path = _resolve_core_data_media_path(root=root, identifier=identifier, filename=filename)
        result[media_pk] = _CoreDataMediaFile(
            path=path,
            filename=filename or (path.name if path else identifier),
            identifier=identifier,
        )
    return result


def _attachment_from_row(
    row: sqlite3.Row,
    *,
    note_id: str,
    root: Path | None,
    id_columns: tuple[str, ...],
    path_columns: tuple[str, ...],
    filename_columns: tuple[str, ...],
    content_type_columns: tuple[str, ...],
    path_override: Path | None = None,
    filename_override: str = "",
    missing_error: str = "",
    raw_extra: Mapping[str, object] | None = None,
) -> AppleNoteAttachment:
    attachment_id = _string_value(row, id_columns) or _string_value(row, ("Z_PK",)) or "attachment"
    raw_path = _string_value(row, path_columns)
    path = path_override or _resolve_attachment_path(raw_path, root)
    filename = filename_override or _string_value(row, filename_columns) or (path.name if path else "") or Path(raw_path).name or attachment_id
    error = _string_value(row, ("error", "ZERROR"))
    content_sha256 = _string_value(row, ("content_sha256", "ZCONTENTSHA256"))
    size_bytes = _int_value(row, ("size_bytes", "size", "ZSIZE"))
    is_missing = _bool_value(row, ("is_missing", "missing"))
    if path is not None and path.exists() and path.is_file():
        size_bytes = path.stat().st_size
        content_sha256 = content_sha256 or file_sha256(path)
        is_missing = False
    elif not is_missing:
        is_missing = True
        error = error or missing_error or "attachment file is not locally available"
    return AppleNoteAttachment(
        attachment_id=attachment_id,
        note_id=note_id,
        filename=filename,
        content_type=normalized_content_type(_string_value(row, content_type_columns), filename=filename),
        path=path if path is not None and path.exists() else None,
        size_bytes=size_bytes,
        content_sha256=content_sha256,
        is_missing=is_missing,
        error=error,
        raw=_public_row(row) | dict(raw_extra or {}),
    )


def _core_data_folders(rows: list[sqlite3.Row], folder_entity_ids: set[int]) -> dict[str, dict[str, str]]:
    folders: dict[str, dict[str, str]] = {}
    for row in rows:
        if int(row["Z_ENT"] or 0) not in folder_entity_ids:
            continue
        folder_id = str(row["Z_PK"])
        folders[folder_id] = {
            "name": _string_value(row, ("ZTITLE2", "ZTITLE1", "ZTITLE", "ZNAME", "ZIDENTIFIER")),
            "parent": _string_value(row, ("ZPARENT", "ZPARENTFOLDER", "ZFOLDER")),
            "account": _string_value(row, ("ZACCOUNT", "ZACCOUNT1", "ZACCOUNT2", "ZACCOUNT3", "ZACCOUNT4")),
        }
    return folders


def _core_data_accounts(rows: list[sqlite3.Row], account_entity_ids: set[int]) -> dict[str, str]:
    accounts: dict[str, str] = {}
    for row in rows:
        if int(row["Z_ENT"] or 0) not in account_entity_ids:
            continue
        accounts[str(row["Z_PK"])] = _string_value(
            row,
            ("ZNAME", "ZACCOUNTNAME", "ZIDENTIFIER", "ZEMAILADDRESS"),
        )
    return accounts


def _core_data_note_data(connection: sqlite3.Connection) -> dict[str, tuple[str, str]]:
    tables = _table_names(connection)
    data_tables = [table for table in tables if table.lower() in {"zicnotedata", "notedata"}]
    if not data_tables:
        return {}
    data_by_pk: dict[str, tuple[str, str]] = {}
    for table in data_tables:
        for row in _select_all(connection, table):
            data_pk = _string_value(row, ("Z_PK", "id"))
            if not data_pk:
                continue
            decoded = ""
            for column in row.keys():
                value = row[column]
                if isinstance(value, bytes):
                    decoded = _decode_note_blob(value)
                    if decoded:
                        break
            body_html = decoded if "<" in decoded and ">" in decoded else ""
            body_text = _plain_text_from_html(body_html) if body_html else decoded
            data_by_pk[data_pk] = (body_text, body_html)
    return data_by_pk


def _core_data_body(row: sqlite3.Row, note_data: Mapping[str, tuple[str, str]]) -> tuple[str, str]:
    text = _string_value(
        row,
        ("ZBODY", "ZBODYTEXT", "ZPLAINTEXT", "ZSNIPPET", "ZSUMMARY", "ZTITLE1"),
    )
    html = _string_value(row, ("ZHTML", "ZBODYHTML", "ZHTMLSTRING"))
    note_data_pk = _string_value(row, ("ZNOTEDATA", "ZNOTEDATA1", "ZDATA"))
    if note_data_pk in note_data:
        data_text, data_html = note_data[note_data_pk]
        text = data_text or text
        html = data_html or html
    return text, html


def _table_names(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]) for row in rows}


def _select_all(connection: sqlite3.Connection, table: str) -> list[sqlite3.Row]:
    return connection.execute(f"SELECT * FROM {_quote_identifier(table)}").fetchall()


def _optional_select_all(connection: sqlite3.Connection, table: str) -> list[sqlite3.Row]:
    return _select_all(connection, table) if table in _table_names(connection) else []


def _quote_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise AppleNotesSchemaError(f"invalid SQLite identifier: {value!r}")
    return f'"{value}"'


def _string_value(row: sqlite3.Row, columns: tuple[str, ...]) -> str:
    keys = set(row.keys())
    for column in columns:
        if column in keys and row[column] is not None:
            return str(row[column])
    return ""


def _int_value(row: sqlite3.Row, columns: tuple[str, ...]) -> int:
    value = _string_value(row, columns)
    if not value:
        return 0
    try:
        return int(float(value))
    except ValueError:
        return 0


def _bool_value(row: sqlite3.Row, columns: tuple[str, ...]) -> bool:
    value = _string_value(row, columns).strip().lower()
    return value in {"1", "true", "yes", "y"}


def _datetime_value(row: sqlite3.Row, columns: tuple[str, ...]) -> datetime:
    keys = set(row.keys())
    for column in columns:
        if column in keys and row[column] is not None:
            return parse_datetime(row[column])
    return datetime.fromtimestamp(0, tz=UTC)


def parse_datetime(value) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, int | float):
        number = float(value)
        if number <= 0:
            parsed = datetime.fromtimestamp(0, tz=UTC)
        elif number < 1_000_000_000:
            parsed = APPLE_EPOCH + timedelta(seconds=number)
        else:
            parsed = datetime.fromtimestamp(number, tz=UTC)
    else:
        text = str(value or "").strip()
        if not text:
            parsed = datetime.fromtimestamp(0, tz=UTC)
        else:
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                try:
                    parsed = parse_datetime(float(text))
                except ValueError:
                    parsed = datetime.fromtimestamp(0, tz=UTC)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _folder_path(folder_id: str, folders: Mapping[str, Mapping[str, str]]) -> str:
    if not folder_id:
        return ""
    parts: list[str] = []
    seen: set[str] = set()
    current = folder_id
    while current and current not in seen:
        seen.add(current)
        folder = folders.get(current)
        if not folder:
            break
        name = str(folder.get("name", ""))
        if name:
            parts.append(name)
        current = str(folder.get("parent", ""))
    return "/".join(reversed(parts))


def _resolve_attachment_path(value: str, root: Path | None) -> Path | None:
    if not value:
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    return root / path if root else path


def _resolve_core_data_media_path(*, root: Path | None, identifier: str, filename: str) -> Path | None:
    if not identifier:
        return None
    for media_root in _account_asset_dirs(root, "Media"):
        media_dir = media_root / identifier
        if not media_dir.is_dir():
            continue
        if filename:
            for candidate in media_dir.rglob("*"):
                if candidate.is_file() and candidate.name == filename:
                    return candidate
        files = [candidate for candidate in media_dir.rglob("*") if candidate.is_file()]
        if len(files) == 1:
            return files[0]
    return None


def _resolve_core_data_fallback_path(row: sqlite3.Row, *, root: Path | None) -> Path | None:
    # Drawings and scanned documents have no ICMedia row; Notes renders them to
    # FallbackImages/<identifier>/<generation>/ and FallbackPDFs/<identifier>/<generation>/.
    identifier = _string_value(row, ("ZIDENTIFIER", "ZUUID"))
    if not identifier:
        return None
    for dir_name, generation_column in (
        ("FallbackImages", "ZFALLBACKIMAGEGENERATION"),
        ("FallbackPDFs", "ZFALLBACKPDFGENERATION"),
    ):
        generation = _string_value(row, (generation_column,))
        if not generation:
            continue
        for fallback_root in _account_asset_dirs(root, dir_name):
            generation_dir = fallback_root / identifier / generation
            if not generation_dir.is_dir():
                continue
            files = [candidate for candidate in generation_dir.rglob("*") if candidate.is_file()]
            if len(files) == 1:
                return files[0]
    return None


def _account_asset_dirs(root: Path | None, name: str) -> list[Path]:
    if root is None:
        return []
    dirs = [path for path in (root / "Accounts").glob(f"*/{name}") if path.is_dir()]
    direct = root / name
    if direct.is_dir():
        dirs.append(direct)
    return dirs


def _fallback_attachment_filename(row: sqlite3.Row, path: Path) -> str:
    base = _string_value(row, ("ZFILENAME", "ZTITLE", "ZNAME")).strip()
    if not base.strip("."):
        base = _string_value(row, ("ZIDENTIFIER", "ZUUID")) or path.stem
    if path.suffix and not base.lower().endswith(path.suffix.lower()):
        base += path.suffix
    return base


def _no_file_attachment_error(row: sqlite3.Row) -> str:
    uti = _string_value(row, ("ZTYPEUTI", "ZTYPEUTI1"))
    if uti in NO_FILE_ATTACHMENT_ERRORS:
        return NO_FILE_ATTACHMENT_ERRORS[uti]
    if uti.startswith(INLINE_ATTACHMENT_UTI_PREFIX):
        return INLINE_ATTACHMENT_ERROR
    return ""


def _decode_note_blob(value: bytes) -> str:
    protobuf_text = _decode_note_protobuf_text(value)
    if protobuf_text:
        return protobuf_text

    text_candidates: list[str] = []
    for candidate in _note_blob_payload_candidates(value):
        for encoding in ("utf-8", "utf-16", "latin-1"):
            try:
                decoded = candidate.decode(encoding)
            except UnicodeDecodeError:
                continue
            cleaned = decoded.replace("\x00", "")
            if cleaned.strip():
                text_candidates.append(cleaned)
                break
    if text_candidates:
        best_candidate = max(text_candidates, key=_decoded_text_quality_score)
        if _decoded_text_is_readable(best_candidate):
            return best_candidate
        return ""
    printable = bytes(byte for byte in value if 32 <= byte <= 126 or byte in {9, 10, 13})
    if len(printable) / max(1, len(value)) < 0.8:
        return ""
    return printable.decode("utf-8", errors="ignore")


def _note_blob_payload_candidates(value: bytes) -> list[bytes]:
    candidates: list[bytes] = []
    for decoder in (gzip.decompress, zlib.decompress):
        try:
            candidates.append(decoder(value))
        except (OSError, zlib.error):
            pass
    candidates.append(value)
    deduped: list[bytes] = []
    seen: set[bytes] = set()
    for candidate in candidates:
        if candidate not in seen:
            deduped.append(candidate)
            seen.add(candidate)
    return deduped


def _decode_note_protobuf_text(value: bytes) -> str:
    for candidate in _note_blob_payload_candidates(value):
        text = _extract_notestore_protobuf_text(candidate)
        if text:
            return text
    return ""


def _extract_notestore_protobuf_text(payload: bytes) -> str:
    # Apple stores modern note bodies as gzip-compressed protobuf:
    # NoteStoreProto.document -> Document.note -> Note.note_text.
    for document in _protobuf_length_delimited_fields(payload, 2):
        for note in _protobuf_length_delimited_fields(document, 3):
            strings = [
                field.decode("utf-8", errors="replace")
                for field in _protobuf_length_delimited_fields(note, 2)
                if field
            ]
            strings = [string for string in strings if string.strip()]
            if strings:
                return max(strings, key=len)
    return ""


def _protobuf_length_delimited_fields(payload: bytes, field_number: int) -> list[bytes]:
    values: list[bytes] = []
    for number, wire_type, value in _iter_protobuf_fields(payload):
        if number == field_number and wire_type == 2 and isinstance(value, bytes):
            values.append(value)
    return values


def _iter_protobuf_fields(payload: bytes):
    offset = 0
    length = len(payload)
    while offset < length:
        try:
            key, offset = _read_protobuf_varint(payload, offset)
        except ValueError:
            return
        field_number = key >> 3
        wire_type = key & 0x07
        try:
            if wire_type == 0:
                value, offset = _read_protobuf_varint(payload, offset)
            elif wire_type == 1:
                value = payload[offset : offset + 8]
                offset += 8
            elif wire_type == 2:
                size, offset = _read_protobuf_varint(payload, offset)
                value = payload[offset : offset + size]
                offset += size
            elif wire_type == 5:
                value = payload[offset : offset + 4]
                offset += 4
            else:
                return
        except ValueError:
            return
        if offset > length:
            return
        yield field_number, wire_type, value


def _read_protobuf_varint(payload: bytes, offset: int) -> tuple[int, int]:
    shift = 0
    result = 0
    while offset < len(payload):
        byte = payload[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, offset
        shift += 7
        if shift > 70:
            raise ValueError("protobuf varint is too long")
    raise ValueError("unexpected end of protobuf varint")


def _decoded_text_quality_score(value: str) -> tuple[float, int, int]:
    if not value:
        return (0, 0, 0)
    printable = sum(1 for character in value if character.isprintable() or character in "\n\r\t")
    weird_controls = sum(
        1
        for character in value
        if ord(character) < 32 and character not in "\n\r\t"
    )
    return (printable / len(value), -weird_controls, len(value))


def _decoded_text_is_readable(value: str) -> bool:
    if not value.strip():
        return False
    printable_ratio, negative_weird_controls, _ = _decoded_text_quality_score(value)
    weird_controls = -negative_weird_controls
    return printable_ratio >= 0.95 and weird_controls == 0


def _plain_text_from_html(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", value)).strip()


def _markdown_from_body(*, body_html: str, body_text: str) -> str:
    if body_html:
        return html_to_markdown(body_html).strip()
    return body_text.strip()


def _html_from_text(value: str) -> str:
    if not value:
        return ""
    return "<html><body><pre>" + escape(value) + "</pre></body></html>"


def _public_row(row: sqlite3.Row) -> dict[str, object]:
    public: dict[str, object] = {}
    for key in row.keys():
        value = row[key]
        if isinstance(value, bytes):
            public[key] = f"<{len(value)} bytes>"
        elif value is None or isinstance(value, str | int | float):
            public[key] = value
        else:
            public[key] = str(value)
    return public


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def content_type_for_path(path: str) -> str:
    return mimetypes.guess_type(path)[0] or "application/octet-stream"


def normalized_content_type(value: str, *, filename: str) -> str:
    content_type = value.strip()
    if "/" in content_type:
        return content_type
    guessed = content_type_for_path(filename) if filename else "application/octet-stream"
    if guessed != "application/octet-stream":
        return guessed
    if content_type in UTI_CONTENT_TYPES:
        return UTI_CONTENT_TYPES[content_type]
    if content_type.startswith("public."):
        extension = content_type.removeprefix("public.").replace("-", "")
        guessed_from_uti = mimetypes.guess_type(f"file.{extension}")[0]
        if guessed_from_uti:
            return guessed_from_uti
    return "application/octet-stream"
