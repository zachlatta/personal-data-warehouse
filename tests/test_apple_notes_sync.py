from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import gzip
import sqlite3

from personal_data_warehouse.config import GOOGLE_DRIVE_SCOPE, load_settings
from personal_data_warehouse_apple_notes.scanner import _decode_note_blob, scan_apple_notes_store
from personal_data_warehouse_apple_notes.state import AppleNotesUploadState
from personal_data_warehouse_apple_notes.sync import AppleNotesUploadRunner, revision_from_note


class FakeLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, *args, **kwargs) -> None:
        self.messages.append(args[0] % args[1:] if len(args) > 1 else str(args[0]))

    def warning(self, *args, **kwargs) -> None:
        self.messages.append(args[0] % args[1:] if len(args) > 1 else str(args[0]))


class FakeObjectStore:
    backend = "fake"

    def __init__(self) -> None:
        self.file_uploads: list[dict[str, object]] = []
        self.json_uploads: list[dict[str, object]] = []
        self.objects: set[tuple[str, str, str]] = set()

    def has_blob(self, *, content_sha256: str) -> bool:
        return ("apple_note_body_html", "content_sha256", content_sha256) in self.objects

    def has_metadata(self, *, content_sha256: str) -> bool:
        return ("apple_note_revision_metadata", "content_sha256", content_sha256) in self.objects

    def has_object(self, *, kind: str, key: str, value: str) -> bool:
        return (kind, key, value) in self.objects

    def presence(self, *, content_sha256: str):
        raise NotImplementedError

    def put_file(
        self,
        *,
        path: Path,
        object_key: str,
        content_sha256: str,
        content_type: str,
        skip_existing_check: bool = False,
        app_properties: dict[str, str] | None = None,
        kind: str | None = None,
    ):
        object_kind = kind or "blob"
        if not skip_existing_check and self.has_object(kind=object_kind, key="content_sha256", value=content_sha256):
            return {
                "storage_backend": self.backend,
                "storage_key": object_key,
                "storage_file_id": f"existing-{content_sha256[:8]}",
                "storage_url": f"https://example.test/{object_key}",
            }
        self.file_uploads.append(
            {
                "path": path,
                "object_key": object_key,
                "content_sha256": content_sha256,
                "content_type": content_type,
                "kind": object_kind,
                "app_properties": app_properties or {},
            }
        )
        self.objects.add((object_kind, "content_sha256", content_sha256))
        for key, value in (app_properties or {}).items():
            self.objects.add((object_kind, key, value))
        return {
            "storage_backend": self.backend,
            "storage_key": object_key,
            "storage_file_id": f"file-{content_sha256[:8]}",
            "storage_url": f"https://example.test/{object_key}",
        }

    def put_json(
        self,
        *,
        object_key: str,
        payload: dict[str, object],
        content_sha256: str,
        source_content_sha256: str | None = None,
        skip_existing_check: bool = False,
        app_properties: dict[str, str] | None = None,
        kind: str | None = None,
    ):
        object_kind = kind or "metadata"
        self.json_uploads.append(
            {
                "object_key": object_key,
                "payload": payload,
                "content_sha256": content_sha256,
                "kind": object_kind,
                "app_properties": app_properties or {},
            }
        )
        self.objects.add((object_kind, "content_sha256", content_sha256))
        for key, value in (app_properties or {}).items():
            self.objects.add((object_kind, key, value))
        return {
            "storage_backend": self.backend,
            "storage_key": object_key,
            "storage_file_id": f"metadata-{content_sha256[:8]}",
            "storage_url": f"https://example.test/{object_key}",
        }


def test_load_settings_adds_drive_scope_when_apple_notes_uses_google_drive(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    monkeypatch.setenv("APPLE_NOTES_ACCOUNT", "zach@example.com")
    monkeypatch.setenv("APPLE_NOTES_GOOGLE_DRIVE_FOLDER_ID", "folder-id")

    settings = load_settings(require_postgres=False, require_gmail=False, require_apple_notes=True)

    assert settings.apple_notes is not None
    assert settings.apple_notes.account == "zach@example.com"
    assert settings.apple_notes.google_drive_folder_id == "folder-id"
    assert GOOGLE_DRIVE_SCOPE in settings.google_scopes


def test_scan_apple_notes_synthetic_store_extracts_notes_and_attachments(tmp_path) -> None:
    store = create_notes_store(tmp_path, body_html="<p>Hello <strong>Notes</strong></p>")

    [note] = scan_apple_notes_store(store, attachments_root=tmp_path)

    assert note.note_id == "note-1"
    assert note.folder_path == "Work/Projects"
    assert note.apple_account_name == "iCloud"
    assert note.body_markdown == "Hello **Notes**"
    assert len(note.attachments) == 1
    assert note.attachments[0].content_sha256
    assert note.attachments[0].is_missing is False


def test_scan_apple_notes_core_data_store_resolves_media_attachment_files(tmp_path) -> None:
    store = create_core_data_notes_store(tmp_path)

    [note] = scan_apple_notes_store(store, attachments_root=tmp_path)

    assert note.note_id == "NOTE-1"
    assert note.created_at == datetime(2026, 5, 19, 12, tzinfo=UTC)
    assert note.body_text == "Clean protobuf body"
    assert note.body_html == "<html><body><pre>Clean protobuf body</pre></body></html>"
    assert note.body_markdown == "Clean protobuf body"
    assert len(note.attachments) == 1
    attachment = note.attachments[0]
    assert attachment.note_id == "NOTE-1"
    assert attachment.filename == "archive.jpg"
    assert attachment.content_type == "image/jpeg"
    assert attachment.path is not None
    assert attachment.path.exists()
    assert attachment.is_missing is False
    assert attachment.content_sha256


def test_decode_note_blob_prefers_gzipped_protobuf_note_text() -> None:
    blob = make_notestore_blob("Readable text, not raw gzip bytes")

    decoded = _decode_note_blob(blob)

    assert decoded == "Readable text, not raw gzip bytes"
    assert "\x1f" not in decoded
    assert "\x8b" not in decoded


def test_apple_notes_runner_uploads_revision_html_attachment_and_metadata(tmp_path) -> None:
    store = create_notes_store(tmp_path, body_html="<p>Hello</p>")
    object_store = FakeObjectStore()
    state = AppleNotesUploadState.empty(account="zach@example.com", store_path=store)

    summary = AppleNotesUploadRunner(
        account="zach@example.com",
        store_path=store,
        object_store=object_store,
        upload_state=state,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 12, tzinfo=UTC),
    ).sync()

    assert summary.notes_seen == 1
    assert summary.revisions_uploaded == 1
    assert summary.metadata_uploaded == 1
    assert summary.body_html_uploaded == 1
    assert summary.attachments_uploaded == 1
    assert len(object_store.json_uploads) == 1
    payload = object_store.json_uploads[0]["payload"]
    assert payload["source"] == "apple_notes"
    assert payload["note"]["note_id"] == "note-1"
    assert payload["note"]["revision_id"] == payload["note"]["content_sha256"]
    assert "storage_key" not in str(payload)
    assert any(upload["kind"] == "apple_note_body_html" for upload in object_store.file_uploads)
    assert any(upload["kind"] == "apple_note_attachment" for upload in object_store.file_uploads)


def test_apple_notes_runner_limit_defers_remaining_changed_notes(tmp_path) -> None:
    store = create_notes_store(tmp_path, body_html="<p>Hello</p>")
    add_note(
        store,
        note_id="note-2",
        title="Later",
        modified_at="2026-05-21T13:00:00+00:00",
        body_html="<p>Later</p>",
    )
    object_store = FakeObjectStore()
    state = AppleNotesUploadState.empty(account="zach@example.com", store_path=store)

    summary = AppleNotesUploadRunner(
        account="zach@example.com",
        store_path=store,
        object_store=object_store,
        upload_state=state,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 14, tzinfo=UTC),
        limit=1,
    ).sync()

    assert summary.notes_seen == 2
    assert summary.notes_selected == 1
    assert summary.notes_deferred == 1
    assert summary.metadata_uploaded == 1
    assert len(state.entries) == 1


def test_apple_notes_runner_remote_existing_check_marks_state_without_upload(tmp_path) -> None:
    store = create_notes_store(tmp_path, body_html="<p>Hello</p>")
    [note] = scan_apple_notes_store(store, attachments_root=tmp_path)
    revision = revision_from_note(note)
    object_store = FakeObjectStore()
    object_store.objects.add(("apple_note_revision_metadata", "revision_id", revision.revision_id))
    state = AppleNotesUploadState.empty(account="zach@example.com", store_path=store)
    saves: list[int] = []

    summary = AppleNotesUploadRunner(
        account="zach@example.com",
        store_path=store,
        object_store=object_store,
        upload_state=state,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 12, tzinfo=UTC),
        check_remote_existing=True,
        state_save_callback=lambda: saves.append(len(state.entries)),
    ).sync()

    assert summary.notes_selected == 1
    assert summary.revisions_uploaded == 0
    assert summary.metadata_uploaded == 0
    assert object_store.file_uploads == []
    assert object_store.json_uploads == []
    assert state.entries["note-1"].complete is True
    assert saves == [1]


def test_apple_notes_runner_skips_unchanged_notes_from_state(tmp_path) -> None:
    store = create_notes_store(tmp_path, body_html="<p>Hello</p>")
    state = AppleNotesUploadState.empty(account="zach@example.com", store_path=store)
    first_store = FakeObjectStore()

    AppleNotesUploadRunner(
        account="zach@example.com",
        store_path=store,
        object_store=first_store,
        upload_state=state,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 12, tzinfo=UTC),
    ).sync()
    second_store = FakeObjectStore()

    summary = AppleNotesUploadRunner(
        account="zach@example.com",
        store_path=store,
        object_store=second_store,
        upload_state=state,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 12, 5, tzinfo=UTC),
    ).sync()

    assert summary.notes_skipped == 1
    assert summary.notes_selected == 0
    assert second_store.file_uploads == []
    assert second_store.json_uploads == []


def test_apple_notes_runner_emits_tombstone_for_deleted_note(tmp_path) -> None:
    store = create_notes_store(tmp_path, body_html="<p>Hello</p>")
    state = AppleNotesUploadState.empty(account="zach@example.com", store_path=store)
    first_store = FakeObjectStore()

    AppleNotesUploadRunner(
        account="zach@example.com",
        store_path=store,
        object_store=first_store,
        upload_state=state,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 12, tzinfo=UTC),
    ).sync()
    clear_notes(store)
    second_store = FakeObjectStore()

    summary = AppleNotesUploadRunner(
        account="zach@example.com",
        store_path=store,
        object_store=second_store,
        upload_state=state,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 13, tzinfo=UTC),
    ).sync()

    assert summary.notes_seen == 0
    assert summary.notes_deleted == 1
    assert summary.metadata_uploaded == 1
    payload = second_store.json_uploads[0]["payload"]
    assert payload["note"]["is_deleted"] is True


def create_notes_store(tmp_path: Path, *, body_html: str) -> Path:
    attachment_dir = tmp_path / "files"
    attachment_dir.mkdir()
    attachment = attachment_dir / "photo.txt"
    attachment.write_text("attachment body")
    store = tmp_path / "NoteStore.sqlite"
    connection = sqlite3.connect(store)
    try:
        connection.executescript(
            """
            CREATE TABLE accounts (
                account_id TEXT PRIMARY KEY,
                name TEXT
            );
            CREATE TABLE folders (
                folder_id TEXT PRIMARY KEY,
                name TEXT,
                parent_folder_id TEXT,
                account_id TEXT
            );
            CREATE TABLE notes (
                note_id TEXT PRIMARY KEY,
                title TEXT,
                folder_id TEXT,
                account_id TEXT,
                created_at TEXT,
                modified_at TEXT,
                body_html TEXT,
                body_text TEXT,
                is_deleted INTEGER
            );
            CREATE TABLE attachments (
                attachment_id TEXT PRIMARY KEY,
                note_id TEXT,
                filename TEXT,
                content_type TEXT,
                path TEXT,
                size_bytes INTEGER,
                content_sha256 TEXT,
                is_missing INTEGER,
                error TEXT
            );
            """
        )
        connection.execute("INSERT INTO accounts VALUES (?, ?)", ("icloud", "iCloud"))
        connection.execute("INSERT INTO folders VALUES (?, ?, ?, ?)", ("work", "Work", "", "icloud"))
        connection.execute("INSERT INTO folders VALUES (?, ?, ?, ?)", ("projects", "Projects", "work", "icloud"))
        connection.execute(
            "INSERT INTO notes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "note-1",
                "Planning",
                "projects",
                "icloud",
                "2026-05-20T12:00:00+00:00",
                "2026-05-21T12:00:00+00:00",
                body_html,
                "",
                0,
            ),
        )
        connection.execute(
            "INSERT INTO attachments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("att-1", "note-1", "photo.txt", "text/plain", "files/photo.txt", 0, "", 0, ""),
        )
        connection.commit()
    finally:
        connection.close()
    return store


def add_note(
    store: Path,
    *,
    note_id: str,
    title: str,
    modified_at: str,
    body_html: str,
) -> None:
    connection = sqlite3.connect(store)
    try:
        connection.execute(
            "INSERT INTO notes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                note_id,
                title,
                "projects",
                "icloud",
                "2026-05-20T12:00:00+00:00",
                modified_at,
                body_html,
                "",
                0,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def create_core_data_notes_store(tmp_path: Path) -> Path:
    media_dir = tmp_path / "Accounts" / "LOCAL-ACCOUNT" / "Media" / "MEDIA-1" / "1_ATTACHMENT"
    media_dir.mkdir(parents=True)
    attachment = media_dir / "archive.jpg"
    attachment.write_text("core media attachment")
    store = tmp_path / "CoreDataNoteStore.sqlite"
    connection = sqlite3.connect(store)
    try:
        connection.executescript(
            """
            CREATE TABLE Z_PRIMARYKEY (
                Z_ENT INTEGER PRIMARY KEY,
                Z_NAME TEXT
            );
            CREATE TABLE ZICCLOUDSYNCINGOBJECT (
                Z_PK INTEGER PRIMARY KEY,
                Z_ENT INTEGER,
                ZIDENTIFIER TEXT,
                ZTITLE TEXT,
                ZTITLE1 TEXT,
                ZSNIPPET TEXT,
                ZFOLDER INTEGER,
                ZACCOUNT INTEGER,
                ZNOTE INTEGER,
                ZMEDIA INTEGER,
                ZFILENAME TEXT,
                ZTYPEUTI TEXT,
                ZNOTEDATA INTEGER,
                ZCREATIONDATE3 TEXT,
                ZCREATIONDATE1 TEXT,
                ZMODIFICATIONDATE1 TEXT
            );
            CREATE TABLE ZICNOTEDATA (
                Z_PK INTEGER PRIMARY KEY,
                ZDATA BLOB
            );
            """
        )
        connection.executemany(
            "INSERT INTO Z_PRIMARYKEY VALUES (?, ?)",
            [
                (1, "ICNote"),
                (2, "ICFolder"),
                (3, "ICAccount"),
                (5, "ICAttachment"),
                (11, "ICMedia"),
            ],
        )
        connection.executemany(
            "INSERT INTO ZICCLOUDSYNCINGOBJECT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (10, 3, "ACCOUNT-1", "iCloud", "", "", None, None, None, None, "", "", None, "", "", ""),
                (20, 2, "FOLDER-1", "Notes", "", "", None, 10, None, None, "", "", None, "", "", ""),
                (
                    100,
                    1,
                    "NOTE-1",
                    "",
                    "Core Note",
                    "Body",
                    20,
                    10,
                    None,
                    None,
                    "",
                    "",
                    400,
                    "2026-05-19T12:00:00+00:00",
                    "2026-05-20T12:00:00+00:00",
                    "2026-05-21T12:00:00+00:00",
                ),
                (200, 11, "MEDIA-1", "", "", "", None, None, None, None, "archive.jpg", "", None, "", "", ""),
                (
                    300,
                    5,
                    "ATTACHMENT-1",
                    "archive.jpg",
                    "",
                    "",
                    None,
                    None,
                    100,
                    200,
                    "",
                    "public.jpeg",
                    None,
                    "",
                    "",
                    "",
                ),
            ],
        )
        connection.execute("INSERT INTO ZICNOTEDATA VALUES (?, ?)", (400, make_notestore_blob("Clean protobuf body")))
        connection.commit()
    finally:
        connection.close()
    return store


def clear_notes(store: Path) -> None:
    connection = sqlite3.connect(store)
    try:
        connection.execute("DELETE FROM notes")
        connection.execute("DELETE FROM attachments")
        connection.commit()
    finally:
        connection.close()


def make_notestore_blob(note_text: str) -> bytes:
    note = proto_field(2, note_text.encode("utf-8"))
    document = proto_field(3, note)
    note_store = proto_field(2, document)
    return gzip.compress(note_store)


def proto_field(field_number: int, value: bytes) -> bytes:
    return proto_varint((field_number << 3) | 2) + proto_varint(len(value)) + value


def proto_varint(value: int) -> bytes:
    chunks = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            chunks.append(byte | 0x80)
        else:
            chunks.append(byte)
            return bytes(chunks)
