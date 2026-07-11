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


class FakeIngestClient:
    """Stands in for the app ingest client; records body/attachment/revision
    posts. file_uploads holds blob posts (with their kind), json_uploads holds
    revision metadata posts."""

    def __init__(self) -> None:
        self.file_uploads: list[dict[str, object]] = []
        self.json_uploads: list[dict[str, object]] = []

    def upload_apple_notes_body(self, html, *, note_id, revision_id, modified_at):
        self.file_uploads.append(
            {"kind": "apple_note_body_html", "content": html, "note_id": note_id, "revision_id": revision_id}
        )
        return {"storage_backend": "google_drive", "storage_key": "body", "storage_file_id": "fid-b", "storage_url": ""}

    def upload_apple_notes_attachment(self, content, *, note_id, revision_id, modified_at, attachment_id, filename, content_type):
        self.file_uploads.append(
            {
                "kind": "apple_note_attachment",
                "content": content,
                "note_id": note_id,
                "revision_id": revision_id,
                "attachment_id": attachment_id,
                "content_type": content_type,
            }
        )
        return {"storage_backend": "google_drive", "storage_key": "att", "storage_file_id": "fid-at", "storage_url": ""}

    def upload_apple_notes_revision(self, payload, *, note_id, revision_id, modified_at, note_content_sha256):
        self.json_uploads.append(
            {"payload": payload, "note_id": note_id, "revision_id": revision_id, "note_content_sha256": note_content_sha256}
        )
        return {"storage_backend": "google_drive", "storage_key": "rev", "storage_file_id": "fid-r", "storage_url": ""}


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


def test_scan_apple_notes_core_data_store_resolves_drawing_and_scan_fallback_files(tmp_path) -> None:
    store = create_core_data_notes_store(tmp_path)
    fallback_image = (
        tmp_path / "Accounts" / "LOCAL-ACCOUNT" / "FallbackImages" / "DRAWING-1" / "1_IMAGE-GEN" / "FallbackImage.png"
    )
    fallback_image.parent.mkdir(parents=True)
    fallback_image.write_bytes(b"drawing fallback image bytes")
    fallback_pdf = (
        tmp_path / "Accounts" / "LOCAL-ACCOUNT" / "FallbackPDFs" / "SCAN-1" / "1_PDF-GEN" / "FallbackPDF.pdf"
    )
    fallback_pdf.parent.mkdir(parents=True)
    fallback_pdf.write_bytes(b"scan fallback pdf bytes")
    add_core_data_attachment(
        store,
        z_pk=310,
        identifier="DRAWING-1",
        type_uti="com.apple.drawing.2",
        title="Whiteboard sketch",
        fallback_image_generation="1_IMAGE-GEN",
    )
    add_core_data_attachment(
        store,
        z_pk=311,
        identifier="SCAN-1",
        type_uti="com.apple.paper.doc.scan",
        title="Scanned receipt",
        fallback_pdf_generation="1_PDF-GEN",
    )
    dot_title_image = (
        tmp_path / "Accounts" / "LOCAL-ACCOUNT" / "FallbackImages" / "DRAWING-DOT" / "1_DOT-GEN" / "FallbackImage.png"
    )
    dot_title_image.parent.mkdir(parents=True)
    dot_title_image.write_bytes(b"dot title drawing")
    add_core_data_attachment(
        store,
        z_pk=312,
        identifier="DRAWING-DOT",
        type_uti="com.apple.paper",
        title=".",
        fallback_image_generation="1_DOT-GEN",
    )

    [note] = scan_apple_notes_store(store, attachments_root=tmp_path)

    attachments = {attachment.attachment_id: attachment for attachment in note.attachments}
    drawing = attachments["DRAWING-1"]
    assert drawing.is_missing is False
    assert drawing.path == fallback_image
    assert drawing.filename == "Whiteboard sketch.png"
    assert drawing.content_type == "image/png"
    assert drawing.size_bytes == len(b"drawing fallback image bytes")
    assert drawing.content_sha256
    scan = attachments["SCAN-1"]
    assert scan.is_missing is False
    assert scan.path == fallback_pdf
    assert scan.filename == "Scanned receipt.pdf"
    assert scan.content_type == "application/pdf"
    assert scan.content_sha256
    dot_title = attachments["DRAWING-DOT"]
    assert dot_title.is_missing is False
    assert dot_title.filename == "DRAWING-DOT.png"
    assert dot_title.content_type == "image/png"


def test_scan_apple_notes_core_data_store_labels_attachments_without_file_payloads(tmp_path) -> None:
    store = create_core_data_notes_store(tmp_path)
    add_core_data_attachment(
        store,
        z_pk=320,
        identifier="LINK-1",
        type_uti="public.url",
        title="Example post",
        url_string="https://example.com/post",
    )
    add_core_data_attachment(store, z_pk=321, identifier="TABLE-1", type_uti="com.apple.notes.table")
    add_core_data_attachment(store, z_pk=322, identifier="GALLERY-1", type_uti="com.apple.notes.gallery")
    add_core_data_attachment(
        store,
        z_pk=323,
        identifier="TAG-1",
        type_uti1="com.apple.notes.inlinetextattachment.hashtag",
    )

    [note] = scan_apple_notes_store(store, attachments_root=tmp_path)

    attachments = {attachment.attachment_id: attachment for attachment in note.attachments}
    link = attachments["LINK-1"]
    assert "URL" in link.error
    assert link.raw is not None and link.raw["ZURLSTRING"] == "https://example.com/post"
    assert "note body" in attachments["TABLE-1"].error
    assert "separate attachments" in attachments["GALLERY-1"].error
    assert "inline" in attachments["TAG-1"].error
    for attachment_id in ("LINK-1", "TABLE-1", "GALLERY-1", "TAG-1"):
        assert attachments[attachment_id].is_missing is False
        assert "not locally available" not in attachments[attachment_id].error


def test_scan_apple_notes_core_data_store_keeps_missing_file_error_for_unavailable_media(tmp_path) -> None:
    store = create_core_data_notes_store(tmp_path)
    add_core_data_attachment(
        store,
        z_pk=330,
        identifier="DRAWING-2",
        type_uti="com.apple.drawing.2",
        title="Sketch without local fallback",
        fallback_image_generation="1_ABSENT-GEN",
    )

    [note] = scan_apple_notes_store(store, attachments_root=tmp_path)

    attachments = {attachment.attachment_id: attachment for attachment in note.attachments}
    drawing = attachments["DRAWING-2"]
    assert drawing.is_missing is True
    assert drawing.error == "attachment file is not locally available"


def test_decode_note_blob_prefers_gzipped_protobuf_note_text() -> None:
    blob = make_notestore_blob("Readable text, not raw gzip bytes")

    decoded = _decode_note_blob(blob)

    assert decoded == "Readable text, not raw gzip bytes"
    assert "\x1f" not in decoded
    assert "\x8b" not in decoded


def test_apple_notes_runner_uploads_revision_html_attachment_and_metadata(tmp_path) -> None:
    store = create_notes_store(tmp_path, body_html="<p>Hello</p>")
    object_store = FakeIngestClient()
    state = AppleNotesUploadState.empty(account="zach@example.com", store_path=store)

    summary = AppleNotesUploadRunner(
        account="zach@example.com",
        store_path=store,
        ingest_client=object_store,
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
    object_store = FakeIngestClient()
    state = AppleNotesUploadState.empty(account="zach@example.com", store_path=store)

    summary = AppleNotesUploadRunner(
        account="zach@example.com",
        store_path=store,
        ingest_client=object_store,
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


def test_apple_notes_runner_skips_unchanged_notes_from_state(tmp_path) -> None:
    store = create_notes_store(tmp_path, body_html="<p>Hello</p>")
    state = AppleNotesUploadState.empty(account="zach@example.com", store_path=store)
    first_store = FakeIngestClient()

    AppleNotesUploadRunner(
        account="zach@example.com",
        store_path=store,
        ingest_client=first_store,
        upload_state=state,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 12, tzinfo=UTC),
    ).sync()
    second_store = FakeIngestClient()

    summary = AppleNotesUploadRunner(
        account="zach@example.com",
        store_path=store,
        ingest_client=second_store,
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
    first_store = FakeIngestClient()

    AppleNotesUploadRunner(
        account="zach@example.com",
        store_path=store,
        ingest_client=first_store,
        upload_state=state,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 12, tzinfo=UTC),
    ).sync()
    clear_notes(store)
    second_store = FakeIngestClient()

    summary = AppleNotesUploadRunner(
        account="zach@example.com",
        store_path=store,
        ingest_client=second_store,
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
                ZTYPEUTI1 TEXT,
                ZURLSTRING TEXT,
                ZFALLBACKIMAGEGENERATION TEXT,
                ZFALLBACKPDFGENERATION TEXT,
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
                (6, "ICInlineAttachment"),
                (11, "ICMedia"),
            ],
        )
        insert_core_data_object(connection, z_pk=10, z_ent=3, identifier="ACCOUNT-1", title="iCloud")
        insert_core_data_object(connection, z_pk=20, z_ent=2, identifier="FOLDER-1", title="Notes", account=10)
        insert_core_data_object(
            connection,
            z_pk=100,
            z_ent=1,
            identifier="NOTE-1",
            title1="Core Note",
            snippet="Body",
            folder=20,
            account=10,
            note_data=400,
            creation_date3="2026-05-19T12:00:00+00:00",
            creation_date1="2026-05-20T12:00:00+00:00",
            modification_date1="2026-05-21T12:00:00+00:00",
        )
        insert_core_data_object(connection, z_pk=200, z_ent=11, identifier="MEDIA-1", filename="archive.jpg")
        insert_core_data_object(
            connection,
            z_pk=300,
            z_ent=5,
            identifier="ATTACHMENT-1",
            title="archive.jpg",
            note=100,
            media=200,
            type_uti="public.jpeg",
        )
        connection.execute("INSERT INTO ZICNOTEDATA VALUES (?, ?)", (400, make_notestore_blob("Clean protobuf body")))
        connection.commit()
    finally:
        connection.close()
    return store


def add_core_data_attachment(
    store: Path,
    *,
    z_pk: int,
    identifier: str,
    note_pk: int = 100,
    z_ent: int | None = None,
    type_uti: str = "",
    type_uti1: str = "",
    title: str = "",
    url_string: str = "",
    fallback_image_generation: str = "",
    fallback_pdf_generation: str = "",
) -> None:
    if z_ent is None:
        z_ent = 6 if type_uti1 else 5
    connection = sqlite3.connect(store)
    try:
        insert_core_data_object(
            connection,
            z_pk=z_pk,
            z_ent=z_ent,
            identifier=identifier,
            title=title,
            note=note_pk,
            type_uti=type_uti,
            type_uti1=type_uti1,
            url_string=url_string,
            fallback_image_generation=fallback_image_generation,
            fallback_pdf_generation=fallback_pdf_generation,
        )
        connection.commit()
    finally:
        connection.close()


def insert_core_data_object(
    connection: sqlite3.Connection,
    *,
    z_pk: int,
    z_ent: int,
    identifier: str,
    title: str = "",
    title1: str = "",
    snippet: str = "",
    folder: int | None = None,
    account: int | None = None,
    note: int | None = None,
    media: int | None = None,
    filename: str = "",
    type_uti: str = "",
    type_uti1: str = "",
    url_string: str = "",
    fallback_image_generation: str = "",
    fallback_pdf_generation: str = "",
    note_data: int | None = None,
    creation_date3: str = "",
    creation_date1: str = "",
    modification_date1: str = "",
) -> None:
    connection.execute(
        """
        INSERT INTO ZICCLOUDSYNCINGOBJECT (
            Z_PK, Z_ENT, ZIDENTIFIER, ZTITLE, ZTITLE1, ZSNIPPET, ZFOLDER, ZACCOUNT, ZNOTE, ZMEDIA,
            ZFILENAME, ZTYPEUTI, ZTYPEUTI1, ZURLSTRING, ZFALLBACKIMAGEGENERATION, ZFALLBACKPDFGENERATION,
            ZNOTEDATA, ZCREATIONDATE3, ZCREATIONDATE1, ZMODIFICATIONDATE1
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            z_pk,
            z_ent,
            identifier,
            title or None,
            title1 or None,
            snippet or None,
            folder,
            account,
            note,
            media,
            filename or None,
            type_uti or None,
            type_uti1 or None,
            url_string or None,
            fallback_image_generation or None,
            fallback_pdf_generation or None,
            note_data,
            creation_date3 or None,
            creation_date1 or None,
            modification_date1 or None,
        ),
    )


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
