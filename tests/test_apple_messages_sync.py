from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
import gzip
import json
import sqlite3
import sys

from personal_data_warehouse.config import GOOGLE_DRIVE_SCOPE, load_settings
from personal_data_warehouse_apple_messages.body import decode_message_body
from personal_data_warehouse_apple_messages.scanner import (
    APPLE_EPOCH,
    parse_apple_timestamp,
    resolve_attachment_path,
    scan_apple_messages_store,
)
from personal_data_warehouse_apple_messages.state import AppleMessagesUploadState
from personal_data_warehouse_apple_messages.sync import AppleMessagesUploadRunner


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
        self.objects: set[tuple[str, str, str]] = set()

    def has_blob(self, *, content_sha256: str) -> bool:
        return ("apple_message_attachment", "content_sha256", content_sha256) in self.objects

    def has_metadata(self, *, content_sha256: str) -> bool:
        return ("apple_message_export_batch", "content_sha256", content_sha256) in self.objects

    def has_object(self, *, kind: str, key: str, value: str) -> bool:
        return (kind, key, value) in self.objects

    def presence(self, *, content_sha256: str):
        raise NotImplementedError

    def put_json(self, **kwargs):
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
        object_kind = kind or "file"
        uploaded = {
            "path": path,
            "object_key": object_key,
            "content_sha256": content_sha256,
            "content_type": content_type,
            "kind": object_kind,
            "app_properties": app_properties or {},
            "bytes": path.read_bytes(),
        }
        self.file_uploads.append(uploaded)
        self.objects.add((object_kind, "content_sha256", content_sha256))
        for key, value in (app_properties or {}).items():
            self.objects.add((object_kind, key, value))
        return {
            "storage_backend": self.backend,
            "storage_key": object_key,
            "storage_file_id": f"file-{content_sha256[:8]}",
            "storage_url": f"https://example.test/{object_key}",
        }


def test_load_settings_adds_drive_scope_when_apple_messages_uses_google_drive(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    monkeypatch.setenv("APPLE_MESSAGES_ACCOUNT", "zach@example.com")
    monkeypatch.setenv("APPLE_MESSAGES_GOOGLE_DRIVE_FOLDER_ID", "folder-id")

    settings = load_settings(require_clickhouse=False, require_gmail=False, require_apple_messages=True)

    assert settings.apple_messages is not None
    assert settings.apple_messages.account == "zach@example.com"
    assert settings.apple_messages.google_drive_folder_id == "folder-id"
    assert settings.apple_messages.upload_workers == 4
    assert GOOGLE_DRIVE_SCOPE in settings.google_scopes


def test_parse_apple_timestamp_handles_nanoseconds_from_2001_epoch() -> None:
    timestamp = apple_ns(datetime(2026, 5, 21, 12, 30, tzinfo=UTC))

    assert parse_apple_timestamp(timestamp) == datetime(2026, 5, 21, 12, 30, tzinfo=UTC)
    assert parse_apple_timestamp(0) == datetime.fromtimestamp(0, tz=UTC)


def test_resolve_attachment_path_expands_home_and_relative_roots(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))

    assert resolve_attachment_path("~/Library/Messages/Attachments/a.txt", root=tmp_path) == (
        tmp_path / "Library/Messages/Attachments/a.txt"
    )
    assert resolve_attachment_path("Attachments/a.txt", root=tmp_path) == tmp_path / "Attachments/a.txt"


def test_decode_message_body_prefers_attributed_body(monkeypatch) -> None:
    ns_string = SimpleNamespace(clazz=SimpleNamespace(name=b"NSString"), value="Decoded from attributedBody")
    attributed = SimpleNamespace(clazz=SimpleNamespace(name=b"NSAttributedString"), contents=[SimpleNamespace(value=ns_string)])
    monkeypatch.setitem(sys.modules, "typedstream", SimpleNamespace(unarchive_from_data=lambda _data: attributed))

    decoded = decode_message_body(text="Fallback text", attributed_body=b"typedstream")

    assert decoded.text == "Decoded from attributedBody"
    assert decoded.source == "attributedBody"
    assert decoded.status == "ok"


def test_scan_apple_messages_synthetic_store_extracts_messages_chats_and_attachments(tmp_path) -> None:
    store = create_messages_store(tmp_path)

    snapshot = scan_apple_messages_store(store, messages_root=tmp_path)

    assert snapshot.handles[0].address == "+15551234567"
    assert snapshot.chats[0].chat_id == "chat-guid"
    assert snapshot.chat_handles[0].handle_id == snapshot.handles[0].handle_id
    assert snapshot.messages[0].message_id == "message-guid"
    assert snapshot.messages[0].body_text if hasattr(snapshot.messages[0], "body_text") else snapshot.messages[0].text
    assert snapshot.messages[0].service == "iMessage"
    assert snapshot.messages[0].message_at == datetime(2026, 5, 21, 12, tzinfo=UTC)
    assert snapshot.chat_messages[0].chat_id == "chat-guid"
    assert snapshot.attachments[0].message_id == "message-guid"
    assert snapshot.attachments[0].resolved_path == tmp_path / "Attachments" / "photo.txt"
    assert snapshot.attachments[0].is_missing is False
    assert snapshot.deleted_messages[0].message_id == "deleted-guid"


def test_apple_messages_runner_uploads_batch_manifest_and_attachment_then_skips_unchanged(tmp_path) -> None:
    store = create_messages_store(tmp_path)
    state = AppleMessagesUploadState.open(tmp_path / "state.sqlite", account="zach@example.com", store_path=store)
    object_store = FakeObjectStore()
    try:
        first_summary = AppleMessagesUploadRunner(
            account="zach@example.com",
            store_path=store,
            object_store=object_store,
            upload_state=state,
            logger=FakeLogger(),
            now=lambda: datetime(2026, 5, 21, 13, tzinfo=UTC),
            attachment_count_per_run=1,
            attachment_bytes_per_run=1024 * 1024,
        ).sync()

        assert first_summary.messages_seen == 1
        assert first_summary.attachments_uploaded == 1
        batch_uploads = [upload for upload in object_store.file_uploads if upload["kind"] == "apple_message_export_batch"]
        attachment_uploads = [upload for upload in object_store.file_uploads if upload["kind"] == "apple_message_attachment"]
        assert len(batch_uploads) == 1
        assert len(attachment_uploads) == 1
        records = [
            json.loads(line)
            for line in gzip.decompress(batch_uploads[0]["bytes"]).decode("utf-8").splitlines()
        ]
        message_records = [record for record in records if record["record_type"] == "message"]
        attachment_records = [record for record in records if record["record_type"] == "attachment"]
        assert message_records[0]["record"]["body_text"] == "Hello from Messages"
        assert any("file" in record["record"] for record in attachment_records)

        second_summary = AppleMessagesUploadRunner(
            account="zach@example.com",
            store_path=store,
            object_store=FakeObjectStore(),
            upload_state=state,
            logger=FakeLogger(),
            now=lambda: datetime(2026, 5, 21, 14, tzinfo=UTC),
            attachment_count_per_run=1,
            attachment_bytes_per_run=1024 * 1024,
        ).sync()

        assert second_summary.records_selected == 0
        assert second_summary.batches_uploaded == 0
        assert second_summary.attachments_uploaded == 0
    finally:
        state.close()


def create_messages_store(tmp_path: Path) -> Path:
    attachment_dir = tmp_path / "Attachments"
    attachment_dir.mkdir()
    (attachment_dir / "photo.txt").write_text("attachment body")
    store = tmp_path / "chat.db"
    connection = sqlite3.connect(store)
    try:
        connection.executescript(
            """
            CREATE TABLE handle (
                id TEXT,
                country TEXT,
                service TEXT,
                uncanonicalized_id TEXT,
                person_centric_id TEXT
            );
            CREATE TABLE chat (
                guid TEXT,
                chat_identifier TEXT,
                service_name TEXT,
                display_name TEXT,
                room_name TEXT,
                account_login TEXT,
                style INTEGER,
                state INTEGER,
                is_archived INTEGER,
                is_filtered INTEGER,
                is_recovered INTEGER,
                is_pending_review INTEGER,
                last_read_message_timestamp INTEGER
            );
            CREATE TABLE chat_handle_join (
                chat_id INTEGER,
                handle_id INTEGER
            );
            CREATE TABLE message (
                guid TEXT,
                handle_id INTEGER,
                service TEXT,
                account TEXT,
                text TEXT,
                attributedBody BLOB,
                subject TEXT,
                country TEXT,
                type INTEGER,
                item_type INTEGER,
                is_from_me INTEGER,
                is_read INTEGER,
                is_sent INTEGER,
                is_delivered INTEGER,
                is_finished INTEGER,
                is_system_message INTEGER,
                is_service_message INTEGER,
                is_forward INTEGER,
                is_empty INTEGER,
                is_audio_message INTEGER,
                is_played INTEGER,
                cache_has_attachments INTEGER,
                has_unseen_mention INTEGER,
                is_spam INTEGER,
                reply_to_guid TEXT,
                associated_message_guid TEXT,
                associated_message_type INTEGER,
                associated_message_emoji TEXT,
                balloon_bundle_id TEXT,
                group_title TEXT,
                group_action_type INTEGER,
                message_action_type INTEGER,
                message_source INTEGER,
                expressive_send_style_id TEXT,
                date INTEGER,
                date_read INTEGER,
                date_delivered INTEGER,
                date_played INTEGER,
                date_edited INTEGER,
                date_retracted INTEGER,
                date_recovered INTEGER
            );
            CREATE TABLE chat_message_join (
                chat_id INTEGER,
                message_id INTEGER,
                message_date INTEGER
            );
            CREATE TABLE attachment (
                guid TEXT,
                original_guid TEXT,
                filename TEXT,
                transfer_name TEXT,
                mime_type TEXT,
                uti TEXT,
                total_bytes INTEGER,
                is_outgoing INTEGER,
                is_sticker INTEGER,
                hide_attachment INTEGER,
                transfer_state INTEGER,
                created_date INTEGER,
                start_date INTEGER
            );
            CREATE TABLE message_attachment_join (
                message_id INTEGER,
                attachment_id INTEGER
            );
            CREATE TABLE deleted_messages (
                guid TEXT
            );
            """
        )
        message_at = apple_ns(datetime(2026, 5, 21, 12, tzinfo=UTC))
        connection.execute(
            "INSERT INTO handle (id, country, service, uncanonicalized_id, person_centric_id) VALUES (?, ?, ?, ?, ?)",
            ("+15551234567", "US", "iMessage", "+15551234567", "person-1"),
        )
        connection.execute(
            """
            INSERT INTO chat (
                guid, chat_identifier, service_name, display_name, room_name, account_login,
                style, state, is_archived, is_filtered, is_recovered, is_pending_review,
                last_read_message_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("chat-guid", "+15551234567", "iMessage", "Test Chat", "", "zach@example.com", 45, 0, 0, 0, 0, 0, message_at),
        )
        connection.execute("INSERT INTO chat_handle_join (chat_id, handle_id) VALUES (1, 1)")
        connection.execute(
            """
            INSERT INTO message (
                guid, handle_id, service, account, text, attributedBody, subject, country,
                type, item_type, is_from_me, is_read, is_sent, is_delivered, is_finished,
                is_system_message, is_service_message, is_forward, is_empty, is_audio_message,
                is_played, cache_has_attachments, has_unseen_mention, is_spam, reply_to_guid,
                associated_message_guid, associated_message_type, associated_message_emoji,
                balloon_bundle_id, group_title, group_action_type, message_action_type,
                message_source, expressive_send_style_id, date, date_read, date_delivered,
                date_played, date_edited, date_retracted, date_recovered
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "message-guid",
                1,
                "iMessage",
                "zach@example.com",
                "Hello from Messages",
                None,
                "",
                "US",
                0,
                0,
                0,
                1,
                1,
                1,
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                1,
                0,
                0,
                "",
                "",
                0,
                "",
                "",
                "",
                0,
                0,
                0,
                "",
                message_at,
                0,
                0,
                0,
                0,
                0,
                0,
            ),
        )
        connection.execute("INSERT INTO chat_message_join (chat_id, message_id, message_date) VALUES (1, 1, ?)", (message_at,))
        connection.execute(
            """
            INSERT INTO attachment (
                guid, original_guid, filename, transfer_name, mime_type, uti, total_bytes,
                is_outgoing, is_sticker, hide_attachment, transfer_state, created_date, start_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "attachment-guid",
                "",
                "Attachments/photo.txt",
                "photo.txt",
                "text/plain",
                "public.plain-text",
                15,
                0,
                0,
                0,
                5,
                message_at,
                message_at,
            ),
        )
        connection.execute("INSERT INTO message_attachment_join (message_id, attachment_id) VALUES (1, 1)")
        connection.execute("INSERT INTO deleted_messages (guid) VALUES ('deleted-guid')")
        connection.commit()
    finally:
        connection.close()
    return store


def apple_ns(value: datetime) -> int:
    return int((value - APPLE_EPOCH).total_seconds() * 1_000_000_000)
