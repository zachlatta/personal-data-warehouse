from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import gzip
import json
import sqlite3
import warnings

from personal_data_warehouse.config import GOOGLE_DRIVE_SCOPE, load_settings
from personal_data_warehouse_whatsapp.batcher import WhatsAppBatcher
from personal_data_warehouse_whatsapp.client import WhatsAppClientRunner, write_qr_artifacts
from personal_data_warehouse_whatsapp.events import (
    chat_type_for_jid,
    history_sync_records,
    message_record_from_event,
    normalize_jid_string,
    timestamp_to_datetime,
)
from personal_data_warehouse_whatsapp.state import WhatsAppUploadState
from personal_data_warehouse_whatsapp.session_store import PostgresWhatsAppSessionStore, sqlite_database_bytes

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from neonize.proto import Neonize_pb2 as N
    from neonize.proto.waCommon import WACommon_pb2 as C
    from neonize.proto.waE2E import WAWebProtobufsE2E_pb2 as E
    from neonize.proto.waHistorySync import WAWebProtobufsHistorySync_pb2 as H
    from neonize.proto.waWeb import WAWebProtobufsWeb_pb2 as W

MESSAGE_AT = datetime(2026, 5, 21, 12, tzinfo=UTC)
UNIX_TS = int(MESSAGE_AT.timestamp())


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
        self.file_uploads.append(
            {
                "object_key": object_key,
                "content_sha256": content_sha256,
                "content_type": content_type,
                "kind": kind or "file",
                "app_properties": app_properties or {},
                "bytes": path.read_bytes(),
            }
        )
        return {
            "storage_backend": self.backend,
            "storage_key": object_key,
            "storage_file_id": f"file-{content_sha256[:8]}",
            "storage_url": f"https://example.test/{object_key}",
        }


class FailOnceBatchObjectStore(FakeObjectStore):
    def __init__(self) -> None:
        super().__init__()
        self.fail_next_batch = True

    def put_file(self, **kwargs):
        if kwargs.get("kind") == "whatsapp_export_batch" and self.fail_next_batch:
            self.fail_next_batch = False
            raise RuntimeError("transient batch upload failure")
        return super().put_file(**kwargs)


class FakeSessionWarehouse:
    def __init__(self) -> None:
        self.row: dict[str, object] | None = None

    def get_whatsapp_client_session(self, *, account: str, session_key: str):
        if self.row is None:
            return None
        if self.row["account"] == account and self.row["session_key"] == session_key:
            return dict(self.row)
        return None

    def upsert_whatsapp_client_session(
        self,
        *,
        account: str,
        session_key: str,
        client_id: str,
        database_bytes: bytes,
        updated_at,
        restored_at=None,
    ):
        row = {
            "account": account,
            "session_key": session_key,
            "client_id": client_id,
            "database_bytes": database_bytes,
            "database_sha256": "sha",
            "database_bytes_size": len(database_bytes),
            "updated_at": updated_at,
        }
        self.row = row
        return row


class FakeClient:
    def __init__(self) -> None:
        self.event = FakeEventRegistry()


class FakeEventRegistry:
    def __init__(self) -> None:
        self.list_func = {1: lambda _client, _event: None}

    def __call__(self, event_type):
        def register(callback):
            self.list_func[event_type.code] = callback

        return register


class AlreadyRegisteredEvent:
    code = 1


class MissingEvent:
    code = 2


def message_event(
    *,
    message: E.Message,
    message_id: str = "msg-1",
    chat_user: str = "15551234567",
    is_from_me: bool = False,
    push_name: str = "Tester",
) -> N.Message:
    return N.Message(
        Info=N.MessageInfo(
            MessageSource=N.MessageSource(
                Chat=N.JID(User=chat_user, Server="s.whatsapp.net"),
                Sender=N.JID(User=chat_user, Server="s.whatsapp.net"),
                IsFromMe=is_from_me,
            ),
            ID=message_id,
            Type="text",
            Pushname=push_name,
            Timestamp=UNIX_TS,
        ),
        Message=message,
    )


def test_load_settings_builds_whatsapp_client_config(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    monkeypatch.setenv("WHATSAPP_ACCOUNT", "zach@example.com")
    monkeypatch.setenv("WHATSAPP_GOOGLE_DRIVE_FOLDER_ID", "folder-id")
    monkeypatch.setenv("WHATSAPP_CLIENT_ENABLED", "1")
    monkeypatch.setenv("WHATSAPP_PAIR_PHONE", "15551234567")

    settings = load_settings(require_postgres=False, require_gmail=False, require_whatsapp=True)

    assert settings.whatsapp is not None
    assert settings.whatsapp.account == "zach@example.com"
    assert settings.whatsapp.client_enabled is True
    assert settings.whatsapp.pair_phone == "15551234567"
    assert settings.whatsapp.session_key == "default"
    assert settings.whatsapp.client_id == ""
    assert settings.whatsapp.client_run_seconds == 10800
    assert settings.whatsapp.session_path.endswith("whatsapp-session.sqlite3")
    assert GOOGLE_DRIVE_SCOPE in settings.google_scopes


def test_postgres_session_store_imports_and_restores_sqlite_snapshot(tmp_path) -> None:
    session_path = tmp_path / "session.sqlite3"
    connection = sqlite3.connect(session_path)
    try:
        connection.execute("CREATE TABLE sample (value TEXT NOT NULL)")
        connection.execute("INSERT INTO sample VALUES ('paired')")
        connection.commit()
    finally:
        connection.close()
    original_bytes = sqlite_database_bytes(session_path)
    warehouse = FakeSessionWarehouse()
    store = PostgresWhatsAppSessionStore(
        warehouse=warehouse,
        account="zach@example.com",
        session_key="default",
    )

    client_id = store.restore_to_path(session_path)

    assert client_id == str(session_path)
    assert warehouse.row is not None
    assert warehouse.row["database_bytes"] == original_bytes

    session_path.unlink()
    stale_sidecars = [Path(f"{session_path}{suffix}") for suffix in ("-wal", "-shm", "-journal")]
    for sidecar in stale_sidecars:
        sidecar.write_bytes(b"stale sqlite sidecar")
    restored_client_id = store.restore_to_path(session_path)

    assert restored_client_id == str(session_path)
    assert all(not sidecar.exists() for sidecar in stale_sidecars)
    restored = sqlite3.connect(session_path)
    try:
        assert restored.execute("SELECT value FROM sample").fetchone()[0] == "paired"
    finally:
        restored.close()


def test_client_registers_noop_handlers_for_unhandled_neonize_events(tmp_path) -> None:
    client = FakeClient()
    runner = WhatsAppClientRunner(
        account="zach@example.com",
        session_path=tmp_path / "session.sqlite",
        object_store=FakeObjectStore(),
        upload_state=None,
        logger=FakeLogger(),
    )

    runner._register_ignored_events(  # noqa: SLF001 - guards neonize dispatcher behavior.
        client,
        {
            AlreadyRegisteredEvent: AlreadyRegisteredEvent.code,
            MissingEvent: MissingEvent.code,
        },
    )

    assert sorted(client.event.list_func) == [1, 2]
    assert client.event.list_func[2](client, object()) is None


def test_write_qr_artifacts_creates_refreshing_pairing_page(tmp_path) -> None:
    write_qr_artifacts(b"pairing-payload", output_dir=tmp_path)

    assert (tmp_path / "whatsapp-pairing.png").read_bytes()
    assert (tmp_path / "whatsapp-pairing.txt").read_text() == "pairing-payload"
    html = (tmp_path / "whatsapp-pairing.html").read_text()
    assert "whatsapp-pairing.png" in html
    assert "pairing-payload" in html


def test_jid_and_timestamp_helpers() -> None:
    assert normalize_jid_string("15551234567:12@s.whatsapp.net") == "15551234567@s.whatsapp.net"
    assert chat_type_for_jid("15551234567@s.whatsapp.net") == "user"
    assert chat_type_for_jid("12345-67890@g.us") == "group"
    assert timestamp_to_datetime(UNIX_TS) == MESSAGE_AT
    assert timestamp_to_datetime(UNIX_TS * 1000) == MESSAGE_AT
    assert timestamp_to_datetime(0) == datetime.fromtimestamp(0, tz=UTC)


def test_text_message_event_converts_to_payload() -> None:
    record = message_record_from_event(message_event(message=E.Message(conversation="hello there")))

    assert record is not None
    assert record.media is None
    assert record.payload["chat_id"] == "15551234567@s.whatsapp.net"
    assert record.payload["message_id"] == "msg-1"
    assert record.payload["sender_jid"] == "15551234567@s.whatsapp.net"
    assert record.payload["push_name"] == "Tester"
    assert record.payload["body_text"] == "hello there"
    assert record.payload["message_kind"] == "text"
    assert record.payload["message_at"] == MESSAGE_AT.isoformat()
    assert record.payload["is_deleted"] is False


def test_image_message_event_yields_media_record_with_caption_body() -> None:
    message = E.Message(
        imageMessage=E.ImageMessage(
            mimetype="image/jpeg",
            caption="look at this",
            fileLength=2048,
            fileSHA256=b"\x01\x02\x03",
        )
    )
    record = message_record_from_event(message_event(message=message))

    assert record is not None
    assert record.payload["message_kind"] == "image"
    assert record.payload["media_type"] == "image"
    assert record.payload["body_text"] == "look at this"
    assert record.media is not None
    assert record.media.payload["mime_type"] == "image/jpeg"
    assert record.media.payload["total_bytes"] == 2048
    assert record.media.payload["file_sha256"] == "010203"
    assert record.media.payload["is_missing"] is True
    # The event proto copies the message on construction, so compare content:
    # the carried e2e message must be the unwrapped media-bearing one.
    assert record.media.e2e_message.imageMessage.mimetype == "image/jpeg"


def test_voice_note_and_quoted_reply_classification() -> None:
    voice = message_record_from_event(
        message_event(message=E.Message(audioMessage=E.AudioMessage(mimetype="audio/ogg", PTT=True)))
    )
    assert voice is not None
    assert voice.payload["message_kind"] == "voice"

    reply = message_record_from_event(
        message_event(
            message=E.Message(
                extendedTextMessage=E.ExtendedTextMessage(
                    text="replying",
                    contextInfo=E.ContextInfo(stanzaID="original-id"),
                )
            )
        )
    )
    assert reply is not None
    assert reply.payload["body_text"] == "replying"
    assert reply.payload["quoted_message_id"] == "original-id"


def test_revoke_protocol_message_marks_target_deleted() -> None:
    message = E.Message(
        protocolMessage=E.ProtocolMessage(
            type=E.ProtocolMessage.REVOKE,
            key=C.MessageKey(ID="target-id"),
        )
    )
    record = message_record_from_event(message_event(message=message, message_id="revoker-id"))

    assert record is not None
    assert record.payload["message_id"] == "target-id"
    assert record.payload["message_kind"] == "revoke"
    assert record.payload["is_deleted"] is True


def test_device_sent_wrapper_is_unwrapped() -> None:
    message = E.Message(
        deviceSentMessage=E.DeviceSentMessage(message=E.Message(conversation="from my phone"))
    )
    record = message_record_from_event(message_event(message=message, is_from_me=True))

    assert record is not None
    assert record.payload["body_text"] == "from my phone"
    assert record.payload["is_from_me"] is True
    # Live events report the sender JID (our own) even for from-me messages;
    # the searchable_text view maps is_from_me rows to who='me'.
    assert record.payload["sender_jid"] == "15551234567@s.whatsapp.net"


def test_history_sync_records_extract_chats_messages_and_pushnames() -> None:
    history = H.HistorySync(
        syncType=H.HistorySync.RECENT,
        conversations=[
            H.Conversation(
                ID="15551234567@s.whatsapp.net",
                name="Old Friend",
                conversationTimestamp=UNIX_TS,
                messages=[
                    H.HistorySyncMsg(
                        message=W.WebMessageInfo(
                            key=C.MessageKey(remoteJID="15551234567@s.whatsapp.net", fromMe=False, ID="hist-1"),
                            message=E.Message(conversation="old message"),
                            messageTimestamp=UNIX_TS,
                            pushName="Old Friend",
                        )
                    )
                ],
            )
        ],
        pushnames=[H.Pushname(ID="15551234567@s.whatsapp.net", pushname="Old Friend")],
    )

    records = history_sync_records(history)

    assert records.chats[0]["chat_id"] == "15551234567@s.whatsapp.net"
    assert records.chats[0]["name"] == "Old Friend"
    assert records.chats[0]["chat_type"] == "user"
    assert records.contacts[0]["jid"] == "15551234567@s.whatsapp.net"
    assert records.contacts[0]["push_name"] == "Old Friend"
    assert records.messages[0].payload["message_id"] == "hist-1"
    assert records.messages[0].payload["body_text"] == "old message"
    assert records.messages[0].payload["message_at"] == MESSAGE_AT.isoformat()


def test_batcher_flushes_batch_with_media_then_skips_unchanged(tmp_path) -> None:
    state = WhatsAppUploadState.open(tmp_path / "state.sqlite", account="zach@example.com", store_path="session")
    object_store = FakeObjectStore()
    batcher = WhatsAppBatcher(
        account="zach@example.com",
        object_store=object_store,
        upload_state=state,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 13, tzinfo=UTC),
    )
    try:
        image = E.Message(
            imageMessage=E.ImageMessage(mimetype="image/jpeg", caption="pic", fileLength=9, fileSHA256=b"\x09")
        )
        record = message_record_from_event(message_event(message=image))
        assert record is not None and record.media is not None
        batcher.add_message(record.payload)
        batcher.add_media(record.media, downloader=lambda _msg: b"media body")
        batcher.add_chat(
            {
                "chat_id": "15551234567@s.whatsapp.net",
                "name": "Test Person",
                "chat_type": "user",
                "is_archived": False,
                "last_message_at": MESSAGE_AT.isoformat(),
                "raw": {},
            }
        )

        summary = batcher.flush()

        assert summary.batches_uploaded == 1
        assert summary.media_uploaded == 1
        batch_uploads = [u for u in object_store.file_uploads if u["kind"] == "whatsapp_export_batch"]
        media_uploads = [u for u in object_store.file_uploads if u["kind"] == "whatsapp_media_item"]
        assert len(batch_uploads) == 1
        assert len(media_uploads) == 1
        assert media_uploads[0]["bytes"] == b"media body"
        assert media_uploads[0]["object_key"].startswith("whatsapp/inbox/media/2026/05/")
        records = [
            json.loads(line)
            for line in gzip.decompress(batch_uploads[0]["bytes"]).decode("utf-8").splitlines()
        ]
        types = sorted(record["record_type"] for record in records)
        # The media item appears twice: the metadata manifest plus the
        # downloaded-blob record; ingest dedupe keeps the latter.
        assert types == ["chat", "media_item", "media_item", "message"]
        media_record = [r for r in records if r["record_type"] == "media_item"][-1]
        assert media_record["record"]["is_missing"] is False
        assert media_record["record"]["size_bytes"] == 10
        assert "file" in media_record["record"]

        # Re-adding identical payloads is a no-op once marked complete.
        batcher.add_message(record.payload)
        batcher.add_chat(
            {
                "chat_id": "15551234567@s.whatsapp.net",
                "name": "Test Person",
                "chat_type": "user",
                "is_archived": False,
                "last_message_at": MESSAGE_AT.isoformat(),
                "raw": {},
            }
        )
        second = batcher.flush()
        assert second.batches_uploaded == 0
        assert second.records_skipped == 2
    finally:
        state.close()


def test_batcher_requeues_records_when_batch_upload_fails() -> None:
    object_store = FailOnceBatchObjectStore()
    batcher = WhatsAppBatcher(
        account="zach@example.com",
        object_store=object_store,
        upload_state=None,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 13, tzinfo=UTC),
    )
    record = message_record_from_event(message_event(message=E.Message(conversation="retry me")))
    assert record is not None
    batcher.add_message(record.payload)

    try:
        batcher.flush()
    except RuntimeError as exc:
        assert "transient batch upload failure" in str(exc)
    else:
        raise AssertionError("expected batch upload failure")

    assert batcher.pending_counts() == (1, 0)
    summary = batcher.flush()

    assert summary.batches_uploaded == 1
    batch_uploads = [u for u in object_store.file_uploads if u["kind"] == "whatsapp_export_batch"]
    assert len(batch_uploads) == 1
