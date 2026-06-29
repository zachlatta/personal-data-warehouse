from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import gzip
import json
import sqlite3
import warnings

from personal_data_warehouse.config import GOOGLE_DRIVE_SCOPE, load_settings
from personal_data_warehouse.ingest_client import CLOUDFLARE_MAX_BODY_BYTES
from personal_data_warehouse.objectstore import ObjectListing
from personal_data_warehouse.objectstore.google_drive import object_stage
from personal_data_warehouse.whatsapp_drive_ingest import (
    WhatsAppDriveIngestRunner,
    iter_batch_payloads,
)
from personal_data_warehouse_whatsapp.batcher import (
    BATCH_RECORD_KIND,
    MEDIA_KIND,
    WhatsAppBatcher,
    batch_object_key,
    media_object_key,
)
from personal_data_warehouse_whatsapp.client import WhatsAppClientRunner, write_qr_artifacts
from personal_data_warehouse_whatsapp.events import (
    chat_payload_from_group_info,
    chat_type_for_jid,
    history_sync_records,
    message_record_from_event,
    normalize_jid_string,
    participant_payloads_from_group_info,
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
    """Stands in for the Drive ObjectStore; records every put_file (tagged by
    kind), with the written object key, app properties, and bytes."""

    backend = "google_drive"

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


def test_load_settings_can_disable_whatsapp_client(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.com")
    monkeypatch.setenv("WHATSAPP_ACCOUNT", "zach@example.com")
    monkeypatch.setenv("WHATSAPP_GOOGLE_DRIVE_FOLDER_ID", "folder-id")
    monkeypatch.setenv("WHATSAPP_CLIENT_ENABLED", "0")

    settings = load_settings(require_postgres=False, require_gmail=False, require_whatsapp=True)

    assert settings.whatsapp is not None
    assert settings.whatsapp.client_enabled is False


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
    assert chat_type_for_jid("9999@lid") == "user"
    # The status feed is its own kind, distinct from broadcast lists.
    assert chat_type_for_jid("status@broadcast") == "status"
    assert chat_type_for_jid("12345@broadcast") == "broadcast"
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
    # search_text() maps is_from_me rows to who='me'.
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


def _group_info() -> "N.GroupInfo":
    return N.GroupInfo(
        JID=N.JID(User="120363274447440808", Server="g.us"),
        OwnerJID=N.JID(User="15550000001", Server="s.whatsapp.net"),
        GroupName=N.GroupName(Name="Founders Group"),
        GroupTopic=N.GroupTopic(Topic="intros & deals"),
        GroupCreated=UNIX_TS,
        Participants=[
            N.GroupParticipant(
                JID=N.JID(User="15550000001", Server="s.whatsapp.net"),
                DisplayName="Alice",
                IsSuperAdmin=True,
            ),
            N.GroupParticipant(
                JID=N.JID(User="15550000002", Server="s.whatsapp.net"),
                DisplayName="Bob",
                IsAdmin=True,
            ),
            N.GroupParticipant(
                JID=N.JID(User="15550000003", Server="s.whatsapp.net"),
                PhoneNumber=N.JID(User="15550000003", Server="s.whatsapp.net"),
            ),
        ],
    )


def test_chat_payload_from_group_info_extracts_subject_and_metadata() -> None:
    chat = chat_payload_from_group_info(_group_info())

    assert chat is not None
    assert chat["chat_id"] == "120363274447440808@g.us"
    assert chat["name"] == "Founders Group"
    assert chat["chat_type"] == "group"
    assert chat["last_message_at"] == datetime.fromtimestamp(0, tz=UTC).isoformat()
    assert chat["raw"]["source"] == "group_info"
    assert chat["raw"]["topic"] == "intros & deals"
    assert chat["raw"]["owner_jid"] == "15550000001@s.whatsapp.net"
    assert chat["raw"]["participant_count"] == 3


def test_participant_payloads_from_group_info_capture_roles() -> None:
    participants = participant_payloads_from_group_info(_group_info())

    assert [p["participant_jid"] for p in participants] == [
        "15550000001@s.whatsapp.net",
        "15550000002@s.whatsapp.net",
        "15550000003@s.whatsapp.net",
    ]
    assert participants[0]["display_name"] == "Alice"
    assert participants[0]["is_super_admin"] is True
    assert participants[0]["is_admin"] is False
    assert participants[1]["is_admin"] is True
    assert participants[2]["phone_jid"] == "15550000003@s.whatsapp.net"
    assert all(p["chat_id"] == "120363274447440808@g.us" for p in participants)


def test_group_info_without_jid_is_skipped() -> None:
    assert chat_payload_from_group_info(N.GroupInfo(GroupName=N.GroupName(Name="x"))) is None
    assert participant_payloads_from_group_info(N.GroupInfo()) == []


class FakeGroupsClient:
    def __init__(self, groups) -> None:
        self._groups = groups
        self.calls = 0

    def get_joined_groups(self):
        self.calls += 1
        return self._groups


def test_dump_groups_queues_named_chats_and_participants(tmp_path) -> None:
    runner = WhatsAppClientRunner(
        account="zach@example.com",
        session_path=tmp_path / "session.sqlite",
        object_store=FakeObjectStore(),
        upload_state=None,
        logger=FakeLogger(),
    )
    client = FakeGroupsClient([_group_info()])

    runner._dump_groups(client)  # noqa: SLF001 - exercises the joined-groups dump

    pending, _ = runner._batcher.pending_counts()  # noqa: SLF001
    assert pending == 4  # one chat + three participants
    assert runner._totals["chats_received"] == 1  # noqa: SLF001
    assert runner._totals["participants_received"] == 3  # noqa: SLF001
    assert runner._groups_dumped is True  # noqa: SLF001


def test_dump_groups_retries_after_failure(tmp_path) -> None:
    class BoomClient:
        def get_joined_groups(self):
            raise RuntimeError("not connected yet")

    runner = WhatsAppClientRunner(
        account="zach@example.com",
        session_path=tmp_path / "session.sqlite",
        object_store=FakeObjectStore(),
        upload_state=None,
        logger=FakeLogger(),
    )

    runner._dump_groups(BoomClient())  # noqa: SLF001

    # A transient failure must leave the flag clear so the next flush retries.
    assert runner._groups_dumped is False  # noqa: SLF001
    assert runner._batcher.pending_counts()[0] == 0  # noqa: SLF001


def test_batcher_flushes_batch_with_media_then_skips_unchanged(tmp_path) -> None:
    state = WhatsAppUploadState.open(tmp_path / "state.sqlite", account="zach@example.com", store_path="session")
    store = FakeObjectStore()
    batcher = WhatsAppBatcher(
        account="zach@example.com",
        object_store=store,
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
        batch_uploads = [u for u in store.file_uploads if u["kind"] == "whatsapp_export_batch"]
        media_uploads = [u for u in store.file_uploads if u["kind"] == "whatsapp_media_item"]
        assert len(batch_uploads) == 1
        assert len(media_uploads) == 1
        assert media_uploads[0]["bytes"] == b"media body"
        # The client builds the object key + tags directly for the Drive writer.
        assert media_uploads[0]["object_key"].startswith("whatsapp/inbox/media/2026/05/")
        assert media_uploads[0]["app_properties"]["chat_id"]
        assert media_uploads[0]["app_properties"]["message_id"]
        assert batch_uploads[0]["object_key"].startswith("whatsapp/inbox/batches/2026/05/")
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
    store = FailOnceBatchObjectStore()
    batcher = WhatsAppBatcher(
        account="zach@example.com",
        object_store=store,
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
    batch_uploads = [u for u in store.file_uploads if u["kind"] == "whatsapp_export_batch"]
    assert len(batch_uploads) == 1


def _image_media_record():
    image = E.Message(
        imageMessage=E.ImageMessage(mimetype="image/jpeg", caption="pic", fileLength=9, fileSHA256=b"\x09")
    )
    record = message_record_from_event(message_event(message=image))
    assert record is not None and record.media is not None
    return record


def test_batcher_writes_drive_objects_with_reader_contract_tags(tmp_path) -> None:
    """The client must write objects whose kind/stage/key/app_properties exactly
    match what the whatsapp_drive_ingest reader queries by (no app-HTTP hop)."""
    state = WhatsAppUploadState.open(tmp_path / "state.sqlite", account="zach@example.com", store_path="session")
    store = FakeObjectStore()
    exported_at = datetime(2026, 5, 21, 13, 30, 5, tzinfo=UTC)
    batcher = WhatsAppBatcher(
        account="zach@example.com",
        object_store=store,
        upload_state=state,
        logger=FakeLogger(),
        now=lambda: exported_at,
    )
    try:
        record = _image_media_record()
        batcher.add_message(record.payload)
        batcher.add_media(record.media, downloader=lambda _msg: b"media body")
        batcher.flush()
    finally:
        state.close()

    batch = next(u for u in store.file_uploads if u["kind"] == BATCH_RECORD_KIND)
    media = next(u for u in store.file_uploads if u["kind"] == MEDIA_KIND)

    # Batch object: key, kind, content_type, and the app_properties the reader
    # (and the GoogleDriveObjectStore) rely on. content_sha256 is the gzip sha.
    import hashlib

    batch_sha = hashlib.sha256(batch["bytes"]).hexdigest()
    assert batch["object_key"] == batch_object_key(exported_at=exported_at, batch_sha256=batch_sha)
    assert batch["object_key"].startswith("whatsapp/inbox/batches/2026/05/")
    assert object_stage(batch["object_key"]) == "inbox"
    assert batch["content_type"] == "application/gzip"
    assert batch["content_sha256"] == batch_sha
    assert batch["app_properties"] == {
        "batch_sha256": batch_sha,
        "exported_at": exported_at.isoformat(),
    }

    # Media object: key uses message_at date + safe(chat-message) + sha + suffix;
    # tags carry chat_id/message_id; content_sha256 is the blob sha.
    media_sha = hashlib.sha256(b"media body").hexdigest()
    payload = dict(record.media.payload)
    assert media["object_key"] == media_object_key(payload, content_sha256=media_sha, fallback_now=exported_at)
    assert object_stage(media["object_key"]) == "inbox"
    assert media["content_sha256"] == media_sha
    assert media["app_properties"]["chat_id"] == payload["chat_id"]
    assert media["app_properties"]["message_id"] == payload["message_id"]


def test_batcher_uploads_media_larger_than_cloudflare_cap(tmp_path) -> None:
    """Writing straight to Drive has no Cloudflare 100 MiB body cap: a >100 MiB
    media item uploads in one shot instead of being 413'd or deferred."""
    store = FakeObjectStore()
    batcher = WhatsAppBatcher(
        account="zach@example.com",
        object_store=store,
        upload_state=None,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 13, tzinfo=UTC),
    )
    record = _image_media_record()
    big = b"\0" * (CLOUDFLARE_MAX_BODY_BYTES + 1)
    batcher.add_message(record.payload)
    batcher.add_media(record.media, downloader=lambda _msg: big)

    summary = batcher.flush()

    assert summary.media_uploaded == 1
    assert summary.media_deferred == 0
    media = next(u for u in store.file_uploads if u["kind"] == MEDIA_KIND)
    assert len(media["bytes"]) == CLOUDFLARE_MAX_BODY_BYTES + 1


class InMemoryDriveStore:
    """In-memory ObjectStore that mirrors GoogleDriveObjectStore tag derivation,
    so a batcher write can be consumed by whatsapp_drive_ingest unchanged."""

    backend = "google_drive"

    def __init__(self, *, folder_id: str = "folder", source: str = "whatsapp") -> None:
        self._folder_id = folder_id
        self._source = source
        self._objects: dict[str, dict[str, object]] = {}
        self._counter = 0

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
        for fid, obj in self._objects.items():  # dedup by (content_sha256, kind)
            props = obj["app_properties"]
            if props.get("content_sha256") == content_sha256 and props.get("pdw_kind") == kind:
                return self._ref(fid)
        self._counter += 1
        fid = f"file-{self._counter}"
        props = {
            "pdw_source": self._source,
            "pdw_kind": kind or "",
            "pdw_root_folder_id": self._folder_id,
            "pdw_stage": object_stage(object_key),
            "content_sha256": content_sha256,
            **(app_properties or {}),
        }
        self._objects[fid] = {
            "bytes": path.read_bytes(),
            "key": object_key,
            "app_properties": props,
            "filename": object_key.rsplit("/", 1)[-1],
        }
        return self._ref(fid)

    def _ref(self, fid: str):
        obj = self._objects[fid]
        return {
            "storage_backend": self.backend,
            "storage_key": obj["key"],
            "storage_file_id": fid,
            "storage_url": "",
        }

    def _listing(self, fid: str) -> ObjectListing:
        obj = self._objects[fid]
        return ObjectListing(
            ref={"storage_backend": self.backend, "storage_key": "", "storage_file_id": fid, "storage_url": ""},
            app_properties=dict(obj["app_properties"]),
            filename=str(obj["filename"]),
        )

    def list_objects(self, *, kind, stage=None, properties=None):
        out = []
        for fid, obj in self._objects.items():
            props = obj["app_properties"]
            if props.get("pdw_kind") != kind:
                continue
            if stage and props.get("pdw_stage") != stage:
                continue
            out.append(self._listing(fid))
        return out

    def find_object(self, *, kind, stage=None, properties=None):
        matches = self.list_objects(kind=kind, stage=stage, properties=properties)
        return matches[0] if matches else None

    def get_object(self, ref):
        return self._objects[str(ref["storage_file_id"])]["bytes"]

    def move_object(self, ref, *, new_object_key, app_properties=None):
        fid = str(ref["storage_file_id"])
        obj = self._objects[fid]
        obj["key"] = new_object_key
        obj["filename"] = new_object_key.rsplit("/", 1)[-1]
        obj["app_properties"]["pdw_stage"] = object_stage(new_object_key)
        obj["app_properties"].update(app_properties or {})
        return self._ref(fid)


class RoundTripWarehouse:
    def __init__(self) -> None:
        self.chats: list[dict] = []
        self.messages: list[dict] = []
        self.media_items: list[dict] = []

    def ensure_whatsapp_tables(self) -> None:
        pass

    def insert_whatsapp_chats(self, rows) -> None:
        self.chats.extend(rows)

    def insert_whatsapp_chat_participants(self, rows) -> None:
        pass

    def insert_whatsapp_contacts(self, rows) -> None:
        pass

    def insert_whatsapp_messages(self, rows) -> None:
        self.messages.extend(rows)

    def insert_whatsapp_media_items(self, rows) -> None:
        self.media_items.extend(rows)

    def backfill_whatsapp_chats_from_messages(self) -> int:
        return 0


def test_batcher_output_is_consumed_by_drive_ingest_unchanged(tmp_path) -> None:
    """End-to-end: the client writes batch + media straight to the store, then
    whatsapp_drive_ingest reads and promotes them with NO reader changes."""
    state = WhatsAppUploadState.open(tmp_path / "state.sqlite", account="zach@example.com", store_path="session")
    store = InMemoryDriveStore()
    batcher = WhatsAppBatcher(
        account="zach@example.com",
        object_store=store,
        upload_state=state,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 13, tzinfo=UTC),
    )
    try:
        record = _image_media_record()
        batcher.add_message(record.payload)
        batcher.add_media(record.media, downloader=lambda _msg: b"media body")
        batcher.flush()
    finally:
        state.close()

    warehouse = RoundTripWarehouse()
    summary = WhatsAppDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: iter_batch_payloads(object_store=store),
        object_store=store,
        logger=FakeLogger(),
    ).sync()

    assert summary.batches_seen == 1
    assert summary.messages_written == 1
    assert summary.media_items_written == 1
    assert summary.files_promoted == 2  # batch + media promoted inbox -> library

    media_row = warehouse.media_items[0]
    assert media_row["content_sha256"] == hashlib_sha256(b"media body")
    assert media_row["is_missing"] == 0
    # Promotion relocates the blob from inbox to library; the row records the
    # library key the reader rewrote it to.
    assert media_row["storage_key"].startswith("whatsapp/library/media/")
    assert media_row["storage_backend"] == "google_drive"
    # Every promoted object now carries the library stage tag.
    assert all(obj["app_properties"]["pdw_stage"] == "library" for obj in store._objects.values())


def hashlib_sha256(content: bytes) -> str:
    import hashlib

    return hashlib.sha256(content).hexdigest()
