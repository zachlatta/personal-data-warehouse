from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import gzip
import json

from personal_data_warehouse.whatsapp_drive_ingest import (
    WhatsAppDriveIngestRunner,
    has_batch_payloads,
    iter_batch_payloads,
    library_batch_payload,
    record_to_contact_row,
    record_to_media_item_row,
    record_to_message_row,
)
from personal_data_warehouse.objectstore import ObjectListing


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeWarehouse:
    def __init__(self) -> None:
        self.ensure_called = False
        self.chats: list[dict[str, object]] = []
        self.contacts: list[dict[str, object]] = []
        self.messages: list[dict[str, object]] = []
        self.media_items: list[dict[str, object]] = []

    def ensure_whatsapp_tables(self) -> None:
        self.ensure_called = True

    def insert_whatsapp_chats(self, rows) -> None:
        self.chats.extend(rows)

    def insert_whatsapp_contacts(self, rows) -> None:
        self.contacts.extend(rows)

    def insert_whatsapp_messages(self, rows) -> None:
        self.messages.extend(rows)

    def insert_whatsapp_media_items(self, rows) -> None:
        self.media_items.extend(rows)


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, *, batch_listings=None, gz_by_id=None) -> None:
        self.batch_listings = list(batch_listings or [])
        self.gz_by_id = dict(gz_by_id or {})
        self.moved: list[tuple[str, str]] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        return list(self.batch_listings) if kind == "whatsapp_export_batch" else []

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == "whatsapp_export_batch" and self.batch_listings:
            return self.batch_listings[0]
        return None

    def get_object(self, ref):
        return self.gz_by_id[str(ref["storage_file_id"])]

    def move_object(self, ref, *, new_object_key, app_properties=None):
        self.moved.append((str(ref.get("storage_file_id", "")), new_object_key))
        return {
            "storage_backend": self.backend,
            "storage_key": new_object_key,
            "storage_file_id": str(ref.get("storage_file_id", "")),
            "storage_url": "https://drive/promoted",
        }


CHAT_JID = "15551234567@s.whatsapp.net"


def decorated_batch_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": "whatsapp",
        "batch_file": {
            "storage_backend": "google_drive",
            "storage_key": "whatsapp/inbox/batches/2026/05/batch.jsonl.gz",
            "storage_file_id": "batch-file",
            "storage_url": "https://drive/batch",
            "content_sha256": "batch-sha",
        },
        "records": [
            envelope(
                "chat",
                {
                    "chat_id": CHAT_JID,
                    "name": "Test Person",
                    "chat_type": "user",
                    "is_archived": False,
                    "last_message_at": "2026-05-21T12:00:00+00:00",
                    "raw": {},
                },
            ),
            envelope(
                "message",
                {
                    "chat_id": CHAT_JID,
                    "message_id": "stanza-1",
                    "sender_jid": CHAT_JID,
                    "push_name": "Tester",
                    "is_from_me": False,
                    "body_text": "Hello",
                    "message_kind": "text",
                    "media_type": "",
                    "quoted_message_id": "",
                    "message_at": "2026-05-21T12:00:00+00:00",
                    "edited_at": "1970-01-01T00:00:00+00:00",
                    "is_deleted": False,
                    "raw": {},
                },
            ),
            envelope("contact", {"jid": CHAT_JID, "push_name": "Tester", "first_name": "", "full_name": "", "business_name": "", "raw": {}}),
            envelope("media_item", media_item_record()),
            envelope("media_item", {**media_item_record(), "content_sha256": "media-sha", "is_missing": False, "file": media_item_file()}),
        ],
    }


def media_item_record() -> dict[str, object]:
    return {
        "chat_id": CHAT_JID,
        "message_id": "stanza-2",
        "media_type": "image",
        "filename": "photo.jpg",
        "mime_type": "image/jpeg",
        "total_bytes": 10,
        "size_bytes": 10,
        "file_sha256": "0102",
        "content_sha256": "",
        "is_missing": True,
        "error": "media not downloaded",
        "message_at": "2026-05-21T12:00:00+00:00",
        "raw": {},
    }


def media_item_file() -> dict[str, str]:
    return {
        "storage_backend": "google_drive",
        "storage_key": "whatsapp/inbox/media/2026/05/2026-05-21-chat-stanza-2-media-sha.jpg",
        "storage_file_id": "media-file",
        "storage_url": "https://drive/media",
    }


def envelope(record_type: str, record: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": "whatsapp",
        "account": "zach@example.com",
        "exported_at": "2026-05-21T13:00:00+00:00",
        "record_type": record_type,
        "record": record,
    }


def test_record_mappers_preserve_message_body_and_media_storage() -> None:
    ingested_at = datetime(2026, 5, 21, 14, tzinfo=UTC)
    batch = decorated_batch_payload()

    message_row = record_to_message_row(batch["records"][1], ingested_at=ingested_at)
    contact_row = record_to_contact_row(batch["records"][2], ingested_at=ingested_at)
    media_item_row = record_to_media_item_row(batch["records"][-1], ingested_at=ingested_at)

    assert message_row["chat_id"] == CHAT_JID
    assert message_row["message_id"] == "stanza-1"
    assert message_row["body_text"] == "Hello"
    assert message_row["message_kind"] == "text"
    assert contact_row["jid"] == CHAT_JID
    assert contact_row["push_name"] == "Tester"
    assert media_item_row["content_sha256"] == "media-sha"
    assert media_item_row["storage_file_id"] == "media-file"


def test_library_batch_payload_rewrites_batch_and_media_storage_keys() -> None:
    payload = library_batch_payload(decorated_batch_payload())

    assert payload["batch_file"]["storage_key"] == "whatsapp/library/batches/2026/05/batch.jsonl.gz"
    assert (
        payload["records"][-1]["record"]["file"]["storage_key"]
        == "whatsapp/library/media/2026/05/2026-05-21-chat-stanza-2-media-sha.jpg"
    )


def test_iter_batch_payloads_decompresses_via_object_store() -> None:
    records = [envelope("chat", {"chat_id": "c", "raw": {}}), envelope("message", {"message_id": "m", "raw": {}})]
    gz = gzip.compress("\n".join(json.dumps(record) for record in records).encode("utf-8"))
    listing = ObjectListing(
        ref={
            "storage_backend": "google_drive",
            "storage_key": "",
            "storage_file_id": "batch-file",
            "storage_url": "https://drive/batch",
        },
        app_properties={"content_sha256": "batch-sha", "exported_at": "2026-05-21T13:00:00+00:00"},
        filename="batch.jsonl.gz",
    )
    store = FakeObjectStore(batch_listings=[listing], gz_by_id={"batch-file": gz})

    payloads = list(iter_batch_payloads(object_store=store))

    assert len(payloads) == 1
    payload = payloads[0]
    assert payload["batch_file"]["storage_key"] == "whatsapp/inbox/batches/2026/05/batch.jsonl.gz"
    assert payload["batch_file"]["storage_file_id"] == "batch-file"
    assert payload["batch_file"]["content_sha256"] == "batch-sha"
    assert [record["record_type"] for record in payload["records"]] == ["chat", "message"]


def test_has_batch_payloads_uses_find_object() -> None:
    listing = ObjectListing(
        ref={"storage_backend": "google_drive", "storage_key": "", "storage_file_id": "batch-file", "storage_url": ""},
        app_properties={},
        filename="batch.jsonl.gz",
    )
    assert has_batch_payloads(object_store=FakeObjectStore(batch_listings=[listing])) is True
    assert has_batch_payloads(object_store=FakeObjectStore()) is False


def test_drive_ingest_runner_dedupes_media_updates_and_promotes() -> None:
    warehouse = FakeWarehouse()
    store = FakeObjectStore()

    summary = WhatsAppDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [decorated_batch_payload()],
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 14, tzinfo=UTC),
    ).sync()

    assert warehouse.ensure_called
    assert summary.batches_seen == 1
    assert summary.chats_written == 1
    assert summary.contacts_written == 1
    assert summary.messages_written == 1
    assert summary.media_items_written == 1
    assert summary.files_promoted == 2  # batch file + one media item with a stored file
    assert warehouse.messages[0]["body_text"] == "Hello"
    assert warehouse.media_items[0]["content_sha256"] == "media-sha"
    assert warehouse.media_items[0]["storage_key"].startswith("whatsapp/library/")
    assert ("batch-file", "whatsapp/library/batches/2026/05/batch.jsonl.gz") in store.moved
    assert (
        "media-file",
        "whatsapp/library/media/2026/05/2026-05-21-chat-stanza-2-media-sha.jpg",
    ) in store.moved


def test_drive_ingest_runner_preserves_media_storage_from_older_records() -> None:
    older = decorated_batch_payload()
    manifest = envelope("media_item", media_item_record())
    manifest["exported_at"] = "2026-05-21T13:05:00+00:00"
    newer = deepcopy(older)
    newer["records"] = [manifest]
    warehouse = FakeWarehouse()

    summary = WhatsAppDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [older, newer],
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 14, tzinfo=UTC),
    ).sync()

    assert summary.media_items_written == 1
    row = warehouse.media_items[0]
    assert row["content_sha256"] == "media-sha"
    assert row["storage_backend"] == "google_drive"
    assert row["storage_file_id"] == "media-file"


def test_drive_ingest_runner_preserves_contact_names_from_store_dump() -> None:
    older = decorated_batch_payload()
    older["records"] = [
        envelope(
            "contact",
            {"jid": CHAT_JID, "push_name": "Tester", "first_name": "Test", "full_name": "Test Person", "business_name": "", "raw": {}},
        )
    ]
    pushname_only = deepcopy(older)
    pushname_only["records"] = [
        envelope("contact", {"jid": CHAT_JID, "push_name": "Tester2", "first_name": "", "full_name": "", "business_name": "", "raw": {}})
    ]
    pushname_only["records"][0]["exported_at"] = "2026-05-21T13:05:00+00:00"
    warehouse = FakeWarehouse()

    WhatsAppDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [older, pushname_only],
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 14, tzinfo=UTC),
    ).sync()

    row = warehouse.contacts[0]
    assert row["push_name"] == "Tester2"
    assert row["full_name"] == "Test Person"


def test_drive_ingest_runner_prefers_latest_duplicate_message_record() -> None:
    older = decorated_batch_payload()
    newer = deepcopy(older)
    newer["records"][1]["exported_at"] = "2026-05-21T13:05:00+00:00"
    newer["records"][1]["record"]["body_text"] = "New body"
    warehouse = FakeWarehouse()

    summary = WhatsAppDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [newer, older],
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 14, tzinfo=UTC),
    ).sync()

    assert summary.messages_written == 1
    assert warehouse.messages[0]["body_text"] == "New body"
