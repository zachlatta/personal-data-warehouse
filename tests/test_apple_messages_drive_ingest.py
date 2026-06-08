from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import gzip
import json

from personal_data_warehouse.apple_messages_drive_ingest import (
    AppleMessagesDriveIngestRunner,
    has_batch_payloads,
    iter_batch_payloads,
    library_batch_payload,
    record_to_attachment_row,
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
        self.handles: list[dict[str, object]] = []
        self.chats: list[dict[str, object]] = []
        self.chat_handles: list[dict[str, object]] = []
        self.messages: list[dict[str, object]] = []
        self.chat_messages: list[dict[str, object]] = []
        self.attachments: list[dict[str, object]] = []

    def ensure_apple_messages_tables(self) -> None:
        self.ensure_called = True

    def insert_apple_message_handles(self, rows) -> None:
        self.handles.extend(rows)

    def insert_apple_message_chats(self, rows) -> None:
        self.chats.extend(rows)

    def insert_apple_message_chat_handles(self, rows) -> None:
        self.chat_handles.extend(rows)

    def insert_apple_messages(self, rows) -> None:
        self.messages.extend(rows)

    def insert_apple_message_chat_messages(self, rows) -> None:
        self.chat_messages.extend(rows)

    def insert_apple_message_attachments(self, rows) -> None:
        self.attachments.extend(rows)


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, *, batch_listings=None, gz_by_id=None) -> None:
        self.batch_listings = list(batch_listings or [])
        self.gz_by_id = dict(gz_by_id or {})
        self.moved: list[tuple[str, str]] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        return list(self.batch_listings) if kind == "apple_message_export_batch" else []

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == "apple_message_export_batch" and self.batch_listings:
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


def decorated_batch_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": "apple_messages",
        "batch_file": {
            "storage_backend": "google_drive",
            "storage_key": "apple-messages/inbox/batches/2026/05/batch.jsonl.gz",
            "storage_file_id": "batch-file",
            "storage_url": "https://drive/batch",
            "content_sha256": "batch-sha",
        },
        "records": [
            envelope(
                "handle",
                {
                    "handle_id": "1",
                    "handle_rowid": 1,
                    "address": "+15551234567",
                    "country": "US",
                    "service": "iMessage",
                    "uncanonicalized_id": "+15551234567",
                    "person_centric_id": "person-1",
                    "raw": {},
                },
            ),
            envelope(
                "message",
                {
                    "message_id": "message-guid",
                    "message_rowid": 1,
                    "handle_id": "1",
                    "service": "iMessage",
                    "message_account": "zach@example.com",
                    "body_text": "Hello",
                    "body_source": "text",
                    "body_decode_status": "ok",
                    "message_at": "2026-05-21T12:00:00+00:00",
                    "is_deleted": False,
                    "raw": {},
                },
            ),
            envelope("attachment", attachment_record()),
            envelope("attachment", {**attachment_record(), "content_sha256": "att-sha", "file": attachment_file()}),
        ],
    }


def attachment_record() -> dict[str, object]:
    return {
        "attachment_id": "attachment-guid",
        "attachment_rowid": 1,
        "message_id": "message-guid",
        "guid": "attachment-guid",
        "filename": "Attachments/photo.txt",
        "transfer_name": "photo.txt",
        "content_type": "text/plain",
        "uti": "public.plain-text",
        "mime_type": "text/plain",
        "total_bytes": 15,
        "size_bytes": 15,
        "content_sha256": "",
        "is_missing": False,
        "error": "",
        "created_at": "2026-05-21T12:00:00+00:00",
        "start_at": "2026-05-21T12:00:00+00:00",
        "raw": {},
    }


def attachment_file() -> dict[str, str]:
    return {
        "storage_backend": "google_drive",
        "storage_key": "apple-messages/inbox/attachments/2026/05/2026-05-21-attachment-guid-att-sha.txt",
        "storage_file_id": "attachment-file",
        "storage_url": "https://drive/attachment",
    }


def envelope(record_type: str, record: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": "apple_messages",
        "account": "zach@example.com",
        "exported_at": "2026-05-21T13:00:00+00:00",
        "record_type": record_type,
        "record": record,
    }


def test_record_mappers_preserve_message_body_and_attachment_storage() -> None:
    ingested_at = datetime(2026, 5, 21, 14, tzinfo=UTC)
    batch = decorated_batch_payload()

    message_row = record_to_message_row(batch["records"][1], ingested_at=ingested_at)
    attachment_row = record_to_attachment_row(batch["records"][-1], ingested_at=ingested_at)

    assert message_row["message_id"] == "message-guid"
    assert message_row["body_text"] == "Hello"
    assert message_row["service"] == "iMessage"
    assert attachment_row["content_sha256"] == "att-sha"
    assert attachment_row["storage_file_id"] == "attachment-file"


def test_library_batch_payload_rewrites_batch_and_attachment_storage_keys() -> None:
    payload = library_batch_payload(decorated_batch_payload())

    assert payload["batch_file"]["storage_key"] == "apple-messages/library/batches/2026/05/batch.jsonl.gz"
    assert (
        payload["records"][-1]["record"]["file"]["storage_key"]
        == "apple-messages/library/attachments/2026/05/2026-05-21-attachment-guid-att-sha.txt"
    )


def test_iter_batch_payloads_decompresses_via_object_store() -> None:
    records = [envelope("handle", {"handle_id": "1", "raw": {}}), envelope("message", {"message_id": "m", "raw": {}})]
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
    assert payload["batch_file"]["storage_key"] == "apple-messages/inbox/batches/2026/05/batch.jsonl.gz"
    assert payload["batch_file"]["storage_file_id"] == "batch-file"
    assert payload["batch_file"]["content_sha256"] == "batch-sha"
    assert [record["record_type"] for record in payload["records"]] == ["handle", "message"]


def test_has_batch_payloads_uses_find_object() -> None:
    listing = ObjectListing(
        ref={"storage_backend": "google_drive", "storage_key": "", "storage_file_id": "batch-file", "storage_url": ""},
        app_properties={},
        filename="batch.jsonl.gz",
    )
    assert has_batch_payloads(object_store=FakeObjectStore(batch_listings=[listing])) is True
    assert has_batch_payloads(object_store=FakeObjectStore()) is False


def test_drive_ingest_runner_dedupes_attachment_updates_and_promotes() -> None:
    warehouse = FakeWarehouse()
    store = FakeObjectStore()

    summary = AppleMessagesDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [decorated_batch_payload()],
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 14, tzinfo=UTC),
    ).sync()

    assert warehouse.ensure_called
    assert summary.batches_seen == 1
    assert summary.messages_written == 1
    assert summary.attachments_written == 1
    assert summary.files_promoted == 2  # batch file + one attachment with a stored file
    assert warehouse.messages[0]["body_text"] == "Hello"
    assert warehouse.attachments[0]["content_sha256"] == "att-sha"
    assert warehouse.attachments[0]["storage_key"].startswith("apple-messages/library/")
    assert ("batch-file", "apple-messages/library/batches/2026/05/batch.jsonl.gz") in store.moved
    assert (
        "attachment-file",
        "apple-messages/library/attachments/2026/05/2026-05-21-attachment-guid-att-sha.txt",
    ) in store.moved


def test_drive_ingest_runner_prefers_latest_duplicate_message_record() -> None:
    older = decorated_batch_payload()
    newer = deepcopy(older)
    newer["records"][1]["exported_at"] = "2026-05-21T13:05:00+00:00"
    newer["records"][1]["record"]["body_text"] = "New body"
    warehouse = FakeWarehouse()

    summary = AppleMessagesDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [newer, older],
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 14, tzinfo=UTC),
    ).sync()

    assert summary.messages_written == 1
    assert warehouse.messages[0]["body_text"] == "New body"
