from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime

from personal_data_warehouse.apple_messages_drive_ingest import (
    AppleMessagesDriveIngestRunner,
    drive_batch_files_query,
    library_batch_payload,
    record_to_attachment_row,
    record_to_message_row,
)


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


class FakePromoter:
    def __init__(self) -> None:
        self.payloads: list[dict[str, object]] = []

    def promote(self, payload) -> int:
        self.payloads.append(payload)
        return 2


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
                "chat",
                {
                    "chat_id": "chat-guid",
                    "chat_rowid": 1,
                    "guid": "chat-guid",
                    "chat_identifier": "+15551234567",
                    "service_name": "iMessage",
                    "display_name": "Test Chat",
                    "room_name": "",
                    "account_login": "zach@example.com",
                    "style": 45,
                    "state": 0,
                    "is_archived": False,
                    "is_filtered": False,
                    "is_recovered": False,
                    "is_pending_review": False,
                    "last_read_message_at": "2026-05-21T12:00:00+00:00",
                    "raw": {},
                },
            ),
            envelope("chat_handle", {"chat_id": "chat-guid", "handle_id": "1", "raw": {}}),
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
                    "body_decode_error": "",
                    "attributed_body_sha256": "",
                    "subject": "",
                    "country": "US",
                    "message_type": 0,
                    "item_type": 0,
                    "is_from_me": False,
                    "is_read": True,
                    "is_sent": True,
                    "is_delivered": True,
                    "is_finished": True,
                    "is_system_message": False,
                    "is_service_message": False,
                    "is_forward": False,
                    "is_empty": False,
                    "is_audio_message": False,
                    "is_played": False,
                    "cache_has_attachments": True,
                    "has_unseen_mention": False,
                    "is_spam": False,
                    "reply_to_guid": "",
                    "associated_message_guid": "",
                    "associated_message_type": 0,
                    "associated_message_emoji": "",
                    "balloon_bundle_id": "",
                    "group_title": "",
                    "group_action_type": 0,
                    "message_action_type": 0,
                    "message_source": 0,
                    "expressive_send_style_id": "",
                    "message_at": "2026-05-21T12:00:00+00:00",
                    "date_ns": 801748800000000000,
                    "date_read": "1970-01-01T00:00:00+00:00",
                    "date_delivered": "1970-01-01T00:00:00+00:00",
                    "date_played": "1970-01-01T00:00:00+00:00",
                    "date_edited": "1970-01-01T00:00:00+00:00",
                    "date_retracted": "1970-01-01T00:00:00+00:00",
                    "date_recovered": "1970-01-01T00:00:00+00:00",
                    "is_deleted": False,
                    "raw": {},
                },
            ),
            envelope(
                "chat_message",
                {
                    "chat_id": "chat-guid",
                    "message_id": "message-guid",
                    "message_date": "2026-05-21T12:00:00+00:00",
                    "message_date_ns": 801748800000000000,
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
        "original_guid": "",
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
        "is_outgoing": False,
        "is_sticker": False,
        "hide_attachment": False,
        "transfer_state": 5,
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

    message_row = record_to_message_row(batch["records"][3], ingested_at=ingested_at)
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


def test_drive_ingest_runner_dedupes_attachment_updates_and_promotes() -> None:
    warehouse = FakeWarehouse()
    promoter = FakePromoter()

    summary = AppleMessagesDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [decorated_batch_payload()],
        promoter=promoter,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 14, tzinfo=UTC),
    ).sync()

    assert warehouse.ensure_called
    assert summary.batches_seen == 1
    assert summary.messages_written == 1
    assert summary.attachments_written == 1
    assert summary.files_promoted == 2
    assert warehouse.messages[0]["body_text"] == "Hello"
    assert warehouse.attachments[0]["content_sha256"] == "att-sha"
    assert warehouse.attachments[0]["storage_key"].startswith("apple-messages/library/")
    assert promoter.payloads == [decorated_batch_payload()]


def test_drive_ingest_runner_prefers_latest_duplicate_message_record() -> None:
    older = decorated_batch_payload()
    newer = deepcopy(older)
    newer["records"][3]["exported_at"] = "2026-05-21T13:05:00+00:00"
    newer["records"][3]["record"]["body_text"] = "New body"
    warehouse = FakeWarehouse()

    summary = AppleMessagesDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: [newer, older],
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 14, tzinfo=UTC),
    ).sync()

    assert summary.messages_written == 1
    assert warehouse.messages[0]["body_text"] == "New body"


def test_drive_batch_query_targets_apple_messages_inbox() -> None:
    query = drive_batch_files_query(folder_id="root-folder", stage="inbox")

    assert "pdw_source' and value='apple_messages" in query
    assert "pdw_kind' and value='apple_message_export_batch" in query
    assert "pdw_stage' and value='inbox" in query
    assert "pdw_root_folder_id" in query
