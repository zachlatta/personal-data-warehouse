from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json

from personal_data_warehouse_alice_voice_recordings.gmail_recovery import (
    AUDIO_KIND,
    EMAIL_BODY_KIND,
    EMAIL_METADATA_KIND,
    TRANSCRIPT_KIND,
    AliceGmailAttachment,
    AliceGmailRecoveryRunner,
    AliceGmailTranscriptEmail,
    metadata_field,
    recording_guid_from_text,
)
from personal_data_warehouse.objectstore import ObjectPresence


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self) -> None:
        self.files: list[dict[str, object]] = []
        self.json_files: list[dict[str, object]] = []
        self.existing_objects: set[tuple[str, str, str]] = set()

    def has_blob(self, *, content_sha256: str) -> bool:
        return False

    def has_metadata(self, *, content_sha256: str) -> bool:
        return False

    def has_object(self, *, kind: str, key: str, value: str) -> bool:
        return (kind, key, value) in self.existing_objects

    def presence(self, *, content_sha256: str) -> ObjectPresence:
        return ObjectPresence(audio_exists=False, metadata_exists=False)

    def put_file(self, **kwargs):
        self.files.append(kwargs)
        return {
            "storage_backend": "google_drive",
            "storage_key": kwargs["object_key"],
            "storage_file_id": f"file-{len(self.files)}",
            "storage_url": f"https://drive/file-{len(self.files)}",
        }

    def put_json(self, **kwargs):
        self.json_files.append(kwargs)
        return {
            "storage_backend": "google_drive",
            "storage_key": kwargs["object_key"],
            "storage_file_id": f"json-{len(self.json_files)}",
            "storage_url": f"https://drive/json-{len(self.json_files)}",
        }


class FakeAttachments:
    def get(self, **kwargs):
        return FakeExecute({"data": "YXVkaW8="})


class FakeMessages:
    def attachments(self):
        return FakeAttachments()


class FakeUsers:
    def messages(self):
        return FakeMessages()


class FakeGmailService:
    def users(self):
        return FakeUsers()


class FakeExecute:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def execute(self):
        return self.payload


def transcript_email() -> AliceGmailTranscriptEmail:
    body = """Hi, here's your recording and transcript.
[Browse](https://aliceapp.ai/recordings/alice-guid-1)

**Name:** Lost Recording
**Duration:** 00:01:02
**Words:** 123
**Size:** 1 MB
"""
    return AliceGmailTranscriptEmail(
        account="zach@example.com",
        message_id="gmail-message-1",
        thread_id="thread-1",
        internal_date=datetime(2024, 12, 7, 17, tzinfo=UTC),
        subject="Lost Recording",
        from_address="alice@aliceapp.ai",
        to_addresses=("zach@example.com",),
        snippet="Hi, here's your recording and transcript.",
        body_markdown=body,
        body_text="",
        body_html="",
        attachments=(
            AliceGmailAttachment(
                account="zach@example.com",
                message_id="gmail-message-1",
                part_id="1",
                attachment_id="attachment-1",
                filename="Lost Recording.txt",
                mime_type="text/plain",
                size=10,
                content_sha256="",
                part_json=json.dumps({"body": {"data": "dHJhbnNjcmlwdA=="}, "filename": "Lost Recording.txt"}),
            ),
            AliceGmailAttachment(
                account="zach@example.com",
                message_id="gmail-message-1",
                part_id="2",
                attachment_id="attachment-2",
                filename="Lost Recording.m4a",
                mime_type="audio/mp4",
                size=10,
                content_sha256="",
                part_json=json.dumps({"body": {"attachmentId": "attachment-2"}, "filename": "Lost Recording.m4a"}),
            ),
        ),
    )


def test_alice_gmail_recovery_archives_email_body_attachments_and_metadata() -> None:
    store = FakeObjectStore()

    summary = AliceGmailRecoveryRunner(
        emails=[transcript_email()],
        object_store=store,
        gmail_services_by_account={"zach@example.com": FakeGmailService()},
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 12, 3, tzinfo=UTC),
    ).sync()

    assert summary.emails_seen == 1
    assert summary.emails_archived == 1
    assert summary.attachments_uploaded == 2
    assert {file["kind"] for file in store.files} == {EMAIL_BODY_KIND, TRANSCRIPT_KIND, AUDIO_KIND}
    assert store.json_files[0]["kind"] == EMAIL_METADATA_KIND
    assert all(str(file["object_key"]).startswith("alice-app-voice-recordings/library/2024/12/") for file in store.files)
    metadata = store.json_files[0]["payload"]
    assert metadata["recording"]["recording_id"] == "alice-guid-1"
    assert metadata["recording"]["title"] == "Lost Recording"
    assert metadata["recording"]["duration"] == "00:01:02"


def test_alice_gmail_recovery_skips_existing_email_body_and_metadata() -> None:
    store = FakeObjectStore()
    store.existing_objects.add((EMAIL_BODY_KIND, "gmail_message_id", "gmail-message-1"))
    store.existing_objects.add((EMAIL_METADATA_KIND, "gmail_message_id", "gmail-message-1"))
    content_sha = hashlib.sha256(b"transcript").hexdigest()
    email = transcript_email()
    existing_attachment = email.attachments[0]
    email = AliceGmailTranscriptEmail(
        **{
            **email.__dict__,
            "attachments": (
                AliceGmailAttachment(
                    **{
                        **existing_attachment.__dict__,
                        "content_sha256": content_sha,
                    }
                ),
            ),
        }
    )
    store.existing_objects.add((TRANSCRIPT_KIND, "content_sha256", content_sha))

    summary = AliceGmailRecoveryRunner(
        emails=[email],
        object_store=store,
        gmail_services_by_account={"zach@example.com": FakeGmailService()},
        logger=FakeLogger(),
    ).sync()

    assert summary.emails_skipped == 1
    assert store.files == []
    assert store.json_files == []


def test_alice_gmail_metadata_helpers_parse_recording_fields() -> None:
    text = "**Name:** Hello\n**Duration:** 00:00:03\nhttps://aliceapp.ai/recordings/abc_123-X"

    assert recording_guid_from_text(text) == "abc_123-X"
    assert metadata_field(text, "Name") == "Hello"
    assert metadata_field(text, "Duration") == "00:00:03"
