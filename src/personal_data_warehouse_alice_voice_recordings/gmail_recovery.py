from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import hashlib
import json
import mimetypes
import re
import tempfile
from typing import Any

from personal_data_warehouse.clickhouse import ClickHouseWarehouse
from personal_data_warehouse.gmail_sync import attachment_content_bytes
from personal_data_warehouse_voice_memos.storage import ObjectStore, StoredObject

from .sync import OBJECT_PREFIX, SOURCE, extension_for_content_type, safe_object_key_part

EMAIL_BODY_KIND = "voice_recording_email"
EMAIL_ATTACHMENT_KIND = "voice_recording_email_attachment"
EMAIL_METADATA_KIND = "voice_recording_email_metadata"
TRANSCRIPT_KIND = "voice_recording_transcript"
AUDIO_KIND = "voice_recording_audio"


@dataclass(frozen=True)
class AliceGmailAttachment:
    account: str
    message_id: str
    part_id: str
    attachment_id: str
    filename: str
    mime_type: str
    size: int
    content_sha256: str
    part_json: str


@dataclass(frozen=True)
class AliceGmailTranscriptEmail:
    account: str
    message_id: str
    thread_id: str
    internal_date: datetime
    subject: str
    from_address: str
    to_addresses: tuple[str, ...]
    snippet: str
    body_markdown: str
    body_text: str
    body_html: str
    attachments: tuple[AliceGmailAttachment, ...]

    @property
    def recording_guid(self) -> str:
        return recording_guid_from_email(self)

    @property
    def title(self) -> str:
        return metadata_field(self.body_markdown, "Name") or self.subject


@dataclass(frozen=True)
class AliceGmailRecoverySummary:
    emails_seen: int
    emails_archived: int
    emails_skipped: int
    attachments_seen: int
    attachments_uploaded: int
    metadata_uploaded: int
    bytes_uploaded: int


class AliceGmailRecoveryRunner:
    def __init__(
        self,
        *,
        emails: Sequence[AliceGmailTranscriptEmail],
        object_store: ObjectStore,
        gmail_services_by_account: Mapping[str, object],
        logger,
        stage: str = "library",
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._emails = emails
        self._object_store = object_store
        self._gmail_services_by_account = gmail_services_by_account
        self._logger = logger
        self._stage = stage
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync(self) -> AliceGmailRecoverySummary:
        emails_archived = 0
        emails_skipped = 0
        attachments_seen = 0
        attachments_uploaded = 0
        metadata_uploaded = 0
        bytes_uploaded = 0

        with tempfile.TemporaryDirectory(prefix="pdw-alice-gmail-recovery-") as temp_dir:
            temp_path = Path(temp_dir)
            for email in self._emails:
                if not email.recording_guid:
                    self._logger.warning(
                        "skip Alice Gmail transcript email %s: no recording URL found",
                        email.message_id,
                    )
                    emails_skipped += 1
                    continue

                uploaded_for_email = 0
                email_prefix = gmail_recording_object_prefix(email=email, stage=self._stage)
                email_body = email_body_markdown(email)
                email_body_sha = hashlib.sha256(email_body.encode("utf-8")).hexdigest()

                if not self._object_store.has_object(
                    kind=EMAIL_BODY_KIND,
                    key="gmail_message_id",
                    value=email.message_id,
                ):
                    body_path = temp_path / f"{email.message_id}.md"
                    body_path.write_text(email_body, encoding="utf-8")
                    self._logger.info("archive Alice Gmail email body %s", email.message_id)
                    self._object_store.put_file(
                        path=body_path,
                        object_key=f"{email_prefix}.email.md",
                        content_sha256=email_body_sha,
                        content_type="text/markdown",
                        kind=EMAIL_BODY_KIND,
                        app_properties=email_app_properties(email),
                    )
                    uploaded_for_email += 1
                    bytes_uploaded += len(email_body.encode("utf-8"))

                attachment_storage: list[dict[str, object]] = []
                for attachment in email.attachments:
                    if not attachment.filename:
                        continue
                    attachments_seen += 1
                    if attachment.content_sha256 and self._object_store.has_object(
                        kind=attachment_kind(attachment),
                        key="content_sha256",
                        value=attachment.content_sha256,
                    ):
                        continue

                    service = self._gmail_services_by_account.get(attachment.account)
                    if service is None:
                        raise RuntimeError(f"No Gmail service configured for {attachment.account}")
                    content = fetch_gmail_attachment_content(service=service, attachment=attachment)
                    content_sha256 = hashlib.sha256(content).hexdigest()
                    extension = attachment_extension(attachment)
                    artifact = attachment_artifact_name(attachment)
                    attachment_path = temp_path / f"{email.message_id}-{attachment.part_id.replace('.', '-')}{extension}"
                    attachment_path.write_bytes(content)
                    stored = self._object_store.put_file(
                        path=attachment_path,
                        object_key=f"{email_prefix}.{artifact}{extension}",
                        content_sha256=content_sha256,
                        content_type=attachment.mime_type or "application/octet-stream",
                        kind=attachment_kind(attachment),
                        app_properties={
                            **email_app_properties(email),
                            "gmail_part_id": attachment.part_id,
                            "gmail_attachment_id_sha256": hashlib.sha256(
                                attachment.attachment_id.encode("utf-8")
                            ).hexdigest(),
                            "alice_artifact": artifact,
                        },
                    )
                    attachment_storage.append(
                        {
                            "filename": attachment.filename,
                            "mime_type": attachment.mime_type,
                            "size": len(content),
                            "content_sha256": content_sha256,
                            "kind": attachment_kind(attachment),
                            "storage": stored,
                        }
                    )
                    uploaded_for_email += 1
                    attachments_uploaded += 1
                    bytes_uploaded += len(content)

                metadata = build_gmail_recovery_metadata(
                    email=email,
                    uploaded_at=self._now(),
                    attachment_storage=attachment_storage,
                    email_body_content_sha256=email_body_sha,
                )
                metadata_sha = hashlib.sha256(json.dumps(metadata, sort_keys=True).encode("utf-8")).hexdigest()
                if not self._object_store.has_object(
                    kind=EMAIL_METADATA_KIND,
                    key="gmail_message_id",
                    value=email.message_id,
                ):
                    self._object_store.put_json(
                        object_key=f"{email_prefix}.email.json",
                        payload=metadata,
                        content_sha256=metadata_sha,
                        kind=EMAIL_METADATA_KIND,
                        app_properties=email_app_properties(email),
                    )
                    metadata_uploaded += 1
                    uploaded_for_email += 1

                if uploaded_for_email:
                    emails_archived += 1
                else:
                    emails_skipped += 1

        return AliceGmailRecoverySummary(
            emails_seen=len(self._emails),
            emails_archived=emails_archived,
            emails_skipped=emails_skipped,
            attachments_seen=attachments_seen,
            attachments_uploaded=attachments_uploaded,
            metadata_uploaded=metadata_uploaded,
            bytes_uploaded=bytes_uploaded,
        )


def load_alice_gmail_transcript_emails(
    *,
    warehouse: ClickHouseWarehouse,
    accounts: Sequence[str],
) -> list[AliceGmailTranscriptEmail]:
    if not accounts:
        return []
    account_values = ", ".join(sql_string(account) for account in accounts)
    message_rows = warehouse._query(
        f"""
        SELECT
            account,
            message_id,
            thread_id,
            internal_date,
            subject,
            from_address,
            to_addresses,
            snippet,
            body_markdown_clean,
            body_text,
            body_html
        FROM gmail_messages FINAL
        WHERE is_deleted = 0
          AND account IN ({account_values})
          AND from_address = 'alice@aliceapp.ai'
          AND (
              positionCaseInsensitive(snippet, 'recording and transcript') > 0
              OR positionCaseInsensitive(body_text, 'recording and transcript') > 0
              OR positionCaseInsensitive(body_html, 'recording and transcript') > 0
              OR positionCaseInsensitive(body_text, 'Head to the recording page') > 0
              OR positionCaseInsensitive(body_html, 'Head to the recording page') > 0
          )
        ORDER BY internal_date
        """
    )
    message_ids_by_account: dict[str, list[str]] = {}
    for row in message_rows:
        message_ids_by_account.setdefault(str(row[0]), []).append(str(row[1]))

    attachments_by_key = load_alice_gmail_attachments(
        warehouse=warehouse,
        message_ids_by_account=message_ids_by_account,
    )
    emails: list[AliceGmailTranscriptEmail] = []
    for row in message_rows:
        account = str(row[0])
        message_id = str(row[1])
        emails.append(
            AliceGmailTranscriptEmail(
                account=account,
                message_id=message_id,
                thread_id=str(row[2]),
                internal_date=ensure_utc(row[3]),
                subject=str(row[4]),
                from_address=str(row[5]),
                to_addresses=tuple(str(value) for value in row[6]),
                snippet=str(row[7]),
                body_markdown=str(row[8]),
                body_text=str(row[9]),
                body_html=str(row[10]),
                attachments=tuple(attachments_by_key.get((account, message_id), ())),
            )
        )
    return emails


def load_alice_gmail_attachments(
    *,
    warehouse: ClickHouseWarehouse,
    message_ids_by_account: Mapping[str, Sequence[str]],
) -> dict[tuple[str, str], list[AliceGmailAttachment]]:
    clauses = []
    for account, message_ids in message_ids_by_account.items():
        if not message_ids:
            continue
        ids = ", ".join(sql_string(message_id) for message_id in message_ids)
        clauses.append(f"(account = {sql_string(account)} AND message_id IN ({ids}))")
    if not clauses:
        return {}
    rows = warehouse._query(
        f"""
        SELECT
            account,
            message_id,
            part_id,
            attachment_id,
            filename,
            mime_type,
            size,
            content_sha256,
            part_json
        FROM gmail_attachments FINAL
        WHERE is_deleted = 0
          AND ({' OR '.join(clauses)})
        ORDER BY internal_date, part_id, filename
        """
    )
    attachments_by_key: dict[tuple[str, str], list[AliceGmailAttachment]] = {}
    for row in rows:
        attachment = AliceGmailAttachment(
            account=str(row[0]),
            message_id=str(row[1]),
            part_id=str(row[2]),
            attachment_id=str(row[3]),
            filename=str(row[4]),
            mime_type=str(row[5]),
            size=int(row[6]),
            content_sha256=str(row[7]),
            part_json=str(row[8]),
        )
        attachments_by_key.setdefault((attachment.account, attachment.message_id), []).append(attachment)
    return attachments_by_key


def fetch_gmail_attachment_content(*, service, attachment: AliceGmailAttachment) -> bytes:
    part = json.loads(attachment.part_json)
    if not isinstance(part, Mapping):
        raise ValueError(f"Gmail attachment part_json is not an object for {attachment.message_id}/{attachment.part_id}")
    return attachment_content_bytes(service=service, message_id=attachment.message_id, part=part)


def build_gmail_recovery_metadata(
    *,
    email: AliceGmailTranscriptEmail,
    uploaded_at: datetime,
    attachment_storage: Sequence[Mapping[str, object]],
    email_body_content_sha256: str,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": SOURCE,
        "recovery_source": "gmail_alice_transcript_email",
        "account": email.account,
        "uploaded_at": uploaded_at.astimezone(UTC).isoformat(),
        "recording": {
            "recording_id": email.recording_guid,
            "title": email.title,
            "recorded_at": email.internal_date.astimezone(UTC).isoformat(),
            "duration": metadata_field(email.body_markdown, "Duration"),
            "words": metadata_field(email.body_markdown, "Words"),
            "size": metadata_field(email.body_markdown, "Size"),
            "recording_page_url": recording_page_url(email),
        },
        "gmail": {
            "account": email.account,
            "message_id": email.message_id,
            "thread_id": email.thread_id,
            "from_address": email.from_address,
            "to_addresses": list(email.to_addresses),
            "subject": email.subject,
            "internal_date": email.internal_date.astimezone(UTC).isoformat(),
            "snippet": email.snippet,
            "email_body_content_sha256": email_body_content_sha256,
            "attachments": list(attachment_storage),
        },
    }


def gmail_recording_object_prefix(*, email: AliceGmailTranscriptEmail, stage: str) -> str:
    recorded_at = email.internal_date.astimezone(UTC)
    date_prefix = recorded_at.strftime("%Y/%m/%Y-%m-%d")
    slug = safe_object_key_part(email.title)[:80]
    return f"{OBJECT_PREFIX}/{stage}/{date_prefix}-alice-{email.recording_guid}-{slug}"


def email_body_markdown(email: AliceGmailTranscriptEmail) -> str:
    return email.body_markdown or email.body_text or email.snippet


def email_app_properties(email: AliceGmailTranscriptEmail) -> dict[str, str]:
    return {
        "alice_recording_guid": email.recording_guid,
        "gmail_account": email.account,
        "gmail_message_id": email.message_id,
    }


def recording_guid_from_email(email: AliceGmailTranscriptEmail) -> str:
    return recording_guid_from_text("\n".join([email.body_markdown, email.body_text, email.body_html, email.snippet]))


def recording_guid_from_text(text: str) -> str:
    match = re.search(r"https?://aliceapp\.ai/recordings/([A-Za-z0-9_-]+)", text)
    return match.group(1) if match else ""


def recording_page_url(email: AliceGmailTranscriptEmail) -> str:
    return f"https://aliceapp.ai/recordings/{email.recording_guid}" if email.recording_guid else ""


def metadata_field(markdown: str, name: str) -> str:
    pattern = rf"\*\*{re.escape(name)}:\*\*\s*([^\n]+)"
    match = re.search(pattern, markdown)
    return match.group(1).strip() if match else ""


def attachment_kind(attachment: AliceGmailAttachment) -> str:
    mime_type = attachment.mime_type.lower()
    filename = attachment.filename.lower()
    if mime_type.startswith("audio/") or filename.endswith((".mp3", ".m4a", ".wav", ".aac")):
        return AUDIO_KIND
    if filename.endswith((".txt", ".docx", ".pdf", ".rtf")):
        return TRANSCRIPT_KIND
    return EMAIL_ATTACHMENT_KIND


def attachment_artifact_name(attachment: AliceGmailAttachment) -> str:
    kind = attachment_kind(attachment)
    if kind == AUDIO_KIND:
        return "audio"
    if kind == TRANSCRIPT_KIND and attachment.filename.lower().endswith(".txt"):
        return "transcript"
    if kind == TRANSCRIPT_KIND:
        return "transcript-formatted"
        return safe_object_key_part(Path(attachment.filename).stem) or "attachment"


def attachment_extension(attachment: AliceGmailAttachment) -> str:
    suffix = Path(attachment.filename).suffix
    if suffix:
        return suffix
    guessed = extension_for_content_type(attachment.mime_type)
    if guessed:
        return guessed
    return mimetypes.guess_extension(attachment.mime_type) or ".bin"


def ensure_utc(value: object) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(str(value)).astimezone(UTC)


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
