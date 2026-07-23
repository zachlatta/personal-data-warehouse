"""Cross-boundary ingest e2e driver: posts every endpoint via the real
IngestClient. Invoked by TestIngestEndToEndPythonClient (PDW_E2E=1) with the live
handler URL and signing secret as argv. Not a standalone tool."""

from __future__ import annotations

import hashlib
import sys
import tempfile
from pathlib import Path

from personal_data_warehouse.ingest_client import IngestClient
from personal_data_warehouse_manual_finance.envelope import (
    provenance_dedup_sha256 as finance_provenance_dedup_sha256,
)
from personal_data_warehouse_photos.envelope import provenance_dedup_sha256


def main() -> int:
    base_url, secret = sys.argv[1], sys.argv[2]
    client = IngestClient(base_url=base_url, signing_key=secret.encode("utf-8"))

    def check(name: str, stored: dict) -> None:
        assert stored.get("storage_file_id"), f"{name}: missing storage_file_id"
        print(f"OK  {name} -> {stored['storage_key']}")

    audio_sha = hashlib.sha256(b"audio-bytes").hexdigest()
    photo_sha = hashlib.sha256(b"heic-bytes").hexdigest()
    photo_dedup_sha = provenance_dedup_sha256(
        source="apple_photos", account="z@x.test", native_id="UUID-1", role="original",
        file_content_sha256=photo_sha,
    )
    statement = b"%PDF-statement-bytes"
    statement_sha = hashlib.sha256(statement).hexdigest()
    statement_dedup_sha = finance_provenance_dedup_sha256(
        source="manual",
        account="z@x.test",
        native_id=statement_sha,
        file_content_sha256=statement_sha,
    )

    check("agent-sessions/batch", client.upload_agent_sessions_batch(b"gzipped-batch", exported_at="2026-06-19T12:34:56+00:00"))
    check("apple-messages/batch", client.upload_apple_messages_batch(b"gzipped-batch", exported_at="2026-06-19T12:34:56+00:00"))
    check("apple-messages/attachment", client.upload_apple_messages_attachment(
        b"attachment-bytes", attachment_guid="A1", message_guid="M1",
        content_type="image/jpeg", created_at="2025-03-04T00:00:00+00:00", filename="p.jpg"))
    with tempfile.TemporaryDirectory() as temp_dir:
        photo_path = Path(temp_dir) / "photo.heic"
        photo_path.write_bytes(b"heic-bytes")
        check("photos/file", client.upload_photo_file_path(
            photo_path, captured_at="2026-06-01T14:30:00", extension=".heic",
            content_type="image/heic", content_sha256=photo_sha))
    check("photos/metadata", client.upload_photo_metadata(
        {"schema_version": 1, "source": "apple_photos"}, captured_at="2026-06-01T14:30:00",
        file_content_sha256=photo_sha, metadata_dedup_sha256=photo_dedup_sha))
    check("voice-memos/audio", client.upload_voice_memo_audio(
        b"audio-bytes", recorded_at="2025-07-15T09:00:00", extension=".m4a", content_type="audio/m4a"))
    check("voice-memos/metadata", client.upload_voice_memo_metadata(
        {"schema_version": 1}, recorded_at="2025-07-15T09:00:00", audio_content_sha256=audio_sha))
    check("apple-notes/body", client.upload_apple_notes_body(
        b"<html>x</html>", note_id="N1", revision_id="R1", modified_at="2026-01-02T03:04:05+00:00"))
    check("apple-notes/attachment", client.upload_apple_notes_attachment(
        b"note-attachment", note_id="N1", revision_id="R1", modified_at="2026-01-02T03:04:05+00:00",
        attachment_id="AT1", filename="d.pdf", content_type="application/pdf"))
    check("apple-notes/revision", client.upload_apple_notes_revision(
        {"schema_version": 1, "source": "apple_notes"}, note_id="N1", revision_id="R1",
        modified_at="2026-01-02T03:04:05+00:00", note_content_sha256="FP1"))
    check("manual-finance/file", client.upload_manual_finance_document(
        statement, modified_at="2026-06-30T10:00:00", account_folder="checking",
        extension=".pdf", content_type="application/pdf"))
    check("manual-finance/metadata", client.upload_manual_finance_metadata(
        {"schema_version": 1, "source": "manual"},
        modified_at="2026-06-30T10:00:00", account_folder="checking",
        file_content_sha256=statement_sha, metadata_dedup_sha256=statement_dedup_sha))

    print("ALL PYTHON CLIENT PATHS OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
