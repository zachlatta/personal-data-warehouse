from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import json

from personal_data_warehouse.apple_notes_drive_ingest import (
    AppleNotesDriveIngestRunner,
    clean_metadata_payload,
    drive_metadata_files_query,
    library_metadata_payload,
    metadata_to_attachment_rows,
    metadata_to_note_row,
    metadata_to_revision_row,
)


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeWarehouse:
    def __init__(self) -> None:
        self.ensure_called = False
        self.notes: list[dict[str, object]] = []
        self.revisions: list[dict[str, object]] = []
        self.attachments: list[dict[str, object]] = []

    def ensure_apple_notes_tables(self) -> None:
        self.ensure_called = True

    def insert_apple_notes(self, rows) -> None:
        self.notes.extend(rows)

    def insert_apple_note_revisions(self, rows) -> None:
        self.revisions.extend(rows)

    def insert_apple_note_attachments(self, rows) -> None:
        self.attachments.extend(rows)


class FakePromoter:
    def __init__(self) -> None:
        self.payloads: list[dict[str, object]] = []

    def promote(self, payload) -> int:
        self.payloads.append(payload)
        return 3


def decorated_metadata_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": "apple_notes",
        "account": "zach@example.com",
        "exported_at": "2026-05-21T12:05:00+00:00",
        "note": {
            "note_id": "note-1",
            "revision_id": "rev-1",
            "title": "Planning",
            "folder_id": "projects",
            "folder_path": "Work/Projects",
            "apple_account_id": "icloud",
            "apple_account_name": "iCloud",
            "created_at": "2026-05-20T12:00:00+00:00",
            "modified_at": "2026-05-21T12:00:00+00:00",
            "body_text": "Hello",
            "body_html": "<p>Hello</p>",
            "body_markdown": "Hello",
            "content_sha256": "note-sha",
            "is_deleted": False,
            "attachments": [
                {
                    "attachment_id": "att-1",
                    "filename": "photo.txt",
                    "content_type": "text/plain",
                    "size_bytes": 12,
                    "content_sha256": "att-sha",
                    "is_missing": False,
                    "error": "",
                    "file": {
                        "storage_backend": "google_drive",
                        "storage_key": "apple-notes/inbox/2026/05/note-1/rev-1/attachments/att-1-att-sha.txt",
                        "storage_file_id": "attachment-file",
                        "storage_url": "https://drive/attachment",
                    },
                }
            ],
        },
        "metadata_file": {
            "storage_backend": "google_drive",
            "storage_key": "apple-notes/inbox/2026/05/note-1/rev-1.json",
            "storage_file_id": "metadata-file",
            "storage_url": "https://drive/metadata",
            "content_sha256": "metadata-sha",
        },
        "html_file": {
            "storage_backend": "google_drive",
            "storage_key": "apple-notes/inbox/2026/05/note-1/rev-1.html",
            "storage_file_id": "html-file",
            "storage_url": "https://drive/html",
            "content_sha256": "html-sha",
        },
    }


def test_metadata_to_rows_maps_latest_revision_and_attachment_rows() -> None:
    ingested_at = datetime(2026, 5, 21, 12, 10, tzinfo=UTC)
    payload = decorated_metadata_payload()

    note_row = metadata_to_note_row(payload, ingested_at=ingested_at)
    revision_row = metadata_to_revision_row(payload, ingested_at=ingested_at)
    attachment_rows = metadata_to_attachment_rows(payload, ingested_at=ingested_at)

    assert note_row["note_id"] == "note-1"
    assert note_row["latest_revision_id"] == "rev-1"
    assert note_row["metadata_storage_key"] == "apple-notes/inbox/2026/05/note-1/rev-1.json"
    assert revision_row["revision_id"] == "rev-1"
    assert revision_row["exported_at"] == datetime(2026, 5, 21, 12, 5, tzinfo=UTC)
    assert attachment_rows[0]["attachment_id"] == "att-1"
    assert attachment_rows[0]["storage_file_id"] == "attachment-file"
    assert "file" not in json.loads(attachment_rows[0]["raw_metadata_json"])
    assert "metadata_file" not in clean_metadata_payload(payload)


def test_library_metadata_payload_rewrites_storage_keys() -> None:
    payload = library_metadata_payload(decorated_metadata_payload())

    assert payload["metadata_file"]["storage_key"] == "apple-notes/library/2026/05/note-1/rev-1.json"
    assert payload["html_file"]["storage_key"] == "apple-notes/library/2026/05/note-1/rev-1.html"
    assert (
        payload["note"]["attachments"][0]["file"]["storage_key"]
        == "apple-notes/library/2026/05/note-1/rev-1/attachments/att-1-att-sha.txt"
    )


def test_drive_ingest_runner_writes_rows_and_promotes() -> None:
    warehouse = FakeWarehouse()
    promoter = FakePromoter()

    summary = AppleNotesDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: [decorated_metadata_payload()],
        promoter=promoter,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 12, 10, tzinfo=UTC),
    ).sync()

    assert warehouse.ensure_called
    assert summary.metadata_seen == 1
    assert summary.notes_written == 1
    assert summary.revisions_written == 1
    assert summary.attachments_written == 1
    assert summary.files_promoted == 3
    assert warehouse.notes[0]["metadata_storage_key"].startswith("apple-notes/library/")
    assert warehouse.revisions[0]["revision_id"] == "rev-1"
    assert warehouse.attachments[0]["content_sha256"] == "att-sha"
    assert promoter.payloads == [decorated_metadata_payload()]


def test_drive_ingest_runner_dedupes_latest_rows_but_keeps_revisions() -> None:
    first = decorated_metadata_payload()
    second = deepcopy(first)
    second["exported_at"] = "2026-05-21T12:06:00+00:00"
    second["note"]["revision_id"] = "rev-2"
    second["note"]["content_sha256"] = "note-sha-2"
    warehouse = FakeWarehouse()

    summary = AppleNotesDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: [first, second],
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 12, 10, tzinfo=UTC),
    ).sync()

    assert summary.metadata_seen == 2
    assert summary.notes_written == 1
    assert summary.revisions_written == 2
    assert warehouse.notes[0]["latest_revision_id"] == "rev-2"
    assert {row["revision_id"] for row in warehouse.revisions} == {"rev-1", "rev-2"}


def test_drive_ingest_runner_can_promote_with_factory_workers() -> None:
    warehouse = FakeWarehouse()
    promoters: list[FakePromoter] = []

    def promoter_factory() -> FakePromoter:
        promoter = FakePromoter()
        promoters.append(promoter)
        return promoter

    summary = AppleNotesDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: [decorated_metadata_payload(), decorated_metadata_payload()],
        promoter_factory=promoter_factory,
        promotion_workers=2,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 12, 10, tzinfo=UTC),
    ).sync()

    assert summary.files_promoted == 6
    assert sum(len(promoter.payloads) for promoter in promoters) == 2


def test_drive_metadata_query_targets_apple_notes_inbox() -> None:
    query = drive_metadata_files_query(folder_id="root-folder", stage="inbox")

    assert "pdw_source' and value='apple_notes" in query
    assert "pdw_kind' and value='apple_note_revision_metadata" in query
    assert "pdw_stage' and value='inbox" in query
    assert "pdw_root_folder_id" in query
