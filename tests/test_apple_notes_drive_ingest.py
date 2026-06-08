from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import json

from personal_data_warehouse.apple_notes_drive_ingest import (
    AppleNotesDriveIngestRunner,
    clean_metadata_payload,
    has_metadata_payloads,
    iter_metadata_payloads,
    library_metadata_payload,
    metadata_to_attachment_rows,
    metadata_to_note_row,
    metadata_to_revision_row,
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


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, *, listings_by_kind=None, json_by_id=None) -> None:
        self.listings_by_kind = listings_by_kind or {}
        self.json_by_id = dict(json_by_id or {})
        self.list_calls: list[dict] = []
        self.moved: list[tuple[str, str]] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        self.list_calls.append({"kind": kind, "stage": stage, "properties": properties})
        return list(self.listings_by_kind.get(kind, []))

    def find_object(self, *, kind, stage=None, properties=None):
        listings = self.listings_by_kind.get(kind, [])
        return listings[0] if listings else None

    def get_object(self, ref):
        return json.dumps(self.json_by_id[str(ref["storage_file_id"])]).encode("utf-8")

    def move_object(self, ref, *, new_object_key, app_properties=None):
        self.moved.append((str(ref.get("storage_file_id", "")), new_object_key))
        return {
            "storage_backend": self.backend,
            "storage_key": new_object_key,
            "storage_file_id": str(ref.get("storage_file_id", "")),
            "storage_url": "https://drive/promoted",
        }


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


def raw_metadata_payload() -> dict[str, object]:
    payload = clean_metadata_payload(decorated_metadata_payload())
    return payload


def _listing(kind_id: str, app_properties: dict, filename: str = "") -> ObjectListing:
    return ObjectListing(
        ref={
            "storage_backend": "google_drive",
            "storage_key": "",
            "storage_file_id": kind_id,
            "storage_url": f"https://drive/{kind_id}",
        },
        app_properties=app_properties,
        filename=filename,
    )


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


def test_iter_metadata_payloads_joins_html_and_attachments_via_object_store() -> None:
    store = FakeObjectStore(
        listings_by_kind={
            "apple_note_revision_metadata": [
                _listing("metadata-file", {"note_id": "note-1", "revision_id": "rev-1", "content_sha256": "metadata-sha"})
            ],
            "apple_note_body_html": [
                _listing("html-file", {"note_id": "note-1", "revision_id": "rev-1", "content_sha256": "html-sha"})
            ],
            "apple_note_attachment": [
                _listing(
                    "attachment-file",
                    {"note_id": "note-1", "revision_id": "rev-1", "attachment_id": "att-1", "content_sha256": "att-sha"},
                )
            ],
        },
        json_by_id={"metadata-file": raw_metadata_payload()},
    )

    payloads = list(iter_metadata_payloads(object_store=store))

    assert len(payloads) == 1
    payload = payloads[0]
    assert payload["metadata_file"]["storage_key"] == "apple-notes/inbox/2026/05/note-1/rev-1.json"
    assert payload["metadata_file"]["storage_file_id"] == "metadata-file"
    assert payload["html_file"]["storage_key"] == "apple-notes/inbox/2026/05/note-1/rev-1.html"
    attachment_file = payload["note"]["attachments"][0]["file"]
    assert attachment_file["storage_key"] == "apple-notes/inbox/2026/05/note-1/rev-1/attachments/att-1-att-sha.txt"
    assert attachment_file["storage_file_id"] == "attachment-file"
    # metadata listed with stage; attachments listed without stage (any).
    metadata_call = next(call for call in store.list_calls if call["kind"] == "apple_note_revision_metadata")
    attachment_call = next(call for call in store.list_calls if call["kind"] == "apple_note_attachment")
    assert metadata_call["stage"] == "inbox"
    assert attachment_call["stage"] == ""


def test_has_metadata_payloads_uses_find_object() -> None:
    store = FakeObjectStore(
        listings_by_kind={"apple_note_revision_metadata": [_listing("metadata-file", {})]}
    )
    assert has_metadata_payloads(object_store=store) is True
    assert has_metadata_payloads(object_store=FakeObjectStore()) is False


def test_drive_ingest_runner_writes_rows_and_promotes() -> None:
    warehouse = FakeWarehouse()
    store = FakeObjectStore()

    summary = AppleNotesDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: [decorated_metadata_payload()],
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 12, 10, tzinfo=UTC),
    ).sync()

    assert warehouse.ensure_called
    assert summary.metadata_seen == 1
    assert summary.notes_written == 1
    assert summary.revisions_written == 1
    assert summary.attachments_written == 1
    assert summary.files_promoted == 3  # metadata + html + attachment
    assert warehouse.notes[0]["metadata_storage_key"].startswith("apple-notes/library/")
    assert warehouse.revisions[0]["revision_id"] == "rev-1"
    assert warehouse.attachments[0]["content_sha256"] == "att-sha"
    assert ("metadata-file", "apple-notes/library/2026/05/note-1/rev-1.json") in store.moved
    assert ("html-file", "apple-notes/library/2026/05/note-1/rev-1.html") in store.moved
    assert (
        "attachment-file",
        "apple-notes/library/2026/05/note-1/rev-1/attachments/att-1-att-sha.txt",
    ) in store.moved


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


def test_drive_ingest_runner_promotes_with_object_store_factory_workers() -> None:
    warehouse = FakeWarehouse()
    stores: list[FakeObjectStore] = []

    def object_store_factory() -> FakeObjectStore:
        store = FakeObjectStore()
        stores.append(store)
        return store

    summary = AppleNotesDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: [decorated_metadata_payload(), decorated_metadata_payload()],
        object_store_factory=object_store_factory,
        promotion_workers=2,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 5, 21, 12, 10, tzinfo=UTC),
    ).sync()

    assert summary.files_promoted == 6  # 2 payloads x 3 files
    assert sum(len(store.moved) for store in stores) == 6
