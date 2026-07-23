from __future__ import annotations

from datetime import UTC, datetime
import gzip
import json

from personal_data_warehouse.apple_contacts_drive_ingest import (
    AppleContactsDriveIngestRunner,
    iter_batch_payloads,
    record_to_contact_row,
)
from personal_data_warehouse.objectstore import ObjectListing


class FakeLogger:
    def info(self, *_args, **_kwargs) -> None:
        pass


class FakeWarehouse:
    def __init__(self) -> None:
        self.ensure_called = False
        self.contacts: list[dict[str, object]] = []

    def ensure_apple_contacts_tables(self) -> None:
        self.ensure_called = True

    def insert_apple_contact_cards(self, rows) -> None:
        self.contacts.extend(rows)


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, *, listings=None, payloads=None) -> None:
        self.listings = list(listings or [])
        self.payloads = dict(payloads or {})
        self.moved: list[tuple[str, str]] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        return list(self.listings) if kind == "apple_contact_export_batch" else []

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == "apple_contact_export_batch" and self.listings:
            return self.listings[0]
        return None

    def get_object(self, ref):
        return self.payloads[str(ref["storage_file_id"])]

    def move_object(self, ref, *, new_object_key, app_properties=None):
        self.moved.append((str(ref["storage_file_id"]), new_object_key))
        return {**ref, "storage_key": new_object_key}


def envelope(*, deleted: bool = False) -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": "apple_contacts",
        "account": "owner@example.test",
        "exported_at": "2026-07-23T12:00:00+00:00",
        "record_type": "contact",
        "record": {
            "source_id": "source-1",
            "contact_id": "contact-1",
            "display_name": "" if deleted else "Example Person",
            "given_name": "" if deleted else "Example",
            "family_name": "" if deleted else "Person",
            "organization": "",
            "job_title": "",
            "primary_email": "" if deleted else "person@example.test",
            "primary_phone": "" if deleted else "+15551234567",
            "emails": [] if deleted else [{"value": "person@example.test"}],
            "phones": [] if deleted else [{"value": "+15551234567"}],
            "addresses": [],
            "organizations": [],
            "urls": [],
            "nicknames": [],
            "groups": [],
            "dates": {},
            "photos": [],
            "notes": "",
            "is_deleted": deleted,
            "source_updated_at": "2026-07-23T11:00:00+00:00",
            "raw": {},
        },
    }


def test_record_to_contact_row_uses_source_owned_identity() -> None:
    row = record_to_contact_row(envelope(), ingested_at=datetime(2026, 7, 23, 13, tzinfo=UTC))

    assert row["source"] == "apple_contacts"
    assert row["source_kind"] == "apple_contacts"
    assert row["address_book_id"] == "source-1"
    assert row["card_id"] == "contact-1"
    assert row["display_name"] == "Example Person"
    assert row["primary_phone"] == "+15551234567"


def test_drive_ingest_dedupes_newest_contact_and_promotes_batch() -> None:
    listing = ObjectListing(
        ref={
            "storage_backend": "google_drive",
            "storage_key": "",
            "storage_file_id": "batch-file",
            "storage_url": "",
        },
        app_properties={
            "content_sha256": "batch-sha",
            "exported_at": "2026-07-23T12:00:00+00:00",
        },
        filename="batch.jsonl.gz",
    )
    records = [envelope(), envelope(deleted=True)]
    payload = gzip.compress("\n".join(json.dumps(record) for record in records).encode())
    store = FakeObjectStore(listings=[listing], payloads={"batch-file": payload})
    warehouse = FakeWarehouse()

    batches = list(iter_batch_payloads(object_store=store))
    summary = AppleContactsDriveIngestRunner(
        warehouse=warehouse,
        batch_source=lambda: batches,
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 7, 23, 13, tzinfo=UTC),
    ).sync()

    assert warehouse.ensure_called
    assert summary.contacts_written == 1
    assert warehouse.contacts[0]["is_deleted"] == 1
    assert store.moved == [
        ("batch-file", "apple-contacts/library/batches/2026/07/batch.jsonl.gz")
    ]
