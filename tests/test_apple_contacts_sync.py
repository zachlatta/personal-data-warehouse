from __future__ import annotations

from datetime import UTC, datetime
import gzip
import json
from pathlib import Path
import sqlite3

from personal_data_warehouse_apple_contacts.scanner import (
    STORE_FILENAME,
    discover_apple_contacts_stores,
    scan_apple_contacts_store,
)
from personal_data_warehouse_apple_contacts.state import AppleContactsUploadState
from personal_data_warehouse_apple_contacts.sync import AppleContactsUploadRunner


class FakeLogger:
    def info(self, *_args, **_kwargs) -> None:
        pass

    def warning(self, *_args, **_kwargs) -> None:
        pass


class FakeIngestClient:
    def __init__(self) -> None:
        self.batches: list[bytes] = []

    def upload_apple_contacts_batch(self, gzip_bytes: bytes, *, exported_at: str):
        self.batches.append(gzip_bytes)
        return {
            "storage_backend": "google_drive",
            "storage_key": f"apple-contacts/inbox/{exported_at}.jsonl.gz",
            "storage_file_id": "batch-file",
            "storage_url": "",
        }


def create_synthetic_store(path: Path, *, include_contact: bool = True) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE contacts (
            contact_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            display_name TEXT NOT NULL,
            given_name TEXT NOT NULL,
            middle_name TEXT NOT NULL,
            family_name TEXT NOT NULL,
            nickname TEXT NOT NULL,
            organization TEXT NOT NULL,
            department TEXT NOT NULL,
            job_title TEXT NOT NULL,
            note TEXT NOT NULL,
            created_at TEXT NOT NULL,
            modified_at TEXT NOT NULL
        );
        CREATE TABLE phones (
            contact_id TEXT NOT NULL,
            value TEXT NOT NULL,
            label TEXT NOT NULL,
            is_primary INTEGER NOT NULL
        );
        CREATE TABLE emails (
            contact_id TEXT NOT NULL,
            value TEXT NOT NULL,
            label TEXT NOT NULL,
            is_primary INTEGER NOT NULL
        );
        """
    )
    if include_contact:
        connection.execute(
            """
            INSERT INTO contacts VALUES (
                'contact-1', 'source-1', 'Example Person', 'Example', '', 'Person', 'Ex',
                'Example Org', 'Programs', 'Director', 'Met at an event',
                '2026-07-20T12:00:00+00:00', '2026-07-21T12:00:00+00:00'
            )
            """
        )
        connection.executemany(
            "INSERT INTO phones VALUES (?, ?, ?, ?)",
            [
                ("contact-1", "+1 (555) 123-4567", "mobile", 1),
                ("contact-1", "+1 (555) 765-4321", "work", 0),
            ],
        )
        connection.execute(
            "INSERT INTO emails VALUES (?, ?, ?, ?)",
            ("contact-1", "person@example.test", "work", 1),
        )
    connection.commit()
    connection.close()


def decode_batch(payload: bytes) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in gzip.decompress(payload).decode("utf-8").splitlines()
        if line
    ]


def test_discovery_includes_local_and_account_contact_stores(tmp_path: Path) -> None:
    local_store = tmp_path / STORE_FILENAME
    account_store = tmp_path / "Sources" / "account-source" / STORE_FILENAME
    account_store.parent.mkdir(parents=True)
    local_store.touch()
    account_store.touch()

    stores = discover_apple_contacts_stores(tmp_path)

    assert stores == [
        ("local", local_store),
        ("account-source", account_store),
    ]


def test_scan_synthetic_apple_contacts_store_preserves_all_contact_points(tmp_path: Path) -> None:
    store = tmp_path / "AddressBook-v22.abcddb"
    create_synthetic_store(store)

    contacts = scan_apple_contacts_store(store, source_id="source-1")

    assert len(contacts) == 1
    contact = contacts[0]
    assert contact.contact_id == "contact-1"
    assert contact.display_name == "Example Person"
    assert contact.primary_phone == "+1 (555) 123-4567"
    assert [item["value"] for item in contact.phones] == [
        "+1 (555) 123-4567",
        "+1 (555) 765-4321",
    ]
    assert contact.primary_email == "person@example.test"
    assert contact.organization == "Example Org"
    assert contact.note == "Met at an event"


def test_incremental_upload_emits_changes_then_tombstone(tmp_path: Path) -> None:
    store = tmp_path / "AddressBook-v22.abcddb"
    create_synthetic_store(store)
    state = AppleContactsUploadState.open(
        tmp_path / "state.sqlite",
        account="owner@example.test",
        store_path=store,
    )
    client = FakeIngestClient()
    now = lambda: datetime(2026, 7, 23, 12, tzinfo=UTC)

    first = AppleContactsUploadRunner(
        account="owner@example.test",
        store_path=store,
        ingest_client=client,
        logger=FakeLogger(),
        upload_state=state,
        now=now,
    ).sync()
    second = AppleContactsUploadRunner(
        account="owner@example.test",
        store_path=store,
        ingest_client=client,
        logger=FakeLogger(),
        upload_state=state,
        now=now,
    ).sync()

    assert first.contacts_seen == 1
    assert first.contacts_selected == 1
    assert second.contacts_selected == 0
    assert len(client.batches) == 1
    first_record = decode_batch(client.batches[0])[0]
    assert first_record["record_type"] == "contact"
    assert first_record["record"]["display_name"] == "Example Person"
    assert first_record["record"]["is_deleted"] is False

    store.unlink()
    create_synthetic_store(store, include_contact=False)
    third = AppleContactsUploadRunner(
        account="owner@example.test",
        store_path=store,
        ingest_client=client,
        logger=FakeLogger(),
        upload_state=state,
        now=now,
    ).sync()

    assert third.contacts_deleted == 1
    tombstone = decode_batch(client.batches[-1])[0]
    assert tombstone["record"]["contact_id"] == "contact-1"
    assert tombstone["record"]["is_deleted"] is True
    state.close()
