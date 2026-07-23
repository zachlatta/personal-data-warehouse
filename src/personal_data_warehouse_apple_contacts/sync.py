from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import gzip
import hashlib
import json
import tempfile

from personal_data_warehouse_apple_contacts.scanner import (
    AppleContact,
    discover_apple_contacts_stores,
    scan_apple_contacts_store,
    snapshot_apple_contacts_store,
)
from personal_data_warehouse_apple_contacts.state import AppleContactsUploadState


@dataclass(frozen=True)
class AppleContactsUploadSummary:
    contacts_seen: int
    contacts_selected: int
    contacts_skipped: int
    contacts_deleted: int
    contacts_deferred: int
    batches_uploaded: int


@dataclass(frozen=True)
class SelectedContact:
    payload: dict[str, object]
    fingerprint: str


class AppleContactsUploadRunner:
    def __init__(
        self,
        *,
        account: str,
        store_path: Path | str,
        ingest_client,
        logger,
        upload_state: AppleContactsUploadState | None = None,
        now: Callable[[], datetime] | None = None,
        mode: str = "incremental",
        limit: int | None = None,
        before_upload_check: Callable[[], str | None] | None = None,
    ) -> None:
        if ingest_client is None:
            raise ValueError("ingest_client is required")
        if mode not in {"full", "incremental"}:
            raise ValueError("mode must be 'full' or 'incremental'")
        if limit is not None and limit < 0:
            raise ValueError("limit must be nonnegative")
        self._account = account
        self._store_path = Path(store_path).expanduser()
        self._ingest_client = ingest_client
        self._logger = logger
        self._upload_state = upload_state
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._mode = mode
        self._limit = limit
        self._before_upload_check = before_upload_check

    def sync(self) -> AppleContactsUploadSummary:
        contacts = self._scan_contacts()
        current_keys = {(contact.source_id, contact.contact_id) for contact in contacts}
        selected: list[SelectedContact] = []
        skipped = 0
        for contact in contacts:
            payload = contact_payload(contact)
            fingerprint = json_sha256(payload)
            if (
                self._mode == "incremental"
                and self._upload_state is not None
                and self._upload_state.is_complete(
                    source_id=contact.source_id,
                    contact_id=contact.contact_id,
                    fingerprint=fingerprint,
                )
            ):
                skipped += 1
                continue
            selected.append(SelectedContact(payload=payload, fingerprint=fingerprint))

        tombstones: list[SelectedContact] = []
        if self._upload_state is not None:
            for entry in self._upload_state.entries():
                key = (entry.source_id, entry.contact_id)
                if key in current_keys or entry.is_deleted:
                    continue
                payload = tombstone_payload(
                    source_id=entry.source_id,
                    contact_id=entry.contact_id,
                    display_name=entry.display_name,
                    deleted_at=self._now(),
                )
                tombstones.append(SelectedContact(payload=payload, fingerprint=json_sha256(payload)))
        selected.extend(tombstones)

        deferred = 0
        if self._limit is not None and len(selected) > self._limit:
            deferred = len(selected) - self._limit
            selected = selected[: self._limit]

        if selected and self._before_upload_check is not None:
            reason = self._before_upload_check()
            if reason:
                self._logger.warning("Skipping Apple Contacts upload: %s", reason)
                return AppleContactsUploadSummary(
                    contacts_seen=len(contacts),
                    contacts_selected=len(selected),
                    contacts_skipped=skipped,
                    contacts_deleted=len(tombstones),
                    contacts_deferred=len(selected) + deferred,
                    batches_uploaded=0,
                )

        batches_uploaded = 0
        exported_at = self._now()
        if selected:
            records = [
                envelope(
                    account=self._account,
                    exported_at=exported_at,
                    record=item.payload,
                )
                for item in selected
            ]
            body = gzip.compress(
                ("\n".join(json.dumps(record, sort_keys=True, separators=(",", ":")) for record in records) + "\n").encode(
                    "utf-8"
                )
            )
            stored = self._ingest_client.upload_apple_contacts_batch(
                body,
                exported_at=exported_at.astimezone(UTC).isoformat(),
            )
            batches_uploaded = 1
            self._logger.info(
                "Uploaded Apple Contacts batch %s with %s records",
                stored["storage_key"],
                len(records),
            )
            if self._upload_state is not None:
                for item in selected:
                    payload = item.payload
                    self._upload_state.mark_success(
                        source_id=str(payload["source_id"]),
                        contact_id=str(payload["contact_id"]),
                        fingerprint=item.fingerprint,
                        display_name=str(payload.get("display_name", "")),
                        is_deleted=bool(payload.get("is_deleted")),
                        now=exported_at,
                    )

        return AppleContactsUploadSummary(
            contacts_seen=len(contacts),
            contacts_selected=len(selected),
            contacts_skipped=skipped,
            contacts_deleted=len(tombstones),
            contacts_deferred=deferred,
            batches_uploaded=batches_uploaded,
        )

    def _scan_contacts(self) -> list[AppleContact]:
        contacts: list[AppleContact] = []
        stores = discover_apple_contacts_stores(self._store_path)
        with tempfile.TemporaryDirectory(prefix="pdw-apple-contacts-") as temp_dir:
            for index, (source_id, source_path) in enumerate(stores):
                self._logger.info("Snapshotting Apple Contacts store at %s", source_path)
                snapshot_path = Path(temp_dir) / f"{index}-{source_id}.abcddb"
                snapshot_apple_contacts_store(source_path, snapshot_path)
                contacts.extend(scan_apple_contacts_store(snapshot_path, source_id=source_id))
        return sorted(contacts, key=lambda contact: (contact.source_id, contact.contact_id))


def contact_payload(contact: AppleContact) -> dict[str, object]:
    return {
        "source_id": contact.source_id,
        "contact_id": contact.contact_id,
        "source_uid": contact.source_uid,
        "display_name": contact.display_name,
        "given_name": contact.given_name,
        "middle_name": contact.middle_name,
        "family_name": contact.family_name,
        "nickname": contact.nickname,
        "organization": contact.organization,
        "department": contact.department,
        "job_title": contact.job_title,
        "primary_email": contact.primary_email,
        "primary_phone": contact.primary_phone,
        "emails": list(contact.emails),
        "phones": list(contact.phones),
        "addresses": list(contact.addresses),
        "organizations": list(contact.organizations),
        "urls": list(contact.urls),
        "nicknames": list(contact.nicknames),
        "groups": list(contact.groups),
        "dates": contact.dates,
        "photos": list(contact.photos),
        "notes": contact.note,
        "is_deleted": False,
        "created_at": contact.created_at.astimezone(UTC).isoformat(),
        "source_updated_at": contact.modified_at.astimezone(UTC).isoformat(),
        "raw": contact.raw,
    }


def tombstone_payload(
    *,
    source_id: str,
    contact_id: str,
    display_name: str,
    deleted_at: datetime,
) -> dict[str, object]:
    return {
        "source_id": source_id,
        "contact_id": contact_id,
        "source_uid": contact_id,
        "display_name": display_name,
        "given_name": "",
        "middle_name": "",
        "family_name": "",
        "nickname": "",
        "organization": "",
        "department": "",
        "job_title": "",
        "primary_email": "",
        "primary_phone": "",
        "emails": [],
        "phones": [],
        "addresses": [],
        "organizations": [],
        "urls": [],
        "nicknames": [],
        "groups": [],
        "dates": {},
        "photos": [],
        "notes": "",
        "is_deleted": True,
        "created_at": "1970-01-01T00:00:00+00:00",
        "source_updated_at": deleted_at.astimezone(UTC).isoformat(),
        "raw": {"deleted": True},
    }


def envelope(
    *,
    account: str,
    exported_at: datetime,
    record: dict[str, object],
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": "apple_contacts",
        "account": account,
        "exported_at": exported_at.astimezone(UTC).isoformat(),
        "record_type": "contact",
        "record": record,
    }


def json_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
