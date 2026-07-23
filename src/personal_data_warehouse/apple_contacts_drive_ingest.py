from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
import gzip
import json
from typing import Any

from personal_data_warehouse.objectstore import ObjectStore

OBJECT_PREFIX = "apple-contacts"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox/"
LIBRARY_PREFIX = f"{OBJECT_PREFIX}/library/"
BATCH_KIND = "apple_contact_export_batch"


@dataclass(frozen=True)
class AppleContactsDriveIngestSummary:
    batches_seen: int
    contacts_written: int
    files_promoted: int


class AppleContactsDriveIngestRunner:
    def __init__(
        self,
        *,
        warehouse,
        batch_source: Callable[[], Iterable[Mapping[str, Any]]],
        object_store: ObjectStore | None = None,
        logger,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._batch_source = batch_source
        self._object_store = object_store
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync(self) -> AppleContactsDriveIngestSummary:
        self._warehouse.ensure_apple_contacts_tables()
        ingested_at = self._now()
        batches = list(self._batch_source())
        records = sorted(
            [record for batch in batches for record in batch_records(batch)],
            key=record_exported_at,
        )
        by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
        for record in records:
            if str(record.get("record_type", "")) != "contact":
                continue
            row = record_to_contact_row(record, ingested_at=ingested_at)
            key = (str(row["account"]), str(row["address_book_id"]), str(row["card_id"]))
            by_key[key] = row
        rows = list(by_key.values())
        self._warehouse.insert_apple_contact_cards(rows)

        promoted = 0
        if self._object_store is not None:
            for batch in batches:
                source = nested_mapping(batch, "batch_file")
                target_key = library_storage_key(str(source.get("storage_key", "")))
                if source.get("storage_file_id") and target_key:
                    self._object_store.move_object(source, new_object_key=target_key)
                    promoted += 1

        self._logger.info(
            "Ingested %s Apple Contacts batches and %s contact rows",
            len(batches),
            len(rows),
        )
        return AppleContactsDriveIngestSummary(
            batches_seen=len(batches),
            contacts_written=len(rows),
            files_promoted=promoted,
        )


def iter_batch_payloads(
    *,
    object_store: ObjectStore,
    stage: str = "inbox",
) -> Iterable[Mapping[str, Any]]:
    for listing in object_store.list_objects(kind=BATCH_KIND, stage=stage):
        exported_at = parse_datetime(listing.app_properties.get("exported_at"))
        storage_key = (
            f"{OBJECT_PREFIX}/{stage}/batches/{exported_at:%Y/%m}/{listing.filename}"
        )
        yield {
            "schema_version": 1,
            "source": "apple_contacts",
            "batch_file": {
                **listing.ref,
                "storage_key": storage_key,
                "content_sha256": str(listing.app_properties.get("content_sha256", "")),
            },
            "records": list(load_jsonl_gz_object(object_store, listing.ref)),
        }


def has_batch_payloads(*, object_store: ObjectStore, stage: str = "inbox") -> bool:
    return object_store.find_object(kind=BATCH_KIND, stage=stage) is not None


def load_jsonl_gz_object(
    object_store: ObjectStore,
    ref: Mapping[str, Any],
) -> Iterable[Mapping[str, Any]]:
    payload = gzip.decompress(object_store.get_object(ref)).decode("utf-8")
    for line in payload.splitlines():
        if not line.strip():
            continue
        value = json.loads(line)
        if isinstance(value, Mapping):
            yield value


def batch_records(batch: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    records = batch.get("records")
    if not isinstance(records, list):
        return []
    return [record for record in records if isinstance(record, Mapping)]


def record_to_contact_row(
    envelope: Mapping[str, Any],
    *,
    ingested_at: datetime,
) -> dict[str, Any]:
    record = nested_mapping(envelope, "record")
    exported_at = parse_datetime(envelope.get("exported_at"))
    source_updated_at = parse_datetime(record.get("source_updated_at"))
    source_id = str(record.get("source_id", ""))
    contact_id = str(record.get("contact_id", ""))
    return {
        "source": "apple_contacts",
        "account": str(envelope.get("account", "")),
        "source_kind": "apple_contacts",
        "address_book_id": source_id,
        "card_id": contact_id,
        "etag": "",
        "source_uid": str(record.get("source_uid", "")) or contact_id,
        "display_name": str(record.get("display_name", "")),
        "given_name": str(record.get("given_name", "")),
        "family_name": str(record.get("family_name", "")),
        "organization": str(record.get("organization", "")),
        "job_title": str(record.get("job_title", "")),
        "primary_email": str(record.get("primary_email", "")),
        "primary_phone": str(record.get("primary_phone", "")),
        "emails": mapping_list(record.get("emails")),
        "phones": mapping_list(record.get("phones")),
        "addresses": mapping_list(record.get("addresses")),
        "organizations": mapping_list(record.get("organizations")),
        "urls": mapping_list(record.get("urls")),
        "nicknames": mapping_list(record.get("nicknames")),
        "groups": mapping_list(record.get("groups")),
        "dates": mapping_value(record.get("dates")),
        "photos": mapping_list(record.get("photos")),
        "notes": str(record.get("notes", "")),
        "is_deleted": int(bool(record.get("is_deleted"))),
        "source_updated_at": source_updated_at,
        "synced_at": exported_at,
        "sync_version": int(exported_at.timestamp() * 1_000_000),
        "raw_json": mapping_value(record.get("raw")),
    }


def record_exported_at(record: Mapping[str, Any]) -> datetime:
    return parse_datetime(record.get("exported_at"))


def library_batch_payload(batch: Mapping[str, Any]) -> dict[str, Any]:
    value = deepcopy(dict(batch))
    batch_file = dict(nested_mapping(value, "batch_file"))
    batch_file["storage_key"] = library_storage_key(str(batch_file.get("storage_key", "")))
    value["batch_file"] = batch_file
    return value


def library_storage_key(storage_key: str) -> str:
    if storage_key.startswith(INBOX_PREFIX):
        return LIBRARY_PREFIX + storage_key[len(INBOX_PREFIX) :]
    return storage_key


def nested_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    nested = value.get(key)
    return nested if isinstance(nested, Mapping) else {}


def mapping_list(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def mapping_value(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def parse_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            parsed = datetime.fromtimestamp(0, tz=UTC)
    else:
        parsed = datetime.fromtimestamp(0, tz=UTC)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
