"""Drive-inbox ingest for manually uploaded finance documents.

Clone of the photos transport with one fixed destination table
(``manual_finance.documents``): the inbox (``manual-finance/inbox/``) receives
document blobs + metadata envelopes, rows upsert by provenance, and objects
promote inbox -> library keeping their account-keyed folder segment. Like
photos, the blob lookup falls back inbox -> library so a re-sent envelope
whose blob was already promoted still resolves.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from personal_data_warehouse.objectstore import ObjectListing, ObjectStore

INBOX_PREFIX = "manual-finance/inbox/"
LIBRARY_PREFIX = "manual-finance/library/"
METADATA_KIND = "manual_finance_metadata"
FILE_KIND = "manual_finance_document"


@dataclass(frozen=True)
class ManualFinanceDriveIngestSummary:
    metadata_seen: int
    rows_written: int
    objects_promoted: int


class ManualFinanceDriveIngestRunner:
    def __init__(
        self,
        *,
        warehouse,
        metadata_source: Callable[[], Iterable[Mapping[str, Any]]],
        object_store: ObjectStore | None = None,
        logger,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._metadata_source = metadata_source
        self._object_store = object_store
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync(self) -> ManualFinanceDriveIngestSummary:
        self._warehouse.ensure_manual_finance_tables()
        ingested_at = self._now()
        payloads = list(self._metadata_source())
        rows = [metadata_to_row(payload, ingested_at=ingested_at) for payload in payloads]
        self._warehouse.insert_manual_finance_documents(rows)
        promoted = 0
        if self._object_store is not None:
            for payload in payloads:
                promoted += promote_payload(self._object_store, payload)
        self._logger.info("Ingested %s manual finance document envelope(s)", len(rows))
        return ManualFinanceDriveIngestSummary(
            metadata_seen=len(payloads),
            rows_written=len(rows),
            objects_promoted=promoted,
        )


def iter_metadata_payloads(
    *,
    object_store: ObjectStore,
    stage: str = "inbox",
) -> Iterable[Mapping[str, Any]]:
    for listing in object_store.list_objects(kind=METADATA_KIND, stage=stage):
        payload = dict(load_json_object(object_store, listing.ref))
        file_listing = find_file_listing(object_store=object_store, payload=payload, stage=stage)
        yield attach_storage_context(
            payload,
            metadata_listing=listing,
            file_listing=file_listing,
        )


def has_metadata_payloads(*, object_store: ObjectStore, stage: str = "inbox") -> bool:
    return object_store.find_object(kind=METADATA_KIND, stage=stage) is not None


def find_file_listing(
    *,
    object_store: ObjectStore,
    payload: Mapping[str, Any],
    stage: str,
) -> ObjectListing:
    file_block = nested_mapping(payload, "file")
    content_sha256 = str(file_block.get("content_sha256", ""))
    if not content_sha256:
        raise ValueError("Manual finance metadata envelope is missing file.content_sha256")
    for candidate_stage in (stage, "library"):
        listing = object_store.find_object(
            kind=FILE_KIND,
            stage=candidate_stage,
            properties={"content_sha256": content_sha256},
        )
        if listing is not None:
            return listing
    raise ValueError(f"Could not find manual finance document for content_sha256={content_sha256}")


def load_json_object(object_store: ObjectStore, ref: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = json.loads(object_store.get_object(ref).decode("utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("Stored object did not contain a JSON object")
    return payload


def attach_storage_context(
    payload: Mapping[str, Any],
    *,
    metadata_listing: ObjectListing,
    file_listing: ObjectListing,
) -> dict[str, Any]:
    decorated = clean_metadata_payload(payload)
    decorated["file_object"] = stored_file_context(file_listing)
    decorated["metadata_object"] = stored_file_context(
        metadata_listing,
        extra={"content_sha256": str(metadata_listing.app_properties.get("content_sha256", ""))},
    )
    return decorated


def stored_file_context(listing: ObjectListing, *, extra: Mapping[str, str] | None = None) -> dict[str, str]:
    context = dict(listing.ref)
    context.update(extra or {})
    return context


def promote_payload(object_store: ObjectStore, payload: Mapping[str, Any]) -> int:
    promoted = 0
    for key in ("file_object", "metadata_object"):
        stored = nested_mapping(payload, key)
        if not stored:
            continue
        storage_key = str(stored.get("storage_key", ""))
        if not storage_key.startswith(INBOX_PREFIX):
            continue
        file_id = str(stored.get("storage_file_id", ""))
        if not file_id:
            raise ValueError(f"Manual finance {key} is missing a storage file id")
        object_store.move_object(stored, new_object_key=library_storage_key(storage_key))
        promoted += 1
    return promoted


def library_storage_key(storage_key: str) -> str:
    # Promotion preserves the account-keyed folder segment.
    if storage_key.startswith(INBOX_PREFIX):
        return f"{LIBRARY_PREFIX}{storage_key[len(INBOX_PREFIX):]}"
    return storage_key


def clean_metadata_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    clean = deepcopy(dict(payload))
    clean.pop("file_object", None)
    clean.pop("metadata_object", None)
    return clean


def metadata_to_row(metadata: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    file_block = nested_mapping(metadata, "file")
    file_object = nested_mapping(metadata, "file_object")
    metadata_object = nested_mapping(metadata, "metadata_object")
    return {
        "source": str(metadata.get("source", "")),
        "account": str(metadata.get("account", "")),
        "source_native_id": str(file_block.get("native_id", "")),
        "filename": str(file_block.get("filename", "")),
        "original_path": str(file_block.get("original_path", "")),
        "mime_type": str(file_block.get("mime_type", "")),
        "size_bytes": int(file_block.get("size_bytes", 0) or 0),
        "content_sha256": str(file_block.get("content_sha256", "")),
        "file_modified_at": parse_timestamp(str(file_block.get("file_modified_at", ""))),
        # Raw envelope kept losslessly (jsonb); only transient storage context
        # is stripped.
        "raw_metadata_json": clean_metadata_payload(metadata),
        "storage_backend": str(file_object.get("storage_backend", "")),
        "storage_key": library_storage_key(str(file_object.get("storage_key", ""))),
        "storage_file_id": str(file_object.get("storage_file_id", "")),
        "storage_url": str(file_object.get("storage_url", "")),
        "metadata_storage_key": library_storage_key(str(metadata_object.get("storage_key", ""))),
        "metadata_storage_file_id": str(metadata_object.get("storage_file_id", "")),
        "metadata_storage_url": str(metadata_object.get("storage_url", "")),
        "metadata_content_sha256": str(metadata_object.get("content_sha256", "")),
        "is_deleted": 0,
        "ingested_at": ingested_at,
        "sync_version": int(ingested_at.timestamp() * 1_000_000),
    }


def parse_timestamp(value: str) -> datetime:
    if not value:
        return datetime.fromtimestamp(0, tz=UTC)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return datetime.fromtimestamp(0, tz=UTC)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def nested_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    nested = value.get(key)
    return nested if isinstance(nested, Mapping) else {}
