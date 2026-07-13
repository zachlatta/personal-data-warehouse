"""Drive-inbox ingest for the shared photos transport.

One inbox (``photos/inbox/``) receives every photo source's blobs + metadata
envelopes; each envelope's ``source`` routes its row into that source's raw
table via ``PHOTO_SOURCE_RELATIONS`` — the agent-sessions pattern. Unknown
sources fail loud (a typo'd slug must never silently drop photos).

Two deliberate deviations from the voice-memos template this clones:

- The blob lookup falls back inbox -> library: a second source's envelope can
  reference bytes an earlier ingest already promoted (identical file from two
  sources), in which case only the metadata object is promoted.
- Envelope metadata objects are deduped by provenance sha (not blob sha), so
  one blob can carry several envelopes; rows upsert by the same provenance
  primary key.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from personal_data_warehouse.objectstore import ObjectListing, ObjectStore
from personal_data_warehouse.relations import PHOTO_SOURCE_RELATIONS

INBOX_PREFIX = "photos/inbox/"
LIBRARY_PREFIX = "photos/library/"
METADATA_KIND = "photo_metadata"
FILE_KIND = "photo_file"


@dataclass(frozen=True)
class PhotosDriveIngestSummary:
    metadata_seen: int
    rows_written: int
    objects_promoted: int


class PhotosDriveIngestRunner:
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

    def sync(self) -> PhotosDriveIngestSummary:
        self._warehouse.ensure_photos_tables()
        ingested_at = self._now()
        payloads = list(self._metadata_source())
        rows_by_table: dict[str, list[dict[str, Any]]] = {}
        for payload in payloads:
            table = photo_source_table(payload)
            rows_by_table.setdefault(table, []).append(
                metadata_to_row(payload, ingested_at=ingested_at)
            )
        for table, rows in rows_by_table.items():
            self._warehouse.insert_photo_source_files(table, rows)
        promoted = 0
        if self._object_store is not None:
            for payload in payloads:
                promoted += promote_payload(self._object_store, payload)
        rows_written = sum(len(rows) for rows in rows_by_table.values())
        self._logger.info(
            "Ingested %s photo metadata envelope(s) into %s table(s)",
            rows_written,
            len(rows_by_table),
        )
        return PhotosDriveIngestSummary(
            metadata_seen=len(payloads),
            rows_written=rows_written,
            objects_promoted=promoted,
        )


def photo_source_table(payload: Mapping[str, Any]) -> str:
    source = str(payload.get("source", ""))
    table = PHOTO_SOURCE_RELATIONS.get(source)
    if table is None:
        raise ValueError(
            f"Unknown photo source {source!r} in metadata envelope; register it in "
            "PHOTO_SOURCE_RELATIONS (relations.py) with its raw table before ingesting"
        )
    return table


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
    """The envelope's blob, wherever it lives.

    Inbox first; then the library: bytes shared by two sources are promoted by
    whichever envelope ingests first, and the second envelope must still
    resolve them (the voice-memos template raises here — do not copy that).
    """
    file_block = nested_mapping(payload, "file")
    content_sha256 = str(file_block.get("content_sha256", ""))
    if not content_sha256:
        raise ValueError("Photo metadata envelope is missing file.content_sha256")
    for candidate_stage in (stage, "library"):
        listing = object_store.find_object(
            kind=FILE_KIND,
            stage=candidate_stage,
            properties={"content_sha256": content_sha256},
        )
        if listing is not None:
            return listing
    raise ValueError(f"Could not find photo file object for content_sha256={content_sha256}")


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
    """Move this envelope's inbox objects to the library; already-promoted
    blobs (found via the library fallback) are left in place."""
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
            raise ValueError(f"Photo {key} is missing a storage file id")
        object_store.move_object(stored, new_object_key=library_storage_key(storage_key))
        promoted += 1
    return promoted


def library_storage_key(storage_key: str) -> str:
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
        "role": str(file_block.get("role", "")),
        "filename": str(file_block.get("filename", "")),
        "mime_type": str(file_block.get("mime_type", "")),
        "size_bytes": int(file_block.get("size_bytes", 0) or 0),
        "width": int(file_block.get("width", 0) or 0),
        "height": int(file_block.get("height", 0) or 0),
        "content_sha256": str(file_block.get("content_sha256", "")),
        "captured_at": parse_captured_at(
            str(file_block.get("captured_at", "")),
            str(file_block.get("capture_tz_offset", "")),
        ),
        "capture_tz_offset": str(file_block.get("capture_tz_offset", "")),
        "camera_make": str(file_block.get("camera_make", "")),
        "camera_model": str(file_block.get("camera_model", "")),
        # Raw envelope kept losslessly (jsonb) so canonicalization can re-run
        # forever; only the transient storage context is stripped.
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


def parse_captured_at(wall_clock: str, tz_offset: str) -> datetime:
    """Combine the envelope's local wall-clock capture time with its offset."""
    if not wall_clock:
        return datetime.fromtimestamp(0, tz=UTC)
    try:
        parsed = datetime.fromisoformat(f"{wall_clock}{tz_offset}" if tz_offset else wall_clock)
    except ValueError:
        return datetime.fromtimestamp(0, tz=UTC)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def nested_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    nested = value.get(key)
    return nested if isinstance(nested, Mapping) else {}
