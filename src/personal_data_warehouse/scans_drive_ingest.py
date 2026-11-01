"""Promote scan records from the Drive inbox into base_scans.documents.

``pdw ingest scanner`` runs beside a network scanner. When Start is pressed
on one of its PDW panel names it pulls the pages, wraps them in one PDF,
sends the PDF through /ingest/scans/document/resumable (straight to the
library) and then a gzipped JSONL batch naming it to /ingest/scans/batch,
which lands in the Drive inbox. Each batch is promoted to the library once
its rows are written. Scans upsert by (account, scan_id), so a re-sent
record lands once.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import islice
from typing import Any

from personal_data_warehouse.browser_history_drive_ingest import (
    batch_records,
    int_value,
    load_jsonl_gz_object,
    nested_mapping,
    parse_datetime,
)
from personal_data_warehouse.objectstore import ObjectStore

OBJECT_PREFIX = "scans"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox/"
LIBRARY_PREFIX = f"{OBJECT_PREFIX}/library/"
BATCH_KIND = "scan_export_batch"
SOURCE = "scans"
BLOB_KIND = "scan_document"
RECORD_TYPE = "scan"


@dataclass(frozen=True)
class ScansDriveIngestSummary:
    batches_seen: int
    scans_written: int
    files_promoted: int


class ScansDriveIngestRunner:
    def __init__(
        self,
        *,
        warehouse,
        batch_source: Callable[[], Iterable[Mapping[str, Any]]],
        object_store: ObjectStore | None = None,
        logger,
        max_batches: int = 0,
    ) -> None:
        self._warehouse = warehouse
        self._batch_source = batch_source
        self._object_store = object_store
        self._logger = logger
        self._max_batches = max_batches

    def sync(self) -> ScansDriveIngestSummary:
        self._warehouse.ensure_scans_tables()
        batches = 0
        written = 0
        promoted = 0
        source = iter(self._batch_source())
        if self._max_batches:
            source = islice(source, self._max_batches)
        for batch in source:
            batches += 1
            rows: dict[tuple[str, str], dict[str, Any]] = {}
            for envelope in batch_records(batch):
                if str(envelope.get("record_type", "")) != RECORD_TYPE:
                    continue
                row = record_to_row(envelope)
                if row["scan_id"]:
                    rows[(row["account"], row["scan_id"])] = row
            self._warehouse.insert_scan_documents(list(rows.values()))
            written += len(rows)
            if self._object_store is not None:
                batch_file = nested_mapping(batch, "batch_file")
                target_key = library_storage_key(str(batch_file.get("storage_key", "")))
                if batch_file.get("storage_file_id") and target_key:
                    self._object_store.move_object(batch_file, new_object_key=target_key)
                    promoted += 1
        self._logger.info("Ingested %s scan batches: %s scans", batches, written)
        return ScansDriveIngestSummary(batches_seen=batches, scans_written=written, files_promoted=promoted)


def iter_batch_payloads(*, object_store: ObjectStore, stage: str = "inbox") -> Iterable[Mapping[str, Any]]:
    """Yield inbox batches oldest export first, loading one at a time."""
    listings = sorted(
        object_store.list_objects(kind=BATCH_KIND, stage=stage),
        key=lambda listing: (parse_datetime(listing.app_properties.get("exported_at")), listing.filename),
    )
    for listing in listings:
        exported_at = parse_datetime(listing.app_properties.get("exported_at"))
        storage_key = f"{OBJECT_PREFIX}/{stage}/batches/{exported_at:%Y/%m}/{listing.filename}"
        yield {
            "schema_version": 1,
            "source": SOURCE,
            "batch_file": {
                **listing.ref,
                "storage_key": storage_key,
                "content_sha256": str(listing.app_properties.get("content_sha256", "")),
            },
            "records": list(load_jsonl_gz_object(object_store, listing.ref)),
        }


def has_batch_payloads(*, object_store: ObjectStore, stage: str = "inbox") -> bool:
    return object_store.find_object(kind=BATCH_KIND, stage=stage) is not None


def _text(record: Mapping[str, Any], key: str) -> str:
    value = record.get(key)
    return "" if value is None else str(value)


def record_to_row(envelope: Mapping[str, Any], *, synced_at: datetime | None = None) -> dict[str, Any]:
    """Map one scan record to a row; the whole record is kept as raw_json.

    synced_at is when the row is written, not when the uploader exported it:
    the timeline reads new scans by it, and a scan spooled through an outage
    is exported hours before it lands.
    """
    record = nested_mapping(envelope, "record")
    exported_at = parse_datetime(envelope.get("exported_at"))
    return {
        "account": str(envelope.get("account", "")),
        "device": str(envelope.get("device", "")),
        "scan_id": _text(record, "scan_id"),
        "scanned_at": parse_datetime(record.get("scanned_at")),
        "completed_at": parse_datetime(record.get("completed_at")),
        "scan_profile": _text(record, "profile"),
        "color_mode": _text(record, "color_mode"),
        "resolution_dpi": int_value(record.get("resolution_dpi")),
        "page_size": _text(record, "page_size"),
        "input_source": _text(record, "input_source"),
        "is_duplex": 1 if record.get("is_duplex") is True else 0,
        "duplex_status": _text(record, "duplex_status"),
        "page_count": int_value(record.get("page_count")),
        "front_page_count": int_value(record.get("front_page_count")),
        "back_page_count": int_value(record.get("back_page_count")),
        "scanner_name": _text(record, "scanner_name"),
        "filename": _text(record, "filename"),
        "mime_type": _text(record, "content_type"),
        "size_bytes": int_value(record.get("size_bytes")),
        "content_sha256": _text(record, "content_sha256"),
        "storage_backend": _text(record, "storage_backend"),
        "storage_key": _text(record, "storage_key"),
        "storage_file_id": _text(record, "storage_file_id"),
        "storage_url": _text(record, "storage_url"),
        "time_zone": _text(record, "time_zone"),
        "raw_json": dict(record),
        "exported_at": exported_at,
        "synced_at": synced_at or datetime.now(UTC),
        "sync_version": int(exported_at.timestamp() * 1_000_000),
    }


def library_storage_key(storage_key: str) -> str:
    if storage_key.startswith(INBOX_PREFIX):
        return LIBRARY_PREFIX + storage_key[len(INBOX_PREFIX) :]
    return storage_key
