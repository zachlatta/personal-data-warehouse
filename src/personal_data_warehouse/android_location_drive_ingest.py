"""Promote authenticated Android location batches into raw source samples."""

from __future__ import annotations
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import islice
import math
from typing import Any
from personal_data_warehouse.browser_history_drive_ingest import (
    batch_records, load_jsonl_gz_object, nested_mapping, parse_datetime,
)
from personal_data_warehouse.objectstore import ObjectStore

OBJECT_PREFIX = "android-location"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox/"
LIBRARY_PREFIX = f"{OBJECT_PREFIX}/library/"
BATCH_KIND = "android_location_export_batch"
SOURCE = "android_location"
BLOB_KIND = "android_location_blob"


@dataclass(frozen=True)
class AndroidLocationDriveIngestSummary:
    batches_seen: int
    events_written: int
    files_promoted: int


class AndroidLocationDriveIngestRunner:
    def __init__(
        self, *, warehouse, batch_source: Callable[[], Iterable[Mapping[str, Any]]],
        object_store: ObjectStore | None = None, logger, max_batches: int = 200,
    ) -> None:
        self.warehouse, self.batch_source, self.object_store = warehouse, batch_source, object_store
        self.logger, self.max_batches = logger, max_batches

    def sync(self) -> AndroidLocationDriveIngestSummary:
        self.warehouse.ensure_android_location_tables()
        batches = events = promoted = 0
        for batch in islice(self.batch_source(), self.max_batches):
            keyed = {}
            for envelope in batch_records(batch):
                row = record_to_row(envelope)
                key = (row["account"], row["device"], row["sample_id"])
                if key in keyed:
                    row["received_at"] = min(row["received_at"], keyed[key]["received_at"])
                keyed[key] = row
            rows = list(keyed.values())
            self.warehouse.insert_android_location_samples(rows)
            events += len(rows)
            batches += 1
            if self.object_store is not None:
                ref = nested_mapping(batch, "batch_file")
                if ref.get("storage_file_id"):
                    self.object_store.move_object(ref, new_object_key=library_storage_key(str(ref["storage_key"])))
                    promoted += 1
        return AndroidLocationDriveIngestSummary(batches, events, promoted)


def record_to_row(envelope: Mapping[str, Any]) -> dict[str, Any]:
    if envelope.get("schema_version") != 1:
        raise ValueError("Unsupported location schema version")
    if envelope.get("source") != SOURCE or envelope.get("record_type") != "location":
        raise ValueError("Not an Android location sample")
    record = nested_mapping(envelope, "record")
    sample_id = str(record.get("sample_id", ""))
    account, device = str(envelope.get("account", "")), str(envelope.get("device", ""))
    if not all((sample_id, account, device)):
        raise ValueError("Location sample identity is required")
    latitude, longitude, accuracy = (float(record[k]) for k in ("latitude", "longitude", "accuracy_m"))
    if (
        not all(math.isfinite(v) for v in (latitude, longitude, accuracy))
        or not -90 <= latitude <= 90 or not -180 <= longitude <= 180 or accuracy <= 0
    ):
        raise ValueError("Invalid location coordinates or accuracy")
    if record.get("provider") != "network":
        raise ValueError("Only the low-power network provider is accepted")
    measured = datetime.fromtimestamp(int(record["measured_at_ms"]) / 1000, tz=UTC)
    received = parse_datetime(envelope.get("received_at"))
    exported = parse_datetime(envelope.get("exported_at"))
    if min(measured.timestamp(), received.timestamp(), exported.timestamp()) <= 0:
        raise ValueError("Measurement, server receipt and export times are required")
    ingested = datetime.now(UTC)
    return dict(account=account, device=device, sample_id=sample_id, latitude=latitude, longitude=longitude,
                accuracy_m=accuracy, measured_at=measured, received_at=received, exported_at=exported,
                provider=str(record.get("provider", "")), raw_json=dict(record), synced_at=ingested,
                sync_version=int(ingested.timestamp()*1_000_000))


def iter_batch_payloads(
    *, object_store: ObjectStore, stage: str = "inbox"
) -> Iterable[Mapping[str, Any]]:
    """Yield inbox batches oldest export first, loading one at a time."""
    listings = sorted(
        object_store.list_objects(kind=BATCH_KIND, stage=stage),
        key=lambda listing: (
            parse_datetime(listing.app_properties.get("exported_at")),
            listing.filename,
        ),
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


def library_storage_key(storage_key: str) -> str:
    if storage_key.startswith(INBOX_PREFIX):
        return LIBRARY_PREFIX + storage_key[len(INBOX_PREFIX) :]
    return storage_key
