"""Promote browser-history batches from the Drive inbox into base_browser_history.

The Mac uploader (``pdw ingest browser-history``) posts gzipped JSONL batches
of visits to the app's /ingest/browser-history/batch endpoint, which writes
them into the Drive inbox. Each batch is written and promoted to the library
on its own, so a first backfill of a few hundred thousand visits never sits
in memory at once.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
import gzip
from itertools import islice
import json
from typing import Any

from personal_data_warehouse.objectstore import ObjectStore

OBJECT_PREFIX = "browser-history"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox/"
LIBRARY_PREFIX = f"{OBJECT_PREFIX}/library/"
BATCH_KIND = "browser_history_export_batch"


@dataclass(frozen=True)
class BrowserHistoryDriveIngestSummary:
    batches_seen: int
    visits_written: int
    files_promoted: int


class BrowserHistoryDriveIngestRunner:
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

    def sync(self) -> BrowserHistoryDriveIngestSummary:
        self._warehouse.ensure_browser_history_tables()
        batches = 0
        written = 0
        promoted = 0
        source = iter(self._batch_source())
        if self._max_batches:
            # islice stops BEFORE pulling the next batch, so the cap never
            # downloads a batch it will not write.
            source = islice(source, self._max_batches)
        for batch in source:
            batches += 1
            by_key: dict[tuple[str, ...], dict[str, Any]] = {}
            for envelope in batch_records(batch):
                if str(envelope.get("record_type", "")) != "visit":
                    continue
                row = record_to_visit_row(envelope)
                by_key[
                    (row["account"], row["device"], row["browser"], row["profile"], row["visit_id"])
                ] = row
            rows = list(by_key.values())
            self._warehouse.insert_browser_history_visits(rows)
            written += len(rows)
            if self._object_store is not None:
                source = nested_mapping(batch, "batch_file")
                target_key = library_storage_key(str(source.get("storage_key", "")))
                if source.get("storage_file_id") and target_key:
                    self._object_store.move_object(source, new_object_key=target_key)
                    promoted += 1
        self._logger.info(
            "Ingested %s browser history batches and %s visit rows", batches, written
        )
        return BrowserHistoryDriveIngestSummary(
            batches_seen=batches, visits_written=written, files_promoted=promoted
        )


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
            "source": "browser_history",
            "batch_file": {
                **listing.ref,
                "storage_key": storage_key,
                "content_sha256": str(listing.app_properties.get("content_sha256", "")),
            },
            "records": list(load_jsonl_gz_object(object_store, listing.ref)),
        }


def has_batch_payloads(*, object_store: ObjectStore, stage: str = "inbox") -> bool:
    return object_store.find_object(kind=BATCH_KIND, stage=stage) is not None


def load_jsonl_gz_object(object_store: ObjectStore, ref: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
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


def record_to_visit_row(envelope: Mapping[str, Any]) -> dict[str, Any]:
    record = nested_mapping(envelope, "record")
    exported_at = parse_datetime(envelope.get("exported_at"))
    raw = record.get("raw")
    return {
        "account": str(envelope.get("account", "")),
        "device": str(record.get("device", "")),
        "browser": str(record.get("browser", "")),
        "profile": str(record.get("profile", "")),
        "visit_id": str(record.get("visit_id", "")),
        "profile_name": str(record.get("profile_name", "")),
        "url_id": str(record.get("url_id", "")),
        "url": str(record.get("url", "")),
        "host": str(record.get("host", "")),
        "title": str(record.get("title", "")),
        "visited_at": parse_datetime(record.get("visited_at")),
        "transition": str(record.get("transition", "")),
        "transition_raw": int_value(record.get("transition_raw")),
        "is_redirect": int(bool(record.get("is_redirect"))),
        "is_synced": int(bool(record.get("is_synced"))),
        "from_visit_id": str(record.get("from_visit_id", "")),
        "visit_duration_ms": int_value(record.get("visit_duration_ms")),
        "search_term": str(record.get("search_term", "")),
        "raw_json": dict(raw) if isinstance(raw, Mapping) else {},
        "exported_at": exported_at,
        "synced_at": exported_at,
        "sync_version": int(exported_at.timestamp() * 1_000_000),
    }


def int_value(value: object) -> int:
    # JSON numbers from the Go uploader may arrive as integral floats.
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int | float):
        return int(value)
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0


def library_storage_key(storage_key: str) -> str:
    if storage_key.startswith(INBOX_PREFIX):
        return LIBRARY_PREFIX + storage_key[len(INBOX_PREFIX) :]
    return storage_key


def nested_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    nested = value.get(key)
    return nested if isinstance(nested, Mapping) else {}


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
