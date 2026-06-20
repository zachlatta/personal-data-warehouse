from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
import gzip
import json
import threading
from typing import Any

from personal_data_warehouse.apple_voice_memos_drive_ingest import parse_datetime
from personal_data_warehouse.objectstore import ObjectListing, ObjectStore


@dataclass(frozen=True)
class WhatsAppDriveIngestSummary:
    batches_seen: int
    chats_written: int
    chat_participants_written: int
    chats_backfilled: int
    contacts_written: int
    messages_written: int
    media_items_written: int
    files_promoted: int


OBJECT_PREFIX = "whatsapp"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox/"
LIBRARY_PREFIX = f"{OBJECT_PREFIX}/library/"
BATCH_KIND = "whatsapp_export_batch"
MEDIA_KIND = "whatsapp_media_item"
MEDIA_STORAGE_COLUMNS = (
    "content_sha256",
    "storage_backend",
    "storage_key",
    "storage_file_id",
    "storage_url",
)
# Pushname-only contact records (history sync) must not wipe the fuller names
# a contact-store dump carried.
CONTACT_NAME_COLUMNS = (
    "push_name",
    "first_name",
    "full_name",
    "business_name",
)


class WhatsAppDriveIngestRunner:
    def __init__(
        self,
        *,
        warehouse,
        batch_source: Callable[[], Iterable[Mapping[str, Any]]],
        object_store: ObjectStore | None = None,
        object_store_factory: Callable[[], ObjectStore] | None = None,
        promotion_workers: int = 1,
        logger,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if object_store is not None and object_store_factory is not None:
            raise ValueError("pass only one of object_store or object_store_factory")
        self._warehouse = warehouse
        self._batch_source = batch_source
        self._object_store = object_store
        self._object_store_factory = object_store_factory
        self._promotion_workers = max(1, promotion_workers)
        self._thread_local = threading.local()
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync(self) -> WhatsAppDriveIngestSummary:
        self._warehouse.ensure_whatsapp_tables()
        ingested_at = self._now()
        batches = list(self._batch_source())
        promote = self._object_store is not None or self._object_store_factory is not None
        row_batches = [library_batch_payload(batch) for batch in batches] if promote else batches
        records = sorted(
            [record for batch in row_batches for record in batch_records(batch)],
            key=record_exported_at,
        )

        chat_rows = dedupe_rows(
            [record_to_chat_row(record, ingested_at=ingested_at) for record in records if record_type(record) == "chat"],
            ("account", "chat_id"),
            preserve_columns=("name",),
        )
        chat_participant_rows = dedupe_rows(
            [
                record_to_chat_participant_row(record, ingested_at=ingested_at)
                for record in records
                if record_type(record) == "chat_participant"
            ],
            ("account", "chat_id", "participant_jid"),
            preserve_columns=("display_name",),
        )
        contact_rows = dedupe_rows(
            [
                record_to_contact_row(record, ingested_at=ingested_at)
                for record in records
                if record_type(record) == "contact"
            ],
            ("account", "jid"),
            preserve_columns=CONTACT_NAME_COLUMNS,
        )
        message_rows = dedupe_rows(
            [
                record_to_message_row(record, ingested_at=ingested_at)
                for record in records
                if record_type(record) == "message"
            ],
            ("account", "chat_id", "message_id"),
        )
        media_item_rows = dedupe_rows(
            [
                record_to_media_item_row(record, ingested_at=ingested_at)
                for record in records
                if record_type(record) == "media_item"
            ],
            ("account", "chat_id", "message_id"),
            preserve_columns=MEDIA_STORAGE_COLUMNS,
        )

        self._warehouse.insert_whatsapp_chats(chat_rows)
        self._warehouse.insert_whatsapp_chat_participants(chat_participant_rows)
        self._warehouse.insert_whatsapp_contacts(contact_rows)
        self._warehouse.insert_whatsapp_messages(message_rows)
        self._warehouse.insert_whatsapp_media_items(media_item_rows)

        # Some chat_ids (notably status@broadcast) never get a chat row from
        # history/group sync; synthesize the gaps so chat_type is reliable and a
        # status post can't masquerade as a DM in a message->chat join.
        chats_backfilled = self._warehouse.backfill_whatsapp_chats_from_messages()

        promoted = 0
        if promote:
            self._logger.info(
                "Promoting %s WhatsApp Drive batch(es) with %s worker(s)",
                len(batches),
                self._promotion_workers,
            )
            if self._object_store_factory is not None and self._promotion_workers > 1:
                promoted = self._promote_stored_files_parallel(batches)
            elif self._promotion_workers == 1 or len(batches) <= 1:
                promoted = sum(self._promote_batch(batch) for batch in batches)
            else:
                with ThreadPoolExecutor(max_workers=self._promotion_workers, thread_name_prefix="whatsapp-promote") as executor:
                    futures = [executor.submit(self._promote_batch, batch) for batch in batches]
                    promoted = sum(future.result() for future in as_completed(futures))

        self._logger.info(
            "Ingested %s WhatsApp batches, %s messages, %s media items",
            len(batches),
            len(message_rows),
            len(media_item_rows),
        )
        return WhatsAppDriveIngestSummary(
            batches_seen=len(batches),
            chats_written=len(chat_rows),
            chat_participants_written=len(chat_participant_rows),
            chats_backfilled=chats_backfilled,
            contacts_written=len(contact_rows),
            messages_written=len(message_rows),
            media_items_written=len(media_item_rows),
            files_promoted=promoted,
        )

    def _promote_batch(self, batch: Mapping[str, Any]) -> int:
        return promote_batch(self._object_store_for_thread(), batch)

    def _promote_stored_files_parallel(self, batches: list[Mapping[str, Any]]) -> int:
        pairs: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
        for batch in batches:
            pairs.extend(stored_files_for_promotion(batch, library_batch_payload(batch)))
        with ThreadPoolExecutor(max_workers=self._promotion_workers, thread_name_prefix="whatsapp-promote") as executor:
            futures = [executor.submit(self._promote_stored_file, source, target) for source, target in pairs]
            return sum(1 for future in as_completed(futures) if future.result())

    def _promote_stored_file(self, source: Mapping[str, Any], target: Mapping[str, Any]) -> bool:
        return promote_stored_file(self._object_store_for_thread(), source=source, target=target)

    def _object_store_for_thread(self) -> ObjectStore:
        if self._object_store is not None:
            return self._object_store
        store = getattr(self._thread_local, "object_store", None)
        if store is None:
            if self._object_store_factory is None:
                raise RuntimeError("object_store_factory is not configured")
            store = self._object_store_factory()
            self._thread_local.object_store = store
        return store


def promote_batch(object_store: ObjectStore, batch: Mapping[str, Any]) -> int:
    promoted = 0
    library_batch = library_batch_payload(batch)
    for source, target in stored_files_for_promotion(batch, library_batch):
        if promote_stored_file(object_store, source=source, target=target):
            promoted += 1
    return promoted


def promote_stored_file(object_store: ObjectStore, *, source: Mapping[str, Any], target: Mapping[str, Any]) -> bool:
    file_id = str(source.get("storage_file_id", ""))
    target_key = str(target.get("storage_key", ""))
    if not file_id or not target_key:
        return False
    object_store.move_object(source, new_object_key=target_key)
    return True


def iter_batch_payloads(*, object_store: ObjectStore, stage: str = "inbox") -> Iterable[Mapping[str, Any]]:
    for listing in object_store.list_objects(kind=BATCH_KIND, stage=stage):
        records = list(load_jsonl_gz_object(object_store, listing.ref))
        yield {
            "schema_version": 1,
            "source": "whatsapp",
            "batch_file": stored_file_context(
                storage_key=batch_storage_key(listing=listing, stage=stage),
                listing=listing,
                extra={"content_sha256": str(listing.app_properties.get("content_sha256", ""))},
            ),
            "records": records,
        }


def has_batch_payloads(*, object_store: ObjectStore, stage: str = "inbox") -> bool:
    return object_store.find_object(kind=BATCH_KIND, stage=stage) is not None


def load_jsonl_gz_object(object_store: ObjectStore, ref: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    content = gzip.decompress(object_store.get_object(ref)).decode("utf-8")
    for line in content.splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, Mapping):
            yield payload


def batch_records(batch: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    records = batch.get("records", [])
    return [record for record in records if isinstance(record, Mapping)]


def dedupe_rows(
    rows: list[dict[str, Any]],
    key_columns: tuple[str, ...],
    *,
    preserve_columns: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    """Collapse rows sharing key_columns, keeping the newest row per key.

    Rows arrive sorted oldest-first. A newer record can legitimately omit
    values an older one carried (a media manifest has no storage info while
    the downloaded-blob record does; a pushname-only contact has no full
    name while a contact-store dump does). For preserve_columns, keep the
    previous row's value when the newer row's is empty - mirroring the
    preserve-non-empty upsert that protects those columns once they reach
    the warehouse.
    """
    by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(row.get(column) for column in key_columns)
        previous = by_key.get(key)
        if previous is not None:
            for column in preserve_columns:
                if not row.get(column) and previous.get(column):
                    row[column] = previous[column]
        by_key[key] = row
    return list(by_key.values())


def record_type(record: Mapping[str, Any]) -> str:
    return str(record.get("record_type", ""))


def record_payload(record: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = record.get("record", {})
    return payload if isinstance(payload, Mapping) else {}


def record_account(record: Mapping[str, Any]) -> str:
    return str(record.get("account", ""))


def record_exported_at(record: Mapping[str, Any]) -> datetime:
    return parse_datetime(str(record.get("exported_at", "")))


def record_to_chat_row(record: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    payload = record_payload(record)
    return {
        "account": record_account(record),
        "chat_id": str(payload.get("chat_id", "")),
        "name": str(payload.get("name", "")),
        "chat_type": str(payload.get("chat_type", "")),
        "is_archived": int(bool(payload.get("is_archived", False))),
        "last_message_at": parse_datetime(str(payload.get("last_message_at", ""))),
        "raw_metadata_json": raw_json(payload),
        "ingested_at": ingested_at,
        "sync_version": sync_version(ingested_at),
    }


def record_to_chat_participant_row(record: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    payload = record_payload(record)
    return {
        "account": record_account(record),
        "chat_id": str(payload.get("chat_id", "")),
        "participant_jid": str(payload.get("participant_jid", "")),
        "phone_jid": str(payload.get("phone_jid", "")),
        "lid_jid": str(payload.get("lid_jid", "")),
        "display_name": str(payload.get("display_name", "")),
        "is_admin": int(bool(payload.get("is_admin", False))),
        "is_super_admin": int(bool(payload.get("is_super_admin", False))),
        "raw_metadata_json": raw_json(payload),
        "ingested_at": ingested_at,
        "sync_version": sync_version(ingested_at),
    }


def record_to_contact_row(record: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    payload = record_payload(record)
    return {
        "account": record_account(record),
        "jid": str(payload.get("jid", "")),
        "push_name": str(payload.get("push_name", "")),
        "first_name": str(payload.get("first_name", "")),
        "full_name": str(payload.get("full_name", "")),
        "business_name": str(payload.get("business_name", "")),
        "raw_metadata_json": raw_json(payload),
        "ingested_at": ingested_at,
        "sync_version": sync_version(ingested_at),
    }


def record_to_message_row(record: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    payload = record_payload(record)
    return {
        "account": record_account(record),
        "chat_id": str(payload.get("chat_id", "")),
        "message_id": str(payload.get("message_id", "")),
        "sender_jid": str(payload.get("sender_jid", "")),
        "push_name": str(payload.get("push_name", "")),
        "is_from_me": int(bool(payload.get("is_from_me", False))),
        "body_text": str(payload.get("body_text", "")),
        "message_kind": str(payload.get("message_kind", "")),
        "media_type": str(payload.get("media_type", "")),
        "quoted_message_id": str(payload.get("quoted_message_id", "")),
        "message_at": parse_datetime(str(payload.get("message_at", ""))),
        "edited_at": parse_datetime(str(payload.get("edited_at", ""))),
        "is_deleted": int(bool(payload.get("is_deleted", False))),
        "raw_metadata_json": raw_json(payload),
        "ingested_at": ingested_at,
        "sync_version": sync_version(ingested_at),
    }


def record_to_media_item_row(record: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    payload = record_payload(record)
    file = nested_mapping(payload, "file")
    return {
        "account": record_account(record),
        "chat_id": str(payload.get("chat_id", "")),
        "message_id": str(payload.get("message_id", "")),
        "media_type": str(payload.get("media_type", "")),
        "filename": str(payload.get("filename", "")),
        "mime_type": str(payload.get("mime_type", "")),
        "total_bytes": int(payload.get("total_bytes", 0) or 0),
        "size_bytes": int(payload.get("size_bytes", 0) or 0),
        "file_sha256": str(payload.get("file_sha256", "")),
        "content_sha256": str(payload.get("content_sha256", "")),
        "is_missing": int(bool(payload.get("is_missing", False))),
        "error": str(payload.get("error", "")),
        "message_at": parse_datetime(str(payload.get("message_at", ""))),
        "storage_backend": str(file.get("storage_backend", "")),
        "storage_key": str(file.get("storage_key", "")),
        "storage_file_id": str(file.get("storage_file_id", "")),
        "storage_url": str(file.get("storage_url", "")),
        "raw_metadata_json": raw_json(clean_media_item_payload(payload)),
        "ingested_at": ingested_at,
        "sync_version": sync_version(ingested_at),
    }


def library_batch_payload(batch: Mapping[str, Any]) -> dict[str, Any]:
    promoted = deepcopy(dict(batch))
    batch_file = promoted.get("batch_file")
    if isinstance(batch_file, dict):
        batch_file["storage_key"] = library_storage_key(str(batch_file.get("storage_key", "")))
    records = []
    for record in batch_records(promoted):
        record_copy = deepcopy(dict(record))
        payload = record_copy.get("record")
        if isinstance(payload, dict):
            file = payload.get("file")
            if isinstance(file, dict):
                file["storage_key"] = library_storage_key(str(file.get("storage_key", "")))
        records.append(record_copy)
    promoted["records"] = records
    return promoted


def stored_files_for_promotion(
    batch: Mapping[str, Any],
    library_batch: Mapping[str, Any],
) -> list[tuple[Mapping[str, Any], Mapping[str, Any]]]:
    pairs: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    source_batch_file = nested_mapping(batch, "batch_file")
    target_batch_file = nested_mapping(library_batch, "batch_file")
    if source_batch_file and target_batch_file:
        pairs.append((source_batch_file, target_batch_file))
    source_records = batch_records(batch)
    target_records = batch_records(library_batch)
    for source_record, target_record in zip(source_records, target_records, strict=False):
        source_file = nested_mapping(record_payload(source_record), "file")
        target_file = nested_mapping(record_payload(target_record), "file")
        if source_file and target_file:
            pairs.append((source_file, target_file))
    return pairs


def clean_media_item_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    clean = deepcopy(dict(payload))
    clean.pop("file", None)
    return clean


def batch_storage_key(*, listing: ObjectListing, stage: str) -> str:
    name = listing.filename
    exported = str(listing.app_properties.get("exported_at", ""))
    exported_at = parse_datetime(exported) if exported else datetime.fromtimestamp(0, tz=UTC)
    if exported_at.year > 1970:
        return f"{OBJECT_PREFIX}/{stage}/batches/{exported_at.year:04d}/{exported_at.month:02d}/{name}"
    return f"{OBJECT_PREFIX}/{stage}/batches/{name}"


def stored_file_context(
    *,
    storage_key: str,
    listing: ObjectListing,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    context = dict(listing.ref)
    context["storage_key"] = storage_key
    context.update(extra or {})
    return context


def library_storage_key(storage_key: str) -> str:
    if storage_key.startswith(INBOX_PREFIX):
        return f"{LIBRARY_PREFIX}{storage_key[len(INBOX_PREFIX):]}"
    return storage_key


def nested_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    nested = value.get(key)
    return nested if isinstance(nested, Mapping) else {}


def raw_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def sync_version(ingested_at: datetime) -> int:
    return int(ingested_at.astimezone(UTC).timestamp() * 1_000_000)
