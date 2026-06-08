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
class AppleMessagesDriveIngestSummary:
    batches_seen: int
    handles_written: int
    chats_written: int
    chat_handles_written: int
    messages_written: int
    chat_messages_written: int
    attachments_written: int
    files_promoted: int


OBJECT_PREFIX = "apple-messages"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox/"
LIBRARY_PREFIX = f"{OBJECT_PREFIX}/library/"
BATCH_KIND = "apple_message_export_batch"
ATTACHMENT_KIND = "apple_message_attachment"


class AppleMessagesDriveIngestRunner:
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

    def sync(self) -> AppleMessagesDriveIngestSummary:
        self._warehouse.ensure_apple_messages_tables()
        ingested_at = self._now()
        batches = list(self._batch_source())
        promote = self._object_store is not None or self._object_store_factory is not None
        row_batches = [library_batch_payload(batch) for batch in batches] if promote else batches
        records = sorted(
            [record for batch in row_batches for record in batch_records(batch)],
            key=record_exported_at,
        )

        handle_rows = dedupe_rows(
            [record_to_handle_row(record, ingested_at=ingested_at) for record in records if record_type(record) == "handle"],
            ("account", "handle_id"),
        )
        chat_rows = dedupe_rows(
            [record_to_chat_row(record, ingested_at=ingested_at) for record in records if record_type(record) == "chat"],
            ("account", "chat_id"),
        )
        chat_handle_rows = dedupe_rows(
            [
                record_to_chat_handle_row(record, ingested_at=ingested_at)
                for record in records
                if record_type(record) == "chat_handle"
            ],
            ("account", "chat_id", "handle_id"),
        )
        message_rows = dedupe_rows(
            [
                record_to_message_row(record, ingested_at=ingested_at)
                for record in records
                if record_type(record) in {"message", "deleted_message"}
            ],
            ("account", "message_id"),
        )
        chat_message_rows = dedupe_rows(
            [
                record_to_chat_message_row(record, ingested_at=ingested_at)
                for record in records
                if record_type(record) == "chat_message"
            ],
            ("account", "chat_id", "message_id"),
        )
        attachment_rows = dedupe_rows(
            [
                record_to_attachment_row(record, ingested_at=ingested_at)
                for record in records
                if record_type(record) == "attachment"
            ],
            ("account", "attachment_id", "message_id"),
        )

        self._warehouse.insert_apple_message_handles(handle_rows)
        self._warehouse.insert_apple_message_chats(chat_rows)
        self._warehouse.insert_apple_message_chat_handles(chat_handle_rows)
        self._warehouse.insert_apple_messages(message_rows)
        self._warehouse.insert_apple_message_chat_messages(chat_message_rows)
        self._warehouse.insert_apple_message_attachments(attachment_rows)

        promoted = 0
        if promote:
            self._logger.info(
                "Promoting %s Apple Messages Drive batch(es) with %s worker(s)",
                len(batches),
                self._promotion_workers,
            )
            if self._object_store_factory is not None and self._promotion_workers > 1:
                promoted = self._promote_stored_files_parallel(batches)
            elif self._promotion_workers == 1 or len(batches) <= 1:
                promoted = sum(self._promote_batch(batch) for batch in batches)
            else:
                with ThreadPoolExecutor(max_workers=self._promotion_workers, thread_name_prefix="apple-messages-promote") as executor:
                    futures = [executor.submit(self._promote_batch, batch) for batch in batches]
                    promoted = sum(future.result() for future in as_completed(futures))

        self._logger.info(
            "Ingested %s Apple Messages batches, %s messages, %s attachments",
            len(batches),
            len(message_rows),
            len(attachment_rows),
        )
        return AppleMessagesDriveIngestSummary(
            batches_seen=len(batches),
            handles_written=len(handle_rows),
            chats_written=len(chat_rows),
            chat_handles_written=len(chat_handle_rows),
            messages_written=len(message_rows),
            chat_messages_written=len(chat_message_rows),
            attachments_written=len(attachment_rows),
            files_promoted=promoted,
        )

    def _promote_batch(self, batch: Mapping[str, Any]) -> int:
        return promote_batch(self._object_store_for_thread(), batch)

    def _promote_stored_files_parallel(self, batches: list[Mapping[str, Any]]) -> int:
        pairs: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
        for batch in batches:
            pairs.extend(stored_files_for_promotion(batch, library_batch_payload(batch)))
        with ThreadPoolExecutor(max_workers=self._promotion_workers, thread_name_prefix="apple-messages-promote") as executor:
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
            "source": "apple_messages",
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


def dedupe_rows(rows: list[dict[str, Any]], key_columns: tuple[str, ...]) -> list[dict[str, Any]]:
    by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        by_key[tuple(row.get(column) for column in key_columns)] = row
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


def record_to_handle_row(record: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    payload = record_payload(record)
    return {
        "account": record_account(record),
        "handle_id": str(payload.get("handle_id", "")),
        "handle_rowid": int(payload.get("handle_rowid", 0) or 0),
        "address": str(payload.get("address", "")),
        "country": str(payload.get("country", "")),
        "service": str(payload.get("service", "")),
        "uncanonicalized_id": str(payload.get("uncanonicalized_id", "")),
        "person_centric_id": str(payload.get("person_centric_id", "")),
        "raw_metadata_json": raw_json(payload),
        "ingested_at": ingested_at,
        "sync_version": sync_version(ingested_at),
    }


def record_to_chat_row(record: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    payload = record_payload(record)
    return {
        "account": record_account(record),
        "chat_id": str(payload.get("chat_id", "")),
        "chat_rowid": int(payload.get("chat_rowid", 0) or 0),
        "guid": str(payload.get("guid", "")),
        "chat_identifier": str(payload.get("chat_identifier", "")),
        "service_name": str(payload.get("service_name", "")),
        "display_name": str(payload.get("display_name", "")),
        "room_name": str(payload.get("room_name", "")),
        "account_login": str(payload.get("account_login", "")),
        "style": int(payload.get("style", 0) or 0),
        "state": int(payload.get("state", 0) or 0),
        "is_archived": int(bool(payload.get("is_archived", False))),
        "is_filtered": int(bool(payload.get("is_filtered", False))),
        "is_recovered": int(bool(payload.get("is_recovered", False))),
        "is_pending_review": int(bool(payload.get("is_pending_review", False))),
        "last_read_message_at": parse_datetime(str(payload.get("last_read_message_at", ""))),
        "raw_metadata_json": raw_json(payload),
        "ingested_at": ingested_at,
        "sync_version": sync_version(ingested_at),
    }


def record_to_chat_handle_row(record: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    payload = record_payload(record)
    return {
        "account": record_account(record),
        "chat_id": str(payload.get("chat_id", "")),
        "handle_id": str(payload.get("handle_id", "")),
        "raw_metadata_json": raw_json(payload),
        "ingested_at": ingested_at,
        "sync_version": sync_version(ingested_at),
    }


def record_to_message_row(record: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    payload = record_payload(record)
    if record_type(record) == "deleted_message":
        message_id = str(payload.get("message_id", ""))
        deleted_at = parse_datetime(str(payload.get("deleted_at", "")))
        return empty_message_row(
            account=record_account(record),
            message_id=message_id,
            message_at=deleted_at,
            raw_metadata_json=raw_json(payload),
            ingested_at=ingested_at,
            is_deleted=1,
        )
    row = empty_message_row(
        account=record_account(record),
        message_id=str(payload.get("message_id", "")),
        message_at=parse_datetime(str(payload.get("message_at", ""))),
        raw_metadata_json=raw_json(payload),
        ingested_at=ingested_at,
        is_deleted=int(bool(payload.get("is_deleted", False))),
    )
    row.update(
        {
            "message_rowid": int(payload.get("message_rowid", 0) or 0),
            "handle_id": str(payload.get("handle_id", "")),
            "service": str(payload.get("service", "")),
            "message_account": str(payload.get("message_account", "")),
            "body_text": str(payload.get("body_text", "")),
            "body_source": str(payload.get("body_source", "")),
            "body_decode_status": str(payload.get("body_decode_status", "")),
            "body_decode_error": str(payload.get("body_decode_error", "")),
            "attributed_body_sha256": str(payload.get("attributed_body_sha256", "")),
            "subject": str(payload.get("subject", "")),
            "country": str(payload.get("country", "")),
            "message_type": int(payload.get("message_type", 0) or 0),
            "message_item_type": int(payload.get("message_item_type", payload.get("item_type", 0)) or 0),
            "is_from_me": int(bool(payload.get("is_from_me", False))),
            "is_read": int(bool(payload.get("is_read", False))),
            "is_sent": int(bool(payload.get("is_sent", False))),
            "is_delivered": int(bool(payload.get("is_delivered", False))),
            "is_finished": int(bool(payload.get("is_finished", False))),
            "is_system_message": int(bool(payload.get("is_system_message", False))),
            "is_service_message": int(bool(payload.get("is_service_message", False))),
            "is_forward": int(bool(payload.get("is_forward", False))),
            "is_empty": int(bool(payload.get("is_empty", False))),
            "is_audio_message": int(bool(payload.get("is_audio_message", False))),
            "is_played": int(bool(payload.get("is_played", False))),
            "cache_has_attachments": int(bool(payload.get("cache_has_attachments", False))),
            "has_unseen_mention": int(bool(payload.get("has_unseen_mention", False))),
            "is_spam": int(bool(payload.get("is_spam", False))),
            "reply_to_guid": str(payload.get("reply_to_guid", "")),
            "associated_message_guid": str(payload.get("associated_message_guid", "")),
            "associated_message_type": int(payload.get("associated_message_type", 0) or 0),
            "associated_message_emoji": str(payload.get("associated_message_emoji", "")),
            "balloon_bundle_id": str(payload.get("balloon_bundle_id", "")),
            "group_title": str(payload.get("group_title", "")),
            "group_action_type": int(payload.get("group_action_type", 0) or 0),
            "message_action_type": int(payload.get("message_action_type", 0) or 0),
            "message_source": int(payload.get("message_source", 0) or 0),
            "expressive_send_style_id": str(payload.get("expressive_send_style_id", "")),
            "date_ns": int(payload.get("date_ns", 0) or 0),
            "date_read": parse_datetime(str(payload.get("date_read", ""))),
            "date_delivered": parse_datetime(str(payload.get("date_delivered", ""))),
            "date_played": parse_datetime(str(payload.get("date_played", ""))),
            "date_edited": parse_datetime(str(payload.get("date_edited", ""))),
            "date_retracted": parse_datetime(str(payload.get("date_retracted", ""))),
            "date_recovered": parse_datetime(str(payload.get("date_recovered", ""))),
        }
    )
    return row


def empty_message_row(
    *,
    account: str,
    message_id: str,
    message_at: datetime,
    raw_metadata_json: str,
    ingested_at: datetime,
    is_deleted: int,
) -> dict[str, Any]:
    return {
        "account": account,
        "message_id": message_id,
        "message_rowid": 0,
        "handle_id": "",
        "service": "",
        "message_account": "",
        "body_text": "",
        "body_source": "",
        "body_decode_status": "",
        "body_decode_error": "",
        "attributed_body_sha256": "",
        "subject": "",
        "country": "",
        "message_type": 0,
        "message_item_type": 0,
        "is_from_me": 0,
        "is_read": 0,
        "is_sent": 0,
        "is_delivered": 0,
        "is_finished": 0,
        "is_system_message": 0,
        "is_service_message": 0,
        "is_forward": 0,
        "is_empty": 0,
        "is_audio_message": 0,
        "is_played": 0,
        "cache_has_attachments": 0,
        "has_unseen_mention": 0,
        "is_spam": 0,
        "reply_to_guid": "",
        "associated_message_guid": "",
        "associated_message_type": 0,
        "associated_message_emoji": "",
        "balloon_bundle_id": "",
        "group_title": "",
        "group_action_type": 0,
        "message_action_type": 0,
        "message_source": 0,
        "expressive_send_style_id": "",
        "message_at": message_at,
        "date_ns": 0,
        "date_read": datetime.fromtimestamp(0, tz=UTC),
        "date_delivered": datetime.fromtimestamp(0, tz=UTC),
        "date_played": datetime.fromtimestamp(0, tz=UTC),
        "date_edited": datetime.fromtimestamp(0, tz=UTC),
        "date_retracted": datetime.fromtimestamp(0, tz=UTC),
        "date_recovered": datetime.fromtimestamp(0, tz=UTC),
        "is_deleted": is_deleted,
        "raw_metadata_json": raw_metadata_json,
        "ingested_at": ingested_at,
        "sync_version": sync_version(ingested_at),
    }


def record_to_chat_message_row(record: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    payload = record_payload(record)
    return {
        "account": record_account(record),
        "chat_id": str(payload.get("chat_id", "")),
        "message_id": str(payload.get("message_id", "")),
        "message_date": parse_datetime(str(payload.get("message_date", ""))),
        "message_date_ns": int(payload.get("message_date_ns", 0) or 0),
        "raw_metadata_json": raw_json(payload),
        "ingested_at": ingested_at,
        "sync_version": sync_version(ingested_at),
    }


def record_to_attachment_row(record: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    payload = record_payload(record)
    file = nested_mapping(payload, "file")
    return {
        "account": record_account(record),
        "attachment_id": str(payload.get("attachment_id", "")),
        "attachment_rowid": int(payload.get("attachment_rowid", 0) or 0),
        "message_id": str(payload.get("message_id", "")),
        "guid": str(payload.get("guid", "")),
        "original_guid": str(payload.get("original_guid", "")),
        "filename": str(payload.get("filename", "")),
        "transfer_name": str(payload.get("transfer_name", "")),
        "content_type": str(payload.get("content_type", "")),
        "uti": str(payload.get("uti", "")),
        "mime_type": str(payload.get("mime_type", "")),
        "total_bytes": int(payload.get("total_bytes", 0) or 0),
        "size_bytes": int(payload.get("size_bytes", 0) or 0),
        "content_sha256": str(payload.get("content_sha256", "")),
        "is_missing": int(bool(payload.get("is_missing", False))),
        "error": str(payload.get("error", "")),
        "is_outgoing": int(bool(payload.get("is_outgoing", False))),
        "is_sticker": int(bool(payload.get("is_sticker", False))),
        "hide_attachment": int(bool(payload.get("hide_attachment", False))),
        "transfer_state": int(payload.get("transfer_state", 0) or 0),
        "created_at": parse_datetime(str(payload.get("created_at", ""))),
        "start_at": parse_datetime(str(payload.get("start_at", ""))),
        "storage_backend": str(file.get("storage_backend", "")),
        "storage_key": str(file.get("storage_key", "")),
        "storage_file_id": str(file.get("storage_file_id", "")),
        "storage_url": str(file.get("storage_url", "")),
        "raw_metadata_json": raw_json(clean_attachment_payload(payload)),
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


def clean_attachment_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
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
