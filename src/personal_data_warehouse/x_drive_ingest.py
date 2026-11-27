"""Promote X (Twitter) batches from the Drive inbox into base_x.

The Mac uploader (``pdw ingest x``) reads Zach's own X account through the
web client's GraphQL API with the signed-in Chrome's session, and posts
gzipped JSONL batches to the app's /ingest/x/batch endpoint, which writes them
into the Drive inbox. A batch carries two record types: ``post`` (one tweet,
normalized by the uploader, with the API's result verbatim in ``raw``) and
``relation`` (why a post is archived: authored / liked / bookmarked /
mentioned). Each batch is written and promoted to the library on its own.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
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

SOURCE = "x"
OBJECT_PREFIX = "x"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox/"
LIBRARY_PREFIX = f"{OBJECT_PREFIX}/library/"
BATCH_KIND = "x_export_batch"
BLOB_KIND = "x_blob"

#: Why a post can be in the archive. Anything else is refused, so a renamed
#: stream in the uploader fails loudly instead of landing an unknown relation
#: the timeline's priority would file as noise.
RELATIONS = frozenset({"authored", "liked", "bookmarked", "mentioned"})


@dataclass(frozen=True)
class XDriveIngestSummary:
    batches_seen: int
    posts_written: int
    relations_written: int
    files_promoted: int


class XDriveIngestRunner:
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

    def sync(self) -> XDriveIngestSummary:
        self._warehouse.ensure_x_tables()
        batches = posts_written = relations_written = promoted = 0
        source = iter(self._batch_source())
        if self._max_batches:
            # islice stops BEFORE pulling the next batch, so the cap never
            # downloads a batch it will not write.
            source = islice(source, self._max_batches)
        for batch in source:
            batches += 1
            posts: dict[tuple[str, str], dict[str, Any]] = {}
            relations: dict[tuple[str, str, str], dict[str, Any]] = {}
            for envelope in batch_records(batch):
                record_type = str(envelope.get("record_type", ""))
                if record_type == "post":
                    row = record_to_post_row(envelope)
                    posts[(row["account"], row["post_id"])] = row
                elif record_type == "relation":
                    row = record_to_relation_row(envelope)
                    relations[(row["account"], row["post_id"], row["relation"])] = row
            # Posts first: a relation names a post the same batch carries.
            self._warehouse.insert_x_posts(list(posts.values()))
            self._warehouse.insert_x_user_posts(list(relations.values()))
            posts_written += len(posts)
            relations_written += len(relations)
            if self._object_store is not None:
                ref = nested_mapping(batch, "batch_file")
                target_key = library_storage_key(str(ref.get("storage_key", "")))
                if ref.get("storage_file_id") and target_key:
                    self._object_store.move_object(ref, new_object_key=target_key)
                    promoted += 1
        self._logger.info(
            "Ingested %s X batches: %s posts and %s relations", batches, posts_written, relations_written
        )
        return XDriveIngestSummary(
            batches_seen=batches,
            posts_written=posts_written,
            relations_written=relations_written,
            files_promoted=promoted,
        )


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


def _account(envelope: Mapping[str, Any]) -> str:
    # X handles are case-insensitive; the timeline compares lowercased.
    account = str(envelope.get("account", "")).strip().lower()
    if not account:
        raise ValueError("X record has no account")
    return account


def _versioning(envelope: Mapping[str, Any]) -> dict[str, Any]:
    exported_at = parse_datetime(envelope.get("exported_at"))
    return {
        "exported_at": exported_at,
        "synced_at": exported_at,
        "sync_version": int(exported_at.timestamp() * 1_000_000),
    }


def record_to_post_row(envelope: Mapping[str, Any]) -> dict[str, Any]:
    record = nested_mapping(envelope, "record")
    post_id = str(record.get("post_id", "")).strip()
    if not post_id:
        raise ValueError("X post record has no post_id")
    raw = record.get("raw")
    text_fields = (
        "author_user_id",
        "author_screen_name",
        "author_name",
        "text",
        "lang",
        "conversation_id",
        "in_reply_to_post_id",
        "in_reply_to_user_id",
        "in_reply_to_screen_name",
        "quoted_post_id",
        "retweeted_post_id",
        "url",
    )
    count_fields = (
        "reply_count",
        "retweet_count",
        "like_count",
        "quote_count",
        "bookmark_count",
        "view_count",
    )
    return {
        "account": _account(envelope),
        "post_id": post_id,
        **{field: str(record.get(field, "") or "") for field in text_fields},
        "created_at": parse_datetime(record.get("created_at")),
        "is_retweet": int(bool(record.get("is_retweet"))),
        "is_quote": int(bool(record.get("is_quote"))),
        "mentions_account": int(bool(record.get("mentions_account"))),
        **{field: int_value(record.get(field)) for field in count_fields},
        "raw_json": dict(raw) if isinstance(raw, Mapping) else {},
        **_versioning(envelope),
    }


def record_to_relation_row(envelope: Mapping[str, Any]) -> dict[str, Any]:
    record = nested_mapping(envelope, "record")
    post_id = str(record.get("post_id", "")).strip()
    relation = str(record.get("relation", "")).strip()
    if not post_id:
        raise ValueError("X relation record has no post_id")
    if relation not in RELATIONS:
        raise ValueError(f"unknown X relation {relation!r}; expected one of {sorted(RELATIONS)}")
    sort_index = record.get("sort_index")
    return {
        "account": _account(envelope),
        "post_id": post_id,
        "relation": relation,
        "discovered_at": parse_datetime(record.get("discovered_at")),
        # X's sortIndex is a 64-bit integer; never round-trip it through float.
        "sort_index": _exact_int(sort_index),
        **_versioning(envelope),
    }


def _exact_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    text = str(value or "").strip()
    if text.lstrip("-").isdigit():
        return int(text)
    return int_value(value)


def library_storage_key(storage_key: str) -> str:
    if storage_key.startswith(INBOX_PREFIX):
        return LIBRARY_PREFIX + storage_key[len(INBOX_PREFIX) :]
    return storage_key
