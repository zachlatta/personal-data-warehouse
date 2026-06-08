from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import re
import threading
from typing import Any

from personal_data_warehouse.apple_voice_memos_drive_ingest import parse_datetime
from personal_data_warehouse.objectstore import ObjectListing, ObjectStore


@dataclass(frozen=True)
class AppleNotesDriveIngestSummary:
    metadata_seen: int
    notes_written: int
    revisions_written: int
    attachments_written: int
    files_promoted: int


OBJECT_PREFIX = "apple-notes"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox/"
LIBRARY_PREFIX = f"{OBJECT_PREFIX}/library/"
METADATA_KIND = "apple_note_revision_metadata"
HTML_KIND = "apple_note_body_html"
ATTACHMENT_KIND = "apple_note_attachment"


class AppleNotesDriveIngestRunner:
    def __init__(
        self,
        *,
        warehouse,
        metadata_source: Callable[[], Iterable[Mapping[str, Any]]],
        object_store: ObjectStore | None = None,
        object_store_factory: Callable[[], ObjectStore] | None = None,
        promotion_workers: int = 1,
        logger,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if object_store is not None and object_store_factory is not None:
            raise ValueError("pass only one of object_store or object_store_factory")
        self._warehouse = warehouse
        self._metadata_source = metadata_source
        self._object_store = object_store
        self._object_store_factory = object_store_factory
        self._promotion_workers = max(1, promotion_workers)
        self._thread_local = threading.local()
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync(self) -> AppleNotesDriveIngestSummary:
        self._warehouse.ensure_apple_notes_tables()
        ingested_at = self._now()
        metadata = list(self._metadata_source())
        promote = self._object_store is not None or self._object_store_factory is not None
        row_metadata = [library_metadata_payload(payload) for payload in metadata] if promote else metadata
        latest_note_metadata = latest_metadata_payloads(row_metadata)
        note_rows = [metadata_to_note_row(payload, ingested_at=ingested_at) for payload in latest_note_metadata]
        revision_rows = [metadata_to_revision_row(payload, ingested_at=ingested_at) for payload in row_metadata]
        attachment_rows = [
            row
            for payload in row_metadata
            for row in metadata_to_attachment_rows(payload, ingested_at=ingested_at)
        ]
        self._warehouse.insert_apple_notes(note_rows)
        self._warehouse.insert_apple_note_revisions(revision_rows)
        self._warehouse.insert_apple_note_attachments(attachment_rows)
        promoted = 0
        if promote:
            self._logger.info(
                "Promoting %s Apple Notes Drive metadata payloads with %s worker(s)",
                len(metadata),
                self._promotion_workers,
            )
            if self._promotion_workers == 1 or len(metadata) <= 1:
                promoted = sum(self._promote_payload(payload) for payload in metadata)
            else:
                with ThreadPoolExecutor(max_workers=self._promotion_workers, thread_name_prefix="apple-notes-promote") as executor:
                    futures = [executor.submit(self._promote_payload, payload) for payload in metadata]
                    promoted = sum(future.result() for future in as_completed(futures))
        self._logger.info(
            "Ingested %s Apple Notes revisions and %s attachments",
            len(revision_rows),
            len(attachment_rows),
        )
        return AppleNotesDriveIngestSummary(
            metadata_seen=len(metadata),
            notes_written=len(note_rows),
            revisions_written=len(revision_rows),
            attachments_written=len(attachment_rows),
            files_promoted=promoted,
        )

    def _promote_payload(self, payload: Mapping[str, Any]) -> int:
        return promote_payload(self._object_store_for_thread(), payload)

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


def promote_payload(object_store: ObjectStore, payload: Mapping[str, Any]) -> int:
    promoted = 0
    library_payload = library_metadata_payload(payload)
    for source, target in stored_files_for_promotion(payload, library_payload):
        file_id = str(source.get("storage_file_id", ""))
        target_key = str(target.get("storage_key", ""))
        if not file_id or not target_key:
            continue
        object_store.move_object(source, new_object_key=target_key)
        promoted += 1
    return promoted


def iter_metadata_payloads(
    *,
    object_store: ObjectStore,
    stage: str = "inbox",
    object_store_factory: Callable[[], ObjectStore] | None = None,
    download_workers: int = 1,
) -> Iterable[Mapping[str, Any]]:
    metadata_listings = object_store.list_objects(kind=METADATA_KIND, stage=stage)
    html_by_revision = {
        revision_key(listing): listing
        for listing in object_store.list_objects(kind=HTML_KIND, stage=stage)
    }
    attachment_by_revision: dict[tuple[str, str, str], ObjectListing] = {}
    attachment_by_sha: dict[str, ObjectListing] = {}
    for listing in object_store.list_objects(kind=ATTACHMENT_KIND, stage=""):
        properties = listing.app_properties
        note_id = str(properties.get("note_id", ""))
        revision_id = str(properties.get("revision_id", ""))
        attachment_id = str(properties.get("attachment_id", ""))
        content_sha256 = str(properties.get("content_sha256", ""))
        if note_id and revision_id and attachment_id:
            attachment_by_revision[(note_id, revision_id, attachment_id)] = listing
        if content_sha256:
            attachment_by_sha.setdefault(content_sha256, listing)

    download_workers = max(1, download_workers)
    if download_workers == 1 or len(metadata_listings) <= 1:
        for listing in metadata_listings:
            yield attach_metadata_listing(
                listing=listing,
                object_store=object_store,
                stage=stage,
                html_by_revision=html_by_revision,
                attachment_by_revision=attachment_by_revision,
                attachment_by_sha=attachment_by_sha,
            )
        return

    thread_local = threading.local()

    def store_for_thread() -> ObjectStore:
        if object_store_factory is None:
            return object_store
        store = getattr(thread_local, "object_store", None)
        if store is None:
            store = object_store_factory()
            thread_local.object_store = store
        return store

    def download(listing: ObjectListing) -> Mapping[str, Any]:
        return attach_metadata_listing(
            listing=listing,
            object_store=store_for_thread(),
            stage=stage,
            html_by_revision=html_by_revision,
            attachment_by_revision=attachment_by_revision,
            attachment_by_sha=attachment_by_sha,
        )

    with ThreadPoolExecutor(max_workers=download_workers, thread_name_prefix="apple-notes-download") as executor:
        futures = [executor.submit(download, listing) for listing in metadata_listings]
        for future in as_completed(futures):
            yield future.result()


def attach_metadata_listing(
    *,
    listing: ObjectListing,
    object_store: ObjectStore,
    stage: str,
    html_by_revision: Mapping[tuple[str, str], ObjectListing],
    attachment_by_revision: Mapping[tuple[str, str, str], ObjectListing],
    attachment_by_sha: Mapping[str, ObjectListing],
) -> Mapping[str, Any]:
    payload = dict(load_json_object(object_store, listing.ref))
    return attach_storage_context(
        payload,
        stage=stage,
        metadata_listing=listing,
        html_by_revision=html_by_revision,
        attachment_by_revision=attachment_by_revision,
        attachment_by_sha=attachment_by_sha,
    )


def has_metadata_payloads(*, object_store: ObjectStore, stage: str = "inbox") -> bool:
    return object_store.find_object(kind=METADATA_KIND, stage=stage) is not None


def load_json_object(object_store: ObjectStore, ref: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = json.loads(object_store.get_object(ref).decode("utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("Stored object did not contain a JSON object")
    return payload


def revision_key(listing: ObjectListing) -> tuple[str, str]:
    properties = listing.app_properties
    return (str(properties.get("note_id", "")), str(properties.get("revision_id", "")))


def attach_storage_context(
    payload: Mapping[str, Any],
    *,
    stage: str,
    metadata_listing: ObjectListing,
    html_by_revision: Mapping[tuple[str, str], ObjectListing],
    attachment_by_revision: Mapping[tuple[str, str, str], ObjectListing],
    attachment_by_sha: Mapping[str, ObjectListing],
) -> dict[str, Any]:
    decorated = clean_metadata_payload(payload)
    note = nested_mapping(decorated, "note")
    revision_id = str(note.get("revision_id", ""))
    note_id = str(note.get("note_id", ""))
    decorated["metadata_file"] = stored_file_context(
        storage_key=metadata_storage_key(decorated, stage=stage),
        listing=metadata_listing,
        extra={"content_sha256": str(metadata_listing.app_properties.get("content_sha256", ""))},
    )
    if not bool(note.get("is_deleted", False)):
        html_listing = html_by_revision.get((note_id, revision_id))
        if html_listing is not None:
            decorated["html_file"] = stored_file_context(
                storage_key=html_storage_key(decorated, stage=stage),
                listing=html_listing,
                extra={"content_sha256": str(html_listing.app_properties.get("content_sha256", ""))},
            )
    attachments: list[dict[str, Any]] = []
    for raw_attachment in note.get("attachments", []):
        if not isinstance(raw_attachment, Mapping):
            continue
        attachment = deepcopy(dict(raw_attachment))
        attachment_id = str(attachment.get("attachment_id", ""))
        content_sha256 = str(attachment.get("content_sha256", ""))
        listing = attachment_by_revision.get((note_id, revision_id, attachment_id))
        if listing is None and content_sha256:
            listing = attachment_by_sha.get(content_sha256)
        if listing is not None:
            attachment["file"] = stored_file_context(
                storage_key=attachment_storage_key(decorated, attachment, stage=stage),
                listing=listing,
            )
        attachments.append(attachment)
    note_copy = dict(note)
    note_copy["attachments"] = attachments
    decorated["note"] = note_copy
    return decorated


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


def clean_metadata_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    clean = deepcopy(dict(payload))
    clean.pop("metadata_file", None)
    clean.pop("html_file", None)
    note = clean.get("note")
    if isinstance(note, dict):
        clean_note = deepcopy(note)
        attachments = []
        for raw_attachment in clean_note.get("attachments", []):
            if isinstance(raw_attachment, dict):
                attachment = deepcopy(raw_attachment)
                attachment.pop("file", None)
                attachments.append(attachment)
        clean_note["attachments"] = attachments
        clean["note"] = clean_note
    return clean


def library_metadata_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    promoted = deepcopy(dict(payload))
    for key in ("metadata_file", "html_file"):
        stored_file = promoted.get(key)
        if isinstance(stored_file, dict):
            stored_file["storage_key"] = library_storage_key(str(stored_file.get("storage_key", "")))
    note = promoted.get("note")
    if isinstance(note, dict):
        for raw_attachment in note.get("attachments", []):
            if isinstance(raw_attachment, dict):
                stored_file = raw_attachment.get("file")
                if isinstance(stored_file, dict):
                    stored_file["storage_key"] = library_storage_key(str(stored_file.get("storage_key", "")))
    return promoted


def stored_files_for_promotion(
    payload: Mapping[str, Any],
    library_payload: Mapping[str, Any],
) -> list[tuple[Mapping[str, Any], Mapping[str, Any]]]:
    pairs: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    for key in ("metadata_file", "html_file"):
        source = nested_mapping(payload, key)
        target = nested_mapping(library_payload, key)
        if source and target:
            pairs.append((source, target))
    source_note = nested_mapping(payload, "note")
    target_note = nested_mapping(library_payload, "note")
    source_attachments = source_note.get("attachments", [])
    target_attachments = target_note.get("attachments", [])
    if isinstance(source_attachments, list) and isinstance(target_attachments, list):
        for source_attachment, target_attachment in zip(source_attachments, target_attachments, strict=False):
            if isinstance(source_attachment, Mapping) and isinstance(target_attachment, Mapping):
                source_file = nested_mapping(source_attachment, "file")
                target_file = nested_mapping(target_attachment, "file")
                if source_file and target_file:
                    pairs.append((source_file, target_file))
    return pairs


def metadata_to_note_row(metadata: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    note = nested_mapping(metadata, "note")
    row = _base_note_row(metadata, ingested_at=ingested_at)
    row["latest_revision_id"] = str(note.get("revision_id", ""))
    return row


def metadata_to_revision_row(metadata: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    note = nested_mapping(metadata, "note")
    row = _base_note_row(metadata, ingested_at=ingested_at)
    row.pop("latest_revision_id", None)
    row["revision_id"] = str(note.get("revision_id", ""))
    row["exported_at"] = parse_datetime(str(metadata.get("exported_at", "")))
    return row


def _base_note_row(metadata: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    note = nested_mapping(metadata, "note")
    metadata_file = nested_mapping(metadata, "metadata_file")
    html_file = nested_mapping(metadata, "html_file")
    return {
        "account": str(metadata.get("account", "")),
        "note_id": str(note.get("note_id", "")),
        "latest_revision_id": str(note.get("revision_id", "")),
        "title": str(note.get("title", "")),
        "folder_id": str(note.get("folder_id", "")),
        "folder_path": str(note.get("folder_path", "")),
        "apple_account_id": str(note.get("apple_account_id", "")),
        "apple_account_name": str(note.get("apple_account_name", "")),
        "created_at": parse_datetime(str(note.get("created_at", ""))),
        "modified_at": parse_datetime(str(note.get("modified_at", ""))),
        "body_text": str(note.get("body_text", "")),
        "body_html": str(note.get("body_html", "")),
        "body_markdown": str(note.get("body_markdown", "")),
        "content_sha256": str(note.get("content_sha256", "")),
        "attachments_json": json.dumps(clean_attachments(note), sort_keys=True, separators=(",", ":")),
        "storage_backend": str(metadata_file.get("storage_backend", "")),
        "metadata_storage_key": str(metadata_file.get("storage_key", "")),
        "metadata_storage_file_id": str(metadata_file.get("storage_file_id", "")),
        "metadata_storage_url": str(metadata_file.get("storage_url", "")),
        "metadata_content_sha256": str(metadata_file.get("content_sha256", "")),
        "html_storage_key": str(html_file.get("storage_key", "")),
        "html_storage_file_id": str(html_file.get("storage_file_id", "")),
        "html_storage_url": str(html_file.get("storage_url", "")),
        "html_content_sha256": str(html_file.get("content_sha256", "")),
        "is_deleted": 1 if bool(note.get("is_deleted", False)) else 0,
        "raw_metadata_json": json.dumps(clean_metadata_payload(metadata), sort_keys=True, separators=(",", ":")),
        "ingested_at": ingested_at,
        "sync_version": int(ingested_at.timestamp() * 1_000_000),
    }


def metadata_to_attachment_rows(metadata: Mapping[str, Any], *, ingested_at: datetime) -> list[dict[str, Any]]:
    note = nested_mapping(metadata, "note")
    rows: list[dict[str, Any]] = []
    for raw_attachment in note.get("attachments", []):
        if not isinstance(raw_attachment, Mapping):
            continue
        file = nested_mapping(raw_attachment, "file")
        clean = deepcopy(dict(raw_attachment))
        clean.pop("file", None)
        rows.append(
            {
                "account": str(metadata.get("account", "")),
                "note_id": str(note.get("note_id", "")),
                "revision_id": str(note.get("revision_id", "")),
                "attachment_id": str(raw_attachment.get("attachment_id", "")),
                "filename": str(raw_attachment.get("filename", "")),
                "content_type": str(raw_attachment.get("content_type", "")),
                "size_bytes": int(raw_attachment.get("size_bytes", 0) or 0),
                "content_sha256": str(raw_attachment.get("content_sha256", "")),
                "is_missing": 1 if bool(raw_attachment.get("is_missing", False)) else 0,
                "error": str(raw_attachment.get("error", "")),
                "storage_backend": str(file.get("storage_backend", "")),
                "storage_key": str(file.get("storage_key", "")),
                "storage_file_id": str(file.get("storage_file_id", "")),
                "storage_url": str(file.get("storage_url", "")),
                "raw_metadata_json": json.dumps(clean, sort_keys=True, separators=(",", ":")),
                "ingested_at": ingested_at,
                "sync_version": int(ingested_at.timestamp() * 1_000_000),
            }
        )
    return rows


def latest_metadata_payloads(metadata: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    latest_by_note: dict[tuple[str, str], Mapping[str, Any]] = {}
    for payload in metadata:
        note = nested_mapping(payload, "note")
        key = (str(payload.get("account", "")), str(note.get("note_id", "")))
        if not key[0] or not key[1]:
            continue
        current = latest_by_note.get(key)
        if current is None or metadata_latest_sort_key(payload) >= metadata_latest_sort_key(current):
            latest_by_note[key] = payload
    return list(latest_by_note.values())


def metadata_latest_sort_key(metadata: Mapping[str, Any]) -> tuple[datetime, datetime, str]:
    note = nested_mapping(metadata, "note")
    return (
        parse_datetime(str(note.get("modified_at", ""))),
        parse_datetime(str(metadata.get("exported_at", ""))),
        str(note.get("revision_id", "")),
    )


def clean_attachments(note: Mapping[str, Any]) -> list[dict[str, Any]]:
    attachments = []
    for raw_attachment in note.get("attachments", []):
        if isinstance(raw_attachment, dict):
            attachment = deepcopy(raw_attachment)
            attachment.pop("file", None)
            attachments.append(attachment)
    return attachments


def metadata_storage_key(payload: Mapping[str, Any], *, stage: str) -> str:
    return object_storage_key(payload, stage=stage, extension=".json")


def html_storage_key(payload: Mapping[str, Any], *, stage: str) -> str:
    return object_storage_key(payload, stage=stage, extension=".html")


def attachment_storage_key(payload: Mapping[str, Any], attachment: Mapping[str, Any], *, stage: str) -> str:
    note = nested_mapping(payload, "note")
    modified_at = parse_datetime(str(note.get("modified_at", "")))
    note_id = safe_object_key_part(str(note.get("note_id", "")))
    revision_id = str(note.get("revision_id", ""))
    attachment_id = safe_object_key_part(str(attachment.get("attachment_id", "")))
    content_sha256 = str(attachment.get("content_sha256", ""))
    suffix = extension_from_filename(str(attachment.get("filename", "")))
    return (
        f"{OBJECT_PREFIX}/{stage}/{modified_at.year:04d}/{modified_at.month:02d}/{note_id}/{revision_id}/"
        f"attachments/{attachment_id}-{content_sha256}{suffix}"
    )


def object_storage_key(payload: Mapping[str, Any], *, stage: str, extension: str) -> str:
    note = nested_mapping(payload, "note")
    modified_at = parse_datetime(str(note.get("modified_at", "")))
    note_id = safe_object_key_part(str(note.get("note_id", "")))
    revision_id = str(note.get("revision_id", ""))
    return f"{OBJECT_PREFIX}/{stage}/{modified_at.year:04d}/{modified_at.month:02d}/{note_id}/{revision_id}{extension}"


def library_storage_key(storage_key: str) -> str:
    if storage_key.startswith(INBOX_PREFIX):
        return f"{LIBRARY_PREFIX}{storage_key[len(INBOX_PREFIX):]}"
    return storage_key


def nested_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    nested = value.get(key)
    return nested if isinstance(nested, Mapping) else {}


def safe_object_key_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip(".-")[:120] or "untitled"


def extension_from_filename(filename: str) -> str:
    suffix = filename.rsplit(".", 1)
    if len(suffix) == 2 and suffix[1]:
        return "." + suffix[1]
    return ".bin"
