from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from typing import Any

from personal_data_warehouse.objectstore import (
    ObjectListing,
    ObjectStore,
    stored_object_from_mapping,
)


@dataclass(frozen=True)
class VoiceMemosDriveIngestSummary:
    metadata_seen: int
    rows_written: int
    recordings_promoted: int


INBOX_PREFIX = "apple-voice-memos/inbox/"
LIBRARY_PREFIX = "apple-voice-memos/library/"
METADATA_KIND = "voice_memo_metadata"
AUDIO_KIND = "voice_memo_audio"


class VoiceMemosDriveIngestRunner:
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

    def sync(self) -> VoiceMemosDriveIngestSummary:
        self._warehouse.ensure_apple_voice_memos_tables()
        ingested_at = self._now()
        metadata = list(self._metadata_source())
        promote = self._object_store is not None
        row_metadata = [library_metadata_payload(payload) for payload in metadata] if promote else metadata
        rows = [metadata_to_row(payload, ingested_at=ingested_at) for payload in row_metadata]
        self._warehouse.insert_apple_voice_memos_files(rows)
        promoted = 0
        if promote:
            for payload in metadata:
                promote_payload(self._object_store, payload)
                promoted += 1
        self._logger.info("Ingested %s Voice Memos metadata", len(rows))
        return VoiceMemosDriveIngestSummary(
            metadata_seen=len(metadata),
            rows_written=len(rows),
            recordings_promoted=promoted,
        )


def iter_metadata_payloads(
    *,
    object_store: ObjectStore,
    stage: str = "inbox",
) -> Iterable[Mapping[str, Any]]:
    for listing in object_store.list_objects(kind=METADATA_KIND, stage=stage):
        payload = dict(load_json_object(object_store, listing.ref))
        audio_listing = find_audio_listing(object_store=object_store, payload=payload, stage=stage)
        yield attach_storage_context(
            payload,
            stage=stage,
            metadata_listing=listing,
            audio_listing=audio_listing,
        )


def has_metadata_payloads(*, object_store: ObjectStore, stage: str = "inbox") -> bool:
    return object_store.find_object(kind=METADATA_KIND, stage=stage) is not None


def find_audio_listing(
    *,
    object_store: ObjectStore,
    payload: Mapping[str, Any],
    stage: str,
) -> ObjectListing:
    recording = nested_mapping(payload, "recording")
    content_sha256 = str(recording.get("content_sha256", ""))
    if content_sha256:
        listing = object_store.find_object(
            kind=AUDIO_KIND,
            stage=stage,
            properties={"content_sha256": content_sha256},
        )
        if listing is not None:
            return listing
    legacy_audio_file = nested_mapping(payload, "audio_file")
    if legacy_audio_file:
        return listing_from_stored_object(legacy_audio_file)
    raise ValueError(f"Could not find audio object for Voice Memo content_sha256={content_sha256}")


def load_json_object(object_store: ObjectStore, ref: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = json.loads(object_store.get_object(ref).decode("utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("Stored object did not contain a JSON object")
    return payload


def listing_from_stored_object(stored_object: Mapping[str, Any]) -> ObjectListing:
    return ObjectListing(ref=stored_object_from_mapping(stored_object), app_properties={}, filename="")


def attach_storage_context(
    payload: Mapping[str, Any],
    *,
    stage: str,
    metadata_listing: ObjectListing,
    audio_listing: ObjectListing,
) -> dict[str, Any]:
    decorated = clean_metadata_payload(payload)
    decorated["audio_file"] = stored_file_context(
        storage_key=audio_storage_key(payload, stage=stage),
        listing=audio_listing,
    )
    decorated["metadata_file"] = stored_file_context(
        storage_key=metadata_storage_key(payload, stage=stage),
        listing=metadata_listing,
        extra={"content_sha256": str(metadata_listing.app_properties.get("content_sha256", ""))},
    )
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


def promote_payload(object_store: ObjectStore, payload: Mapping[str, Any]) -> int:
    library_payload = library_metadata_payload(payload)
    promoted = 0
    for source, target in stored_files_for_promotion(payload, library_payload):
        file_id = str(source.get("storage_file_id", ""))
        target_key = str(target.get("storage_key", ""))
        if not file_id or not target_key:
            raise ValueError("Voice Memos metadata is missing a storage file id or target storage key")
        object_store.move_object(source, new_object_key=target_key)
        promoted += 1
    return promoted


def stored_files_for_promotion(
    payload: Mapping[str, Any],
    library_payload: Mapping[str, Any],
) -> list[tuple[Mapping[str, Any], Mapping[str, Any]]]:
    pairs: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    for key in ("audio_file", "metadata_file"):
        source = nested_mapping(payload, key)
        target = nested_mapping(library_payload, key)
        if source and target:
            pairs.append((source, target))
    return pairs


def library_metadata_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    promoted = deepcopy(dict(payload))
    for key in ("audio_file", "metadata_file"):
        stored_file = promoted.get(key)
        if isinstance(stored_file, dict):
            stored_file["storage_key"] = library_storage_key(str(stored_file.get("storage_key", "")))
    return promoted


def clean_metadata_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    clean = deepcopy(dict(payload))
    clean.pop("audio_file", None)
    clean.pop("metadata_file", None)
    return clean


def library_storage_key(storage_key: str) -> str:
    if storage_key.startswith(INBOX_PREFIX):
        return f"{LIBRARY_PREFIX}{storage_key[len(INBOX_PREFIX):]}"
    return storage_key


def audio_storage_key(payload: Mapping[str, Any], *, stage: str) -> str:
    recording = nested_mapping(payload, "recording")
    extension = str(recording.get("extension", ""))
    return object_storage_key(payload, stage=stage, extension=extension)


def metadata_storage_key(payload: Mapping[str, Any], *, stage: str) -> str:
    return object_storage_key(payload, stage=stage, extension=".json")


def object_storage_key(payload: Mapping[str, Any], *, stage: str, extension: str) -> str:
    recording = nested_mapping(payload, "recording")
    recorded_at = parse_datetime(str(recording.get("recorded_at", "")))
    content_sha256 = str(recording.get("content_sha256", ""))
    normalized_extension = extension if extension.startswith(".") else f".{extension}"
    return (
        f"apple-voice-memos/{stage}/{recorded_at.year:04d}/{recorded_at.month:02d}/"
        f"{recorded_at.date().isoformat()}-{content_sha256}{normalized_extension}"
    )


def metadata_to_row(metadata: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    recording = nested_mapping(metadata, "recording")
    audio_file = nested_mapping(metadata, "audio_file")
    metadata_file = nested_mapping(metadata, "metadata_file")
    return {
        "account": str(metadata.get("account", "")),
        "recording_id": str(recording.get("recording_id", "")),
        "title": str(recording.get("title", "")),
        "original_path": str(recording.get("original_path", "")),
        "filename": str(recording.get("filename", "")),
        "extension": str(recording.get("extension", "")),
        "content_type": str(recording.get("content_type", "")),
        "size_bytes": int(recording.get("size_bytes", 0) or 0),
        "content_sha256": str(recording.get("content_sha256", "")),
        "file_created_at": parse_datetime(str(recording.get("file_created_at", ""))),
        "file_modified_at": parse_datetime(str(recording.get("file_modified_at", ""))),
        "recorded_at": parse_datetime(str(recording.get("recorded_at", ""))),
        "storage_backend": str(audio_file.get("storage_backend", "")),
        "storage_key": str(audio_file.get("storage_key", "")),
        "storage_file_id": str(audio_file.get("storage_file_id", "")),
        "storage_url": str(audio_file.get("storage_url", "")),
        "metadata_storage_key": str(metadata_file.get("storage_key", "")),
        "metadata_storage_file_id": str(metadata_file.get("storage_file_id", "")),
        "metadata_storage_url": str(metadata_file.get("storage_url", "")),
        "metadata_content_sha256": str(metadata_file.get("content_sha256", "")),
        "is_deleted": 0,
        "raw_metadata_json": json.dumps(clean_metadata_payload(metadata), sort_keys=True, separators=(",", ":")),
        "ingested_at": ingested_at,
        "sync_version": int(ingested_at.timestamp() * 1_000_000),
    }


def nested_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    nested = value.get(key)
    return nested if isinstance(nested, Mapping) else {}


def parse_datetime(value: str) -> datetime:
    if not value:
        return datetime.fromtimestamp(0, tz=UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
