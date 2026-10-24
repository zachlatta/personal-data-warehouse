"""Promote Google Recorder uploads from the Drive inbox into base_google_recorder.

The Pixel's uploader (`pdw ingest google-recorder`, run as root on the phone)
reads Recorder's own database and posts each finished recording's audio and a
JSON sidecar to the app, which files both under ``google-recorder/inbox/``.
This reader pairs each sidecar with its audio by content sha, writes the row
with the library keys, then moves both objects into ``google-recorder/library/``
-- the Apple Voice Memos shape, for the same reason: the inbox is the durable
queue, and a run that dies between the insert and the move is redone onto the
same row.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from typing import Any

from personal_data_warehouse.apple_voice_memos_drive_ingest import (
    clean_metadata_payload,
    load_json_object,
    nested_mapping,
    parse_datetime,
    stored_file_context,
)
from personal_data_warehouse.objectstore import ObjectListing, ObjectStore

SOURCE = "google_recorder"
INBOX_PREFIX = "google-recorder/inbox/"
LIBRARY_PREFIX = "google-recorder/library/"
METADATA_KIND = "google_recorder_metadata"
AUDIO_KIND = "google_recorder_audio"


@dataclass(frozen=True)
class GoogleRecorderDriveIngestSummary:
    metadata_seen: int
    rows_written: int
    recordings_promoted: int


class GoogleRecorderDriveIngestRunner:
    def __init__(
        self,
        *,
        warehouse,
        metadata_source: Callable[[], Iterable[Mapping[str, Any]]],
        object_store: ObjectStore,
        logger,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._metadata_source = metadata_source
        self._object_store = object_store
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync(self) -> GoogleRecorderDriveIngestSummary:
        self._warehouse.ensure_google_recorder_tables()
        ingested_at = self._now()
        metadata = list(self._metadata_source())
        rows = [metadata_to_row(library_metadata_payload(payload), ingested_at=ingested_at) for payload in metadata]
        self._warehouse.insert_google_recorder_recordings(rows)
        for payload in metadata:
            promote_payload(self._object_store, payload)
        self._logger.info("Ingested %s Google Recorder sidecars", len(rows))
        return GoogleRecorderDriveIngestSummary(
            metadata_seen=len(metadata),
            rows_written=len(rows),
            recordings_promoted=len(metadata),
        )


def iter_metadata_payloads(*, object_store: ObjectStore, stage: str = "inbox") -> Iterable[Mapping[str, Any]]:
    for listing in object_store.list_objects(kind=METADATA_KIND, stage=stage):
        payload = dict(load_json_object(object_store, listing.ref))
        audio_listing = find_audio_listing(object_store=object_store, payload=payload, stage=stage)
        decorated = clean_metadata_payload(payload)
        decorated["audio_file"] = stored_file_context(
            storage_key=str(audio_listing.ref.get("storage_key", "")) or object_storage_key(payload, stage=stage),
            listing=audio_listing,
        )
        decorated["metadata_file"] = stored_file_context(
            storage_key=str(listing.ref.get("storage_key", "")) or object_storage_key(payload, stage=stage, extension=".json"),
            listing=listing,
            extra={"content_sha256": str(listing.app_properties.get("content_sha256", ""))},
        )
        yield decorated


def has_metadata_payloads(*, object_store: ObjectStore, stage: str = "inbox") -> bool:
    return object_store.find_object(kind=METADATA_KIND, stage=stage) is not None


def find_audio_listing(*, object_store: ObjectStore, payload: Mapping[str, Any], stage: str) -> ObjectListing:
    content_sha256 = str(nested_mapping(payload, "recording").get("content_sha256", ""))
    if content_sha256:
        # A rename in Recorder re-sends only the sidecar, so an inbox sidecar
        # may describe audio that was promoted to the library long ago.
        stages = (stage, "library") if stage == "inbox" else (stage,)
        for audio_stage in stages:
            listing = object_store.find_object(
                kind=AUDIO_KIND,
                stage=audio_stage,
                properties={"content_sha256": content_sha256},
            )
            if listing is not None:
                return listing
    raise ValueError(f"Could not find audio object for Google Recorder content_sha256={content_sha256}")


def promote_payload(object_store: ObjectStore, payload: Mapping[str, Any]) -> None:
    library_payload = library_metadata_payload(payload)
    for key in ("audio_file", "metadata_file"):
        source = nested_mapping(payload, key)
        target_key = str(nested_mapping(library_payload, key).get("storage_key", ""))
        if not source.get("storage_file_id") or not target_key:
            raise ValueError("Google Recorder metadata is missing a storage file id or target storage key")
        if str(source.get("storage_key", "")) == target_key:
            continue
        object_store.move_object(source, new_object_key=target_key)


def library_metadata_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    promoted = deepcopy(dict(payload))
    for key in ("audio_file", "metadata_file"):
        stored_file = promoted.get(key)
        if isinstance(stored_file, dict):
            stored_file["storage_key"] = library_storage_key(str(stored_file.get("storage_key", "")))
    return promoted


def library_storage_key(storage_key: str) -> str:
    if storage_key.startswith(INBOX_PREFIX):
        return f"{LIBRARY_PREFIX}{storage_key[len(INBOX_PREFIX):]}"
    return storage_key


def object_storage_key(payload: Mapping[str, Any], *, stage: str, extension: str | None = None) -> str:
    """The key the app files an object under (app/internal/server/ingest.go)."""
    recording = nested_mapping(payload, "recording")
    recorded_at = parse_datetime(str(recording.get("recorded_at", "")))
    suffix = extension if extension is not None else str(recording.get("extension", ""))
    if suffix and not suffix.startswith("."):
        suffix = f".{suffix}"
    return (
        f"google-recorder/{stage}/{recorded_at.year:04d}/{recorded_at.month:02d}/"
        f"{recorded_at.date().isoformat()}-{recording.get('content_sha256', '')}{suffix}"
    )


def metadata_to_row(metadata: Mapping[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    recording = nested_mapping(metadata, "recording")
    audio_file = nested_mapping(metadata, "audio_file")
    metadata_file = nested_mapping(metadata, "metadata_file")
    location = nested_mapping(recording, "location")
    uploaded_at = parse_datetime(str(metadata.get("uploaded_at", "")))
    return {
        "account": str(metadata.get("account", "")),
        "recording_id": str(recording.get("recording_id", "")),
        "device": str(metadata.get("device", "")),
        "title": str(recording.get("title", "")),
        "recorded_at": parse_datetime(str(recording.get("recorded_at", ""))),
        "duration_seconds": float(recording.get("duration_seconds", 0) or 0),
        "location_name": str(location.get("name", "")),
        "filename": str(recording.get("filename", "")),
        "extension": str(recording.get("extension", "")),
        "content_type": str(recording.get("content_type", "")),
        "size_bytes": int(recording.get("size_bytes", 0) or 0),
        "content_sha256": str(recording.get("content_sha256", "")),
        "file_modified_at": parse_datetime(str(recording.get("file_modified_at", ""))),
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
        # The sidecar's own upload time, not the run's: two sidecars of one
        # recording (a rename) can be promoted in the same run, and the newer
        # must win both inside the batch and against the stored row.
        "sync_version": int(uploaded_at.timestamp() * 1_000_000),
    }
