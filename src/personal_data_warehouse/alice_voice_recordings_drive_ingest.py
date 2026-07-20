"""Materialize the Alice Drive archive into queryable recording rows.

Alice predates the app-ingest pipelines and intentionally writes directly to
the object-store library.  This reader is the canonical bridge from those
immutable sidecars/artifacts into Postgres; the timeline never scans Drive.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import mimetypes
from typing import Any

from personal_data_warehouse.objectstore import ObjectListing, ObjectStore
from personal_data_warehouse_alice_voice_recordings.gmail_recovery import (
    AUDIO_KIND,
    EMAIL_ATTACHMENT_KIND,
    EMAIL_BODY_KIND,
    EMAIL_METADATA_KIND,
    TRANSCRIPT_KIND,
)
from personal_data_warehouse_alice_voice_recordings.sync import METADATA_KIND, PAGE_HTML_KIND


ARTIFACT_KINDS = (
    AUDIO_KIND,
    TRANSCRIPT_KIND,
    EMAIL_BODY_KIND,
    EMAIL_ATTACHMENT_KIND,
    PAGE_HTML_KIND,
)


@dataclass(frozen=True)
class AliceVoiceRecordingsDriveIngestSummary:
    metadata_seen: int
    recordings_written: int
    artifacts_written: int


class AliceVoiceRecordingsDriveIngestRunner:
    def __init__(
        self,
        *,
        warehouse,
        metadata_source: Callable[[], Iterable[Mapping[str, Any]]],
        logger,
    ) -> None:
        self._warehouse = warehouse
        self._metadata_source = metadata_source
        self._logger = logger

    def sync(self) -> AliceVoiceRecordingsDriveIngestSummary:
        self._warehouse.ensure_alice_voice_recordings_tables()
        payloads = list(self._metadata_source())
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for payload in payloads:
            recording = nested_mapping(payload, "recording")
            recording_id = str(recording.get("recording_id", ""))
            if not recording_id:
                raise ValueError("Alice metadata is missing recording.recording_id")
            # Gmail recovery can arrive through a different mailbox than the
            # configured Alice API account. Alice's recording id is global, so
            # merge those witnesses into one canonical recording.
            grouped.setdefault(recording_id, []).append(payload)

        recording_rows: list[dict[str, Any]] = []
        artifact_rows: list[dict[str, Any]] = []
        for values in grouped.values():
            merged = merge_recording_payloads(values)
            recording_rows.append(metadata_to_recording_row(merged))
            artifact_rows.extend(metadata_to_artifact_rows(merged))

        self._warehouse.insert_alice_voice_recordings(recording_rows)
        self._warehouse.insert_alice_voice_recording_artifacts(artifact_rows)
        self._logger.info(
            "Materialized %s Alice recording(s) and %s artifact(s)",
            len(recording_rows),
            len(artifact_rows),
        )
        return AliceVoiceRecordingsDriveIngestSummary(
            metadata_seen=len(payloads),
            recordings_written=len(recording_rows),
            artifacts_written=len(artifact_rows),
        )


def iter_archive_payloads(*, object_store: ObjectStore) -> Iterable[Mapping[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for kind in (METADATA_KIND, EMAIL_METADATA_KIND):
        for listing in object_store.list_objects(kind=kind, stage="library"):
            payload = dict(load_json_object(object_store, listing.ref))
            payload["_metadata_object"] = listing_context(listing)
            payloads.append(payload)

    by_recording_id: dict[str, list[dict[str, Any]]] = {}
    for payload in payloads:
        recording_id = str(nested_mapping(payload, "recording").get("recording_id", ""))
        if recording_id:
            by_recording_id.setdefault(recording_id, []).append(payload)

    for kind in ARTIFACT_KINDS:
        for listing in object_store.list_objects(kind=kind, stage="library"):
            recording_id = str(
                listing.app_properties.get("alice_upload_id")
                or listing.app_properties.get("alice_recording_guid")
                or ""
            )
            targets = by_recording_id.get(recording_id, [])
            for target in targets:
                target.setdefault("_artifacts", []).append(
                    {
                        "kind": kind,
                        "filename": listing.filename,
                        "storage": listing_context(listing),
                        "app_properties": dict(listing.app_properties),
                    }
                )

    yield from payloads


def load_json_object(object_store: ObjectStore, ref: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = json.loads(object_store.get_object(ref).decode("utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("Stored Alice metadata did not contain a JSON object")
    return payload


def listing_context(listing: ObjectListing) -> dict[str, str]:
    context = dict(listing.ref)
    context["filename"] = listing.filename
    context["content_sha256"] = str(listing.app_properties.get("content_sha256", ""))
    return context


def merge_recording_payloads(payloads: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    values = list(payloads)
    if not values:
        raise ValueError("Cannot merge an empty Alice metadata group")
    # The API sidecar has audio metadata; the Gmail recovery sidecar fills gaps
    # and contributes durable transcript/email artifacts.
    values.sort(key=lambda item: ("alice" not in item, str(item.get("uploaded_at", ""))))
    merged = deepcopy(dict(values[0]))
    merged["account"] = next(
        (str(payload.get("account", "")) for payload in values if payload.get("account")),
        "",
    )
    if not merged["account"]:
        raise ValueError("Alice metadata is missing account")
    merged_recording = dict(nested_mapping(merged, "recording"))
    artifacts: list[Mapping[str, Any]] = []
    for payload in values:
        for key, value in nested_mapping(payload, "recording").items():
            if merged_recording.get(key) in (None, "", 0, []):
                merged_recording[key] = value
        raw_artifacts = payload.get("_artifacts")
        if isinstance(raw_artifacts, list):
            artifacts.extend(item for item in raw_artifacts if isinstance(item, Mapping))
    merged["recording"] = merged_recording
    merged["_artifacts"] = dedupe_artifacts(artifacts)
    merged["_archive_payloads"] = [clean_payload(payload) for payload in values]
    merged["uploaded_at"] = max(str(payload.get("uploaded_at", "")) for payload in values)
    recovery_sources = [str(payload.get("recovery_source", "")) for payload in values]
    merged["recovery_source"] = next((value for value in recovery_sources if value), "")
    return merged


def dedupe_artifacts(artifacts: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    by_id: dict[str, Mapping[str, Any]] = {}
    for artifact in artifacts:
        storage = nested_mapping(artifact, "storage")
        artifact_id = str(storage.get("storage_file_id", ""))
        if artifact_id:
            by_id[artifact_id] = artifact
    return list(by_id.values())


def metadata_to_recording_row(metadata: Mapping[str, Any]) -> dict[str, Any]:
    recording = nested_mapping(metadata, "recording")
    alice = nested_mapping(metadata, "alice")
    artifacts = metadata.get("_artifacts") if isinstance(metadata.get("_artifacts"), list) else []
    audio = next((item for item in artifacts if item.get("kind") == AUDIO_KIND), {})
    audio_storage = nested_mapping(audio, "storage")
    metadata_storage = nested_mapping(metadata, "_metadata_object")
    ingested_at = parse_timestamp(str(metadata.get("uploaded_at", "")))
    duration = alice.get("duration_in_secs", 0)
    try:
        duration_seconds = int(float(duration or 0))
    except (TypeError, ValueError):
        duration_seconds = 0
    return {
        "account": str(metadata.get("account", "")),
        "recording_id": str(recording.get("recording_id", "")),
        "title": str(recording.get("title", "")),
        "filename": str(recording.get("filename", "") or audio.get("filename", "")),
        "content_type": str(recording.get("content_type", "") or artifact_content_type(audio, metadata)),
        "size_bytes": int(recording.get("size_bytes", 0) or artifact_size(audio, metadata)),
        "content_sha256": str(recording.get("content_sha256", "") or audio_storage.get("content_sha256", "")),
        "recorded_at": parse_timestamp(str(recording.get("recorded_at", ""))),
        "duration_seconds": duration_seconds,
        "recording_page_url": str(
            alice.get("recording_page_url", "") or recording.get("recording_page_url", "")
        ),
        "recovery_source": str(metadata.get("recovery_source", "")),
        "storage_backend": str(audio_storage.get("storage_backend", "")),
        "storage_key": str(audio_storage.get("storage_key", "")),
        "storage_file_id": str(audio_storage.get("storage_file_id", "")),
        "storage_url": str(audio_storage.get("storage_url", "")),
        "metadata_storage_key": str(metadata_storage.get("storage_key", "")),
        "metadata_storage_file_id": str(metadata_storage.get("storage_file_id", "")),
        "metadata_storage_url": str(metadata_storage.get("storage_url", "")),
        "metadata_content_sha256": str(metadata_storage.get("content_sha256", "")),
        "raw_metadata_json": clean_payload(metadata),
        "ingested_at": ingested_at,
        "sync_version": int(ingested_at.timestamp() * 1_000_000),
    }


def metadata_to_artifact_rows(metadata: Mapping[str, Any]) -> list[dict[str, Any]]:
    account = str(metadata.get("account", ""))
    recording_id = str(nested_mapping(metadata, "recording").get("recording_id", ""))
    ingested_at = parse_timestamp(str(metadata.get("uploaded_at", "")))
    rows: list[dict[str, Any]] = []
    artifacts = metadata.get("_artifacts") if isinstance(metadata.get("_artifacts"), list) else []
    for artifact in artifacts:
        if not isinstance(artifact, Mapping):
            continue
        storage = nested_mapping(artifact, "storage")
        artifact_id = str(storage.get("storage_file_id", ""))
        if not artifact_id:
            continue
        rows.append(
            {
                "account": account,
                "recording_id": recording_id,
                "artifact_id": artifact_id,
                "kind": str(artifact.get("kind", "")),
                "filename": str(artifact.get("filename", "")),
                "content_type": artifact_content_type(artifact, metadata),
                "size_bytes": artifact_size(artifact, metadata),
                "content_sha256": str(storage.get("content_sha256", "")),
                "storage_backend": str(storage.get("storage_backend", "")),
                "storage_key": str(storage.get("storage_key", "")),
                "storage_file_id": artifact_id,
                "storage_url": str(storage.get("storage_url", "")),
                "raw_metadata_json": dict(artifact.get("app_properties", {})),
                "ingested_at": ingested_at,
                "sync_version": int(ingested_at.timestamp() * 1_000_000),
            }
        )
    return rows


def artifact_content_type(artifact: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    kind = str(artifact.get("kind", ""))
    filename = str(artifact.get("filename", ""))
    if kind == AUDIO_KIND:
        value = str(nested_mapping(metadata, "recording").get("content_type", ""))
        if value:
            return value
    fixed = {
        TRANSCRIPT_KIND: "text/plain",
        EMAIL_BODY_KIND: "text/markdown",
        PAGE_HTML_KIND: "text/html",
    }
    return fixed.get(kind, "") or (mimetypes.guess_type(filename)[0] or "application/octet-stream")


def artifact_size(artifact: Mapping[str, Any], metadata: Mapping[str, Any]) -> int:
    if artifact.get("kind") == AUDIO_KIND:
        return int(nested_mapping(metadata, "recording").get("size_bytes", 0) or 0)
    storage_id = str(nested_mapping(artifact, "storage").get("storage_file_id", ""))
    gmail = nested_mapping(metadata, "gmail")
    attachments = gmail.get("attachments") if isinstance(gmail.get("attachments"), list) else []
    for attachment in attachments:
        if not isinstance(attachment, Mapping):
            continue
        if str(nested_mapping(attachment, "storage").get("storage_file_id", "")) == storage_id:
            return int(attachment.get("size", 0) or 0)
    return 0


def clean_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    clean = deepcopy(dict(payload))
    clean.pop("_metadata_object", None)
    clean.pop("_artifacts", None)
    return clean


def nested_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    nested = value.get(key)
    return nested if isinstance(nested, Mapping) else {}


def parse_timestamp(value: str) -> datetime:
    if not value:
        return datetime.fromtimestamp(0, tz=UTC)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.fromtimestamp(0, tz=UTC)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
