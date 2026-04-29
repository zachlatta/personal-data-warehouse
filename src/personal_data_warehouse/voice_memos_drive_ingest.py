from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
import json
import time
from typing import Any

import google_auth_httplib2
import httplib2
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from personal_data_warehouse.config import GOOGLE_DRIVE_SCOPE, Settings
from personal_data_warehouse.google_auth import load_google_credentials


@dataclass(frozen=True)
class VoiceMemosDriveIngestSummary:
    metadata_seen: int
    rows_written: int
    recordings_promoted: int


INBOX_PREFIX = "apple-voice-memos/inbox/"
LIBRARY_PREFIX = "apple-voice-memos/library/"


class VoiceMemosDriveIngestRunner:
    def __init__(
        self,
        *,
        warehouse,
        metadata_source: Callable[[], Iterable[Mapping[str, Any]]],
        promoter=None,
        logger,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._metadata_source = metadata_source
        self._promoter = promoter
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync(self) -> VoiceMemosDriveIngestSummary:
        self._warehouse.ensure_voice_memos_tables()
        ingested_at = self._now()
        metadata = list(self._metadata_source())
        row_metadata = [library_metadata_payload(payload) for payload in metadata] if self._promoter else metadata
        rows = [metadata_to_row(payload, ingested_at=ingested_at) for payload in row_metadata]
        self._warehouse.insert_voice_memo_files(rows)
        promoted = 0
        if self._promoter:
            for payload in metadata:
                self._promoter.promote(payload)
                promoted += 1
        self._logger.info("Ingested %s Voice Memos metadata", len(rows))
        return VoiceMemosDriveIngestSummary(
            metadata_seen=len(metadata),
            rows_written=len(rows),
            recordings_promoted=promoted,
        )


class GoogleDriveVoiceMemosPromoter:
    def __init__(self, *, service, folder_id: str, max_attempts: int = 3) -> None:
        self._service = service
        self._folder_id = folder_id
        self._max_attempts = max_attempts
        self._folder_cache: dict[tuple[str, str], str] = {}

    def promote(self, payload: Mapping[str, Any]) -> None:
        library_payload = library_metadata_payload(payload)
        self._promote_stored_file(
            source=nested_mapping(payload, "audio_file"),
            target=nested_mapping(library_payload, "audio_file"),
        )
        self._promote_stored_file(
            source=nested_mapping(payload, "metadata_file"),
            target=nested_mapping(library_payload, "metadata_file"),
        )

    def _promote_stored_file(
        self,
        *,
        source: Mapping[str, Any],
        target: Mapping[str, Any],
    ) -> None:
        file_id = str(source.get("storage_file_id", ""))
        storage_key = str(target.get("storage_key", ""))
        if not file_id or not storage_key:
            raise ValueError("Voice Memos metadata is missing a Drive file id or target storage key")

        parent_id = self._ensure_parent_folder(storage_key)
        existing = self._execute(
            self._service.files().get(
                fileId=file_id,
                fields="parents",
                supportsAllDrives=True,
            )
        )
        old_parent_ids = [str(parent_id) for parent_id in existing.get("parents", [])]
        remove_parent_ids = [old_parent_id for old_parent_id in old_parent_ids if old_parent_id != parent_id]
        app_properties = {
            "pdw_stage": storage_stage(storage_key),
        }
        update_kwargs = {
            "fileId": file_id,
            "body": {
                "name": drive_name_from_object_key(storage_key),
                "appProperties": app_properties,
            },
            "addParents": parent_id,
            "removeParents": ",".join(remove_parent_ids),
            "fields": "id,webViewLink,appProperties",
            "supportsAllDrives": True,
        }
        self._execute(self._service.files().update(**update_kwargs))

    def _ensure_parent_folder(self, object_key: str) -> str:
        parts = [part for part in object_key.split("/")[:-1] if part]
        parent_id = self._folder_id
        for part in parts:
            parent_id = self._ensure_folder(parent_id=parent_id, name=part)
        return parent_id

    def _ensure_folder(self, *, parent_id: str, name: str) -> str:
        cache_key = (parent_id, name)
        if cache_key in self._folder_cache:
            return self._folder_cache[cache_key]
        response = self._execute(
            self._service.files().list(
                q=(
                    f"'{escape_drive_query_value(parent_id)}' in parents and trashed = false "
                    "and mimeType = 'application/vnd.google-apps.folder' "
                    f"and name = '{escape_drive_query_value(name)}'"
                ),
                pageSize=1,
                fields="files(id,webViewLink)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )
        files = response.get("files", []) if isinstance(response, dict) else []
        first = files[0] if files else None
        if isinstance(first, dict) and first.get("id"):
            folder_id = str(first["id"])
        else:
            created = self._execute(
                self._service.files().create(
                    body={
                        "name": name,
                        "parents": [parent_id],
                        "mimeType": "application/vnd.google-apps.folder",
                        "appProperties": {
                            "pdw_source": "voice_memos",
                            "pdw_kind": "object_storage_prefix",
                            "pdw_root_folder_id": self._folder_id,
                            "pdw_stage": storage_stage_from_folder_name(name),
                        },
                    },
                    fields="id,webViewLink",
                    supportsAllDrives=True,
                )
            )
            folder_id = str(created.get("id", ""))
        self._folder_cache[cache_key] = folder_id
        return folder_id

    def _execute(self, request):
        last_exc = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                try:
                    return request.execute(num_retries=2)
                except TypeError:
                    return request.execute()
            except Exception as exc:
                last_exc = exc
                if attempt == self._max_attempts:
                    break
                time.sleep(min(10, attempt * 2))
        raise RuntimeError(f"Google Drive promotion request failed after {self._max_attempts} attempts: {last_exc}") from last_exc


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


def attach_storage_context(
    payload: Mapping[str, Any],
    *,
    stage: str,
    metadata_file: Mapping[str, Any],
    audio_file: Mapping[str, Any],
) -> dict[str, Any]:
    decorated = clean_metadata_payload(payload)
    decorated["audio_file"] = {
        "storage_backend": "google_drive",
        "storage_key": audio_storage_key(payload, stage=stage),
        "storage_file_id": str(audio_file.get("id", "")),
        "storage_url": str(audio_file.get("webViewLink", "")),
    }
    decorated["metadata_file"] = {
        "storage_backend": "google_drive",
        "storage_key": metadata_storage_key(payload, stage=stage),
        "storage_file_id": str(metadata_file.get("id", "")),
        "storage_url": str(metadata_file.get("webViewLink", "")),
        "content_sha256": str(nested_mapping(metadata_file, "appProperties").get("content_sha256", "")),
    }
    return decorated


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


def iter_drive_metadata_payloads(
    *,
    service,
    folder_id: str,
    stage: str = "inbox",
) -> Iterable[Mapping[str, Any]]:
    page_token: str | None = None
    while True:
        response = execute_drive_request(service.files().list(
            q=(
                "trashed = false "
                "and appProperties has { key='pdw_source' and value='voice_memos' } "
                f"and appProperties has {{ key='pdw_root_folder_id' and value='{escape_drive_query_value(folder_id)}' }} "
                "and appProperties has { key='pdw_kind' and value='voice_memo_metadata' } "
                f"and appProperties has {{ key='pdw_stage' and value='{escape_drive_query_value(stage)}' }}"
            ),
            pageSize=1000,
            pageToken=page_token,
            fields="nextPageToken,files(id,name,webViewLink,appProperties)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ))
        for file in response.get("files", []):
            file_id = str(file.get("id", ""))
            if file_id:
                payload = dict(download_drive_json(service=service, file_id=file_id))
                audio_file = find_drive_audio_file(service=service, folder_id=folder_id, payload=payload, stage=stage)
                yield attach_storage_context(
                    payload,
                    stage=stage,
                    metadata_file=file,
                    audio_file=audio_file,
                )
        page_token = response.get("nextPageToken")
        if not page_token:
            return


def download_drive_json(*, service, file_id: str) -> Mapping[str, Any]:
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    buffer = BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk(num_retries=2)
    payload = json.loads(buffer.getvalue().decode("utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"Drive file {file_id} did not contain a JSON object")
    return payload


def find_drive_audio_file(*, service, folder_id: str, payload: Mapping[str, Any], stage: str) -> Mapping[str, Any]:
    recording = nested_mapping(payload, "recording")
    content_sha256 = str(recording.get("content_sha256", ""))
    if not content_sha256:
        legacy_audio_file = nested_mapping(payload, "audio_file")
        if legacy_audio_file:
            return drive_file_from_stored_object(legacy_audio_file)
        raise ValueError("Voice Memos metadata is missing recording.content_sha256")

    response = execute_drive_request(service.files().list(
        q=(
            "trashed = false "
            "and appProperties has { key='pdw_source' and value='voice_memos' } "
            f"and appProperties has {{ key='pdw_root_folder_id' and value='{escape_drive_query_value(folder_id)}' }} "
            "and appProperties has { key='pdw_kind' and value='voice_memo_audio' } "
            f"and appProperties has {{ key='pdw_stage' and value='{escape_drive_query_value(stage)}' }} "
            f"and appProperties has {{ key='content_sha256' and value='{escape_drive_query_value(content_sha256)}' }}"
        ),
        pageSize=1,
        fields="files(id,name,webViewLink,appProperties)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ))
    files = response.get("files", []) if isinstance(response, dict) else []
    first = files[0] if files else None
    if isinstance(first, Mapping):
        return first

    legacy_audio_file = nested_mapping(payload, "audio_file")
    if legacy_audio_file:
        return drive_file_from_stored_object(legacy_audio_file)
    raise ValueError(f"Could not find Drive audio file for Voice Memo content_sha256={content_sha256}")


def drive_file_from_stored_object(stored_object: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": str(stored_object.get("storage_file_id", "")),
        "webViewLink": str(stored_object.get("storage_url", "")),
        "appProperties": {},
    }


def drive_name_from_object_key(object_key: str) -> str:
    return object_key.rsplit("/", 1)[-1]


def storage_stage(storage_key: str) -> str:
    parts = [part for part in storage_key.split("/") if part]
    if len(parts) > 1 and parts[1] in {"inbox", "library"}:
        return parts[1]
    return ""


def storage_stage_from_folder_name(name: str) -> str:
    return name if name in {"inbox", "library"} else ""


def build_google_drive_service(*, account: str, settings: Settings):
    credentials = load_google_credentials(
        email_address=account,
        settings=settings,
        scopes=(GOOGLE_DRIVE_SCOPE,),
        service_name="Google Drive",
    )
    http = google_auth_httplib2.AuthorizedHttp(credentials, http=httplib2.Http(timeout=60))
    return build("drive", "v3", http=http, cache_discovery=False)


def execute_drive_request(request):
    try:
        return request.execute(num_retries=2)
    except TypeError:
        return request.execute()


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


def escape_drive_query_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")
