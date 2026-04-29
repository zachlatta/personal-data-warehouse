from __future__ import annotations

from io import BytesIO
from pathlib import Path
import json
import threading
import time
from typing import Any

from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload

from personal_data_warehouse_voice_memos.storage import StoredObject

_FOLDER_LOCKS: dict[tuple[str, str], threading.Lock] = {}
_FOLDER_LOCKS_LOCK = threading.Lock()


class GoogleDriveObjectStore:
    backend = "google_drive"

    def __init__(self, *, folder_id: str, service, max_attempts: int = 5) -> None:
        self._folder_id = folder_id
        self._service = service
        self._max_attempts = max_attempts
        self._folder_cache: dict[tuple[str, str], str] = {}

    def has_blob(self, *, content_sha256: str) -> bool:
        return self._find_by_app_property(
            key="content_sha256",
            value=content_sha256,
            kind="voice_memo_audio",
        ) is not None

    def has_metadata(self, *, content_sha256: str) -> bool:
        return self._find_by_app_property(
            key="audio_content_sha256",
            value=content_sha256,
            kind="voice_memo_metadata",
        ) is not None

    def put_file(
        self,
        *,
        path: Path,
        object_key: str,
        content_sha256: str,
        content_type: str,
    ) -> StoredObject:
        existing = self._find_by_app_property(
            key="content_sha256",
            value=content_sha256,
            kind="voice_memo_audio",
        )
        if existing is not None:
            return self._stored_object(existing, object_key)

        body = {
            "name": drive_name_from_object_key(object_key),
            "parents": [self._folder_id],
            "mimeType": content_type,
            "appProperties": {
                "pdw_source": "voice_memos",
                "pdw_kind": "voice_memo_audio",
                "pdw_root_folder_id": self._folder_id,
                "pdw_stage": object_stage(object_key),
                "content_sha256": content_sha256,
            },
        }
        parent_id = self._ensure_parent_folder(object_key)
        body["name"] = drive_name_from_object_key(object_key)
        body["parents"] = [parent_id]
        media = MediaFileUpload(str(path), mimetype=content_type, resumable=True)
        response = self._execute(
            lambda: self._service.files()
            .create(
                body=body,
                media_body=media,
                fields="id,webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )
        return self._stored_object(response, object_key)

    def put_json(
        self,
        *,
        object_key: str,
        payload: dict[str, object],
        content_sha256: str,
        source_content_sha256: str | None = None,
    ) -> StoredObject:
        if source_content_sha256:
            existing = self._find_by_app_property(
                key="audio_content_sha256",
                value=source_content_sha256,
                kind="voice_memo_metadata",
            )
            if existing is not None:
                return self._stored_object(existing, object_key)

        app_properties = {
            "pdw_source": "voice_memos",
            "pdw_kind": "voice_memo_metadata",
            "pdw_root_folder_id": self._folder_id,
            "pdw_stage": object_stage(object_key),
            "content_sha256": content_sha256,
        }
        if source_content_sha256:
            app_properties["audio_content_sha256"] = source_content_sha256
        body = {
            "name": drive_name_from_object_key(object_key),
            "parents": [self._ensure_parent_folder(object_key)],
            "mimeType": "application/json",
            "appProperties": app_properties,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        media = MediaIoBaseUpload(BytesIO(encoded), mimetype="application/json", resumable=False)
        response = self._execute(
            lambda: self._service.files()
            .create(
                body=body,
                media_body=media,
                fields="id,webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )
        return self._stored_object(response, object_key)

    def _find_by_app_property(self, *, key: str, value: str, kind: str) -> dict[str, Any] | None:
        query = (
            "trashed = false "
            "and appProperties has { key='pdw_source' and value='voice_memos' } "
            f"and appProperties has {{ key='pdw_root_folder_id' and value='{escape_query_value(self._folder_id)}' }} "
            f"and appProperties has {{ key='pdw_kind' and value='{escape_query_value(kind)}' }} "
            "and ("
            "appProperties has { key='pdw_stage' and value='inbox' } "
            "or appProperties has { key='pdw_stage' and value='library' }"
            ") "
            f"and appProperties has {{ key='{escape_query_value(key)}' and value='{escape_query_value(value)}' }}"
        )
        response = self._execute(
            lambda: self._service.files()
            .list(
                q=query,
                pageSize=1,
                fields="files(id,webViewLink,appProperties)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files = response.get("files", []) if isinstance(response, dict) else []
        first = files[0] if files else None
        return first if isinstance(first, dict) else None

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

        with folder_lock(cache_key):
            if cache_key in self._folder_cache:
                return self._folder_cache[cache_key]

            query = (
                f"'{escape_query_value(parent_id)}' in parents and trashed = false "
                "and mimeType = 'application/vnd.google-apps.folder' "
                f"and name = '{escape_query_value(name)}'"
            )
            response = self._execute(
                lambda: self._service.files()
                .list(
                    q=query,
                    pageSize=1,
                    fields="files(id,webViewLink)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )
            files = response.get("files", []) if isinstance(response, dict) else []
            first = files[0] if files else None
            if isinstance(first, dict) and first.get("id"):
                folder_id = str(first["id"])
                self._folder_cache[cache_key] = folder_id
                return folder_id

            created = self._execute(
                lambda: self._service.files()
                .create(
                    body={
                        "name": name,
                        "parents": [parent_id],
                        "mimeType": "application/vnd.google-apps.folder",
                        "appProperties": {
                            "pdw_source": "voice_memos",
                            "pdw_kind": "object_storage_prefix",
                            "pdw_root_folder_id": self._folder_id,
                        },
                    },
                    fields="id,webViewLink",
                    supportsAllDrives=True,
                )
                .execute()
            )
            folder_id = str(created.get("id", ""))
            self._folder_cache[cache_key] = folder_id
            return folder_id

    def _stored_object(self, response: dict[str, Any], object_key: str) -> StoredObject:
        return {
            "storage_backend": self.backend,
            "storage_key": object_key,
            "storage_file_id": str(response.get("id", "")),
            "storage_url": str(response.get("webViewLink", "")),
        }

    def _execute(self, operation):
        last_exc = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                return operation()
            except Exception as exc:  # Google client raises several transport/API types.
                last_exc = exc
                if attempt == self._max_attempts:
                    break
                time.sleep(min(30, attempt))
        raise RuntimeError(f"Google Drive request failed after {self._max_attempts} attempts: {last_exc}") from last_exc


def drive_name_from_object_key(object_key: str) -> str:
    return object_key.rsplit("/", 1)[-1]


def object_stage(object_key: str) -> str:
    parts = [part for part in object_key.split("/") if part]
    if len(parts) > 1 and parts[1] in {"inbox", "library"}:
        return parts[1]
    return ""


def escape_query_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def folder_lock(cache_key: tuple[str, str]) -> threading.Lock:
    with _FOLDER_LOCKS_LOCK:
        lock = _FOLDER_LOCKS.get(cache_key)
        if lock is None:
            lock = threading.Lock()
            _FOLDER_LOCKS[cache_key] = lock
        return lock
