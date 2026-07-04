"""Google Drive backend for the object storage abstraction.

Objects are stored under a configured Drive folder (the "namespace"/root) and
tagged with ``appProperties`` (``pdw_source``, ``pdw_kind``,
``pdw_root_folder_id``, ``pdw_stage``, ``content_sha256``) so the upload
pipelines can deduplicate by content hash without a separate index. Filenames
and MIME types are preserved on upload.

This module also owns construction of the authenticated Drive ``service`` so the
factory can build a Drive-backed store from settings alone, keeping
Drive-specific concepts out of application call sites.
"""

from __future__ import annotations

from collections.abc import Mapping
from io import BytesIO
from pathlib import Path
import json
import re
import threading
import time
from typing import Any

import google_auth_httplib2
import httplib2
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload

from personal_data_warehouse.config import GOOGLE_DRIVE_SCOPE
from personal_data_warehouse.google_auth import load_google_credentials
from personal_data_warehouse.objectstore.base import (
    ObjectListing,
    ObjectMetadata,
    ObjectNotFoundError,
    ObjectPresence,
    StoredObject,
    stored_object_from_mapping,
)

_FOLDER_LOCKS: dict[tuple[str, str], threading.Lock] = {}
_FOLDER_LOCKS_LOCK = threading.Lock()

# Google Drive's upload API rejects a malformed media type (e.g. a bare
# ``application`` with no subtype) with HTTP 400 ``badContent``
# ("Media type 'application' is not supported. Valid media types: [*/*]").
# Some upstream sources carry such malformed MIME types verbatim — notably Gmail
# attachment parts whose ``mimeType`` header is broken in the original email — so
# anything that is not a well-formed ``type/subtype`` is coerced to this default.
_DEFAULT_MEDIA_TYPE = "application/octet-stream"
# RFC 6838-ish token for the type and subtype; permissive enough for real-world
# media types (``application/vnd.ms-excel``, ``image/svg+xml``) but requiring a
# non-empty type and subtype separated by a single slash.
_MEDIA_TYPE_TOKEN = r"[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*"
_MEDIA_TYPE_RE = re.compile(rf"^{_MEDIA_TYPE_TOKEN}/{_MEDIA_TYPE_TOKEN}$")


def sanitize_media_type(content_type: str) -> str:
    """Return a Drive-acceptable media type, falling back to octet-stream.

    Strips any ``;``-delimited parameters, then validates that what remains is a
    well-formed ``type/subtype``. Malformed or empty values become
    ``application/octet-stream`` so a single broken attachment can no longer
    fail its Drive upload (and, before this, retry forever).
    """
    candidate = (content_type or "").split(";", 1)[0].strip()
    if _MEDIA_TYPE_RE.match(candidate):
        return candidate
    return _DEFAULT_MEDIA_TYPE


APPLE_VOICE_MEMOS_DRIVE_SOURCE_QUERY = (
    "("
    "appProperties has { key='pdw_source' and value='apple_voice_memos' } "
    "or appProperties has { key='pdw_source' and value='voice_memos' }"
    ")"
)


def build_google_drive_service(*, account: str, settings, request_timeout_seconds: int = 30):
    credentials = load_google_credentials(
        email_address=account,
        settings=settings,
        scopes=(GOOGLE_DRIVE_SCOPE,),
        service_name="Google Drive",
        request_timeout_seconds=request_timeout_seconds,
    )
    http = build_google_drive_http(credentials=credentials, timeout_seconds=request_timeout_seconds)
    return build("drive", "v3", http=http, cache_discovery=False)


def build_google_drive_http(*, credentials, timeout_seconds: int):
    http = httplib2.Http(timeout=timeout_seconds)
    http.redirect_codes = frozenset(code for code in http.redirect_codes if code != 308)
    return google_auth_httplib2.AuthorizedHttp(credentials, http=http)


class GoogleDriveObjectStore:
    backend = "google_drive"

    def __init__(
        self,
        *,
        folder_id: str,
        service,
        max_attempts: int = 5,
        source: str = "apple_voice_memos",
        legacy_sources: tuple[str, ...] = ("voice_memos",),
        audio_kind: str = "voice_memo_audio",
        metadata_kind: str = "voice_memo_metadata",
    ) -> None:
        self._folder_id = folder_id
        self._service = service
        self._max_attempts = max_attempts
        self._folder_cache: dict[tuple[str, str], str] = {}
        self._source = source
        self._legacy_sources = legacy_sources
        self._audio_kind = audio_kind
        self._metadata_kind = metadata_kind

    def has_blob(self, *, content_sha256: str) -> bool:
        return self._find_by_app_property(
            key="content_sha256",
            value=content_sha256,
            kind=self._audio_kind,
        ) is not None

    def has_metadata(self, *, content_sha256: str) -> bool:
        return self._find_by_app_property(
            key="audio_content_sha256",
            value=content_sha256,
            kind=self._metadata_kind,
        ) is not None

    def has_object(self, *, kind: str, key: str, value: str) -> bool:
        return self._find_by_app_property(key=key, value=value, kind=kind) is not None

    def presence(self, *, content_sha256: str) -> ObjectPresence:
        response = self._find_all_by_content_sha256(content_sha256=content_sha256)
        files = response.get("files", []) if isinstance(response, dict) else []
        audio_exists = False
        metadata_exists = False
        for file in files:
            if not isinstance(file, dict):
                continue
            app_properties = file.get("appProperties", {})
            if not isinstance(app_properties, dict):
                continue
            kind = app_properties.get("pdw_kind")
            if kind == self._audio_kind:
                audio_exists = True
            elif kind == self._metadata_kind:
                metadata_exists = True
        return ObjectPresence(audio_exists=audio_exists, metadata_exists=metadata_exists)

    def put_file(
        self,
        *,
        path: Path,
        object_key: str,
        content_sha256: str,
        content_type: str,
        skip_existing_check: bool = False,
        app_properties: dict[str, str] | None = None,
        kind: str | None = None,
    ) -> StoredObject:
        object_kind = kind or self._audio_kind
        if not skip_existing_check:
            existing = self._find_by_app_property(
                key="content_sha256",
                value=content_sha256,
                kind=object_kind,
            )
            if existing is not None:
                return self._stored_object(existing, object_key)

        object_app_properties = {
            "pdw_source": self._source,
            "pdw_kind": object_kind,
            "pdw_root_folder_id": self._folder_id,
            "pdw_stage": object_stage(object_key),
            "content_sha256": content_sha256,
        }
        object_app_properties.update(app_properties or {})
        media_type = sanitize_media_type(content_type)
        body = {
            "name": drive_name_from_object_key(object_key),
            "parents": [self._folder_id],
            "mimeType": media_type,
            "appProperties": object_app_properties,
        }
        parent_id = self._ensure_parent_folder(object_key)
        body["name"] = drive_name_from_object_key(object_key)
        body["parents"] = [parent_id]
        media = MediaFileUpload(str(path), mimetype=media_type, resumable=True)
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
        skip_existing_check: bool = False,
        app_properties: dict[str, str] | None = None,
        kind: str | None = None,
    ) -> StoredObject:
        object_kind = kind or self._metadata_kind
        if source_content_sha256 and not skip_existing_check:
            existing = self._find_by_app_property(
                key="audio_content_sha256",
                value=source_content_sha256,
                kind=object_kind,
            )
            if existing is not None:
                return self._stored_object(existing, object_key)
        if not source_content_sha256 and not skip_existing_check:
            existing = self._find_by_app_property(
                key="content_sha256",
                value=content_sha256,
                kind=object_kind,
            )
            if existing is not None:
                return self._stored_object(existing, object_key)

        app_properties = {
            "pdw_source": self._source,
            "pdw_kind": object_kind,
            "pdw_root_folder_id": self._folder_id,
            "pdw_stage": object_stage(object_key),
            "content_sha256": content_sha256,
            **(app_properties or {}),
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

    def get_object(self, ref: Mapping[str, object]) -> bytes:
        file_id = self._file_id(ref)
        buffer = BytesIO()
        self._download(file_id, buffer)
        return buffer.getvalue()

    def download_to_path(self, ref: Mapping[str, object], path: Path) -> None:
        file_id = self._file_id(ref)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as output:
            self._download(file_id, output)

    def object_exists(self, ref: Mapping[str, object]) -> bool:
        file_id = self._file_id(ref)
        try:
            response = self._request(
                lambda: self._service.files()
                .get(fileId=file_id, fields="id,trashed", supportsAllDrives=True)
                .execute()
            )
        except HttpError as exc:
            if _http_status(exc) == 404:
                return False
            raise
        return not (isinstance(response, dict) and response.get("trashed", False))

    def delete_object(self, ref: Mapping[str, object]) -> None:
        file_id = self._file_id(ref)
        try:
            self._request(
                lambda: self._service.files()
                .delete(fileId=file_id, supportsAllDrives=True)
                .execute()
            )
        except HttpError as exc:
            if _http_status(exc) == 404:
                return
            raise

    def get_metadata(self, ref: Mapping[str, object]) -> ObjectMetadata:
        stored = stored_object_from_mapping(ref)
        file_id = self._file_id(stored)
        try:
            response = self._request(
                lambda: self._service.files()
                .get(
                    fileId=file_id,
                    fields="id,name,mimeType,size,createdTime,modifiedTime,webViewLink,appProperties",
                    supportsAllDrives=True,
                )
                .execute()
            )
        except HttpError as exc:
            if _http_status(exc) == 404:
                raise ObjectNotFoundError(f"Drive object {file_id} not found") from exc
            raise
        response = response if isinstance(response, dict) else {}
        app_properties = response.get("appProperties") or {}
        size = response.get("size")
        return ObjectMetadata(
            backend=self.backend,
            storage_key=stored["storage_key"],
            storage_file_id=str(response.get("id", file_id)),
            content_type=str(response.get("mimeType", "")),
            size_bytes=int(size) if isinstance(size, (str, int)) and str(size).isdigit() else None,
            content_sha256=app_properties.get("content_sha256") if isinstance(app_properties, dict) else None,
            filename=response.get("name"),
            created_time=response.get("createdTime"),
            modified_time=response.get("modifiedTime"),
            storage_url=str(response.get("webViewLink", "") or stored["storage_url"]),
        )

    def list_objects(
        self,
        *,
        kind: str,
        stage: str | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> list[ObjectListing]:
        listings: list[ObjectListing] = []
        page_token: str | None = None
        query = self._objects_query(kind=kind, stage=stage, properties=properties)
        while True:
            token = page_token
            response = self._execute(
                lambda token=token: self._service.files()
                .list(
                    q=query,
                    pageSize=1000,
                    pageToken=token,
                    fields="nextPageToken,files(id,name,webViewLink,appProperties)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )
            files = response.get("files", []) if isinstance(response, dict) else []
            for file in files:
                if isinstance(file, dict) and file.get("id"):
                    listings.append(self._listing_from_file(file))
            page_token = response.get("nextPageToken") if isinstance(response, dict) else None
            if not page_token:
                return listings

    def find_object(
        self,
        *,
        kind: str,
        stage: str | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> ObjectListing | None:
        query = self._objects_query(kind=kind, stage=stage, properties=properties)
        response = self._execute(
            lambda: self._service.files()
            .list(
                q=query,
                pageSize=1,
                fields="files(id,name,webViewLink,appProperties)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files = response.get("files", []) if isinstance(response, dict) else []
        first = files[0] if files else None
        return self._listing_from_file(first) if isinstance(first, dict) and first.get("id") else None

    def move_object(
        self,
        ref: Mapping[str, object],
        *,
        new_object_key: str,
        app_properties: Mapping[str, str] | None = None,
    ) -> StoredObject:
        file_id = self._file_id(ref)
        if not new_object_key:
            raise ValueError("move_object requires a non-empty new_object_key")
        parent_id = self._ensure_parent_folder(new_object_key)
        existing = self._execute(
            lambda: self._service.files()
            .get(fileId=file_id, fields="parents", supportsAllDrives=True)
            .execute()
        )
        old_parents = [str(parent) for parent in existing.get("parents", [])] if isinstance(existing, dict) else []
        remove_parents = [parent for parent in old_parents if parent != parent_id]
        body = {
            "name": drive_name_from_object_key(new_object_key),
            "appProperties": {"pdw_stage": object_stage(new_object_key), **dict(app_properties or {})},
        }
        response = self._execute(
            lambda: self._service.files()
            .update(
                fileId=file_id,
                body=body,
                addParents=parent_id,
                removeParents=",".join(remove_parents),
                fields="id,webViewLink,appProperties",
                supportsAllDrives=True,
            )
            .execute()
        )
        return {
            "storage_backend": self.backend,
            "storage_key": new_object_key,
            "storage_file_id": str(response.get("id", file_id)) if isinstance(response, dict) else file_id,
            "storage_url": str(response.get("webViewLink", "")) if isinstance(response, dict) else "",
        }

    def get_share_url(self, ref: Mapping[str, object]) -> str | None:
        stored = stored_object_from_mapping(ref)
        if stored["storage_url"]:
            return stored["storage_url"]
        url = self.get_metadata(ref).storage_url
        return url or None

    def _objects_query(
        self,
        *,
        kind: str,
        stage: str | None,
        properties: Mapping[str, str] | None,
    ) -> str:
        query = (
            "trashed = false "
            f"and {self._source_query()} "
            f"and appProperties has {{ key='pdw_root_folder_id' and value='{escape_query_value(self._folder_id)}' }} "
            f"and appProperties has {{ key='pdw_kind' and value='{escape_query_value(kind)}' }} "
        )
        if stage:
            query += f"and appProperties has {{ key='pdw_stage' and value='{escape_query_value(stage)}' }} "
        for key, value in (properties or {}).items():
            query += (
                f"and appProperties has {{ key='{escape_query_value(key)}' "
                f"and value='{escape_query_value(value)}' }} "
            )
        return query.strip()

    def _listing_from_file(self, file: dict[str, Any]) -> ObjectListing:
        app_properties = file.get("appProperties")
        normalized = (
            {str(key): str(value) for key, value in app_properties.items()}
            if isinstance(app_properties, dict)
            else {}
        )
        return ObjectListing(
            ref={
                "storage_backend": self.backend,
                "storage_key": "",
                "storage_file_id": str(file.get("id", "")),
                "storage_url": str(file.get("webViewLink", "")),
            },
            app_properties=normalized,
            filename=str(file.get("name", "")),
        )

    def _download(self, file_id: str, output) -> None:
        try:
            self._download_media(file_id, output, acknowledge_abuse=False)
        except HttpError as exc:
            if not _is_abusive_file_error(exc):
                raise
            # Drive refuses alt=media for objects it has flagged as malware or
            # spam unless the owner acknowledges the risk. These are our own
            # stored blobs (e.g. a phishing PDF a contact texted us), captured
            # deliberately for enrichment, so acknowledge and retry rather than
            # letting the attachment fail its download on every run forever.
            _rewind(output)
            self._download_media(file_id, output, acknowledge_abuse=True)

    def _download_media(self, file_id: str, output, *, acknowledge_abuse: bool) -> None:
        request = self._service.files().get_media(
            fileId=file_id, supportsAllDrives=True, acknowledgeAbuse=acknowledge_abuse
        )
        downloader = MediaIoBaseDownload(output, request)
        done = False
        while not done:
            _, done = downloader.next_chunk(num_retries=2)

    def _file_id(self, ref: Mapping[str, object]) -> str:
        file_id = str(ref.get("storage_file_id", "")) if isinstance(ref, Mapping) else ""
        if not file_id:
            raise ObjectNotFoundError("StoredObject reference is missing storage_file_id")
        return file_id

    def _find_by_app_property(self, *, key: str, value: str, kind: str) -> dict[str, Any] | None:
        query = (
            "trashed = false "
            f"and {self._source_query()} "
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

    def _find_all_by_content_sha256(self, *, content_sha256: str) -> dict[str, Any]:
        escaped_folder_id = escape_query_value(self._folder_id)
        escaped_sha = escape_query_value(content_sha256)
        query = (
            "trashed = false "
            f"and {self._source_query()} "
            f"and appProperties has {{ key='pdw_root_folder_id' and value='{escaped_folder_id}' }} "
            "and ("
            "appProperties has { key='pdw_stage' and value='inbox' } "
            "or appProperties has { key='pdw_stage' and value='library' }"
            ") "
            "and ("
            "("
            f"appProperties has {{ key='pdw_kind' and value='{escape_query_value(self._audio_kind)}' }} "
            f"and appProperties has {{ key='content_sha256' and value='{escaped_sha}' }}"
            ") "
            "or ("
            f"appProperties has {{ key='pdw_kind' and value='{escape_query_value(self._metadata_kind)}' }} "
            f"and appProperties has {{ key='audio_content_sha256' and value='{escaped_sha}' }}"
            ")"
            ")"
        )
        return self._execute(
            lambda: self._service.files()
            .list(
                q=query,
                pageSize=10,
                fields="files(id,webViewLink,appProperties)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )

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
                            "pdw_source": self._source,
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

    def _source_query(self) -> str:
        sources = (self._source, *self._legacy_sources)
        clauses = [
            f"appProperties has {{ key='pdw_source' and value='{escape_query_value(source)}' }}"
            for source in dict.fromkeys(sources)
        ]
        if len(clauses) == 1:
            return clauses[0]
        return "(" + " or ".join(clauses) + ")"

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

    def _request(self, operation):
        """Execute an idempotent request, retrying only on transient errors.

        Unlike :meth:`_execute` (used by the write/dedup paths, which retry on
        any exception), this surfaces non-transient errors immediately so that
        callers can distinguish e.g. a 404 from a flaky network.
        """
        last_exc = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                return operation()
            except Exception as exc:
                last_exc = exc
                if not is_transient_google_error(exc) or attempt == self._max_attempts:
                    raise
                time.sleep(min(30, attempt))
        raise last_exc  # pragma: no cover - loop always returns or raises


def _http_status(exc: HttpError) -> int | None:
    return getattr(getattr(exc, "resp", None), "status", None)


def _rewind(output) -> None:
    """Reset a partially-written, seekable output before a download retry."""
    try:
        if output.seekable():
            output.seek(0)
            output.truncate()
    except (AttributeError, OSError):  # pragma: no cover - non-seekable stream
        pass


def _is_abusive_file_error(exc: HttpError) -> bool:
    """True for Drive's 403 refusing to serve a malware/abuse-flagged file.

    Downloading such a file requires ``acknowledgeAbuse=true``; the API signals
    the condition with HTTP 403 and reason ``cannotDownloadAbusiveFile``.
    """
    if _http_status(exc) != 403:
        return False
    content = getattr(exc, "content", b"") or b""
    if isinstance(content, bytes):
        content = content.decode("utf-8", "replace")
    text = str(content).lower()
    return "cannotdownloadabusivefile" in text or "malware or spam" in text


def is_transient_google_error(exc: Exception) -> bool:
    if isinstance(exc, HttpError):
        status = getattr(exc.resp, "status", None)
        return status in {408, 429, 500, 502, 503, 504}
    message = str(exc).lower()
    return any(
        token in message
        for token in (
            "timed out",
            "timeout",
            "temporary failure",
            "connection reset",
            "connection aborted",
            "connection refused",
            "network is unreachable",
            "name or service not known",
        )
    )


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
