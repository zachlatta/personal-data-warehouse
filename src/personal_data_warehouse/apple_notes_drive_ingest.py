from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
import json
import threading
import time
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from googleapiclient.http import MediaIoBaseDownload

from personal_data_warehouse.apple_voice_memos_drive_ingest import (
    build_google_drive_service,
    drive_name_from_object_key,
    escape_drive_query_value,
    execute_drive_request,
    parse_datetime,
)


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
APPLE_NOTES_DRIVE_SOURCE_QUERY = "appProperties has { key='pdw_source' and value='apple_notes' }"
_PROMOTER_FOLDER_CACHE: dict[tuple[str, str], str] = {}
_PROMOTER_FOLDER_LOCKS: dict[tuple[str, str], threading.Lock] = {}
_PROMOTER_FOLDER_LOCKS_LOCK = threading.Lock()


class AppleNotesDriveIngestRunner:
    def __init__(
        self,
        *,
        warehouse,
        metadata_source: Callable[[], Iterable[Mapping[str, Any]]],
        promoter=None,
        promoter_factory: Callable[[], Any] | None = None,
        promotion_workers: int = 1,
        logger,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if promoter is not None and promoter_factory is not None:
            raise ValueError("pass only one of promoter or promoter_factory")
        self._warehouse = warehouse
        self._metadata_source = metadata_source
        self._promoter = promoter
        self._promoter_factory = promoter_factory
        self._promotion_workers = max(1, promotion_workers)
        self._thread_local = threading.local()
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync(self) -> AppleNotesDriveIngestSummary:
        self._warehouse.ensure_apple_notes_tables()
        ingested_at = self._now()
        metadata = list(self._metadata_source())
        should_record_library_keys = self._promoter is not None or self._promoter_factory is not None
        row_metadata = [library_metadata_payload(payload) for payload in metadata] if should_record_library_keys else metadata
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
        if self._promoter or self._promoter_factory:
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
        return self._promoter_for_thread().promote(payload)

    def _promoter_for_thread(self):
        if self._promoter is not None:
            return self._promoter
        promoter = getattr(self._thread_local, "promoter", None)
        if promoter is None:
            if self._promoter_factory is None:
                raise RuntimeError("promoter_factory is not configured")
            promoter = self._promoter_factory()
            self._thread_local.promoter = promoter
        return promoter


class GoogleDriveAppleNotesPromoter:
    def __init__(self, *, service, folder_id: str, max_attempts: int = 3) -> None:
        self._service = service
        self._folder_id = folder_id
        self._max_attempts = max_attempts
        self._folder_cache: dict[tuple[str, str], str] = {}

    def promote(self, payload: Mapping[str, Any]) -> int:
        promoted = 0
        library_payload = library_metadata_payload(payload)
        for source, target in stored_files_for_promotion(payload, library_payload):
            if self._promote_stored_file(source=source, target=target):
                promoted += 1
        return promoted

    def _promote_stored_file(self, *, source: Mapping[str, Any], target: Mapping[str, Any]) -> bool:
        file_id = str(source.get("storage_file_id", ""))
        storage_key = str(target.get("storage_key", ""))
        if not file_id or not storage_key:
            return False
        parent_id = self._ensure_parent_folder(storage_key)
        existing = self._execute(
            self._service.files().get(fileId=file_id, fields="parents", supportsAllDrives=True)
        )
        old_parent_ids = [str(parent_id) for parent_id in existing.get("parents", [])]
        remove_parent_ids = [old_parent_id for old_parent_id in old_parent_ids if old_parent_id != parent_id]
        self._execute(
            self._service.files().update(
                fileId=file_id,
                body={
                    "name": drive_name_from_object_key(storage_key),
                    "appProperties": {"pdw_stage": storage_stage(storage_key)},
                },
                addParents=parent_id,
                removeParents=",".join(remove_parent_ids),
                fields="id,webViewLink,appProperties",
                supportsAllDrives=True,
            )
        )
        return True

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
        if cache_key in _PROMOTER_FOLDER_CACHE:
            folder_id = _PROMOTER_FOLDER_CACHE[cache_key]
            self._folder_cache[cache_key] = folder_id
            return folder_id
        with promoter_folder_lock(cache_key):
            if cache_key in _PROMOTER_FOLDER_CACHE:
                folder_id = _PROMOTER_FOLDER_CACHE[cache_key]
                self._folder_cache[cache_key] = folder_id
                return folder_id
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
                                "pdw_source": "apple_notes",
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
            _PROMOTER_FOLDER_CACHE[cache_key] = folder_id
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


def iter_drive_metadata_payloads(
    *,
    service,
    folder_id: str,
    stage: str = "inbox",
    download_service_factory: Callable[[], Any] | None = None,
    download_workers: int = 1,
) -> Iterable[Mapping[str, Any]]:
    metadata_files = list(iter_drive_files_by_kind(service=service, folder_id=folder_id, kind="apple_note_revision_metadata", stage=stage))
    html_files_by_revision = {
        drive_note_revision_key(file): file
        for file in iter_drive_files_by_kind(service=service, folder_id=folder_id, kind="apple_note_body_html", stage=stage)
    }
    attachment_files_by_revision: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    attachment_files_by_sha: dict[str, Mapping[str, Any]] = {}
    for file in iter_drive_files_by_kind(service=service, folder_id=folder_id, kind="apple_note_attachment", stage=""):
        properties = nested_mapping(file, "appProperties")
        note_id = str(properties.get("note_id", ""))
        revision_id = str(properties.get("revision_id", ""))
        attachment_id = str(properties.get("attachment_id", ""))
        content_sha256 = str(properties.get("content_sha256", ""))
        if note_id and revision_id and attachment_id:
            attachment_files_by_revision[(note_id, revision_id, attachment_id)] = file
        if content_sha256:
            attachment_files_by_sha.setdefault(content_sha256, file)

    download_workers = max(1, download_workers)
    if download_workers == 1 or len(metadata_files) <= 1:
        for file in metadata_files:
            yield attach_drive_metadata_file(
                file=file,
                service=service,
                folder_id=folder_id,
                stage=stage,
                html_files_by_revision=html_files_by_revision,
                attachment_files_by_revision=attachment_files_by_revision,
                attachment_files_by_sha=attachment_files_by_sha,
            )
        return

    thread_local = threading.local()

    def service_for_thread():
        if download_service_factory is None:
            return service
        thread_service = getattr(thread_local, "service", None)
        if thread_service is None:
            thread_service = download_service_factory()
            thread_local.service = thread_service
        return thread_service

    def download_file(file: Mapping[str, Any]) -> Mapping[str, Any]:
        return attach_drive_metadata_file(
            file=file,
            service=service_for_thread(),
            folder_id=folder_id,
            stage=stage,
            html_files_by_revision=html_files_by_revision,
            attachment_files_by_revision=attachment_files_by_revision,
            attachment_files_by_sha=attachment_files_by_sha,
        )

    with ThreadPoolExecutor(max_workers=download_workers, thread_name_prefix="apple-notes-download") as executor:
        futures = [executor.submit(download_file, file) for file in metadata_files]
        for future in as_completed(futures):
            yield future.result()


def attach_drive_metadata_file(
    *,
    file: Mapping[str, Any],
    service,
    folder_id: str,
    stage: str,
    html_files_by_revision: Mapping[tuple[str, str], Mapping[str, Any]],
    attachment_files_by_revision: Mapping[tuple[str, str, str], Mapping[str, Any]],
    attachment_files_by_sha: Mapping[str, Mapping[str, Any]],
) -> Mapping[str, Any]:
    file_id = str(file.get("id", ""))
    if file_id:
        payload = dict(download_drive_json(service=service, file_id=file_id))
        return attach_storage_context(
            payload,
            service=service,
            folder_id=folder_id,
            stage=stage,
            metadata_file=file,
            html_files_by_revision=html_files_by_revision,
            attachment_files_by_revision=attachment_files_by_revision,
            attachment_files_by_sha=attachment_files_by_sha,
        )
    return {}


def iter_drive_files_by_kind(
    *,
    service,
    folder_id: str,
    kind: str,
    stage: str,
) -> Iterable[Mapping[str, Any]]:
    page_token: str | None = None
    while True:
        response = execute_drive_request(
            service.files().list(
                q=drive_files_query(folder_id=folder_id, kind=kind, stage=stage),
                pageSize=1000,
                pageToken=page_token,
                fields="nextPageToken,files(id,name,webViewLink,appProperties)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )
        for file in response.get("files", []):
            if isinstance(file, Mapping):
                yield file
        page_token = response.get("nextPageToken")
        if not page_token:
            return


def has_drive_metadata_payloads(*, service, folder_id: str, stage: str = "inbox") -> bool:
    response = execute_drive_request(
        service.files().list(
            q=drive_metadata_files_query(folder_id=folder_id, stage=stage),
            pageSize=1,
            fields="files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
    )
    return bool(response.get("files", [])) if isinstance(response, Mapping) else False


def drive_metadata_files_query(*, folder_id: str, stage: str) -> str:
    return drive_files_query(folder_id=folder_id, kind="apple_note_revision_metadata", stage=stage)


def drive_files_query(*, folder_id: str, kind: str, stage: str) -> str:
    query = (
        "trashed = false "
        f"and {APPLE_NOTES_DRIVE_SOURCE_QUERY} "
        f"and appProperties has {{ key='pdw_root_folder_id' and value='{escape_drive_query_value(folder_id)}' }} "
        f"and appProperties has {{ key='pdw_kind' and value='{escape_drive_query_value(kind)}' }} "
    )
    if stage:
        query += f"and appProperties has {{ key='pdw_stage' and value='{escape_drive_query_value(stage)}' }}"
    return query


def drive_note_revision_key(file: Mapping[str, Any]) -> tuple[str, str]:
    properties = nested_mapping(file, "appProperties")
    return (
        str(properties.get("note_id", "")),
        str(properties.get("revision_id", "")),
    )


def attach_storage_context(
    payload: Mapping[str, Any],
    *,
    service,
    folder_id: str,
    stage: str,
    metadata_file: Mapping[str, Any],
    html_files_by_revision: Mapping[tuple[str, str], Mapping[str, Any]] | None = None,
    attachment_files_by_revision: Mapping[tuple[str, str, str], Mapping[str, Any]] | None = None,
    attachment_files_by_sha: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    decorated = clean_metadata_payload(payload)
    note = nested_mapping(decorated, "note")
    revision_id = str(note.get("revision_id", ""))
    note_id = str(note.get("note_id", ""))
    decorated["metadata_file"] = stored_file_context(
        storage_key=metadata_storage_key(decorated, stage=stage),
        file=metadata_file,
        extra={"content_sha256": str(nested_mapping(metadata_file, "appProperties").get("content_sha256", ""))},
    )
    if not bool(note.get("is_deleted", False)):
        html_file = (
            html_files_by_revision.get((note_id, revision_id), {})
            if html_files_by_revision is not None
            else find_drive_object(
                service=service,
                folder_id=folder_id,
                kind="apple_note_body_html",
                stage=stage,
                properties={"note_id": note_id, "revision_id": revision_id},
            )
        )
        if html_file:
            decorated["html_file"] = stored_file_context(
                storage_key=html_storage_key(decorated, stage=stage),
                file=html_file,
                extra={"content_sha256": str(nested_mapping(html_file, "appProperties").get("content_sha256", ""))},
            )
    attachments: list[dict[str, Any]] = []
    for raw_attachment in note.get("attachments", []):
        if not isinstance(raw_attachment, Mapping):
            continue
        attachment = deepcopy(dict(raw_attachment))
        attachment_id = str(attachment.get("attachment_id", ""))
        content_sha256 = str(attachment.get("content_sha256", ""))
        file = {}
        if attachment_files_by_revision is not None:
            file = attachment_files_by_revision.get((note_id, revision_id, attachment_id), {})
        else:
            file = find_drive_object(
                service=service,
                folder_id=folder_id,
                kind="apple_note_attachment",
                stage=stage,
                properties={"note_id": note_id, "revision_id": revision_id, "attachment_id": attachment_id},
            )
        if not file and content_sha256:
            file = (
                attachment_files_by_sha.get(content_sha256, {})
                if attachment_files_by_sha is not None
                else find_drive_object(
                    service=service,
                    folder_id=folder_id,
                    kind="apple_note_attachment",
                    stage="",
                    properties={"content_sha256": content_sha256},
                )
            )
        if file:
            attachment["file"] = stored_file_context(
                storage_key=attachment_storage_key(decorated, attachment, stage=stage),
                file=file,
            )
        attachments.append(attachment)
    note_copy = dict(note)
    note_copy["attachments"] = attachments
    decorated["note"] = note_copy
    return decorated


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


def find_drive_object(
    *,
    service,
    folder_id: str,
    kind: str,
    stage: str,
    properties: Mapping[str, str],
) -> Mapping[str, Any]:
    query = (
        "trashed = false "
        f"and {APPLE_NOTES_DRIVE_SOURCE_QUERY} "
        f"and appProperties has {{ key='pdw_root_folder_id' and value='{escape_drive_query_value(folder_id)}' }} "
        f"and appProperties has {{ key='pdw_kind' and value='{escape_drive_query_value(kind)}' }} "
    )
    if stage:
        query += f"and appProperties has {{ key='pdw_stage' and value='{escape_drive_query_value(stage)}' }} "
    for key, value in properties.items():
        query += f"and appProperties has {{ key='{escape_drive_query_value(key)}' and value='{escape_drive_query_value(value)}' }} "
    response = execute_drive_request(
        service.files().list(
            q=query,
            pageSize=1,
            fields="files(id,name,webViewLink,appProperties)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
    )
    files = response.get("files", []) if isinstance(response, dict) else []
    first = files[0] if files else None
    return first if isinstance(first, Mapping) else {}


def stored_file_context(
    *,
    storage_key: str,
    file: Mapping[str, Any],
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    context = {
        "storage_backend": "google_drive",
        "storage_key": storage_key,
        "storage_file_id": str(file.get("id", "")),
        "storage_url": str(file.get("webViewLink", "")),
    }
    context.update(extra or {})
    return context


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


def storage_stage(storage_key: str) -> str:
    parts = [part for part in storage_key.split("/") if part]
    if len(parts) > 1 and parts[1] in {"inbox", "library"}:
        return parts[1]
    return ""


def storage_stage_from_folder_name(name: str) -> str:
    return name if name in {"inbox", "library"} else ""


def promoter_folder_lock(cache_key: tuple[str, str]) -> threading.Lock:
    with _PROMOTER_FOLDER_LOCKS_LOCK:
        lock = _PROMOTER_FOLDER_LOCKS.get(cache_key)
        if lock is None:
            lock = threading.Lock()
            _PROMOTER_FOLDER_LOCKS[cache_key] = lock
        return lock


def nested_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    nested = value.get(key)
    return nested if isinstance(nested, Mapping) else {}


def safe_object_key_part(value: str) -> str:
    import re

    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip(".-")[:120] or "untitled"


def extension_from_filename(filename: str) -> str:
    suffix = filename.rsplit(".", 1)
    if len(suffix) == 2 and suffix[1]:
        return "." + suffix[1]
    return ".bin"
