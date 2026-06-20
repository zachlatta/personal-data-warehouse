"""Ingest Google Drive files (metadata + extracted text) into the warehouse.

This is Google Drive *as a data source*, distinct from Drive-as-transport used
by the object-storage backends. Files are mirrored into ``google_drive_files``
and their text into ``google_drive_file_texts`` so they surface through the unified
``search_text()`` function. Bytes are not copied: rows carry the Drive file id so
the Go app's account-aware download proxy can stream them on demand.

Incremental sync uses the Drive Changes API: a full ``files.list`` crawl on the
first run captures everything and records a ``startPageToken``; later runs read
``changes.list`` from that token. The warehouse's own transport folders are
excluded by walking each file's folder ancestry against the configured
excluded folder ids.
"""

from __future__ import annotations

import fnmatch
import hashlib
import logging
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
from typing import Any, Callable, Protocol

from googleapiclient.http import MediaIoBaseDownload

from personal_data_warehouse.config import GoogleDriveSourceConfig, Settings

logger = logging.getLogger(__name__)

FOLDER_MIME = "application/vnd.google-apps.folder"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Native Google formats we can export to text. Everything else under the
# vnd.google-apps.* namespace (drawings, forms, sites, ...) has no useful text
# export and is recorded as unsupported.
GOOGLE_EXPORT_MIME = {
    "application/vnd.google-apps.document": "text/plain",
    "application/vnd.google-apps.spreadsheet": "text/csv",
    "application/vnd.google-apps.presentation": "text/plain",
}

_PLAIN_TEXT_MIME_EXACT = {
    "application/json",
    "application/xml",
    "application/x-yaml",
    "application/yaml",
    "application/javascript",
    "application/x-sh",
    "application/csv",
    "application/x-ndjson",
}

_PLAIN_TEXT_EXTENSIONS = (
    ".txt", ".text", ".md", ".markdown", ".rst", ".json", ".ndjson", ".csv", ".tsv",
    ".yaml", ".yml", ".xml", ".toml", ".ini", ".cfg", ".conf", ".env", ".log",
    ".py", ".js", ".ts", ".tsx", ".jsx", ".go", ".rs", ".java", ".kt", ".c", ".h",
    ".cc", ".cpp", ".hpp", ".rb", ".php", ".pl", ".sh", ".bash", ".zsh", ".sql",
    ".html", ".htm", ".css", ".scss", ".svg", ".tex", ".r", ".lua", ".swift",
)

# Drive file fields requested for every file/change.
FILE_FIELDS = (
    "id,name,mimeType,parents,size,md5Checksum,modifiedTime,createdTime,"
    "viewedByMeTime,webViewLink,iconLink,owners(displayName,emailAddress),"
    "lastModifyingUser(displayName,emailAddress),trashed,shared,starred,driveId"
)

MAX_BUFFERED_FILE_ROWS = 200
MAX_BUFFERED_TEXT_ROWS = 25
MAX_BUFFERED_TEXT_CHARS = 10_000_000


def _is_plain_text(mime_type: str, name: str) -> bool:
    if mime_type.startswith("text/"):
        return True
    if mime_type in _PLAIN_TEXT_MIME_EXACT:
        return True
    lowered = name.lower()
    return any(lowered.endswith(ext) for ext in _PLAIN_TEXT_EXTENSIONS)


@dataclass(frozen=True)
class ExtractionPlan:
    mode: str  # "export" | "download" | "skip"
    extractor: str  # drive_export | plain | "" (skip)
    export_mime: str
    skip_status: str  # only meaningful when mode == "skip"


def decide_extraction(*, mime_type: str, name: str, size_bytes: int, config: GoogleDriveSourceConfig) -> ExtractionPlan:
    """Choose how (if at all) to extract searchable text from a Drive file."""
    if mime_type in GOOGLE_EXPORT_MIME:
        # Native Google files report size 0; the export size cap is enforced by
        # the Drive API and surfaces as an export error we record at runtime.
        return ExtractionPlan("export", "drive_export", GOOGLE_EXPORT_MIME[mime_type], "")
    if mime_type.startswith("application/vnd.google-apps."):
        return ExtractionPlan("skip", "", "", "unsupported")
    if size_bytes and size_bytes > config.extract_max_bytes:
        return ExtractionPlan("skip", "", "", "too_large")
    if _is_plain_text(mime_type, name):
        return ExtractionPlan("download", "plain", "", "")
    # PDF/Office/images are Phase 3 (binary extraction); recorded unsupported now.
    return ExtractionPlan("skip", "", "", "unsupported")


class FolderTree:
    """Resolve a file's folder path and exclusion from a folder-id -> node map.

    ``folders`` maps each non-trashed folder id to a node with ``name`` and
    ``parents``. Drive folders can have multiple parents; we follow the first
    parent chain, which matches how the path is displayed to the user.
    """

    def __init__(self, folders: Mapping[str, Mapping[str, Any]]) -> None:
        self._folders = folders
        self._chain_cache: dict[str, tuple[tuple[str, ...], frozenset[str]]] = {}

    def _chain(self, folder_id: str) -> tuple[tuple[str, ...], frozenset[str]]:
        cached = self._chain_cache.get(folder_id)
        if cached is not None:
            return cached
        names: list[str] = []
        ids: set[str] = set()
        seen: set[str] = set()
        cursor = folder_id
        while cursor and cursor in self._folders and cursor not in seen:
            seen.add(cursor)
            ids.add(cursor)
            node = self._folders[cursor]
            names.append(str(node.get("name", "")))
            parents = node.get("parents") or []
            cursor = parents[0] if parents else ""
        names.reverse()
        result = (tuple(names), frozenset(ids))
        self._chain_cache[folder_id] = result
        return result

    def resolve(
        self,
        file: Mapping[str, Any],
        *,
        exclude_ids: frozenset[str],
        exclude_globs: Sequence[str],
    ) -> tuple[str, bool, str]:
        parents = file.get("parents") or []
        primary = parents[0] if parents else ""
        names, ancestor_ids = self._chain(primary)
        folder_path = "/" + "/".join(names) if names else ""
        if ancestor_ids & exclude_ids:
            return folder_path, True, "excluded_folder"
        if folder_path and any(fnmatch.fnmatch(folder_path, glob) for glob in exclude_globs):
            return folder_path, True, "excluded_path"
        return folder_path, False, ""


class DriveClient(Protocol):
    def get_start_page_token(self) -> str: ...
    def iter_folders(self) -> Iterable[Mapping[str, Any]]: ...
    def iter_files(self) -> Iterable[Mapping[str, Any]]: ...
    def iter_changes(self, page_token: str, *, max_changes: int | None = None) -> tuple[list[Mapping[str, Any]], str]: ...
    def export_text(self, file_id: str, export_mime: str) -> bytes: ...
    def download_bytes(self, file_id: str) -> bytes: ...


@dataclass
class AccountSyncSummary:
    account: str
    status: str
    sync_type: str
    files_seen: int
    files_written: int
    texts_written: int
    error: str = ""


def _parse_dt(value: Any) -> datetime:
    if not value:
        return _EPOCH
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return _EPOCH


class GoogleDriveSourceSyncRunner:
    def __init__(
        self,
        *,
        settings: Settings,
        warehouse: Any,
        client_factory: Callable[[str], DriveClient],
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._settings = settings
        self._warehouse = warehouse
        self._config = settings.google_drive_source
        self._client_factory = client_factory
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync_all(self) -> list[AccountSyncSummary]:
        if self._config is None:
            return []
        self._warehouse.ensure_google_drive_source_tables()
        states = self._warehouse.load_google_drive_sync_state()
        summaries: list[AccountSyncSummary] = []
        for account in self._config.accounts:
            try:
                summaries.append(self._sync_account(account, states.get(account)))
            except Exception as exc:  # noqa: BLE001 - per-account isolation
                logger.exception("Google Drive source sync failed for %s", account)
                summaries.append(
                    AccountSyncSummary(account, "failed", "unknown", 0, 0, 0, error=str(exc))
                )
                self._warehouse.upsert_google_drive_sync_state(
                    self._state_row(
                        account=account,
                        start_page_token=getattr(states.get(account), "start_page_token", ""),
                        sync_type="unknown",
                        status="failed",
                        error=str(exc),
                        files_seen=0,
                        full_crawled_at=getattr(states.get(account), "full_crawled_at", _EPOCH),
                    )
                )
        return summaries

    def _sync_account(self, account: str, state: Any) -> AccountSyncSummary:
        client = self._client_factory(account)
        if state is None or not getattr(state, "start_page_token", ""):
            return self._full_crawl(account, client)
        return self._incremental(account, client, state)

    def _build_tree(self, client: DriveClient) -> FolderTree:
        folders: dict[str, Mapping[str, Any]] = {}
        for folder in client.iter_folders():
            folder_id = str(folder.get("id", ""))
            if folder_id:
                folders[folder_id] = folder
        return FolderTree(folders)

    def _full_crawl(self, account: str, client: DriveClient) -> AccountSyncSummary:
        synced_at = self._now()
        start_token = client.get_start_page_token()
        tree = self._build_tree(client)
        known_text_state = self._warehouse.load_google_drive_text_state(account)
        file_rows: list[dict[str, Any]] = []
        text_rows: list[dict[str, Any]] = []
        seen = 0
        files_written = 0
        texts_written = 0
        for file in client.iter_files():
            seen += 1
            self._process_file(account, client, file, tree, known_text_state, synced_at, file_rows, text_rows)
            if self._should_flush(file_rows, text_rows):
                files, texts = self._flush_buffer(file_rows, text_rows)
                files_written += files
                texts_written += texts
        files, texts = self._flush_buffer(file_rows, text_rows)
        files_written += files
        texts_written += texts
        self._warehouse.upsert_google_drive_sync_state(
            self._state_row(
                account=account,
                start_page_token=start_token,
                sync_type="full_crawl",
                status="ok",
                error="",
                files_seen=seen,
                synced_at=synced_at,
            )
        )
        return AccountSyncSummary(account, "ok", "full_crawl", seen, files_written, texts_written)

    def _incremental(self, account: str, client: DriveClient, state: Any) -> AccountSyncSummary:
        synced_at = self._now()
        changes, new_token = client.iter_changes(
            state.start_page_token,
            max_changes=self._config.files_per_run,
        )
        tree = self._build_tree(client)
        known_text_state = self._warehouse.load_google_drive_text_state(account)
        file_rows: list[dict[str, Any]] = []
        text_rows: list[dict[str, Any]] = []
        trashed_ids: list[str] = []
        seen = 0
        files_written = 0
        texts_written = 0
        for change in changes:
            seen += 1
            file = change.get("file")
            file_id = str(change.get("fileId", "") or (file or {}).get("id", ""))
            if change.get("removed") or (file or {}).get("trashed") or file is None:
                if file_id:
                    trashed_ids.append(file_id)
                continue
            if file.get("mimeType") == FOLDER_MIME:
                continue
            self._process_file(account, client, file, tree, known_text_state, synced_at, file_rows, text_rows)
            if self._should_flush(file_rows, text_rows):
                files, texts = self._flush_buffer(file_rows, text_rows)
                files_written += files
                texts_written += texts
        files, texts = self._flush_buffer(file_rows, text_rows)
        files_written += files
        texts_written += texts
        self._warehouse.mark_google_drive_files_trashed(
            account=account, file_ids=trashed_ids, sync_version=int(synced_at.timestamp() * 1000)
        )
        self._warehouse.upsert_google_drive_sync_state(
            self._state_row(
                account=account,
                start_page_token=new_token or state.start_page_token,
                sync_type="incremental",
                status="ok",
                error="",
                files_seen=seen,
                full_crawled_at=getattr(state, "full_crawled_at", _EPOCH),
                synced_at=synced_at,
            )
        )
        return AccountSyncSummary(account, "ok", "incremental", seen, files_written, texts_written)

    def _process_file(
        self,
        account: str,
        client: DriveClient,
        file: Mapping[str, Any],
        tree: FolderTree,
        known_text_state: Mapping[str, tuple[datetime, str]],
        synced_at: datetime,
        file_rows: list[dict[str, Any]],
        text_rows: list[dict[str, Any]],
    ) -> None:
        folder_path, is_excluded, reason = tree.resolve(
            file,
            exclude_ids=frozenset(self._config.exclude_folder_ids),
            exclude_globs=self._config.exclude_path_globs,
        )
        file_id = str(file.get("id", ""))
        modified_time = _parse_dt(file.get("modifiedTime"))
        content_sha256 = ""
        if not is_excluded:
            plan = decide_extraction(
                mime_type=str(file.get("mimeType", "")),
                name=str(file.get("name", "")),
                size_bytes=int(file.get("size") or 0),
                config=self._config,
            )
            already = known_text_state.get(file_id)
            if plan.mode in {"export", "download"} and already is not None and already[0] == modified_time:
                content_sha256 = already[1]
            elif plan.mode in {"export", "download"}:
                text, status, error, content_sha256, truncated = self._extract(client, file_id, plan)
                text_rows.append(
                    self._text_row(
                        account=account,
                        file_id=file_id,
                        content_sha256=content_sha256,
                        extractor=plan.extractor,
                        text=text,
                        status=status,
                        error=error,
                        truncated=truncated,
                        modified_time=modified_time,
                        synced_at=synced_at,
                    )
                )
            elif plan.mode == "skip":
                text_rows.append(
                    self._text_row(
                        account=account,
                        file_id=file_id,
                        content_sha256="",
                        extractor="none",
                        text="",
                        status=plan.skip_status,
                        error="",
                        truncated=0,
                        modified_time=modified_time,
                        synced_at=synced_at,
                    )
                )
        file_rows.append(
            self._file_row(
                account=account,
                file=file,
                folder_path=folder_path,
                is_excluded=is_excluded,
                reason=reason,
                content_sha256=content_sha256,
                synced_at=synced_at,
            )
        )

    def _extract(
        self, client: DriveClient, file_id: str, plan: ExtractionPlan
    ) -> tuple[str, str, str, str, int]:
        try:
            if plan.mode == "export":
                data = client.export_text(file_id, plan.export_mime)
            else:
                data = client.download_bytes(file_id)
        except Exception as exc:  # noqa: BLE001 - extraction is best-effort
            return "", "error", str(exc)[:500], "", 0
        content_sha256 = hashlib.sha256(data).hexdigest()
        text = data.decode("utf-8", errors="replace")
        truncated = 0
        if len(text) > self._config.text_max_chars:
            text = text[: self._config.text_max_chars]
            truncated = 1
        status = "ok" if text.strip() else "empty"
        return text, status, "", content_sha256, truncated

    def _should_flush(self, file_rows: list[dict[str, Any]], text_rows: list[dict[str, Any]]) -> bool:
        if len(file_rows) >= MAX_BUFFERED_FILE_ROWS or len(text_rows) >= MAX_BUFFERED_TEXT_ROWS:
            return True
        text_chars = sum(len(str(row.get("text", ""))) for row in text_rows)
        return text_chars >= MAX_BUFFERED_TEXT_CHARS

    def _flush_buffer(self, file_rows: list[dict[str, Any]], text_rows: list[dict[str, Any]]) -> tuple[int, int]:
        files_written = len(file_rows)
        texts_written = len(text_rows)
        if text_rows:
            self._warehouse.insert_google_drive_file_texts(text_rows)
        if file_rows:
            self._warehouse.insert_google_drive_files(file_rows)
        file_rows.clear()
        text_rows.clear()
        return files_written, texts_written

    def _file_row(
        self,
        *,
        account: str,
        file: Mapping[str, Any],
        folder_path: str,
        is_excluded: bool,
        reason: str,
        content_sha256: str,
        synced_at: datetime,
    ) -> dict[str, Any]:
        parents = list(file.get("parents") or [])
        mime = str(file.get("mimeType", ""))
        last_user = file.get("lastModifyingUser") or {}
        return {
            "account": account,
            "file_id": str(file.get("id", "")),
            "drive_id": str(file.get("driveId", "")),
            "name": str(file.get("name", "")),
            "mime_type": mime,
            "is_google_native": 1 if mime.startswith("application/vnd.google-apps.") else 0,
            "parents_json": parents,
            "folder_path": folder_path,
            "parent_folder_id": parents[0] if parents else "",
            "size_bytes": int(file.get("size") or 0),
            "md5_checksum": str(file.get("md5Checksum", "")),
            "content_sha256": content_sha256,
            "web_view_link": str(file.get("webViewLink", "")),
            "icon_link": str(file.get("iconLink", "")),
            "owners_json": list(file.get("owners") or []),
            "last_modifying_user": str(last_user.get("displayName") or last_user.get("emailAddress") or ""),
            "created_time": _parse_dt(file.get("createdTime")),
            "modified_time": _parse_dt(file.get("modifiedTime")),
            "viewed_by_me_time": _parse_dt(file.get("viewedByMeTime")),
            "starred": 1 if file.get("starred") else 0,
            "shared": 1 if file.get("shared") else 0,
            "trashed": 1 if file.get("trashed") else 0,
            "is_excluded": 1 if is_excluded else 0,
            "exclude_reason": reason,
            "storage_backend": "google_drive_source",
            "storage_key": "",
            "storage_file_id": str(file.get("id", "")),
            "storage_url": str(file.get("webViewLink", "")),
            "storage_status": "available",
            "raw_metadata_json": dict(file),
            "ingested_at": synced_at,
            "sync_version": int(synced_at.timestamp() * 1000),
        }

    def _text_row(
        self,
        *,
        account: str,
        file_id: str,
        content_sha256: str,
        extractor: str,
        text: str,
        status: str,
        error: str,
        truncated: int,
        modified_time: datetime,
        synced_at: datetime,
    ) -> dict[str, Any]:
        return {
            "account": account,
            "file_id": file_id,
            "content_sha256": content_sha256,
            "extractor": extractor,
            "extractor_version": "1",
            "text": text,
            "text_extraction_status": status,
            "text_extraction_error": error,
            "char_count": len(text),
            "truncated": truncated,
            "source_modified_time": modified_time,
            "extracted_at": synced_at,
            "sync_version": int(synced_at.timestamp() * 1000),
        }

    def _state_row(
        self,
        *,
        account: str,
        start_page_token: str,
        sync_type: str,
        status: str,
        error: str,
        files_seen: int,
        full_crawled_at: datetime | None = None,
        synced_at: datetime | None = None,
    ) -> dict[str, Any]:
        synced_at = synced_at or self._now()
        if sync_type == "full_crawl":
            full_crawled_at = synced_at
        elif full_crawled_at is None:
            full_crawled_at = _EPOCH
        return {
            "account": account,
            "start_page_token": start_page_token,
            "last_page_token": start_page_token,
            "drive_id": "",
            "last_sync_type": sync_type,
            "status": status,
            "error": error,
            "full_crawled_at": full_crawled_at,
            "files_seen": files_seen,
            "updated_at": synced_at,
            "sync_version": int(synced_at.timestamp() * 1000),
        }


class GoogleApiDriveClient:
    """Drive v3 client adapting googleapiclient to the DriveClient protocol."""

    def __init__(self, service: Any, *, config: GoogleDriveSourceConfig) -> None:
        self._service = service
        self._config = config

    def _shared_drive_params(self) -> dict[str, Any]:
        if self._config.include_shared_drives:
            return {
                "includeItemsFromAllDrives": True,
                "supportsAllDrives": True,
                "corpora": "allDrives",
            }
        return {"supportsAllDrives": True}

    def get_start_page_token(self) -> str:
        resp = self._service.changes().getStartPageToken(supportsAllDrives=True).execute()
        return str(resp.get("startPageToken", ""))

    def iter_folders(self) -> Iterator[Mapping[str, Any]]:
        try:
            root = self._service.files().get(
                fileId="root",
                fields="id,name,parents,driveId",
                supportsAllDrives=True,
            ).execute()
        except Exception as exc:  # noqa: BLE001 - root naming is metadata quality, not sync-critical
            logger.warning("Google Drive root folder lookup failed: %s", exc)
        else:
            root_id = str(root.get("id", ""))
            if root_id:
                yield {"id": root_id, "name": "My Drive", "parents": [], "driveId": root.get("driveId", "")}
        yield from self._list(
            f"mimeType = '{FOLDER_MIME}' and trashed = false",
            "nextPageToken,files(id,name,parents,driveId)",
        )

    def iter_files(self) -> Iterator[Mapping[str, Any]]:
        clauses = ["trashed = false", f"mimeType != '{FOLDER_MIME}'"]
        if not self._config.include_shared_with_me and not self._config.include_shared_drives:
            clauses.append("'me' in owners")
        yield from self._list(" and ".join(clauses), f"nextPageToken,files({FILE_FIELDS})")

    def _list(self, query: str, fields: str) -> Iterator[Mapping[str, Any]]:
        page_token = ""
        while True:
            params: dict[str, Any] = {
                "q": query,
                "fields": fields,
                "pageSize": 1000,
                **self._shared_drive_params(),
            }
            if page_token:
                params["pageToken"] = page_token
            response = self._service.files().list(**params).execute()
            yield from response.get("files", [])
            page_token = response.get("nextPageToken", "")
            if not page_token:
                return

    def iter_changes(self, page_token: str, *, max_changes: int | None = None) -> tuple[list[Mapping[str, Any]], str]:
        changes: list[Mapping[str, Any]] = []
        token = page_token
        new_start = page_token
        while token:
            params: dict[str, Any] = {
                "pageToken": token,
                "includeRemoved": True,
                "fields": f"newStartPageToken,nextPageToken,changes(fileId,removed,file({FILE_FIELDS}))",
                **self._shared_drive_params(),
            }
            response = self._service.changes().list(**params).execute()
            changes.extend(response.get("changes", []))
            if response.get("newStartPageToken"):
                new_start = str(response["newStartPageToken"])
            next_token = response.get("nextPageToken", "")
            if max_changes is not None and len(changes) >= max_changes and next_token:
                return changes, str(next_token)
            token = next_token
        return changes, new_start

    def export_text(self, file_id: str, export_mime: str) -> bytes:
        request = self._service.files().export_media(fileId=file_id, mimeType=export_mime)
        return self._download(request)

    def download_bytes(self, file_id: str) -> bytes:
        request = self._service.files().get_media(fileId=file_id, supportsAllDrives=True)
        return self._download(request)

    @staticmethod
    def _download(request: Any) -> bytes:
        buffer = BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk(num_retries=2)
        return buffer.getvalue()


def build_drive_client_factory(settings: Settings) -> Callable[[str], DriveClient]:
    config = settings.google_drive_source
    if config is None:
        raise ValueError("google_drive_source is not configured")

    def factory(account: str) -> DriveClient:
        # Imported lazily so unit tests can inject a fake factory without the
        # googleapiclient discovery dependency.
        from personal_data_warehouse.objectstore.google_drive import build_google_drive_service

        service = build_google_drive_service(
            account=account,
            settings=settings,
            request_timeout_seconds=config.request_timeout_seconds,
        )
        return GoogleApiDriveClient(service, config=config)

    return factory


def main() -> None:
    import argparse
    import json

    from personal_data_warehouse.config import load_settings
    from personal_data_warehouse.warehouse import warehouse_from_settings

    parser = argparse.ArgumentParser(description="Sync Google Drive files into the personal data warehouse.")
    parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings(require_gmail=False, require_google_drive_source=True)
    warehouse = warehouse_from_settings(settings)
    summaries = GoogleDriveSourceSyncRunner(
        settings=settings,
        warehouse=warehouse,
        client_factory=build_drive_client_factory(settings),
    ).sync_all()
    print(json.dumps([vars(summary) for summary in summaries], sort_keys=True, default=str))


if __name__ == "__main__":
    main()
