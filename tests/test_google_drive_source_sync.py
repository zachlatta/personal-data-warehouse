from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest
from dotenv import load_dotenv

from tests.conftest import cleanup_test_warehouse, make_test_schema

from personal_data_warehouse.config import GoogleDriveSourceConfig, Settings
from personal_data_warehouse.google_drive_source_sync import (
    FolderTree,
    GoogleDriveSourceSyncRunner,
    decide_extraction,
)
from personal_data_warehouse.postgres import PostgresWarehouse


def _postgres_url() -> str:
    load_dotenv()
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


@pytest.fixture()
def warehouse():
    schema = make_test_schema()
    wh = PostgresWarehouse(_postgres_url(), schema=schema)
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


# --- Fake Drive client -----------------------------------------------------


class FakeDriveClient:
    def __init__(self, *, folders, files, exports=None, downloads=None, changes=None):
        self.folders = folders
        self.files = files
        self.exports = exports or {}
        self.downloads = downloads or {}
        self._changes = changes or []
        self.export_calls: list[str] = []
        self.download_calls: list[str] = []

    def get_start_page_token(self) -> str:
        return "token-1"

    def iter_folders(self):
        return list(self.folders)

    def iter_files(self):
        return list(self.files)

    def iter_changes(self, page_token, *, max_changes=None):
        if max_changes is not None and len(self._changes) > max_changes:
            return list(self._changes[:max_changes]), "token-page-2"
        return list(self._changes), "token-2"

    def export_text(self, file_id, export_mime):
        self.export_calls.append(file_id)
        return self.exports[file_id]

    def download_bytes(self, file_id):
        self.download_calls.append(file_id)
        return self.downloads[file_id]


class RecordingWarehouse:
    def __init__(self):
        self.file_batches: list[int] = []
        self.text_batches: list[int] = []
        self.sync_states: list[dict] = []

    def ensure_google_drive_source_tables(self):
        pass

    def load_google_drive_sync_state(self):
        return {}

    def load_google_drive_text_state(self, account):
        return {}

    def insert_google_drive_files(self, rows):
        self.file_batches.append(len(rows))

    def insert_google_drive_file_texts(self, rows):
        self.text_batches.append(len(rows))

    def upsert_google_drive_sync_state(self, row):
        self.sync_states.append(row)


def _settings(**overrides) -> Settings:
    config = GoogleDriveSourceConfig(
        accounts=("zach@hackclub.com",),
        exclude_folder_ids=overrides.pop("exclude_folder_ids", ()),
        exclude_path_globs=overrides.pop("exclude_path_globs", ()),
        **overrides,
    )
    return Settings(
        gmail_accounts=(),
        gmail_oauth_client_secrets_json=None,
        gmail_scopes=(),
        gmail_page_size=100,
        gmail_include_spam_trash=True,
        gmail_force_full_sync=False,
        gmail_full_sync_query=None,
        gmail_attachment_max_bytes=0,
        gmail_attachment_text_max_chars=0,
        gmail_attachment_backfill_batch_size=0,
        slack_accounts=(),
        slack_page_size=0,
        slack_lookback_days=0,
        slack_thread_audit_days=0,
        slack_force_full_sync=False,
        google_drive_source=config,
    )


def _ensure_all_table_groups(wh: PostgresWarehouse) -> None:
    # search_text() is only created once every source table it reads exists.
    wh.ensure_tables()
    wh.ensure_calendar_tables()
    wh.ensure_contacts_tables()
    wh.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    wh.ensure_apple_notes_tables()
    wh.ensure_apple_messages_tables()
    wh.ensure_whatsapp_tables()
    wh.ensure_agent_sessions_tables()
    wh.ensure_slack_tables()
    wh.ensure_upstream_mutation_tables()
    wh.ensure_google_drive_source_tables()
    wh.ensure_timeline_tables()


def _now():
    return datetime(2026, 6, 19, 12, 0, 0, tzinfo=UTC)


def _runner(warehouse, client, settings=None):
    settings = settings or _settings()
    return GoogleDriveSourceSyncRunner(
        settings=settings,
        warehouse=warehouse,
        client_factory=lambda account: client,
        now=_now,
    )


# --- decide_extraction (pure) ---------------------------------------------


def test_decide_extraction_native_and_plain_and_unsupported():
    cfg = GoogleDriveSourceConfig(accounts=("a",), extract_max_bytes=1000)
    doc = decide_extraction(mime_type="application/vnd.google-apps.document", name="x", size_bytes=0, config=cfg)
    assert doc.mode == "export" and doc.extractor == "drive_export" and doc.export_mime == "text/plain"

    sheet = decide_extraction(mime_type="application/vnd.google-apps.spreadsheet", name="x", size_bytes=0, config=cfg)
    assert sheet.export_mime == "text/csv"

    md = decide_extraction(mime_type="text/markdown", name="notes.md", size_bytes=10, config=cfg)
    assert md.mode == "download" and md.extractor == "plain"

    pdf = decide_extraction(mime_type="application/pdf", name="x.pdf", size_bytes=10, config=cfg)
    assert pdf.mode == "skip" and pdf.skip_status == "unsupported"

    big = decide_extraction(mime_type="text/plain", name="x.txt", size_bytes=5000, config=cfg)
    assert big.mode == "skip" and big.skip_status == "too_large"

    form = decide_extraction(mime_type="application/vnd.google-apps.form", name="x", size_bytes=0, config=cfg)
    assert form.mode == "skip" and form.skip_status == "unsupported"


def test_folder_tree_resolves_my_drive_root_path():
    tree = FolderTree({"root-id": {"id": "root-id", "name": "My Drive", "parents": []}})

    folder_path, is_excluded, reason = tree.resolve(
        {"id": "f1", "parents": ["root-id"]},
        exclude_ids=frozenset(),
        exclude_globs=(),
    )

    assert folder_path == "/My Drive"
    assert is_excluded is False
    assert reason == ""


# --- full crawl ------------------------------------------------------------


def test_full_crawl_mirrors_metadata_and_text(warehouse):
    folders = [
        {"id": "root", "name": "My Drive", "parents": []},
        {"id": "docs", "name": "Docs", "parents": ["root"]},
    ]
    files = [
        {
            "id": "f_doc",
            "name": "Roadmap",
            "mimeType": "application/vnd.google-apps.document",
            "parents": ["docs"],
            "modifiedTime": "2026-06-01T00:00:00Z",
            "createdTime": "2026-01-01T00:00:00Z",
            "webViewLink": "https://drive/f_doc",
            "owners": [{"displayName": "Zach", "emailAddress": "zach@hackclub.com"}],
            "lastModifyingUser": {"displayName": "Zach"},
        },
        {
            "id": "f_txt",
            "name": "notes.md",
            "mimeType": "text/markdown",
            "parents": ["root"],
            "size": "20",
            "modifiedTime": "2026-06-02T00:00:00Z",
        },
    ]
    client = FakeDriveClient(
        folders=folders,
        files=files,
        exports={"f_doc": b"the pineapple roadmap for q3"},
        downloads={"f_txt": b"# markdown body kiwi"},
    )
    summary = _runner(warehouse, client).sync_all()[0]
    assert summary.status == "ok" and summary.sync_type == "full_crawl"
    assert summary.files_seen == 2 and summary.texts_written == 2

    rows = warehouse._query(
        "SELECT folder_path, is_excluded, storage_backend, storage_file_id FROM google_drive_files "
        "WHERE file_id = 'f_doc'"
    )
    assert rows == [("/My Drive/Docs", 0, "google_drive_source", "f_doc")]

    texts = warehouse._query(
        "SELECT extractor, text_extraction_status FROM google_drive_file_texts WHERE file_id = 'f_doc'"
    )
    assert texts == [("drive_export", "ok")]

    state = warehouse.load_google_drive_sync_state()["zach@hackclub.com"]
    assert state.start_page_token == "token-1"
    assert state.last_sync_type == "full_crawl"
    assert state.files_seen == 2


def test_full_crawl_excludes_transport_folders(warehouse):
    folders = [
        {"id": "root", "name": "My Drive", "parents": []},
        {"id": "transport", "name": "pdw-staging", "parents": ["root"]},
        {"id": "sub", "name": "whatsapp", "parents": ["transport"]},
    ]
    files = [
        {"id": "keep", "name": "keep.txt", "mimeType": "text/plain", "parents": ["root"], "size": "5",
         "modifiedTime": "2026-06-01T00:00:00Z"},
        {"id": "drop", "name": "blob.txt", "mimeType": "text/plain", "parents": ["sub"], "size": "5",
         "modifiedTime": "2026-06-01T00:00:00Z"},
    ]
    client = FakeDriveClient(
        folders=folders,
        files=files,
        downloads={"keep": b"keep me", "drop": b"secret transport blob"},
    )
    settings = _settings(exclude_folder_ids=("transport",))
    _runner(warehouse, client, settings).sync_all()

    excluded = warehouse._query(
        "SELECT file_id, is_excluded, exclude_reason FROM google_drive_files ORDER BY file_id"
    )
    assert excluded == [("drop", 1, "excluded_folder"), ("keep", 0, "")]
    # The excluded file's bytes were never downloaded and it has no text row.
    assert "drop" not in client.download_calls
    assert warehouse._query("SELECT count(*) FROM google_drive_file_texts WHERE file_id = 'drop'") == [(0,)]


def test_full_crawl_records_skip_status_for_unsupported_files(warehouse):
    client = FakeDriveClient(
        folders=[{"id": "root", "name": "My Drive", "parents": []}],
        files=[
            {
                "id": "pdf",
                "name": "scan.pdf",
                "mimeType": "application/pdf",
                "parents": ["root"],
                "size": "1024",
                "modifiedTime": "2026-06-01T00:00:00Z",
            },
        ],
    )

    _runner(warehouse, client).sync_all()

    assert warehouse._query(
        "SELECT extractor, text_extraction_status, text FROM google_drive_file_texts WHERE file_id = 'pdf'"
    ) == [("none", "unsupported", "")]


def test_full_crawl_flushes_batches_to_bound_memory():
    files = [
        {
            "id": f"f{i}",
            "name": f"note-{i}.txt",
            "mimeType": "text/plain",
            "parents": ["root"],
            "size": "10",
            "modifiedTime": "2026-06-01T00:00:00Z",
        }
        for i in range(30)
    ]
    client = FakeDriveClient(
        folders=[{"id": "root", "name": "My Drive", "parents": []}],
        files=files,
        downloads={f"f{i}": b"body" for i in range(30)},
    )
    warehouse = RecordingWarehouse()

    summary = _runner(warehouse, client).sync_all()[0]

    assert summary.files_written == 30
    assert summary.texts_written == 30
    assert warehouse.file_batches == [25, 5]
    assert warehouse.text_batches == [25, 5]
    assert warehouse.sync_states[-1]["status"] == "ok"


# --- incremental -----------------------------------------------------------


def _seed_full(warehouse):
    folders = [{"id": "root", "name": "My Drive", "parents": []}]
    files = [
        {"id": "f1", "name": "a.txt", "mimeType": "text/plain", "parents": ["root"], "size": "3",
         "modifiedTime": "2026-06-01T00:00:00Z"},
    ]
    client = FakeDriveClient(folders=folders, files=files, downloads={"f1": b"old"})
    _runner(warehouse, client).sync_all()


def test_incremental_upserts_and_soft_deletes(warehouse):
    _ensure_all_table_groups(warehouse)
    _seed_full(warehouse)

    folders = [{"id": "root", "name": "My Drive", "parents": []}]
    changes = [
        # f1 trashed.
        {"fileId": "f1", "removed": False, "file": {"id": "f1", "trashed": True}},
        # new file f2.
        {
            "fileId": "f2",
            "removed": False,
            "file": {
                "id": "f2",
                "name": "b.txt",
                "mimeType": "text/plain",
                "parents": ["root"],
                "size": "10",
                "modifiedTime": "2026-06-10T00:00:00Z",
            },
        },
        # hard-removed file f3.
        {"fileId": "f3", "removed": True},
    ]
    client = FakeDriveClient(
        folders=folders, files=[], changes=changes, downloads={"f2": b"brand new content mango"}
    )
    summary = _runner(warehouse, client).sync_all()[0]
    assert summary.sync_type == "incremental"

    assert warehouse._query("SELECT trashed FROM google_drive_files WHERE file_id = 'f1'") == [(1,)]
    assert warehouse._query("SELECT name FROM google_drive_files WHERE file_id = 'f2'") == [("b.txt",)]
    hits = warehouse._query(
        """
        SELECT f.account || ':' || f.file_id
        FROM google_drive_files f
        JOIN google_drive_file_texts t USING (account, file_id)
        WHERE f.trashed = 0 AND f.is_excluded = 0 AND t.text ILIKE '%mango%'
        """
    )
    assert hits == [("zach@hackclub.com:f2",)]
    # f1 is trashed -> should be filtered from search-style joins even though its text row remains.
    assert warehouse._query(
        """
        SELECT count(*)
        FROM google_drive_files f
        JOIN google_drive_file_texts t USING (account, file_id)
        WHERE f.trashed = 0 AND f.is_excluded = 0 AND f.account = 'zach@hackclub.com' AND f.file_id = 'f1'
        """
    ) == [(0,)]

    state = warehouse.load_google_drive_sync_state()["zach@hackclub.com"]
    assert state.start_page_token == "token-2"
    assert state.last_sync_type == "incremental"


def test_incremental_skips_reextraction_when_unchanged(warehouse):
    _seed_full(warehouse)
    folders = [{"id": "root", "name": "My Drive", "parents": []}]
    # f1 reappears in a change with the SAME modifiedTime (e.g. starred toggle).
    changes = [
        {
            "fileId": "f1",
            "removed": False,
            "file": {
                "id": "f1",
                "name": "a.txt",
                "mimeType": "text/plain",
                "parents": ["root"],
                "size": "3",
                "starred": True,
                "modifiedTime": "2026-06-01T00:00:00Z",
            },
        }
    ]
    client = FakeDriveClient(folders=folders, files=[], changes=changes, downloads={"f1": b"old"})
    _runner(warehouse, client).sync_all()
    # No re-download since modifiedTime is unchanged and an ok text row exists.
    assert client.download_calls == []
    # Metadata still updated (starred flag).
    assert warehouse._query("SELECT starred FROM google_drive_files WHERE file_id = 'f1'") == [(1,)]
    # The file row keeps pointing at the previously extracted content.
    assert warehouse._query("SELECT content_sha256 != '' FROM google_drive_files WHERE file_id = 'f1'") == [(True,)]


def test_incremental_preserves_full_crawl_timestamp(warehouse):
    _seed_full(warehouse)
    first_state = warehouse.load_google_drive_sync_state()["zach@hackclub.com"]

    client = FakeDriveClient(folders=[{"id": "root", "name": "My Drive", "parents": []}], files=[], changes=[])
    _runner(warehouse, client).sync_all()

    state = warehouse.load_google_drive_sync_state()["zach@hackclub.com"]
    assert state.last_sync_type == "incremental"
    assert state.full_crawled_at == first_state.full_crawled_at


def test_incremental_stores_page_token_when_capped(warehouse):
    _seed_full(warehouse)
    folders = [{"id": "root", "name": "My Drive", "parents": []}]
    changes = [
        {
            "fileId": "f2",
            "removed": False,
            "file": {
                "id": "f2",
                "name": "b.txt",
                "mimeType": "text/plain",
                "parents": ["root"],
                "size": "10",
                "modifiedTime": "2026-06-10T00:00:00Z",
            },
        },
        {
            "fileId": "f3",
            "removed": False,
            "file": {
                "id": "f3",
                "name": "c.txt",
                "mimeType": "text/plain",
                "parents": ["root"],
                "size": "10",
                "modifiedTime": "2026-06-10T00:00:00Z",
            },
        },
    ]
    client = FakeDriveClient(folders=folders, files=[], changes=changes, downloads={"f2": b"two", "f3": b"three"})
    settings = _settings(files_per_run=1)

    summary = _runner(warehouse, client, settings).sync_all()[0]

    assert summary.files_seen == 1
    assert warehouse._query("SELECT file_id FROM google_drive_files WHERE file_id IN ('f2', 'f3')") == [("f2",)]
    state = warehouse.load_google_drive_sync_state()["zach@hackclub.com"]
    assert state.start_page_token == "token-page-2"
