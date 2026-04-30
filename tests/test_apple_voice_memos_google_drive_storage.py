from __future__ import annotations

from pathlib import Path

from personal_data_warehouse_voice_memos.google_drive_storage import GoogleDriveObjectStore


class FakeRequest:
    def __init__(self, response):
        self.response = response

    def execute(self):
        return self.response


class FakeFilesResource:
    def __init__(self) -> None:
        self.list_calls: list[dict[str, object]] = []
        self.create_calls: list[dict[str, object]] = []
        self.responses = [
            {"files": [{"id": "existing", "webViewLink": "https://drive/existing"}]},
            {"files": []},
            {"files": []},
            {"id": "folder-voice-memos", "webViewLink": "https://drive/folder-voice-memos"},
            {"files": []},
            {"id": "folder-inbox", "webViewLink": "https://drive/folder-inbox"},
            {"files": []},
            {"id": "folder-2026", "webViewLink": "https://drive/folder-2026"},
            {"files": []},
            {"id": "folder-04", "webViewLink": "https://drive/folder-04"},
            {"id": "created-file", "webViewLink": "https://drive/created-file"},
            {"files": []},
            {"id": "created-json", "webViewLink": "https://drive/created-json"},
        ]

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        return FakeRequest(self.responses.pop(0))

    def create(self, **kwargs):
        self.create_calls.append(kwargs)
        return FakeRequest(self.responses.pop(0))


class FakeDriveService:
    def __init__(self) -> None:
        self.files_resource = FakeFilesResource()

    def files(self):
        return self.files_resource


def test_google_drive_store_dedupes_and_uploads_with_app_properties(tmp_path) -> None:
    service = FakeDriveService()
    store = GoogleDriveObjectStore(folder_id="folder-id", service=service)
    source = tmp_path / "memo.qta"
    source.write_bytes(b"memo")

    assert store.has_blob(content_sha256="abc123") is True

    stored_file = store.put_file(
        path=source,
        object_key="apple-voice-memos/inbox/2026/04/2026-04-27-abc123.qta",
        content_sha256="def456",
        content_type="audio/quicktime",
    )
    stored_json = store.put_json(
        object_key="apple-voice-memos/inbox/2026/04/2026-04-27-def456.json",
        payload={"schema_version": 1},
        content_sha256="metadata-sha",
        source_content_sha256="def456",
    )

    assert stored_file["storage_backend"] == "google_drive"
    assert stored_file["storage_file_id"] == "created-file"
    assert stored_json["storage_file_id"] == "created-json"
    assert "appProperties has" in str(service.files_resource.list_calls[0]["q"])
    assert "pdw_stage" in str(service.files_resource.list_calls[0]["q"])
    assert "value='apple_voice_memos'" in str(service.files_resource.list_calls[0]["q"])
    assert "value='voice_memos'" in str(service.files_resource.list_calls[0]["q"])
    created_folders = [
        call["body"]["name"]
        for call in service.files_resource.create_calls
        if call["body"]["mimeType"] == "application/vnd.google-apps.folder"
    ]
    assert created_folders == ["apple-voice-memos", "inbox", "2026", "04"]
    file_body = service.files_resource.create_calls[4]["body"]
    json_body = service.files_resource.create_calls[5]["body"]
    assert file_body["name"] == "2026-04-27-abc123.qta"
    assert file_body["parents"] == ["folder-04"]
    assert file_body["appProperties"]["pdw_source"] == "apple_voice_memos"
    assert file_body["appProperties"]["pdw_root_folder_id"] == "folder-id"
    assert file_body["appProperties"]["pdw_stage"] == "inbox"
    assert "storage_key" not in file_body["appProperties"]
    assert file_body["appProperties"]["content_sha256"] == "def456"
    assert json_body["name"] == "2026-04-27-def456.json"
    assert json_body["parents"] == ["folder-04"]
    assert json_body["mimeType"] == "application/json"
    assert json_body["appProperties"]["pdw_kind"] == "voice_memo_metadata"
    assert json_body["appProperties"]["pdw_root_folder_id"] == "folder-id"
    assert json_body["appProperties"]["pdw_stage"] == "inbox"
    assert "storage_key" not in json_body["appProperties"]
