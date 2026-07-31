from __future__ import annotations

from personal_data_warehouse.objectstore.google_drive import GoogleDriveObjectStore


class FakeRequest:
    def __init__(self, response) -> None:
        self.response = response

    def execute(self, *args, **kwargs):
        return self.response


class FakeFiles:
    def __init__(self, *, list_responses=None, get_responses=None) -> None:
        self.list_calls: list[dict] = []
        self.create_calls: list[dict] = []
        self.get_calls: list[dict] = []
        self.update_calls: list[dict] = []
        self._list_responses = list(list_responses or [{"files": []}])
        self._get_responses = dict(get_responses or {})

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        response = self._list_responses.pop(0) if self._list_responses else {"files": []}
        return FakeRequest(response)

    def create(self, **kwargs):
        self.create_calls.append(kwargs)
        return FakeRequest({"id": f"folder-{kwargs['body']['name']}", "webViewLink": "https://drive/folder"})

    def get(self, **kwargs):
        self.get_calls.append(kwargs)
        response = self._get_responses.get(kwargs.get("fileId"), {"parents": ["old-parent"]})
        return FakeRequest(response)

    def update(self, **kwargs):
        self.update_calls.append(kwargs)
        return FakeRequest({"id": kwargs["fileId"], "webViewLink": "https://drive/promoted"})


class FakeService:
    def __init__(self, files: FakeFiles) -> None:
        self._files = files

    def files(self):
        return self._files


def voice_memos_store(files: FakeFiles) -> GoogleDriveObjectStore:
    return GoogleDriveObjectStore(folder_id="root-folder", service=FakeService(files))


def test_list_objects_query_uses_app_properties_not_parents() -> None:
    files = FakeFiles(
        list_responses=[
            {
                "files": [
                    {
                        "id": "f1",
                        "name": "rec.json",
                        "webViewLink": "https://drive/f1",
                        "appProperties": {"pdw_kind": "voice_memo_metadata", "content_sha256": "abc"},
                    }
                ]
            }
        ]
    )
    store = voice_memos_store(files)

    listings = store.list_objects(kind="voice_memo_metadata", stage="inbox")

    query = files.list_calls[0]["q"]
    assert "'root-folder' in parents" not in query
    assert "pdw_root_folder_id" in query and "root-folder" in query
    assert "value='apple_voice_memos'" in query
    assert "value='voice_memos'" in query  # legacy source preserved
    assert "pdw_kind' and value='voice_memo_metadata'" in query
    assert "pdw_stage' and value='inbox'" in query

    assert len(listings) == 1
    listing = listings[0]
    assert listing.ref["storage_file_id"] == "f1"
    assert listing.ref["storage_backend"] == "google_drive"
    assert listing.ref["storage_url"] == "https://drive/f1"
    assert listing.filename == "rec.json"
    assert listing.app_properties["content_sha256"] == "abc"


def test_list_objects_requests_parents_for_key_reconstruction() -> None:
    files = FakeFiles(list_responses=[{"files": []}])
    store = voice_memos_store(files)

    store.list_objects(kind="voice_memo_metadata", stage="inbox")

    assert "parents" in files.list_calls[0]["fields"]


def test_find_object_requests_parents_for_key_reconstruction() -> None:
    files = FakeFiles(list_responses=[{"files": []}])
    store = voice_memos_store(files)

    store.find_object(kind="voice_memo_audio", stage="inbox")

    assert "parents" in files.list_calls[0]["fields"]


def test_list_objects_reconstructs_storage_key_from_parent_chain() -> None:
    files = FakeFiles(
        list_responses=[
            {
                "files": [
                    {
                        "id": "f1",
                        "name": "2026-07-15-abc123.pdf",
                        "parents": ["fld-account"],
                        "appProperties": {"pdw_kind": "manual_finance_document"},
                    }
                ]
            }
        ],
        get_responses={
            "fld-account": {"id": "fld-account", "name": "chase-checking-1234", "parents": ["fld-inbox"]},
            "fld-inbox": {"id": "fld-inbox", "name": "inbox", "parents": ["fld-mf"]},
            "fld-mf": {"id": "fld-mf", "name": "manual-finance", "parents": ["root-folder"]},
        },
    )
    store = voice_memos_store(files)

    listings = store.list_objects(kind="manual_finance_document", stage="inbox")

    assert listings[0].ref["storage_key"] == (
        "manual-finance/inbox/chase-checking-1234/2026-07-15-abc123.pdf"
    )


def test_list_objects_storage_key_for_file_directly_under_root() -> None:
    files = FakeFiles(
        list_responses=[
            {"files": [{"id": "f1", "name": "top-level.json", "parents": ["root-folder"]}]}
        ]
    )
    store = voice_memos_store(files)

    listings = store.list_objects(kind="voice_memo_metadata", stage="")

    assert listings[0].ref["storage_key"] == "top-level.json"
    assert files.get_calls == []


def test_list_objects_caches_folder_lookups_across_files() -> None:
    files = FakeFiles(
        list_responses=[
            {
                "files": [
                    {"id": "f1", "name": "a.json", "parents": ["fld-batches"]},
                    {"id": "f2", "name": "b.json", "parents": ["fld-batches"]},
                ]
            }
        ],
        get_responses={
            "fld-batches": {"id": "fld-batches", "name": "batches", "parents": ["fld-inbox"]},
            "fld-inbox": {"id": "fld-inbox", "name": "inbox", "parents": ["root-folder"]},
        },
    )
    store = voice_memos_store(files)

    listings = store.list_objects(kind="voice_memo_metadata", stage="inbox")

    assert [listing.ref["storage_key"] for listing in listings] == [
        "inbox/batches/a.json",
        "inbox/batches/b.json",
    ]
    # Two distinct folders in the chain -> exactly two metadata lookups, cached
    # for the second file.
    assert len(files.get_calls) == 2


def test_list_objects_leaves_storage_key_empty_when_chain_unresolvable() -> None:
    files = FakeFiles(
        list_responses=[
            {"files": [{"id": "f1", "name": "orphan.json", "parents": ["fld-unknown"]}]}
        ],
        get_responses={"fld-unknown": {}},
    )
    store = voice_memos_store(files)

    listings = store.list_objects(kind="voice_memo_metadata", stage="inbox")

    assert listings[0].ref["storage_key"] == ""


def test_list_objects_storage_key_survives_folder_cycles() -> None:
    files = FakeFiles(
        list_responses=[
            {"files": [{"id": "f1", "name": "loop.json", "parents": ["fld-a"]}]}
        ],
        get_responses={
            "fld-a": {"id": "fld-a", "name": "a", "parents": ["fld-b"]},
            "fld-b": {"id": "fld-b", "name": "b", "parents": ["fld-a"]},
        },
    )
    store = voice_memos_store(files)

    listings = store.list_objects(kind="voice_memo_metadata", stage="inbox")

    assert listings[0].ref["storage_key"] == ""


def test_list_objects_paginates() -> None:
    files = FakeFiles(
        list_responses=[
            {"files": [{"id": "a"}], "nextPageToken": "p2"},
            {"files": [{"id": "b"}]},
        ]
    )
    store = voice_memos_store(files)

    listings = store.list_objects(kind="voice_memo_metadata", stage="inbox")

    assert [listing.ref["storage_file_id"] for listing in listings] == ["a", "b"]
    assert files.list_calls[1]["pageToken"] == "p2"


def test_find_object_filters_by_properties_and_returns_first() -> None:
    files = FakeFiles(list_responses=[{"files": [{"id": "audio-1", "appProperties": {"content_sha256": "sha"}}]}])
    store = voice_memos_store(files)

    listing = store.find_object(
        kind="voice_memo_audio",
        stage="inbox",
        properties={"content_sha256": "sha"},
    )

    assert listing is not None
    assert listing.ref["storage_file_id"] == "audio-1"
    query = files.list_calls[0]["q"]
    assert "content_sha256' and value='sha'" in query
    assert files.list_calls[0]["pageSize"] == 1


def test_find_object_returns_none_when_empty() -> None:
    store = voice_memos_store(FakeFiles(list_responses=[{"files": []}]))
    assert store.find_object(kind="voice_memo_audio", stage="inbox") is None


def test_move_object_relocates_to_library_folder() -> None:
    files = FakeFiles(list_responses=[{"files": []}] * 4)  # four folder lookups, all empty -> create
    store = voice_memos_store(files)

    result = store.move_object(
        {"storage_file_id": "drive-file-id"},
        new_object_key="apple-voice-memos/library/2026/03/2026-03-25-abc123.qta",
    )

    created_folders = [call["body"]["name"] for call in files.create_calls]
    assert created_folders == ["apple-voice-memos", "library", "2026", "03"]

    assert len(files.update_calls) == 1
    update = files.update_calls[0]
    assert update["fileId"] == "drive-file-id"
    assert update["addParents"] == "folder-03"
    assert update["removeParents"] == "old-parent"
    assert update["body"]["name"] == "2026-03-25-abc123.qta"
    assert update["body"]["appProperties"]["pdw_stage"] == "library"
    assert "storage_key" not in update["body"]["appProperties"]

    assert result == {
        "storage_backend": "google_drive",
        "storage_key": "apple-voice-memos/library/2026/03/2026-03-25-abc123.qta",
        "storage_file_id": "drive-file-id",
        "storage_url": "https://drive/promoted",
    }
