from __future__ import annotations

from pathlib import Path

import pytest
from googleapiclient.errors import HttpError

from personal_data_warehouse.objectstore import google_drive as gd
from personal_data_warehouse.objectstore.base import ObjectMetadata, ObjectNotFoundError
from personal_data_warehouse.objectstore.google_drive import GoogleDriveObjectStore


class FakeResp:
    def __init__(self, status: int) -> None:
        self.status = status
        self.reason = "error"


def http_error(status: int) -> HttpError:
    return HttpError(FakeResp(status), b"{}")


class FakeRequest:
    def __init__(self, *, result=None, error: Exception | None = None) -> None:
        self._result = result
        self._error = error

    def execute(self):
        if self._error is not None:
            raise self._error
        return self._result


class FakeMediaRequest:
    def __init__(self, content: bytes) -> None:
        self.content = content


class FakeFiles:
    def __init__(self, *, media=None, get=None, delete_error=None) -> None:
        self._media = media or {}
        self._get = get or {}
        self._delete_error = delete_error
        self.deleted: list[str] = []
        self.get_calls: list[dict] = []

    def get_media(self, *, fileId, supportsAllDrives):
        return FakeMediaRequest(self._media[fileId])

    def get(self, *, fileId, fields, supportsAllDrives):
        self.get_calls.append({"fileId": fileId, "fields": fields})
        entry = self._get.get(fileId)
        if isinstance(entry, Exception):
            return FakeRequest(error=entry)
        return FakeRequest(result=entry)

    def delete(self, *, fileId, supportsAllDrives):
        if self._delete_error is not None:
            return FakeRequest(error=self._delete_error)
        self.deleted.append(fileId)
        return FakeRequest(result="")


class FakeService:
    def __init__(self, files: FakeFiles) -> None:
        self._files = files

    def files(self):
        return self._files


class FakeDownloader:
    """Stand-in for googleapiclient MediaIoBaseDownload."""

    def __init__(self, output, request: FakeMediaRequest) -> None:
        self._output = output
        self._request = request

    def next_chunk(self, num_retries: int = 0):
        self._output.write(self._request.content)
        return (None, True)


def make_store(files: FakeFiles) -> GoogleDriveObjectStore:
    return GoogleDriveObjectStore(folder_id="root", service=FakeService(files))


def test_get_object_returns_bytes(monkeypatch) -> None:
    monkeypatch.setattr(gd, "MediaIoBaseDownload", FakeDownloader)
    store = make_store(FakeFiles(media={"fid": b"audio-bytes"}))

    assert store.get_object({"storage_file_id": "fid"}) == b"audio-bytes"


def test_download_to_path_writes_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(gd, "MediaIoBaseDownload", FakeDownloader)
    store = make_store(FakeFiles(media={"fid": b"hello"}))

    target = tmp_path / "nested" / "out.bin"
    store.download_to_path({"storage_file_id": "fid"}, target)

    assert target.read_bytes() == b"hello"


def test_missing_file_id_raises() -> None:
    store = make_store(FakeFiles())
    with pytest.raises(ObjectNotFoundError):
        store.get_object({"storage_file_id": ""})


def test_object_exists_true_false_and_missing() -> None:
    files = FakeFiles(
        get={
            "live": {"id": "live", "trashed": False},
            "trashed": {"id": "trashed", "trashed": True},
            "gone": http_error(404),
        }
    )
    store = make_store(files)

    assert store.object_exists({"storage_file_id": "live"}) is True
    assert store.object_exists({"storage_file_id": "trashed"}) is False
    assert store.object_exists({"storage_file_id": "gone"}) is False


def test_object_exists_propagates_non_404() -> None:
    store = make_store(FakeFiles(get={"boom": http_error(403)}))
    with pytest.raises(HttpError):
        store.object_exists({"storage_file_id": "boom"})


def test_delete_object_calls_delete_and_is_idempotent() -> None:
    files = FakeFiles()
    store = make_store(files)
    store.delete_object({"storage_file_id": "fid"})
    assert files.deleted == ["fid"]

    # Missing object: 404 is swallowed.
    store_missing = make_store(FakeFiles(delete_error=http_error(404)))
    store_missing.delete_object({"storage_file_id": "fid"})

    # Other errors propagate.
    store_err = make_store(FakeFiles(delete_error=http_error(500)))
    with pytest.raises(HttpError):
        store_err.delete_object({"storage_file_id": "fid"})


def test_get_metadata_maps_fields() -> None:
    files = FakeFiles(
        get={
            "fid": {
                "id": "fid",
                "name": "memo.m4a",
                "mimeType": "audio/mp4",
                "size": "2048",
                "createdTime": "2026-01-02T03:04:05Z",
                "modifiedTime": "2026-01-02T04:05:06Z",
                "webViewLink": "https://drive/fid",
                "appProperties": {"content_sha256": "abc123"},
            }
        }
    )
    store = make_store(files)

    metadata = store.get_metadata({"storage_file_id": "fid", "storage_key": "k.m4a"})

    assert metadata == ObjectMetadata(
        backend="google_drive",
        storage_key="k.m4a",
        storage_file_id="fid",
        content_type="audio/mp4",
        size_bytes=2048,
        content_sha256="abc123",
        filename="memo.m4a",
        created_time="2026-01-02T03:04:05Z",
        modified_time="2026-01-02T04:05:06Z",
        storage_url="https://drive/fid",
    )


def test_get_metadata_missing_raises_not_found() -> None:
    store = make_store(FakeFiles(get={"fid": http_error(404)}))
    with pytest.raises(ObjectNotFoundError):
        store.get_metadata({"storage_file_id": "fid", "storage_key": "k"})


def test_get_share_url_prefers_stored_url_without_api_call() -> None:
    files = FakeFiles()
    store = make_store(files)

    url = store.get_share_url({"storage_file_id": "fid", "storage_url": "https://drive/known"})

    assert url == "https://drive/known"
    assert files.get_calls == []  # no metadata lookup needed


def test_get_share_url_falls_back_to_metadata() -> None:
    files = FakeFiles(get={"fid": {"id": "fid", "webViewLink": "https://drive/looked-up"}})
    store = make_store(files)

    url = store.get_share_url({"storage_file_id": "fid", "storage_url": ""})

    assert url == "https://drive/looked-up"
    assert files.get_calls  # metadata was fetched
