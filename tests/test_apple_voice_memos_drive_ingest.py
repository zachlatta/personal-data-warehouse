from __future__ import annotations

from datetime import UTC, datetime

from personal_data_warehouse.apple_voice_memos_drive_ingest import (
    GoogleDriveVoiceMemosPromoter,
    VoiceMemosDriveIngestRunner,
    attach_storage_context,
    clean_metadata_payload,
    library_metadata_payload,
    iter_drive_metadata_payloads,
    metadata_to_row,
)


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeWarehouse:
    def __init__(self) -> None:
        self.ensure_apple_voice_memos_tables_called = False
        self.rows: list[dict[str, object]] = []

    def ensure_apple_voice_memos_tables(self) -> None:
        self.ensure_apple_voice_memos_tables_called = True

    def insert_apple_voice_memos_files(self, rows) -> None:
        self.rows.extend(rows)


class FakePromoter:
    def __init__(self) -> None:
        self.payloads: list[dict[str, object]] = []

    def promote(self, payload) -> None:
        self.payloads.append(payload)


class FakeDriveRequest:
    def __init__(self, response) -> None:
        self.response = response

    def execute(self):
        return self.response


class FakeDriveFiles:
    def __init__(self) -> None:
        self.list_calls: list[dict[str, object]] = []
        self.media_requests: list[str] = []

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        return FakeDriveRequest({"files": []})

    def get_media(self, **kwargs):
        self.media_requests.append(str(kwargs["fileId"]))
        return object()


class FakeDriveService:
    def __init__(self) -> None:
        self.files_resource = FakeDriveFiles()

    def files(self):
        return self.files_resource


class FakePromoteDriveRequest:
    def __init__(self, response) -> None:
        self.response = response

    def execute(self):
        return self.response


class FakePromoteDriveFiles:
    def __init__(self) -> None:
        self.list_calls: list[dict[str, object]] = []
        self.create_calls: list[dict[str, object]] = []
        self.get_calls: list[dict[str, object]] = []
        self.update_calls: list[dict[str, object]] = []

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        return FakePromoteDriveRequest({"files": []})

    def create(self, **kwargs):
        self.create_calls.append(kwargs)
        return FakePromoteDriveRequest({"id": f"folder-{kwargs['body']['name']}"})

    def get(self, **kwargs):
        self.get_calls.append(kwargs)
        return FakePromoteDriveRequest({"parents": ["old-parent"]})

    def update(self, **kwargs):
        self.update_calls.append(kwargs)
        return FakePromoteDriveRequest({"id": kwargs["fileId"], "webViewLink": "https://drive/promoted"})


class FakePromoteDriveService:
    def __init__(self) -> None:
        self.files_resource = FakePromoteDriveFiles()

    def files(self):
        return self.files_resource


def metadata_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": "apple_voice_memos",
        "account": "zach@example.com",
        "uploaded_at": "2026-04-27T12:00:00+00:00",
        "recording": {
            "recording_id": "20260325 145019-DAAC9394",
            "title": "The Flying Pig Bookstore 6",
            "original_path": "/Users/zrl/VoiceMemos/20260325 145019-DAAC9394.qta",
            "filename": "20260325 145019-DAAC9394.qta",
            "extension": ".qta",
            "content_type": "audio/quicktime",
            "size_bytes": 123,
            "content_sha256": "abc123",
            "file_created_at": "2026-03-25T18:50:19+00:00",
            "file_modified_at": "2026-03-25T19:05:00+00:00",
            "recorded_at": "2026-03-25T18:50:19+00:00",
        },
    }


def decorated_metadata_payload() -> dict[str, object]:
    payload = metadata_payload()
    payload.update(
        {
            "audio_file": {
                "storage_backend": "google_drive",
                "storage_key": "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.qta",
                "storage_file_id": "drive-file-id",
                "storage_url": "https://drive/audio",
            },
            "metadata_file": {
                "storage_backend": "google_drive",
                "storage_key": "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.json",
                "storage_file_id": "drive-metadata-file-id",
                "storage_url": "https://drive/metadata",
                "content_sha256": "metadata-sha",
            },
        }
    )
    return payload


def test_attach_storage_context_derives_google_drive_keys_without_json_storage_fields() -> None:
    payload = attach_storage_context(
        metadata_payload(),
        stage="inbox",
        metadata_file={
            "id": "drive-metadata-file-id",
            "webViewLink": "https://drive/metadata",
            "appProperties": {"content_sha256": "metadata-sha"},
        },
        audio_file={
            "id": "drive-file-id",
            "webViewLink": "https://drive/audio",
        },
    )

    assert payload["audio_file"]["storage_key"] == "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.qta"
    assert payload["metadata_file"]["storage_key"] == "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.json"
    assert "audio_file" not in clean_metadata_payload(payload)
    assert "metadata_file" not in clean_metadata_payload(payload)


def legacy_metadata_payload() -> dict[str, object]:
    return {
        **metadata_payload(),
        "audio_file": {
            "storage_backend": "google_drive",
            "storage_key": "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.qta",
            "storage_file_id": "drive-file-id",
            "storage_url": "https://drive/audio",
        },
        "metadata_file": {
            "storage_backend": "google_drive",
            "storage_key": "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.json",
            "storage_file_id": "drive-metadata-file-id",
            "storage_url": "https://drive/metadata",
            "content_sha256": "metadata-sha",
        },
    }


def test_metadata_to_row_maps_drive_metadata_to_clickhouse_row() -> None:
    row = metadata_to_row(decorated_metadata_payload(), ingested_at=datetime(2026, 4, 27, 13, tzinfo=UTC))

    assert row["account"] == "zach@example.com"
    assert row["recording_id"] == "20260325 145019-DAAC9394"
    assert row["content_sha256"] == "abc123"
    assert row["storage_backend"] == "google_drive"
    assert row["storage_file_id"] == "drive-file-id"
    assert row["metadata_storage_key"] == "apple-voice-memos/inbox/2026/03/2026-03-25-abc123.json"
    assert row["metadata_storage_file_id"] == "drive-metadata-file-id"
    assert row["recorded_at"] == datetime(2026, 3, 25, 18, 50, 19, tzinfo=UTC)
    assert "audio_file" not in row["raw_metadata_json"]


def test_library_metadata_payload_rewrites_inbox_storage_keys() -> None:
    payload = library_metadata_payload(decorated_metadata_payload())

    assert payload["audio_file"]["storage_key"] == "apple-voice-memos/library/2026/03/2026-03-25-abc123.qta"
    assert payload["metadata_file"]["storage_key"] == "apple-voice-memos/library/2026/03/2026-03-25-abc123.json"
    assert payload["metadata_file"]["content_sha256"] == "metadata-sha"


def test_drive_ingest_runner_writes_metadata_rows() -> None:
    warehouse = FakeWarehouse()
    promoter = FakePromoter()

    summary = VoiceMemosDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: [decorated_metadata_payload()],
        promoter=promoter,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 4, 27, 13, tzinfo=UTC),
    ).sync()

    assert warehouse.ensure_apple_voice_memos_tables_called
    assert summary.metadata_seen == 1
    assert summary.rows_written == 1
    assert summary.recordings_promoted == 1
    assert warehouse.rows[0]["content_sha256"] == "abc123"
    assert warehouse.rows[0]["storage_key"] == "apple-voice-memos/library/2026/03/2026-03-25-abc123.qta"
    assert promoter.payloads == [decorated_metadata_payload()]


def test_drive_metadata_listing_uses_root_folder_app_property_not_direct_parent() -> None:
    service = FakeDriveService()

    assert list(iter_drive_metadata_payloads(service=service, folder_id="root-folder")) == []

    query = service.files_resource.list_calls[0]["q"]
    assert "'root-folder' in parents" not in query
    assert "pdw_root_folder_id" in query
    assert "root-folder" in query
    assert "value='apple_voice_memos'" in query
    assert "value='voice_memos'" in query
    assert "pdw_stage' and value='inbox" in query


def test_drive_metadata_listing_can_target_library_stage() -> None:
    service = FakeDriveService()

    assert list(iter_drive_metadata_payloads(service=service, folder_id="root-folder", stage="library")) == []

    query = service.files_resource.list_calls[0]["q"]
    assert "pdw_stage' and value='library" in query


def test_google_drive_promoter_moves_audio_and_metadata_to_library() -> None:
    service = FakePromoteDriveService()

    GoogleDriveVoiceMemosPromoter(service=service, folder_id="root-folder").promote(decorated_metadata_payload())

    created_folders = [call["body"]["name"] for call in service.files_resource.create_calls]
    assert created_folders == ["apple-voice-memos", "library", "2026", "03"]
    assert [call["fileId"] for call in service.files_resource.update_calls] == [
        "drive-file-id",
        "drive-metadata-file-id",
    ]
    audio_update = service.files_resource.update_calls[0]
    metadata_update = service.files_resource.update_calls[1]
    assert audio_update["addParents"] == "folder-03"
    assert audio_update["removeParents"] == "old-parent"
    assert audio_update["body"]["name"] == "2026-03-25-abc123.qta"
    assert audio_update["body"]["appProperties"]["pdw_stage"] == "library"
    assert "storage_key" not in audio_update["body"]["appProperties"]
    assert metadata_update["body"]["name"] == "2026-03-25-abc123.json"
    assert "storage_key" not in metadata_update["body"]["appProperties"]
    assert "media_body" not in metadata_update
