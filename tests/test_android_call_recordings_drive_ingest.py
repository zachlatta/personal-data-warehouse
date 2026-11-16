"""BCR's real uploader envelope maps to the call source, without Recorder fields."""

import json
from datetime import UTC, datetime
from pathlib import Path

from personal_data_warehouse.android_call_recordings_drive_ingest import metadata_to_row
from personal_data_warehouse.postgres import POSTGRES_TABLES
from personal_data_warehouse.relations import relation
from personal_data_warehouse.schema import ANDROID_CALL_RECORDINGS_RECORDING_COLUMNS


def test_bcr_uploader_sidecar_maps_call_fields_and_keeps_verbatim_metadata():
    payload = json.loads((Path(__file__).parent / "fixtures/android_call_recordings_metadata.json").read_text())
    payload["audio_file"] = {
        "storage_key": "android-call-recordings/library/example.oga",
        "storage_file_id": "example-audio",
    }
    payload["metadata_file"] = {
        "storage_key": "android-call-recordings/library/example.json",
        "storage_file_id": "example-sidecar",
    }
    row = metadata_to_row(payload, ingested_at=datetime(2026, 11, 16, tzinfo=UTC))
    assert set(row) == set(ANDROID_CALL_RECORDINGS_RECORDING_COLUMNS)
    assert row["recording_id"] == "example-call" and row["direction"] == "out"
    assert row["extension"] == ".oga" and row["content_type"] == "audio/ogg"
    assert row["duration_seconds"] == 4.25 and row["sim_slot"] == 1
    assert json.loads(row["contact_names"]) == ["Example Contact"]
    assert json.loads(row["phone_numbers"]) == ["+15550100100"]
    assert row["raw_metadata_json"]["bcr_raw_json"] == payload["bcr_raw_json"]
    assert row["is_deleted"] == 0 and row["content_sha256"] == "a" * 64
    assert "location_name" not in row
    assert POSTGRES_TABLES["android_call_recordings_recordings"].primary_key == ("account", "recording_id")
    assert relation("android_call_recordings_recordings").schema == "base_android_call_recordings"
