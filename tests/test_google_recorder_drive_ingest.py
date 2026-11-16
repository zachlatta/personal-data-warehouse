"""Google Recorder's source-owned row mapping remains unchanged."""

import json
from datetime import UTC, datetime

from personal_data_warehouse.google_recorder_drive_ingest import metadata_to_row
from personal_data_warehouse.postgres import POSTGRES_TABLES
from personal_data_warehouse.relations import relation
from personal_data_warehouse.schema import GOOGLE_RECORDER_RECORDING_COLUMNS
from tests.test_paired_audio_inbox import AUDIO_SHA, INBOX_AUDIO_KEY, INBOX_SIDECAR_KEY, sidecar


def test_the_raw_table_is_source_owned_and_registered() -> None:
    recordings = relation("google_recorder_recordings")
    assert (recordings.schema, recordings.name) == ("base_google_recorder", "recordings")
    assert POSTGRES_TABLES["google_recorder_recordings"].columns == GOOGLE_RECORDER_RECORDING_COLUMNS
    assert POSTGRES_TABLES["google_recorder_recordings"].primary_key == ("account", "recording_id")


def test_a_sidecar_maps_to_one_row_with_the_recorders_own_fields() -> None:
    payload = sidecar()
    payload["audio_file"] = {
        "storage_backend": "google_drive",
        "storage_key": INBOX_AUDIO_KEY.replace("/inbox/", "/library/"),
        "storage_file_id": "audio-id",
        "storage_url": "https://drive/audio-id",
    }
    payload["metadata_file"] = {
        "storage_backend": "google_drive",
        "storage_key": INBOX_SIDECAR_KEY.replace("/inbox/", "/library/"),
        "storage_file_id": "meta-id",
        "storage_url": "https://drive/meta-id",
        "content_sha256": "meta-sha",
    }
    ingested_at = datetime(2026, 10, 23, 22, 0, tzinfo=UTC)

    row = metadata_to_row(payload, ingested_at=ingested_at)

    assert set(row) == set(GOOGLE_RECORDER_RECORDING_COLUMNS)
    assert row["account"] == "zach@example.com"
    assert row["recording_id"] == "0e3f45d8-690c-4e95-b145-01a0312704cf"
    assert row["device"] == "pichu"
    assert row["title"] == "Oct 23 at 5:56 PM"
    # Recorder's created_time is epoch milliseconds in UTC: no zone guessing.
    assert row["recorded_at"] == datetime(2026, 10, 23, 21, 56, 24, 803000, tzinfo=UTC)
    assert row["duration_seconds"] == 4.35
    assert row["location_name"] == "Springfield Park"
    assert row["content_sha256"] == AUDIO_SHA
    assert row["size_bytes"] == 29742
    assert row["storage_file_id"] == "audio-id"
    assert row["storage_key"].startswith("google-recorder/library/")
    assert row["metadata_storage_file_id"] == "meta-id"
    assert row["metadata_content_sha256"] == "meta-sha"
    assert row["is_deleted"] == 0
    assert row["ingested_at"] == ingested_at
    raw = json.loads(row["raw_metadata_json"])
    assert raw["recording"]["location"]["latitude"] == 40.01
    assert "audio_file" not in raw and "metadata_file" not in raw


def test_the_newest_sidecar_wins_when_two_describe_one_recording() -> None:
    """A rename in Recorder sends a second sidecar for the same audio.

    Both can sit in the inbox at once, so the row's version is the sidecar's
    own upload time rather than the ingest run's, which both would share.
    """
    older = metadata_to_row(sidecar(title="Oct 23 at 5:56 PM"), ingested_at=datetime(2026, 10, 24, tzinfo=UTC))
    newer_payload = sidecar(title="Grocery list")
    newer_payload["uploaded_at"] = "2026-10-23T22:10:00Z"
    newer = metadata_to_row(newer_payload, ingested_at=datetime(2026, 10, 24, tzinfo=UTC))
    assert newer["sync_version"] > older["sync_version"]


def test_a_sidecar_without_a_location_stores_an_empty_name() -> None:
    payload = sidecar()
    del payload["recording"]["location"]
    row = metadata_to_row(payload, ingested_at=datetime(2026, 10, 24, tzinfo=UTC))
    assert row["location_name"] == ""


def test_search_scopes_pixel_recordings_with_every_other_voice_source() -> None:
    """One voice adapter covers every source, so the Recorder's names resolve
    to the `transcript` token rather than raising as unknown sources."""
    from personal_data_warehouse.postgres import SEARCH_SOURCE_ALIASES

    for alias in ("google_recorder", "pixel_recorder", "recorder"):
        assert SEARCH_SOURCE_ALIASES[alias] == "transcript", alias
