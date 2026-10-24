"""Google Recorder (the Pixel's voice recorder) lands in base_google_recorder.recordings.

The phone's uploader (`pdw ingest google-recorder`, run on the rooted Pixel)
posts each finished recording's audio and a JSON sidecar to the app, which
files both in the Drive inbox. This reader promotes them into the raw table,
the same inbox-to-library shape as Apple Voice Memos, and the voice mart then
transcribes and enriches the recording with the code every voice source uses.
"""

from __future__ import annotations

from datetime import UTC, datetime
import json

import pytest

from personal_data_warehouse.google_recorder_drive_ingest import (
    AUDIO_KIND,
    METADATA_KIND,
    GoogleRecorderDriveIngestRunner,
    has_metadata_payloads,
    iter_metadata_payloads,
    metadata_to_row,
)
from personal_data_warehouse.objectstore import ObjectListing
from personal_data_warehouse.postgres import POSTGRES_TABLES
from personal_data_warehouse.relations import relation
from personal_data_warehouse.schema import GOOGLE_RECORDER_RECORDING_COLUMNS

AUDIO_SHA = "3c01b5369cf46460c46ffd796365fc40736c614a48239e67447039344cf1c723"


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeWarehouse:
    def __init__(self) -> None:
        self.ensured = False
        self.rows: list[dict[str, object]] = []

    def ensure_google_recorder_tables(self) -> None:
        self.ensured = True

    def insert_google_recorder_recordings(self, rows) -> None:
        self.rows.extend(rows)


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self, *, metadata_listings, audio_by_stage, json_by_id) -> None:
        self.metadata_listings = list(metadata_listings)
        self.audio_by_stage = dict(audio_by_stage)
        self.json_by_id = dict(json_by_id)
        self.moved: list[tuple[str, str]] = []
        self.audio_lookups: list[tuple[str, dict]] = []

    def list_objects(self, *, kind, stage=None, properties=None):
        return list(self.metadata_listings) if kind == METADATA_KIND else []

    def find_object(self, *, kind, stage=None, properties=None):
        if kind == METADATA_KIND:
            return self.metadata_listings[0] if self.metadata_listings else None
        if kind == AUDIO_KIND:
            self.audio_lookups.append((str(stage), dict(properties or {})))
            return self.audio_by_stage.get(stage)
        return None

    def get_object(self, ref):
        return json.dumps(self.json_by_id[str(ref["storage_file_id"])]).encode("utf-8")

    def move_object(self, ref, *, new_object_key, app_properties=None):
        self.moved.append((str(ref.get("storage_file_id", "")), new_object_key))
        return {**ref, "storage_key": new_object_key}


def sidecar(**recording_overrides) -> dict[str, object]:
    recording = {
        "recording_id": "0e3f45d8-690c-4e95-b145-01a0312704cf",
        "title": "Oct 23 at 5:56 PM",
        "recorded_at": "2026-10-23T21:56:24.803Z",
        "duration_seconds": 4.35,
        "filename": "0e3f45d8-690c-4e95-b145-01a0312704cf.m4a",
        "extension": ".m4a",
        "content_type": "audio/mp4",
        "size_bytes": 29742,
        "content_sha256": AUDIO_SHA,
        "file_modified_at": "2026-10-23T21:56:30Z",
        "location": {"name": "Springfield Park", "latitude": 40.01, "longitude": -75.02},
        "recording_state": 3,
    }
    recording.update(recording_overrides)
    return {
        "schema_version": 1,
        "source": "google_recorder",
        "account": "zach@example.com",
        "device": "pixel11pro",
        "uploaded_at": "2026-10-23T21:56:40Z",
        "recording": recording,
    }


def listing(file_id: str, key: str) -> ObjectListing:
    return ObjectListing(
        ref={
            "storage_backend": "google_drive",
            "storage_key": key,
            "storage_file_id": file_id,
            "storage_url": f"https://drive/{file_id}",
        },
        app_properties={"content_sha256": f"sha-of-{file_id}"},
        filename=key.rsplit("/", 1)[-1],
    )


INBOX_AUDIO_KEY = f"google-recorder/inbox/2026/10/2026-10-23-{AUDIO_SHA}.m4a"
INBOX_SIDECAR_KEY = f"google-recorder/inbox/2026/10/2026-10-23-{AUDIO_SHA}.json"


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
    assert row["device"] == "pixel11pro"
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


def test_inbox_sidecars_are_paired_with_their_audio_by_content_sha() -> None:
    store = FakeObjectStore(
        metadata_listings=[listing("meta-id", INBOX_SIDECAR_KEY)],
        audio_by_stage={"inbox": listing("audio-id", INBOX_AUDIO_KEY)},
        json_by_id={"meta-id": sidecar()},
    )

    payloads = list(iter_metadata_payloads(object_store=store))

    assert len(payloads) == 1
    assert store.audio_lookups[0] == ("inbox", {"content_sha256": AUDIO_SHA})
    assert payloads[0]["audio_file"]["storage_file_id"] == "audio-id"
    assert payloads[0]["audio_file"]["storage_key"] == INBOX_AUDIO_KEY
    assert payloads[0]["metadata_file"]["storage_key"] == INBOX_SIDECAR_KEY
    assert payloads[0]["metadata_file"]["content_sha256"] == "sha-of-meta-id"


def test_a_renamed_recordings_sidecar_finds_audio_already_in_the_library() -> None:
    """A rename re-sends only the sidecar; its audio was promoted long ago."""
    library_audio = listing("audio-id", INBOX_AUDIO_KEY.replace("/inbox/", "/library/"))
    store = FakeObjectStore(
        metadata_listings=[listing("meta-id", INBOX_SIDECAR_KEY)],
        audio_by_stage={"library": library_audio},
        json_by_id={"meta-id": sidecar(title="Grocery list")},
    )

    [payload] = list(iter_metadata_payloads(object_store=store))

    assert [stage for stage, _ in store.audio_lookups] == ["inbox", "library"]
    assert payload["audio_file"]["storage_file_id"] == "audio-id"
    assert payload["audio_file"]["storage_key"] == library_audio.ref["storage_key"]


def test_a_sidecar_whose_audio_never_arrived_fails_loudly() -> None:
    store = FakeObjectStore(
        metadata_listings=[listing("meta-id", INBOX_SIDECAR_KEY)],
        audio_by_stage={},
        json_by_id={"meta-id": sidecar()},
    )
    with pytest.raises(ValueError, match=AUDIO_SHA):
        list(iter_metadata_payloads(object_store=store))


def test_has_metadata_payloads_reads_one_inbox_listing() -> None:
    empty = FakeObjectStore(metadata_listings=[], audio_by_stage={}, json_by_id={})
    full = FakeObjectStore(metadata_listings=[listing("meta-id", INBOX_SIDECAR_KEY)], audio_by_stage={}, json_by_id={})
    assert has_metadata_payloads(object_store=empty) is False
    assert has_metadata_payloads(object_store=full) is True


def test_a_sync_writes_library_rows_then_promotes_both_objects() -> None:
    store = FakeObjectStore(
        metadata_listings=[listing("meta-id", INBOX_SIDECAR_KEY)],
        audio_by_stage={"inbox": listing("audio-id", INBOX_AUDIO_KEY)},
        json_by_id={"meta-id": sidecar()},
    )
    warehouse = FakeWarehouse()

    summary = GoogleRecorderDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: iter_metadata_payloads(object_store=store),
        object_store=store,
        logger=FakeLogger(),
        now=lambda: datetime(2026, 10, 23, 22, 0, tzinfo=UTC),
    ).sync()

    assert warehouse.ensured
    assert summary.metadata_seen == 1
    assert summary.rows_written == 1
    assert summary.recordings_promoted == 1
    [row] = warehouse.rows
    # The row is written with the keys the objects are about to have, so a
    # crash between the insert and the move leaves an inbox the next run
    # re-promotes onto the same row rather than a row naming the inbox.
    assert row["storage_key"] == INBOX_AUDIO_KEY.replace("/inbox/", "/library/")
    assert row["metadata_storage_key"] == INBOX_SIDECAR_KEY.replace("/inbox/", "/library/")
    assert store.moved == [
        ("audio-id", INBOX_AUDIO_KEY.replace("/inbox/", "/library/")),
        ("meta-id", INBOX_SIDECAR_KEY.replace("/inbox/", "/library/")),
    ]


def test_audio_already_in_the_library_is_not_moved_again() -> None:
    library_audio = listing("audio-id", INBOX_AUDIO_KEY.replace("/inbox/", "/library/"))
    store = FakeObjectStore(
        metadata_listings=[listing("meta-id", INBOX_SIDECAR_KEY)],
        audio_by_stage={"library": library_audio},
        json_by_id={"meta-id": sidecar(title="Grocery list")},
    )
    warehouse = FakeWarehouse()

    GoogleRecorderDriveIngestRunner(
        warehouse=warehouse,
        metadata_source=lambda: iter_metadata_payloads(object_store=store),
        object_store=store,
        logger=FakeLogger(),
    ).sync()

    assert store.moved == [("meta-id", INBOX_SIDECAR_KEY.replace("/inbox/", "/library/"))]
    assert warehouse.rows[0]["title"] == "Grocery list"


def test_search_scopes_pixel_recordings_with_every_other_voice_source() -> None:
    """One voice adapter covers every source, so the Recorder's names resolve
    to the `transcript` token rather than raising as unknown sources."""
    from personal_data_warehouse.postgres import SEARCH_SOURCE_ALIASES

    for alias in ("google_recorder", "pixel_recorder", "recorder"):
        assert SEARCH_SOURCE_ALIASES[alias] == "transcript", alias
