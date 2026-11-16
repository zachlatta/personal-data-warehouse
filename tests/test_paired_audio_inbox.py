"""Shared paired-audio promotion: pairing, corrections, retries and durable moves.

The phone's uploader (`pdw ingest google-recorder`, run on the rooted Pixel)
posts each finished recording's audio and a JSON sidecar to the app, which
files both in the Drive inbox. This reader promotes them into the raw table,
the same inbox-to-library shape as Apple Voice Memos, and the voice mart then
transcribes and enriches the recording with the code every voice source uses.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from personal_data_warehouse.google_recorder_drive_ingest import INBOX
from personal_data_warehouse.objectstore import ObjectListing
from personal_data_warehouse.paired_audio_inbox import PairedAudioIngestRunner

AUDIO_KIND, METADATA_KIND = INBOX.audio_kind, INBOX.metadata_kind
has_metadata_payloads = INBOX.has_metadata_payloads
iter_metadata_payloads = INBOX.iter_metadata_payloads

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
        "device": "pichu",
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

    summary = PairedAudioIngestRunner(
        inbox=INBOX,
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

    PairedAudioIngestRunner(
        inbox=INBOX,
        warehouse=warehouse,
        metadata_source=lambda: iter_metadata_payloads(object_store=store),
        object_store=store,
        logger=FakeLogger(),
    ).sync()

    assert store.moved == [("meta-id", INBOX_SIDECAR_KEY.replace("/inbox/", "/library/"))]
    assert warehouse.rows[0]["title"] == "Grocery list"
