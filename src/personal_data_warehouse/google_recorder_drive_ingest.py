"""Google Recorder row mapping and paired-inbox configuration."""

import json
from functools import partial

from personal_data_warehouse.apple_voice_memos_drive_ingest import nested_mapping
from personal_data_warehouse.paired_audio_inbox import (
    PairedAudioInbox,
    PairedAudioIngestRunner,
    PairedAudioIngestSummary,
    recording_metadata_row,
)


def metadata_to_row(metadata, *, ingested_at):
    row = recording_metadata_row(metadata, ingested_at=ingested_at)
    recording = nested_mapping(metadata, "recording")
    row["location_name"] = str(nested_mapping(recording, "location").get("name", ""))
    row["raw_metadata_json"] = json.dumps(row["raw_metadata_json"], sort_keys=True, separators=(",", ":"))
    return row


SOURCE = "google_recorder"
INBOX = PairedAudioInbox(
    SOURCE,
    "Google Recorder",
    "google-recorder",
    "ensure_google_recorder_tables",
    "insert_google_recorder_recordings",
    metadata_to_row,
)
INBOX_PREFIX, LIBRARY_PREFIX = INBOX.inbox_prefix, INBOX.library_prefix
AUDIO_KIND, METADATA_KIND = INBOX.audio_kind, INBOX.metadata_kind
GoogleRecorderDriveIngestSummary = PairedAudioIngestSummary
GoogleRecorderDriveIngestRunner = partial(PairedAudioIngestRunner, inbox=INBOX)
iter_metadata_payloads = INBOX.iter_metadata_payloads
has_metadata_payloads = INBOX.has_metadata_payloads
