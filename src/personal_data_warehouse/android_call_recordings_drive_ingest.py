"""Android call recordings row mapping and paired-inbox configuration."""

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
    for key in ("direction", "sim_slot", "call_log_name"):
        row[key] = recording.get(key) if recording.get(key) is not None else ""
    for key in ("phone_numbers", "contact_names", "caller_names"):
        row[key] = json.dumps(recording.get(key) or [])
    return row


SOURCE = "android_call_recordings"
INBOX = PairedAudioInbox(
    SOURCE,
    "Android call recordings",
    "android-call-recordings",
    "ensure_android_call_recordings_tables",
    "insert_android_call_recordings_recordings",
    metadata_to_row,
)
INBOX_PREFIX, LIBRARY_PREFIX = INBOX.inbox_prefix, INBOX.library_prefix
AUDIO_KIND, METADATA_KIND = INBOX.audio_kind, INBOX.metadata_kind
AndroidCallRecordingsDriveIngestSummary = PairedAudioIngestSummary
AndroidCallRecordingsDriveIngestRunner = partial(PairedAudioIngestRunner, inbox=INBOX)
iter_metadata_payloads = INBOX.iter_metadata_payloads
has_metadata_payloads = INBOX.has_metadata_payloads
