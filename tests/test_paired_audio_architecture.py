"""Both Android audio sources use one durable paired-object inbox."""

from personal_data_warehouse import android_call_recordings_drive_ingest as calls
from personal_data_warehouse import google_recorder_drive_ingest as recorder


def test_sources_share_pairing_and_promotion_implementation():
    assert recorder.INBOX.__class__ is calls.INBOX.__class__
    assert recorder.INBOX.source == "google_recorder"
    assert calls.INBOX.source == "android_call_recordings"
