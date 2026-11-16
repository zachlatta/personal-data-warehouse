"""Dagster registration for android_call_recordings paired audio uploads."""

from dagster import Definitions, definitions

from personal_data_warehouse.android_call_recordings_drive_ingest import INBOX
from personal_data_warehouse.paired_audio_defs import build_paired_audio_defs

runtime = build_paired_audio_defs(INBOX, 7403111856)
android_call_recordings_object_store = runtime.store
ingest_android_call_recordings_inbox = runtime.ingest
android_call_recordings_inbox_has_metadata = runtime.has_metadata
android_call_recordings_drive_ingest = runtime.asset
android_call_recordings_drive_ingest_job = runtime.job
android_call_recordings_drive_inbox_sensor = runtime.sensor


@definitions
def defs():
    return Definitions(assets=[runtime.asset], jobs=[runtime.job], sensors=[runtime.sensor])
