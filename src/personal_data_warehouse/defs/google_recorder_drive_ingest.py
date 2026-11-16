"""Dagster registration for google_recorder paired audio uploads."""

from dagster import Definitions, definitions

from personal_data_warehouse.google_recorder_drive_ingest import INBOX
from personal_data_warehouse.paired_audio_defs import build_paired_audio_defs

runtime = build_paired_audio_defs(INBOX, 7403111855)
google_recorder_object_store = runtime.store
ingest_google_recorder_inbox = runtime.ingest
google_recorder_inbox_has_metadata = runtime.has_metadata
google_recorder_drive_ingest = runtime.asset
google_recorder_drive_ingest_job = runtime.job
google_recorder_drive_inbox_sensor = runtime.sensor


@definitions
def defs():
    return Definitions(assets=[runtime.asset], jobs=[runtime.job], sensors=[runtime.sensor])
