"""The fifth voice source reaches shared consumers without a raw enrichment read."""
from datetime import UTC, datetime, timedelta
import json

from tests.test_marts_layering import warehouse, _row
from personal_data_warehouse.schema import ANDROID_CALL_RECORDINGS_RECORDING_COLUMNS, UPLOADER_HEARTBEAT_COLUMNS, VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS
from personal_data_warehouse.apple_voice_memos_enrichment import load_enrichment_candidates
from personal_data_warehouse.timeline import adapter_by_name, BACKFILL_CURSOR_START, TIMELINE_NORMALIZED_COLUMNS


def test_call_reaches_mart_with_hints_and_inventory_detects_missing_call(warehouse):
    warehouse.ensure_android_call_recordings_tables()
    now = datetime.now(UTC)
    fields = dict(direction="out", contact_names=["Example Contact"], phone_numbers=["+15550100100"])
    warehouse.insert_android_call_recordings_recordings([_row(
        ANDROID_CALL_RECORDINGS_RECORDING_COLUMNS,
        account="owner@example.com", recording_id="example-call", device="example-phone",
        recorded_at=now-timedelta(minutes=10), duration_seconds=12.5,
        storage_key="android-call-recordings/library/example.oga", size_bytes=42,
        content_sha256="a"*64, raw_metadata_json={"recording":fields},
        ingested_at=now, sync_version=1,
    )])
    [row] = warehouse._query("SELECT source, call_metadata_json, is_deleted FROM @marts_voice_memos_recordings WHERE source='android_call_recordings'")
    assert row[0] == "android_call_recordings" and json.loads(row[1]) == fields and row[2] == 0
    [event] = warehouse._query(adapter_by_name("voice_memo").backfill_sql,
                               {"cursor_ts": BACKFILL_CURSOR_START, "cursor_id": "", "limit": 5})
    event = dict(zip(TIMELINE_NORMALIZED_COLUMNS, event, strict=True))
    assert event["priority"] == "self"
    assert "android_call_recordings" in event["event_id"]
    warehouse.insert_apple_voice_memos_transcription_runs([_row(
        VOICE_MEMO_TRANSCRIPTION_RUN_COLUMNS, source="android_call_recordings",
        account="owner@example.com", recording_id="example-call", provider="assemblyai",
        content_sha256="a"*64, provider_transcript_id="example-transcript", status="completed",
        transcript_text="Example conversation", completed_at=now, requested_at=now,
    )])
    [candidate] = load_enrichment_candidates(warehouse, provider="agent", prompt_version="example", limit=5,
                                              max_error_attempts=0)
    assert candidate["source"] == "android_call_recordings"
    assert json.loads(candidate["call_metadata_json"]) == fields
    assert candidate["transcript_text"] == "Example conversation"
    warehouse.ensure_pipeline_health_tables()
    warehouse._insert_rows("uploader_heartbeats", [_row(
        UPLOADER_HEARTBEAT_COLUMNS, pipeline="android_call_recordings", device="example-phone",
        ran_at=now, newest_finished_at=now-timedelta(minutes=6), status="ok", updated_at=now,
    )], UPLOADER_HEARTBEAT_COLUMNS)
    assert warehouse._query("SELECT status FROM @marts_ops_android_call_recordings_health") == [("failing",)]
    warehouse._command("UPDATE @uploader_heartbeats SET newest_finished_at = (SELECT max(recorded_at + make_interval(secs=>duration_seconds::float8)) FROM @android_call_recordings_recordings)")
    assert warehouse._query("SELECT status FROM @marts_ops_android_call_recordings_health") == [("ok",)]
