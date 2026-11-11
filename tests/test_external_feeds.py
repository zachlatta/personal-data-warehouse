"""Exercise the actual HTTP feed's SQL against the unified voice mart."""
from datetime import UTC, datetime, timedelta
from pathlib import Path
import os
import subprocess
import json

from tests.test_voice_memo_continuity import warehouse, _seed, _transcribe


def test_feed_only_exposes_recent_transcribed_and_enriched_recordings(warehouse):
    now = datetime.now(UTC)
    _seed(warehouse, [
        ("ready", now - timedelta(hours=1), 20, "Complete contents"),
        ("old", now - timedelta(hours=49), 20, "Old contents"),
        ("future", now + timedelta(hours=1), 20, "Future contents"),
        ("pending", now - timedelta(hours=2), 20, "Pending contents"),
        ("untranscribed", now - timedelta(hours=3), 20, "No transcript yet"),
        ("deleted", now - timedelta(hours=4), 20, "Deleted contents"),
        ("retranscribed", now - timedelta(hours=5), 20, "Stale enrichment"),
    ])
    warehouse._command("DELETE FROM @apple_voice_memos_enrichments WHERE recording_id = 'pending'")
    warehouse._command("DELETE FROM @apple_voice_memos_transcription_runs WHERE recording_id = 'untranscribed'")
    warehouse._command("UPDATE @apple_voice_memos_files SET is_deleted = 1 WHERE recording_id = 'deleted'")
    _transcribe(warehouse, "retranscribed", "new-transcript", now)
    warehouse._ensure_voice_memos_mart_views()
    source = Path("app/internal/externalfeeds/registry.go").read_text()
    sql = source.split("const voiceFeedSQL = `", 1)[1].split("`", 1)[0]
    rows = [json.loads(row["payload_json"]) for row in warehouse._query_dicts(sql)]
    assert [row["recording_id"] for row in rows] == ["ready"]
    assert rows[0]["transcript"] == "Complete contents"
    assert rows[0]["summary"] == "Summary ready"
    assert rows[0]["calendar_event_id"] is None  # unmatched is a valid completed outcome


def test_existing_token_table_gains_usage_counter(warehouse):
    warehouse.ensure_app_access_token_tables()
    warehouse._command("ALTER TABLE @app_access_tokens DROP COLUMN request_count")
    warehouse.ensure_app_access_token_tables()
    columns = warehouse._query_dicts("SELECT request_count FROM @app_access_tokens LIMIT 1")
    assert columns == []


def test_go_token_persistence_and_usage_against_disposable_postgres():
    result = subprocess.run(
        ["go", "test", "./internal/accesstokens", "-run", "^TestPostgresStoreRoundTripsTheTokenLifecycle$", "-count=1", "-v"],
        cwd=Path("app"),
        env={**os.environ, "PDW_TEST_POSTGRES_URL": os.environ["POSTGRES_DATABASE_URL"]},
        text=True, capture_output=True, timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "SKIP" not in result.stdout


def test_go_projection_cursor_with_concurrent_and_backdated_arrivals(warehouse):
    warehouse.ensure_external_feed_tables()
    result = subprocess.run(
        ["go", "test", "./internal/externalfeeds", "-run", "^TestPostgresProjectionCursor$", "-count=1", "-v"],
        cwd=Path("app"), env={
            **os.environ, "PDW_EXTERNAL_FEED_TEST_URL": os.environ["POSTGRES_DATABASE_URL"],
            "PDW_EXTERNAL_FEED_TEST_RELATIONS": json.dumps({
                name: warehouse.sql_relation(name) for name in ("external_feed_items", "external_feed_seq")
            }),
        }, text=True, capture_output=True, timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "SKIP" not in result.stdout
