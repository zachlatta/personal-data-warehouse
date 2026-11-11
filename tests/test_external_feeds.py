"""Exercise the actual HTTP feed's SQL against the unified voice mart."""
from datetime import UTC, datetime, timedelta
from pathlib import Path
import os
import subprocess
import json
import socket
import time
import pytest
from personal_data_warehouse.postgres import PostgresWarehouse

pytestmark = pytest.mark.local_integration

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


def test_go_projection_cursor_with_concurrent_and_backdated_arrivals():
    # Use the same physical names and real expander as the running app. Each
    # integration test owns a disposable database; no SQL rewriting is needed.
    wh = PostgresWarehouse(os.environ["POSTGRES_DATABASE_URL"])
    wh.ensure_external_feed_tables()
    result = subprocess.run(
        ["go", "test", "./internal/externalfeeds", "-run", "^TestPostgresProjectionCursor$", "-count=1", "-v"],
        cwd=Path("app"), env={
            **os.environ, "PDW_EXTERNAL_FEED_TEST_URL": os.environ["POSTGRES_DATABASE_URL"],
        }, text=True, capture_output=True, timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "SKIP" not in result.stdout


def test_real_go_app_serves_python_provisioned_memos_with_an_issued_key(tmp_path):
    """Real catalog expansion, real app process and real HTTP via curl."""
    wh = PostgresWarehouse(os.environ["POSTGRES_DATABASE_URL"])
    now = datetime.now(UTC)
    _seed(wh, [
        ("ready", now - timedelta(hours=1), 20, "Expected memo contents"),
        ("old", now - timedelta(hours=49), 20, "Expired window"),
        ("stale", now - timedelta(hours=2), 20, "Stale enrichment"),
    ])
    _transcribe(wh, "stale", "new-transcript", now)
    wh.ensure_upstream_mutation_tables()
    wh.ensure_timeline_tables()
    binary = tmp_path / "pdw-mcp"
    subprocess.run(["go", "build", "-o", str(binary), "./cmd/pdw-mcp"], cwd="app", check=True, capture_output=True, timeout=180)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    master = "disposable-feed-test-master-secret-32-chars"
    env = {"PATH": os.environ["PATH"], "POSTGRES_DATABASE_URL": os.environ["POSTGRES_DATABASE_URL"],
           "MCP_ADDR": f"127.0.0.1:{port}", "MCP_BASE_URL": base, "PDW_SECRET_TOKEN": master}
    body_path, headers_path = tmp_path / "response.json", tmp_path / "headers.txt"

    def curl(path, key="", data=None, etag=None):
        args = ["curl", "--silent", "--show-error", "--max-time", "5", "--output", str(body_path),
                "--dump-header", str(headers_path), "--write-out", "%{http_code}"]
        if key:
            args += ["--header", f"Authorization: Bearer e2e:{key}"]
        if etag:
            args += ["--header", f"If-None-Match: {etag}"]
        if data is not None:
            args += ["--header", "Content-Type: application/json", "--data-binary", "@-"]
        result = subprocess.run([*args, base + path], input=json.dumps(data) if data is not None else None,
                                text=True, capture_output=True, timeout=10)
        return result.returncode, result.stdout, body_path.read_text() if body_path.exists() else ""

    with (tmp_path / "server.log").open("w+") as log:
        process = subprocess.Popen([str(binary)], env=env, stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 30
            while True:
                _, status, _ = curl("/healthz")
                if status == "204":
                    break
                if process.poll() is not None or time.monotonic() >= deadline:
                    log.seek(0)
                    pytest.fail("disposable app did not start: " + log.read()[-4000:])
                time.sleep(0.05)
            _, status, body = curl("/api/auth/tokens", master, {
                "client_name": "e2e-consumer", "scope": "feed:voice-memos", "expires_at": "2030-01-02T03:04:00Z"})
            assert status == "201", body
            issued = json.loads(body)
            assert issued["info"]["expires_at"] == "2030-01-02T03:04:00Z"
            key = issued["token"]
            _, status, body = curl("/api/external-feeds/voice-memos", key)
            assert status == "200", body
            page = json.loads(body)
            assert [row["recording_id"] for row in page["recordings"]] == ["ready"]
            assert page["recordings"][0]["transcript"] == "Expected memo contents"
            path = "/api/external-feeds/voice-memos?since=" + page["next_cursor"]
            _, status, body = curl(path, key)
            assert status == "200", body
            assert json.loads(body)["recordings"] == []
            etag = next(line.split(":", 1)[1].strip() for line in headers_path.read_text().splitlines() if line.lower().startswith("etag:"))
            assert curl(path, key, etag=etag)[1] == "304"
            print("Disposable cmd/pdw-mcp + Python ensure + issued key + curl: HTTP 200, expected memo; idle poll HTTP 304")
        finally:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
