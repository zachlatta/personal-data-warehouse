from __future__ import annotations

from datetime import UTC, datetime
import gzip
import json
from pathlib import Path

from personal_data_warehouse_agent_sessions.scanner import (
    SessionFile,
    claude_session_id,
    codex_session_id,
    discover_session_files,
    openclaw_session_id,
)
from personal_data_warehouse_agent_sessions.state import AgentSessionsUploadState
from personal_data_warehouse_agent_sessions.sync import AgentSessionsUploadRunner


class FakeLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


class FakeObjectStore:
    backend = "google_drive"

    def __init__(self) -> None:
        self.batches: list[list[dict]] = []

    def put_file(self, *, path, object_key, content_sha256, content_type, kind=None, app_properties=None, skip_existing_check=False):
        records = []
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    records.append(json.loads(line))
        self.batches.append(records)
        return {
            "storage_backend": self.backend,
            "storage_key": object_key,
            "storage_file_id": f"file-{len(self.batches)}",
            "storage_url": "https://drive/batch",
        }


NOW = datetime(2026, 6, 14, 18, tzinfo=UTC)


# --- scanner ----------------------------------------------------------------


def test_claude_session_id_from_filename() -> None:
    assert claude_session_id(Path("/x/bd51eaa0-f030-4b13-96d9-37c8c1bda53d.jsonl")) == "bd51eaa0-f030-4b13-96d9-37c8c1bda53d"


def test_codex_session_id_extracts_trailing_uuid() -> None:
    name = "rollout-2026-06-14T13-16-17-019ec722-5a03-7980-bd45-f34ba5bd9502.jsonl"
    assert codex_session_id(Path("/x/" + name)) == "019ec722-5a03-7980-bd45-f34ba5bd9502"


def test_discover_finds_both_tools_and_ignores_wakatime(tmp_path: Path) -> None:
    claude_dir = tmp_path / "claude"
    proj = claude_dir / "-Users-zrl-project"
    proj.mkdir(parents=True)
    (proj / "sess-a.jsonl").write_text("{}\n")
    (proj / "sess-a.jsonl.wakatime").write_text("ignore me")
    codex_dir = tmp_path / "codex" / "2026" / "06" / "14"
    codex_dir.mkdir(parents=True)
    (codex_dir / "rollout-2026-06-14T13-16-17-019ec722-5a03-7980-bd45-f34ba5bd9502.jsonl").write_text("{}\n")

    files = discover_session_files(claude_projects_dir=claude_dir, codex_sessions_dir=codex_dir.parents[2])
    by_tool = {f.tool: f for f in files}
    assert set(by_tool) == {"claude_code", "codex"}
    assert by_tool["claude_code"].session_id == "sess-a"
    assert by_tool["codex"].session_id == "019ec722-5a03-7980-bd45-f34ba5bd9502"


def test_openclaw_session_id_from_filename() -> None:
    assert openclaw_session_id(Path("/x/ebf9f4b8-4f8e-442c-84a7-d5015f64fdb7.jsonl")) == "ebf9f4b8-4f8e-442c-84a7-d5015f64fdb7"


def test_discover_openclaw_ignores_trajectory_and_json_sidecars(tmp_path: Path) -> None:
    sessions = tmp_path / "openclaw" / "agents" / "main" / "sessions"
    sessions.mkdir(parents=True)
    (sessions / "sess-oc.jsonl").write_text("{}\n")
    # Sidecars that must NOT be treated as transcripts.
    (sessions / "sess-oc.trajectory.jsonl").write_text("{}\n")
    (sessions / "sess-oc.trajectory-path.json").write_text("{}\n")
    (sessions / "sess-oc.jsonl.codex-app-server.json").write_text("{}\n")
    (sessions / "sessions.json").write_text("{}\n")

    files = discover_session_files(
        claude_projects_dir=None,
        codex_sessions_dir=None,
        openclaw_sessions_dir=sessions,
    )
    assert [(f.tool, f.session_id) for f in files] == [("openclaw", "sess-oc")]


def test_discover_handles_missing_dirs(tmp_path: Path) -> None:
    assert (
        discover_session_files(
            claude_projects_dir=tmp_path / "nope",
            codex_sessions_dir=None,
            openclaw_sessions_dir=tmp_path / "nope-oc",
        )
        == []
    )


# --- state ------------------------------------------------------------------


def test_state_round_trips_progress(tmp_path: Path) -> None:
    state = AgentSessionsUploadState.open(tmp_path / "state.sqlite", account="zach@example.com")
    assert state.progress_for("/x/a.jsonl").uploaded_offset == 0
    state.record_progress(path="/x/a.jsonl", uploaded_offset=120, uploaded_lines=4, now=NOW)
    progress = state.progress_for("/x/a.jsonl")
    assert progress.uploaded_offset == 120
    assert progress.uploaded_lines == 4
    state.close()


def test_state_resets_on_account_change(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite"
    state = AgentSessionsUploadState.open(path, account="a@example.com")
    state.record_progress(path="/x/a.jsonl", uploaded_offset=120, uploaded_lines=4, now=NOW)
    state.close()
    reopened = AgentSessionsUploadState.open(path, account="b@example.com")
    assert reopened.progress_for("/x/a.jsonl").uploaded_offset == 0
    reopened.close()


# --- sync runner ------------------------------------------------------------


def _claude_file(tmp_path: Path, *lines: dict) -> Path:
    proj = tmp_path / "claude" / "-proj"
    proj.mkdir(parents=True, exist_ok=True)
    path = proj / "sess-1.jsonl"
    with path.open("w", encoding="utf-8") as handle:
        for line in lines:
            handle.write(json.dumps(line) + "\n")
    return path


def _runner(tmp_path, store, state, *, mode="incremental", limit=None, batch_size=20000) -> AgentSessionsUploadRunner:
    return AgentSessionsUploadRunner(
        account="zach@example.com",
        device="porygon",
        claude_projects_dir=tmp_path / "claude",
        codex_sessions_dir=None,
        object_store=store,
        logger=FakeLogger(),
        upload_state=state,
        now=lambda: NOW,
        mode=mode,
        limit=limit,
        batch_size=batch_size,
    )


def test_runner_uploads_new_lines_then_only_appended_lines(tmp_path: Path) -> None:
    path = _claude_file(
        tmp_path,
        {"type": "user", "message": {"role": "user", "content": "one"}, "uuid": "u1"},
        {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "two"}]}, "uuid": "u2"},
    )
    store = FakeObjectStore()
    state = AgentSessionsUploadState.open(tmp_path / "state.sqlite", account="zach@example.com")

    summary = _runner(tmp_path, store, state).sync()
    assert summary.lines_selected == 2
    assert summary.batches_uploaded == 1
    records = store.batches[0]
    assert [r["record"]["seq"] for r in records] == [0, 1]
    assert records[0]["record"]["tool"] == "claude_code"
    assert records[0]["device"] == "porygon"
    assert records[0]["record"]["line"]["uuid"] == "u1"

    # Append a third line; only it should upload, with the correct seq.
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"type": "user", "message": {"role": "user", "content": "three"}, "uuid": "u3"}) + "\n")

    summary2 = _runner(tmp_path, store, state).sync()
    assert summary2.lines_selected == 1
    assert [r["record"]["seq"] for r in store.batches[1]] == [2]
    state.close()


def test_runner_ignores_trailing_partial_line(tmp_path: Path) -> None:
    proj = tmp_path / "claude" / "-proj"
    proj.mkdir(parents=True)
    path = proj / "sess-1.jsonl"
    # second line has no trailing newline yet (still being written)
    path.write_text(json.dumps({"type": "user", "uuid": "u1"}) + "\n" + json.dumps({"type": "user", "uuid": "u2"}))
    store = FakeObjectStore()
    state = AgentSessionsUploadState.open(tmp_path / "state.sqlite", account="zach@example.com")

    summary = _runner(tmp_path, store, state).sync()
    assert summary.lines_selected == 1
    assert [r["record"]["line"]["uuid"] for r in store.batches[0]] == ["u1"]

    # Once the line is completed, it uploads on the next run.
    with path.open("a", encoding="utf-8") as handle:
        handle.write("\n")
    summary2 = _runner(tmp_path, store, state).sync()
    assert summary2.lines_selected == 1
    assert [r["record"]["line"]["uuid"] for r in store.batches[1]] == ["u2"]
    state.close()


def test_runner_respects_limit_and_defers_remainder(tmp_path: Path) -> None:
    _claude_file(
        tmp_path,
        {"type": "user", "uuid": "u1"},
        {"type": "user", "uuid": "u2"},
        {"type": "user", "uuid": "u3"},
    )
    store = FakeObjectStore()
    state = AgentSessionsUploadState.open(tmp_path / "state.sqlite", account="zach@example.com")

    summary = _runner(tmp_path, store, state, limit=2).sync()
    assert summary.lines_selected == 2
    summary2 = _runner(tmp_path, store, state, limit=2).sync()
    assert summary2.lines_selected == 1
    assert [r["record"]["line"]["uuid"] for r in store.batches[1]] == ["u3"]
    state.close()


def test_runner_full_mode_reuploads_everything(tmp_path: Path) -> None:
    _claude_file(tmp_path, {"type": "user", "uuid": "u1"}, {"type": "user", "uuid": "u2"})
    store = FakeObjectStore()
    state = AgentSessionsUploadState.open(tmp_path / "state.sqlite", account="zach@example.com")
    _runner(tmp_path, store, state).sync()
    summary = _runner(tmp_path, store, state, mode="full").sync()
    assert summary.lines_selected == 2
    state.close()


def test_runner_chunks_into_multiple_batches(tmp_path: Path) -> None:
    _claude_file(tmp_path, *[{"type": "user", "uuid": f"u{i}"} for i in range(5)])
    store = FakeObjectStore()
    state = AgentSessionsUploadState.open(tmp_path / "state.sqlite", account="zach@example.com")
    summary = _runner(tmp_path, store, state, batch_size=2).sync()
    assert summary.lines_selected == 5
    assert summary.batches_uploaded == 3
    assert [len(b) for b in store.batches] == [2, 2, 1]
    state.close()
