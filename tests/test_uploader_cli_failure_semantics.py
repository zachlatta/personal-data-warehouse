"""Scheduled uploaders must report mid-run transport failures to launchd.

The shell wrapper records ``last_success`` only when the uploader exits zero.
Returning normally after a request timeout therefore turns a real failed upload
into a green heartbeat. Network-policy and preflight decisions remain deliberate
successful skips inside the runners; this contract covers exceptions after a run
has actually started.
"""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests


class _FakeState:
    entries: dict = {}

    def close(self) -> None:
        pass

    def save(self, _path: Path) -> None:
        pass

    def clear_failures(self) -> int:
        return 0


class _TimeoutRunner:
    def __init__(self, **_kwargs) -> None:
        pass

    def sync(self):
        raise requests.Timeout("mid-run upload timed out")


def _config() -> SimpleNamespace:
    path = Path("/tmp/pdw-uploader-cli-test-source")
    return SimpleNamespace(
        account="test@example.test",
        device="test-device",
        store_path=path,
        library_path=path,
        recordings_path=path,
        extensions=(".m4a",),
        claude_projects_dir=path,
        codex_sessions_dir=path,
        openclaw_sessions_dir=path,
        pi_sessions_dir=path,
        upload_workers=1,
        attachment_bytes_per_run=1,
        attachment_count_per_run=1,
    )


@pytest.mark.parametrize(
    ("module_name", "settings_name", "state_class", "state_loader", "runner_class", "extra_args"),
    [
        (
            "personal_data_warehouse_agent_sessions.cli",
            "agent_sessions",
            "AgentSessionsUploadState",
            "open",
            "AgentSessionsUploadRunner",
            (),
        ),
        (
            "personal_data_warehouse_apple_contacts.cli",
            "apple_contacts",
            "AppleContactsUploadState",
            "open",
            "AppleContactsUploadRunner",
            (),
        ),
        (
            "personal_data_warehouse_apple_messages.cli",
            "apple_messages",
            "AppleMessagesUploadState",
            "open",
            "AppleMessagesUploadRunner",
            (),
        ),
        (
            "personal_data_warehouse_apple_notes.cli",
            "apple_notes",
            "AppleNotesUploadState",
            "load",
            "AppleNotesUploadRunner",
            (),
        ),
        (
            "personal_data_warehouse_photos.cli",
            "photos",
            "PhotosUploadState",
            "open",
            "PhotosUploadRunner",
            (),
        ),
        (
            "personal_data_warehouse_voice_memos.cli",
            "voice_memos",
            "VoiceMemosUploadState",
            "load",
            "VoiceMemosUploadRunner",
            ("--no-writeback",),
        ),
    ],
)
def test_mid_run_timeout_propagates_to_the_scheduler(
    monkeypatch,
    tmp_path,
    module_name,
    settings_name,
    state_class,
    state_loader,
    runner_class,
    extra_args,
) -> None:
    module = import_module(module_name)
    config = _config()
    monkeypatch.setattr(
        module,
        "load_settings",
        lambda **_kwargs: SimpleNamespace(**{settings_name: config}),
    )
    monkeypatch.setattr(module, "ingest_client_from_env", lambda: object())
    monkeypatch.setattr(getattr(module, state_class), state_loader, lambda *args, **kwargs: _FakeState())
    monkeypatch.setattr(module, runner_class, _TimeoutRunner)
    if hasattr(module, "ensure_notes_app_running"):
        monkeypatch.setattr(module, "ensure_notes_app_running", lambda *_args, **_kwargs: None)

    argv = [
        module_name,
        "--state-file",
        str(tmp_path / "state.json"),
        "--lock-file",
        str(tmp_path / "upload.lock"),
        *extra_args,
    ]
    monkeypatch.setattr("sys.argv", argv)

    with pytest.raises(requests.Timeout, match="mid-run upload timed out"):
        module.main()
