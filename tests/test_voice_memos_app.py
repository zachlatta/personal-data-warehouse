"""The voice-memos sibling of tests/test_apple_notes_notes_app.py.

Apple Notes learned that macOS freezes the local store when the app is not
running, and that the uploader reports perfect health throughout. Voice memos
has the same shape and did not have the same guard, which is how it went 15
days without a recording on porygon while every check said OK.
"""

from __future__ import annotations

import logging
from pathlib import Path

from personal_data_warehouse_voice_memos.voice_memos_app import (
    VOICE_MEMOS_STORE_CONTAINER,
    ensure_voice_memos_app_running,
)

LOGGER = logging.getLogger(__name__)
SYSTEM_STORE = Path.home() / "Library/Group Containers" / VOICE_MEMOS_STORE_CONTAINER / "Recordings"


class _Run:
    """Record subprocess calls and answer pgrep/open with canned results."""

    def __init__(self, running: set[str], launch_returncode: int = 0) -> None:
        self.running = running
        self.launch_returncode = launch_returncode
        self.calls: list[list[str]] = []

    def __call__(self, argv, capture_output=False):
        self.calls.append(list(argv))

        class Result:
            pass

        result = Result()
        if argv[0] == "pgrep":
            result.returncode = 0 if argv[-1] in self.running else 1
            return result
        result.returncode = self.launch_returncode
        result.stderr = b"boom" if self.launch_returncode else b""
        return result


def test_a_frozen_store_gets_the_app_launched_hidden() -> None:
    run = _Run(running=set())
    kick = ensure_voice_memos_app_running(
        SYSTEM_STORE, LOGGER, platform="darwin", environ={}, run=run
    )
    assert kick.attempted and kick.launched
    assert ["open", "-g", "-j", "-b", "com.apple.VoiceMemos"] in run.calls, (
        "launch by BUNDLE ID: the bundle is VoiceMemos.app while the app presents as\n"
        "'Voice Memos', and open -a 'Voice Memos' fails outright. Hidden (-g) and\n"
        "non-activating (-j) because porygon is headless."
    )


def test_a_live_sync_daemon_is_enough_and_nothing_is_launched() -> None:
    """`voicememod` alone means CloudKit delivery is live.

    Launching the GUI app when the daemon is already syncing would be pointless
    churn on a headless Mac.
    """

    run = _Run(running={"voicememod"})
    kick = ensure_voice_memos_app_running(
        SYSTEM_STORE, LOGGER, platform="darwin", environ={}, run=run
    )
    assert kick.attempted and not kick.launched
    assert "voicememod" in kick.reason
    assert not any(call[0] == "open" for call in run.calls)


def test_the_check_never_touches_a_test_store_path(tmp_path) -> None:
    """CI and unit runs point the uploader at temp dirs; they must not launch a GUI app."""

    run = _Run(running=set())
    kick = ensure_voice_memos_app_running(tmp_path, LOGGER, platform="darwin", environ={}, run=run)
    assert not kick.attempted and not kick.launched
    assert not run.calls


def test_the_kill_switch_and_non_macos_hosts_are_respected(tmp_path) -> None:
    run = _Run(running=set())
    off = ensure_voice_memos_app_running(
        SYSTEM_STORE, LOGGER, platform="darwin", environ={"VOICE_MEMOS_OPEN_APP": "0"}, run=run
    )
    assert not off.attempted
    linux = ensure_voice_memos_app_running(
        SYSTEM_STORE, LOGGER, platform="linux", environ={}, run=run
    )
    assert not linux.attempted
    assert not run.calls


def test_a_failed_launch_never_blocks_the_upload() -> None:
    """A stale store still holds memos worth uploading."""

    run = _Run(running=set(), launch_returncode=1)
    kick = ensure_voice_memos_app_running(
        SYSTEM_STORE, LOGGER, platform="darwin", environ={}, run=run
    )
    assert kick.attempted and not kick.launched
    assert "launch failed" in kick.reason


def test_the_sync_run_kicks_the_store_before_it_trusts_what_it_scanned() -> None:
    """Order matters: scanning first would read the frozen store and report OK."""

    import inspect

    from personal_data_warehouse_voice_memos import sync

    source = inspect.getsource(sync.VoiceMemosUploadRunner.sync)
    kick_at = source.index("ensure_voice_memos_app_running")
    scan_at = source.index("scan_voice_memo_file_candidates")
    assert kick_at < scan_at, "the app check must run BEFORE the scan, or it cannot help this run"
