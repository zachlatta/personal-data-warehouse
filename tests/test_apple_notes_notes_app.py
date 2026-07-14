from __future__ import annotations

from personal_data_warehouse_apple_notes.notes_app import ensure_notes_app_running

SYSTEM_STORE_PATH = "~/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite"


class FakeLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, *args, **kwargs) -> None:
        self.messages.append(args[0] % args[1:] if len(args) > 1 else str(args[0]))

    def warning(self, *args, **kwargs) -> None:
        self.messages.append(args[0] % args[1:] if len(args) > 1 else str(args[0]))


class FakeCompleted:
    def __init__(self, returncode: int, stderr: bytes = b"") -> None:
        self.returncode = returncode
        self.stderr = stderr


class FakeRun:
    """Records subprocess invocations; returns per-command results."""

    def __init__(self, *, pgrep_returncode: int, open_returncode: int = 0, error: OSError | None = None) -> None:
        self.calls: list[list[str]] = []
        self._pgrep_returncode = pgrep_returncode
        self._open_returncode = open_returncode
        self._error = error

    def __call__(self, argv, **kwargs) -> FakeCompleted:
        self.calls.append(list(argv))
        if self._error is not None:
            raise self._error
        if argv[0] == "pgrep":
            return FakeCompleted(self._pgrep_returncode)
        return FakeCompleted(self._open_returncode, stderr=b"boom" if self._open_returncode else b"")


def test_launches_hidden_notes_app_when_not_running() -> None:
    run = FakeRun(pgrep_returncode=1)
    logger = FakeLogger()
    kick = ensure_notes_app_running(SYSTEM_STORE_PATH, logger, platform="darwin", environ={}, run=run)
    assert kick.launched
    assert run.calls == [["pgrep", "-x", "Notes"], ["open", "-g", "-j", "-a", "Notes"]]
    assert any("Launched Notes.app" in message for message in logger.messages)


def test_skips_launch_when_notes_app_already_running() -> None:
    run = FakeRun(pgrep_returncode=0)
    logger = FakeLogger()
    kick = ensure_notes_app_running(SYSTEM_STORE_PATH, logger, platform="darwin", environ={}, run=run)
    assert not kick.launched
    assert kick.attempted
    assert run.calls == [["pgrep", "-x", "Notes"]]
    assert logger.messages == []


def test_disabled_by_env_flag() -> None:
    run = FakeRun(pgrep_returncode=1)
    kick = ensure_notes_app_running(
        SYSTEM_STORE_PATH,
        FakeLogger(),
        platform="darwin",
        environ={"APPLE_NOTES_OPEN_NOTES_APP": "0"},
        run=run,
    )
    assert not kick.attempted
    assert not kick.launched
    assert run.calls == []


def test_skips_on_non_macos() -> None:
    run = FakeRun(pgrep_returncode=1)
    kick = ensure_notes_app_running(SYSTEM_STORE_PATH, FakeLogger(), platform="linux", environ={}, run=run)
    assert not kick.attempted
    assert run.calls == []


def test_skips_for_overridden_store_path(tmp_path) -> None:
    run = FakeRun(pgrep_returncode=1)
    kick = ensure_notes_app_running(tmp_path / "NoteStore.sqlite", FakeLogger(), platform="darwin", environ={}, run=run)
    assert not kick.attempted
    assert run.calls == []


def test_launch_failure_warns_and_does_not_raise() -> None:
    run = FakeRun(pgrep_returncode=1, open_returncode=1)
    logger = FakeLogger()
    kick = ensure_notes_app_running(SYSTEM_STORE_PATH, logger, platform="darwin", environ={}, run=run)
    assert kick.attempted
    assert not kick.launched
    assert any("Could not launch Notes.app" in message for message in logger.messages)


def test_subprocess_oserror_warns_and_does_not_raise() -> None:
    run = FakeRun(pgrep_returncode=1, error=OSError("no such binary"))
    logger = FakeLogger()
    kick = ensure_notes_app_running(SYSTEM_STORE_PATH, logger, platform="darwin", environ={}, run=run)
    assert kick.attempted
    assert not kick.launched
    assert any("Could not launch Notes.app" in message for message in logger.messages)
