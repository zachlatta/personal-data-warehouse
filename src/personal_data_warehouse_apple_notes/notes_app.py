"""Keep Notes.app running so the local store keeps receiving iCloud changes.

The uploader only sees what is in this Mac's NoteStore.sqlite, and macOS only
pulls Notes CloudKit changes while Notes.app is running. With the app quit,
the store silently freezes: the uploader stays healthy (exit 0, selected=0
every run) while edits made on other devices never reach this machine — an
outage invisible to heartbeat checks. Every upload run therefore makes sure
Notes.app is running, launching it hidden and in the background when needed.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import subprocess
import sys

# Only kick the real system store; test/CI runs point the uploader at
# temporary store paths and must never launch a GUI app.
NOTES_STORE_CONTAINER = "group.com.apple.notes"

_FALSY = {"0", "false", "no", "off"}


@dataclass(frozen=True)
class NotesAppKick:
    """What the pre-snapshot Notes.app check did.

    ``attempted`` is False when the check does not apply (disabled, not macOS,
    or a non-system store path); ``launched`` is True only when Notes.app was
    actually started by this run.
    """

    attempted: bool
    launched: bool
    reason: str


def ensure_notes_app_running(
    store_path: Path | str,
    logger,
    *,
    platform: str | None = None,
    environ=None,
    run=None,
) -> NotesAppKick:
    platform = platform if platform is not None else sys.platform
    environ = environ if environ is not None else os.environ
    run = run if run is not None else subprocess.run

    flag = environ.get("APPLE_NOTES_OPEN_NOTES_APP", "1").strip().lower()
    if flag in _FALSY:
        return NotesAppKick(attempted=False, launched=False, reason="disabled by APPLE_NOTES_OPEN_NOTES_APP")
    if platform != "darwin":
        return NotesAppKick(attempted=False, launched=False, reason=f"not macOS (platform={platform})")
    if NOTES_STORE_CONTAINER not in Path(store_path).expanduser().parts:
        return NotesAppKick(attempted=False, launched=False, reason="store path is not the system Notes store")

    # Failures never block the upload: a run against a stale store still
    # uploads whatever has already synced.
    try:
        if run(["pgrep", "-x", "Notes"], capture_output=True).returncode == 0:
            return NotesAppKick(attempted=True, launched=False, reason="Notes.app already running")
        launch = run(["open", "-g", "-j", "-a", "Notes"], capture_output=True)
    except OSError as exc:
        logger.warning("Could not launch Notes.app to resume iCloud sync: %s", exc)
        return NotesAppKick(attempted=True, launched=False, reason=f"launch error: {exc}")
    if launch.returncode != 0:
        detail = (launch.stderr or b"").decode(errors="replace").strip() or f"exit {launch.returncode}"
        logger.warning("Could not launch Notes.app to resume iCloud sync: %s", detail)
        return NotesAppKick(attempted=True, launched=False, reason=f"launch failed: {detail}")
    logger.info("Launched Notes.app (hidden) so the local store receives iCloud changes")
    return NotesAppKick(attempted=True, launched=True, reason="launched")
