"""Keep Voice Memos running so the local store keeps receiving iCloud changes.

The uploader only sees what is in this Mac's Voice Memos container, and macOS
only pulls Voice Memos CloudKit changes while ``voicememod`` (or the app that
starts it) is alive. With both quit, the store silently freezes: the uploader
stays healthy -- exit 0, ``selected=0`` every run -- while memos recorded on
other devices never reach this machine. Nothing about that reads as an outage.

This is not hypothetical, and it is not new. Apple Notes hit exactly this and
grew ``notes_app.py`` in response; voice memos, its sibling in every other
respect, never got the same guard. Measured on porygon 2026-08-26: the local
store had held 596 recordings since 2026-08-11 11:03, `voicememod` was not
running, the Voice Memos app was not running, and
``marts_ops.pipeline_health`` read `late` at 14.9 days against an SLA of 7 --
a threshold derived from 730 days of history in which this source had never
once gone longer than 6.56 days.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import subprocess
import sys

# Only kick the real system store; test/CI runs point the uploader at
# temporary directories and must never launch a GUI app.
VOICE_MEMOS_STORE_CONTAINER = "group.com.apple.VoiceMemos.shared"
# Launch by BUNDLE ID, not display name. The bundle is `VoiceMemos.app` while
# the app presents as "Voice Memos", and `open -a "Voice Memos"` fails outright
# with "Unable to find application named 'Voice Memos'" -- verified on porygon
# 2026-08-26. Notes.app happens to have no such gap between its two names,
# which is why copying its call shape directly does not work here.
VOICE_MEMOS_BUNDLE_ID = "com.apple.VoiceMemos"
# The process names as `pgrep -x` sees them: the daemon that actually syncs,
# and the app binary (no space) that starts and keeps it alive.
VOICE_MEMOS_PROCESS_NAMES = ("voicememod", "VoiceMemos")

_FALSY = {"0", "false", "no", "off"}


@dataclass(frozen=True)
class VoiceMemosAppKick:
    """What the pre-scan Voice Memos check did.

    ``attempted`` is False when the check does not apply (disabled, not macOS,
    or a non-system store path); ``launched`` is True only when the app was
    actually started by this run.
    """

    attempted: bool
    launched: bool
    reason: str


def ensure_voice_memos_app_running(
    recordings_path: Path | str,
    logger,
    *,
    platform: str | None = None,
    environ=None,
    run=None,
) -> VoiceMemosAppKick:
    platform = platform if platform is not None else sys.platform
    environ = environ if environ is not None else os.environ
    run = run if run is not None else subprocess.run

    flag = environ.get("VOICE_MEMOS_OPEN_APP", "1").strip().lower()
    if flag in _FALSY:
        return VoiceMemosAppKick(attempted=False, launched=False, reason="disabled by VOICE_MEMOS_OPEN_APP")
    if platform != "darwin":
        return VoiceMemosAppKick(attempted=False, launched=False, reason=f"not macOS (platform={platform})")
    if VOICE_MEMOS_STORE_CONTAINER not in Path(recordings_path).expanduser().parts:
        return VoiceMemosAppKick(
            attempted=False, launched=False, reason="recordings path is not the system Voice Memos store"
        )

    # Failures never block the upload: a run against a stale store still
    # uploads whatever has already synced.
    try:
        # `voicememod` is the daemon that actually syncs; the app starts it and
        # keeps it alive. Either one running means CloudKit delivery is live,
        # so check both before launching anything.
        for process_name in VOICE_MEMOS_PROCESS_NAMES:
            if run(["pgrep", "-x", process_name], capture_output=True).returncode == 0:
                return VoiceMemosAppKick(
                    attempted=True, launched=False, reason=f"{process_name} already running"
                )
        launch = run(["open", "-g", "-j", "-b", VOICE_MEMOS_BUNDLE_ID], capture_output=True)
    except OSError as exc:
        logger.warning("Could not launch Voice Memos to resume iCloud sync: %s", exc)
        return VoiceMemosAppKick(attempted=True, launched=False, reason=f"launch error: {exc}")
    if launch.returncode != 0:
        detail = (launch.stderr or b"").decode(errors="replace").strip() or f"exit {launch.returncode}"
        logger.warning("Could not launch Voice Memos to resume iCloud sync: %s", detail)
        return VoiceMemosAppKick(attempted=True, launched=False, reason=f"launch failed: {detail}")
    logger.info("Launched Voice Memos (hidden) so the local store receives iCloud changes")
    return VoiceMemosAppKick(attempted=True, launched=True, reason="launched")
