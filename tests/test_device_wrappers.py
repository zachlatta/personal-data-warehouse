"""Contract for the device-side wrappers in bin/ and their launchd/systemd units.

Every job that runs on one of Zach's Macs or on the openclaw VM -- the
uploaders, the two resident mutation workers, the browser-session publishers,
the Claude Desktop auth push -- is native Go inside the signed ``pdw`` binary.
The wrappers exist only to pin the environment, record the run verdict and
post the heartbeat. Two things used to be true and are now false, and this is
where they are held false:

* the wrappers do not run ``uv`` or Python anywhere in the exec chain (the
  Python client packages are gone), and
* pdw is IN the chain on purpose. It was kept out to protect TCC grants from
  self-updates; release binaries are signed with a stable identity now, so the
  grants survive and the one binary is the whole client.
"""

from __future__ import annotations

import os
import plistlib
import re
import signal
import subprocess
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
BIN = REPO_ROOT / "bin"
LAUNCHD = REPO_ROOT / "ops" / "launchd"
SYSTEMD = REPO_ROOT / "ops" / "systemd"
ANDROID_MODULE = REPO_ROOT / "ops" / "android" / "pdw-google-recorder"
NOTIFICATIONS_MODULE = REPO_ROOT / "ops" / "android" / "pdw-android-notifications"
MESSAGES_MODULE = REPO_ROOT / "ops" / "android" / "pdw-android-messages"
SCREEN_TIME_MODULE = REPO_ROOT / "ops" / "android" / "pdw-android-screen-time"
SCREEN_TIMELAPSE_MODULE = REPO_ROOT / "ops" / "android" / "pdw-screen-timelapse"
LIB = "_pdw-upload-lib.sh"

# wrapper -> (the pdw invocation it must contain, heartbeat pipelines or None)
WRAPPERS: dict[str, tuple[str, str | None]] = {
    "agent-sessions-upload-launchd": ('"$PDW" ingest agent-sessions --mode incremental', "claude_code,codex,pi,muse_code,argon"),
    "agent-sessions-upload-systemd": ('"$PDW" ingest agent-sessions --mode incremental', "${AGENT_SESSIONS_HEARTBEAT_PIPELINES:-openclaw}"),
    "apple-contacts-upload-launchd": ('"$PDW" ingest apple-contacts --mode incremental', "apple_contacts"),
    "apple-messages-upload-launchd": ('"$PDW" ingest apple-messages --mode incremental', "apple_messages"),
    "browser-history-upload-launchd": ('"$PDW" ingest browser-history --mode incremental', "browser_history"),
    "screen-timelapse-upload-launchd": ('"$PDW" ingest screen-timelapse', "screen_timelapse"),
    "screen-timelapse-upload-android": ('"$PDW" ingest screen-timelapse', "screen_timelapse"),
    "apple-notes-upload-launchd": ('"$PDW" ingest apple-notes --mode incremental', "apple_notes"),
    "voice-memos-upload-launchd": ('"$PDW" ingest voice-memos --mode incremental', "apple_voice_memos"),
    "google-recorder-upload-android": ('"$PDW" ingest google-recorder', "google_recorder"),
    "android-notifications-upload-android": ('"$PDW" ingest android-notifications', "android_notifications"),
    "android-messages-upload-android": ('"$PDW" ingest android-messages', "android_messages"),
    "android-screen-time-upload-android": ('"$PDW" ingest android-screen-time', "android_screen_time"),
    "photos-upload-launchd": ('"$PDW" ingest apple-photos --mode incremental --limit', "apple_photos"),
    "apple-notes-mutation-worker-launchd": ('exec "$PDW" mutations apple-notes', None),
    "apple-contacts-mutation-worker-launchd": ('exec "$PDW" mutations apple-contacts', None),
    "apple-messages-mutation-worker-launchd": ('exec "$PDW" mutations apple-messages', None),
    "android-messages-mutation-worker-launchd": ('exec "$PDW" mutations android-messages', None),
    "chatgpt-auth-launchd": ('"$PDW" chatgpt publish-session --non-interactive', None),
    "claude-desktop-auth-launchd": ('"$PDW" ingest claude-desktop', None),
}

# LaunchAgent label -> (wrapper, resident?)
LAUNCH_AGENTS: dict[str, tuple[str, bool]] = {
    "agent-sessions-upload": ("agent-sessions-upload-launchd", False),
    "apple-contacts-upload": ("apple-contacts-upload-launchd", False),
    "apple-messages-upload": ("apple-messages-upload-launchd", False),
    "browser-history-upload": ("browser-history-upload-launchd", False),
    "screen-timelapse-upload": ("screen-timelapse-upload-launchd", False),
    "apple-notes-upload": ("apple-notes-upload-launchd", False),
    "voice-memos-upload": ("voice-memos-upload-launchd", False),
    "photos-upload": ("photos-upload-launchd", False),
    "apple-notes-mutation-worker": ("apple-notes-mutation-worker-launchd", True),
    "apple-contacts-mutation-worker": ("apple-contacts-mutation-worker-launchd", True),
    "apple-messages-mutation-worker": ("apple-messages-mutation-worker-launchd", True),
    "android-messages-mutation-worker": ("android-messages-mutation-worker-launchd", True),
    "chatgpt-auth": ("chatgpt-auth-launchd", False),
    "claude-desktop-auth": ("claude-desktop-auth-launchd", False),
}

_INTERPRETER_INVOCATION = re.compile(r"(?m)^[^#\n]*(\buv\s+run\b|\bpython3?\b\s+-m|/bin/uv\b|\bUV_BIN\b|\bPDW_UV_BIN\b)")


def _wrapper(name: str) -> str:
    return (BIN / name).read_text()


@pytest.mark.parametrize("name", sorted(WRAPPERS))
def test_wrapper_runs_the_signed_pdw_binary_and_nothing_else(name: str) -> None:
    text = _wrapper(name)
    invocation, _ = WRAPPERS[name]
    assert invocation in text, f"{name} must invoke {invocation!r}"
    assert 'PDW="$(pdw_resolve_bin)"' in text, f"{name} must resolve pdw through the lib"
    assert f"bin/{LIB}" in text or f"/{LIB}" in text, f"{name} must source {LIB}"
    offenders = [m.group(0).strip() for m in _INTERPRETER_INVOCATION.finditer(text)]
    assert offenders == [], f"{name} still runs uv/python: {offenders}"


@pytest.mark.parametrize("name", sorted(WRAPPERS))
def test_wrapper_fails_loudly_without_a_pdw_binary(name: str) -> None:
    assert 'if [ -z "$PDW" ]' in _wrapper(name), name


@pytest.mark.parametrize("name", sorted(n for n, (_, p) in WRAPPERS.items() if p))
def test_uploader_wrapper_posts_its_heartbeat_through_pdw_heartbeat(name: str) -> None:
    text = _wrapper(name)
    _, pipelines = WRAPPERS[name]
    assert f'pdw_post_heartbeat "{pipelines}"' in text
    assert "pdw_record_run" in text
    lib = (BIN / LIB).read_text()
    assert '"$_pdw" heartbeat' in lib


def test_no_wrapper_is_missing_from_the_contract() -> None:
    present = sorted(
        p.name for p in BIN.iterdir() if p.name.endswith(("-launchd", "-systemd", "-android")) and "-status" not in p.name
    )
    assert present == sorted(WRAPPERS)


@pytest.mark.parametrize("label", sorted(LAUNCH_AGENTS))
def test_launch_agent_points_at_its_wrapper(label: str) -> None:
    wrapper, resident = LAUNCH_AGENTS[label]
    full_label = f"com.zachlatta.personal-data-warehouse.{label}"
    with (LAUNCHD / f"{full_label}.plist").open("rb") as handle:
        plist = plistlib.load(handle)
    assert plist["Label"] == full_label
    assert plist["ProgramArguments"] == [f"/Users/zrl/.local/share/personal-data-warehouse/bin/{wrapper}"]
    assert plist["RunAtLoad"] is True
    if resident:
        assert plist["KeepAlive"] is True
        assert "StartInterval" not in plist
    else:
        assert plist["StartInterval"] in (300, 900, 1800, 3600), label
    env = plist.get("EnvironmentVariables", {})
    assert not any("uv" in k.lower() or "python" in v.lower() for k, v in env.items()), env


def test_every_launch_agent_plist_is_in_the_contract() -> None:
    present = sorted(p.stem.removeprefix("com.zachlatta.personal-data-warehouse.") for p in LAUNCHD.glob("*.plist"))
    assert present == sorted(LAUNCH_AGENTS)


def test_uploaders_run_every_five_minutes_and_photos_every_thirty() -> None:
    def interval(label: str) -> int:
        with (LAUNCHD / f"com.zachlatta.personal-data-warehouse.{label}.plist").open("rb") as handle:
            return plistlib.load(handle)["StartInterval"]

    for label in ("agent-sessions-upload", "apple-contacts-upload", "apple-messages-upload", "apple-notes-upload", "voice-memos-upload", "screen-timelapse-upload"):
        assert interval(label) == 300, label
    assert interval("photos-upload") == 1800
    # History lands in the browser's own database continuously; fifteen
    # minutes keeps the copy of a large History file off the hot path.
    assert interval("browser-history-upload") == 900
    for label in ("chatgpt-auth", "claude-desktop-auth"):
        assert interval(label) == 3600, label


def test_nothing_schedules_the_slack_session_publisher() -> None:
    """The Slack session is pasted by hand, never captured on a schedule.

    Until 2026-09-29 an hourly LaunchAgent on crobat read the Slack desktop
    app's own login and called auth.test + client.counts with it from Go.
    Slack flagged every run as `unexpected_scraping` ("Go-based tool") and
    signed Zach out of every device -- four times on 09-29, each to the second
    of a run. Re-adding a scheduled capture brings that back.
    """
    offenders = [
        p.name
        for directory in (BIN, LAUNCHD, SYSTEMD)
        for p in directory.iterdir()
        if p.is_file() and ("slack publish-session" in p.read_text(errors="ignore") or "slack-auth" in p.name)
    ]
    assert offenders == [], offenders


def test_systemd_unit_runs_the_self_locating_wrapper() -> None:
    service = (SYSTEMD / "personal-data-warehouse-agent-sessions-upload.service").read_text()
    timer = (SYSTEMD / "personal-data-warehouse-agent-sessions-upload.timer").read_text()
    assert "ExecStart=%h/dev/zachlatta/personal-data-warehouse/bin/agent-sessions-upload-systemd" in service
    assert "OnUnitActiveSec=300s" in timer
    assert not _INTERPRETER_INVOCATION.search(service)


def test_message_and_note_uploaders_also_fire_when_the_store_changes() -> None:
    """The five-minute tick is a floor; a new message should not wait for it.

    Measured 2026-09-25, iMessage landed p50 5 min / p95 9 min and the whole
    of it was two serial five-minute clocks. launchd WatchPaths on the store's
    WAL fires the upload seconds after Messages.app writes; ThrottleInterval
    keeps a chatty WAL to one run a minute.
    """
    expected = {
        "apple-messages-upload": "/Users/zrl/Library/Messages/chat.db-wal",
        "apple-notes-upload": "/Users/zrl/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite-wal",
    }
    for label, path in expected.items():
        with (LAUNCHD / f"com.zachlatta.personal-data-warehouse.{label}.plist").open("rb") as handle:
            plist = plistlib.load(handle)
        assert plist["WatchPaths"] == [path], label
        assert plist["ThrottleInterval"] == 60, label
        assert plist["StartInterval"] == 300, label


def test_voice_memos_uploader_fires_within_seconds_of_a_new_memo() -> None:
    """A voice memo should land in ~30 s, so its uploader cannot wait for a tick.

    Measured 2026-10-16, Mac-file-to-uploaded was p50 5.4 min: a 120 s file-age
    gate plus the five-minute StartInterval. The store's WAL changes when a memo
    arrives from iCloud and again when its audio finishes downloading, and the
    directory changes when the file appears, so either fires a run. The run
    itself waits out a few seconds of settle, which is why the throttle is
    10 s rather than the 60 s a chatty message store needs.
    """
    recordings = "/Users/zrl/Library/Group Containers/group.com.apple.VoiceMemos.shared/Recordings"
    with (LAUNCHD / "com.zachlatta.personal-data-warehouse.voice-memos-upload.plist").open("rb") as handle:
        plist = plistlib.load(handle)
    assert plist["WatchPaths"] == [f"{recordings}/CloudRecordings.db-wal", recordings]
    assert plist["ThrottleInterval"] == 10
    assert plist["StartInterval"] == 300


# The Pixel's Google Recorder uploader runs under Magisk, not launchd: Android's
# /system/bin/sh (mksh), no checkout on the phone, and the phone suspends
# within seconds of the screen going off.
_BASHISMS = re.compile(r"(?m)^[^#\n]*(\[\[\s|\bfunction\b|\$\{[A-Za-z_]+\[|\bsource\b|<<<|\bdeclare\b)")


def test_the_android_scripts_are_plain_sh_for_android() -> None:
    scripts = [
        BIN / "google-recorder-upload-android",
        BIN / "android-notifications-upload-android",
        BIN / "android-messages-upload-android",
        BIN / "android-screen-time-upload-android",
        BIN / "screen-timelapse-upload-android",
        BIN / LIB,
        *sorted(ANDROID_MODULE.iterdir()),
        *sorted(NOTIFICATIONS_MODULE.iterdir()),
        *sorted(MESSAGES_MODULE.iterdir()),
        *sorted(SCREEN_TIME_MODULE.iterdir()),
        SCREEN_TIMELAPSE_MODULE / "service.sh",
    ]
    for script in scripts:
        text = script.read_text()
        if script.name != LIB:
            assert text.startswith("#!/system/bin/sh\n"), script.name
            assert script.stat().st_mode & 0o111, f"{script.name} must be executable"
        offenders = [m.group(0).strip() for m in _BASHISMS.finditer(text)]
        assert offenders == [], f"{script.name} uses shell features mksh lacks: {offenders}"


def test_the_screen_timelapse_android_service_runs_every_five_minutes_and_compresses_pngs() -> None:
    service = (SCREEN_TIMELAPSE_MODULE / "service.sh").read_text()
    assert '"$MODDIR/bin/screen-timelapse-upload-android"' in service
    assert "FLOOR=300" in service
    state = re.search(r"(?m)^STATE=(\S+)$", service).group(1)
    assert state == "/data/misc_ce/0/pdw-screen-timelapse"
    text = _wrapper("screen-timelapse-upload-android")
    assert 'STATE_DIR="${PDW_SCREEN_TIMELAPSE_STATE_DIR:-/data/misc_ce/0/pdw-screen-timelapse}"' in text
    assert 'SOURCES="${SCREEN_TIMELAPSE_SOURCES:-pichu=/data/adb/screen-timelapse/out}"' in text
    assert '--sources "$SOURCES"' in text
    assert '--state-file "$STATE_DIR/screen-timelapse-upload-state.sqlite"' in text
    assert "--compress-png" in text
    assert "--delete-after-upload" in text


# Every Magisk service bounds a run with toybox `timeout N`, which sends TERM
# to the wrapper's shell alone. A wrapper that ran pdw in the foreground died
# there and left pdw (and the `content read` / Skia helper children it starts)
# running with the wakelock released, so it suspended mid-run and held the
# next run's lock (pichu, 2026-10-29). Each wrapper therefore runs pdw through
# the lib's pdw_run_stoppable, and each service holds its wakelock 30 s past
# the timeout so the phone stays awake while that teardown finishes.
# module -> (wrapper, state-dir env var, timeout seconds)
ANDROID_UPLOADERS: dict[str, tuple[str, str, int]] = {
    "pdw-google-recorder": ("google-recorder-upload-android", "PDW_GOOGLE_RECORDER_STATE_DIR", 960),
    "pdw-android-notifications": ("android-notifications-upload-android", "PDW_ANDROID_NOTIFICATIONS_STATE_DIR", 660),
    "pdw-android-messages": ("android-messages-upload-android", "PDW_ANDROID_MESSAGES_STATE_DIR", 660),
    "pdw-android-screen-time": ("android-screen-time-upload-android", "PDW_ANDROID_SCREEN_TIME_STATE_DIR", 360),
    "pdw-screen-timelapse": ("screen-timelapse-upload-android", "PDW_SCREEN_TIMELAPSE_STATE_DIR", 360),
}
STOP_WAKELOCK_MARGIN_SECONDS = 30


def test_every_android_wrapper_is_in_the_stoppable_run_contract() -> None:
    present = sorted(p.name for p in BIN.iterdir() if p.name.endswith("-upload-android"))
    assert present == sorted(w for w, _, _ in ANDROID_UPLOADERS.values())
    modules = sorted(p.name for p in (REPO_ROOT / "ops" / "android").iterdir() if (p / "service.sh").exists())
    assert modules == sorted(ANDROID_UPLOADERS)


@pytest.mark.parametrize("module", sorted(ANDROID_UPLOADERS))
def test_android_service_holds_its_wakelock_past_the_timeout_under_platform_sh(module: str) -> None:
    wrapper, _, seconds = ANDROID_UPLOADERS[module]
    service = (REPO_ROOT / "ops" / "android" / module / "service.sh").read_text()
    # Magisk's standalone BusyBox ash would shadow toybox timeout/pgrep/pkill.
    assert "unset ASH_STANDALONE" in service
    assert 'exec /system/bin/sh "$0" "$@"' in service
    run = f'timeout {seconds} sh "$MODDIR/bin/{wrapper}"'
    assert re.findall(r"(?m)^\s*timeout \d+ .*$", service) == [f"    {run} >/dev/null 2>&1"]
    wakelock_ns = (seconds + STOP_WAKELOCK_MARGIN_SECONDS) * 1_000_000_000
    lock = f'echo "$WAKE {wakelock_ns}" > /sys/power/wake_lock'
    assert re.findall(r'echo "\$WAKE \d+" > /sys/power/wake_lock', service) == [lock]
    assert service.index(lock) < service.index(run) < service.index('echo "$WAKE" > /sys/power/wake_unlock')


@pytest.mark.parametrize("module", sorted(ANDROID_UPLOADERS))
def test_android_wrapper_runs_pdw_through_the_shared_stoppable_runner(module: str) -> None:
    wrapper, _, _ = ANDROID_UPLOADERS[module]
    text = _wrapper(wrapper)
    invocation, _ = WRAPPERS[wrapper]
    runner = r'pdw_run_stoppable "\$HEARTBEAT_FILE" "\$LOG_FILE" "[a-z ]+" \\\n\s+' + re.escape(invocation)
    assert re.search(runner, text), f"{wrapper} must run {invocation!r} through pdw_run_stoppable"
    assert text.count('"$PDW" ingest') == 1, f"{wrapper} must not run pdw outside the runner"
    # The stop routine lives once, in the lib.
    assert "trap " not in text and "pgrep" not in text and "pkill" not in text


def test_the_stop_routine_lives_only_in_the_lib() -> None:
    lib = (BIN / LIB).read_text()
    assert "pdw_run_stoppable() {" in lib
    owners = sorted(p.name for p in BIN.iterdir() if p.is_file() and re.search(r"\bpgrep -P\b|\bpkill -P\b", p.read_text()))
    assert owners == [LIB]


_FAKE_PDW = """#!/bin/sh
set -u
if [ "${1:-}" != "ingest" ]; then
  echo "pdw_$1" >> "$FAKE_DIR/events.log"
  exit 0
fi
lock_file="$FAKE_DIR/run.lock"
events_file="$FAKE_DIR/events.log"
echo "$$" > "$lock_file"
echo "$$" > "$FAKE_DIR/pdw.pid"
sleep 60 &
echo "$!" > "$FAKE_DIR/child_term.pid"
sh -c "trap '' TERM; exec sleep 60" &
echo "$!" > "$FAKE_DIR/child_stubborn.pid"
cleanup() {
  trap - TERM
  sleep 0.2
  rm -f "$lock_file"
  echo "pdw_exited" >> "$events_file"
  exit 143
}
trap cleanup TERM
: > "$FAKE_DIR/ready"
sleep 60 &
wait $!
"""


@pytest.mark.parametrize("module", sorted(ANDROID_UPLOADERS))
def test_android_wrapper_timeout_stops_process_tree_and_waits_for_pdw_exit(tmp_path: Path, module: str) -> None:
    wrapper, state_env, _ = ANDROID_UPLOADERS[module]
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    fake_dir = tmp_path / "fake"
    fake_dir.mkdir()
    fake_pdw = tmp_path / "fake-pdw"
    fake_pdw.write_text(_FAKE_PDW)
    fake_pdw.chmod(0o755)

    env = os.environ.copy()
    env[state_env] = str(state_dir)
    env["FAKE_DIR"] = str(fake_dir)
    env["PDW_BIN"] = str(fake_pdw)
    env["PDW_STOP_GRACE_TICKS"] = "3"

    proc = subprocess.Popen(
        ["sh", str(BIN / wrapper)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 5.0
        while not (fake_dir / "ready").exists():
            assert time.monotonic() < deadline, "fake pdw did not reach ready state"
            time.sleep(0.05)

        pids = [int((fake_dir / f"{name}.pid").read_text()) for name in ("pdw", "child_term", "child_stubborn")]
        lock_file = fake_dir / "run.lock"
        assert lock_file.exists()

        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=5.0)
        with (fake_dir / "events.log").open("a") as handle:
            handle.write("wrapper_exited\n")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=2.0)

    assert proc.returncode == 143
    assert not lock_file.exists(), "lock file must be released before wrapper exits"
    assert (fake_dir / "events.log").read_text().splitlines() == ["pdw_exited", "wrapper_exited"]
    for pid in pids:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)

    # Recorded locally only: events.log above has no "pdw_heartbeat", so a
    # hung network cannot stall the teardown inside the wakelock margin.
    base = wrapper.removesuffix("-android")
    logs = state_dir / "logs"
    assert (logs / f"{base}.heartbeat").read_text().strip().endswith(" exit_code=143")
    assert not (logs / f"{base}.last-success").exists()
    run_log = (logs / f"{base}.run.log").read_text()
    assert re.search(r"(?m)^\[[^]]+\] finished .* upload exit_code=143 duration_seconds=\d+$", run_log)


def test_the_stoppable_runner_returns_the_exit_code_and_clears_its_trap(tmp_path: Path) -> None:
    script = tmp_path / "run.sh"
    log = tmp_path / "run.log"
    script.write_text(
        f'. "{BIN / LIB}"\n'
        f'pdw_run_stoppable "{tmp_path}/hb" "{log}" test sh -c "echo hello; exit 7"\n'
        'echo "code=$?"\n'
        "trap\n"
    )
    out = subprocess.run(["sh", str(script)], capture_output=True, text=True, timeout=10).stdout
    assert out.splitlines() == ["code=7"]
    assert log.read_text() == "hello\n"
    assert not (tmp_path / "hb").exists()


def test_android_screen_batches_leave_time_for_the_service_deadline() -> None:
    # Compression and upload happen serially on the phone. The CLI default
    # (300 files) can outlive the supervisor's six-minute timeout.
    # Forty files still drain faster than the capture loop produces them.
    text = _wrapper("screen-timelapse-upload-android")
    assert "--limit 40" in text
    service = (SCREEN_TIMELAPSE_MODULE / "service.sh").read_text()
    assert "timeout 360" in service


def test_the_android_service_runs_the_wrapper_on_every_recorder_write_and_holds_the_phone_awake() -> None:
    service = (ANDROID_MODULE / "service.sh").read_text()
    wake = (ANDROID_MODULE / "google-recorder-upload-wake").read_text()
    assert '"$MODDIR/bin/google-recorder-upload-android"' in service
    assert 'inotifyd "$MODDIR/bin/google-recorder-upload-wake" "$REC/databases:c" "$REC/files/recordings:' in service
    # The handler's marker and wakelock are what the loop waits on.
    state = re.search(r"(?m)^STATE=(\S+)$", service).group(1)
    assert f": > {state}/due" in wake
    assert "/sys/power/wake_lock" in wake


def test_the_android_wrapper_keeps_its_state_out_of_recorders_directory() -> None:
    text = _wrapper("google-recorder-upload-android")
    # Credential-encrypted storage: the token is the master secret and the
    # bootloader is unlocked, so it must not sit in /data/adb.
    assert 'STATE_DIR="${PDW_GOOGLE_RECORDER_STATE_DIR:-/data/misc_ce/0/pdw-google-recorder}"' in text
    assert '--state-file "$STATE_DIR/google-recorder-upload-state.sqlite"' in text
    assert 'export PDW_BIN="${PDW_BIN:-$SCRIPT_DIR/pdw}"' in text


def test_the_notification_service_sends_the_spool_on_every_finished_event_and_holds_the_phone_awake() -> None:
    service = (NOTIFICATIONS_MODULE / "service.sh").read_text()
    wake = (NOTIFICATIONS_MODULE / "android-notifications-upload-wake").read_text()
    assert '"$MODDIR/bin/android-notifications-upload-android"' in service
    # The listener renames each finished event into the spool: moved_to.
    assert 'inotifyd "$MODDIR/bin/android-notifications-upload-wake" "$SPOOL:y"' in service
    assert 'cmd notification allow_listener "$LISTENER"' in service
    state = re.search(r"(?m)^STATE=(\S+)$", service).group(1)
    assert f": > {state}/due" in wake
    assert "/sys/power/wake_lock" in wake
    text = _wrapper("android-notifications-upload-android")
    assert 'STATE_DIR="${PDW_ANDROID_NOTIFICATIONS_STATE_DIR:-/data/misc_ce/0/pdw-android-notifications}"' in text
    assert state == "/data/misc_ce/0/pdw-android-notifications"


def test_the_messages_service_runs_on_every_messages_write_and_holds_the_phone_awake() -> None:
    service = (MESSAGES_MODULE / "service.sh").read_text()
    wake = (MESSAGES_MODULE / "android-messages-upload-wake").read_text()
    assert '"$MODDIR/bin/android-messages-upload-android"' in service
    assert 'inotifyd "$MODDIR/bin/android-messages-upload-wake" "$DBDIR/bugle_db-wal:c"' in service
    state = re.search(r"(?m)^STATE=(\S+)$", service).group(1)
    assert f": > {state}/due" in wake
    assert "/sys/power/wake_lock" in wake
    text = _wrapper("android-messages-upload-android")
    assert 'STATE_DIR="${PDW_ANDROID_MESSAGES_STATE_DIR:-/data/misc_ce/0/pdw-android-messages}"' in text
    assert '--state-file "$STATE_DIR/android-messages-upload.json"' in text
    assert state == "/data/misc_ce/0/pdw-android-messages"


def test_the_screen_time_service_runs_every_five_minutes_and_keeps_usage_access() -> None:
    service = (SCREEN_TIME_MODULE / "service.sh").read_text()
    assert '"$MODDIR/bin/android-screen-time-upload-android"' in service
    assert 'appops set "$PKG" GET_USAGE_STATS allow' in service
    assert "FLOOR=300" in service
    state = re.search(r"(?m)^STATE=(\S+)$", service).group(1)
    assert state == "/data/misc_ce/0/pdw-android-screen-time"
    text = _wrapper("android-screen-time-upload-android")
    assert 'STATE_DIR="${PDW_ANDROID_SCREEN_TIME_STATE_DIR:-/data/misc_ce/0/pdw-android-screen-time}"' in text
    assert '--state-file "$STATE_DIR/android-screen-time-upload.json"' in text
