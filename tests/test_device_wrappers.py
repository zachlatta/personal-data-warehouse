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

import plistlib
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
BIN = REPO_ROOT / "bin"
LAUNCHD = REPO_ROOT / "ops" / "launchd"
SYSTEMD = REPO_ROOT / "ops" / "systemd"
LIB = "_pdw-upload-lib.sh"

# wrapper -> (the pdw invocation it must contain, heartbeat pipelines or None)
WRAPPERS: dict[str, tuple[str, str | None]] = {
    "agent-sessions-upload-launchd": ('"$PDW" ingest agent-sessions --mode incremental', "claude_code,codex,pi"),
    "agent-sessions-upload-systemd": ('"$PDW" ingest agent-sessions --mode incremental', "openclaw"),
    "apple-contacts-upload-launchd": ('"$PDW" ingest apple-contacts --mode incremental', "apple_contacts"),
    "apple-messages-upload-launchd": ('"$PDW" ingest apple-messages --mode incremental', "apple_messages"),
    "apple-notes-upload-launchd": ('"$PDW" ingest apple-notes --mode incremental', "apple_notes"),
    "voice-memos-upload-launchd": ('"$PDW" ingest voice-memos --mode incremental', "apple_voice_memos"),
    "photos-upload-launchd": ('"$PDW" ingest apple-photos --mode incremental --limit', "apple_photos"),
    "apple-notes-mutation-worker-launchd": ('exec "$PDW" mutations apple-notes', None),
    "apple-contacts-mutation-worker-launchd": ('exec "$PDW" mutations apple-contacts', None),
    "slack-auth-launchd": ('"$PDW" slack publish-session', None),
    "chatgpt-auth-launchd": ('"$PDW" chatgpt publish-session --non-interactive', None),
    "claude-desktop-auth-launchd": ('"$PDW" ingest claude-desktop', None),
}

# LaunchAgent label -> (wrapper, resident?)
LAUNCH_AGENTS: dict[str, tuple[str, bool]] = {
    "agent-sessions-upload": ("agent-sessions-upload-launchd", False),
    "apple-contacts-upload": ("apple-contacts-upload-launchd", False),
    "apple-messages-upload": ("apple-messages-upload-launchd", False),
    "apple-notes-upload": ("apple-notes-upload-launchd", False),
    "voice-memos-upload": ("voice-memos-upload-launchd", False),
    "photos-upload": ("photos-upload-launchd", False),
    "apple-notes-mutation-worker": ("apple-notes-mutation-worker-launchd", True),
    "apple-contacts-mutation-worker": ("apple-contacts-mutation-worker-launchd", True),
    "slack-auth": ("slack-auth-launchd", False),
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
    present = sorted(p.name for p in BIN.iterdir() if p.name.endswith(("-launchd", "-systemd")) and "-status" not in p.name)
    assert present == sorted(WRAPPERS)


@pytest.mark.parametrize("label", sorted(LAUNCH_AGENTS))
def test_launch_agent_points_at_its_wrapper(label: str) -> None:
    wrapper, resident = LAUNCH_AGENTS[label]
    full_label = f"com.zachlatta.personal-data-warehouse.{label}"
    with (LAUNCHD / f"{full_label}.plist").open("rb") as handle:
        plist = plistlib.load(handle)
    assert plist["Label"] == full_label
    assert plist["ProgramArguments"] == [f"/Users/zrl/dev/zachlatta/personal-data-warehouse/bin/{wrapper}"]
    assert plist["RunAtLoad"] is True
    if resident:
        assert plist["KeepAlive"] is True
        assert "StartInterval" not in plist
    else:
        assert plist["StartInterval"] in (300, 1800, 3600), label
    env = plist.get("EnvironmentVariables", {})
    assert not any("uv" in k.lower() or "python" in v.lower() for k, v in env.items()), env


def test_every_launch_agent_plist_is_in_the_contract() -> None:
    present = sorted(p.stem.removeprefix("com.zachlatta.personal-data-warehouse.") for p in LAUNCHD.glob("*.plist"))
    assert present == sorted(LAUNCH_AGENTS)


def test_uploaders_run_every_five_minutes_and_photos_every_thirty() -> None:
    def interval(label: str) -> int:
        with (LAUNCHD / f"com.zachlatta.personal-data-warehouse.{label}.plist").open("rb") as handle:
            return plistlib.load(handle)["StartInterval"]

    for label in ("agent-sessions-upload", "apple-contacts-upload", "apple-messages-upload", "apple-notes-upload", "voice-memos-upload"):
        assert interval(label) == 300, label
    assert interval("photos-upload") == 1800
    for label in ("slack-auth", "chatgpt-auth", "claude-desktop-auth"):
        assert interval(label) == 3600, label


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
