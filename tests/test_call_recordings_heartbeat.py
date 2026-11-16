"""Run the actual phone wrapper with a fake pdw, never a phone or live API."""
import os
from pathlib import Path
import shutil
import subprocess

import pytest

BIN = Path(__file__).resolve().parents[1] / "bin"


@pytest.mark.parametrize("inventory", ["2026-11-16T12:00:00Z", ""])
@pytest.mark.parametrize("exit_code", [0, 1])
def test_wrapper_posts_one_current_heartbeat_with_resolved_device_and_inventory(tmp_path, inventory, exit_code):
    for name in ("call-recordings-upload-android", "_pdw-upload-lib.sh"):
        shutil.copyfile(BIN / name, tmp_path / name)
    fake = tmp_path / "pdw"
    fake.write_text('''#!/bin/sh
case "$1" in
  ingest)
    echo "heartbeat_device=example-phone"
    echo "newest_finished_at=$TEST_INVENTORY"
    echo "upload complete: seen=0 audio=0 sidecars=0 unsettled=0 remaining=0"
    exit "$TEST_EXIT_CODE"
    ;;
  heartbeat)
    printf '%s\\n' "$@" >> "$TEST_CALLS"
    ;;
esac
''')
    fake.chmod(0o755)
    calls = tmp_path / "calls"
    env = {**os.environ, "PDW_ANDROID_CALL_RECORDINGS_STATE_DIR": str(tmp_path / "state"),
           "PDW_BIN": str(fake), "TEST_CALLS": str(calls), "TEST_INVENTORY": inventory,
           "TEST_EXIT_CODE": str(exit_code)}
    for current_inventory in (inventory, ""):
        env["TEST_INVENTORY"] = current_inventory
        result = subprocess.run(["/bin/sh", str(tmp_path / "call-recordings-upload-android")],
                                env=env, capture_output=True, text=True)
        assert result.returncode == exit_code, result.stderr
    args = calls.read_text().splitlines()
    assert args.count("heartbeat") == 2, "each run, including empty runs, must post exactly once"
    assert args.count("example-phone") == 2
    assert "pichu" not in args
    assert args.count("--newest-finished-at") == 2
    assert args[args.index("--newest-finished-at") + 1] == inventory
    last_inventory = len(args) - 1 - args[::-1].index("--newest-finished-at")
    assert args[last_inventory + 1] == "", "the post-reaping run must clear the earlier inventory"
    assert args.count("--ran-at") == 2
