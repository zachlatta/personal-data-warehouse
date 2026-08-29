from __future__ import annotations

import plistlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_apple_notes_mutation_worker_is_a_resident_launch_agent() -> None:
    plist_path = ROOT / "ops/launchd/com.zachlatta.personal-data-warehouse.apple-notes-mutation-worker.plist"
    with plist_path.open("rb") as handle:
        plist = plistlib.load(handle)

    assert plist["Label"] == "com.zachlatta.personal-data-warehouse.apple-notes-mutation-worker"
    assert plist["RunAtLoad"] is True
    assert plist["KeepAlive"] is True
    assert "StartInterval" not in plist
    assert plist["ProgramArguments"] == [
        "/Users/zrl/dev/zachlatta/personal-data-warehouse/bin/apple-notes-mutation-worker-launchd"
    ]
