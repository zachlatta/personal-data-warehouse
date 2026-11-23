"""`uv run pytest` runs the Go suite too.

src/personal_data_warehouse/warehouse_catalog.json generates into app/, so a
catalog edit looks purely Python and is checked by Go tests it never ran:
TestOverviewGuidanceStaysWithinBudget reached main red behind a green
`uv run pytest` on 869103b and again on 5fef623's parent.
"""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

APP = Path(__file__).resolve().parents[1] / "app"


def test_the_go_suite_passes() -> None:
    go = shutil.which("go")
    assert go, "Go is not on PATH; install the version app/go.mod names, since uv run pytest also runs go test ./..."
    env = {key: value for key, value in os.environ.items() if not (key.startswith("PDW_") and "TEST" in key)}
    result = subprocess.run(
        [go, "test", "./..."], cwd=APP, env=env, capture_output=True, text=True, timeout=900, check=False
    )
    failures = [line for line in result.stdout.splitlines() if line.startswith(("--- FAIL", "FAIL", "panic:"))]
    assert result.returncode == 0, "\n".join(failures or [result.stdout[-4000:], result.stderr[-4000:]])
