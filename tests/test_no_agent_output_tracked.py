"""This repository is public: agent work items (reports, audits, query output) never belong in it.

Reports and analyses go to a private repository. Anything committed here is published, so Git must
not track the folders agents keep their output in.
"""

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIRS = ("work/", "scratch/", ".factory-scratch/", ".factory-evidence/")


def test_no_agent_output_is_tracked():
    tracked = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split("\0")
    offenders = [path for path in tracked if path.startswith(OUTPUT_DIRS)]
    assert not offenders, (
        "Agent output is tracked in this public repository; deliver it privately and remove it: "
        f"{offenders[:20]}"
    )
