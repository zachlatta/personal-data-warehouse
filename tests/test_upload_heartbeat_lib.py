"""Tests for bin/_pdw-upload-lib.sh.

The upload wrappers (bin/*-upload-launchd, bin/*-upload-systemd) and their status
helpers share this sourced POSIX-sh library so that heartbeats reflect real run
health instead of merely "the job fired". These tests drive the shell functions
directly via /bin/sh so the health logic is covered regardless of which wrapper
sources it.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

LIB = Path(__file__).resolve().parent.parent / "bin" / "_pdw-upload-lib.sh"


def _run(snippet: str, *, env: dict[str, str] | None = None) -> str:
    """Source the lib and run a shell snippet, returning combined stdout."""
    script = f'. "{LIB}"\n{snippet}\n'
    full_env = {**os.environ, **(env or {})}
    result = subprocess.run(
        ["/bin/sh", "-c", script],
        capture_output=True,
        text=True,
        env=full_env,
        check=True,
    )
    return result.stdout


def test_success_file_path_derivation():
    out = _run('pdw_success_file "/tmp/foo/bar-upload.heartbeat"')
    assert out.strip() == "/tmp/foo/bar-upload.last-success"
    # Without the .heartbeat suffix, append rather than mangle.
    out = _run('pdw_success_file "/tmp/foo/bar"')
    assert out.strip() == "/tmp/foo/bar.last-success"


def test_record_run_success_writes_heartbeat_and_success(tmp_path: Path):
    hb = tmp_path / "x-upload.heartbeat"
    _run(f'pdw_record_run "{hb}" "2026-06-28T09:00:00-04:00" 0')
    assert hb.read_text().strip() == "2026-06-28T09:00:00-04:00 exit_code=0"
    success = tmp_path / "x-upload.last-success"
    assert success.read_text().strip() == "2026-06-28T09:00:00-04:00"


def test_record_run_failure_does_not_touch_success(tmp_path: Path):
    hb = tmp_path / "x-upload.heartbeat"
    success = tmp_path / "x-upload.last-success"
    # Seed an earlier success, then record a failure: heartbeat advances but the
    # success marker must stay pinned at the last good run.
    _run(f'pdw_record_run "{hb}" "2026-06-25T02:00:00-04:00" 0')
    _run(f'pdw_record_run "{hb}" "2026-06-28T09:05:00-04:00" 1')
    assert hb.read_text().strip() == "2026-06-28T09:05:00-04:00 exit_code=1"
    assert success.read_text().strip() == "2026-06-25T02:00:00-04:00"


def test_health_ok_for_fresh_success(tmp_path: Path):
    hb = tmp_path / "x-upload.heartbeat"
    _run(f'pdw_record_run "{hb}" "2026-06-28T09:00:00-04:00" 0')
    out = _run(f'pdw_print_health "{hb}"')
    assert "Health: OK" in out
    assert "last success:" in out
    assert "never" not in out


def test_health_failing_when_last_run_nonzero(tmp_path: Path):
    hb = tmp_path / "x-upload.heartbeat"
    _run(f'pdw_record_run "{hb}" "2026-06-25T02:00:00-04:00" 0')
    _run(f'pdw_record_run "{hb}" "2026-06-28T09:05:00-04:00" 1')
    out = _run(f'pdw_print_health "{hb}"')
    assert "Health: FAILING - last run exited 1" in out
    # The last *successful* run is still surfaced so you can see how long data
    # has actually been stale.
    assert "2026-06-25T02:00:00-04:00" in out


def test_health_stale_when_old_but_successful(tmp_path: Path):
    hb = tmp_path / "x-upload.heartbeat"
    _run(f'pdw_record_run "{hb}" "2026-06-28T09:00:00-04:00" 0')
    # Force the heartbeat mtime far into the past so the staleness gate trips.
    old = 1_000_000_000
    os.utime(hb, (old, old))
    os.utime(tmp_path / "x-upload.last-success", (old, old))
    out = _run(f'pdw_print_health "{hb}" 1800')
    assert "Health: STALE" in out


def test_health_unknown_when_missing(tmp_path: Path):
    hb = tmp_path / "missing-upload.heartbeat"
    out = _run(f'pdw_print_health "{hb}"')
    assert "Health: UNKNOWN" in out
    assert "no heartbeat" in out


def test_health_unknown_for_legacy_bare_timestamp(tmp_path: Path):
    # A heartbeat written by the pre-exit-code wrappers is just an ISO line.
    hb = tmp_path / "x-upload.heartbeat"
    hb.write_text("2026-06-28T09:00:00-04:00\n")
    out = _run(f'pdw_print_health "{hb}"')
    assert "Health: UNKNOWN" in out
    assert "legacy heartbeat" in out


def test_health_reports_never_when_no_success_yet(tmp_path: Path):
    hb = tmp_path / "x-upload.heartbeat"
    _run(f'pdw_record_run "{hb}" "2026-06-28T09:05:00-04:00" 1')
    out = _run(f'pdw_print_health "{hb}"')
    assert "Health: FAILING" in out
    assert "last success: never" in out
