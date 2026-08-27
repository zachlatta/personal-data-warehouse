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


def test_post_heartbeat_invokes_the_python_module_with_the_run_verdict(tmp_path: Path):
    """The wrapper ships the exit code it observed; the fake uv records the argv."""
    fake_uv = tmp_path / "uv"
    fake_uv.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$FAKE_UV_LOG"\n')
    fake_uv.chmod(0o755)
    log = tmp_path / "uv.log"
    out = _run(
        'pdw_post_heartbeat "claude_code,codex" "2026-08-27T03:00:00-04:00" 1 42; echo "rc=$?"',
        env={"PDW_UV": str(fake_uv), "PDW_REPO_DIR": str(tmp_path), "FAKE_UV_LOG": str(log)},
    )
    assert out.strip().endswith("rc=0")
    argv = log.read_text().split("\n")
    assert argv[:4] == ["run", "--directory", str(tmp_path), "python"]
    assert "personal_data_warehouse.uploader_heartbeat" in argv
    assert argv[argv.index("--pipeline") + 1] == "claude_code,codex"
    assert argv[argv.index("--exit-code") + 1] == "1"
    assert argv[argv.index("--duration-seconds") + 1] == "42"
    assert argv[argv.index("--ran-at") + 1] == "2026-08-27T03:00:00-04:00"


def test_post_heartbeat_never_changes_the_wrappers_exit_code(tmp_path: Path):
    fake_uv = tmp_path / "uv"
    fake_uv.write_text("#!/bin/sh\nexit 7\n")
    fake_uv.chmod(0o755)
    out = _run(
        'pdw_post_heartbeat "apple_notes" "2026-08-27T03:00:00-04:00" 0 1 2>/dev/null; echo "rc=$?"',
        env={"PDW_UV": str(fake_uv), "PDW_REPO_DIR": str(tmp_path)},
    )
    assert out.strip() == "rc=0"


def test_post_heartbeat_is_a_noop_without_a_repo_and_uv():
    out = _run('pdw_post_heartbeat "apple_notes" "2026-08-27T03:00:00-04:00" 0 1; echo "rc=$?"')
    assert out.strip() == "rc=0"


# --- credential resolution ----------------------------------------------------
#
# The heartbeat post runs `uv run python -m ...` DIRECTLY, outside the pdw CLI,
# so it inherits none of the URL/token pdw resolves for `pdw ingest`. The two
# agent-sessions wrappers run their uploader THROUGH pdw and so never exported
# those variables themselves -- and their heartbeat therefore failed on every
# run from the day it shipped, leaving claude_code, codex, pi and openclaw with
# last_run_at NULL on /pipelines while the uploads themselves worked fine. The
# five Apple wrappers only escaped because each had hand-rolled the same config
# read for its own uploader. Resolving credentials here, once, is what makes the
# heartbeat independent of how a given wrapper chose to invoke its uploader.


def _pdw_config(tmp_path: Path, url: str = "https://warehouse.example", token: str = "s3cret") -> Path:
    config = tmp_path / "config.json"
    config.write_text(f'{{"base_url": "{url}", "token": "{token}", "client_name": "test"}}')
    return config


def _argv_env(tmp_path: Path, name: str = "uv") -> tuple[Path, Path]:
    """A fake uv that records the PDW_API_URL/PDW_SECRET_TOKEN it was handed."""
    fake_uv = tmp_path / name
    fake_uv.write_text('#!/bin/sh\nprintf "%s|%s\\n" "${PDW_API_URL-unset}" "${PDW_SECRET_TOKEN-unset}" > "$FAKE_UV_LOG"\n')
    fake_uv.chmod(0o755)
    return fake_uv, tmp_path / "uv.log"


def test_post_heartbeat_reaches_the_app_when_only_pdw_login_is_configured(tmp_path: Path):
    """The regression: `pdw login` alone must be enough, as it is for the uploader."""
    config = _pdw_config(tmp_path)
    fake_uv, log = _argv_env(tmp_path)
    _run(
        'pdw_post_heartbeat "claude_code,codex,pi" "2026-08-27T03:00:00-04:00" 0 1',
        env={
            "PDW_UV": str(fake_uv),
            "PDW_REPO_DIR": str(tmp_path),
            "FAKE_UV_LOG": str(log),
            "PDW_CONFIG": str(config),
            "PDW_API_URL": "",
            "PDW_SECRET_TOKEN": "",
        },
    )
    assert log.read_text().strip() == "https://warehouse.example|s3cret"


def test_export_app_credentials_never_overrides_an_explicit_environment(tmp_path: Path):
    """A wrapper or an operator that set the vars deliberately always wins."""
    config = _pdw_config(tmp_path)
    out = _run(
        'pdw_export_app_credentials; printf "%s|%s\\n" "$PDW_API_URL" "$PDW_SECRET_TOKEN"',
        env={
            "PDW_CONFIG": str(config),
            "PDW_API_URL": "https://direct.example",
            "PDW_SECRET_TOKEN": "explicit",
        },
    )
    assert out.strip() == "https://direct.example|explicit"


def test_export_app_credentials_is_a_noop_without_a_readable_config(tmp_path: Path):
    """No config is the openclaw/CI case: skip quietly, never fail the run."""
    out = _run(
        'pdw_export_app_credentials; echo "rc=$? url=${PDW_API_URL-unset}"',
        env={"PDW_CONFIG": str(tmp_path / "absent.json"), "PDW_API_URL": ""},
    )
    assert out.strip() == "rc=0 url="


def test_no_upload_wrapper_hand_rolls_the_pdw_config_read():
    """One implementation, in the lib -- the duplication is what let this rot.

    Five wrappers each carried their own copy of the config read and the two
    that did not were exactly the two whose heartbeat was broken. A new wrapper
    must inherit the behaviour by sourcing the lib, not by copying the snippet.
    """
    bin_dir = LIB.parent
    offenders = sorted(
        path.name
        for path in bin_dir.iterdir()
        if path.is_file() and path.name != LIB.name and "json.load(open(sys.argv[1]))" in path.read_text(errors="ignore")
    )
    assert offenders == [], f"these wrappers duplicate pdw_export_app_credentials: {offenders}"


def test_every_heartbeat_posting_wrapper_can_resolve_credentials():
    """Sourcing the lib is the contract; calling the poster without it is a bug."""
    bin_dir = LIB.parent
    missing = []
    for path in sorted(bin_dir.iterdir()):
        if not path.is_file() or path.name == LIB.name:
            continue
        text = path.read_text(errors="ignore")
        if "pdw_post_heartbeat" not in text:
            continue
        if "_pdw-upload-lib.sh" not in text:
            missing.append(path.name)
    assert missing == [], f"these wrappers post a heartbeat without sourcing the lib: {missing}"
