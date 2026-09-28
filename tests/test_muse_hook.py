"""The Muse hook script: runs `pdw ingest muse`, posts a heartbeat, never wakes the model."""

from __future__ import annotations

from pathlib import Path
import subprocess

HOOK = Path(__file__).resolve().parents[1] / "ops" / "muse" / "pdw-ingest-hook.sh"

FAKE_RUNTIME = """
silent() { printf 'HATCH_HOOK_RESULT:silent:%s\\n' "${1-}"; exit 0; }
wake() { printf 'HATCH_HOOK_RESULT:wake:%s\\n' "${1-}"; exit 0; }
"""


def _home(tmp_path: Path, *, ingest_rc: int) -> tuple[Path, Path]:
    home = tmp_path / "hatch"
    (home / ".local" / "bin").mkdir(parents=True)
    (home / ".config" / "pdw").mkdir(parents=True)
    (home / ".config" / "pdw" / "muse.env").write_text("MUSE_ACCOUNT=z@example.test\n")
    calls = tmp_path / "calls.log"
    pdw = home / ".local" / "bin" / "pdw"
    pdw.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "%s|MUSE_ACCOUNT=%s|NO_UPDATE=%s|HOME=%s\\n" "$*" "${{MUSE_ACCOUNT:-}}" "${{PDW_NO_AUTO_UPDATE:-}}" "$HOME" >> {calls}\n'
        'if [ "$1" = ingest ]; then echo "Muse upload complete: transcripts=1"; '
        f'[ {ingest_rc} -ne 0 ] && echo "pdw ingest muse: boom" >&2; exit {ingest_rc}; fi\n'
        "exit 0\n"
    )
    pdw.chmod(0o755)
    runtime = tmp_path / "runtime.sh"
    runtime.write_text(FAKE_RUNTIME)
    return home, runtime


def _run(home: Path, runtime: Path, **env: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(HOOK)],
        env={"PATH": "/usr/bin:/bin", "MUSE_HOME": str(home), "HATCH_HOOK_RUNTIME": str(runtime), **env},
        capture_output=True,
        text=True,
        check=False,
    )


def test_hook_ingests_posts_a_heartbeat_and_stays_silent(tmp_path: Path) -> None:
    home, runtime = _home(tmp_path, ingest_rc=0)
    result = _run(home, runtime)
    assert result.returncode == 0
    assert result.stdout.startswith("HATCH_HOOK_RESULT:silent:pdw ingest muse exited 0")
    calls = (tmp_path / "calls.log").read_text().splitlines()
    assert calls[0].startswith(f"ingest muse --home {home}|MUSE_ACCOUNT=z@example.test|NO_UPDATE=1|HOME={home}")
    assert calls[1].startswith("heartbeat --pipeline muse --device muse --exit-code 0 ")
    assert "Muse upload complete" in (home / ".local" / "state" / "pdw" / "muse-upload.run.log").read_text()


def test_a_failed_ingest_is_reported_in_the_heartbeat_not_by_waking_the_model(tmp_path: Path) -> None:
    home, runtime = _home(tmp_path, ingest_rc=1)
    result = _run(home, runtime)
    assert "HATCH_HOOK_RESULT:silent:pdw ingest muse exited 1" in result.stdout
    heartbeat = (tmp_path / "calls.log").read_text().splitlines()[1]
    assert "--exit-code 1 " in heartbeat
    assert "--error pdw ingest muse: boom" in heartbeat


def test_dry_run_checks_the_binary_without_uploading(tmp_path: Path) -> None:
    home, runtime = _home(tmp_path, ingest_rc=0)
    result = _run(home, runtime, HATCH_HOOK_DRY_RUN="1")
    assert "HATCH_HOOK_RESULT:silent:dry run" in result.stdout
    calls = (tmp_path / "calls.log").read_text()
    assert "ingest muse --help" in calls
    assert "heartbeat" not in calls
