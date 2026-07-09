"""Discover local AI agent CLI session transcript files.

Claude Code, Codex, OpenClaw, and pi each write append-only JSONL transcripts, one
file per session, under a per-tool directory tree. The session id is encoded in
the filename so we can attribute every line to a session without parsing the
file.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

CLAUDE_CODE_TOOL = "claude_code"
CODEX_TOOL = "codex"
OPENCLAW_TOOL = "openclaw"
PI_TOOL = "pi"

# OpenClaw writes several sidecar files next to each "<sessionId>.jsonl"
# transcript: a lower-level "<sessionId>.trajectory.jsonl" runtime trace plus
# "<sessionId>.*.json" metadata. Only the bare "<sessionId>.jsonl" is the
# conversational transcript; the trajectory file also ends in ".jsonl", so it
# has to be excluded explicitly (the ".json" sidecars are filtered by globbing
# for ".jsonl").
_OPENCLAW_SIDECAR_SUFFIX = ".trajectory.jsonl"

_UUID_RE = re.compile(
    r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
)


@dataclass(frozen=True)
class SessionFile:
    tool: str
    session_id: str
    path: Path


def discover_session_files(
    *,
    claude_projects_dir: Path | str | None,
    codex_sessions_dir: Path | str | None,
    openclaw_sessions_dir: Path | str | None = None,
    pi_sessions_dir: Path | str | None = None,
) -> list[SessionFile]:
    files: list[SessionFile] = []
    files.extend(_discover_claude_code(claude_projects_dir))
    files.extend(_discover_codex(codex_sessions_dir))
    files.extend(_discover_openclaw(openclaw_sessions_dir))
    files.extend(_discover_pi(pi_sessions_dir))
    # Stable order keeps batching deterministic across runs.
    files.sort(key=lambda f: (f.tool, str(f.path)))
    return files


def _discover_claude_code(root: Path | str | None) -> list[SessionFile]:
    if not root:
        return []
    base = Path(root).expanduser()
    if not base.is_dir():
        return []
    files: list[SessionFile] = []
    for path in base.rglob("*.jsonl"):
        if not path.is_file():
            continue
        files.append(SessionFile(tool=CLAUDE_CODE_TOOL, session_id=claude_session_id(path), path=path))
    return files


def _discover_codex(root: Path | str | None) -> list[SessionFile]:
    if not root:
        return []
    base = Path(root).expanduser()
    if not base.is_dir():
        return []
    files: list[SessionFile] = []
    for path in base.rglob("rollout-*.jsonl"):
        if not path.is_file():
            continue
        files.append(SessionFile(tool=CODEX_TOOL, session_id=codex_session_id(path), path=path))
    return files


def _discover_openclaw(root: Path | str | None) -> list[SessionFile]:
    if not root:
        return []
    base = Path(root).expanduser()
    if not base.is_dir():
        return []
    files: list[SessionFile] = []
    for path in base.rglob("*.jsonl"):
        if not path.is_file():
            continue
        if path.name.endswith(_OPENCLAW_SIDECAR_SUFFIX):
            continue
        files.append(SessionFile(tool=OPENCLAW_TOOL, session_id=openclaw_session_id(path), path=path))
    return files


def _discover_pi(root: Path | str | None) -> list[SessionFile]:
    if not root:
        return []
    base = Path(root).expanduser()
    if not base.is_dir():
        return []
    return [
        SessionFile(tool=PI_TOOL, session_id=pi_session_id(path), path=path)
        for path in base.rglob("*.jsonl")
        if path.is_file()
    ]


def claude_session_id(path: Path) -> str:
    # Claude Code names each transcript "<sessionId>.jsonl".
    return path.name[: -len(".jsonl")] if path.name.endswith(".jsonl") else path.stem


def codex_session_id(path: Path) -> str:
    # Codex names each rollout "rollout-<ts>-<uuid>.jsonl"; the trailing UUID is
    # the session id.
    matches = _UUID_RE.findall(path.name)
    if matches:
        return matches[-1].lower()
    stem = path.name[: -len(".jsonl")] if path.name.endswith(".jsonl") else path.stem
    return stem[len("rollout-"):] if stem.startswith("rollout-") else stem


def openclaw_session_id(path: Path) -> str:
    # OpenClaw names each transcript "<sessionId>.jsonl".
    return path.name[: -len(".jsonl")] if path.name.endswith(".jsonl") else path.stem


def pi_session_id(path: Path) -> str:
    # pi names transcripts "<timestamp>_<sessionId>.jsonl".
    matches = _UUID_RE.findall(path.name)
    if matches:
        return matches[-1].lower()
    stem = path.name[: -len(".jsonl")] if path.name.endswith(".jsonl") else path.stem
    return stem.rsplit("_", 1)[-1]
