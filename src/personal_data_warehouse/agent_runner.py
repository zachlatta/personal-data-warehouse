from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import uuid
from typing import Any

from dagster import ConfigurableResource


DEFAULT_AGENT_AUTH_VOLUME = "pdw-agent-auth"
DEFAULT_AGENT_RUNS_VOLUME = "pdw-agent-runs"
DEFAULT_AGENT_RUNS_DIR = "/agent-runs"
DEFAULT_AGENT_CONTAINER_AUTH_DIR = "/agent-auth"
DEFAULT_AGENT_CONTAINER_RUNS_DIR = "/agent-runs"
DEFAULT_AGENT_TOOLS_DIR_NAME = "tools"
DEFAULT_AGENT_TOOL_MANIFEST_NAME = "TOOLS.md"
DEFAULT_AGENT_MEMORY = "4g"
DEFAULT_AGENT_CPUS = "2"
DEFAULT_AGENT_PIDS_LIMIT = 512
DEFAULT_AGENT_NETWORK = "bridge"


@dataclass(frozen=True)
class AgentContainerConfig:
    image: str
    provider: str = "codex"
    model: str = ""
    auth_volume: str = DEFAULT_AGENT_AUTH_VOLUME
    runs_volume: str = DEFAULT_AGENT_RUNS_VOLUME
    runs_dir: Path = Path(DEFAULT_AGENT_RUNS_DIR)
    network: str = DEFAULT_AGENT_NETWORK
    memory: str = DEFAULT_AGENT_MEMORY
    cpus: str = DEFAULT_AGENT_CPUS
    pids_limit: int = DEFAULT_AGENT_PIDS_LIMIT
    timeout_seconds: int = 1800

    @property
    def normalized_provider(self) -> str:
        provider = self.provider.strip().lower()
        if provider not in {"codex", "claude"}:
            raise ValueError("AGENT_PROVIDER must be codex or claude")
        return provider


@dataclass(frozen=True)
class AgentRunRequest:
    prompt: str
    schema: Mapping[str, Any]
    task_type: str = "generic"
    subject_id: str = ""
    run_id: str = field(default_factory=lambda: f"agent-{uuid.uuid4().hex}")
    provider: str | None = None
    model: str | None = None

    @property
    def input_sha256(self) -> str:
        payload = json.dumps(
            {
                "prompt": self.prompt,
                "schema": self.schema,
                "task_type": self.task_type,
                "subject_id": self.subject_id,
                "provider": self.provider,
                "model": self.model,
            },
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AgentRunEvent:
    event_index: int
    stream: str
    event_type: str
    event_json: Mapping[str, Any]
    text: str
    created_at: datetime


@dataclass(frozen=True)
class AgentRunResult:
    run_id: str
    provider: str
    model: str
    task_type: str
    subject_id: str
    input_sha256: str
    status: str
    final_output_json: Mapping[str, Any]
    error: str
    exit_code: int
    started_at: datetime
    completed_at: datetime
    events: Sequence[AgentRunEvent]


class ContainerAgentRunner:
    def __init__(self, config: AgentContainerConfig, *, runner=subprocess.run) -> None:
        self._config = config
        self._runner = runner

    def run(self, request: AgentRunRequest) -> AgentRunResult:
        provider = (request.provider or self._config.normalized_provider).strip().lower()
        if provider not in {"codex", "claude"}:
            raise ValueError("agent provider must be codex or claude")
        model = request.model if request.model is not None else self._config.model
        run_dir = self._prepare_run_dir(request)
        self._sync_run_dir_to_volume(request.run_id, run_dir)
        command = self.docker_command(request=request, provider=provider, model=model)
        started_at = datetime.now(tz=UTC)
        events: list[AgentRunEvent] = []
        error = ""
        exit_code = 0
        with provider_auth_lock(provider):
            try:
                completed = self._runner(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=self._config.timeout_seconds,
                    check=False,
                )
                exit_code = int(getattr(completed, "returncode", 1))
                completed_at = datetime.now(tz=UTC)
                events.extend(
                    agent_events_from_streams(
                        stdout=completed.stdout or "",
                        stderr=completed.stderr or "",
                        created_at=completed_at,
                    )
                )
            except subprocess.TimeoutExpired as exc:
                completed_at = datetime.now(tz=UTC)
                exit_code = 124
                error = f"agent container timed out after {self._config.timeout_seconds}s"
                stdout = exc.stdout if isinstance(exc.stdout, str) else ""
                stderr = exc.stderr if isinstance(exc.stderr, str) else ""
                events.extend(agent_events_from_streams(stdout=stdout, stderr=stderr, created_at=completed_at))

        self._sync_run_dir_from_volume(request.run_id, run_dir)
        final_output, final_error = load_agent_final_output(run_dir=run_dir, provider=provider, events=events)
        if final_error and not error:
            error = final_error
        if exit_code != 0 and not error:
            error = f"agent container exited with code {exit_code}"
        status = "completed" if exit_code == 0 and not error else "error"
        return AgentRunResult(
            run_id=request.run_id,
            provider=provider,
            model=model,
            task_type=request.task_type,
            subject_id=request.subject_id,
            input_sha256=request.input_sha256,
            status=status,
            final_output_json=final_output,
            error=error,
            exit_code=exit_code,
            started_at=started_at,
            completed_at=completed_at,
            events=events,
        )

    def _sync_run_dir_to_volume(self, run_id: str, run_dir: Path) -> None:
        self._runner(
            volume_copy_command(
                volume=self._config.runs_volume,
                run_id=run_id,
                direction="to_volume",
                host_dir=run_dir,
            ),
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )

    def _sync_run_dir_from_volume(self, run_id: str, run_dir: Path) -> None:
        self._runner(
            volume_copy_command(
                volume=self._config.runs_volume,
                run_id=run_id,
                direction="from_volume",
                host_dir=run_dir,
            ),
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )

    def _prepare_run_dir(self, request: AgentRunRequest) -> Path:
        run_dir = self._config.runs_dir / request.run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "prompt.txt").write_text(request.prompt, encoding="utf-8")
        (run_dir / "schema.json").write_text(json.dumps(request.schema, sort_keys=True), encoding="utf-8")
        (run_dir / "request.json").write_text(
            json.dumps(
                {
                    "run_id": request.run_id,
                    "task_type": request.task_type,
                    "subject_id": request.subject_id,
                    "input_sha256": request.input_sha256,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        write_builtin_cli_tools(run_dir)
        return run_dir

    def docker_command(self, *, request: AgentRunRequest, provider: str, model: str) -> list[str]:
        name = docker_safe_name(f"pdw-agent-{request.run_id}")
        container_run_dir = f"{DEFAULT_AGENT_CONTAINER_RUNS_DIR}/{request.run_id}"
        env = [
            "--env",
            f"AGENT_PROVIDER={provider}",
            "--env",
            f"AGENT_MODEL={model}",
            "--env",
            f"AGENT_RUN_ID={request.run_id}",
            "--env",
            f"AGENT_PROMPT_PATH={container_run_dir}/prompt.txt",
            "--env",
            f"AGENT_SCHEMA_PATH={container_run_dir}/schema.json",
            "--env",
            f"AGENT_FINAL_JSON_PATH={container_run_dir}/final.json",
            "--env",
            f"AGENT_FINAL_MESSAGE_PATH={container_run_dir}/final.md",
            "--env",
            f"AGENT_TOOLS_DIR={container_run_dir}/{DEFAULT_AGENT_TOOLS_DIR_NAME}",
            "--env",
            f"AGENT_TOOL_MANIFEST_PATH={container_run_dir}/{DEFAULT_AGENT_TOOL_MANIFEST_NAME}",
            "--env",
            f"CODEX_HOME={DEFAULT_AGENT_CONTAINER_AUTH_DIR}/codex",
            "--env",
            f"CLAUDE_CONFIG_DIR={DEFAULT_AGENT_CONTAINER_AUTH_DIR}/claude",
            "--env",
            "HOME=/tmp/agent-home",
        ]
        return [
            "docker",
            "run",
            "--rm",
            "--name",
            name,
            "--workdir",
            container_run_dir,
            "--network",
            self._config.network,
            "--memory",
            self._config.memory,
            "--cpus",
            self._config.cpus,
            "--pids-limit",
            str(self._config.pids_limit),
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--read-only",
            "--tmpfs",
            "/tmp:rw,nosuid,size=1g",
            "--tmpfs",
            "/tmp/agent-home:rw,nosuid,size=512m",
            "--mount",
            f"type=volume,src={self._config.auth_volume},dst={DEFAULT_AGENT_CONTAINER_AUTH_DIR}",
            "--mount",
            f"type=volume,src={self._config.runs_volume},dst={DEFAULT_AGENT_CONTAINER_RUNS_DIR}",
            *env,
            self._config.image,
        ]


class AgentResource(ConfigurableResource):
    provider: str = "codex"
    model: str = ""
    docker_image: str
    auth_volume: str = DEFAULT_AGENT_AUTH_VOLUME
    runs_volume: str = DEFAULT_AGENT_RUNS_VOLUME
    runs_dir: str = DEFAULT_AGENT_RUNS_DIR
    network: str = DEFAULT_AGENT_NETWORK
    memory: str = DEFAULT_AGENT_MEMORY
    cpus: str = DEFAULT_AGENT_CPUS
    pids_limit: int = DEFAULT_AGENT_PIDS_LIMIT
    timeout_seconds: int = 1800

    def container_config(self) -> AgentContainerConfig:
        return AgentContainerConfig(
            image=self.docker_image,
            provider=self.provider,
            model=self.model,
            auth_volume=self.auth_volume,
            runs_volume=self.runs_volume,
            runs_dir=Path(self.runs_dir),
            network=self.network,
            memory=self.memory,
            cpus=self.cpus,
            pids_limit=self.pids_limit,
            timeout_seconds=self.timeout_seconds,
        )

    def runner(self) -> ContainerAgentRunner:
        return ContainerAgentRunner(self.container_config())

    def run(self, request: AgentRunRequest) -> AgentRunResult:
        effective_request = request
        if request.provider is None or request.model is None:
            effective_request = AgentRunRequest(
                prompt=request.prompt,
                schema=request.schema,
                task_type=request.task_type,
                subject_id=request.subject_id,
                run_id=request.run_id,
                provider=request.provider or self.provider,
                model=request.model if request.model is not None else self.model,
            )
        return self.runner().run(effective_request)


def agent_events_from_streams(*, stdout: str, stderr: str, created_at: datetime) -> list[AgentRunEvent]:
    events: list[AgentRunEvent] = []
    for stream, text in (("stdout", stdout), ("stderr", stderr)):
        for line in text.splitlines():
            index = len(events)
            stripped = line.strip()
            event_type = "text"
            event_json: Mapping[str, Any] = {"text": line}
            if stripped:
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    parsed = None
                if isinstance(parsed, Mapping):
                    event_json = parsed
                    event_type = str(parsed.get("type") or parsed.get("event") or parsed.get("subtype") or "json")
            events.append(
                AgentRunEvent(
                    event_index=index,
                    stream=stream,
                    event_type=event_type,
                    event_json=event_json,
                    text=line,
                    created_at=created_at,
                )
            )
    return events


def agent_run_row(result: AgentRunResult) -> dict[str, Any]:
    return {
        "run_id": result.run_id,
        "provider": result.provider,
        "model": result.model,
        "task_type": result.task_type,
        "subject_id": result.subject_id,
        "status": result.status,
        "input_sha256": result.input_sha256,
        "final_output_json": json.dumps(result.final_output_json, sort_keys=True, separators=(",", ":"), default=str),
        "error": result.error,
        "exit_code": result.exit_code,
        "started_at": result.started_at,
        "completed_at": result.completed_at,
        "sync_version": int(result.completed_at.timestamp() * 1_000_000),
    }


def agent_run_event_rows(result: AgentRunResult) -> list[dict[str, Any]]:
    rows = []
    sync_version = int(result.completed_at.timestamp() * 1_000_000)
    for event in result.events:
        rows.append(
            {
                "run_id": result.run_id,
                "event_index": event.event_index,
                "stream": event.stream,
                "event_type": event.event_type,
                "event_json": json.dumps(event.event_json, sort_keys=True, separators=(",", ":"), default=str),
                "text": event.text,
                "created_at": event.created_at,
                "sync_version": sync_version,
            }
        )
    return rows


def agent_run_tool_call_rows(result: AgentRunResult) -> list[dict[str, Any]]:
    rows = []
    sync_version = int(result.completed_at.timestamp() * 1_000_000)
    for event in result.events:
        payload = event.event_json
        tool_name = extract_tool_name(payload)
        if not tool_name:
            continue
        rows.append(
            {
                "run_id": result.run_id,
                "event_index": event.event_index,
                "tool_name": tool_name,
                "arguments_json": json.dumps(extract_tool_arguments(payload), sort_keys=True, separators=(",", ":"), default=str),
                "result_json": json.dumps(extract_tool_result(payload), sort_keys=True, separators=(",", ":"), default=str),
                "error": str(payload.get("error") or ""),
                "started_at": event.created_at,
                "completed_at": event.created_at,
                "sync_version": sync_version,
            }
        )
    return rows


def extract_tool_name(payload: Mapping[str, Any]) -> str:
    for key in ("tool_name", "tool", "name"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            event_type = str(payload.get("type") or payload.get("event") or "")
            if "tool" in event_type.lower() or "mcp" in event_type.lower() or key in {"tool_name", "tool"}:
                return value
    mcp = payload.get("mcp")
    if isinstance(mcp, Mapping):
        value = mcp.get("tool") or mcp.get("name")
        if isinstance(value, str):
            return value
    return ""


def extract_tool_arguments(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    value = payload.get("arguments") or payload.get("args") or payload.get("input")
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {"value": value}
        if isinstance(parsed, Mapping):
            return parsed
    return {}


def extract_tool_result(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    value = payload.get("result") or payload.get("output")
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        return {"value": value}
    return {}


def load_agent_final_output(
    *,
    run_dir: Path,
    provider: str,
    events: Sequence[AgentRunEvent],
) -> tuple[Mapping[str, Any], str]:
    final_json_path = run_dir / "final.json"
    if final_json_path.exists():
        return load_json_mapping(final_json_path.read_text(encoding="utf-8"))

    final_message_path = run_dir / "final.md"
    if final_message_path.exists():
        return load_json_mapping(strip_markdown_code_fence(final_message_path.read_text(encoding="utf-8")))

    if provider == "claude":
        extracted = extract_final_text_from_events(events)
        if extracted:
            return load_json_mapping(strip_markdown_code_fence(extracted))
    return {}, "agent did not produce parseable final JSON"


def load_json_mapping(value: str) -> tuple[Mapping[str, Any], str]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        return {}, f"agent final output was not valid JSON: {exc}"
    if not isinstance(parsed, Mapping):
        return {}, "agent final output JSON must be an object"
    return parsed, ""


def extract_final_text_from_events(events: Sequence[AgentRunEvent]) -> str:
    for event in reversed(events):
        payload = event.event_json
        for key in ("result", "final", "output", "message", "content", "text"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value
        content = payload.get("content")
        if isinstance(content, Sequence) and not isinstance(content, (str, bytes, bytearray)):
            text_parts = []
            for item in content:
                if isinstance(item, Mapping) and isinstance(item.get("text"), str):
                    text_parts.append(item["text"])
            if text_parts:
                return "\n".join(text_parts)
    return ""


def strip_markdown_code_fence(value: str) -> str:
    stripped = value.strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    if len(lines) >= 2 and lines[-1].strip() == "```":
        return "\n".join(lines[1:-1]).strip()
    return stripped


def docker_safe_name(value: str) -> str:
    cleaned = "".join(character if character.isalnum() or character in {"-", "_", "."} else "-" for character in value)
    return cleaned[:120].strip("-_.") or "pdw-agent"


def volume_copy_command(*, volume: str, run_id: str, direction: str, host_dir: Path) -> list[str]:
    run_name = docker_safe_name(run_id)
    host_mount = str(host_dir.resolve())
    if direction == "to_volume":
        shell_command = (
            f"rm -rf /volume/{run_name}"
            f" && mkdir -p /volume/{run_name}"
            f" && cp -a /host/. /volume/{run_name}/"
        )
    elif direction == "from_volume":
        shell_command = f"if [ -d /volume/{run_name} ]; then cp -a /volume/{run_name}/. /host/; fi"
    else:
        raise ValueError("direction must be to_volume or from_volume")
    return [
        "docker",
        "run",
        "--rm",
        "--mount",
        f"type=volume,src={volume},dst=/volume",
        "--mount",
        f"type=bind,src={host_mount},dst=/host",
        "alpine:3.20",
        "sh",
        "-lc",
        shell_command,
    ]


def write_builtin_cli_tools(run_dir: Path) -> None:
    tools_dir = run_dir / DEFAULT_AGENT_TOOLS_DIR_NAME
    tools_dir.mkdir(parents=True, exist_ok=True)
    validate_tool = tools_dir / "pdw-validate-json"
    validate_tool.write_text(PDW_VALIDATE_JSON_SCRIPT, encoding="utf-8")
    validate_tool.chmod(0o755)

    help_tool = tools_dir / "pdw-tool-help"
    help_tool.write_text(PDW_TOOL_HELP_SCRIPT, encoding="utf-8")
    help_tool.chmod(0o755)

    (run_dir / DEFAULT_AGENT_TOOL_MANIFEST_NAME).write_text(cli_tool_manifest(), encoding="utf-8")


def cli_tool_manifest() -> str:
    return """# Agent CLI Tools

These commands are available on PATH inside the agent container. They are local deterministic helpers and do not have production credentials.

## pdw-tool-help

Print this tool manifest.

## pdw-validate-json

Validate a JSON file against a simple JSON object schema.

Usage:

```bash
pdw-validate-json candidate.json schema.json
```

Supported schema checks: object root, required keys, additionalProperties=false, and primitive `string`, `number`, `integer`, `boolean`, `array`, and `object` property types.
"""


PDW_TOOL_HELP_SCRIPT = """#!/usr/bin/env sh
set -eu
manifest="${AGENT_TOOL_MANIFEST_PATH:-./TOOLS.md}"
if [ -f "$manifest" ]; then
  cat "$manifest"
else
  echo "No agent tool manifest found at $manifest" >&2
  exit 1
fi
"""


PDW_VALIDATE_JSON_SCRIPT = r"""#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: pdw-validate-json CANDIDATE_JSON SCHEMA_JSON", file=sys.stderr)
        return 2
    candidate_path = Path(argv[1])
    schema_path = Path(argv[2])
    try:
        candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"invalid JSON input: {exc}", file=sys.stderr)
        return 1
    issues = validate(candidate, schema)
    if issues:
        for issue in issues:
            print(issue, file=sys.stderr)
        return 1
    print("ok")
    return 0


def validate(candidate: Any, schema: Any) -> list[str]:
    issues: list[str] = []
    if not isinstance(schema, dict):
        return ["schema must be a JSON object"]
    if schema.get("type") == "object" and not isinstance(candidate, dict):
        return ["candidate must be a JSON object"]
    if not isinstance(candidate, dict):
        return []
    required = schema.get("required") or []
    if isinstance(required, list):
        for key in required:
            if isinstance(key, str) and key not in candidate:
                issues.append(f"missing required key: {key}")
    properties = schema.get("properties") or {}
    if isinstance(properties, dict):
        for key, value_schema in properties.items():
            if key in candidate and isinstance(value_schema, dict):
                expected_type = value_schema.get("type")
                if isinstance(expected_type, str) and not matches_type(candidate[key], expected_type):
                    issues.append(f"{key} must be {expected_type}")
    if schema.get("additionalProperties") is False and isinstance(properties, dict):
        extras = sorted(set(candidate) - set(properties))
        for key in extras:
            issues.append(f"unexpected key: {key}")
    return issues


def matches_type(value: Any, expected_type: str) -> bool:
    if expected_type == "string":
        return isinstance(value, str)
    if expected_type == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected_type == "boolean":
        return isinstance(value, bool)
    if expected_type == "array":
        return isinstance(value, list)
    if expected_type == "object":
        return isinstance(value, dict)
    if expected_type == "null":
        return value is None
    return True


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
"""


@contextmanager
def provider_auth_lock(provider: str):
    lock_path = Path(os.getenv("AGENT_AUTH_LOCK_DIR", "/tmp")) / f"pdw-agent-{docker_safe_name(provider)}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = lock_path.open("a+")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()


def agent_config_from_env() -> AgentContainerConfig:
    image = os.getenv("AGENT_DOCKER_IMAGE", "").strip()
    if not image:
        raise ValueError("AGENT_DOCKER_IMAGE must be set")
    return AgentContainerConfig(
        image=image,
        provider=os.getenv("AGENT_PROVIDER", "codex"),
        model=os.getenv("AGENT_MODEL", ""),
        auth_volume=os.getenv("AGENT_AUTH_VOLUME", DEFAULT_AGENT_AUTH_VOLUME),
        runs_volume=os.getenv("AGENT_RUNS_VOLUME", DEFAULT_AGENT_RUNS_VOLUME),
        runs_dir=Path(os.getenv("AGENT_RUNS_DIR", DEFAULT_AGENT_RUNS_DIR)),
        network=os.getenv("AGENT_DOCKER_NETWORK", DEFAULT_AGENT_NETWORK),
        memory=os.getenv("AGENT_DOCKER_MEMORY", DEFAULT_AGENT_MEMORY),
        cpus=os.getenv("AGENT_DOCKER_CPUS", DEFAULT_AGENT_CPUS),
        pids_limit=int(os.getenv("AGENT_DOCKER_PIDS_LIMIT", str(DEFAULT_AGENT_PIDS_LIMIT))),
        timeout_seconds=int(os.getenv("AGENT_TIMEOUT_SECONDS", "1800")),
    )


def auth_docker_command(*, provider: str, action: str, config: AgentContainerConfig, interactive: bool) -> list[str]:
    provider = provider.strip().lower()
    if provider not in {"codex", "claude"}:
        raise ValueError("provider must be codex or claude")
    command = {
        ("codex", "login"): "codex login --device-auth",
        ("codex", "status"): "codex login status",
        ("claude", "login"): "claude",
        ("claude", "status"): "claude doctor",
    }[(provider, action)]
    shell_command = (
        "mkdir -p \"$CODEX_HOME\" \"$CLAUDE_CONFIG_DIR\" \"$HOME\""
        f" && {command}"
    )
    return [
        "docker",
        "run",
        "--rm",
        *(["-it"] if interactive else []),
        "--mount",
        f"type=volume,src={config.auth_volume},dst={DEFAULT_AGENT_CONTAINER_AUTH_DIR}",
        "--env",
        f"CODEX_HOME={DEFAULT_AGENT_CONTAINER_AUTH_DIR}/codex",
        "--env",
        f"CLAUDE_CONFIG_DIR={DEFAULT_AGENT_CONTAINER_AUTH_DIR}/claude",
        "--env",
        "HOME=/tmp/agent-home",
        "--tmpfs",
        "/tmp/agent-home:rw,nosuid,size=512m",
        config.image,
        "sh",
        "-lc",
        shell_command,
    ]


def auth_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Manage subscription-authenticated Codex/Claude agent CLI volumes.")
    parser.add_argument("action", choices=["login", "status"])
    parser.add_argument("provider", choices=["codex", "claude"])
    parser.add_argument("--non-interactive", action="store_true", help="Do not allocate a TTY for the login/status container.")
    args = parser.parse_args(argv)
    config = agent_config_from_env()
    command = auth_docker_command(
        provider=args.provider,
        action=args.action,
        config=config,
        interactive=not args.non_interactive,
    )
    return subprocess.call(command)


if __name__ == "__main__":
    raise SystemExit(auth_main())
