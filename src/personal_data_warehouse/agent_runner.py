from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
import argparse
import base64
from contextlib import contextmanager
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import threading
import time
import uuid
from typing import Any

from personal_data_warehouse.pdw_cli_commands import PDW_CLI_READ_SUBCOMMANDS
from personal_data_warehouse.agent_tool_proxy import (
    DEFAULT_AGENT_TOOL_ALLOWLIST,
    DEFAULT_AGENT_TOOL_PROXY_MAX_ROWS,
    WarehouseAppConfig,
    AgentExclusionCounts,
    run_agent_tool_proxy,
    warehouse_app_config_from_env,
)


logger = logging.getLogger(__name__)


DEFAULT_AGENT_AUTH_VOLUME = "pdw-agent-auth"
DEFAULT_AGENT_RUNS_VOLUME = "pdw-agent-runs"
DEFAULT_AGENT_RUNS_DIR = ".agent-runs"
AGENT_CREDENTIALS_VOLUME_PREFIX = "pdw-agent-credentials-"
DEFAULT_AGENT_CREDENTIALS_VOLUME_SWEEP_MARGIN_SECONDS = 600
_STARTUP_CREDENTIALS_SWEEP_LOCK = threading.Lock()
_STARTUP_CREDENTIALS_SWEEP_DONE = False
DEFAULT_AGENT_CONTAINER_AUTH_DIR = "/agent-auth"
DEFAULT_AGENT_CONTAINER_CREDENTIALS_DIR = "/agent-credentials"
DEFAULT_AGENT_CONTAINER_RUNS_DIR = "/agent-runs"
DEFAULT_AGENT_CONTAINER_CODEX_HOME = "/tmp/agent-codex-home"
DEFAULT_AGENT_CONTAINER_CODEX_SQLITE_HOME = "/tmp/agent-codex-sqlite"
DEFAULT_AGENT_CONTAINER_CLAUDE_CONFIG_DIR = "/tmp/agent-claude-config"
DEFAULT_AGENT_TOOLS_DIR_NAME = "tools"
DEFAULT_AGENT_INPUTS_DIR_NAME = "inputs"
DEFAULT_AGENT_TOOL_MANIFEST_NAME = "TOOLS.md"
DEFAULT_AGENT_MEMORY = "4g"

# AGENT_AUTH_LOCK_MODE governs how agent runs stage and refresh the provider credential:
# "exclusive" (default) keeps the historical one-run-at-a-time behavior;
# "auto" lets runs overlap using disposable credential snapshots while the
# codex access token is comfortably far from expiry, going exclusive within
# the refresh margin so one run can rotate and persist the refresh token.
AGENT_AUTH_LOCK_MODE_ENV = "AGENT_AUTH_LOCK_MODE"
AGENT_AUTH_REFRESH_MARGIN_SECONDS_ENV = "AGENT_AUTH_REFRESH_MARGIN_SECONDS"
DEFAULT_AGENT_AUTH_REFRESH_MARGIN_SECONDS = 90 * 60
_AUTH_EXP_CACHE_TTL_SECONDS = 600
_AUTH_EXP_CACHE: dict[tuple[str, str], tuple[float | None, float]] = {}
_AUTH_EXP_CACHE_LOCK = threading.Lock()
DEFAULT_AGENT_CPUS = "2"
DEFAULT_AGENT_PIDS_LIMIT = 512
DEFAULT_AGENT_NETWORK = "bridge"
DEFAULT_AGENT_MODEL = "gpt-6.1-sol"
DEFAULT_AGENT_REASONING_EFFORT = "medium"
DEFAULT_AGENT_IMAGE_REPOSITORY = "personal-data-warehouse-agent"
DEFAULT_AGENT_DOCKERFILE_PATH = Path(__file__).resolve().parents[2] / "docker" / "agent.Dockerfile"
DEFAULT_AGENT_ENTRYPOINT_PATH = Path(__file__).resolve().parents[2] / "docker" / "agent-entrypoint.sh"
DEFAULT_AGENT_BUILD_CONTEXT_DIR = Path(__file__).resolve().parents[2]
# The image compiles the pdw CLI from this tree, so its sources are image inputs.
DEFAULT_AGENT_CLI_SOURCE_DIR = Path(__file__).resolve().parents[2] / "app"
DEFAULT_AGENT_TOOL_PROXY_BIND_HOST = "0.0.0.0"
DEFAULT_AGENT_TOOL_PROXY_PUBLIC_HOST = "host.docker.internal"
DEFAULT_AGENT_MAX_PROMPT_CHARS = 900_000
MAX_AGENT_RUN_EVENT_JSON_CHARS = 32_000
MAX_AGENT_RUN_EVENT_TEXT_CHARS = 16_000
MAX_AGENT_RUN_ERROR_CHARS = 4_000
_REDACTED = "[REDACTED]"

# These patterns cover recognizable provider credentials and conventional
# secret assignments. Free-form text cannot be proven secret-free, so the
# stronger structured-key guard below runs before these best-effort patterns.
_SECRET_ASSIGNMENT_RE = re.compile(
    r"""
    (?<![a-z0-9])
    (?P<prefix>
        ["']?
        (?:
            (?:[a-z0-9]{1,32}[_-])?api[_-]?key
            | (?:access|refresh|id|bearer|secret)[_-]?token
            | client[_-]?secret
            | session[_-]?key
            | passw(?:or)?d
            | credentials?
            | (?:set[_-]?)?cookie
        )
        ["']?\s*[:=]\s*
    )
    (?P<quote>["']?)
    (?P<bare_secret>[^\s,"';}\]]+)
    (?P=quote)
    """,
    re.IGNORECASE | re.VERBOSE,
)
_AUTHORIZATION_RE = re.compile(
    r"""
    (?P<prefix>
        \bauthorization["']?\s*[:=]\s*["']?
        (?:(?:bearer|basic)\s+)?
    )
    (?P<secret>[^\s,"';}\]]+)
    """,
    re.IGNORECASE | re.VERBOSE,
)
_PROVIDER_TOKEN_RE = re.compile(
    r"(?<![A-Za-z0-9])sk-(?:proj-|ant-)?[A-Za-z0-9_-]{12,}(?![A-Za-z0-9])"
)
_JWT_RE = re.compile(
    r"(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}(?![A-Za-z0-9_-])"
)
_SENSITIVE_EVENT_KEYS = frozenset(
    {
        "access_token",
        "anthropic_api_key",
        "api_key",
        "apikey",
        "authorization",
        "bearer_token",
        "client_secret",
        "cookie",
        "credential",
        "credentials",
        "id_token",
        "mcp_secret_token",
        "openai_api_key",
        "password",
        "passwd",
        "proxy_authorization",
        "refresh_token",
        "secret",
        "secret_token",
        "session_key",
        "sessionkey",
        "set_cookie",
    }
)


@dataclass(frozen=True)
class AgentContainerConfig:
    image: str
    provider: str = "codex"
    model: str = ""
    reasoning_effort: str = DEFAULT_AGENT_REASONING_EFFORT
    auth_volume: str = DEFAULT_AGENT_AUTH_VOLUME
    runs_volume: str = DEFAULT_AGENT_RUNS_VOLUME
    runs_dir: Path = Path(DEFAULT_AGENT_RUNS_DIR)
    network: str = DEFAULT_AGENT_NETWORK
    memory: str = DEFAULT_AGENT_MEMORY
    cpus: str = DEFAULT_AGENT_CPUS
    pids_limit: int = DEFAULT_AGENT_PIDS_LIMIT
    timeout_seconds: int = 1800
    tool_proxy_bind_host: str = "0.0.0.0"
    tool_proxy_public_host: str = "host.docker.internal"
    max_prompt_chars: int = DEFAULT_AGENT_MAX_PROMPT_CHARS

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
    prompt_version: str = ""
    run_id: str = field(default_factory=lambda: f"agent-{uuid.uuid4().hex}")
    provider: str | None = None
    model: str | None = None
    extra_env: Mapping[str, str] = field(default_factory=dict)
    input_files: Mapping[str, str | bytes] = field(default_factory=dict)

    @property
    def input_sha256(self) -> str:
        # Binary input files are folded in as their content hash so the payload
        # stays JSON-serializable; text inputs keep hashing their literal content.
        input_files = {
            name: hashlib.sha256(content).hexdigest() if isinstance(content, bytes) else content
            for name, content in self.input_files.items()
        }
        payload = json.dumps(
            {
                "prompt": self.prompt,
                "schema": self.schema,
                "task_type": self.task_type,
                "subject_id": self.subject_id,
                "prompt_version": self.prompt_version,
                "provider": self.provider,
                "model": self.model,
                "input_files": input_files,
            },
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def with_extra_env(self, env: Mapping[str, str]) -> AgentRunRequest:
        return AgentRunRequest(
            prompt=self.prompt,
            schema=self.schema,
            task_type=self.task_type,
            subject_id=self.subject_id,
            prompt_version=self.prompt_version,
            run_id=self.run_id,
            provider=self.provider,
            model=self.model,
            extra_env={**dict(self.extra_env), **dict(env)},
            input_files=self.input_files,
        )


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
    prompt_version: str
    input_sha256: str
    status: str
    final_output_json: Mapping[str, Any]
    error: str
    exit_code: int
    started_at: datetime
    completed_at: datetime
    events: Sequence[AgentRunEvent]
    exclusion_counts: Mapping[str, int] = field(default_factory=dict)


def agent_prompt_size_error(prompt: str, max_prompt_chars: int) -> str:
    if max_prompt_chars <= 0 or len(prompt) <= max_prompt_chars:
        return ""
    return f"agent prompt exceeds maximum length of {max_prompt_chars} characters: {len(prompt)}"


def safe_agent_input_path(inputs_dir: Path, relative_path: str) -> Path:
    path = Path(relative_path)
    if path.is_absolute() or ".." in path.parts or not str(path).strip():
        raise ValueError(f"agent input file path must be relative and stay inside inputs dir: {relative_path!r}")
    return inputs_dir / path


@contextmanager
def _shield_cleanup_signals():
    """Defer SIGINT and SIGTERM until container/credential cleanup finishes, then re-deliver."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    deferred: tuple[int, Any] | None = None

    def _defer_signal(signum: int, frame: Any) -> None:
        nonlocal deferred
        if deferred is None:
            deferred = (signum, frame)

    previous_handlers: dict[int, Any] = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            previous_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, _defer_signal)
        except (OSError, ValueError):
            continue
    try:
        yield
    finally:
        for sig, handler in previous_handlers.items():
            try:
                signal.signal(sig, handler)
            except (OSError, ValueError):
                continue
        if deferred is not None:
            signum, frame = deferred
            prev_handler = previous_handlers.get(signum)
            if callable(prev_handler):
                prev_handler(signum, frame)
            elif prev_handler == signal.SIG_DFL:
                signal.raise_signal(signum)


class ContainerAgentRunner:
    def __init__(self, config: AgentContainerConfig, *, runner=subprocess.run) -> None:
        self._config = config
        self._runner = runner

    def run(self, request: AgentRunRequest) -> AgentRunResult:
        provider = (request.provider or self._config.normalized_provider).strip().lower()
        if provider not in {"codex", "claude"}:
            raise ValueError("agent provider must be codex or claude")
        configured_model = request.model if request.model is not None else self._config.model
        model = configured_model.strip() or default_agent_model(provider)
        prompt_too_large = agent_prompt_size_error(request.prompt, self._config.max_prompt_chars)
        if prompt_too_large:
            now = datetime.now(tz=UTC)
            return AgentRunResult(
                run_id=request.run_id,
                provider=provider,
                model=model,
                task_type=request.task_type,
                subject_id=request.subject_id,
                prompt_version=request.prompt_version,
                input_sha256=request.input_sha256,
                status="error",
                final_output_json={},
                error=prompt_too_large,
                exit_code=2,
                started_at=now,
                completed_at=now,
                events=[],
            )
        self._ensure_managed_image()
        self._ensure_startup_credentials_volume_sweep()
        run_dir = self._prepare_run_dir(request)
        self._sync_run_dir_to_volume(request.run_id, run_dir)
        credentials_volume = agent_credentials_volume_name(request.run_id)
        command = self.docker_command(
            request=request,
            provider=provider,
            model=model,
            credentials_volume=credentials_volume,
        )
        started_at = datetime.now(tz=UTC)
        events: list[AgentRunEvent] = []
        error = ""
        exit_code = 0
        auth_lock_exclusive = self._auth_lock_must_be_exclusive(provider)
        with provider_auth_lock(provider, exclusive=auth_lock_exclusive):
            container_running = False
            try:
                self._prepare_credentials_volume(
                    provider=provider,
                    credentials_volume=credentials_volume,
                )
                try:
                    container_running = True
                    completed = self._runner(
                        command,
                        capture_output=True,
                        text=True,
                        timeout=self._config.timeout_seconds,
                        check=False,
                    )
                    container_running = False
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
                    self._stop_agent_container(request.run_id)
                    container_running = False
                    completed_at = datetime.now(tz=UTC)
                    exit_code = 124
                    error = f"agent container timed out after {self._config.timeout_seconds}s"
                    stdout = exc.stdout if isinstance(exc.stdout, str) else ""
                    stderr = exc.stderr if isinstance(exc.stderr, str) else ""
                    events.extend(
                        agent_events_from_streams(
                            stdout=stdout,
                            stderr=stderr,
                            created_at=completed_at,
                        )
                    )
                if auth_lock_exclusive:
                    self._persist_credentials_volume(
                        provider=provider,
                        credentials_volume=credentials_volume,
                    )
                    with _AUTH_EXP_CACHE_LOCK:
                        _AUTH_EXP_CACHE.pop((self._config.auth_volume, provider), None)
            finally:
                with _shield_cleanup_signals():
                    if container_running:
                        self._stop_agent_container(request.run_id)
                    self._remove_credentials_volume(credentials_volume, run_id=request.run_id)

        self._sync_run_dir_from_volume(request.run_id, run_dir)
        final_output, final_error = load_agent_final_output(run_dir=run_dir, provider=provider, events=events)
        if exit_code != 0 and not error:
            error = agent_exit_error(exit_code=exit_code, events=events)
        if final_error and not error:
            error = final_error
        status = "completed" if exit_code == 0 and not error else "error"
        return AgentRunResult(
            run_id=request.run_id,
            provider=provider,
            model=model,
            task_type=request.task_type,
            subject_id=request.subject_id,
            prompt_version=request.prompt_version,
            input_sha256=request.input_sha256,
            status=status,
            final_output_json=final_output,
            error=error,
            exit_code=exit_code,
            started_at=started_at,
            completed_at=completed_at,
            events=events,
        )

    def _auth_lock_must_be_exclusive(self, provider: str) -> bool:
        mode = (os.getenv(AGENT_AUTH_LOCK_MODE_ENV) or "exclusive").strip().lower()
        if mode != "auto":
            return True
        if provider != "codex":
            # Only codex's auth expiry is understood; other providers keep the
            # historical one-run-at-a-time behavior.
            return True
        exp = self._shared_auth_token_exp(provider)
        if exp is None:
            return True
        margin = int(
            os.getenv(AGENT_AUTH_REFRESH_MARGIN_SECONDS_ENV)
            or DEFAULT_AGENT_AUTH_REFRESH_MARGIN_SECONDS
        )
        return time.time() >= exp - margin

    def _shared_auth_token_exp(self, provider: str) -> float | None:
        key = (self._config.auth_volume, provider)
        now = time.time()
        with _AUTH_EXP_CACHE_LOCK:
            cached = _AUTH_EXP_CACHE.get(key)
            if cached and now - cached[1] < _AUTH_EXP_CACHE_TTL_SECONDS:
                return cached[0]
        exp = self._read_codex_auth_exp()
        with _AUTH_EXP_CACHE_LOCK:
            _AUTH_EXP_CACHE[key] = (exp, now)
        return exp

    def _read_codex_auth_exp(self) -> float | None:
        """Access-token expiry from the shared auth volume, via a helper
        container (the volume is not mounted into this process)."""
        command = [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--entrypoint",
            "cat",
            "--mount",
            f"type=volume,src={self._config.auth_volume},dst=/agent-auth,readonly",
            self._config.image,
            "/agent-auth/codex/auth.json",
        ]
        try:
            completed = self._runner(command, capture_output=True, text=True, timeout=60, check=False)
            if int(getattr(completed, "returncode", 1)) != 0:
                return None
            payload = json.loads(completed.stdout or "")
            tokens = payload.get("tokens") or {}
            return jwt_expiry_epoch(str(tokens.get("access_token") or ""))
        except Exception:  # noqa: BLE001 - unknown expiry falls back to exclusive
            return None

    def _prepare_credentials_volume(self, *, provider: str, credentials_volume: str) -> None:
        created = self._runner(
            [
                "docker",
                "volume",
                "create",
                "--label",
                "personal-data-warehouse.agent-credentials=ephemeral",
                credentials_volume,
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        if int(getattr(created, "returncode", 1)) != 0:
            raise RuntimeError(f"failed to create ephemeral agent credentials volume {credentials_volume}")
        staged = self._runner(
            agent_credentials_copy_command(
                auth_volume=self._config.auth_volume,
                credentials_volume=credentials_volume,
                provider=provider,
                direction="stage",
            ),
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        if int(getattr(staged, "returncode", 1)) != 0:
            raise RuntimeError(f"failed to stage {provider} credential for agent run")

    def _persist_credentials_volume(self, *, provider: str, credentials_volume: str) -> None:
        persisted = self._runner(
            agent_credentials_copy_command(
                auth_volume=self._config.auth_volume,
                credentials_volume=credentials_volume,
                provider=provider,
                direction="persist",
            ),
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        if int(getattr(persisted, "returncode", 1)) != 0:
            raise RuntimeError(f"failed to persist refreshed {provider} credential after agent run")

    def _remove_credentials_volume(self, credentials_volume: str, *, run_id: str = "") -> None:
        try:
            removed = self._runner(
                ["docker", "volume", "rm", "-f", credentials_volume],
                capture_output=True,
                text=True,
                check=False,
                timeout=60,
            )
            removed_ok = int(getattr(removed, "returncode", 1)) == 0
        except (OSError, subprocess.SubprocessError):
            removed_ok = False
        if removed_ok:
            return
        for container_name in _credentials_volume_container_names(
            credentials_volume=credentials_volume,
            run_id=run_id,
        ):
            self._force_remove_container(container_name)
        try:
            self._runner(
                ["docker", "volume", "rm", "-f", credentials_volume],
                capture_output=True,
                text=True,
                check=False,
                timeout=60,
            )
        except (OSError, subprocess.SubprocessError):
            logger.warning("Failed to remove agent credentials volume %s", credentials_volume, exc_info=True)

    def _stop_agent_container(self, run_id: str) -> None:
        container_name = agent_container_name(run_id)
        try:
            stopped = self._runner(
                ["docker", "stop", "--time", "10", container_name],
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
            stopped_ok = int(getattr(stopped, "returncode", 1)) == 0
        except (OSError, subprocess.SubprocessError):
            stopped_ok = False
        if not stopped_ok:
            self._force_remove_container(container_name)

    def _force_remove_container(self, container_name: str) -> None:
        try:
            self._runner(
                ["docker", "rm", "-f", container_name],
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
        except (OSError, subprocess.SubprocessError):
            logger.warning("Failed to force-remove agent container %s", container_name, exc_info=True)

    def sweep_stale_credentials_volumes(self, *, now: datetime | None = None) -> list[str]:
        return sweep_stale_credentials_volumes(
            timeout_seconds=self._config.timeout_seconds,
            runner=self._runner,
            now=now,
        )

    def _ensure_startup_credentials_volume_sweep(self) -> None:
        global _STARTUP_CREDENTIALS_SWEEP_DONE
        if _STARTUP_CREDENTIALS_SWEEP_DONE:
            return
        with _STARTUP_CREDENTIALS_SWEEP_LOCK:
            if _STARTUP_CREDENTIALS_SWEEP_DONE:
                return
            _STARTUP_CREDENTIALS_SWEEP_DONE = True
            try:
                self.sweep_stale_credentials_volumes()
            except Exception:  # noqa: BLE001 - startup sweep is best-effort
                logger.warning("Startup agent credentials volume sweep failed", exc_info=True)

    def run_with_pdw(
        self,
        request: AgentRunRequest,
        *,
        app: WarehouseAppConfig | None = None,
        max_rows: int = DEFAULT_AGENT_TOOL_PROXY_MAX_ROWS,
        exclude_adapters: Collection[str] = (),
        exclude_refs: Collection[str] = (),
    ) -> AgentRunResult:
        """Run the agent with an authenticated `pdw` CLI pointed at a per-run proxy.

        The container gets a throwaway token for a proxy that only forwards
        read-only warehouse tools and stored-object reads; the app's real
        credential stays in this process. Exclusions default to empty; setting
        them also enables context for this run. Counters return only in the
        result's exclusion_counts. SQL selecting only text without identifying
        cells cannot be recognized; the benchmark detects that from the trace.
        """

        app = app or warehouse_app_config_from_env()
        counts = AgentExclusionCounts()
        with run_agent_tool_proxy(
            app=app,
            run_id=request.run_id,
            max_rows=max_rows,
            exclude_adapters=exclude_adapters,
            exclude_refs=exclude_refs,
            exclusion_counts=counts,
            bind_host=self._config.tool_proxy_bind_host,
            public_host=self._config.tool_proxy_public_host,
        ) as tool_env:
            result = self.run(request.with_extra_env(tool_env))
        return replace(result, exclusion_counts=counts.snapshot())

    def _ensure_managed_image(self) -> None:
        if self._config.image != default_agent_docker_image():
            return
        ensure_agent_image(runner=self._runner)

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
        inputs_dir = run_dir / DEFAULT_AGENT_INPUTS_DIR_NAME
        if request.input_files:
            inputs_dir.mkdir(parents=True, exist_ok=True)
            for relative_path, content in request.input_files.items():
                input_path = safe_agent_input_path(inputs_dir, relative_path)
                input_path.parent.mkdir(parents=True, exist_ok=True)
                if isinstance(content, bytes):
                    input_path.write_bytes(content)
                else:
                    input_path.write_text(content, encoding="utf-8")
        (run_dir / "request.json").write_text(
            json.dumps(
                {
                    "run_id": request.run_id,
                    "task_type": request.task_type,
                    "subject_id": request.subject_id,
                    "prompt_version": request.prompt_version,
                    "input_sha256": request.input_sha256,
                    "input_files": sorted(request.input_files),
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        write_builtin_cli_tools(run_dir)
        return run_dir

    def docker_command(
        self,
        *,
        request: AgentRunRequest,
        provider: str,
        model: str,
        credentials_volume: str,
    ) -> list[str]:
        name = agent_container_name(request.run_id)
        container_run_dir = f"{DEFAULT_AGENT_CONTAINER_RUNS_DIR}/{request.run_id}"
        credential_path = provider_credential_relative_path(provider)
        container_credential_path = f"{DEFAULT_AGENT_CONTAINER_CREDENTIALS_DIR}/{credential_path}"
        env = [
            "--env",
            f"AGENT_PROVIDER={provider}",
            "--env",
            f"AGENT_MODEL={model}",
            "--env",
            f"AGENT_REASONING_EFFORT={self._config.reasoning_effort}",
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
            f"AGENT_INPUT_DIR={container_run_dir}/{DEFAULT_AGENT_INPUTS_DIR_NAME}",
            "--env",
            f"PDW_TOOL_HELP={container_run_dir}/{DEFAULT_AGENT_TOOLS_DIR_NAME}/pdw-tool-help",
            "--env",
            f"PDW_VALIDATE_JSON={container_run_dir}/{DEFAULT_AGENT_TOOLS_DIR_NAME}/pdw-validate-json",
            "--env",
            f"AGENT_AUTH_SOURCE={container_credential_path}",
            "--env",
            f"AGENT_AUTH_OUTPUT={container_credential_path}",
            "--env",
            f"CODEX_HOME={DEFAULT_AGENT_CONTAINER_CODEX_HOME}",
            "--env",
            f"CODEX_SQLITE_HOME={DEFAULT_AGENT_CONTAINER_CODEX_SQLITE_HOME}",
            "--env",
            f"CLAUDE_CONFIG_DIR={DEFAULT_AGENT_CONTAINER_CLAUDE_CONFIG_DIR}",
            "--env",
            "HOME=/tmp/agent-home",
        ]
        for key, value in sorted(request.extra_env.items()):
            env.extend(["--env", f"{key}={value}"])
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
            "--add-host",
            "host.docker.internal:host-gateway",
            "--memory",
            self._config.memory,
            "--memory-swap",
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
            (
                f"type=volume,src={credentials_volume},"
                f"dst={DEFAULT_AGENT_CONTAINER_CREDENTIALS_DIR}"
            ),
            "--mount",
            f"type=volume,src={self._config.runs_volume},dst={DEFAULT_AGENT_CONTAINER_RUNS_DIR}",
            *env,
            self._config.image,
        ]


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
        "prompt_version": result.prompt_version,
        "status": result.status,
        "input_sha256": result.input_sha256,
        "final_output_json": json.dumps(result.final_output_json, sort_keys=True, separators=(",", ":"), default=str),
        "error": _bounded_agent_diagnostic(result.error, MAX_AGENT_RUN_ERROR_CHARS),
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
                "event_json": _serialized_agent_event_json(
                    event.event_json,
                    event_type=event.event_type,
                ),
                "text": _bounded_agent_diagnostic(
                    event.text,
                    MAX_AGENT_RUN_EVENT_TEXT_CHARS,
                ),
                "created_at": event.created_at,
                "sync_version": sync_version,
            }
        )
    return rows


def _serialized_agent_event_json(
    payload: Mapping[str, Any],
    *,
    event_type: str,
) -> str:
    """Return a redacted, valid, bounded JSON representation for durable storage.

    Container stdout/stderr is an untrusted diagnostic boundary: a provider or
    tool can echo an environment assignment or authorization header. Structured
    secret fields are removed deterministically, recognizable secrets embedded
    in strings are masked best-effort, and oversized payloads become an explicit
    envelope that retains the event type plus a bounded preview.
    """

    sanitized = _redact_agent_diagnostic_value(payload)
    serialized = json.dumps(sanitized, sort_keys=True, separators=(",", ":"), default=str)
    if len(serialized) <= MAX_AGENT_RUN_EVENT_JSON_CHARS:
        return serialized

    envelope: dict[str, Any] = {
        "_pdw_event_type": _truncate_with_marker(
            _redact_recognizable_credentials(event_type),
            256,
        ),
        "_pdw_original_chars": len(serialized),
        "_pdw_truncated": True,
    }
    if isinstance(sanitized, Mapping):
        for key in ("type", "event", "subtype", "status"):
            value = sanitized.get(key)
            if isinstance(value, (str, int, float, bool)) or value is None:
                envelope[key] = (
                    _truncate_with_marker(value, 256) if isinstance(value, str) else value
                )

    # JSON escaping makes preview size non-linear. Binary search keeps the
    # stored envelope within the hard limit without ever emitting invalid JSON.
    best = json.dumps(envelope, sort_keys=True, separators=(",", ":"), default=str)
    low = 0
    high = min(len(serialized), MAX_AGENT_RUN_EVENT_JSON_CHARS)
    while low <= high:
        midpoint = (low + high) // 2
        candidate = {
            **envelope,
            "_pdw_preview": _truncate_with_marker(serialized, midpoint),
        }
        candidate_json = json.dumps(
            candidate,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        if len(candidate_json) <= MAX_AGENT_RUN_EVENT_JSON_CHARS:
            best = candidate_json
            low = midpoint + 1
        else:
            high = midpoint - 1
    return best


def _redact_agent_diagnostic_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        redacted: dict[Any, Any] = {}
        for key, child in value.items():
            if _is_sensitive_event_key(key):
                redacted[key] = _REDACTED
            else:
                redacted[key] = _redact_agent_diagnostic_value(child)
        return redacted
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_redact_agent_diagnostic_value(child) for child in value]
    if isinstance(value, str):
        return _redact_recognizable_credentials(value)
    return value


def _is_sensitive_event_key(key: Any) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(key).strip().lower()).strip("_")
    return normalized in _SENSITIVE_EVENT_KEYS or normalized.endswith(
        ("_api_key", "_access_token", "_refresh_token", "_client_secret", "_session_key")
    )


def _redact_recognizable_credentials(value: str) -> str:
    def replace_assignment(match: re.Match[str]) -> str:
        quote = match.group("quote") or ""
        return f"{match.group('prefix')}{quote}{_REDACTED}{quote}"

    redacted = _AUTHORIZATION_RE.sub(
        lambda match: f"{match.group('prefix')}{_REDACTED}",
        value,
    )
    redacted = _SECRET_ASSIGNMENT_RE.sub(replace_assignment, redacted)
    redacted = _PROVIDER_TOKEN_RE.sub(_REDACTED, redacted)
    return _JWT_RE.sub(_REDACTED, redacted)


def _bounded_agent_diagnostic(value: str, max_chars: int) -> str:
    return _truncate_with_marker(_redact_recognizable_credentials(str(value)), max_chars)


def _truncate_with_marker(value: str, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    if len(value) <= max_chars:
        return value
    marker = f"\n...[truncated; original length {len(value)} characters]...\n"
    if len(marker) >= max_chars:
        return marker[:max_chars]
    remaining = max_chars - len(marker)
    head_chars = remaining // 2
    tail_chars = remaining - head_chars
    return value[:head_chars] + marker + value[-tail_chars:]


def _claude_blocks(payload: Mapping[str, Any], block_type: str) -> list[tuple[int, Mapping[str, Any]]]:
    message = payload.get("message")
    if not isinstance(message, Mapping):
        return []
    content = message.get("content")
    if not isinstance(content, list):
        return []
    return [(index, block) for index, block in enumerate(content)
            if isinstance(block, Mapping) and block.get("type") == block_type]


def tool_call_payloads(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Normalize every tool block in one event for both CLI providers."""
    if payload.get("type") == "assistant":
        return [block for _, block in _claude_blocks(payload, "tool_use")]
    return [payload] if extract_tool_name(payload) else []


def agent_run_tool_call_rows(result: AgentRunResult) -> list[dict[str, Any]]:
    """Pair Claude results by ID and preserve every call without a schema change.

    Codex indexes keep naming their raw event. Claude call indexes start above
    all raw events, in content-block order; _pdw_trace in arguments maps each
    synthetic index back to the raw event and the provider's tool_use_id.
    """
    rows = []
    sync_version = int(result.completed_at.timestamp() * 1_000_000)
    returned = {}
    for event in result.events:
        if event.event_json.get("type") == "user":
            for _, block in _claude_blocks(event.event_json, "tool_result"):
                if block.get("tool_use_id"):
                    returned[block["tool_use_id"]] = (event, block)
    next_index = max((event.event_index for event in result.events), default=-1) + 1
    for event in result.events:
        payload = event.event_json
        claude = payload.get("type") == "assistant"
        calls = (_claude_blocks(payload, "tool_use") if claude
                 else [(0, call) for call in tool_call_payloads(payload)])
        for block_index, call in calls:
            arguments = dict(extract_tool_arguments(call))
            paired = returned.get(call.get("id")) if claude else None
            completed_event, output = paired if paired else (event, call)
            event_index = event.event_index
            if claude:
                event_index = next_index
                next_index += 1
                arguments["_pdw_trace"] = {
                    "tool_use_id": call.get("id", ""),
                    "source_event_index": event.event_index,
                    "content_block_index": block_index,
                }
            rows.append({
                "run_id": result.run_id,
                "event_index": event_index,
                "tool_name": extract_tool_name(call),
                "arguments_json": json.dumps(arguments, sort_keys=True, separators=(",", ":"), default=str),
                "result_json": json.dumps(extract_tool_result(output), sort_keys=True, separators=(",", ":"), default=str),
                "error": str(output.get("error") or ("tool_result is_error" if output.get("is_error") else "")),
                "started_at": event.created_at,
                "completed_at": completed_event.created_at,
                "sync_version": sync_version,
            })
    return rows


def extract_tool_name(payload: Mapping[str, Any]) -> str:
    blocks = _claude_blocks(payload, "tool_use")
    if payload.get("type") == "assistant" and blocks:
        return extract_tool_name(blocks[0][1])
    if payload.get("type") == "tool_use":
        name = str(payload.get("name") or "")
        if name == "Bash":
            return _shell_tool_name(str(extract_tool_arguments(payload).get("command") or ""))
        return name
    item = payload.get("item")
    if isinstance(item, Mapping) and item.get("type") == "command_execution":
        return _shell_tool_name(str(item.get("command") or ""))
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


def _shell_tool_name(command: str) -> str:
    if "pdw-validate-json" in command or "PDW_VALIDATE_JSON" in command:
        return "pdw-validate-json"
    return pdw_cli_invocation(command) or "bash"


PDW_CLI_SUBCOMMANDS = frozenset(PDW_CLI_READ_SUBCOMMANDS)


def pdw_cli_invocation(command: str) -> str:
    """Name the `pdw` invocation inside a shell command, or "" if there is none.

    Shell tool use arrives as Codex command events or Claude Bash blocks; this is how
    warehouse access gets attributed in `agent_run_tool_calls`. `pdw call <tool>`
    keeps the tool name because that is the interesting part.
    """

    tokens = command.replace("'", " ").replace('"', " ").split()
    for index, token in enumerate(tokens):
        if token != "pdw" and not token.endswith("/pdw"):
            continue
        rest = [item for item in tokens[index + 1 :] if not item.startswith("-")]
        if not rest:
            return "pdw"
        subcommand = rest[0]
        if subcommand not in PDW_CLI_SUBCOMMANDS:
            return "pdw"
        if subcommand == "call":
            # The tool name may follow the flags rather than precede them, and
            # the --data JSON has been flattened into tokens by now — so match
            # known tool names rather than guessing from token shape, or
            # `--data '{"storage_file_id":...}' get_object` reads as a call to
            # "storage_file_id".
            for candidate in rest[1:]:
                if candidate in DEFAULT_AGENT_TOOL_ALLOWLIST | {"context"}:
                    return f"pdw call {candidate}"
        return f"pdw {subcommand}"
    return ""


def extract_tool_arguments(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    blocks = _claude_blocks(payload, "tool_use")
    if payload.get("type") == "assistant" and blocks:
        return extract_tool_arguments(blocks[0][1])
    item = payload.get("item")
    if isinstance(item, Mapping) and item.get("type") == "command_execution":
        return {"command": str(item.get("command") or "")}
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
    blocks = _claude_blocks(payload, "tool_result")
    if payload.get("type") == "user" and blocks:
        return extract_tool_result(blocks[0][1])
    if payload.get("type") == "tool_result":
        content = payload.get("content", "")
        output = ""
        if isinstance(content, str):
            output = content
        elif isinstance(content, list):
            output = "\n".join(str(block.get("text") or "") for block in content if isinstance(block, Mapping))
        return {"output": output, "is_error": bool(payload.get("is_error"))}
    item = payload.get("item")
    if isinstance(item, Mapping) and item.get("type") == "command_execution":
        return {
            "status": str(item.get("status") or ""),
            "exit_code": item.get("exit_code"),
            "output": str(item.get("aggregated_output") or ""),
        }
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


def agent_exit_error(*, exit_code: int, events: Sequence[AgentRunEvent]) -> str:
    fatal = _last_structured_failure_text(events)
    if fatal:
        return f"agent container exited with code {exit_code}: {fatal}"
    diagnostic = _last_failure_diagnostic(events)
    if diagnostic:
        # stderr can contain a startup warning that is unrelated to the exit.
        # Keep it useful without presenting a synthesized last line as the
        # exact root cause when the agent emitted no structured fatal event.
        return f"agent container exited with code {exit_code}; last diagnostic: {diagnostic}"
    return f"agent container exited with code {exit_code}"


def last_error_event_text(events: Sequence[AgentRunEvent]) -> str:
    return _last_structured_failure_text(events) or _last_failure_diagnostic(events)


def _last_structured_failure_text(events: Sequence[AgentRunEvent]) -> str:
    for event in reversed(events):
        payload = event.event_json
        event_type = event.event_type.strip().lower()
        status = str(payload.get("status") or payload.get("subtype") or "").strip().lower()
        is_failure = (
            "error" in event_type
            or "fatal" in event_type
            or event_type.endswith(".failed")
            or status in {"error", "failed", "fatal", "error_during_execution"}
            or status.startswith("error_")
            or payload.get("is_error") is True
        )
        if not is_failure:
            continue
        for value in (
            payload.get("message"),
            payload.get("error"),
            payload.get("detail"),
            payload.get("result"),
        ):
            text = _structured_diagnostic_text(value)
            if text:
                return text[-1000:]
        text = event.text.strip()
        if text:
            return text[-1000:]
    return ""


def _structured_diagnostic_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Mapping):
        for key in ("message", "error", "detail", "reason"):
            text = _structured_diagnostic_text(value.get(key))
            if text:
                return text
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return ""


def _last_failure_diagnostic(events: Sequence[AgentRunEvent]) -> str:
    for event in reversed(events):
        text = event.text.strip()
        if not text:
            continue
        if event.stream == "stderr" or "error" in event.event_type.lower() or "Error:" in text or "ERROR" in text:
            return text[-1000:]
    return ""


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


def agent_container_name(run_id: str) -> str:
    return docker_safe_name(f"pdw-agent-{run_id}")


def agent_credentials_volume_name(run_id: str) -> str:
    run_name = docker_safe_name(run_id)[:60]
    return f"{AGENT_CREDENTIALS_VOLUME_PREFIX}{run_name}-{uuid.uuid4().hex}"


def agent_credentials_helper_container_name(credentials_volume: str, direction: str) -> str:
    return docker_safe_name(f"{credentials_volume}-{direction}")


def _credentials_volume_run_name(credentials_volume: str) -> str:
    if not credentials_volume.startswith(AGENT_CREDENTIALS_VOLUME_PREFIX):
        return ""
    remainder = credentials_volume[len(AGENT_CREDENTIALS_VOLUME_PREFIX) :]
    if (
        len(remainder) > 33
        and remainder[-33] == "-"
        and all(ch in "0123456789abcdefABCDEF" for ch in remainder[-32:])
    ):
        return remainder[:-33]
    return remainder


def _credentials_volume_container_names(*, credentials_volume: str, run_id: str = "") -> list[str]:
    names: list[str] = []
    if run_id.strip():
        names.append(agent_container_name(run_id))
    run_name = _credentials_volume_run_name(credentials_volume)
    if run_name:
        inferred = docker_safe_name(f"pdw-agent-{run_name}")
        if inferred not in names:
            names.append(inferred)
    for direction in ("stage", "persist"):
        helper_name = agent_credentials_helper_container_name(credentials_volume, direction)
        if helper_name not in names:
            names.append(helper_name)
    return names


def _parse_docker_timestamp(value: str) -> datetime | None:
    cleaned = value.strip()
    if not cleaned:
        return None
    if cleaned.endswith("Z"):
        cleaned = f"{cleaned[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _credentials_volume_is_in_use(
    credentials_volume: str,
    *,
    running_containers: Collection[str],
    running_mounts: Collection[str],
) -> bool:
    if credentials_volume in running_mounts:
        return True
    for name in _credentials_volume_container_names(credentials_volume=credentials_volume):
        if name in running_containers:
            return True
    run_name = _credentials_volume_run_name(credentials_volume)
    if len(run_name) == 60:
        prefix = docker_safe_name(f"pdw-agent-{run_name}")
        if any(container.startswith(prefix) for container in running_containers):
            return True
    return False


def sweep_stale_credentials_volumes(
    *,
    timeout_seconds: int = 1800,
    sweep_margin_seconds: int = DEFAULT_AGENT_CREDENTIALS_VOLUME_SWEEP_MARGIN_SECONDS,
    runner=subprocess.run,
    now: datetime | None = None,
) -> list[str]:
    """Remove orphaned per-run credential volumes older than timeout + margin with no running container."""
    try:
        listed = runner(
            [
                "docker",
                "volume",
                "ls",
                "--filter",
                f"name={AGENT_CREDENTIALS_VOLUME_PREFIX}",
                "--format",
                "{{.Name}}",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError):
        logger.warning("Failed to list agent credentials volumes for sweep", exc_info=True)
        return []
    if int(getattr(listed, "returncode", 1)) != 0:
        return []
    volume_names = [
        line.strip()
        for line in (listed.stdout or "").splitlines()
        if line.strip().startswith(AGENT_CREDENTIALS_VOLUME_PREFIX)
    ]
    if not volume_names:
        return []

    try:
        inspected = runner(
            ["docker", "volume", "inspect", *volume_names],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError):
        logger.warning("Failed to inspect agent credentials volumes for sweep", exc_info=True)
        return []
    if int(getattr(inspected, "returncode", 1)) != 0:
        return []
    try:
        parsed_inspect = json.loads(inspected.stdout or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed_inspect, list):
        return []

    effective_now = now.astimezone(UTC) if now is not None else datetime.now(tz=UTC)
    cutoff_seconds = max(int(timeout_seconds), 0) + max(int(sweep_margin_seconds), 0)
    stale_candidates: list[str] = []
    for entry in parsed_inspect:
        if not isinstance(entry, Mapping):
            continue
        name = str(entry.get("Name") or "").strip()
        if not name.startswith(AGENT_CREDENTIALS_VOLUME_PREFIX):
            continue
        created_at = _parse_docker_timestamp(str(entry.get("CreatedAt") or ""))
        if created_at is None:
            continue
        if (effective_now - created_at).total_seconds() > cutoff_seconds:
            stale_candidates.append(name)
    if not stale_candidates:
        return []

    try:
        ps_completed = runner(
            ["docker", "ps", "--no-trunc", "--format", "{{.Names}}\t{{.Mounts}}"],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError):
        logger.warning("Failed to inspect running containers before credentials volume sweep", exc_info=True)
        return []
    if int(getattr(ps_completed, "returncode", 1)) != 0:
        return []

    running_containers: set[str] = set()
    running_mounts: set[str] = set()
    for line in (ps_completed.stdout or "").splitlines():
        if not line.strip():
            continue
        names_field, _, mounts_field = line.partition("\t")
        for raw_name in names_field.split(","):
            cleaned_name = raw_name.strip().lstrip("/")
            if cleaned_name:
                running_containers.add(cleaned_name)
        for raw_mount in mounts_field.split(","):
            cleaned_mount = raw_mount.strip()
            if cleaned_mount:
                running_mounts.add(cleaned_mount)

    removed: list[str] = []
    for volume_name in stale_candidates:
        if _credentials_volume_is_in_use(
            volume_name,
            running_containers=running_containers,
            running_mounts=running_mounts,
        ):
            continue
        try:
            rm_completed = runner(
                ["docker", "volume", "rm", "-f", volume_name],
                capture_output=True,
                text=True,
                check=False,
                timeout=60,
            )
        except (OSError, subprocess.SubprocessError):
            logger.warning("Failed to remove stale agent credentials volume %s", volume_name, exc_info=True)
            continue
        if int(getattr(rm_completed, "returncode", 1)) == 0:
            removed.append(volume_name)
    return removed


def provider_credential_relative_path(provider: str) -> str:
    provider = provider.strip().lower()
    if provider == "codex":
        return "codex/auth.json"
    if provider == "claude":
        return "claude/.credentials.json"
    raise ValueError("provider must be codex or claude")


def agent_credentials_copy_command(
    *,
    auth_volume: str,
    credentials_volume: str,
    provider: str,
    direction: str,
) -> list[str]:
    credential_path = provider_credential_relative_path(provider)
    persistent_path = f"/persistent-auth/{credential_path}"
    run_path = f"/run-credentials/{credential_path}"
    if direction == "stage":
        source_path = persistent_path
        destination_path = run_path
        auth_mount = f"type=volume,src={auth_volume},dst=/persistent-auth,readonly"
        credentials_mount = f"type=volume,src={credentials_volume},dst=/run-credentials"
    elif direction == "persist":
        source_path = run_path
        destination_path = persistent_path
        auth_mount = f"type=volume,src={auth_volume},dst=/persistent-auth"
        credentials_mount = (
            f"type=volume,src={credentials_volume},dst=/run-credentials,readonly"
        )
    else:
        raise ValueError("direction must be stage or persist")
    destination_dir = destination_path.rsplit("/", 1)[0]
    temporary_path = f"{destination_path}.tmp"
    shell_command = (
        f'test -s "{source_path}"'
        f' || {{ echo "missing {provider} agent credential" >&2; exit 1; }}'
        f' && mkdir -p "{destination_dir}"'
        f' && cp "{source_path}" "{temporary_path}"'
        f' && chmod 600 "{temporary_path}"'
        f' && mv "{temporary_path}" "{destination_path}"'
    )
    return [
        "docker",
        "run",
        "--rm",
        "--name",
        agent_credentials_helper_container_name(credentials_volume, direction),
        "--network",
        "none",
        "--read-only",
        "--tmpfs",
        "/tmp:rw,nosuid,size=16m",
        "--mount",
        auth_mount,
        "--mount",
        credentials_mount,
        "alpine:3.20",
        "sh",
        "-lc",
        shell_command,
    ]


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

## pdw

`pdw` is the personal data warehouse CLI, already authenticated for this run and
on `PATH`. It is the way to reach warehouse data. Run `pdw help` for the full
usage; the commands available to an agent run are:

```bash
pdw search --priority self,direct,cc 'distinctive terms' # FIRST for text/person/topic/id
pdw sql -q 'read hit context' "SELECT * FROM timeline.context('REF', 5, 5)"
pdw schema                                             # relations + keys, only for structured SQL
pdw columns base_gmail.messages                        # exact columns + types for ONE relation
pdw sql -q 'why you are asking' 'SELECT ...'   # read-only SQL
pdw sql -q 'why you are asking' --file query.sql   # SQL from a file (no shell quoting)
pdw call get_object --data '{"storage_file_id":"..."}'   # locate a stored blob
```

Notes that save a round trip:

- Start with `search`, using the priority scope that matches the question.
  Use `self,direct,cc` for attention/correspondence, `self` for Zach's own
  acts or words, `self,background` for prior agent conclusions, and `noise`
  for automated traffic. Omit the filter only for broad recall or when the
  relevant tier is genuinely unknown. Every hit includes `priority` and `ref`.
- `schema` lists relations but NOT columns. Run `columns` on every relation you
  are about to reference; guessing column names is the top cause of failed SQL.
- Always pass `-q` with a short plain-English intent. It is logged server-side.
- `--output csv|json|nd-json` selects the result format; CSV is the default.
- Results are capped per query, so use focused `SELECT` lists and `LIMIT`. A
  `truncated` flag with `proxy_row_limit` in the response means you are seeing a
  slice of a larger result; narrow the query rather than paging blindly.
- Writes are impossible: the CLI reaches a read-only surface through a per-run
  proxy, and no database URL or app credential exists in this container.

`pdw call get_object` returns metadata plus a signed, time-limited
`download_url`. Fetch the bytes with that URL — it needs no extra
authentication:

```bash
url=$(pdw call get_object --data '{"storage_file_id":"FILE_ID"}' | jq -r .download_url)
curl -fsS "$url" -o attachment.pdf
```

Task input files that were too large for the prompt are already on disk in
`$AGENT_INPUT_DIR`; read those directly instead of re-fetching them.

## pdw-tool-help

Print this tool manifest.

Usage:

```bash
"$PDW_TOOL_HELP"
```

## pdw-validate-json

Validate a JSON file against a simple JSON object schema.

Usage:

```bash
"$PDW_VALIDATE_JSON" candidate.json schema.json
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


def jwt_expiry_epoch(token: str) -> float | None:
    """The `exp` claim of a JWT, without signature verification."""
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(padded))
        exp = claims.get("exp")
        return float(exp) if exp else None
    except Exception:  # noqa: BLE001 - malformed token means unknown expiry
        return None


@contextmanager
def provider_auth_lock(provider: str, *, exclusive: bool = True):
    """Serialize provider auth across agent containers.

    Shared holders run from disposable credential snapshots and discard any
    changes. A run that might trigger a token refresh takes the lock EXCLUSIVE,
    executes alone, and persists only its refreshed credential. ChatGPT-mode
    codex rotates refresh tokens, so two containers refreshing concurrently
    can invalidate the login and force a manual re-authentication.
    """
    lock_path = Path(os.getenv("AGENT_AUTH_LOCK_DIR", "/tmp")) / f"pdw-agent-{docker_safe_name(provider)}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = lock_path.open("a+")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        yield
    finally:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()


def agent_config_from_env() -> AgentContainerConfig:
    network = os.getenv("AGENT_DOCKER_NETWORK", DEFAULT_AGENT_NETWORK)
    provider = os.getenv("AGENT_PROVIDER", "codex")
    return AgentContainerConfig(
        image=default_agent_docker_image(),
        provider=provider,
        model=(os.getenv("AGENT_MODEL") or default_agent_model(provider)).strip(),
        reasoning_effort=(
            os.getenv("AGENT_REASONING_EFFORT") or DEFAULT_AGENT_REASONING_EFFORT
        ).strip(),
        auth_volume=os.getenv("AGENT_AUTH_VOLUME", DEFAULT_AGENT_AUTH_VOLUME),
        runs_volume=os.getenv("AGENT_RUNS_VOLUME", DEFAULT_AGENT_RUNS_VOLUME),
        runs_dir=Path(os.getenv("AGENT_RUNS_DIR", DEFAULT_AGENT_RUNS_DIR)),
        network=network,
        memory=os.getenv("AGENT_DOCKER_MEMORY", DEFAULT_AGENT_MEMORY),
        cpus=os.getenv("AGENT_DOCKER_CPUS", DEFAULT_AGENT_CPUS),
        pids_limit=int(os.getenv("AGENT_DOCKER_PIDS_LIMIT", str(DEFAULT_AGENT_PIDS_LIMIT))),
        timeout_seconds=int(os.getenv("AGENT_TIMEOUT_SECONDS", "1800")),
        tool_proxy_bind_host=os.getenv("AGENT_TOOL_PROXY_BIND_HOST", DEFAULT_AGENT_TOOL_PROXY_BIND_HOST),
        tool_proxy_public_host=os.getenv(
            "AGENT_TOOL_PROXY_PUBLIC_HOST",
            default_agent_tool_proxy_public_host(network),
        ),
    )


def default_agent_model(provider: str) -> str:
    return DEFAULT_AGENT_MODEL if provider.strip().lower() == "codex" else ""


def default_agent_tool_proxy_public_host(network: str) -> str:
    if network and network != DEFAULT_AGENT_NETWORK:
        return socket.gethostname()
    return DEFAULT_AGENT_TOOL_PROXY_PUBLIC_HOST


def default_agent_docker_image(
    *,
    repository: str = DEFAULT_AGENT_IMAGE_REPOSITORY,
    dockerfile_path: Path = DEFAULT_AGENT_DOCKERFILE_PATH,
    entrypoint_path: Path = DEFAULT_AGENT_ENTRYPOINT_PATH,
    cli_source_dir: Path = DEFAULT_AGENT_CLI_SOURCE_DIR,
) -> str:
    digest = hashlib.sha256()
    for path in (dockerfile_path, entrypoint_path, *pdw_cli_source_files(cli_source_dir)):
        digest.update(str(path.name).encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return f"{repository}:{digest.hexdigest()[:6]}"


def pdw_cli_source_files(cli_source_dir: Path) -> list[Path]:
    """The Go sources compiled into the image's `pdw`, in a stable order.

    Test files are excluded: they never reach the binary, so letting them move
    the image tag would rebuild the image on every test edit.
    """

    if not cli_source_dir.is_dir():
        return []
    sources = [
        path
        for path in cli_source_dir.rglob("*.go")
        if path.is_file() and not path.name.endswith("_test.go")
    ]
    for name in ("go.mod", "go.sum"):
        manifest = cli_source_dir / name
        if manifest.is_file():
            sources.append(manifest)
    return sorted(sources)


def ensure_agent_image(
    *,
    dockerfile_path: Path = DEFAULT_AGENT_DOCKERFILE_PATH,
    entrypoint_path: Path = DEFAULT_AGENT_ENTRYPOINT_PATH,
    context_dir: Path = DEFAULT_AGENT_BUILD_CONTEXT_DIR,
    cli_source_dir: Path = DEFAULT_AGENT_CLI_SOURCE_DIR,
    runner=subprocess.run,
) -> str:
    resolved_image = default_agent_docker_image(
        dockerfile_path=dockerfile_path,
        entrypoint_path=entrypoint_path,
        cli_source_dir=cli_source_dir,
    )
    inspect = runner(
        ["docker", "image", "inspect", resolved_image],
        capture_output=True,
        text=True,
        check=False,
    )
    if getattr(inspect, "returncode", 1) == 0:
        return resolved_image
    build = runner(
        [
            "docker",
            "build",
            "-f",
            str(dockerfile_path),
            "-t",
            resolved_image,
            str(context_dir),
        ],
        check=False,
    )
    if getattr(build, "returncode", 1) != 0:
        raise RuntimeError(f"failed to build agent Docker image {resolved_image}")
    return resolved_image


def auth_docker_command(*, provider: str, action: str, config: AgentContainerConfig, interactive: bool) -> list[str]:
    provider = provider.strip().lower()
    if provider not in {"codex", "claude"}:
        raise ValueError("provider must be codex or claude")
    command = {
        ("codex", "login"): "codex login --device-auth",
        ("codex", "status"): "codex login status",
        ("claude", "login"): "claude",
        ("claude", "status"): "claude auth status",
    }[(provider, action)]
    shell_command = (
        "mkdir -p \"$CODEX_HOME\" \"$CODEX_SQLITE_HOME\" \"$CLAUDE_CONFIG_DIR\" \"$HOME\""
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
        f"CODEX_SQLITE_HOME={DEFAULT_AGENT_CONTAINER_CODEX_SQLITE_HOME}",
        "--env",
        f"CLAUDE_CONFIG_DIR={DEFAULT_AGENT_CONTAINER_AUTH_DIR}/claude",
        "--env",
        "HOME=/tmp/agent-home",
        "--tmpfs",
        "/tmp/agent-home:rw,nosuid,size=512m",
        "--tmpfs",
        f"{DEFAULT_AGENT_CONTAINER_CODEX_SQLITE_HOME}:rw,nosuid,size=512m",
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
    image = ensure_agent_image()
    config = agent_config_from_env()
    config = AgentContainerConfig(
        image=image,
        provider=config.provider,
        model=config.model,
        reasoning_effort=config.reasoning_effort,
        auth_volume=config.auth_volume,
        runs_volume=config.runs_volume,
        runs_dir=config.runs_dir,
        network=config.network,
        memory=config.memory,
        cpus=config.cpus,
        pids_limit=config.pids_limit,
        timeout_seconds=config.timeout_seconds,
        tool_proxy_bind_host=config.tool_proxy_bind_host,
        tool_proxy_public_host=config.tool_proxy_public_host,
    )
    command = auth_docker_command(
        provider=args.provider,
        action=args.action,
        config=config,
        interactive=not args.non_interactive,
    )
    return subprocess.call(command)


if __name__ == "__main__":
    raise SystemExit(auth_main())
