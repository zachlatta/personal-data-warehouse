"""Local pytest integration bootstrap helpers.

This module lives under ``tests`` deliberately: it provisions disposable test
infrastructure and must never become part of the warehouse runtime.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, MutableMapping
import inspect
import os
import re
import shutil
import subprocess
import time
import uuid

from personal_data_warehouse.agent_runner import AgentContainerConfig, auth_docker_command

DEFAULT_POSTGRES_IMAGE = "ghcr.io/zachlatta/personal-data-warehouse-postgres-pgbackrest:latest"
POSTGRES_EXTENSIONS = frozenset({"vector", "pg_trgm", "pg_textsearch"})
_UNIT_ONLY_COMMAND = "uv run pytest --unit-only"
_POSTGRES_URL_MARKER = "POSTGRES_DATABASE_URL"


class LocalTestStartupError(RuntimeError):
    """A local integration prerequisite could not be made ready."""


def subscription_tests_enabled(env: Mapping[str, str]) -> bool:
    """Select real subscription smokes without ambiguous truthy parsing."""
    if "RUN_LIVE_AGENT_TESTS" in env:
        value = env["RUN_LIVE_AGENT_TESTS"]
        if value == "1":
            return True
        if value == "0":
            return False
        raise LocalTestStartupError("RUN_LIVE_AGENT_TESTS must be 0 or 1 when set")
    return not bool(env.get("CI"))


def subscription_tests_skip_reason(env: Mapping[str, str]) -> str:
    if env.get("RUN_LIVE_AGENT_TESTS") == "0":
        return "RUN_LIVE_AGENT_TESTS=0 explicitly disables subscription-backed agent tests"
    return "subscription-backed agent tests default off under CI; set RUN_LIVE_AGENT_TESTS=1 to enable them"


def preflight_subscription_auth(
    config: AgentContainerConfig,
    *,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> None:
    """Verify a provider login without exposing the status command's output."""
    provider = config.normalized_provider
    login_command = f"uv run personal-data-warehouse-agent-auth login {provider}"
    try:
        volume = runner(
            ["docker", "volume", "inspect", config.auth_volume],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise LocalTestStartupError(
            f"Could not inspect the subscription auth volume. Run `{login_command}` once, then retry."
        ) from error
    if int(getattr(volume, "returncode", 1)) != 0:
        raise LocalTestStartupError(
            f"Subscription auth volume {config.auth_volume!r} is missing. Run `{login_command}` once, then retry."
        )

    try:
        status = runner(
            auth_docker_command(provider=provider, action="status", config=config, interactive=False),
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise LocalTestStartupError(
            f"Could not verify {provider} subscription auth. Run `{login_command}` once, then retry."
        ) from error
    if int(getattr(status, "returncode", 1)) != 0:
        raise LocalTestStartupError(
            f"{provider} subscription auth is missing or invalid. Run `{login_command}` once, then retry."
        )


def callable_requires_postgres(
    function: Callable[..., object],
    *,
    include_root: bool = True,
) -> bool:
    """Whether a test/fixture reaches a helper that skips without the DB URL."""
    module_name = getattr(function, "__module__", "")
    seen: set[int] = set()

    def visit(candidate: object, *, is_root: bool = False) -> bool:
        if not inspect.isfunction(candidate) or id(candidate) in seen:
            return False
        seen.add(id(candidate))
        code = candidate.__code__
        reads_postgres_url = "environ" in code.co_names and any(
            _POSTGRES_URL_MARKER in value
            for value in code.co_consts
            if isinstance(value, str)
        )
        if (include_root or not is_root) and reads_postgres_url and "skip" in code.co_names:
            return True

        for name in code.co_names:
            referenced = candidate.__globals__.get(name)
            if inspect.isfunction(referenced) and (
                getattr(referenced, "__module__", "") == module_name
                or getattr(referenced, "__module__", "").startswith("tests.")
            ):
                if visit(referenced):
                    return True

        if candidate.__closure__:
            for cell in candidate.__closure__:
                try:
                    referenced = cell.cell_contents
                except ValueError:
                    continue
                if inspect.isfunction(referenced) and visit(referenced):
                    return True
        return False

    return visit(function, is_root=True)


class PostgresTestRuntime:
    """Own one disposable extension-complete Postgres for a pytest session."""

    def __init__(
        self,
        *,
        env: MutableMapping[str, str] = os.environ,
        runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
        which: Callable[[str], str | None] = shutil.which,
        name_factory: Callable[[], str] = lambda: f"pdw-tests-{uuid.uuid4().hex}",
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        readiness_timeout_seconds: float = 120,
        unit_only: bool = False,
    ) -> None:
        self._env = env
        self._runner = runner
        self._which = which
        self._name_factory = name_factory
        self._sleep = sleep
        self._monotonic = monotonic
        self._readiness_timeout_seconds = readiness_timeout_seconds
        self._unit_only = unit_only
        self._owned_container: str | None = None
        self._injected_url: str | None = None

    @property
    def owned_container(self) -> str | None:
        return self._owned_container

    def start(self) -> str | None:
        if self._unit_only:
            return None
        configured_url = self._env.get("POSTGRES_DATABASE_URL", "").strip()
        if configured_url:
            return configured_url
        if self._which("docker") is None:
            self._raise_startup_error("Docker CLI is not installed")

        daemon = self._run(["docker", "info"], timeout=30, failure="Docker daemon is unavailable")
        if int(getattr(daemon, "returncode", 1)) != 0:
            self._raise_startup_error("Docker daemon is unavailable")

        container = self._name_factory()
        self._owned_container = container
        image = self._env.get("PDW_POSTGRES_IMAGE", "").strip() or DEFAULT_POSTGRES_IMAGE
        try:
            return self._start_owned_container(container, image)
        except BaseException:
            self.close()
            raise

    def _start_owned_container(self, container: str, image: str) -> str:
        launched = self._run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                container,
                "--env",
                "POSTGRES_PASSWORD=postgres",
                "--env",
                "POSTGRES_DB=pdw",
                "--env",
                "PDW_PGBACKREST_ENABLED=false",
                "--publish",
                "127.0.0.1::5432",
                image,
                "postgres",
                "-c",
                "shared_preload_libraries=pg_textsearch",
            ],
            timeout=300,
            failure=f"could not start the warehouse Postgres image {image}",
        )
        if int(getattr(launched, "returncode", 1)) != 0:
            self._raise_startup_error(
                f"could not start the warehouse Postgres image {image}",
                include_logs=True,
            )

        published = self._run(
            ["docker", "port", container, "5432/tcp"],
            timeout=30,
            failure="could not resolve the disposable Postgres published port",
        )
        if int(getattr(published, "returncode", 1)) != 0:
            self._raise_startup_error(
                "could not resolve the disposable Postgres published port",
                include_logs=True,
            )
        port = self._parse_port(str(getattr(published, "stdout", "") or ""))
        if port is None:
            self._raise_startup_error(
                "Docker returned a malformed published port for disposable Postgres",
                include_logs=True,
            )

        self._wait_until_ready()
        self._install_extensions()

        url = f"postgresql://postgres:postgres@127.0.0.1:{port}/pdw"
        self._env["POSTGRES_DATABASE_URL"] = url
        self._injected_url = url
        return url

    def close(self) -> None:
        container = self._owned_container
        self._owned_container = None
        if container is not None:
            try:
                self._runner(
                    ["docker", "rm", "-f", container],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=30,
                )
            except (OSError, subprocess.SubprocessError):
                pass
        if self._injected_url is not None and self._env.get("POSTGRES_DATABASE_URL") == self._injected_url:
            self._env.pop("POSTGRES_DATABASE_URL", None)
        self._injected_url = None

    def _run(self, command: list[str], *, timeout: float, failure: str) -> subprocess.CompletedProcess:
        try:
            return self._runner(
                command,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout,
            )
        except (OSError, subprocess.SubprocessError) as error:
            self._raise_startup_error(failure, include_logs=self._owned_container is not None, cause=error)

    @staticmethod
    def _parse_port(output: str) -> int | None:
        lines = [line.strip() for line in output.splitlines() if line.strip()]
        if len(lines) != 1:
            return None
        match = re.fullmatch(r"127\.0\.0\.1:(\d{1,5})", lines[0])
        if match is None:
            return None
        port = int(match.group(1))
        return port if 1 <= port <= 65535 else None

    def _wait_until_ready(self) -> None:
        assert self._owned_container is not None
        deadline = self._monotonic() + self._readiness_timeout_seconds
        while True:
            ready = self._run(
                ["docker", "exec", self._owned_container, "pg_isready", "-U", "postgres", "-d", "pdw"],
                timeout=15,
                failure="could not check disposable Postgres readiness",
            )
            if int(getattr(ready, "returncode", 1)) == 0:
                initialized = self._run(
                    [
                        "docker",
                        "exec",
                        self._owned_container,
                        "psql",
                        "-U",
                        "postgres",
                        "-d",
                        "pdw",
                        "-Atqc",
                        "SELECT 1",
                    ],
                    timeout=15,
                    failure="could not check disposable Postgres database initialization",
                )
                if int(getattr(initialized, "returncode", 1)) == 0 and str(
                    getattr(initialized, "stdout", "") or ""
                ).strip() == "1":
                    return
            if self._monotonic() >= deadline:
                self._raise_startup_error(
                    "disposable Postgres did not become ready before the timeout",
                    include_logs=True,
                )
            self._sleep(2)

    def _install_extensions(self) -> None:
        assert self._owned_container is not None
        sql = " ".join(f"CREATE EXTENSION IF NOT EXISTS {extension};" for extension in sorted(POSTGRES_EXTENSIONS))
        installed = self._run(
            [
                "docker",
                "exec",
                self._owned_container,
                "psql",
                "-U",
                "postgres",
                "-d",
                "pdw",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                sql,
            ],
            timeout=120,
            failure="could not install the warehouse search extensions",
        )
        if int(getattr(installed, "returncode", 1)) != 0:
            self._raise_startup_error(
                "could not install the warehouse search extensions; verify PDW_POSTGRES_IMAGE",
                include_logs=True,
            )

        verified = self._run(
            [
                "docker",
                "exec",
                self._owned_container,
                "psql",
                "-U",
                "postgres",
                "-d",
                "pdw",
                "-At",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                (
                    "SELECT extname FROM pg_extension "
                    "WHERE extname IN ('vector', 'pg_trgm', 'pg_textsearch') "
                    "ORDER BY extname"
                ),
            ],
            timeout=30,
            failure="could not verify the warehouse search extensions",
        )
        found = {line.strip() for line in str(getattr(verified, "stdout", "") or "").splitlines() if line.strip()}
        missing = sorted(POSTGRES_EXTENSIONS - found)
        if int(getattr(verified, "returncode", 1)) != 0 or missing:
            missing_names = ", ".join(missing) or "verification failed"
            detail = (
                f"warehouse Postgres is missing search extensions: {missing_names}; "
                "verify PDW_POSTGRES_IMAGE"
            )
            self._raise_startup_error(detail, include_logs=True)

    def _container_logs(self) -> str:
        if self._owned_container is None:
            return ""
        try:
            completed = self._runner(
                ["docker", "logs", "--tail", "100", self._owned_container],
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
        except (OSError, subprocess.SubprocessError):
            return ""
        output = "\n".join(
            part.strip() for part in (getattr(completed, "stdout", ""), getattr(completed, "stderr", "")) if part
        )
        return output[-4000:]

    def _raise_startup_error(
        self,
        detail: str,
        *,
        include_logs: bool = False,
        cause: BaseException | None = None,
    ) -> None:
        logs = self._container_logs() if include_logs else ""
        self.close()
        message = (
            f"Local integration startup failed: {detail}. Start Docker and retry `uv run pytest`, "
            f"or explicitly run `{_UNIT_ONLY_COMMAND}`."
        )
        if logs:
            message += f"\nLast disposable Postgres logs (bounded):\n{logs}"
        error = LocalTestStartupError(message)
        if cause is None:
            raise error
        raise error from cause
