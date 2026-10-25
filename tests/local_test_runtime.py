"""Local pytest integration bootstrap helpers.

This module lives under ``tests`` deliberately: it provisions disposable test
infrastructure and must never become part of the warehouse runtime.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
from datetime import UTC, datetime
import functools
import inspect
import os
import re
import shutil
import socket
import subprocess
import time
from urllib.parse import urlsplit, urlunsplit
import uuid

import psycopg2
import psycopg2.sql

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


# Set by the controller to the URL of the Postgres it launched, so a worker can
# tell its own disposable cluster from a configured database it must not touch.
OWNED_POSTGRES_URL_ENV = "PDW_TEST_OWNED_POSTGRES_URL"


# Workers per CPU when each worker gets its own database. A worker spends much
# of its time waiting on its Postgres backend, so one per CPU left 20-30% of
# mew-dev idle; 1.5 beat 1 and 2 on 2026-10-25. More workers than that need
# more Postgres clusters, and each cluster's data churns through the disk.
WORKERS_PER_CPU = 1.5


# What one worker costs while the suite runs: its Python process (~150 MB after
# importing every test module) plus its Postgres backend (~30 MB), with margin.
WORKER_BYTES = 200 * 1024**2


def auto_worker_count(
    env: Mapping[str, str],
    *,
    cpus: int,
    busy_cpus: float = 0.0,
    available_bytes: int | None = None,
) -> int:
    """What `-n auto` means on this host right now.

    Many agents run the suite on one machine at once. A run sizes itself to the
    CPUs the others leave idle (more workers than that add RAM and contention,
    not speed) and takes at most half of the memory still available.
    Oversubscribe only when workers get their own databases.
    """
    idle = max(1.0, cpus - busy_cpus)
    per_cpu = 1.0 if env.get("POSTGRES_DATABASE_URL", "").strip() else WORKERS_PER_CPU
    workers = max(1, round(idle * per_cpu))
    if available_bytes is not None:
        workers = min(workers, max(1, available_bytes // (2 * WORKER_BYTES)))
    return workers


def busy_cpus_now(sample_seconds: float = 0.25) -> float:
    """CPUs busy over a short sample of /proc/stat; 0 where it is unavailable.

    Not the load average: that decays over a minute, so a run started right
    after the last one finished would mistake its predecessor for a neighbour.
    """
    def snapshot() -> tuple[int, int] | None:
        try:
            with open("/proc/stat") as stat:
                fields = [int(value) for value in stat.readline().split()[1:9]]
        except (OSError, ValueError):
            return None
        return fields[3] + fields[4], sum(fields)

    before = snapshot()
    if before is None:
        return 0.0
    time.sleep(sample_seconds)
    after = snapshot()
    if after is None or after[1] <= before[1]:
        return 0.0
    idle_share = (after[0] - before[0]) / (after[1] - before[1])
    return max(0.0, (1.0 - idle_share) * (os.cpu_count() or 1))


def available_memory_bytes() -> int | None:
    """MemAvailable from /proc/meminfo, None where it is unavailable."""
    try:
        with open("/proc/meminfo") as meminfo:
            for line in meminfo:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError):
        return None
    return None


# What a test with no recorded duration is assumed to cost: about what an
# average Postgres-backed test and an average unit test took on 2026-10-25.
_UNMEASURED_POSTGRES_TEST_SECONDS = 1.0
_UNMEASURED_UNIT_TEST_SECONDS = 0.005


def schedule_key(nodeid: str, *, requires_postgres: bool, durations: Mapping[str, float]) -> float:
    """Sort key that hands xdist the longest tests first (longest-processing-time).

    Started late, one 20 s test kept a single worker busy while the other 23
    sat idle for the last ~10 s of a 74 s run. Durations come from the previous
    run, so a test with no history gets a typical cost for its kind.
    """
    default = _UNMEASURED_POSTGRES_TEST_SECONDS if requires_postgres else _UNMEASURED_UNIT_TEST_SECONDS
    return -float(durations.get(nodeid, default))


def direct_postgres_url(
    url: str,
    container: str,
    *,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
    probe: Callable[..., object] = socket.create_connection,
) -> str:
    """The URL of the container's own address, when the host can reach it.

    The published 127.0.0.1 port goes through docker-proxy, a userspace copy
    that adds two process wakeups to every query: removing it took ~6% off a
    full run on mew-dev (2026-10-25). Docker Desktop cannot route to bridge
    IPs, so anything but a working direct connection keeps the given URL.
    """
    try:
        inspected = runner(
            ["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return url
    address = str(getattr(inspected, "stdout", "") or "").strip()
    if int(getattr(inspected, "returncode", 1)) != 0 or not re.fullmatch(r"\d{1,3}(\.\d{1,3}){3}", address):
        return url
    try:
        connection = probe((address, 5432), timeout=1)
    except OSError:
        return url
    close = getattr(connection, "close", None)
    if callable(close):
        close()
    parts = urlsplit(url)
    credentials = parts.netloc.rpartition("@")[0]
    netloc = f"{credentials}@{address}:5432" if credentials else f"{address}:5432"
    return urlunsplit(parts._replace(netloc=netloc))


def use_worker_database(env: MutableMapping[str, str], worker: str | None) -> bool:
    """Point an xdist worker at its own database in the disposable cluster.

    Two costs scale with how many workers share one database, and on
    2026-10-25 (mew-dev, 16 vCPUs) they were the suite's limit. Every DDL
    statement broadcasts catalog invalidations that each backend in the same
    database must replay, so workers creating and dropping schemas taxed each
    other's CPU: more workers made the suite slower. And advisory locks are
    per database, so the warehouse's serialised ensure_* steps queued every
    worker behind the others. A database per worker removes both, and 24
    workers became faster than 16 instead of slower. A configured
    POSTGRES_DATABASE_URL is never split; neither is a serial run.
    """
    url = env.get("POSTGRES_DATABASE_URL", "")
    if worker is None or not url or env.get(OWNED_POSTGRES_URL_ENV) != url:
        return False
    if not re.fullmatch(r"gw\d+", worker):
        raise LocalTestStartupError(f"unexpected xdist worker id {worker!r}")
    database = f"pdw_{worker}"
    worker_url = urlunsplit(urlsplit(url)._replace(path=f"/{database}"))
    try:
        admin = psycopg2.connect(url)
        admin.autocommit = True
        try:
            with admin.cursor() as cursor:
                name = psycopg2.sql.Identifier(database)
                cursor.execute(psycopg2.sql.SQL("DROP DATABASE IF EXISTS {}").format(name))
                cursor.execute(psycopg2.sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(name))
        finally:
            admin.close()
        connection = psycopg2.connect(worker_url)
        connection.autocommit = True
        try:
            with connection.cursor() as cursor:
                for extension in sorted(POSTGRES_EXTENSIONS):
                    cursor.execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")
                # The warehouse's big views carry cost estimates that trip JIT
                # even over empty test tables: one marts probe spent 275 ms
                # compiling for a query that returned no rows. Tests hold a
                # few rows, so JIT can only cost time. Set on the role, not the
                # database: a database cloned from a template does not inherit
                # the template's settings.
                cursor.execute("ALTER ROLE CURRENT_USER SET jit = off")
        finally:
            connection.close()
    except psycopg2.Error as error:
        raise LocalTestStartupError(
            f"Local integration startup failed: could not create worker database {database}: {error}. "
            f"Retry `uv run pytest`, or explicitly run `{_UNIT_ONLY_COMMAND}`."
        ) from error
    env["POSTGRES_DATABASE_URL"] = worker_url
    return True


# --- one database per Postgres test ------------------------------------------
#
# Building a test warehouse is ~2,100 statements (1.2 s) and dropping its 64
# schemas another ~0.3 s; a full run built ~680 of them, ~70% of its database
# time (mew-dev, 2026-10-25). Cloning a database from a template takes 10-60 ms
# and dropping one 5-30 ms, so in the run's own cluster each Postgres test gets
# a fresh database cloned from a per-worker template, and a test whose helper
# provisions the whole warehouse starts from a clone of a template that same
# helper built once. Contracts are unchanged: a test still sees exactly the
# state its own code would have built, and nothing it creates outlives it.

_PROVISIONS_TEMPLATE = "_pdw_provisions_template"
# WAL_LOG copies the template block by block through WAL; FILE_COPY would
# force a cluster-wide checkpoint for every test.
CLONE_STRATEGY = "WAL_LOG"


@dataclass
class ActiveDatabase:
    """The disposable database the current test runs against."""

    name: str
    provisioner: Callable[..., object] | None
    namespace: str
    namespace_taken: bool = False
    provisioned: bool = False


_active: ActiveDatabase | None = None


def active_database() -> ActiveDatabase | None:
    return _active


def set_active_database(database: ActiveDatabase | None) -> None:
    global _active
    _active = database


def warehouse_database_name(warehouse: object) -> str | None:
    info = getattr(getattr(warehouse, "_connection", None), "info", None)
    return getattr(info, "dbname", None)


def provisions_template(function: Callable[..., object]) -> Callable[..., object]:
    """Mark a helper that provisions a whole test warehouse.

    A test that calls it starts from a database cloned from a template the
    helper itself built, in the namespace make_test_schema() hands that test
    first, so the helper's first call on that warehouse has nothing left to do.
    Every other call (another warehouse, a second call, no template) runs it.
    """

    @functools.wraps(function)
    def provision(warehouse, *args, **kwargs):
        active = _active
        if (
            active is not None
            and active.provisioner is provision
            and not active.provisioned
            and getattr(warehouse, "schema_namespace", None) == active.namespace
            and warehouse_database_name(warehouse) == active.name
        ):
            active.provisioned = True
            return None
        return function(warehouse, *args, **kwargs)

    setattr(provision, _PROVISIONS_TEMPLATE, True)
    return provision


def template_provisioner(function: Callable[..., object]) -> Callable[..., object] | None:
    """The @provisions_template helper a test or fixture calls, if exactly one."""
    found: set[Callable[..., object]] = set()
    seen: set[int] = set()

    def visit(candidate: object) -> None:
        if not inspect.isfunction(candidate) or id(candidate) in seen:
            return
        seen.add(id(candidate))
        if getattr(candidate, _PROVISIONS_TEMPLATE, False):
            found.add(candidate)
            return
        for name in candidate.__code__.co_names:
            referenced = candidate.__globals__.get(name)
            if inspect.isfunction(referenced) and (
                getattr(referenced, _PROVISIONS_TEMPLATE, False)
                or getattr(referenced, "__module__", "").startswith("tests.")
                or getattr(referenced, "__module__", "") == getattr(function, "__module__", "")
            ):
                visit(referenced)
        for cell in candidate.__closure__ or ():
            try:
                visit(cell.cell_contents)
            except ValueError:
                continue

    visit(function)
    return next(iter(found)) if len(found) == 1 else None


class DisposableDatabases:
    """A worker's templates and per-test clones in the run's own cluster."""

    def __init__(self, admin, admin_url: str, worker: str, namespace: str) -> None:
        self._admin = admin
        self._admin_url = admin_url
        self._worker = worker
        self.namespace = namespace
        self.base_template = f"pdw_{worker}_template"
        self._full_templates: dict[int, str] = {}
        self._clones = 0

    @classmethod
    def create(cls, admin_url: str, *, worker: str) -> "DisposableDatabases":
        if not re.fullmatch(r"gw\d+", worker):
            raise LocalTestStartupError(f"unexpected xdist worker id {worker!r}")
        stamp = datetime.now(tz=UTC).strftime("%Y%m%d%H%M%S")
        try:
            admin = psycopg2.connect(admin_url)
            admin.autocommit = True
            databases = cls(admin, admin_url, worker, f"pdw_test_{stamp}_{uuid.uuid4().hex}")
            databases._drop_matching(f"pdw_{worker}_")
            databases._execute("CREATE DATABASE {} TEMPLATE template0", databases.base_template)
            connection = psycopg2.connect(databases._url(databases.base_template))
            connection.autocommit = True
            try:
                with connection.cursor() as cursor:
                    for extension in sorted(POSTGRES_EXTENSIONS):
                        cursor.execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")
            finally:
                connection.close()
        except psycopg2.Error as error:
            raise LocalTestStartupError(
                f"Local integration startup failed: could not create {worker}'s template database: {error}. "
                f"Retry `uv run pytest`, or explicitly run `{_UNIT_ONLY_COMMAND}`."
            ) from error
        return databases

    def template_for(self, provisioner: Callable[..., object] | None) -> str:
        """The base template, or the one ``provisioner`` built (built on first use)."""
        if provisioner is None:
            return self.base_template
        key = id(provisioner)
        if key not in self._full_templates:
            from personal_data_warehouse.postgres import PostgresWarehouse

            name = f"pdw_{self._worker}_full{len(self._full_templates)}"
            self._execute("CREATE DATABASE {} TEMPLATE {}", name, self.base_template)
            warehouse = PostgresWarehouse(self._url(name), schema=self.namespace)
            try:
                getattr(provisioner, "__wrapped__", provisioner)(warehouse)
            finally:
                warehouse.close()
            self._full_templates[key] = name
        return self._full_templates[key]

    def clone(self, template: str) -> str:
        self._clones += 1
        name = f"pdw_{self._worker}_c{self._clones}"
        self._execute(f"CREATE DATABASE {{}} TEMPLATE {{}} STRATEGY {CLONE_STRATEGY}", name, template)
        return self._url(name)

    def drop(self, url: str) -> None:
        self._execute("DROP DATABASE IF EXISTS {} WITH (FORCE)", urlsplit(url).path.lstrip("/"))

    def existing(self) -> list[str]:
        """This worker's per-test clones still present."""
        with self._admin.cursor() as cursor:
            cursor.execute(
                "SELECT datname FROM pg_database WHERE starts_with(datname, %s) ORDER BY datname",
                (f"pdw_{self._worker}_c",),
            )
            return [row[0] for row in cursor.fetchall()]

    def close(self) -> None:
        try:
            self._drop_matching(f"pdw_{self._worker}_")
        finally:
            self._admin.close()

    def _drop_matching(self, prefix: str) -> None:
        with self._admin.cursor() as cursor:
            cursor.execute("SELECT datname FROM pg_database WHERE starts_with(datname, %s)", (prefix,))
            names = [row[0] for row in cursor.fetchall()]
        for name in names:
            self._execute("DROP DATABASE IF EXISTS {} WITH (FORCE)", name)

    def _execute(self, statement: str, *names: str) -> None:
        with self._admin.cursor() as cursor:
            cursor.execute(psycopg2.sql.SQL(statement).format(*map(psycopg2.sql.Identifier, names)))

    def _url(self, database: str) -> str:
        return urlunsplit(urlsplit(self._admin_url)._replace(path=f"/{database}"))


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
                # Disposable database: durability buys nothing, and on
                # Docker-for-Mac every fsync is a virtual block device round
                # trip (~1.4 ms measured 2026-09-20). Measured on the same 167
                # DB-heavy tests: 352s with fsync on, 190s with these off.
                "-c",
                "fsync=off",
                "-c",
                "synchronous_commit=off",
                "-c",
                "full_page_writes=off",
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
                # -v also removes the anonymous volume the image declares for
                # /var/lib/postgresql; without it every run leaks ~0.5 GB.
                self._runner(
                    ["docker", "rm", "-f", "-v", container],
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
