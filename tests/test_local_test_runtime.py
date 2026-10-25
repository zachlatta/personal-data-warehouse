from __future__ import annotations

import os
import subprocess

import pytest

from personal_data_warehouse.agent_runner import AgentContainerConfig
from tests.local_test_runtime import (
    DEFAULT_POSTGRES_IMAGE,
    auto_worker_count,
    OWNED_POSTGRES_URL_ENV,
    POSTGRES_EXTENSIONS,
    LocalTestStartupError,
    PostgresTestRuntime,
    callable_requires_postgres,
    direct_postgres_url,
    preflight_subscription_auth,
    schedule_key,
    subscription_tests_enabled,
    use_worker_database,
)


class FakeDocker:
    def __init__(
        self,
        *,
        daemon_returncode: int = 0,
        port_output: str = "127.0.0.1:49152\n",
        ready_after: int = 1,
        extension_returncode: int = 0,
        extensions_output: str = "pg_textsearch\npg_trgm\nvector\n",
    ) -> None:
        self.calls: list[list[str]] = []
        self.daemon_returncode = daemon_returncode
        self.port_output = port_output
        self.ready_after = ready_after
        self.extension_returncode = extension_returncode
        self.extensions_output = extensions_output
        self.ready_calls = 0

    def __call__(self, command, **kwargs):
        command = list(command)
        self.calls.append(command)
        if command == ["docker", "info"]:
            return subprocess.CompletedProcess(command, self.daemon_returncode, stdout="", stderr="daemon unavailable")
        if command[:2] == ["docker", "run"]:
            return subprocess.CompletedProcess(command, 0, stdout="container-id\n", stderr="")
        if command[:2] == ["docker", "port"]:
            return subprocess.CompletedProcess(command, 0, stdout=self.port_output, stderr="")
        if "pg_isready" in command:
            self.ready_calls += 1
            code = 0 if self.ready_calls >= self.ready_after else 1
            return subprocess.CompletedProcess(command, code, stdout="", stderr="")
        if "SELECT 1" in " ".join(command):
            return subprocess.CompletedProcess(command, 0, stdout="1\n", stderr="")
        if "CREATE EXTENSION" in " ".join(command):
            return subprocess.CompletedProcess(command, self.extension_returncode, stdout="", stderr="extension failed")
        if "SELECT extname" in " ".join(command):
            return subprocess.CompletedProcess(command, 0, stdout=self.extensions_output, stderr="")
        if command[:3] == ["docker", "logs", "--tail"]:
            return subprocess.CompletedProcess(command, 0, stdout="bounded postgres logs\n", stderr="")
        if command[:3] == ["docker", "rm", "-f"]:
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected command: {command}")


def runtime(env: dict[str, str], docker: FakeDocker, **kwargs) -> PostgresTestRuntime:
    return PostgresTestRuntime(
        env=env,
        runner=docker,
        which=lambda executable: f"/usr/local/bin/{executable}",
        name_factory=lambda: "pdw-tests-owned-123",
        sleep=lambda _seconds: None,
        **kwargs,
    )


def test_existing_database_url_is_preserved_without_docker() -> None:
    env = {"POSTGRES_DATABASE_URL": "postgresql://configured.example/pdw"}
    docker = FakeDocker()
    postgres = runtime(env, docker)

    assert postgres.start() == env["POSTGRES_DATABASE_URL"]
    postgres.close()

    assert docker.calls == []
    assert env["POSTGRES_DATABASE_URL"] == "postgresql://configured.example/pdw"


def test_default_start_launches_extension_complete_postgres_on_a_dynamic_port() -> None:
    env: dict[str, str] = {}
    docker = FakeDocker(ready_after=2)
    postgres = runtime(env, docker)

    assert postgres.start() == "postgresql://postgres:postgres@127.0.0.1:49152/pdw"
    assert env["POSTGRES_DATABASE_URL"] == "postgresql://postgres:postgres@127.0.0.1:49152/pdw"

    run = next(command for command in docker.calls if command[:2] == ["docker", "run"])
    assert run == [
        "docker",
        "run",
        "-d",
        "--name",
        "pdw-tests-owned-123",
        "--env",
        "POSTGRES_PASSWORD=postgres",
        "--env",
        "POSTGRES_DB=pdw",
        "--env",
        "PDW_PGBACKREST_ENABLED=false",
        "--publish",
        "127.0.0.1::5432",
        DEFAULT_POSTGRES_IMAGE,
        "postgres",
        "-c",
        "shared_preload_libraries=pg_textsearch",
        # The database is created and destroyed per run, so durability buys
        # nothing; on Docker-for-Mac each fsync is a virtual block device round
        # trip (~1.4 ms measured 2026-09-20), and these three flags took the same
        # 167 DB-heavy tests from 352s to 190s.
        "-c",
        "fsync=off",
        "-c",
        "synchronous_commit=off",
        "-c",
        "full_page_writes=off",
    ]
    assert ["docker", "port", "pdw-tests-owned-123", "5432/tcp"] in docker.calls
    assert docker.ready_calls == 2
    assert any("SELECT 1" in " ".join(command) for command in docker.calls)
    assert any("CREATE EXTENSION IF NOT EXISTS vector" in " ".join(command) for command in docker.calls)
    assert any("SELECT extname" in " ".join(command) for command in docker.calls)

    postgres.close()

    assert docker.calls[-1] == ["docker", "rm", "-f", "-v", "pdw-tests-owned-123"]
    assert "POSTGRES_DATABASE_URL" not in env


def test_postgres_image_can_be_overridden() -> None:
    env = {"PDW_POSTGRES_IMAGE": "registry.example/pdw-postgres:test"}
    docker = FakeDocker()
    postgres = runtime(env, docker)

    postgres.start()

    run = next(command for command in docker.calls if command[:2] == ["docker", "run"])
    assert "registry.example/pdw-postgres:test" in run
    postgres.close()


def test_unit_only_is_the_explicit_no_docker_path() -> None:
    env: dict[str, str] = {}
    docker = FakeDocker()
    postgres = runtime(env, docker, unit_only=True)

    assert postgres.start() is None
    postgres.close()

    assert docker.calls == []
    assert "POSTGRES_DATABASE_URL" not in env


def test_missing_docker_fails_with_the_recovery_command() -> None:
    postgres = PostgresTestRuntime(
        env={},
        runner=FakeDocker(),
        which=lambda _executable: None,
        name_factory=lambda: "pdw-tests-owned-123",
    )

    with pytest.raises(LocalTestStartupError, match=r"Docker CLI.*uv run pytest --unit-only"):
        postgres.start()


def test_unavailable_docker_daemon_fails_with_the_recovery_command() -> None:
    postgres = runtime({}, FakeDocker(daemon_returncode=1))

    with pytest.raises(LocalTestStartupError, match=r"Docker daemon.*uv run pytest --unit-only"):
        postgres.start()


def test_readiness_timeout_removes_the_owned_container_and_reports_bounded_logs() -> None:
    docker = FakeDocker(ready_after=999)
    postgres = runtime({}, docker, readiness_timeout_seconds=0)

    with pytest.raises(LocalTestStartupError, match=r"(?s)did not become ready.*bounded postgres logs"):
        postgres.start()

    assert docker.calls[-1] == ["docker", "rm", "-f", "-v", "pdw-tests-owned-123"]


def test_malformed_dynamic_port_removes_the_exact_owned_container() -> None:
    docker = FakeDocker(port_output="not-a-published-port\n")
    postgres = runtime({}, docker)

    with pytest.raises(LocalTestStartupError, match=r"published port.*uv run pytest --unit-only"):
        postgres.start()

    assert docker.calls[-1] == ["docker", "rm", "-f", "-v", "pdw-tests-owned-123"]


def test_unexpected_interrupt_during_startup_removes_the_exact_owned_container() -> None:
    docker = FakeDocker()

    def interrupted_run(command, **kwargs):
        if list(command)[:2] == ["docker", "port"]:
            raise KeyboardInterrupt
        return docker(command, **kwargs)

    postgres = PostgresTestRuntime(
        env={},
        runner=interrupted_run,
        which=lambda executable: f"/usr/local/bin/{executable}",
        name_factory=lambda: "pdw-tests-owned-123",
    )

    with pytest.raises(KeyboardInterrupt):
        postgres.start()

    assert docker.calls[-1] == ["docker", "rm", "-f", "-v", "pdw-tests-owned-123"]


def test_extension_creation_failure_is_actionable_and_cleans_up() -> None:
    docker = FakeDocker(extension_returncode=1)
    postgres = runtime({}, docker)

    with pytest.raises(LocalTestStartupError, match=r"search extensions.*PDW_POSTGRES_IMAGE"):
        postgres.start()

    assert docker.calls[-1] == ["docker", "rm", "-f", "-v", "pdw-tests-owned-123"]


def test_extension_verification_requires_every_extension() -> None:
    docker = FakeDocker(extensions_output="pg_trgm\nvector\n")
    postgres = runtime({}, docker)

    with pytest.raises(LocalTestStartupError, match=r"pg_textsearch"):
        postgres.start()

    assert docker.calls[-1] == ["docker", "rm", "-f", "-v", "pdw-tests-owned-123"]


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({}, True),
        ({"CI": "true"}, False),
        ({"CI": "true", "RUN_LIVE_AGENT_TESTS": "1"}, True),
        ({"RUN_LIVE_AGENT_TESTS": "1"}, True),
        ({"RUN_LIVE_AGENT_TESTS": "0"}, False),
    ],
)
def test_subscription_selection_is_explicit_and_local_first(env, expected) -> None:
    assert subscription_tests_enabled(env) is expected


@pytest.mark.parametrize("value", ["", "true", "false", "yes", "2"])
def test_subscription_selection_rejects_ambiguous_values(value) -> None:
    with pytest.raises(LocalTestStartupError, match=r"RUN_LIVE_AGENT_TESTS must be 0 or 1"):
        subscription_tests_enabled({"RUN_LIVE_AGENT_TESTS": value})


def test_subscription_auth_preflight_uses_provider_status_without_leaking_output() -> None:
    calls = []

    def fake_run(command, **kwargs):
        calls.append(list(command))
        return subprocess.CompletedProcess(command, 0, stdout="secret status output", stderr="another secret")

    config = AgentContainerConfig(image="pdw-agent:test", provider="codex", auth_volume="pdw-agent-auth")

    preflight_subscription_auth(config, runner=fake_run)

    assert calls[0] == ["docker", "volume", "inspect", "pdw-agent-auth"]
    assert "codex login status" in calls[1][-1]


def test_subscription_auth_preflight_missing_or_invalid_volume_gives_exact_login_command() -> None:
    def missing_volume(command, **kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="credential contents", stderr="credential contents")

    config = AgentContainerConfig(image="pdw-agent:test", provider="claude", auth_volume="missing")

    with pytest.raises(LocalTestStartupError) as exc_info:
        preflight_subscription_auth(config, runner=missing_volume)

    message = str(exc_info.value)
    assert "uv run personal-data-warehouse-agent-auth login claude" in message
    assert "credential contents" not in message


def test_subscription_auth_preflight_invalid_status_gives_exact_login_command() -> None:
    calls = 0

    def invalid_status(command, **kwargs):
        nonlocal calls
        calls += 1
        code = 0 if calls == 1 else 1
        return subprocess.CompletedProcess(command, code, stdout="credential contents", stderr="credential contents")

    config = AgentContainerConfig(image="pdw-agent:test", provider="codex", auth_volume="pdw-agent-auth")

    with pytest.raises(LocalTestStartupError) as exc_info:
        preflight_subscription_auth(config, runner=invalid_status)

    message = str(exc_info.value)
    assert "uv run personal-data-warehouse-agent-auth login codex" in message
    assert "credential contents" not in message


def test_callable_postgres_detection_follows_same_module_helpers() -> None:
    def database_url():
        url = os.environ.get("POSTGRES_DATABASE_URL")
        if not url:
            pytest.skip("POSTGRES_DATABASE_URL is not set")
        return url

    def integration_test():
        return database_url()

    def unit_test():
        return "unit"

    assert callable_requires_postgres(integration_test)
    assert not callable_requires_postgres(unit_test)


def test_callable_postgres_detection_does_not_classify_a_pure_env_policy_test() -> None:
    def pure_environment_policy_test():
        url = os.environ.get("POSTGRES_DATABASE_URL")
        if not url:
            pytest.skip("POSTGRES_DATABASE_URL is not set")
        return url

    assert not callable_requires_postgres(pure_environment_policy_test, include_root=False)


def _owned_postgres_url() -> str:
    url = os.environ.get("POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("POSTGRES_DATABASE_URL is not set")
    return url


def test_a_worker_gets_its_own_extension_complete_database_in_the_owned_cluster() -> None:
    import psycopg2

    url = _owned_postgres_url()
    env = {"POSTGRES_DATABASE_URL": url, OWNED_POSTGRES_URL_ENV: url}

    use_worker_database(env, "gw999")

    worker_url = env["POSTGRES_DATABASE_URL"]
    assert worker_url.rsplit("/", 1) == [url.rsplit("/", 1)[0], "pdw_gw999"]
    connection = psycopg2.connect(worker_url)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            assert cursor.fetchone() == ("pdw_gw999",)
            cursor.execute("SELECT extname FROM pg_extension")
            assert POSTGRES_EXTENSIONS <= {row[0] for row in cursor.fetchall()}
    finally:
        connection.close()
    admin = psycopg2.connect(url)
    admin.autocommit = True
    try:
        with admin.cursor() as cursor:
            cursor.execute("DROP DATABASE pdw_gw999")
    finally:
        admin.close()


def test_a_configured_database_is_never_split_per_worker() -> None:
    env = {"POSTGRES_DATABASE_URL": "postgresql://configured.example/pdw"}

    use_worker_database(env, "gw0")

    assert env == {"POSTGRES_DATABASE_URL": "postgresql://configured.example/pdw"}


def test_the_controller_and_serial_runs_keep_the_owned_database() -> None:
    env = {"POSTGRES_DATABASE_URL": "postgresql://h/pdw", OWNED_POSTGRES_URL_ENV: "postgresql://h/pdw"}

    use_worker_database(env, None)

    assert env["POSTGRES_DATABASE_URL"] == "postgresql://h/pdw"


def test_an_unexpected_worker_id_is_refused() -> None:
    env = {"POSTGRES_DATABASE_URL": "postgresql://h/pdw", OWNED_POSTGRES_URL_ENV: "postgresql://h/pdw"}

    with pytest.raises(LocalTestStartupError, match="worker id"):
        use_worker_database(env, "gw0; DROP")


def test_auto_workers_oversubscribe_only_the_owned_per_worker_databases() -> None:
    assert auto_worker_count({}, cpus=16) == 24
    assert auto_worker_count({}, cpus=1) == 2
    # A configured database is shared by every worker, where extra workers
    # contend instead of filling idle CPU.
    assert auto_worker_count({"POSTGRES_DATABASE_URL": "postgresql://ci/pdw"}, cpus=4) == 4


def test_workers_bypass_the_userland_port_proxy_when_the_container_ip_answers() -> None:
    runner = FakeDocker()
    seen: list[tuple[str, int]] = []

    def inspect(command, **kwargs):
        if command[:2] == ["docker", "inspect"]:
            return subprocess.CompletedProcess(command, 0, stdout="172.17.0.9\n", stderr="")
        return runner(command, **kwargs)

    def reachable(address, timeout):
        seen.append(address)
        return True

    url = direct_postgres_url(
        "postgresql://postgres:postgres@127.0.0.1:49152/pdw",
        "pdw-tests-owned-123",
        runner=inspect,
        probe=reachable,
    )

    assert url == "postgresql://postgres:postgres@172.17.0.9:5432/pdw"
    assert seen == [("172.17.0.9", 5432)]


def test_the_published_port_stays_when_the_container_ip_is_unreachable() -> None:
    # Docker Desktop on macOS runs containers in a VM whose bridge IPs the host
    # cannot route to; the published port is the only way in there.
    def inspect(command, **kwargs):
        return subprocess.CompletedProcess(command, 0, stdout="172.17.0.9\n", stderr="")

    def unreachable(address, timeout):
        raise OSError("no route to host")

    url = "postgresql://postgres:postgres@127.0.0.1:49152/pdw"
    assert direct_postgres_url(url, "c", runner=inspect, probe=unreachable) == url


def test_the_published_port_stays_when_docker_reports_no_ip() -> None:
    def inspect(command, **kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="no such container")

    url = "postgresql://postgres:postgres@127.0.0.1:49152/pdw"
    assert direct_postgres_url(url, "c", runner=inspect, probe=lambda *_: True) == url


def test_tests_are_scheduled_longest_first_from_the_last_runs_durations() -> None:
    durations = {"a::slow": 20.0, "a::medium": 3.0, "b::fast": 0.01}

    order = sorted(
        ["b::fast", "b::new_unit", "a::medium", "c::new_db", "a::slow"],
        key=lambda nodeid: schedule_key(
            nodeid, requires_postgres=nodeid.endswith("_db"), durations=durations
        ),
    )

    # Measured tests longest first; a test with no history is assumed to cost
    # what a typical Postgres test or unit test does.
    assert order == ["a::slow", "a::medium", "c::new_db", "b::fast", "b::new_unit"]
