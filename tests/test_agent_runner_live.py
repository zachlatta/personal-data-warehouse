from __future__ import annotations

import json
import os
import shutil
import subprocess

import pytest

from personal_data_warehouse.agent_runner import (
    DEFAULT_AGENT_MODEL,
    DEFAULT_AGENT_REASONING_EFFORT,
    AgentContainerConfig,
    AgentRunRequest,
    ContainerAgentRunner,
    ensure_agent_image,
    write_builtin_cli_tools,
)
from personal_data_warehouse.agent_tool_proxy import WarehouseAppConfig, run_agent_tool_proxy
from tests.local_test_runtime import LocalTestStartupError, preflight_subscription_auth
from tests.warehouse_app_stub import StubWarehouseApp


pytestmark = pytest.mark.local_integration


def live_agent_config(tmp_path) -> AgentContainerConfig:
    image = ensure_agent_image()
    return AgentContainerConfig(
        image=image,
        provider=os.getenv("AGENT_PROVIDER", "codex"),
        model=os.getenv("AGENT_MODEL", DEFAULT_AGENT_MODEL),
        reasoning_effort=os.getenv("AGENT_REASONING_EFFORT", DEFAULT_AGENT_REASONING_EFFORT),
        auth_volume=os.getenv("AGENT_AUTH_VOLUME", "pdw-agent-auth"),
        runs_volume=os.getenv("AGENT_RUNS_VOLUME", "pdw-agent-runs"),
        runs_dir=tmp_path,
        timeout_seconds=int(os.getenv("AGENT_TIMEOUT_SECONDS", "1800")),
    )


def require_docker() -> None:
    if not shutil.which("docker"):
        pytest.fail(
            "Docker CLI is required for local agent integration tests; "
            "install/start Docker or explicitly use `uv run pytest --unit-only`",
            pytrace=False,
        )
    try:
        completed = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        completed = None
    if completed is None or completed.returncode != 0:
        pytest.fail(
            "Docker daemon is required for local agent integration tests; "
            "start Docker or explicitly use `uv run pytest --unit-only`",
            pytrace=False,
        )


@pytest.fixture(scope="module")
def subscription_agent_config(tmp_path_factory):
    require_docker()
    config = live_agent_config(tmp_path_factory.mktemp("subscription-agent"))
    try:
        preflight_subscription_auth(config)
    except LocalTestStartupError as error:
        pytest.fail(str(error), pytrace=False)
    return config


def test_live_agent_image_has_clis_and_no_socket(tmp_path) -> None:
    require_docker()
    config = live_agent_config(tmp_path)

    completed = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--mount",
            f"type=volume,src={config.auth_volume},dst=/agent-auth",
            config.image,
            "sh",
            "-lc",
            "command -v codex && command -v claude && test ! -S /var/run/docker.sock && test -d /agent-auth",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )

    assert completed.returncode == 0, completed.stderr


def test_live_agent_builtin_cli_tools_are_available_on_path(tmp_path) -> None:
    require_docker()
    config = live_agent_config(tmp_path)
    write_builtin_cli_tools(tmp_path)
    (tmp_path / "candidate.json").write_text('{"ok":true,"message":"tool smoke"}', encoding="utf-8")
    (tmp_path / "schema.json").write_text(
        json.dumps(
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "ok": {"type": "boolean"},
                    "message": {"type": "string"},
                },
                "required": ["ok", "message"],
            }
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--mount",
            f"type=bind,src={tmp_path.resolve()},dst=/run",
            "--env",
            "AGENT_TOOL_MANIFEST_PATH=/run/TOOLS.md",
            config.image,
            "sh",
            "-lc",
            (
                "export PATH=/run/tools:$PATH"
                " && command -v pdw-validate-json"
                " && pdw-tool-help | grep pdw-validate-json"
                " && pdw-validate-json /run/candidate.json /run/schema.json"
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )

    assert completed.returncode == 0, completed.stderr
    assert "pdw-validate-json" in completed.stdout
    assert "ok" in completed.stdout


def test_live_agent_pdw_cli_reaches_the_warehouse_through_the_host_proxy(tmp_path) -> None:
    """The image's real `pdw` must work against the per-run proxy end to end.

    Everything here is exercised for real except the app itself, which is a
    local stub: the container's binary, its env-only auth, the proxy hop, and
    the container->host network path that Docker networking gets wrong most
    often.
    """

    require_docker()
    config = live_agent_config(tmp_path)

    app = StubWarehouseApp(
        responses={"sql": {"rows": "answer\n42", "total_rows": 1, "format": "csv"}},
        tools=[{"name": "sql", "title": "Run SQL", "description": "", "input_schema": {}}],
    )
    try:
        with run_agent_tool_proxy(app=WarehouseAppConfig(base_url=app.base_url, token="live-test-token")) as env:
            completed = subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--add-host",
                    "host.docker.internal:host-gateway",
                    "--env",
                    f"PDW_API_URL={env['PDW_API_URL']}",
                    "--env",
                    f"PDW_SECRET_TOKEN={env['PDW_SECRET_TOKEN']}",
                    "--env",
                    f"PDW_CLIENT_NAME={env['PDW_CLIENT_NAME']}",
                    "--env",
                    f"PDW_NO_AUTO_UPDATE={env['PDW_NO_AUTO_UPDATE']}",
                    config.image,
                    "sh",
                    "-lc",
                    "command -v pdw && pdw sql -q 'live smoke' --output csv 'SELECT 42 AS answer'",
                ],
                capture_output=True,
                text=True,
                check=False,
                timeout=120,
            )
    finally:
        app.close()

    assert completed.returncode == 0, completed.stderr
    assert "/usr/local/bin/pdw" in completed.stdout
    assert "answer\n42" in completed.stdout


def test_live_agent_pdw_cli_cannot_reach_blocked_tools(tmp_path) -> None:
    require_docker()
    config = live_agent_config(tmp_path)

    app = StubWarehouseApp(responses={"propose_mutation": {"proposal_id": "should-never-be-reached"}})
    try:
        with run_agent_tool_proxy(app=WarehouseAppConfig(base_url=app.base_url, token="live-test-token")) as env:
            completed = subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--add-host",
                    "host.docker.internal:host-gateway",
                    "--env",
                    f"PDW_API_URL={env['PDW_API_URL']}",
                    "--env",
                    f"PDW_SECRET_TOKEN={env['PDW_SECRET_TOKEN']}",
                    "--env",
                    f"PDW_CLIENT_NAME={env['PDW_CLIENT_NAME']}",
                    "--env",
                    f"PDW_NO_AUTO_UPDATE={env['PDW_NO_AUTO_UPDATE']}",
                    config.image,
                    "sh",
                    "-lc",
                    "pdw call propose_mutation --data '{}'",
                ],
                capture_output=True,
                text=True,
                check=False,
                timeout=120,
            )
    finally:
        app.close()

    assert completed.returncode != 0
    assert "tool_not_found" in completed.stderr
    assert app.tool_call_paths() == []


@pytest.mark.subscription_agent
def test_live_agent_subscription_smoke_returns_schema_json(subscription_agent_config) -> None:
    config = subscription_agent_config

    result = ContainerAgentRunner(config).run(
        AgentRunRequest(
            prompt='Return JSON exactly like {"ok":true,"message":"subscription smoke test"}.',
            schema={
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "ok": {"type": "boolean"},
                    "message": {"type": "string"},
                },
                "required": ["ok", "message"],
            },
            task_type="live_subscription_smoke_test",
        )
    )

    assert result.status == "completed", result.error
    assert result.final_output_json["ok"] is True
    assert "smoke" in result.final_output_json["message"].lower()


@pytest.mark.subscription_agent
def test_live_agent_can_use_builtin_cli_tool_before_final_json(subscription_agent_config) -> None:
    config = subscription_agent_config

    result = ContainerAgentRunner(config).run(
        AgentRunRequest(
            prompt=(
                "Use bash to write a file named candidate.json containing exactly "
                '{"ok":true,"message":"cli tool smoke test"}. '
                "Then run: \"$PDW_VALIDATE_JSON\" candidate.json \"$AGENT_SCHEMA_PATH\". "
                "After the command succeeds, return the same JSON object as your final answer."
            ),
            schema={
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "ok": {"type": "boolean"},
                    "message": {"type": "string"},
                },
                "required": ["ok", "message"],
            },
            task_type="live_cli_tool_smoke_test",
        )
    )

    assert result.status == "completed", result.error
    assert result.final_output_json["ok"] is True
    assert "cli tool" in result.final_output_json["message"].lower()
    assert any("PDW_VALIDATE_JSON" in event.text for event in result.events)
