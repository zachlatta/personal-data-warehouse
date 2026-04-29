from __future__ import annotations

import os
import json
import shutil
import subprocess

import pytest

from personal_data_warehouse.agent_runner import (
    AgentContainerConfig,
    AgentRunRequest,
    ContainerAgentRunner,
    ensure_agent_image,
    write_builtin_cli_tools,
)
from personal_data_warehouse.agent_tool_proxy import run_agent_tool_proxy
from personal_data_warehouse.clickhouse_readonly import ClickHouseReadOnlyService, RawResult


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_LIVE_AGENT_TESTS") != "1",
    reason="set RUN_LIVE_AGENT_TESTS=1 to run Docker/subscription-backed agent tests",
)


def live_agent_config(tmp_path) -> AgentContainerConfig:
    image = ensure_agent_image()
    return AgentContainerConfig(
        image=image,
        provider=os.getenv("AGENT_PROVIDER", "codex"),
        model=os.getenv("AGENT_MODEL", ""),
        auth_volume=os.getenv("AGENT_AUTH_VOLUME", "pdw-agent-auth"),
        runs_volume=os.getenv("AGENT_RUNS_VOLUME", "pdw-agent-runs"),
        runs_dir=tmp_path,
        timeout_seconds=int(os.getenv("AGENT_TIMEOUT_SECONDS", "1800")),
    )


def require_docker() -> None:
    if not shutil.which("docker"):
        pytest.skip("docker CLI is not installed")


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


def test_live_agent_builtin_cli_can_query_host_proxy(tmp_path) -> None:
    require_docker()
    config = live_agent_config(tmp_path)
    write_builtin_cli_tools(tmp_path)

    class FakeRunner:
        def query(self, sql, *, max_rows):
            return RawResult(columns=["answer"], rows=[{"answer": 42}])

    service = ClickHouseReadOnlyService(FakeRunner(), max_rows=2, max_field_chars=100)
    with run_agent_tool_proxy(query_service=service) as env:
        completed = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--add-host",
                "host.docker.internal:host-gateway",
                "--mount",
                f"type=bind,src={tmp_path.resolve()},dst=/run",
                "--env",
                "AGENT_TOOL_MANIFEST_PATH=/run/TOOLS.md",
                "--env",
                f"PDW_AGENT_TOOL_PROXY_URL={env['PDW_AGENT_TOOL_PROXY_URL']}",
                "--env",
                f"PDW_AGENT_TOOL_PROXY_TOKEN={env['PDW_AGENT_TOOL_PROXY_TOKEN']}",
                config.image,
                "sh",
                "-lc",
                "export PATH=/run/tools:$PATH && pdw-clickhouse-query 'SELECT 42 AS answer'",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["csv"] == "answer\n42"
    assert payload["error"] == ""


def test_live_agent_subscription_smoke_returns_schema_json(tmp_path) -> None:
    require_docker()
    config = live_agent_config(tmp_path)

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


def test_live_agent_can_use_builtin_cli_tool_before_final_json(tmp_path) -> None:
    require_docker()
    config = live_agent_config(tmp_path)

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
