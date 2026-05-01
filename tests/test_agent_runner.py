from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from urllib import request

from personal_data_warehouse.agent_resource import AgentResource
from personal_data_warehouse.agent_runner import (
    AgentContainerConfig,
    AgentRunRequest,
    AgentRunResult,
    AgentRunEvent,
    ContainerAgentRunner,
    agent_run_event_rows,
    agent_run_row,
    agent_run_tool_call_rows,
    agent_config_from_env,
    auth_docker_command,
    default_agent_docker_image,
    default_agent_tool_proxy_public_host,
    ensure_agent_image,
    volume_copy_command,
    write_builtin_cli_tools,
)
from personal_data_warehouse.agent_tool_proxy import run_agent_tool_proxy
from personal_data_warehouse.clickhouse_readonly import ClickHouseReadOnlyService, RawResult
from personal_data_warehouse.config import load_settings


def test_container_agent_runner_builds_locked_down_docker_command(tmp_path) -> None:
    config = AgentContainerConfig(
        image="pdw-agent:latest",
        provider="codex",
        model="gpt-test",
        runs_dir=tmp_path,
    )
    request = AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-1")

    command = ContainerAgentRunner(config).docker_command(request=request, provider="codex", model="gpt-test")

    assert command[:3] == ["docker", "run", "--rm"]
    assert "/var/run/docker.sock" not in command
    assert "--privileged" not in command
    assert "--network" in command
    assert "host" not in command[command.index("--network") + 1]
    assert "--cap-drop" in command
    assert "ALL" in command
    assert "--security-opt" in command
    assert "no-new-privileges" in command
    assert "--read-only" in command
    assert "OPENAI_API_KEY" not in " ".join(command)
    assert "ANTHROPIC_API_KEY" not in " ".join(command)
    assert "CLICKHOUSE_URL" not in " ".join(command)
    assert "type=volume,src=pdw-agent-auth,dst=/agent-auth" in command
    assert "type=volume,src=pdw-agent-runs,dst=/agent-runs" in command
    assert "--add-host" in command
    assert "host.docker.internal:host-gateway" in command
    assert any(item.endswith("/tools/pdw-clickhouse-query") for item in command if item.startswith("PDW_CLICKHOUSE_QUERY="))


def test_container_agent_runner_writes_prompt_schema_and_parses_final_json(tmp_path) -> None:
    volume_copy_calls = 0

    def fake_run(command, **kwargs):
        nonlocal volume_copy_calls
        if command[:2] == ["docker", "run"] and "alpine:3.20" in command:
            volume_copy_calls += 1
            run_dir = tmp_path / "run-1"
            if volume_copy_calls == 1:
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
            (run_dir / "final.json").write_text('{"meeting_title":"Done"}', encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout='{"type":"agent_message","text":"ok"}\n', stderr="")

    config = AgentContainerConfig(image="pdw-agent:latest", runs_dir=tmp_path)
    request = AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-1")

    result = ContainerAgentRunner(config, runner=fake_run).run(request)

    assert (tmp_path / "run-1" / "prompt.txt").read_text(encoding="utf-8") == "Return JSON"
    assert json.loads((tmp_path / "run-1" / "schema.json").read_text(encoding="utf-8")) == {"type": "object"}
    assert result.status == "completed"
    assert result.final_output_json == {"meeting_title": "Done"}
    assert result.events[0].event_type == "agent_message"
    assert (tmp_path / "run-1" / "tools" / "pdw-validate-json").exists()
    assert (tmp_path / "run-1" / "tools" / "pdw-clickhouse-query").exists()
    assert (tmp_path / "run-1" / "tools" / "pdw-clickhouse-schema").exists()
    assert (tmp_path / "run-1" / "TOOLS.md").exists()


def test_container_agent_runner_builds_missing_managed_image_before_run(tmp_path) -> None:
    calls = []
    volume_copy_calls = 0
    image = default_agent_docker_image()

    def fake_run(command, **kwargs):
        nonlocal volume_copy_calls
        calls.append(command)
        if command == ["docker", "image", "inspect", image]:
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="")
        if command[:2] == ["docker", "build"]:
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command[:2] == ["docker", "run"] and "alpine:3.20" in command:
            volume_copy_calls += 1
            if volume_copy_calls == 2:
                (tmp_path / "run-1" / "final.json").write_text('{"ok":true}', encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout='{"type":"agent_message","text":"ok"}\n', stderr="")

    config = AgentContainerConfig(image=image, runs_dir=tmp_path)
    request = AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-1")

    result = ContainerAgentRunner(config, runner=fake_run).run(request)

    assert result.status == "completed"
    assert calls[0] == ["docker", "image", "inspect", image]
    assert calls[1][:2] == ["docker", "build"]
    assert calls[1][calls[1].index("-t") + 1] == image
    agent_run_index = next(index for index, call in enumerate(calls) if call[:2] == ["docker", "run"] and image in call)
    build_index = next(index for index, call in enumerate(calls) if call[:2] == ["docker", "build"])
    assert build_index < agent_run_index


def test_container_agent_runner_can_inject_clickhouse_proxy_without_raw_url(tmp_path) -> None:
    volume_copy_calls = 0
    agent_command = []

    def fake_run(command, **kwargs):
        nonlocal volume_copy_calls, agent_command
        if command[:2] == ["docker", "run"] and "alpine:3.20" in command:
            volume_copy_calls += 1
            if volume_copy_calls == 2:
                (tmp_path / "run-1" / "final.json").write_text('{"ok":true}', encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        agent_command = command
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    config = AgentContainerConfig(
        image="pdw-agent:latest",
        runs_dir=tmp_path,
        tool_proxy_bind_host="127.0.0.1",
        tool_proxy_public_host="127.0.0.1",
    )
    result = ContainerAgentRunner(config, runner=fake_run).run_with_clickhouse(
        AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-1"),
        warehouse=object(),
    )

    joined = " ".join(agent_command)
    assert result.status == "completed"
    assert "PDW_AGENT_TOOL_PROXY_URL=http://127.0.0.1:" in joined
    assert "PDW_AGENT_TOOL_PROXY_TOKEN=" in joined
    assert "PDW_CLICKHOUSE_QUERY=" in joined
    assert "CLICKHOUSE_URL" not in joined


def test_volume_copy_command_copies_run_files_without_socket(tmp_path) -> None:
    command = volume_copy_command(
        volume="pdw-agent-runs",
        run_id="run-1",
        direction="to_volume",
        host_dir=tmp_path,
    )

    joined = " ".join(command)
    assert "type=volume,src=pdw-agent-runs,dst=/volume" in command
    assert f"type=bind,src={tmp_path.resolve()},dst=/host" in command
    assert "/var/run/docker.sock" not in joined
    assert "alpine:3.20" in command


def test_agent_result_rows_serialize_events_and_tool_calls() -> None:
    now = datetime(2026, 4, 29, tzinfo=UTC)
    result = AgentRunResult(
        run_id="run-1",
        provider="codex",
        model="gpt-test",
        task_type="apple_voice_memo_enrichment",
        subject_id="rec-1",
        prompt_version="prompt-v1",
        input_sha256="abc",
        status="completed",
        final_output_json={"ok": True},
        error="",
        exit_code=0,
        started_at=now,
        completed_at=now,
        events=[
            AgentRunEvent(
                event_index=0,
                stream="stdout",
                event_type="mcp_tool_call",
                event_json={"type": "mcp_tool_call", "tool_name": "query", "arguments": {"sql": "SELECT 1"}},
                text="{}",
                created_at=now,
            ),
            AgentRunEvent(
                event_index=1,
                stream="stdout",
                event_type="item.completed",
                event_json={
                    "type": "item.completed",
                    "item": {
                        "type": "command_execution",
                        "command": '/bin/bash -lc "$PDW_CLICKHOUSE_QUERY SELECT 1"',
                        "aggregated_output": '{"csv":"1\\n1"}',
                        "exit_code": 0,
                        "status": "completed",
                    },
                },
                text="{}",
                created_at=now,
            )
        ],
    )

    assert agent_run_row(result)["final_output_json"] == '{"ok":true}'
    assert agent_run_row(result)["prompt_version"] == "prompt-v1"
    assert agent_run_event_rows(result)[0]["event_type"] == "mcp_tool_call"
    tool_rows = agent_run_tool_call_rows(result)
    assert [row["tool_name"] for row in tool_rows] == ["query", "pdw-clickhouse-query"]
    assert tool_rows[0]["arguments_json"] == '{"sql":"SELECT 1"}'
    assert "PDW_CLICKHOUSE_QUERY" in tool_rows[1]["arguments_json"]


def test_container_agent_runner_rejects_oversized_prompt_before_docker(tmp_path) -> None:
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    config = AgentContainerConfig(image="pdw-agent:latest", runs_dir=tmp_path, max_prompt_chars=10)
    request = AgentRunRequest(
        prompt="x" * 11,
        schema={"type": "object"},
        run_id="run-1",
        prompt_version="prompt-v1",
    )

    result = ContainerAgentRunner(config, runner=fake_run).run(request)

    assert result.status == "error"
    assert result.prompt_version == "prompt-v1"
    assert "agent prompt exceeds maximum length of 10 characters" in result.error
    assert calls == []


def test_auth_command_uses_subscription_volume_without_api_keys() -> None:
    command = auth_docker_command(
        provider="codex",
        action="login",
        config=AgentContainerConfig(image="pdw-agent:latest"),
        interactive=True,
    )

    assert command[:4] == ["docker", "run", "--rm", "-it"]
    assert "type=volume,src=pdw-agent-auth,dst=/agent-auth" in command
    assert "OPENAI_API_KEY" not in " ".join(command)
    assert command[-3:-1] == ["sh", "-lc"]
    assert "mkdir -p" in command[-1]
    assert "codex login --device-auth" in command[-1]


def test_default_agent_docker_image_uses_agent_image_inputs_hash(tmp_path) -> None:
    dockerfile = tmp_path / "agent.Dockerfile"
    entrypoint = tmp_path / "agent-entrypoint.sh"
    dockerfile.write_text("FROM alpine\n", encoding="utf-8")
    entrypoint.write_text("#!/bin/sh\n", encoding="utf-8")

    image = default_agent_docker_image(
        repository="pdw-agent",
        dockerfile_path=dockerfile,
        entrypoint_path=entrypoint,
    )
    first = image.rsplit(":", 1)[1]

    entrypoint.write_text("#!/bin/sh\necho changed\n", encoding="utf-8")
    changed = default_agent_docker_image(
        repository="pdw-agent",
        dockerfile_path=dockerfile,
        entrypoint_path=entrypoint,
    )

    assert image.startswith("pdw-agent:")
    assert len(first) == 6
    assert changed != image


def test_agent_config_uses_container_hostname_for_non_bridge_proxy(monkeypatch) -> None:
    monkeypatch.setenv("AGENT_DOCKER_NETWORK", "coolify")
    monkeypatch.setattr("personal_data_warehouse.agent_runner.socket.gethostname", lambda: "dagster-container")

    config = agent_config_from_env()

    assert config.network == "coolify"
    assert config.tool_proxy_public_host == "dagster-container"
    assert default_agent_tool_proxy_public_host("bridge") == "host.docker.internal"


def test_ensure_agent_image_skips_build_when_derived_image_exists() -> None:
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0)

    image = ensure_agent_image(runner=fake_run)

    assert image.startswith("personal-data-warehouse-agent:")
    assert calls == [(["docker", "image", "inspect", image], {"capture_output": True, "text": True, "check": False})]


def test_ensure_agent_image_builds_when_image_is_missing(tmp_path) -> None:
    dockerfile = tmp_path / "agent.Dockerfile"
    entrypoint = tmp_path / "agent-entrypoint.sh"
    context_dir = tmp_path / "context"
    dockerfile.write_text("FROM alpine\n", encoding="utf-8")
    entrypoint.write_text("#!/bin/sh\n", encoding="utf-8")
    context_dir.mkdir()
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 1 if command[:3] == ["docker", "image", "inspect"] else 0)

    image = ensure_agent_image(
        dockerfile_path=dockerfile,
        entrypoint_path=entrypoint,
        context_dir=context_dir,
        runner=fake_run,
    )

    assert image.startswith("personal-data-warehouse-agent:")
    assert calls[0][0] == ["docker", "image", "inspect", image]
    assert calls[1][0] == ["docker", "build", "-f", str(dockerfile), "-t", image, str(context_dir)]


def test_agent_entrypoint_skips_codex_git_repo_check() -> None:
    entrypoint = Path("docker/agent-entrypoint.sh").read_text(encoding="utf-8")

    assert "codex exec --json --skip-git-repo-check" in entrypoint
    assert "--dangerously-bypass-approvals-and-sandbox" in entrypoint
    assert "shell_environment_policy.inherit=all" in entrypoint
    assert 'export PATH="$tools_dir:$PATH"' in entrypoint
    assert '< "$prompt_path"' in entrypoint


def test_builtin_cli_tools_validate_json(tmp_path) -> None:
    write_builtin_cli_tools(tmp_path)
    candidate = tmp_path / "candidate.json"
    schema = tmp_path / "schema.json"
    candidate.write_text('{"ok":true,"message":"hello"}', encoding="utf-8")
    schema.write_text(
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
        [str(tmp_path / "tools" / "pdw-validate-json"), str(candidate), str(schema)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert completed.stdout.strip() == "ok"


def test_builtin_cli_tools_query_clickhouse_proxy(tmp_path) -> None:
    class FakeRunner:
        def query(self, sql, *, max_rows):
            assert max_rows == 3
            return RawResult(columns=["answer"], rows=[{"answer": 42}])

    write_builtin_cli_tools(tmp_path)
    service = ClickHouseReadOnlyService(FakeRunner(), max_rows=2, max_field_chars=100)
    with run_agent_tool_proxy(query_service=service, bind_host="127.0.0.1", public_host="127.0.0.1") as env:
        completed = subprocess.run(
            [str(tmp_path / "tools" / "pdw-clickhouse-query"), "SELECT 42 AS answer"],
            env={**env, "PATH": os.environ.get("PATH", "")},
            capture_output=True,
            text=True,
            check=False,
        )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["csv"] == "answer\n42"
    assert payload["error"] == ""


def test_builtin_cli_tools_reject_write_queries_through_proxy(tmp_path) -> None:
    class FakeRunner:
        def query(self, sql, *, max_rows):
            raise AssertionError("write query should be rejected before runner")

    write_builtin_cli_tools(tmp_path)
    service = ClickHouseReadOnlyService(FakeRunner(), max_rows=2, max_field_chars=100)
    with run_agent_tool_proxy(query_service=service, bind_host="127.0.0.1", public_host="127.0.0.1") as env:
        completed = subprocess.run(
            [str(tmp_path / "tools" / "pdw-clickhouse-query"), "DROP TABLE apple_voice_memos_enrichments"],
            env={**env, "PATH": os.environ.get("PATH", "")},
            capture_output=True,
            text=True,
            check=False,
        )

    assert completed.returncode == 0
    payload = json.loads(completed.stdout)
    assert "read-only" in payload["error"]


def test_agent_tool_proxy_serializes_clickhouse_queries() -> None:
    active = 0
    max_active = 0
    guard = threading.Lock()

    class FakeRunner:
        def query(self, sql, *, max_rows):
            nonlocal active, max_active
            with guard:
                active += 1
                max_active = max(max_active, active)
            try:
                time.sleep(0.05)
                return RawResult(columns=["sql"], rows=[{"sql": sql}])
            finally:
                with guard:
                    active -= 1

    service = ClickHouseReadOnlyService(FakeRunner(), max_rows=2, max_field_chars=100)
    with run_agent_tool_proxy(query_service=service, bind_host="127.0.0.1", public_host="127.0.0.1") as env:
        results = []

        def post(sql: str) -> None:
            req = request.Request(
                f"{env['PDW_AGENT_TOOL_PROXY_URL']}/query",
                data=json.dumps({"sql": sql}).encode("utf-8"),
                headers={
                    "authorization": f"Bearer {env['PDW_AGENT_TOOL_PROXY_TOKEN']}",
                    "content-type": "application/json",
                },
                method="POST",
            )
            with request.urlopen(req, timeout=5) as response:
                results.append(json.loads(response.read().decode("utf-8")))

        threads = [threading.Thread(target=post, args=(f"SELECT {index}",)) for index in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert len(results) == 3
    assert all(result["error"] == "" for result in results)
    assert max_active == 1


def test_builtin_cli_tools_reject_invalid_json_shape(tmp_path) -> None:
    write_builtin_cli_tools(tmp_path)
    candidate = tmp_path / "candidate.json"
    schema = tmp_path / "schema.json"
    candidate.write_text('{"ok":"yes","extra":1}', encoding="utf-8")
    schema.write_text(
        json.dumps(
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {"ok": {"type": "boolean"}},
                "required": ["ok", "message"],
            }
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [str(tmp_path / "tools" / "pdw-validate-json"), str(candidate), str(schema)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 1
    assert "missing required key: message" in completed.stderr
    assert "ok must be boolean" in completed.stderr
    assert "unexpected key: extra" in completed.stderr


def test_load_settings_reads_agent_config_without_api_keys(monkeypatch) -> None:
    monkeypatch.setenv("AGENT_PROVIDER", "claude")
    monkeypatch.setenv("AGENT_MODEL", "claude-test")
    monkeypatch.setenv("AGENT_TOOL_PROXY_PUBLIC_HOST", "dagster")

    settings = load_settings(require_clickhouse=False, require_gmail=False, require_agent=True)

    assert settings.agent is not None
    assert settings.agent.provider == "claude"
    assert settings.agent.model == "claude-test"
    assert settings.agent.docker_image.startswith("personal-data-warehouse-agent:")
    assert settings.agent.tool_proxy_public_host == "dagster"


def test_load_settings_derives_agent_image_when_required(monkeypatch) -> None:
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_agent=True)

    assert settings.agent is not None
    assert settings.agent.docker_image.startswith("personal-data-warehouse-agent:")
    assert len(settings.agent.docker_image.rsplit(":", 1)[1]) == 6


def test_load_settings_uses_container_hostname_for_non_bridge_agent_network(monkeypatch) -> None:
    monkeypatch.setenv("AGENT_DOCKER_NETWORK", "coolify")
    monkeypatch.setattr("personal_data_warehouse.agent_runner.socket.gethostname", lambda: "dagster-container")

    settings = load_settings(require_clickhouse=False, require_gmail=False, require_agent=True)

    assert settings.agent is not None
    assert settings.agent.docker_network == "coolify"
    assert settings.agent.tool_proxy_public_host == "dagster-container"


def test_agent_resource_builds_container_config() -> None:
    resource = AgentResource(
        docker_image="pdw-agent:latest",
        provider="claude",
        model="claude-test",
        auth_volume="auth-vol",
        runs_volume="runs-vol",
        runs_dir="/tmp/runs",
        timeout_seconds=123,
        tool_proxy_public_host="dagster",
    )

    config = resource.container_config()

    assert config.image == "pdw-agent:latest"
    assert config.provider == "claude"
    assert config.model == "claude-test"
    assert config.auth_volume == "auth-vol"
    assert config.runs_volume == "runs-vol"
    assert str(config.runs_dir) == "/tmp/runs"
    assert config.timeout_seconds == 123
    assert config.tool_proxy_public_host == "dagster"


def test_agent_resource_disabled_fails_with_clear_error() -> None:
    resource = AgentResource.disabled()

    assert resource.is_configured is False
    try:
        resource.container_config()
    except RuntimeError as exc:
        assert "AgentResource is not configured" in str(exc)
    else:
        raise AssertionError("disabled AgentResource should not build a container config")
