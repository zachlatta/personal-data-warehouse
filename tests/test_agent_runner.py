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
    agent_credentials_copy_command,
    agent_credentials_volume_name,
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
from personal_data_warehouse.agent_tool_proxy import WarehouseAppConfig
from personal_data_warehouse.config import load_settings


def is_run_volume_copy(command: list[str]) -> bool:
    return (
        command[:2] == ["docker", "run"]
        and "alpine:3.20" in command
        and any(item.endswith("dst=/volume") for item in command)
    )


def is_agent_container_run(command: list[str], image: str = "pdw-agent:latest") -> bool:
    return command[:2] == ["docker", "run"] and "--name" in command and image in command


def test_container_agent_runner_builds_locked_down_docker_command(tmp_path) -> None:
    config = AgentContainerConfig(
        image="pdw-agent:latest",
        provider="codex",
        model="gpt-test",
        runs_dir=tmp_path,
    )
    request = AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-1")

    credentials_volume = agent_credentials_volume_name(request.run_id)
    command = ContainerAgentRunner(config).docker_command(
        request=request,
        provider="codex",
        model="gpt-test",
        credentials_volume=credentials_volume,
    )

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
    assert command[command.index("--memory-swap") + 1] == config.memory
    assert "OPENAI_API_KEY" not in " ".join(command)
    assert "ANTHROPIC_API_KEY" not in " ".join(command)
    assert "POSTGRES_DATABASE_URL" not in " ".join(command)
    assert "AGENT_MODEL=gpt-test" in command
    assert "AGENT_REASONING_EFFORT=medium" in command
    assert "type=volume,src=pdw-agent-auth,dst=/agent-auth" not in command
    assert f"type=volume,src={credentials_volume},dst=/agent-credentials" in command
    assert "CODEX_HOME=/tmp/agent-codex-home" in command
    assert "CODEX_SQLITE_HOME=/tmp/agent-codex-sqlite" in command
    assert "CLAUDE_CONFIG_DIR=/tmp/agent-claude-config" in command
    assert "AGENT_AUTH_SOURCE=/agent-credentials/codex/auth.json" in command
    assert "AGENT_AUTH_OUTPUT=/agent-credentials/codex/auth.json" in command
    assert "type=volume,src=pdw-agent-runs,dst=/agent-runs" in command
    assert "--add-host" in command
    assert "host.docker.internal:host-gateway" in command
    # The warehouse is reached with the real `pdw` CLI, not a bespoke shim.
    assert "PDW_POSTGRES_QUERY" not in " ".join(command)
    assert "PDW_POSTGRES_SCHEMA" not in " ".join(command)


def test_agent_credentials_copy_commands_stage_and_persist_only_codex_auth_file() -> None:
    stage = agent_credentials_copy_command(
        auth_volume="pdw-agent-auth",
        credentials_volume="pdw-agent-credentials-run-1",
        provider="codex",
        direction="stage",
    )
    persist = agent_credentials_copy_command(
        auth_volume="pdw-agent-auth",
        credentials_volume="pdw-agent-credentials-run-1",
        provider="codex",
        direction="persist",
    )

    assert "type=volume,src=pdw-agent-auth,dst=/persistent-auth,readonly" in stage
    assert "type=volume,src=pdw-agent-credentials-run-1,dst=/run-credentials" in stage
    assert "/persistent-auth/codex/auth.json" in stage[-1]
    assert "/run-credentials/codex/auth.json" in stage[-1]
    assert "/persistent-auth/." not in stage[-1]

    assert "type=volume,src=pdw-agent-auth,dst=/persistent-auth" in persist
    assert "type=volume,src=pdw-agent-credentials-run-1,dst=/run-credentials,readonly" in persist
    assert "/run-credentials/codex/auth.json" in persist[-1]
    assert "/persistent-auth/codex/auth.json" in persist[-1]
    assert "/run-credentials/." not in persist[-1]


def test_container_agent_runner_writes_prompt_schema_and_parses_final_json(tmp_path) -> None:
    volume_copy_calls = 0

    def fake_run(command, **kwargs):
        nonlocal volume_copy_calls
        if is_run_volume_copy(command):
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
    assert not (tmp_path / "run-1" / "tools" / "pdw-postgres-query").exists()
    assert not (tmp_path / "run-1" / "tools" / "pdw-postgres-schema").exists()
    assert "pdw sql" in (tmp_path / "run-1" / "TOOLS.md").read_text(encoding="utf-8")


def test_container_agent_runner_stages_refreshes_and_removes_per_run_credentials(tmp_path) -> None:
    calls = []
    volume_copy_calls = 0

    def fake_run(command, **kwargs):
        nonlocal volume_copy_calls
        calls.append(command)
        if is_run_volume_copy(command):
            volume_copy_calls += 1
            if volume_copy_calls == 2:
                (tmp_path / "run-1" / "final.json").write_text('{"ok":true}', encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    result = ContainerAgentRunner(
        AgentContainerConfig(image="pdw-agent:latest", runs_dir=tmp_path),
        runner=fake_run,
    ).run(AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-1"))

    assert result.status == "completed"
    create_index = next(index for index, call in enumerate(calls) if call[:3] == ["docker", "volume", "create"])
    stage_index = next(
        index
        for index, call in enumerate(calls)
        if "type=volume,src=pdw-agent-auth,dst=/persistent-auth,readonly" in call
    )
    agent_index = next(index for index, call in enumerate(calls) if is_agent_container_run(call))
    persist_index = next(
        index
        for index, call in enumerate(calls)
        if "type=volume,src=pdw-agent-auth,dst=/persistent-auth" in call
        and "type=volume,src=pdw-agent-auth,dst=/persistent-auth,readonly" not in call
    )
    remove_index = next(index for index, call in enumerate(calls) if call[:4] == ["docker", "volume", "rm", "-f"])

    assert create_index < stage_index < agent_index < persist_index < remove_index
    assert "pdw-agent-auth" not in calls[agent_index]


def test_shared_auth_run_does_not_replace_persistent_credential(tmp_path, monkeypatch) -> None:
    calls = []
    volume_copy_calls = 0
    monkeypatch.setattr(
        ContainerAgentRunner,
        "_auth_lock_must_be_exclusive",
        lambda self, provider: False,
    )

    def fake_run(command, **kwargs):
        nonlocal volume_copy_calls
        calls.append(command)
        if is_run_volume_copy(command):
            volume_copy_calls += 1
            if volume_copy_calls == 2:
                (tmp_path / "run-1" / "final.json").write_text('{"ok":true}', encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    result = ContainerAgentRunner(
        AgentContainerConfig(image="pdw-agent:latest", runs_dir=tmp_path),
        runner=fake_run,
    ).run(AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-1"))

    assert result.status == "completed"
    assert any(
        "type=volume,src=pdw-agent-auth,dst=/persistent-auth,readonly" in call
        for call in calls
    )
    assert not any(
        "type=volume,src=pdw-agent-auth,dst=/persistent-auth" in call
        and "type=volume,src=pdw-agent-auth,dst=/persistent-auth,readonly" not in call
        for call in calls
    )
    assert any(call[:4] == ["docker", "volume", "rm", "-f"] for call in calls)


def test_timed_out_agent_is_stopped_before_refreshed_auth_is_persisted(tmp_path) -> None:
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        if is_agent_container_run(command):
            raise subprocess.TimeoutExpired(command, timeout=1)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    result = ContainerAgentRunner(
        AgentContainerConfig(
            image="pdw-agent:latest",
            runs_dir=tmp_path,
            timeout_seconds=1,
        ),
        runner=fake_run,
    ).run(AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-1"))

    stop_index = next(index for index, call in enumerate(calls) if call[:2] == ["docker", "stop"])
    persist_index = next(
        index
        for index, call in enumerate(calls)
        if "type=volume,src=pdw-agent-auth,dst=/persistent-auth" in call
        and "type=volume,src=pdw-agent-auth,dst=/persistent-auth,readonly" not in call
    )
    assert result.status == "error"
    assert stop_index < persist_index


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
        if is_run_volume_copy(command):
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


def test_container_agent_runner_points_pdw_cli_at_a_per_run_proxy(tmp_path) -> None:
    volume_copy_calls = 0
    agent_command = []

    def fake_run(command, **kwargs):
        nonlocal volume_copy_calls, agent_command
        if is_run_volume_copy(command):
            volume_copy_calls += 1
            if volume_copy_calls == 2:
                (tmp_path / "run-1" / "final.json").write_text('{"ok":true}', encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if is_agent_container_run(command):
            agent_command = command
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    config = AgentContainerConfig(
        image="pdw-agent:latest",
        runs_dir=tmp_path,
        tool_proxy_bind_host="127.0.0.1",
        tool_proxy_public_host="127.0.0.1",
    )
    result = ContainerAgentRunner(config, runner=fake_run).run_with_pdw(
        AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-1"),
        app=WarehouseAppConfig(base_url="https://warehouse.example.com", token="real-app-token-32-chars-minimum!!"),
    )

    joined = " ".join(agent_command)
    assert result.status == "completed"
    assert "PDW_API_URL=http://127.0.0.1:" in joined
    assert "PDW_SECRET_TOKEN=" in joined
    assert "PDW_CLIENT_NAME=pdw-agent-run-1" in joined
    assert "PDW_NO_AUTO_UPDATE=1" in joined
    # Neither the raw database nor the real app credential ever reaches the container.
    assert "POSTGRES_DATABASE_URL" not in joined
    assert "real-app-token-32-chars-minimum!!" not in joined
    assert "warehouse.example.com" not in joined


def test_container_agent_runner_writes_input_files_and_exports_input_dir(tmp_path) -> None:
    agent_command = []
    volume_copy_calls = 0

    def fake_run(command, **kwargs):
        nonlocal agent_command, volume_copy_calls
        if is_run_volume_copy(command):
            volume_copy_calls += 1
            if volume_copy_calls == 2:
                (tmp_path / "run-1" / "final.json").write_text('{"ok":true}', encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if is_agent_container_run(command):
            agent_command = command
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    config = AgentContainerConfig(image="pdw-agent:latest", runs_dir=tmp_path)
    request = AgentRunRequest(
        prompt="Read input file",
        schema={"type": "object"},
        run_id="run-1",
        input_files={"payload/task.json": '{"big":"payload"}'},
    )

    result = ContainerAgentRunner(config, runner=fake_run).run(request)

    assert result.status == "completed"
    assert (tmp_path / "run-1" / "inputs" / "payload" / "task.json").read_text(encoding="utf-8") == '{"big":"payload"}'
    assert "AGENT_INPUT_DIR=/agent-runs/run-1/inputs" in agent_command
    assert json.loads((tmp_path / "run-1" / "request.json").read_text(encoding="utf-8"))["input_files"] == [
        "payload/task.json"
    ]


def test_container_agent_runner_writes_binary_input_files(tmp_path) -> None:
    volume_copy_calls = 0

    def fake_run(command, **kwargs):
        nonlocal volume_copy_calls
        if is_run_volume_copy(command):
            volume_copy_calls += 1
            if volume_copy_calls == 2:
                (tmp_path / "run-1" / "final.json").write_text('{"ok":true}', encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    config = AgentContainerConfig(image="pdw-agent:latest", runs_dir=tmp_path)
    image_bytes = b"\x89PNG\r\n\x1a\n binary image data"
    request = AgentRunRequest(
        prompt="View the image",
        schema={"type": "object"},
        run_id="run-1",
        input_files={"attachment.png": image_bytes},
    )

    result = ContainerAgentRunner(config, runner=fake_run).run(request)

    assert result.status == "completed"
    assert (tmp_path / "run-1" / "inputs" / "attachment.png").read_bytes() == image_bytes
    # The request hash must be derived from the binary content without crashing.
    assert request.input_sha256 == AgentRunRequest(
        prompt="View the image",
        schema={"type": "object"},
        run_id="other-run",
        input_files={"attachment.png": image_bytes},
    ).input_sha256


def test_container_agent_runner_rejects_unsafe_input_file_paths(tmp_path) -> None:
    config = AgentContainerConfig(image="pdw-agent:latest", runs_dir=tmp_path)
    request = AgentRunRequest(
        prompt="Read input file",
        schema={"type": "object"},
        run_id="run-1",
        input_files={"../outside.json": "{}"},
    )

    try:
        ContainerAgentRunner(config, runner=lambda command, **kwargs: None).run(request)
    except ValueError as exc:
        assert "agent input file path must be relative" in str(exc)
    else:
        raise AssertionError("expected unsafe input path to fail")


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
                        "command": "/bin/bash -lc \"pdw sql -q 'row count' 'SELECT 1'\"",
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
    assert [row["tool_name"] for row in tool_rows] == ["query", "pdw sql"]
    assert tool_rows[0]["arguments_json"] == '{"sql":"SELECT 1"}'
    assert "pdw sql" in tool_rows[1]["arguments_json"]


def test_agent_run_tool_call_rows_names_each_pdw_subcommand() -> None:
    now = datetime.now(tz=UTC)
    commands = [
        ("pdw schema", "pdw schema"),
        ("pdw columns gmail.messages", "pdw columns"),
        ("bash -lc \"pdw call get_object --data '{}'\"", "pdw call get_object"),
        # Flags may precede the tool name; the --data blob must not be mistaken for it.
        ("pdw call --data '{\"storage_file_id\":\"fid\"}' get_object", "pdw call get_object"),
        ("pdw call something_unregistered --data '{}'", "pdw call"),
        ("pdw sql --file query.sql", "pdw sql"),
        ("/usr/local/bin/pdw help", "pdw"),
        ("rg -n TODO .", "bash"),
    ]
    events = [
        AgentRunEvent(
            event_index=index,
            stream="stdout",
            event_type="item.completed",
            event_json={"type": "item.completed", "item": {"type": "command_execution", "command": command}},
            text="{}",
            created_at=now,
        )
        for index, (command, _expected) in enumerate(commands)
    ]
    result = AgentRunResult(
        run_id="run-1",
        provider="codex",
        model="gpt-test",
        task_type="voice_memo_enrichment",
        subject_id="rec-1",
        prompt_version="prompt-v1",
        input_sha256="sha",
        status="completed",
        final_output_json={},
        error="",
        exit_code=0,
        started_at=now,
        completed_at=now,
        events=events,
    )

    assert [row["tool_name"] for row in agent_run_tool_call_rows(result)] == [expected for _command, expected in commands]


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


def test_container_agent_runner_reports_nonzero_exit_stderr_before_json_parse_error(tmp_path) -> None:
    volume_copy_calls = 0

    def fake_run(command, **kwargs):
        nonlocal volume_copy_calls
        if is_run_volume_copy(command):
            volume_copy_calls += 1
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if is_agent_container_run(command):
            return subprocess.CompletedProcess(
                command,
                1,
                stdout='{"type":"thread.started"}\n',
                stderr="Error: turn/start failed: Input exceeds the maximum length of 1048576 characters.\n",
            )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    config = AgentContainerConfig(image="pdw-agent:latest", runs_dir=tmp_path)
    request = AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-1")

    result = ContainerAgentRunner(config, runner=fake_run).run(request)

    assert result.status == "error"
    assert "agent container exited with code 1" in result.error
    assert "Input exceeds the maximum length" in result.error


def test_auth_command_uses_subscription_volume_without_api_keys() -> None:
    command = auth_docker_command(
        provider="codex",
        action="login",
        config=AgentContainerConfig(image="pdw-agent:latest"),
        interactive=True,
    )

    assert command[:4] == ["docker", "run", "--rm", "-it"]
    assert "type=volume,src=pdw-agent-auth,dst=/agent-auth" in command
    assert "CODEX_SQLITE_HOME=/tmp/agent-codex-sqlite" in command
    assert "/tmp/agent-codex-sqlite:rw,nosuid,size=512m" in command
    assert "OPENAI_API_KEY" not in " ".join(command)
    assert command[-3:-1] == ["sh", "-lc"]
    assert "mkdir -p" in command[-1]
    assert "codex login --device-auth" in command[-1]


def test_claude_status_command_checks_auth_not_updater_doctor() -> None:
    command = auth_docker_command(
        provider="claude",
        action="status",
        config=AgentContainerConfig(image="pdw-agent:latest"),
        interactive=False,
    )

    assert command[:3] == ["docker", "run", "--rm"]
    assert "-it" not in command
    assert "type=volume,src=pdw-agent-auth,dst=/agent-auth" in command
    assert "claude auth status" in command[-1]
    assert "claude doctor" not in command[-1]


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


def test_default_agent_docker_image_tracks_the_pdw_cli_source(tmp_path) -> None:
    """The image bakes in a compiled `pdw`, so CLI edits must rebuild it.

    Hashing only the Dockerfile would leave every agent run on a stale binary
    after a CLI change, with no signal that it happened.
    """

    dockerfile = tmp_path / "agent.Dockerfile"
    entrypoint = tmp_path / "agent-entrypoint.sh"
    cli_dir = tmp_path / "app" / "cmd" / "pdw-cli"
    cli_dir.mkdir(parents=True)
    dockerfile.write_text("FROM alpine\n", encoding="utf-8")
    entrypoint.write_text("#!/bin/sh\n", encoding="utf-8")
    (tmp_path / "app" / "go.mod").write_text("module example\n", encoding="utf-8")
    (cli_dir / "run.go").write_text("package main\n", encoding="utf-8")

    def image_now() -> str:
        return default_agent_docker_image(
            repository="pdw-agent",
            dockerfile_path=dockerfile,
            entrypoint_path=entrypoint,
            cli_source_dir=tmp_path / "app",
        )

    before = image_now()
    (cli_dir / "run.go").write_text("package main\n// changed\n", encoding="utf-8")
    after = image_now()

    # A test-only .go file is not shipped in the binary and must not churn the tag.
    (cli_dir / "run_test.go").write_text("package main\n", encoding="utf-8")
    with_test_file = image_now()

    assert after != before
    assert with_test_file == after


def test_dagster_image_ships_every_input_the_agent_image_build_needs() -> None:
    """Agent images are built from inside the Dagster container.

    `ensure_agent_image` runs `docker build` with this repo root as the context,
    so anything agent.Dockerfile COPYs must also have been COPYed into the
    Dagster image — otherwise the build only fails in production, on the first
    agent run after deploy.
    """

    repo_root = Path(__file__).resolve().parents[1]
    dagster_dockerfile = (repo_root / "Dockerfile").read_text(encoding="utf-8")
    agent_dockerfile = (repo_root / "docker" / "agent.Dockerfile").read_text(encoding="utf-8")

    copied_into_dagster = {
        line.split()[1].lstrip("./")
        for line in dagster_dockerfile.splitlines()
        if line.startswith("COPY ") and len(line.split()) >= 3
    }
    build_context_paths = {
        source.lstrip("./").split("/")[0]
        for line in agent_dockerfile.splitlines()
        if line.startswith("COPY ") and "--from=" not in line
        for source in line.split()[1:-1]
    }

    missing = {
        path
        for path in build_context_paths
        if not any(copied == path or copied.startswith(path + "/") for copied in copied_into_dagster)
    }
    assert not missing, f"agent.Dockerfile needs {sorted(missing)} but the Dagster image does not COPY it"


def test_agent_config_uses_container_hostname_for_non_bridge_proxy(monkeypatch) -> None:
    monkeypatch.setenv("AGENT_DOCKER_NETWORK", "coolify")
    monkeypatch.setattr("personal_data_warehouse.agent_runner.socket.gethostname", lambda: "dagster-container")

    config = agent_config_from_env()

    assert config.network == "coolify"
    assert config.tool_proxy_public_host == "dagster-container"
    assert default_agent_tool_proxy_public_host("bridge") == "host.docker.internal"


def test_agent_config_defaults_codex_to_gpt_5_6_sol_with_medium_reasoning(monkeypatch) -> None:
    monkeypatch.setenv("AGENT_PROVIDER", "codex")
    monkeypatch.delenv("AGENT_MODEL", raising=False)
    monkeypatch.delenv("AGENT_REASONING_EFFORT", raising=False)

    config = agent_config_from_env()

    assert config.model == "gpt-5.6-sol"
    assert config.reasoning_effort == "medium"


def test_agent_config_keeps_claude_model_unset_by_default(monkeypatch) -> None:
    monkeypatch.setenv("AGENT_PROVIDER", "claude")
    monkeypatch.delenv("AGENT_MODEL", raising=False)

    config = agent_config_from_env()

    assert config.model == ""


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
    assert 'model_reasoning_effort="$reasoning_effort"' in entrypoint
    assert 'model="${model:-gpt-5.6-sol}"' in entrypoint
    assert 'export PATH="$tools_dir:$PATH"' in entrypoint
    assert '< "$prompt_path"' in entrypoint


def test_agent_entrypoint_keeps_codex_state_ephemeral_and_returns_only_refreshed_auth(tmp_path) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_codex = bin_dir / "codex"
    fake_codex.write_text(
        """#!/bin/sh
set -eu
test "$(cat "$CODEX_HOME/auth.json")" = "original-auth"
printf 'sqlite-state' > "$CODEX_SQLITE_HOME/state_5.sqlite"
printf 'refreshed-auth' > "$CODEX_HOME/auth.json"
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output-last-message" ]; then
    shift
    printf '{"ok":true}' > "$1"
    exit 0
  fi
  shift
done
exit 2
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)

    prompt = tmp_path / "prompt.txt"
    schema = tmp_path / "schema.json"
    final_message = tmp_path / "final.md"
    final_json = tmp_path / "final.json"
    credential_source = tmp_path / "credential-source.json"
    credential_output = tmp_path / "credential-output.json"
    codex_home = tmp_path / "codex-home"
    sqlite_home = tmp_path / "codex-sqlite"
    prompt.write_text("Return JSON", encoding="utf-8")
    schema.write_text('{"type":"object"}', encoding="utf-8")
    credential_source.write_text("original-auth", encoding="utf-8")

    completed = subprocess.run(
        ["sh", "docker/agent-entrypoint.sh"],
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "AGENT_PROVIDER": "codex",
            "AGENT_PROMPT_PATH": str(prompt),
            "AGENT_SCHEMA_PATH": str(schema),
            "AGENT_FINAL_MESSAGE_PATH": str(final_message),
            "AGENT_FINAL_JSON_PATH": str(final_json),
            "AGENT_AUTH_SOURCE": str(credential_source),
            "AGENT_AUTH_OUTPUT": str(credential_output),
            "CODEX_HOME": str(codex_home),
            "CODEX_SQLITE_HOME": str(sqlite_home),
            "HOME": str(tmp_path / "home"),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert credential_source.read_text(encoding="utf-8") == "original-auth"
    assert credential_output.read_text(encoding="utf-8") == "refreshed-auth"
    assert (sqlite_home / "state_5.sqlite").read_text(encoding="utf-8") == "sqlite-state"
    assert not (credential_output.parent / "state_5.sqlite").exists()
    assert json.loads(final_json.read_text(encoding="utf-8")) == {"ok": True}


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
    monkeypatch.setenv("AGENT_REASONING_EFFORT", "high")
    monkeypatch.setenv("AGENT_TOOL_PROXY_PUBLIC_HOST", "dagster")

    settings = load_settings(require_postgres=False, require_gmail=False, require_agent=True)

    assert settings.agent is not None
    assert settings.agent.provider == "claude"
    assert settings.agent.model == "claude-test"
    assert settings.agent.reasoning_effort == "high"
    assert settings.agent.docker_image.startswith("personal-data-warehouse-agent:")
    assert settings.agent.runs_dir == ".agent-runs"
    assert settings.agent.tool_proxy_public_host == "dagster"


def test_load_settings_derives_agent_image_when_required(monkeypatch) -> None:
    monkeypatch.setenv("AGENT_PROVIDER", "codex")
    monkeypatch.delenv("AGENT_MODEL", raising=False)
    monkeypatch.delenv("AGENT_REASONING_EFFORT", raising=False)

    settings = load_settings(require_postgres=False, require_gmail=False, require_agent=True)

    assert settings.agent is not None
    assert settings.agent.model == "gpt-5.6-sol"
    assert settings.agent.reasoning_effort == "medium"
    assert settings.agent.docker_image.startswith("personal-data-warehouse-agent:")
    assert len(settings.agent.docker_image.rsplit(":", 1)[1]) == 6


def test_load_settings_uses_container_hostname_for_non_bridge_agent_network(monkeypatch) -> None:
    monkeypatch.setenv("AGENT_DOCKER_NETWORK", "coolify")
    monkeypatch.setattr("personal_data_warehouse.agent_runner.socket.gethostname", lambda: "dagster-container")

    settings = load_settings(require_postgres=False, require_gmail=False, require_agent=True)

    assert settings.agent is not None
    assert settings.agent.docker_network == "coolify"
    assert settings.agent.tool_proxy_public_host == "dagster-container"


def test_agent_resource_builds_container_config() -> None:
    resource = AgentResource(
        docker_image="pdw-agent:latest",
        provider="claude",
        model="claude-test",
        reasoning_effort="high",
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
    assert config.reasoning_effort == "high"
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


def test_jwt_expiry_epoch_parses_and_rejects() -> None:
    from personal_data_warehouse.agent_runner import jwt_expiry_epoch
    import base64 as b64

    def jwt(claims: dict) -> str:
        payload = b64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
        return f"eyJhbGciOiJIUzI1NiJ9.{payload}.sig"

    assert jwt_expiry_epoch(jwt({"exp": 1800000000})) == 1800000000.0
    assert jwt_expiry_epoch(jwt({"sub": "x"})) is None
    assert jwt_expiry_epoch("not-a-jwt") is None
    assert jwt_expiry_epoch("") is None


def test_provider_auth_lock_shared_holders_overlap(tmp_path, monkeypatch) -> None:
    from personal_data_warehouse.agent_runner import provider_auth_lock

    monkeypatch.setenv("AGENT_AUTH_LOCK_DIR", str(tmp_path))
    entered = threading.Event()
    release = threading.Event()
    overlapped = threading.Event()

    def hold_shared() -> None:
        with provider_auth_lock("codex", exclusive=False):
            entered.set()
            release.wait(timeout=10)

    holder = threading.Thread(target=hold_shared)
    holder.start()
    assert entered.wait(timeout=5)
    # A second SHARED holder gets in while the first still holds the lock.
    with provider_auth_lock("codex", exclusive=False):
        overlapped.set()
    assert overlapped.is_set()
    release.set()
    holder.join(timeout=5)


def test_auth_lock_mode_auto_shares_far_from_expiry(tmp_path, monkeypatch) -> None:
    from personal_data_warehouse import agent_runner as ar

    config = AgentContainerConfig(image="img", provider="codex")
    runner = ContainerAgentRunner(config)
    monkeypatch.setattr(ar.ContainerAgentRunner, "_read_codex_auth_exp", lambda self: time.time() + 6 * 3600)
    ar._AUTH_EXP_CACHE.clear()

    # Default (exclusive) mode never shares.
    monkeypatch.delenv("AGENT_AUTH_LOCK_MODE", raising=False)
    assert runner._auth_lock_must_be_exclusive("codex") is True

    monkeypatch.setenv("AGENT_AUTH_LOCK_MODE", "auto")
    assert runner._auth_lock_must_be_exclusive("codex") is False
    # Non-codex providers stay exclusive even in auto mode.
    assert runner._auth_lock_must_be_exclusive("claude") is True

    # Near expiry (inside the refresh margin) goes exclusive so the refresh
    # runs alone.
    monkeypatch.setattr(ar.ContainerAgentRunner, "_read_codex_auth_exp", lambda self: time.time() + 60)
    ar._AUTH_EXP_CACHE.clear()
    assert runner._auth_lock_must_be_exclusive("codex") is True

    # Unknown expiry (helper failed) is treated as exclusive.
    monkeypatch.setattr(ar.ContainerAgentRunner, "_read_codex_auth_exp", lambda self: None)
    ar._AUTH_EXP_CACHE.clear()
    assert runner._auth_lock_must_be_exclusive("codex") is True
