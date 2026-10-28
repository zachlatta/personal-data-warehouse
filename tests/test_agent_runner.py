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
    cli_tool_manifest,
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


def test_agent_run_event_rows_redact_recognizable_credentials_in_json_and_text() -> None:
    now = datetime.now(tz=UTC)
    secrets = {
        "access": "access-json-secret-123",
        "bearer": "bearer-json-secret-456",
        "embedded": "embedded-json-secret-789",
        "text_bearer": "bearer-text-secret-012",
        "text_api_key": "api-text-secret-345",
        "text_refresh": "refresh-text-secret-678",
        "provider_key": "sk-proj-FAKECREDENTIAL1234567890",
        "jwt": "eyJFAKEHEADER.eyJFAKEPAYLOAD.eyJFAKESIGNATURE",
        "run_error": "error-diagnostic-secret-901",
    }
    result = AgentRunResult(
        run_id="run-sensitive",
        provider="codex",
        model="gpt-test",
        task_type="receipt_transaction_match",
        subject_id="ft-1",
        prompt_version="prompt-v1",
        input_sha256="sha",
        status="error",
        final_output_json={},
        error=f"ANTHROPIC_API_KEY={secrets['run_error']}",
        exit_code=1,
        started_at=now,
        completed_at=now,
        events=[
            AgentRunEvent(
                event_index=7,
                stream="stderr",
                event_type="turn.failed",
                event_json={
                    "type": "turn.failed",
                    "access_token": secrets["access"],
                    "headers": {"Authorization": f"Bearer {secrets['bearer']}"},
                    "message": f"OPENAI_API_KEY={secrets['embedded']}",
                },
                text=(
                    f"Authorization: Bearer {secrets['text_bearer']} "
                    f"OPENAI_API_KEY={secrets['text_api_key']} "
                    f"\"refresh_token\":\"{secrets['text_refresh']}\" "
                    f"{secrets['provider_key']} {secrets['jwt']}"
                ),
                created_at=now,
            )
        ],
    )

    row = agent_run_event_rows(result)[0]
    run_row = agent_run_row(result)
    serialized = row["event_json"] + "\n" + row["text"] + "\n" + run_row["error"]

    assert all(secret not in serialized for secret in secrets.values())
    assert json.loads(row["event_json"])["access_token"] == "[REDACTED]"
    assert json.loads(row["event_json"])["headers"]["Authorization"] == "[REDACTED]"
    assert serialized.count("[REDACTED]") >= 6
    assert row["event_index"] == 7
    assert row["event_type"] == "turn.failed"


def test_agent_run_event_rows_bound_oversized_json_and_text_without_losing_event_identity() -> None:
    now = datetime.now(tz=UTC)
    result = AgentRunResult(
        run_id="run-oversized",
        provider="codex",
        model="gpt-test",
        task_type="receipt_transaction_match",
        subject_id="ft-1",
        prompt_version="prompt-v1",
        input_sha256="sha",
        status="error",
        final_output_json={},
        error="failed",
        exit_code=1,
        started_at=now,
        completed_at=now,
        events=[
            AgentRunEvent(
                event_index=23,
                stream="stdout",
                event_type="turn.failed",
                event_json={"type": "turn.failed", "message": "j" * 100_000},
                text="diagnostic-start " + ("t" * 100_000) + " diagnostic-end",
                created_at=now,
            )
        ],
    )

    row = agent_run_event_rows(result)[0]
    stored_json = json.loads(row["event_json"])

    assert len(row["event_json"]) <= 32_000
    assert len(row["text"]) <= 16_000
    assert stored_json["_pdw_truncated"] is True
    assert stored_json["type"] == "turn.failed"
    assert row["text"].startswith("diagnostic-start ")
    assert row["text"].endswith(" diagnostic-end")
    assert "truncated" in row["text"]
    assert row["run_id"] == "run-oversized"
    assert row["event_index"] == 23
    assert row["stream"] == "stdout"
    assert row["event_type"] == "turn.failed"
    assert row["created_at"] == now


def test_agent_run_tool_call_rows_names_each_pdw_subcommand() -> None:
    now = datetime.now(tz=UTC)
    commands = [
        ("pdw search --priority self,direct,cc budget", "pdw search"),
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


def test_agent_cli_manifest_teaches_search_before_schema() -> None:
    manifest = cli_tool_manifest()

    assert manifest.index("pdw search --priority") < manifest.index("pdw schema")
    assert "Every hit includes `priority` and `ref`" in manifest


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


def test_container_agent_runner_prefers_structured_fatal_event_over_stderr_warning(tmp_path) -> None:
    def fake_run(command, **kwargs):
        if is_run_volume_copy(command):
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if is_agent_container_run(command):
            return subprocess.CompletedProcess(
                command,
                1,
                stdout=(
                    '{"type":"turn.failed","error":{"message":"structured fatal failure"}}\n'
                ),
                stderr="warning: executable directory is not on PATH\n",
            )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    result = ContainerAgentRunner(
        AgentContainerConfig(image="pdw-agent:latest", runs_dir=tmp_path),
        runner=fake_run,
    ).run(AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-1"))

    assert result.status == "error"
    assert "structured fatal failure" in result.error
    assert "not on PATH" not in result.error


def test_container_agent_runner_labels_plain_stderr_as_a_bounded_diagnostic(tmp_path) -> None:
    def fake_run(command, **kwargs):
        if is_run_volume_copy(command):
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if is_agent_container_run(command):
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="x" * 1_200)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    result = ContainerAgentRunner(
        AgentContainerConfig(image="pdw-agent:latest", runs_dir=tmp_path),
        runner=fake_run,
    ).run(AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-1"))

    assert result.status == "error"
    assert "last diagnostic:" in result.error
    assert result.error.endswith("x" * 1_000)
    assert len(result.error) < 1_100


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


def test_agent_config_defaults_codex_to_gpt_6_1_sol_with_medium_reasoning(monkeypatch) -> None:
    monkeypatch.setenv("AGENT_PROVIDER", "codex")
    monkeypatch.delenv("AGENT_MODEL", raising=False)
    monkeypatch.delenv("AGENT_REASONING_EFFORT", raising=False)

    config = agent_config_from_env()

    assert config.model == "gpt-6.1-sol"
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
    assert 'model="${model:-gpt-6.1-sol}"' in entrypoint
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
    assert settings.agent.model == "gpt-6.1-sol"
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


def test_go_build_images_pin_the_module_toolchain_patch() -> None:
    """A cached floating image must not be older than go.mod's minimum Go."""
    import re

    root = Path(__file__).resolve().parents[1]
    version = re.search(r"^go (\d+\.\d+\.\d+)$", (root / "app/go.mod").read_text(), re.MULTILINE)
    assert version is not None
    for path in ("app/Dockerfile", "docker/agent.Dockerfile"):
        assert f"FROM golang:{version.group(1)}-bookworm AS " in (root / path).read_text(), path


def test_claude_tool_blocks_pair_by_id_and_keep_identical_calls() -> None:
    from personal_data_warehouse.agent_runner import agent_events_from_streams, extract_tool_name, extract_tool_arguments, extract_tool_result
    from personal_data_warehouse.apple_voice_memos_enrichment import count_warehouse_cli_calls

    now = datetime.now(tz=UTC)
    assistant = {"type": "assistant", "message": {"role": "assistant", "content": [
        {"type": "text", "text": "Research"},
        {"type": "tool_use", "id": "tool-a", "name": "Bash", "input": {"command": "pdw search budget"}},
        {"type": "tool_use", "id": "tool-b", "name": "Bash", "input": {"command": "pdw search budget"}},
        {"type": "tool_use", "id": "tool-c", "name": "Read", "input": {"file_path": "input.json"}},
    ]}}
    returned = {"type": "user", "message": {"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": "tool-b", "content": "second", "is_error": True},
        {"type": "tool_result", "tool_use_id": "tool-a", "content": [{"type": "text", "text": "first"}]},
    ]}}
    events = agent_events_from_streams(stdout="\n".join(map(json.dumps, [assistant, returned])), stderr="", created_at=now)
    result = AgentRunResult("run-claude", "claude", "test", "generic", "", "", "", "completed", {}, "", 0, now, now, events)
    rows = agent_run_tool_call_rows(result)
    assert [r["tool_name"] for r in rows] == ["pdw search", "pdw search", "Read"]
    assert len({(r["run_id"], r["event_index"], r["tool_name"]) for r in rows}) == 3
    assert [r["event_index"] for r in rows] == [2, 3, 4]
    assert json.loads(rows[0]["result_json"])["output"] == "first"
    assert json.loads(rows[1]["result_json"])["output"] == "second"
    assert rows[1]["error"]
    assert json.loads(rows[2]["result_json"]) == {}
    assert json.loads(rows[0]["arguments_json"])["_pdw_trace"] == {
        "tool_use_id": "tool-a", "source_event_index": 0, "content_block_index": 1,
    }
    assert count_warehouse_cli_calls(events) == 2
    assert extract_tool_name(assistant) == "pdw search"
    assert extract_tool_arguments(assistant) == {"command": "pdw search budget"}
    assert extract_tool_result(returned)["output"] == "second"


def test_provider_shell_attribution_uses_same_parser() -> None:
    from personal_data_warehouse.agent_runner import extract_tool_name, extract_tool_arguments
    for command, expected in [
        ("pdw context 'gmail_email:a|m'", "pdw context"),
        ("pdw call get_object --data '{}'", "pdw call get_object"),
        ("PDW_VALIDATE_JSON answer.json schema.json", "pdw-validate-json"),
        ("echo hello", "bash"),
    ]:
        codex = {"item": {"type": "command_execution", "command": command}}
        claude = {"type": "assistant", "message": {"content": [
            {"type": "tool_use", "id": "tool-a", "name": "Bash", "input": {"command": command}}
        ]}}
        assert extract_tool_name(codex) == extract_tool_name(claude) == expected
        assert extract_tool_arguments(codex) == extract_tool_arguments(claude) == {"command": command}


def test_run_with_pdw_returns_private_exclusion_counts(tmp_path, monkeypatch) -> None:
    from tests.warehouse_app_stub import StubWarehouseApp
    from dataclasses import replace

    now = datetime.now(tz=UTC)
    app = StubWarehouseApp(responses={"search": {"rows": [{"ref": "agent_session:codex|s1"}]}})
    runner = ContainerAgentRunner(AgentContainerConfig(image="test", runs_dir=tmp_path,
        tool_proxy_bind_host="127.0.0.1", tool_proxy_public_host="127.0.0.1"))
    plain = AgentRunResult("run", "codex", "test", "generic", "", "", "", "failed", {}, "test", 1, now, now, [])
    captured = {}

    def fake_run(req):
        captured.update(req.extra_env)
        response = request.urlopen(request.Request(req.extra_env["PDW_API_URL"] + "/api/tools/search",
            data=b'{"query":"test"}',
            headers={"authorization": "Bearer " + req.extra_env["PDW_SECRET_TOKEN"]},
            method="POST"), timeout=10)
        with response:
            assert json.load(response)["data"]["rows"] == []
        return replace(plain, run_id=req.run_id)

    monkeypatch.setattr(runner, "run", fake_run)
    try:
        result = runner.run_with_pdw(AgentRunRequest(prompt="test", schema={}),
            app=WarehouseAppConfig(app.base_url, "test-token"), exclude_adapters=["agent_session"])
    finally:
        app.close()
    assert result.status == "failed"
    assert result.exclusion_counts == {"search_rows_dropped": 1, "context_rows_dropped": 0,
                                       "sql_rows_dropped": 0, "context_calls_refused": 0}
    assert not any("exclu" in key for key in captured)
    assert not plain.exclusion_counts


def test_agent_resource_forwards_run_exclusions(monkeypatch) -> None:
    from types import SimpleNamespace
    captured = {}

    def run_with_pdw(req, **kwargs):
        captured.update(kwargs)
        captured["provider"] = req.provider
        return "result"

    monkeypatch.setattr(AgentResource, "runner", lambda self: SimpleNamespace(run_with_pdw=run_with_pdw))
    resource = AgentResource(docker_image="test", provider="claude")
    assert resource.run_with_pdw(AgentRunRequest(prompt="test", schema={}),
        exclude_refs=["gmail_email:a|m"], exclude_adapters=["agent_session"], max_rows=7) == "result"
    assert captured == {"app": None, "provider": "claude", "max_rows": 7,
                        "exclude_refs": ["gmail_email:a|m"], "exclude_adapters": ["agent_session"]}


def test_unmatched_claude_result_is_not_a_call() -> None:
    from personal_data_warehouse.agent_runner import agent_events_from_streams
    now = datetime.now(tz=UTC)
    events = agent_events_from_streams(stdout=json.dumps({
        "type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "unknown", "content": "orphan"},
        ]}}), stderr="", created_at=now)
    result = AgentRunResult("run", "claude", "test", "generic", "", "", "", "completed", {}, "", 0, now, now, events)
    assert agent_run_tool_call_rows(result) == []


def test_cancelled_agent_run_stops_container_and_removes_credentials_volume(tmp_path) -> None:
    calls: list[list[str]] = []

    class SimulatedCancellation(BaseException):
        pass

    def fake_run(command, **kwargs):
        calls.append(command)
        if is_agent_container_run(command):
            raise SimulatedCancellation("dagster step interrupted")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    runner = ContainerAgentRunner(
        AgentContainerConfig(image="pdw-agent:latest", runs_dir=tmp_path),
        runner=fake_run,
    )
    try:
        runner.run(AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-cancel"))
    except SimulatedCancellation:
        pass
    else:
        raise AssertionError("expected SimulatedCancellation to propagate")

    stop_index = next(
        index
        for index, call in enumerate(calls)
        if call == ["docker", "stop", "--time", "10", "pdw-agent-run-cancel"]
    )
    remove_index = next(
        index
        for index, call in enumerate(calls)
        if call[:4] == ["docker", "volume", "rm", "-f"]
    )
    assert stop_index < remove_index
    # Cancelled runs must not persist partial/untrusted credentials back to the shared auth volume.
    assert not any(
        "type=volume,src=pdw-agent-auth,dst=/persistent-auth" in call
        and "type=volume,src=pdw-agent-auth,dst=/persistent-auth,readonly" not in call
        for call in calls
    )


def test_remove_credentials_volume_force_removes_containers_when_volume_is_in_use(tmp_path) -> None:
    calls: list[tuple[list[str], dict]] = []
    volume_rm_attempts = 0

    def fake_run(command, **kwargs):
        nonlocal volume_rm_attempts
        calls.append((command, kwargs))
        if command[:2] == ["docker", "stop"]:
            raise subprocess.TimeoutExpired(command, timeout=30)
        if command[:4] == ["docker", "volume", "rm", "-f"]:
            volume_rm_attempts += 1
            if volume_rm_attempts == 1:
                return subprocess.CompletedProcess(
                    command,
                    1,
                    stdout="",
                    stderr="Error response from daemon: remove vol: volume is in use - [abc]",
                )
        if is_agent_container_run(command):
            raise subprocess.TimeoutExpired(command, timeout=1)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    result = ContainerAgentRunner(
        AgentContainerConfig(image="pdw-agent:latest", runs_dir=tmp_path, timeout_seconds=1),
        runner=fake_run,
    ).run(AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-busy"))

    assert result.status == "error"
    assert volume_rm_attempts == 2
    assert any(
        call == ["docker", "rm", "-f", "pdw-agent-run-busy"] and kwargs.get("timeout") == 30
        for call, kwargs in calls
    )


def test_shield_cleanup_signals_defers_and_redelivers_cancellation_signal(monkeypatch) -> None:
    import signal
    from personal_data_warehouse.agent_runner import _shield_cleanup_signals

    events: list[str] = []

    class DeferredInterrupt(BaseException):
        pass

    def previous_handler(signum, frame):
        events.append(f"delivered:{signum}")
        raise DeferredInterrupt(f"signal {signum}")

    original = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, previous_handler)
    try:
        try:
            with _shield_cleanup_signals():
                events.append("cleanup:start")
                active_handler = signal.getsignal(signal.SIGTERM)
                assert callable(active_handler)
                active_handler(signal.SIGTERM, None)
                events.append("cleanup:after-signal")
        except DeferredInterrupt:
            events.append("caught:after-cleanup")
        else:
            raise AssertionError("expected deferred signal handler to raise after cleanup")
        assert signal.getsignal(signal.SIGTERM) is previous_handler

        raised_signals: list[int] = []
        monkeypatch.setattr(signal, "raise_signal", lambda sig: raised_signals.append(sig))
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        with _shield_cleanup_signals():
            active_handler = signal.getsignal(signal.SIGTERM)
            assert callable(active_handler)
            active_handler(signal.SIGTERM, None)
            assert raised_signals == []
        assert raised_signals == [signal.SIGTERM]
    finally:
        signal.signal(signal.SIGTERM, original)

    assert events == [
        "cleanup:start",
        "cleanup:after-signal",
        f"delivered:{signal.SIGTERM}",
        "caught:after-cleanup",
    ]


def test_sweep_stale_credentials_volumes_respects_margin_and_running_containers() -> None:
    from personal_data_warehouse.agent_runner import sweep_stale_credentials_volumes

    now = datetime(2026, 10, 28, 20, 0, 0, tzinfo=UTC)
    vol_old_idle = "pdw-agent-credentials-agent-old-idle-11111111111111111111111111111111"
    vol_boundary = "pdw-agent-credentials-agent-boundary-22222222222222222222222222222222"
    vol_mounted = "pdw-agent-credentials-agent-mounted-33333333333333333333333333333333"
    vol_named_running = "pdw-agent-credentials-agent-running-44444444444444444444444444444444"

    inspect_payload = [
        # 2401s old (> 1800s timeout + 600s margin): eligible for removal
        {"Name": vol_old_idle, "CreatedAt": "2026-10-28T19:19:59Z"},
        # 1801s old (timeout + 1s, inside the 600s stage/stop/persist margin): must be kept
        {"Name": vol_boundary, "CreatedAt": "2026-10-28T19:29:59Z"},
        # 3600s old, but mounted by a running container: must be kept
        {"Name": vol_mounted, "CreatedAt": "2026-10-28T19:00:00Z"},
        # 3600s old, and its pdw-agent-<run_id> container is still running: must be kept
        {"Name": vol_named_running, "CreatedAt": "2026-10-28T19:00:00Z"},
    ]
    calls: list[list[str]] = []

    def fake_run(command, **kwargs):
        calls.append(command)
        if command[:3] == ["docker", "volume", "ls"]:
            return subprocess.CompletedProcess(
                command,
                0,
                stdout="\n".join([vol_old_idle, vol_boundary, vol_mounted, vol_named_running]) + "\n",
                stderr="",
            )
        if command[:3] == ["docker", "volume", "inspect"]:
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=json.dumps(inspect_payload),
                stderr="",
            )
        if command[:2] == ["docker", "ps"]:
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=(
                    f"some-container\t{vol_mounted},pdw-agent-runs\n"
                    "pdw-agent-agent-running\tpdw-agent-runs\n"
                ),
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    removed = sweep_stale_credentials_volumes(
        timeout_seconds=1800,
        sweep_margin_seconds=600,
        runner=fake_run,
        now=now,
    )

    assert removed == [vol_old_idle]
    rm_calls = [call for call in calls if call[:4] == ["docker", "volume", "rm", "-f"]]
    assert rm_calls == [["docker", "volume", "rm", "-f", vol_old_idle]]


def test_sweep_stale_credentials_volumes_fails_closed_when_docker_ps_errors() -> None:
    from personal_data_warehouse.agent_runner import sweep_stale_credentials_volumes

    now = datetime(2026, 10, 28, 20, 0, 0, tzinfo=UTC)
    vol_old = "pdw-agent-credentials-agent-old-11111111111111111111111111111111"
    calls: list[list[str]] = []

    def fake_run(command, **kwargs):
        calls.append(command)
        if command[:3] == ["docker", "volume", "ls"]:
            return subprocess.CompletedProcess(command, 0, stdout=f"{vol_old}\n", stderr="")
        if command[:3] == ["docker", "volume", "inspect"]:
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=json.dumps([{"Name": vol_old, "CreatedAt": "2026-10-28T18:00:00Z"}]),
                stderr="",
            )
        if command[:2] == ["docker", "ps"]:
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="daemon error")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    removed = sweep_stale_credentials_volumes(timeout_seconds=1800, runner=fake_run, now=now)

    assert removed == []
    assert not any(call[:4] == ["docker", "volume", "rm", "-f"] for call in calls)


def test_container_agent_runner_continues_when_startup_sweep_fails(tmp_path, monkeypatch) -> None:
    from personal_data_warehouse import agent_runner as ar

    monkeypatch.setattr(ar, "_STARTUP_CREDENTIALS_SWEEP_DONE", False)
    volume_copy_calls = 0

    def fake_run(command, **kwargs):
        nonlocal volume_copy_calls
        if command[:3] == ["docker", "volume", "ls"]:
            raise OSError("docker socket temporarily unavailable")
        if is_run_volume_copy(command):
            volume_copy_calls += 1
            if volume_copy_calls == 2:
                (tmp_path / "run-sweep-err" / "final.json").write_text('{"ok":true}', encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    result = ContainerAgentRunner(
        AgentContainerConfig(image="pdw-agent:latest", runs_dir=tmp_path),
        runner=fake_run,
    ).run(AgentRunRequest(prompt="Return JSON", schema={"type": "object"}, run_id="run-sweep-err"))

    assert result.status == "completed"
    assert result.final_output_json == {"ok": True}

