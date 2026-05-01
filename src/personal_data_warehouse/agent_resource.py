from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dagster import ConfigurableResource

from personal_data_warehouse.agent_runner import (
    DEFAULT_AGENT_AUTH_VOLUME,
    DEFAULT_AGENT_CPUS,
    DEFAULT_AGENT_MEMORY,
    DEFAULT_AGENT_NETWORK,
    DEFAULT_AGENT_PIDS_LIMIT,
    DEFAULT_AGENT_RUNS_DIR,
    DEFAULT_AGENT_RUNS_VOLUME,
    AgentContainerConfig,
    AgentRunRequest,
    AgentRunResult,
    ContainerAgentRunner,
)

if TYPE_CHECKING:
    from personal_data_warehouse.config import AgentConfig


class AgentResource(ConfigurableResource):
    enabled: bool = True
    provider: str = "codex"
    model: str = ""
    docker_image: str = ""
    auth_volume: str = DEFAULT_AGENT_AUTH_VOLUME
    runs_volume: str = DEFAULT_AGENT_RUNS_VOLUME
    runs_dir: str = DEFAULT_AGENT_RUNS_DIR
    network: str = DEFAULT_AGENT_NETWORK
    memory: str = DEFAULT_AGENT_MEMORY
    cpus: str = DEFAULT_AGENT_CPUS
    pids_limit: int = DEFAULT_AGENT_PIDS_LIMIT
    timeout_seconds: int = 1800
    tool_proxy_bind_host: str = "0.0.0.0"
    tool_proxy_public_host: str = "host.docker.internal"

    @classmethod
    def disabled(cls) -> AgentResource:
        return cls(enabled=False)

    @classmethod
    def from_config(cls, config: AgentConfig) -> AgentResource:
        return cls(
            docker_image=config.docker_image,
            provider=config.provider,
            model=config.model,
            auth_volume=config.auth_volume,
            runs_volume=config.runs_volume,
            runs_dir=config.runs_dir,
            network=config.docker_network,
            memory=config.docker_memory,
            cpus=config.docker_cpus,
            pids_limit=config.docker_pids_limit,
            timeout_seconds=config.timeout_seconds,
            tool_proxy_bind_host=config.tool_proxy_bind_host,
            tool_proxy_public_host=config.tool_proxy_public_host,
        )

    @property
    def is_configured(self) -> bool:
        return self.enabled and bool(self.docker_image.strip())

    def container_config(self) -> AgentContainerConfig:
        self._raise_if_unconfigured()
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
            tool_proxy_bind_host=self.tool_proxy_bind_host,
            tool_proxy_public_host=self.tool_proxy_public_host,
        )

    def runner(self) -> ContainerAgentRunner:
        return ContainerAgentRunner(self.container_config())

    def run(self, request: AgentRunRequest) -> AgentRunResult:
        return self.runner().run(self._effective_request(request))

    def run_with_clickhouse(
        self,
        request: AgentRunRequest,
        *,
        warehouse,
        max_rows: int = 50,
        max_field_chars: int = 3000,
    ) -> AgentRunResult:
        effective_request = self._effective_request(request)
        return self.runner().run_with_clickhouse(
            effective_request,
            warehouse=warehouse,
            max_rows=max_rows,
            max_field_chars=max_field_chars,
        )

    def _effective_request(self, request: AgentRunRequest) -> AgentRunRequest:
        self._raise_if_unconfigured()
        if request.provider is not None and request.model is not None:
            return request
        return AgentRunRequest(
            prompt=request.prompt,
            schema=request.schema,
            task_type=request.task_type,
            subject_id=request.subject_id,
            prompt_version=request.prompt_version,
            run_id=request.run_id,
            provider=request.provider or self.provider,
            model=request.model if request.model is not None else self.model,
            extra_env=request.extra_env,
        )

    def _raise_if_unconfigured(self) -> None:
        if not self.is_configured:
            raise RuntimeError("AgentResource is not configured. Enable agent settings before running agent assets.")
