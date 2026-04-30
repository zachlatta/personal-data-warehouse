from __future__ import annotations

from dagster import AssetKey, DagsterInstance, Definitions, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.defs.calendar_sync import calendar_event_sync
from personal_data_warehouse.defs import apple_voice_memos_enrichment as apple_voice_memos_enrichment_defs
from personal_data_warehouse.defs.apple_voice_memos_drive_ingest import apple_voice_memos_drive_ingest
from personal_data_warehouse.defs.apple_voice_memos_enrichment import (
    DEFAULT_VOICE_MEMOS_ENRICHMENT_BATCH_SIZE,
    UNCONFIGURED_AGENT_RESOURCE,
    apple_voice_memos_enrichment,
    apple_voice_memos_enrichment_job,
    apple_voice_memos_enrichment_hourly,
    apple_voice_memos_enrichment_model,
    apple_voice_memos_enrichment_prompt_version,
    apple_voice_memos_enrichment_provider,
    defs,
)
from personal_data_warehouse.defs.apple_voice_memos_transcription import apple_voice_memos_transcription
from personal_data_warehouse.apple_voice_memos_enrichment import (
    AGENT_ENRICHMENT_PROMPT_VERSION,
    DEFAULT_ENRICHMENT_LOOKBACK_WEEKS,
)


def test_apple_voice_memos_enrichment_job_selects_asset() -> None:
    assert apple_voice_memos_enrichment_job.name == "apple_voice_memos_enrichment_job"


def test_apple_voice_memos_enrichment_defaults_to_all_recent_transcripts() -> None:
    assert DEFAULT_VOICE_MEMOS_ENRICHMENT_BATCH_SIZE == 0
    assert DEFAULT_ENRICHMENT_LOOKBACK_WEEKS == 12


def test_apple_voice_memos_enrichment_uses_agent_provider_model_and_prompt() -> None:
    settings = FakeSettings()

    assert apple_voice_memos_enrichment_provider(settings) == "agent_codex"
    assert apple_voice_memos_enrichment_model(settings) == "gpt-agent"
    assert apple_voice_memos_enrichment_prompt_version() == AGENT_ENRICHMENT_PROMPT_VERSION


def test_apple_voice_memos_enrichment_client_uses_configured_agent_over_unconfigured_resource() -> None:
    client = apple_voice_memos_enrichment_defs.apple_voice_memos_enrichment_client(
        settings=FakeSettings(),
        warehouse=object(),
        logger=None,
        agent=UNCONFIGURED_AGENT_RESOURCE,
    )

    assert client._agent.docker_image == "pdw-agent:latest"


def test_apple_voice_memos_defs_provides_default_agent_resource() -> None:
    repository = defs().get_repository_def()

    assert "agent" in repository.get_top_level_resources()


def test_apple_voice_memos_enrichment_depends_on_transcription_and_calendar() -> None:
    assert apple_voice_memos_enrichment.asset_deps[AssetKey("apple_voice_memos_enrichment")] == {
        AssetKey("calendar_event_sync"),
        AssetKey("apple_voice_memos_transcription"),
    }


def test_apple_voice_memos_enrichment_job_selects_full_upstream_lineage() -> None:
    repository = Definitions(
        assets=[
            calendar_event_sync,
            apple_voice_memos_drive_ingest,
            apple_voice_memos_transcription,
            apple_voice_memos_enrichment,
        ],
        resources={"agent": UNCONFIGURED_AGENT_RESOURCE},
    ).get_repository_def()

    assert apple_voice_memos_enrichment_job.selection.resolve(repository.asset_graph) == {
        AssetKey("calendar_event_sync"),
        AssetKey("apple_voice_memos_drive_ingest"),
        AssetKey("apple_voice_memos_transcription"),
        AssetKey("apple_voice_memos_enrichment"),
    }


def test_apple_voice_memos_enrichment_schedule_runs_hourly() -> None:
    assert apple_voice_memos_enrichment_hourly.cron_schedule == "17 * * * *"
    assert apple_voice_memos_enrichment_hourly.default_status.value == "RUNNING"


def test_apple_voice_memos_enrichment_backlog_sensor_runs_every_minute() -> None:
    sensor = apple_voice_memos_enrichment_defs.apple_voice_memos_enrichment_backlog_sensor

    assert sensor.minimum_interval_seconds == 60
    assert sensor.default_status.value == "RUNNING"


def test_apple_voice_memos_enrichment_backlog_sensor_skips_when_backlog_is_empty(monkeypatch) -> None:
    calls = []
    settings_calls = []
    monkeypatch.setattr(
        apple_voice_memos_enrichment_defs,
        "load_settings",
        lambda **kwargs: settings_calls.append(kwargs) or FakeSettings(),
    )
    monkeypatch.setattr(
        apple_voice_memos_enrichment_defs,
        "ClickHouseWarehouse",
        lambda _url: object(),
    )
    monkeypatch.setattr(
        apple_voice_memos_enrichment_defs,
        "load_enrichment_candidates",
        lambda *args, **kwargs: calls.append((args, kwargs)) or [],
    )

    with DagsterInstance.ephemeral() as instance:
        result = apple_voice_memos_enrichment_defs.apple_voice_memos_enrichment_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, SkipReason)
    assert "No unenriched Voice Memos" in result.skip_message
    assert settings_calls[0]["require_agent"] is True
    assert calls[0][1]["limit"] == 1
    assert calls[0][1]["model"] == "gpt-agent"


def test_apple_voice_memos_enrichment_backlog_sensor_launches_when_backlog_exists(monkeypatch) -> None:
    monkeypatch.setattr(
        apple_voice_memos_enrichment_defs,
        "load_settings",
        lambda **_kwargs: FakeSettings(),
    )
    monkeypatch.setattr(
        apple_voice_memos_enrichment_defs,
        "ClickHouseWarehouse",
        lambda _url: object(),
    )
    monkeypatch.setattr(
        apple_voice_memos_enrichment_defs,
        "load_enrichment_candidates",
        lambda *args, **kwargs: [{"recording_id": "memo-1"}],
    )

    with DagsterInstance.ephemeral() as instance:
        result = apple_voice_memos_enrichment_defs.apple_voice_memos_enrichment_backlog_sensor(
            build_sensor_context(instance=instance)
        )

    assert isinstance(result, RunRequest)
    assert result.tags == {"apple_voice_memos_trigger": "enrichment_backlog"}


class FakeSettings:
    clickhouse_url = "clickhouse://example"
    agent = type(
        "FakeAgentConfig",
        (),
        {
            "provider": "codex",
            "model": "gpt-agent",
            "docker_image": "pdw-agent:latest",
            "auth_volume": "auth-vol",
            "runs_volume": "runs-vol",
            "runs_dir": "/tmp/runs",
            "docker_network": "bridge",
            "docker_memory": "4g",
            "docker_cpus": "2",
            "docker_pids_limit": 512,
            "timeout_seconds": 1800,
            "tool_proxy_bind_host": "127.0.0.1",
            "tool_proxy_public_host": "127.0.0.1",
        },
    )()
