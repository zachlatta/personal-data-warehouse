from __future__ import annotations

from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context

from personal_data_warehouse.config import GitHubConfig
from personal_data_warehouse.definitions import defs
from personal_data_warehouse.defs import github_sync as gh_defs


def test_the_job_and_sensor_are_registered_with_a_runtime_cap() -> None:
    loaded = defs()
    assert loaded.resolve_job_def("github_sync_job") is not None
    assert loaded.resolve_sensor_def("github_sync_sensor") is not None
    assert int(gh_defs.github_sync_job.tags["dagster/max_runtime"]) < 14400


def test_tokens_win_over_the_proxy_and_the_proxy_over_anonymous() -> None:
    tokens = gh_defs.github_credentials(
        GitHubConfig(account="me", tokens=("a", "b"), proxy_base_url="https://p.test", proxy_api_key="k")
    )
    assert [c.scope.split(":")[0] for c in tokens] == ["token", "token"]
    assert len({c.scope for c in tokens}) == 2
    proxy = gh_defs.github_credentials(GitHubConfig(account="me", proxy_base_url="https://p.test", proxy_api_key="k"))
    assert [c.scope for c in proxy] == ["proxy"]
    assert [c.scope for c in gh_defs.github_credentials(GitHubConfig(account="me"))] == ["anonymous"]


def test_the_sensor_skips_until_configured_then_polls_on_its_interval(monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_ACCOUNT", raising=False)
    monkeypatch.setenv("POSTGRES_DATABASE_URL", "postgresql://unused")
    instance = DagsterInstance.ephemeral()
    result = gh_defs.github_sync_sensor(build_sensor_context(instance=instance))
    assert isinstance(result, SkipReason) and "GITHUB_ACCOUNT" in str(result.skip_message)

    monkeypatch.setenv("GITHUB_ACCOUNT", "me")
    context = build_sensor_context(instance=instance)
    assert isinstance(gh_defs.github_sync_sensor(context), RunRequest)
    again = gh_defs.github_sync_sensor(build_sensor_context(instance=instance, cursor=context.cursor))
    assert isinstance(again, SkipReason) and "poll interval" in str(again.skip_message)
