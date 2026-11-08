"""An agent turn lands -- in the warehouse and on the timeline -- seconds after its device posts it.

Measured 2026-11-08 over seven days of timeline.events first_seen_at minus
occurred_at: Claude Code, Codex, OpenClaw and Argon turns landed p50 3-4 min,
p99 6-7 min. Almost none of it was work. The uploader ran every five
minutes; the inbox waited for a 60 s sensor and a Dagster run start (~5 s);
the run's fast lane spent 15.2 s, 14 of them compiling JIT code for a 2 ms
session roll-up. Now the device watches its transcripts (`pdw ingest
agent-sessions --watch`), the app NOTIFYs this worker when it stores a batch,
the worker runs the asset's own ingest and lands the turns through the fast
lane, and the timeline engine runs with JIT off. The sensor stays the floor.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dagster import build_asset_context

from personal_data_warehouse import agent_sessions_landing_worker as worker
from personal_data_warehouse.defs import agent_sessions_drive_ingest as agent_defs

URL = "postgresql://example/warehouse"
ROOT = Path(__file__).resolve().parents[1]


def _settings():
    return SimpleNamespace(postgres_database_url=URL, agent_sessions=object())


def _summary(events: int = 0, files: int = 0, batches: int = 1):
    return SimpleNamespace(
        batches_seen=batches, events_written=events, files_promoted=batches, files_written=files,
        codex_rows_renormalized=0,
    )


def test_the_processor_reads_nothing_when_the_inbox_is_empty(monkeypatch) -> None:
    monkeypatch.setattr(worker, "load_settings", lambda **_k: _settings())
    monkeypatch.setattr(worker, "agent_sessions_inbox_has_batches", lambda _s: False)
    monkeypatch.setattr(
        worker, "ingest_agent_sessions_inbox", lambda **_k: (_ for _ in ()).throw(AssertionError("ingest ran"))
    )
    assert worker.AgentSessionsLandingProcessor().process_pending() is False


def test_the_processor_runs_the_assets_own_ingest_then_lands_the_turns(monkeypatch) -> None:
    settings = _settings()
    monkeypatch.setattr(worker, "load_settings", lambda **_k: settings)
    monkeypatch.setattr(worker, "agent_sessions_inbox_has_batches", lambda _s: True)
    seen: list[object] = []
    summary = _summary(events=3)
    monkeypatch.setattr(
        worker, "ingest_agent_sessions_inbox", lambda *, settings, logger: seen.append(("ingest", settings)) or summary
    )
    monkeypatch.setattr(
        worker,
        "land_agent_sessions_on_timeline",
        lambda *, settings, summary, logger: seen.append(("timeline", summary)) or {"rows": 3},
    )
    assert worker.AgentSessionsLandingProcessor().process_pending() is True, (
        "a batch that landed may have had another arrive behind it"
    )
    assert seen == [("ingest", settings), ("timeline", summary)]


def test_landing_picks_the_adapters_for_what_the_ingest_wrote(monkeypatch) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(
        agent_defs,
        "land_sources_on_timeline",
        lambda *, postgres_url, sources, logger: calls.append(list(sources)) or {"rows": 1},
    )
    log = build_asset_context().log
    agent_defs.land_agent_sessions_on_timeline(settings=_settings(), summary=_summary(events=2), logger=log)
    agent_defs.land_agent_sessions_on_timeline(settings=_settings(), summary=_summary(files=1), logger=log)
    assert agent_defs.land_agent_sessions_on_timeline(settings=_settings(), summary=_summary(), logger=log) == {
        "enabled": False,
        "rows": 0,
    }
    assert agent_defs.land_agent_sessions_on_timeline(settings=_settings(), summary=None, logger=log)["rows"] == 0
    assert calls == [["agent_sessions"], ["muse"]]


def test_the_asset_and_the_worker_share_one_ingest() -> None:
    source = (ROOT / "src/personal_data_warehouse/defs/agent_sessions_drive_ingest.py").read_text()
    asset_body = source.split("def agent_sessions_drive_ingest(", 1)[1].split("\n\n\n", 1)[0]
    assert "ingest_agent_sessions_inbox(" in asset_body
    assert "AgentSessionsDriveIngestRunner(" not in asset_body


def test_the_worker_listens_on_the_channel_the_app_notifies() -> None:
    app_ingest = (ROOT / "app/internal/server/ingest.go").read_text()
    assert f'agentSessionsLandingChannel = "{worker.AGENT_SESSIONS_LANDING_CHANNEL}"' in app_ingest


def test_the_dagster_container_supervises_the_agent_sessions_landing_worker() -> None:
    start_script = (ROOT / "docker/start-dagster.sh").read_text()
    assert "python -m personal_data_warehouse.agent_sessions_landing_worker" in start_script
    assert "AGENT_SESSIONS_LANDING_WORKER_ENABLED" in start_script
    assert 'agent_sessions_worker_pid=""' in start_script
    assert '"$agent_sessions_worker_pid"' in start_script.split("shutdown()", 1)[1].split("}", 1)[0]
