"""Every ingest asset that lands a chat source calls the timeline fast lane when it wrote rows.

Measured 2026-09-25: WhatsApp, iMessage, Slack DMs and Gmail all landed in
p50 4-5 min / p95 9 min, almost entirely two serial five-minute clocks (the
source's own poll, then timeline_sync). Calling the fast lane from the ingest
run itself removes the second clock. A run that wrote nothing must not pay
for it, and a fast-lane failure must never fail ingestion.
"""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from dagster import build_asset_context

from personal_data_warehouse.defs import agent_sessions_drive_ingest as agent_defs
from personal_data_warehouse.defs import apple_messages_drive_ingest as apple_messages_defs
from personal_data_warehouse.defs import gmail_sync as gmail_defs
from personal_data_warehouse.defs import slack_sync as slack_defs
from personal_data_warehouse.defs import whatsapp_drive_ingest as whatsapp_defs

URL = "postgresql://example/warehouse"


@contextmanager
def _acquired(**_kwargs):
    yield True


class _FakeWarehouse:
    def close(self) -> None:
        pass


class _Settings(SimpleNamespace):
    postgres_database_url = URL


def _runner_returning(summary):
    class _Runner:
        def __init__(self, **_kwargs) -> None:
            pass

        def sync(self):
            return summary

        def sync_all(self):
            return summary

    return _Runner


def _capture(monkeypatch, module):
    calls: list[dict] = []

    def fake_land(*, postgres_url, sources, logger):
        calls.append({"postgres_url": postgres_url, "sources": list(sources)})
        return {"enabled": True, "rows": 1, "adapters": [], "seconds": 0.1, "skipped": [], "errors": {}}

    monkeypatch.setattr(module, "land_sources_on_timeline", fake_land)
    return calls


def _whatsapp_summary(messages: int):
    return SimpleNamespace(
        batches_seen=1, chats_written=0, chat_participants_written=0, chats_backfilled=0,
        contacts_written=0, messages_written=messages, media_items_written=0, files_promoted=1,
    )


def _apple_messages_summary(messages: int):
    return SimpleNamespace(
        batches_seen=1, handles_written=0, chats_written=0, chat_handles_written=0,
        messages_written=messages, chat_messages_written=0, attachments_written=0, files_promoted=1,
    )


@pytest.mark.parametrize(
    ("module", "asset", "runner_name", "summary_for", "source", "extra"),
    [
        (whatsapp_defs, "whatsapp_drive_ingest", "WhatsAppDriveIngestRunner", _whatsapp_summary, "whatsapp",
         {"whatsapp": object()}),
        (apple_messages_defs, "apple_messages_drive_ingest", "AppleMessagesDriveIngestRunner",
         _apple_messages_summary, "apple_messages", {"apple_messages": object()}),
        (agent_defs, "agent_sessions_drive_ingest", "AgentSessionsDriveIngestRunner",
         lambda n: SimpleNamespace(batches_seen=1, events_written=n, files_promoted=1), "agent_sessions",
         {"agent_sessions": object()}),
    ],
)
def test_drive_ingest_lands_its_source_on_the_timeline_only_when_it_wrote_rows(
    monkeypatch, module, asset, runner_name, summary_for, source, extra
) -> None:
    monkeypatch.setattr(module, "load_settings", lambda **_k: _Settings(**extra))
    monkeypatch.setattr(module, "warehouse_from_settings", lambda _s: _FakeWarehouse())
    monkeypatch.setattr(module, "iter_batch_payloads", lambda **_k: iter(()))
    for name in ("_whatsapp_object_store", "_apple_messages_object_store", "_agent_sessions_object_store"):
        if hasattr(module, name):
            monkeypatch.setattr(module, name, lambda _s: object())
    if hasattr(module, "exclusive_sync_lock"):
        monkeypatch.setattr(module, "exclusive_sync_lock", _acquired)
    calls = _capture(monkeypatch, module)

    monkeypatch.setattr(module, runner_name, _runner_returning(summary_for(0)))
    result = getattr(module, asset)(build_asset_context())
    assert calls == [], "nothing written, nothing to land"
    assert result.metadata["timeline_fast_lane"].value == {"enabled": False, "rows": 0}

    monkeypatch.setattr(module, runner_name, _runner_returning(summary_for(3)))
    result = getattr(module, asset)(build_asset_context())
    assert calls == [{"postgres_url": URL, "sources": [source]}]
    assert result.metadata["timeline_fast_lane"].value["rows"] == 1


def test_gmail_sync_lands_gmail_on_the_timeline_when_a_mailbox_wrote(monkeypatch) -> None:
    monkeypatch.setattr(gmail_defs, "load_settings", lambda **_k: _Settings())
    monkeypatch.setattr(gmail_defs, "build_attachment_object_store_factory", lambda **_k: None)
    monkeypatch.setattr(gmail_defs, "warehouse_from_settings", lambda _s: _FakeWarehouse())
    calls = _capture(monkeypatch, gmail_defs)

    def mailbox(n: int):
        return SimpleNamespace(
            account="z@x.test", sync_type="incremental", next_history_id="1", messages_written=n,
            deleted_messages=0, attachments_written=0, attachments_stored=0, attachment_text_chars=0,
            attachment_backfill_candidates=0, attachment_backfill_rows_written=0, query="",
        )

    monkeypatch.setattr(gmail_defs, "GmailSyncRunner", _runner_returning([mailbox(0)]))
    gmail_defs.gmail_mailbox_sync(build_asset_context())
    assert calls == []

    monkeypatch.setattr(gmail_defs, "GmailSyncRunner", _runner_returning([mailbox(2)]))
    gmail_defs.gmail_mailbox_sync(build_asset_context())
    assert calls == [{"postgres_url": URL, "sources": ["gmail"]}]


def test_only_the_slack_freshness_stage_lands_slack_on_the_timeline(monkeypatch) -> None:
    monkeypatch.setattr(slack_defs, "load_settings", lambda **_k: _Settings())
    monkeypatch.setattr(slack_defs, "warehouse_from_settings", lambda _s: _FakeWarehouse())
    monkeypatch.setattr(slack_defs, "exclusive_sync_lock", _acquired)
    monkeypatch.setattr(slack_defs, "build_metadata", lambda: {"git_sha": "test"})
    calls = _capture(monkeypatch, slack_defs)

    def workspace(n: int):
        return SimpleNamespace(
            account="z", team_id="T1", sync_type="freshness", conversations_seen=1,
            messages_written=n, users_written=0, files_written=0,
        )

    monkeypatch.setattr(slack_defs, "run_slack_freshness_sync", lambda **_k: [workspace(0)])
    slack_defs.slack_workspace_sync(build_asset_context())
    assert calls == []

    monkeypatch.setattr(slack_defs, "run_slack_freshness_sync", lambda **_k: [workspace(4)])
    slack_defs.slack_workspace_sync(build_asset_context())
    assert calls == [{"postgres_url": URL, "sources": ["slack"]}]

    # Coverage, sweeps and the rest are history and metadata: the scheduled
    # pass lands those, the fast lane is for what a person is waiting on.
    monkeypatch.setattr(slack_defs, "run_slack_coverage_sync", lambda **_k: [workspace(4)])
    slack_defs.slack_workspace_coverage_sync(build_asset_context())
    assert len(calls) == 1
