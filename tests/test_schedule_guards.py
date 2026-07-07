from __future__ import annotations

import importlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from dagster import DagsterRunStatus, RunRequest, SkipReason

from personal_data_warehouse.schedule_guards import (
    ACTIVE_RUN_STATUSES,
    IN_PROGRESS_RUN_STATUSES,
    skip_if_job_active,
    skip_if_job_in_progress,
)


SENSOR_FILES_THAT_OPEN_A_WAREHOUSE = [
    "gmail_attachment_enrichment.py",
    "apple_messages_attachment_enrichment.py",
    "apple_messages_audio_transcription.py",
    "apple_voice_memos_enrichment.py",
    "apple_voice_memos_transcription.py",
    "whatsapp_audio_transcription.py",
    "whatsapp_media_enrichment.py",
]

BACKLOG_SENSOR_CASES = [
    (
        "personal_data_warehouse.defs.gmail_attachment_enrichment",
        "gmail_attachment_enrichment_backlog_sensor",
        "has_file_enrichment_candidate",
    ),
    (
        "personal_data_warehouse.defs.apple_messages_attachment_enrichment",
        "apple_messages_attachment_enrichment_backlog_sensor",
        "has_file_enrichment_candidate",
    ),
    (
        "personal_data_warehouse.defs.apple_messages_audio_transcription",
        "apple_messages_audio_transcription_backlog_sensor",
        "has_audio_enrichment_candidate",
    ),
    (
        "personal_data_warehouse.defs.apple_voice_memos_enrichment",
        "apple_voice_memos_enrichment_backlog_sensor",
        "load_enrichment_candidates",
    ),
    (
        "personal_data_warehouse.defs.apple_voice_memos_transcription",
        "apple_voice_memos_transcription_backlog_sensor",
        None,
    ),
    (
        "personal_data_warehouse.defs.whatsapp_audio_transcription",
        "whatsapp_audio_transcription_backlog_sensor",
        "has_audio_enrichment_candidate",
    ),
    (
        "personal_data_warehouse.defs.whatsapp_media_enrichment",
        "whatsapp_media_enrichment_backlog_sensor",
        "has_file_enrichment_candidate",
    ),
]


class FakeInstance:
    def __init__(self, runs):
        self.runs = runs
        self.filters = None
        self.limit = None

    def get_runs(self, *, filters, limit):
        self.filters = filters
        self.limit = limit
        return self.runs


class FakeContext:
    def __init__(self, runs):
        self.instance = FakeInstance(runs)


class FakeWarehouse:
    def __init__(self, has_candidate: bool):
        self.has_candidate = has_candidate
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def load_untranscribed_apple_voice_memos_files(self, *, provider, limit):
        return [object()] if self.has_candidate else []


def fake_settings():
    return SimpleNamespace(
        agent=SimpleNamespace(provider="test_provider", model="test_model"),
        apple_messages=object(),
        assemblyai=object(),
        voice_memos=object(),
        whatsapp=object(),
    )


def test_skip_if_job_active_skips_when_prior_run_is_active() -> None:
    context = FakeContext([object()])

    result = skip_if_job_active(context, job_name="gmail_mailbox_sync_job")

    assert isinstance(result, SkipReason)
    assert "gmail_mailbox_sync_job" in result.skip_message
    assert context.instance.filters.job_name == "gmail_mailbox_sync_job"
    assert context.instance.filters.statuses == ACTIVE_RUN_STATUSES
    assert context.instance.filters.updated_after > datetime.now(tz=UTC) - timedelta(minutes=11)
    assert context.instance.limit == 1


def test_skip_if_job_active_allows_run_when_no_prior_run_is_active() -> None:
    context = FakeContext([])

    result = skip_if_job_active(context, job_name="calendar_event_sync_job")

    assert result == {}
    assert context.instance.filters.statuses == (
        DagsterRunStatus.QUEUED,
        DagsterRunStatus.STARTING,
        DagsterRunStatus.STARTED,
    )
    assert context.instance.filters.updated_after > datetime.now(tz=UTC) - timedelta(minutes=11)


def test_skip_if_job_in_progress_skips_any_active_run_without_stale_cutoff() -> None:
    context = FakeContext([object()])

    result = skip_if_job_in_progress(context, job_name="apple_voice_memos_transcription_job")

    assert isinstance(result, SkipReason)
    assert "apple_voice_memos_transcription_job" in result.skip_message
    assert context.instance.filters.job_name == "apple_voice_memos_transcription_job"
    assert context.instance.filters.statuses == IN_PROGRESS_RUN_STATUSES
    assert context.instance.filters.updated_after is None
    assert context.instance.limit == 1


def test_sensor_close_regression_for_backlog_warehouse_users() -> None:
    defs_dir = Path(__file__).resolve().parents[1] / "src" / "personal_data_warehouse" / "defs"
    missing = []
    for name in SENSOR_FILES_THAT_OPEN_A_WAREHOUSE:
        text = (defs_dir / name).read_text()
        if "warehouse_from_settings(" in text and "warehouse.close()" not in text:
            missing.append(name)

    assert not missing, f"sensor modules open a warehouse but never close it: {missing}"


@pytest.mark.parametrize(("has_candidate", "expected_type"), [(True, RunRequest), (False, SkipReason)])
@pytest.mark.parametrize(("module_name", "sensor_name", "candidate_function_name"), BACKLOG_SENSOR_CASES)
def test_backlog_sensor_invocations_close_their_warehouse(
    monkeypatch,
    module_name: str,
    sensor_name: str,
    candidate_function_name: str | None,
    has_candidate: bool,
    expected_type,
) -> None:
    module = importlib.import_module(module_name)
    warehouse = FakeWarehouse(has_candidate=has_candidate)
    monkeypatch.setattr(module, "skip_if_job_in_progress", lambda context, job_name: {})
    monkeypatch.setattr(module, "load_settings", lambda **kwargs: fake_settings())
    monkeypatch.setattr(module, "warehouse_from_settings", lambda settings: warehouse)
    if candidate_function_name is not None:
        if candidate_function_name == "load_enrichment_candidates":
            monkeypatch.setattr(
                module,
                candidate_function_name,
                lambda *args, **kwargs: [object()] if has_candidate else [],
            )
        else:
            monkeypatch.setattr(module, candidate_function_name, lambda *args, **kwargs: has_candidate)

    result = getattr(module, sensor_name)(None)

    assert isinstance(result, expected_type)
    assert warehouse.closed
