from __future__ import annotations

from datetime import UTC, datetime, timedelta

from dagster import DagsterRunStatus, SkipReason

from personal_data_warehouse.schedule_guards import (
    ACTIVE_RUN_STATUSES,
    IN_PROGRESS_RUN_STATUSES,
    skip_if_job_active,
    skip_if_job_in_progress,
)


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

    result = skip_if_job_in_progress(context, job_name="voice_memos_transcription_job")

    assert isinstance(result, SkipReason)
    assert "voice_memos_transcription_job" in result.skip_message
    assert context.instance.filters.job_name == "voice_memos_transcription_job"
    assert context.instance.filters.statuses == IN_PROGRESS_RUN_STATUSES
    assert context.instance.filters.updated_after is None
    assert context.instance.limit == 1
