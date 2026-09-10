"""Frequent jobs carry their own run-time cap.

On 2026-09-09 at 14:00Z, right after a deploy, seven jobs (gmail, contacts,
plaid, drive, pipeline_health, slack coverage, whatsapp) started together and
hung in their step subprocess without logging a line for 3.5 hours, until the
next deploy cancelled them. With ``max_concurrent_runs: 8`` that left one slot
for everything else: the five-minute Slack freshness and timeline syncs sat
QUEUED for up to 46 minutes each, and DM landing p95 read 42 minutes on a day
nothing else was wrong. The global run-monitoring cap (4 hours) exists for the
WhatsApp client's three-hour windows and the multi-hour user sync; a job that
finishes in seconds must not be allowed to hold a slot for four hours.
"""

from __future__ import annotations

import pytest

from personal_data_warehouse.defs import contacts_sync, gmail_sync, google_drive_source_sync
from personal_data_warehouse.defs import pipeline_health as pipeline_health_defs
from personal_data_warehouse.defs import plaid_sync, slack_sync, timeline_sync

MAX_RUNTIME_TAG = "dagster/max_runtime"


@pytest.mark.parametrize(
    ("job", "cap_seconds"),
    [
        (pipeline_health_defs.pipeline_health_job, 900),
        (timeline_sync.timeline_sync_job, 1800),
        (slack_sync.slack_workspace_sync_job, 1800),
        (slack_sync.slack_workspace_coverage_sync_job, 3600),
        (gmail_sync.gmail_mailbox_sync_job, 3600),
        (contacts_sync.contacts_sync_job, 3600),
        (plaid_sync.plaid_finance_sync_job, 3600),
        (google_drive_source_sync.google_drive_source_sync_job, 7200),
    ],
)
def test_frequent_jobs_carry_a_run_time_cap_below_the_global_one(job, cap_seconds) -> None:
    assert MAX_RUNTIME_TAG in job.tags, f"{job.name} has no {MAX_RUNTIME_TAG} tag"
    assert int(job.tags[MAX_RUNTIME_TAG]) == cap_seconds, job.name
    assert int(job.tags[MAX_RUNTIME_TAG]) < 14400
