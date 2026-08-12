from __future__ import annotations

import pytest
from dagster import AssetKey

from personal_data_warehouse.attachment_text_extraction import DEFAULT_TEXT_EXTRACTION_BATCH_SIZE
from personal_data_warehouse.defs.apple_messages_attachment_text import (
    apple_messages_attachment_text,
    apple_messages_attachment_text_batch_size,
    apple_messages_attachment_text_hourly,
    apple_messages_attachment_text_job,
)


def test_job_selects_the_asset() -> None:
    assert apple_messages_attachment_text_job.name == "apple_messages_attachment_text_job"


def test_asset_depends_on_drive_ingest() -> None:
    # Attachments must be promoted into the warehouse before there is anything
    # to extract.
    assert apple_messages_attachment_text.asset_deps[AssetKey("apple_messages_attachment_text")] == {
        AssetKey("apple_messages_drive_ingest"),
    }


def test_schedule_runs_hourly_off_peak_from_the_vision_pass() -> None:
    # The vision pass runs at :53; staggering avoids two attachment jobs
    # contending for the same Drive reads and Postgres locks.
    assert apple_messages_attachment_text_hourly.cron_schedule == "23 * * * *"
    assert apple_messages_attachment_text_hourly.default_status.value == "RUNNING"


def test_batch_size_defaults_and_is_overridable(monkeypatch) -> None:
    monkeypatch.delenv("APPLE_MESSAGES_ATTACHMENT_TEXT_BATCH_SIZE", raising=False)
    assert apple_messages_attachment_text_batch_size() == DEFAULT_TEXT_EXTRACTION_BATCH_SIZE

    monkeypatch.setenv("APPLE_MESSAGES_ATTACHMENT_TEXT_BATCH_SIZE", "50")
    assert apple_messages_attachment_text_batch_size() == 50


def test_batch_size_rejects_negative_values(monkeypatch) -> None:
    # A negative batch would silently become "unbounded" in the runner's
    # `limit if limit > 0 else None`, turning a typo into a full-corpus scan.
    monkeypatch.setenv("APPLE_MESSAGES_ATTACHMENT_TEXT_BATCH_SIZE", "-1")
    with pytest.raises(ValueError, match="must be non-negative"):
        apple_messages_attachment_text_batch_size()
