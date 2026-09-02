from __future__ import annotations

from types import SimpleNamespace

import pytest

from personal_data_warehouse.definitions import defs
from personal_data_warehouse.defs import apple_notes_attachment_enrichment as defs_module
from personal_data_warehouse.file_attachment_enrichment import APPLE_NOTES_SOURCE
from personal_data_warehouse.attachment_text_extraction import APPLE_NOTES_TEXT_SOURCE


def test_apple_notes_attachment_passes_are_registered_and_read_the_mart() -> None:
    repository = defs().get_repository_def()
    assets = {key.to_user_string() for key in repository.assets_defs_by_key}
    schedules = {schedule.name for schedule in repository.schedule_defs}

    assert {"apple_notes_attachment_enrichment", "apple_notes_attachment_text"} <= assets
    assert {
        "apple_notes_attachment_enrichment_hourly",
        "apple_notes_attachment_text_hourly",
    } <= schedules
    assert APPLE_NOTES_SOURCE.table == "marts_files_attachments"
    assert APPLE_NOTES_TEXT_SOURCE.table == "marts_files_attachments"
    assert "a.source = 'apple_notes'" in APPLE_NOTES_SOURCE.stored_predicate
    assert "a.source = 'apple_notes'" in APPLE_NOTES_TEXT_SOURCE.stored_predicate


def test_apple_notes_attachment_store_factory_reuses_the_source_store(monkeypatch) -> None:
    store = object()
    monkeypatch.setattr(defs_module, "apple_notes_object_store", lambda _settings: store)

    factory = defs_module.apple_notes_attachment_object_store_factory(
        settings=SimpleNamespace()
    )

    assert factory("first") is store
    assert factory("second") is store


@pytest.mark.parametrize(
    ("name", "loader"),
    [
        (
            defs_module.APPLE_NOTES_ATTACHMENT_ENRICHMENT_BATCH_SIZE_ENV,
            defs_module.apple_notes_attachment_enrichment_batch_size,
        ),
        (
            defs_module.APPLE_NOTES_ATTACHMENT_TEXT_BATCH_SIZE_ENV,
            defs_module.apple_notes_attachment_text_batch_size,
        ),
    ],
)
def test_apple_notes_attachment_batch_sizes_reject_negative_values(
    monkeypatch, name, loader
) -> None:
    monkeypatch.setenv(name, "-1")
    with pytest.raises(ValueError, match="non-negative"):
        loader()
