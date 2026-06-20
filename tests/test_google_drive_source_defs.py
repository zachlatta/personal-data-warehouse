from __future__ import annotations

from personal_data_warehouse.defs import google_drive_source_sync as defs_mod


def test_definitions_expose_asset_job_and_schedule() -> None:
    defs = defs_mod.defs()
    asset_keys = {key.to_user_string() for key in defs.resolve_asset_graph().get_all_asset_keys()}
    assert "google_drive_source_sync" in asset_keys
    assert defs.resolve_job_def("google_drive_source_sync_job") is not None


def test_schedule_skips_when_prior_run_is_in_progress(monkeypatch) -> None:
    calls: list[tuple[object, str]] = []
    expected = object()

    def fake_skip_if_job_in_progress(context, *, job_name: str):
        calls.append((context, job_name))
        return expected

    monkeypatch.setattr(defs_mod, "skip_if_job_in_progress", fake_skip_if_job_in_progress)
    context = object()

    result = defs_mod.google_drive_source_sync_every_thirty_minutes._execution_fn.decorated_fn(context)

    assert result is expected
    assert calls == [(context, "google_drive_source_sync_job")]


def test_unique_advisory_lock_id() -> None:
    # The lock id sits in the drive-ingest band and must not collide with peers.
    from personal_data_warehouse.defs import apple_messages_drive_ingest, whatsapp_drive_ingest

    ids = {
        defs_mod.GOOGLE_DRIVE_SOURCE_SYNC_POSTGRES_LOCK_ID,
        apple_messages_drive_ingest.APPLE_MESSAGES_DRIVE_INGEST_POSTGRES_LOCK_ID,
        whatsapp_drive_ingest.WHATSAPP_DRIVE_INGEST_POSTGRES_LOCK_ID,
    }
    assert len(ids) == 3
