"""A voice memo lands -- in the warehouse, transcribed, on the timeline -- seconds after upload.

Measured 2026-10-16 over the last 30 days of Apple Voice Memos: end of
recording to transcript p50 9 min, and almost none of it was work. The upload
waited for a 120 s file-age gate and a five-minute tick; the Drive inbox for a
60 s sensor and a Dagster run start; transcription for another 60 s sensor and
run start; the timeline for the five-minute ``timeline_sync``. AssemblyAI
itself took 7 s on a short memo. The app now NOTIFYs a resident worker when a
sidecar is stored, the worker runs the same ingest and transcription code the
Dagster assets run, and every voice asset lands its rows on the timeline
through the fast lane. The sensors and schedules stay as the floor.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from dagster import build_asset_context

from personal_data_warehouse import voice_memo_landing_worker as worker
from personal_data_warehouse.defs import apple_voice_memos_drive_ingest as ingest_defs
from personal_data_warehouse.defs import apple_voice_memos_enrichment as enrichment_defs
from personal_data_warehouse.defs import apple_voice_memos_transcription as transcription_defs

URL = "postgresql://example/warehouse"


class _Logger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


def test_a_landing_pass_ingests_transcribes_then_lands_on_the_timeline() -> None:
    calls: list[str] = []

    def ingest() -> int:
        calls.append("ingest")
        return 1

    def transcribe() -> int:
        calls.append("transcribe")
        return 1

    did_work = worker.land_pending_voice_memos(
        ingest=ingest,
        transcribe=transcribe,
        land_on_timeline=lambda: calls.append("timeline"),
        logger=_Logger(),
    )

    assert calls == ["ingest", "transcribe", "timeline"]
    assert did_work is True, "a transcription batch may have left more behind it"


def test_a_pass_with_nothing_new_lands_nothing() -> None:
    landed: list[str] = []
    did_work = worker.land_pending_voice_memos(
        ingest=lambda: 0,
        transcribe=lambda: 0,
        land_on_timeline=lambda: landed.append("timeline"),
        logger=_Logger(),
    )
    assert landed == []
    assert did_work is False


def test_an_ingest_whose_transcription_is_elsewhere_still_lands_the_recording() -> None:
    # The transcription lock held by a Dagster run: the recording still goes on
    # the timeline now, and the run that holds the lock transcribes it.
    landed: list[str] = []
    did_work = worker.land_pending_voice_memos(
        ingest=lambda: 2,
        transcribe=lambda: 0,
        land_on_timeline=lambda: landed.append("timeline"),
        logger=_Logger(),
    )
    assert landed == ["timeline"]
    assert did_work is False, "nothing left for this worker to drain"


def test_the_processor_skips_ingest_and_transcription_when_nothing_is_waiting(monkeypatch) -> None:
    monkeypatch.setattr(
        worker,
        "load_settings",
        lambda **_k: SimpleNamespace(postgres_database_url=URL, voice_memos=object(), assemblyai=object()),
    )
    monkeypatch.setattr(worker, "voice_memos_inbox_has_metadata", lambda _settings: False)
    monkeypatch.setattr(worker, "google_recorder_inbox_has_metadata", lambda _settings: False)
    monkeypatch.setattr(worker, "voice_recordings_await_transcription", lambda _settings: False)
    monkeypatch.setattr(worker, "ingest_voice_memos_inbox", lambda **_k: (_ for _ in ()).throw(AssertionError("ingest ran")))
    monkeypatch.setattr(
        worker, "ingest_google_recorder_inbox", lambda **_k: (_ for _ in ()).throw(AssertionError("recorder ingest ran"))
    )
    monkeypatch.setattr(
        worker, "transcribe_voice_recordings", lambda **_k: (_ for _ in ()).throw(AssertionError("transcription ran"))
    )
    monkeypatch.setattr(
        worker, "land_sources_on_timeline", lambda **_k: (_ for _ in ()).throw(AssertionError("timeline ran"))
    )

    assert worker.VoiceMemoLandingProcessor().process_pending() is False


def test_the_processor_runs_the_dagster_assets_own_code(monkeypatch) -> None:
    settings = SimpleNamespace(postgres_database_url=URL, voice_memos=object(), assemblyai=object())
    monkeypatch.setattr(worker, "load_settings", lambda **_k: settings)
    monkeypatch.setattr(worker, "voice_memos_inbox_has_metadata", lambda _settings: True)
    monkeypatch.setattr(worker, "google_recorder_inbox_has_metadata", lambda _settings: False)
    monkeypatch.setattr(worker, "voice_recordings_await_transcription", lambda _settings: True)
    seen: dict[str, object] = {}

    def fake_ingest(*, settings, logger):
        seen["ingest"] = settings
        return SimpleNamespace(rows_written=1)

    def fake_transcribe(*, settings, logger, limit):
        seen["transcribe"] = settings
        return SimpleNamespace(recordings_transcribed=1)

    def fake_land(*, postgres_url, sources, logger):
        seen["timeline"] = (postgres_url, list(sources))
        return {"rows": 1}

    monkeypatch.setattr(worker, "ingest_voice_memos_inbox", fake_ingest)
    monkeypatch.setattr(worker, "transcribe_voice_recordings", fake_transcribe)
    monkeypatch.setattr(worker, "land_sources_on_timeline", fake_land)

    assert worker.VoiceMemoLandingProcessor().process_pending() is True
    assert seen == {"ingest": settings, "transcribe": settings, "timeline": (URL, ["voice_memos"])}


def test_the_processor_lands_a_pixel_recording_the_same_way(monkeypatch) -> None:
    """The phone's Recorder sidecars wake the same worker (the app notifies the
    same channel), so the Pixel inbox is drained beside the Mac's."""
    settings = SimpleNamespace(postgres_database_url=URL, voice_memos=object(), assemblyai=object())
    monkeypatch.setattr(worker, "load_settings", lambda **_k: settings)
    monkeypatch.setattr(worker, "voice_memos_inbox_has_metadata", lambda _settings: False)
    monkeypatch.setattr(worker, "google_recorder_inbox_has_metadata", lambda _settings: True)
    monkeypatch.setattr(worker, "voice_recordings_await_transcription", lambda _settings: True)
    seen: list[str] = []
    monkeypatch.setattr(
        worker, "ingest_voice_memos_inbox", lambda **_k: (_ for _ in ()).throw(AssertionError("empty Mac inbox read"))
    )
    monkeypatch.setattr(
        worker,
        "ingest_google_recorder_inbox",
        lambda *, settings, logger: seen.append("recorder") or SimpleNamespace(rows_written=1),
    )
    monkeypatch.setattr(
        worker,
        "transcribe_voice_recordings",
        lambda *, settings, logger, limit: seen.append("transcribe") or SimpleNamespace(recordings_transcribed=1),
    )
    monkeypatch.setattr(worker, "land_sources_on_timeline", lambda **_k: seen.append("timeline") or {"rows": 1})

    assert worker.VoiceMemoLandingProcessor().process_pending() is True
    assert seen == ["recorder", "transcribe", "timeline"]


def test_the_app_wakes_the_worker_for_recorder_sidecars_too() -> None:
    app_ingest = (Path(__file__).resolve().parents[1] / "app/internal/server/ingest.go").read_text()
    recorder = app_ingest.split('"/ingest/google-recorder/metadata"', 1)[1].split("endpoint:", 1)[0]
    assert "notifyChannel: voiceMemoLandingChannel" in recorder


def test_the_worker_listens_on_the_channel_the_app_notifies() -> None:
    app_ingest = (Path(__file__).resolve().parents[1] / "app/internal/server/ingest.go").read_text()
    assert f'voiceMemoLandingChannel = "{worker.VOICE_MEMO_LANDING_CHANNEL}"' in app_ingest


def test_the_dagster_container_supervises_the_voice_landing_worker() -> None:
    start_script = (Path(__file__).resolve().parents[1] / "docker/start-dagster.sh").read_text()
    assert "python -m personal_data_warehouse.voice_memo_landing_worker" in start_script
    assert "VOICE_MEMO_LANDING_WORKER_ENABLED" in start_script
    assert 'voice_worker_pid=""' in start_script


# --- every voice asset lands what it wrote on the timeline ------------------------


@contextmanager
def _acquired(**_kwargs):
    yield True


class _Warehouse:
    def close(self) -> None:
        pass


def _capture(monkeypatch, module) -> list[dict]:
    calls: list[dict] = []

    def fake_land(*, postgres_url, sources, logger):
        calls.append({"postgres_url": postgres_url, "sources": list(sources)})
        return {"enabled": True, "rows": 1, "adapters": [], "seconds": 0.1, "skipped": [], "errors": {}}

    monkeypatch.setattr(module, "land_sources_on_timeline", fake_land)
    return calls


def test_drive_ingest_lands_voice_memos_on_the_timeline_only_when_it_wrote(monkeypatch) -> None:
    settings = SimpleNamespace(postgres_database_url=URL)
    monkeypatch.setattr(ingest_defs, "load_settings", lambda **_k: settings)
    calls = _capture(monkeypatch, ingest_defs)

    monkeypatch.setattr(ingest_defs, "ingest_voice_memos_inbox", lambda **_k: SimpleNamespace(metadata_seen=0, rows_written=0, recordings_promoted=0))
    ingest_defs.apple_voice_memos_drive_ingest(build_asset_context())
    assert calls == []

    monkeypatch.setattr(ingest_defs, "ingest_voice_memos_inbox", lambda **_k: SimpleNamespace(metadata_seen=1, rows_written=1, recordings_promoted=1))
    result = ingest_defs.apple_voice_memos_drive_ingest(build_asset_context())
    assert calls == [{"postgres_url": URL, "sources": ["voice_memos"]}]
    assert result.metadata["timeline_fast_lane"].value["rows"] == 1


def test_transcription_lands_voice_memos_on_the_timeline_only_when_it_transcribed(monkeypatch) -> None:
    settings = SimpleNamespace(postgres_database_url=URL)
    monkeypatch.setattr(transcription_defs, "load_settings", lambda **_k: settings)
    calls = _capture(monkeypatch, transcription_defs)

    def summary(n: int):
        return SimpleNamespace(recordings_seen=1, recordings_transcribed=n, recordings_failed=0, segments_written=n)

    monkeypatch.setattr(transcription_defs, "transcribe_voice_recordings", lambda **_k: summary(0))
    transcription_defs.apple_voice_memos_transcription(build_asset_context())
    assert calls == []

    monkeypatch.setattr(transcription_defs, "transcribe_voice_recordings", lambda **_k: summary(2))
    transcription_defs.apple_voice_memos_transcription(build_asset_context())
    assert calls == [{"postgres_url": URL, "sources": ["voice_memos"]}]


def test_enrichment_lands_voice_memos_on_the_timeline_only_when_it_enriched(monkeypatch) -> None:
    settings = SimpleNamespace(postgres_database_url=URL, agent=SimpleNamespace(provider="codex"))
    monkeypatch.setattr(enrichment_defs, "load_settings", lambda **_k: settings)
    monkeypatch.setattr(enrichment_defs, "warehouse_from_settings", lambda _s: _Warehouse())
    monkeypatch.setattr(enrichment_defs, "exclusive_sync_lock", _acquired)
    monkeypatch.setattr(enrichment_defs, "apple_voice_memos_enrichment_client", lambda **_k: object())
    calls = _capture(monkeypatch, enrichment_defs)

    def runner(n: int):
        class _Runner:
            def __init__(self, **_kwargs) -> None:
                pass

            def sync(self, *, limit):
                return SimpleNamespace(recordings_seen=1, recordings_enriched=n, recordings_failed=0)

        return _Runner

    monkeypatch.setattr(enrichment_defs, "VoiceMemosEnrichmentRunner", runner(0))
    enrichment_defs.apple_voice_memos_enrichment(build_asset_context(), agent=object())
    assert calls == []

    monkeypatch.setattr(enrichment_defs, "VoiceMemosEnrichmentRunner", runner(1))
    enrichment_defs.apple_voice_memos_enrichment(build_asset_context(), agent=object())
    assert calls == [{"postgres_url": URL, "sources": ["voice_memos"]}]
