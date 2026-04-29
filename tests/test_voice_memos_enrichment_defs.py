from __future__ import annotations

from personal_data_warehouse.defs.voice_memos_enrichment import (
    DEFAULT_VOICE_MEMOS_ENRICHMENT_BATCH_SIZE,
    voice_memos_enrichment_job,
)
from personal_data_warehouse.voice_memos_enrichment import DEFAULT_ENRICHMENT_LOOKBACK_WEEKS


def test_voice_memos_enrichment_job_selects_asset() -> None:
    assert voice_memos_enrichment_job.name == "voice_memos_enrichment_job"


def test_voice_memos_enrichment_defaults_to_all_recent_transcripts() -> None:
    assert DEFAULT_VOICE_MEMOS_ENRICHMENT_BATCH_SIZE == 0
    assert DEFAULT_ENRICHMENT_LOOKBACK_WEEKS == 8
