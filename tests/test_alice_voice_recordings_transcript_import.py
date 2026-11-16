"""An Alice recording with no audio but a stored transcript is a transcript.

A meeting could sit in marts_voice_memos.recordings with 0 audio bytes, no
transcript and no enrichment while the transcript Alice had mailed for it was
archived beside it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
from io import BytesIO
import zipfile

import pytest

from personal_data_warehouse.alice_voice_recordings_transcript_import import (
    DOCX_CONTENT_TYPE,
    TRANSCRIPT_DOCUMENT_PROVIDER,
    AliceTranscriptDocumentImporter,
    document_paragraphs,
    load_transcript_document_candidates,
)
from personal_data_warehouse.apple_voice_memos_enrichment import (
    load_enrichment_candidates,
    load_transcript_segments,
)
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.schema import (
    ALICE_VOICE_RECORDING_ARTIFACT_COLUMNS,
    ALICE_VOICE_RECORDING_COLUMNS,
)
from tests.conftest import cleanup_test_warehouse, make_test_schema
from tests.test_marts_layering import _postgres_url, _row

RECORDED_AT = datetime(2025, 3, 14, 9, 30, tzinfo=UTC)
NOW = datetime(2026, 11, 11, 12, tzinfo=UTC)


@pytest.fixture()
def warehouse():
    wh = PostgresWarehouse(_postgres_url(), schema=make_test_schema("alice_transcript"))
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


class Logger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


def _docx(*paragraphs: str) -> bytes:
    body = "".join(f"<w:p><w:r><w:t>{p}</w:t></w:r></w:p>" if p else "<w:p/>" for p in paragraphs)
    document = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body>{body}</w:body></w:document>"
    )
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("word/document.xml", document)
    return buffer.getvalue()


DOCUMENT = _docx(
    "Example Planning Meeting",
    "",
    "00:00:01 Speaker A",
    "Let us start with the budget.",
    "",
    "00:00:09 Speaker B",
    "The budget is on track.",
)
DOCUMENT_SHA = hashlib.sha256(DOCUMENT).hexdigest()


class FakeStore:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.reads: list[str] = []

    def get_object(self, ref) -> bytes:
        self.reads.append(str(ref["storage_file_id"]))
        return self.objects[str(ref["storage_file_id"])]


def _recording(recording_id: str, *, size_bytes: int, sha: str = "") -> dict:
    return _row(
        ALICE_VOICE_RECORDING_COLUMNS,
        account="alice",
        recording_id=recording_id,
        title=f"Recording {recording_id}",
        filename=f"{recording_id}.mp3",
        content_type="audio/mpeg",
        size_bytes=size_bytes,
        content_sha256=sha,
        recorded_at=RECORDED_AT,
        storage_file_id=f"audio-{recording_id}" if size_bytes else "",
        ingested_at=RECORDED_AT,
        sync_version=1,
    )


def _artifact(recording_id: str, artifact_id: str, *, kind: str, size_bytes: int, file_id: str, sha: str) -> dict:
    return _row(
        ALICE_VOICE_RECORDING_ARTIFACT_COLUMNS,
        account="alice",
        recording_id=recording_id,
        artifact_id=artifact_id,
        kind=kind,
        filename=f"{recording_id}.transcript-formatted.docx" if kind == "voice_recording_transcript" else "x",
        content_type=DOCX_CONTENT_TYPE if kind == "voice_recording_transcript" else "audio/mpeg",
        size_bytes=size_bytes,
        content_sha256=sha,
        storage_backend="google_drive",
        storage_file_id=file_id,
        ingested_at=RECORDED_AT,
        sync_version=1,
    )


def _seed(wh: PostgresWarehouse) -> None:
    wh.ensure_alice_voice_recordings_tables()
    wh.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
    wh.insert_alice_voice_recordings(
        [
            # Audio gone, transcript kept: imported.
            _recording("board-2", size_bytes=0),
            # Audio present: AssemblyAI's job, never imported from a document.
            _recording("with-audio", size_bytes=10, sha="sha-audio"),
            # An empty upstream stub: nothing to import.
            _recording("stub", size_bytes=0),
        ]
    )
    wh.insert_alice_voice_recording_artifacts(
        [
            _artifact("board-2", "mp3", kind="voice_recording_audio", size_bytes=0, file_id="", sha=""),
            _artifact("board-2", "doc", kind="voice_recording_transcript", size_bytes=len(DOCUMENT),
                      file_id="doc-board-2", sha=DOCUMENT_SHA),
            _artifact("with-audio", "doc", kind="voice_recording_transcript", size_bytes=len(DOCUMENT),
                      file_id="doc-with-audio", sha=DOCUMENT_SHA),
            _artifact("stub", "page", kind="voice_recording_page_html", size_bytes=0, file_id="", sha=""),
        ]
    )


def _importer(wh: PostgresWarehouse, store: FakeStore) -> AliceTranscriptDocumentImporter:
    return AliceTranscriptDocumentImporter(warehouse=wh, object_store=store, logger=Logger(), now=lambda: NOW)


def test_a_stored_transcript_becomes_the_recordings_transcript_and_is_enriched_next(
    warehouse: PostgresWarehouse,
) -> None:
    _seed(warehouse)
    store = FakeStore({"doc-board-2": DOCUMENT, "doc-with-audio": DOCUMENT})

    summary = _importer(warehouse, store).sync()

    assert (summary.recordings_seen, summary.recordings_imported, summary.recordings_failed) == (1, 1, 0)
    assert store.reads == ["doc-board-2"]
    (row,) = warehouse._query_dicts(
        "SELECT transcript, transcript_provider FROM @marts_voice_memos_recordings"
        " WHERE source = 'alice_voice_recordings' AND recording_id = 'board-2'"
    )
    assert row["transcript_provider"] == TRANSCRIPT_DOCUMENT_PROVIDER
    assert "00:00:01 Speaker A" in row["transcript"]
    assert "The budget is on track." in row["transcript"]

    # Enrichment and calendar matching take it from here, reading its segments.
    candidates = {
        c["recording_id"]: c
        for c in load_enrichment_candidates(warehouse, provider="agent_test", prompt_version="v1", limit=None)
    }
    assert set(candidates) == {"board-2"}
    segments = load_transcript_segments(warehouse, candidates["board-2"])
    assert [s["text"] for s in segments] == [
        "Example Planning Meeting",
        "00:00:01 Speaker A Let us start with the budget.",
        "00:00:09 Speaker B The budget is on track.",
    ]

    # Done once: a second run reads nothing.
    again = _importer(warehouse, FakeStore({}))
    assert again.sync().recordings_seen == 0
    # And AssemblyAI is never asked for audio that does not exist.
    assert {r["recording_id"] for r in warehouse.load_untranscribed_voice_recordings(provider="assemblyai", limit=10)} == {
        "with-audio"
    }


def test_a_document_that_does_not_match_its_archive_hash_is_an_error_and_retried(
    warehouse: PostgresWarehouse,
) -> None:
    _seed(warehouse)

    summary = _importer(warehouse, FakeStore({"doc-board-2": b"not the document"})).sync()

    assert (summary.recordings_imported, summary.recordings_failed) == (0, 1)
    (run,) = warehouse._query_dicts(
        "SELECT status, error FROM @apple_voice_memos_transcription_runs WHERE recording_id = 'board-2'"
    )
    assert run["status"] == "error"
    assert "does not match" in run["error"]
    assert [c["recording_id"] for c in load_transcript_document_candidates(warehouse)] == ["board-2"]


def test_document_paragraphs_keep_each_block_verbatim() -> None:
    assert document_paragraphs("a\nb\n\n\nc\n") == ["a b", "c"]
