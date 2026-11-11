"""Alice recordings whose audio is gone but whose transcript was kept.

Alice mails a formatted transcript (``.transcript-formatted.docx``) when a
recording finishes, and the Gmail recovery pass archives it as a
``voice_recording_transcript`` artifact. For a recording whose audio Alice no
longer serves, that document is the only copy of what was said: until
2026-11-11 such a meeting sat in ``marts_voice_memos.recordings`` with 0 audio
bytes, no transcript and no enrichment while its transcript was stored beside
it.

This pass moves that document's text into the voice domain as a transcription
run under its own provider (``alice_transcript_document``), with the document's
paragraphs as segments. It is data movement, not judgment: the text is copied
as Alice wrote it, speaker names and timestamps included, and the enrichment
agent reads it like any other transcript. A recording with audio is never
imported this way: AssemblyAI transcribes the audio, which carries diarization.

It reads Alice's own archive tables, like the Alice Drive ingest does, because
only Alice produces transcript documents; everything downstream (the mart,
enrichment, calendar matching, continuity, the timeline) reads the voice mart.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from typing import Any

from personal_data_warehouse.google_drive_source_sync import extract_docx_text
from personal_data_warehouse_alice_voice_recordings.gmail_recovery import TRANSCRIPT_KIND
from personal_data_warehouse_alice_voice_recordings.sync import SOURCE

TRANSCRIPT_DOCUMENT_PROVIDER = "alice_transcript_document"
DOCX_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


@dataclass(frozen=True)
class AliceTranscriptImportSummary:
    recordings_seen: int
    recordings_imported: int
    recordings_failed: int
    segments_written: int


def load_transcript_document_candidates(warehouse, *, limit: int | None = None) -> list[dict[str, Any]]:
    """Alice recordings with no audio, a stored transcript, and no import of it yet.

    The newest nonempty transcript artifact per recording; an import is current
    while it read that document (``provider_transcript_id`` is the document's
    sha), so a corrected transcript is imported again.
    """
    limit_sql = f"LIMIT {int(limit)}" if limit is not None and limit > 0 else ""
    return warehouse._query_dicts(
        f"""
        SELECT
            r.account,
            r.recording_id,
            r.content_sha256,
            a.artifact_id,
            a.filename,
            a.content_type,
            a.size_bytes,
            a.content_sha256 AS document_sha256,
            a.storage_backend,
            a.storage_key,
            a.storage_file_id,
            a.storage_url
        FROM @alice_voice_recordings r
        JOIN LATERAL (
            SELECT *
            FROM @alice_voice_recording_artifacts a
            WHERE a.account = r.account
              AND a.recording_id = r.recording_id
              AND a.kind = '{TRANSCRIPT_KIND}'
              AND a.size_bytes > 0
              AND a.storage_file_id <> ''
            ORDER BY a.ingested_at DESC, a.artifact_id DESC
            LIMIT 1
        ) a ON true
        LEFT JOIN @apple_voice_memos_transcription_runs t
          ON t.source = '{SOURCE}'
         AND t.account = r.account
         AND t.recording_id = r.recording_id
         AND t.provider = '{TRANSCRIPT_DOCUMENT_PROVIDER}'
         AND t.status IN ('completed', 'rejected')
         AND t.provider_transcript_id = a.content_sha256
        WHERE r.size_bytes = 0
          AND t.recording_id IS NULL
        ORDER BY r.recorded_at DESC
        {limit_sql}
        """
    )


def document_text(*, data: bytes, filename: str, content_type: str) -> str:
    if content_type == DOCX_CONTENT_TYPE or filename.lower().endswith(".docx"):
        return extract_docx_text(data)
    if content_type.startswith("text/") or filename.lower().endswith((".txt", ".md")):
        return data.decode("utf-8", errors="replace")
    raise ValueError(f"unsupported transcript document type {content_type or filename!r}")


def document_paragraphs(text: str) -> list[str]:
    """Blank-line separated blocks, each kept verbatim on one line."""
    blocks: list[str] = []
    current: list[str] = []
    for line in text.splitlines():
        if line.strip():
            current.append(line.strip())
        elif current:
            blocks.append(" ".join(current))
            current = []
    if current:
        blocks.append(" ".join(current))
    return blocks


class AliceTranscriptDocumentImporter:
    def __init__(
        self,
        *,
        warehouse,
        object_store,
        logger,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._object_store = object_store
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync(self, *, limit: int | None = None) -> AliceTranscriptImportSummary:
        self._warehouse.ensure_alice_voice_recordings_tables()
        self._warehouse.ensure_apple_voice_memos_tables(backfill_content_hashes=False)
        candidates = load_transcript_document_candidates(self._warehouse, limit=limit)
        imported = failed = segments = 0
        for candidate in candidates:
            recording_id = str(candidate["recording_id"])
            requested_at = self._now()
            try:
                data = self._object_store.get_object(candidate)
                actual_sha = hashlib.sha256(data).hexdigest()
                expected_sha = str(candidate.get("document_sha256") or "")
                if expected_sha and actual_sha != expected_sha:
                    raise ValueError(f"transcript document sha256 {actual_sha} does not match the archive's {expected_sha}")
                text = document_text(
                    data=data,
                    filename=str(candidate.get("filename") or ""),
                    content_type=str(candidate.get("content_type") or ""),
                ).strip()
                if not text:
                    raise ValueError("transcript document has no text")
                completed_at = self._now()
                rows = segment_rows(candidate, text, transcript_id=actual_sha, created_at=completed_at)
                self._warehouse.insert_apple_voice_memos_transcription_runs(
                    [run_row(candidate, text=text, transcript_id=actual_sha, requested_at=requested_at, completed_at=completed_at)]
                )
                self._warehouse.replace_voice_recording_transcript_segments(
                    source=SOURCE,
                    account=str(candidate["account"]),
                    recording_id=recording_id,
                    provider=TRANSCRIPT_DOCUMENT_PROVIDER,
                    provider_transcript_id=actual_sha,
                    rows=rows,
                )
                imported += 1
                segments += len(rows)
                self._logger.info("imported Alice transcript document for %s: %s segments", recording_id, len(rows))
            except Exception as exc:  # recorded as the run's state, retried next run
                failed += 1
                completed_at = self._now()
                self._warehouse.insert_apple_voice_memos_transcription_runs(
                    [
                        run_row(
                            candidate,
                            text="",
                            transcript_id=str(candidate.get("document_sha256") or ""),
                            requested_at=requested_at,
                            completed_at=completed_at,
                            status="error",
                            error=str(exc),
                        )
                    ]
                )
                self._logger.warning("failed to import Alice transcript document for %s: %s", recording_id, exc)
        return AliceTranscriptImportSummary(
            recordings_seen=len(candidates),
            recordings_imported=imported,
            recordings_failed=failed,
            segments_written=segments,
        )


def run_row(
    candidate: Mapping[str, Any],
    *,
    text: str,
    transcript_id: str,
    requested_at: datetime,
    completed_at: datetime,
    status: str = "completed",
    error: str = "",
) -> dict[str, Any]:
    return {
        "source": SOURCE,
        "account": str(candidate["account"]),
        "recording_id": str(candidate["recording_id"]),
        # The recording's audio identity (empty: there is no audio), so the
        # enrichment pass matches this run to the mart row it belongs to.
        "content_sha256": str(candidate.get("content_sha256") or ""),
        "provider": TRANSCRIPT_DOCUMENT_PROVIDER,
        "provider_transcript_id": transcript_id,
        "model": "",
        "status": status,
        "error": error,
        "transcript_text": text,
        "raw_result_json": json.dumps(
            {
                "artifact_id": str(candidate.get("artifact_id") or ""),
                "filename": str(candidate.get("filename") or ""),
                "content_type": str(candidate.get("content_type") or ""),
                "size_bytes": int(candidate.get("size_bytes") or 0),
                "document_sha256": transcript_id,
                "storage_file_id": str(candidate.get("storage_file_id") or ""),
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        "requested_at": requested_at,
        "completed_at": completed_at,
        "sync_version": int(completed_at.timestamp() * 1_000_000),
    }


def segment_rows(
    candidate: Mapping[str, Any], text: str, *, transcript_id: str, created_at: datetime
) -> list[dict[str, Any]]:
    return [
        {
            "source": SOURCE,
            "account": str(candidate["account"]),
            "recording_id": str(candidate["recording_id"]),
            "provider": TRANSCRIPT_DOCUMENT_PROVIDER,
            "provider_transcript_id": transcript_id,
            "segment_index": index,
            # The document names its speakers in its own words, inside the text;
            # deciding who spoke is the enrichment agent's job.
            "speaker_label": "",
            "start_ms": 0,
            "end_ms": 0,
            "confidence": 0.0,
            "text": paragraph,
            "words_json": "[]",
            "created_at": created_at,
            "sync_version": int(created_at.timestamp() * 1_000_000),
        }
        for index, paragraph in enumerate(document_paragraphs(text))
    ]
