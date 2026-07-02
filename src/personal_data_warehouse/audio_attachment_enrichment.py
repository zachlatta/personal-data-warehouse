"""Audio transcription + agent cleanup enrichment for stored voice-message attachments.

A source-agnostic pipeline scans a source's attachment table for downloaded
audio blobs with no useful searchable text yet, transcribes each through
AssemblyAI, and - when the raw transcript is non-empty - runs the transcript
through the sandboxed agent container (codex/claude CLI) for cleanup/summary,
mirroring how ``file_attachment_enrichment.py`` cleans up vision output. The
result is upserted into the SAME shared ``file_attachment_enrichments`` table
under its own ``ai_provider``/``ai_model``/``ai_prompt_version`` identity, so
AssemblyAI is an invisible intermediate step (like ``pdftoppm`` is for PDFs)
and the existing BM25 search branches that join that table pick up transcripts
automatically.

Each source (Apple Messages voice messages, WhatsApp voice notes, etc.) is
described by an :class:`AudioEnrichmentSource` that names its candidate table
and the columns the generic candidate query reads.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import tempfile
from typing import Any, Callable

from personal_data_warehouse.agent_runner import (
    AgentRunRequest,
    AgentRunResult,
    agent_run_event_rows,
    agent_run_row,
    agent_run_tool_call_rows,
)
from personal_data_warehouse.apple_voice_memos_transcription import clean_transcript_text
from personal_data_warehouse.file_attachment_enrichment import (
    COMPLETED_STATUSES,
    ENRICHMENT_TABLE,
    STATUS_ERROR,
    STATUS_NOT_USEFUL,
    STATUS_OK,
    _clean_str,
    _error_window_clause,
    _indexable_lines,
    attachment_storage_ref,
)

DEFAULT_AUDIO_ENRICHMENT_MAX_ERROR_ATTEMPTS = 3
DEFAULT_AUDIO_ENRICHMENT_ERROR_WINDOW_DAYS = 14
MIN_AUDIO_BYTES = 256

# MIME prefixes (LIKE patterns) rather than exact equality: WhatsApp reports
# voice notes as 'audio/ogg; codecs=opus' (a parameter suffix), which exact
# matching would silently exclude entirely.
AUDIO_MIME_LIKE_PATTERNS = (
    "audio/x-m4a%",
    "audio/mp4%",
    "audio/mpeg%",
    "audio/amr%",
    "audio/ogg%",
    "audio/opus%",
    "audio/aac%",
    "audio/x-caf%",
)
AUDIO_EXTENSIONS = (".m4a", ".caf", ".amr", ".mp3", ".ogg", ".opus", ".aac", ".wav")


def _safe_audio_filename(filename: str) -> str:
    """A filesystem-safe on-disk name for a temp-downloaded audio blob.

    ``filename`` is sender-supplied attachment metadata (iMessage transfer name /
    WhatsApp document filename) that is never sanitized on ingest. Joining it
    directly into a path is unsafe: ``Path(tempdir) / filename`` silently
    discards ``tempdir`` when ``filename`` is absolute (arbitrary-file-write),
    and raises ``FileNotFoundError`` when it contains a ``/`` (no such
    subdirectory). Keep only a short suffix for readability - AssemblyAI's
    content-type comes from the explicit ``content_type`` header, not this name.
    """
    suffix = Path(filename).suffix
    if not suffix or len(suffix) > 10:
        suffix = ""
    return f"audio{suffix}"


@dataclass(frozen=True)
class AudioEnrichmentSource:
    """Describes one audio-attachment source the shared transcription runner can scan.

    ``table`` is the source's attachment table; the column names map the generic
    candidate query onto it. ``stored_predicate`` is a SQL fragment (using table
    alias ``a``) that selects only rows whose blob is in the object store. Every
    source's storage columns are assumed to be the standard
    ``storage_backend``/``storage_key``/``storage_file_id``/``storage_url``.
    """

    name: str
    label: str
    task_type: str
    prompt_version: str
    table: str
    stored_predicate: str
    sha_column: str = "content_sha256"
    filename_column: str = "filename"
    mime_column: str = "mime_type"
    size_column: str = "size"
    order_column: str = "internal_date"


APPLE_MESSAGES_AUDIO_SOURCE = AudioEnrichmentSource(
    name="apple_messages",
    label="iMessage voice message",
    task_type="apple_messages_audio_transcription",
    prompt_version="apple-messages-audio-agent-v1",
    table="apple_message_attachments",
    stored_predicate="a.is_missing = 0 AND a.content_sha256 <> ''",
    size_column="size_bytes",
    order_column="created_at",
)

WHATSAPP_AUDIO_SOURCE = AudioEnrichmentSource(
    name="whatsapp",
    label="WhatsApp voice message",
    task_type="whatsapp_audio_transcription",
    prompt_version="whatsapp-audio-agent-v1",
    table="whatsapp_media_items",
    stored_predicate="a.is_missing = 0 AND a.content_sha256 <> ''",
    size_column="size_bytes",
    order_column="message_at",
)


@dataclass(frozen=True)
class AudioAttachmentEnrichmentSummary:
    attachments_seen: int
    attachments_enriched: int
    attachments_not_useful: int
    attachments_failed: int


class AudioAttachmentTranscriptionRunner:
    def __init__(
        self,
        *,
        source: AudioEnrichmentSource,
        warehouse,
        agent,
        object_store_factory: Callable[[str], Any],
        transcription_client,
        logger,
        provider: str,
        model: str = "",
        text_max_chars: int = 20_000,
        max_error_attempts: int = DEFAULT_AUDIO_ENRICHMENT_MAX_ERROR_ATTEMPTS,
        error_window_days: int = DEFAULT_AUDIO_ENRICHMENT_ERROR_WINDOW_DAYS,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._source = source
        self._warehouse = warehouse
        self._agent = agent
        self._object_store_factory = object_store_factory
        self._transcription_client = transcription_client
        self._logger = logger
        self._provider = provider
        self._model = model
        self._prompt_version = source.prompt_version
        self._text_max_chars = text_max_chars
        self._max_error_attempts = max_error_attempts
        self._error_window_days = error_window_days
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._object_stores: dict[str, Any] = {}

    @property
    def enrichment_provider(self) -> str:
        return f"agent_{self._provider}"

    def sync(self, *, limit: int | None) -> AudioAttachmentEnrichmentSummary:
        self._warehouse.ensure_file_attachment_enrichment_tables()
        self._warehouse.ensure_agent_tables()
        candidates = load_audio_enrichment_candidates(
            self._warehouse,
            source=self._source,
            provider=self.enrichment_provider,
            model=self._model,
            prompt_version=self._prompt_version,
            limit=limit,
            max_error_attempts=self._max_error_attempts,
            error_window_days=self._error_window_days,
        )
        enriched = 0
        not_useful = 0
        failed = 0
        for index, candidate in enumerate(candidates, start=1):
            content_sha256 = str(candidate.get("content_sha256", ""))
            label = f"{candidate.get('filename') or '<unnamed>'} ({content_sha256[:12]})"
            try:
                self._logger.info(
                    "[%s/%s] transcribing %s %s", index, len(candidates), self._source.label, label
                )
                status = self._enrich_candidate(candidate)
            except Exception as exc:
                failed += 1
                self._record_failure(candidate, error=str(exc), status=STATUS_ERROR)
                self._logger.warning("[%s/%s] failed %s: %s", index, len(candidates), label, exc)
                continue
            if status == STATUS_OK:
                enriched += 1
            else:
                not_useful += 1
        self._logger.info(
            "%s transcription: saw %s attachments, enriched %s, not useful %s, failed %s",
            self._source.label,
            len(candidates),
            enriched,
            not_useful,
            failed,
        )
        return AudioAttachmentEnrichmentSummary(
            attachments_seen=len(candidates),
            attachments_enriched=enriched,
            attachments_not_useful=not_useful,
            attachments_failed=failed,
        )

    def _enrich_candidate(self, candidate: Mapping[str, Any]) -> str:
        content = self._object_store(str(candidate.get("account", ""))).get_object(
            attachment_storage_ref(candidate)
        )
        mime_type = str(candidate.get("mime_type", ""))
        with tempfile.TemporaryDirectory(prefix="audio-attachment-") as directory:
            path = Path(directory) / _safe_audio_filename(str(candidate.get("filename", "")))
            path.write_bytes(content)
            transcription_result = self._transcription_client.transcribe_file(path=path, content_type=mime_type)
        raw_transcript = clean_transcript_text(str(transcription_result.get("text", "") or ""))

        if not raw_transcript:
            self._warehouse.insert_attachment_enrichments(
                [self._enrichment_row(candidate, text="", status=STATUS_NOT_USEFUL, error="", prompt="", result=None)]
            )
            return STATUS_NOT_USEFUL

        prompt = audio_transcript_cleanup_prompt(
            transcript=raw_transcript, candidate=candidate, source_label=self._source.label
        )
        request = AgentRunRequest(
            prompt=prompt,
            schema=audio_transcript_cleanup_schema(),
            task_type=self._source.task_type,
            subject_id=str(candidate.get("content_sha256", "")),
            prompt_version=self._prompt_version,
            provider=self._provider,
            model=self._model,
        )
        result = self._agent.run(request)
        self._record_agent_result(result)
        if result.status != "completed":
            raise RuntimeError(result.error or f"agent run {result.run_id} failed")
        output = dict(result.final_output_json)
        issues = validate_audio_transcript_cleanup_result(output)
        if issues:
            raise RuntimeError("agent output failed validation: " + "; ".join(issues[:5]))

        text = audio_transcript_enrichment_text(output)
        status = STATUS_OK if text else STATUS_NOT_USEFUL
        if len(text) > self._text_max_chars:
            text = text[: self._text_max_chars]
        self._warehouse.insert_attachment_enrichments(
            [
                self._enrichment_row(
                    candidate,
                    text=text,
                    status=status,
                    error="",
                    prompt=prompt,
                    result=result,
                )
            ]
        )
        return status

    def _record_failure(
        self, candidate: Mapping[str, Any], *, error: str, status: str = STATUS_ERROR
    ) -> None:
        try:
            self._warehouse.insert_attachment_enrichments(
                [
                    self._enrichment_row(
                        candidate,
                        text="",
                        status=status,
                        error=error[:2000],
                        prompt="",
                        result=None,
                    )
                ]
            )
        except Exception as exc:
            self._logger.warning(
                "Could not record %s enrichment failure for %s: %s",
                self._source.label,
                candidate.get("content_sha256", ""),
                exc,
            )

    def _enrichment_row(
        self,
        candidate: Mapping[str, Any],
        *,
        text: str,
        status: str,
        error: str,
        prompt: str,
        result: AgentRunResult | None,
    ) -> dict[str, Any]:
        updated_at = self._now()
        elapsed_ms = 0
        processed_at = updated_at
        if result is not None:
            elapsed_ms = int((result.completed_at - result.started_at).total_seconds() * 1000)
            processed_at = result.completed_at
        return {
            "content_sha256": str(candidate.get("content_sha256", "")),
            "ai_provider": self.enrichment_provider,
            "ai_model": self._model,
            "ai_prompt_version": self._prompt_version,
            "text": text,
            "text_extraction_status": status,
            "text_extraction_error": error,
            "ai_base_url": "",
            "ai_prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest() if prompt else "",
            "ai_prompt": prompt,
            "ai_source_status": "",
            "ai_elapsed_ms": elapsed_ms,
            "ai_processed_at": processed_at,
            "updated_at": updated_at,
            "sync_version": int(updated_at.timestamp() * 1000),
        }

    def _record_agent_result(self, result: AgentRunResult) -> None:
        self._warehouse.insert_agent_runs([agent_run_row(result)])
        event_rows = agent_run_event_rows(result)
        if event_rows:
            self._warehouse.insert_agent_run_events(event_rows)
        tool_call_rows = agent_run_tool_call_rows(result)
        if tool_call_rows:
            self._warehouse.insert_agent_run_tool_calls(tool_call_rows)

    def _object_store(self, account: str):
        store = self._object_stores.get(account)
        if store is None:
            store = self._object_store_factory(account)
            self._object_stores[account] = store
        return store


def _audio_candidate_query(source: AudioEnrichmentSource, *, projection: str, tail: str) -> str:
    """Build a candidate-selection query for ``source``.

    Structurally parallel to ``file_attachment_enrichment._candidate_query``
    (retry cap via a failed_runs CTE, stored predicate, not-yet-completed
    guard) but with an audio MIME/extension kind gate instead of image/PDF, and
    no PDF-style prior-extraction concept. Kept independent (not sharing that
    function's SQL assembly) so this module can't regress the live Gmail/
    WhatsApp vision candidate query.

    Unlike the vision pipeline, the retry cap here counts STATUS_ERROR rows in
    ``file_attachment_enrichments`` directly rather than ``agent_runs``: a
    transcription-stage failure (AssemblyAI upload/transcribe error) never
    reaches the agent, so it would never write an agent_runs row, and an
    agent_runs-keyed cap would let a permanently-broken audio attachment
    re-download and re-transcribe forever. ``_record_failure`` always writes to
    ``file_attachment_enrichments`` regardless of which stage failed, so that
    table is the one place every failure is reliably counted.
    """
    sha = source.sha_column
    filename = source.filename_column
    mime = source.mime_column
    size = source.size_column
    return f"""
        WITH failed_runs AS (
            SELECT content_sha256 AS subject_id, count(*) AS error_attempts
            FROM {ENRICHMENT_TABLE}
            WHERE ai_provider = %s
              AND ai_model = %s
              AND ai_prompt_version = %s
              AND text_extraction_status = %s
              {{error_window_sql}}
            GROUP BY content_sha256
        )
        {projection}
        FROM {source.table} a
        LEFT JOIN failed_runs runs
            ON runs.subject_id = a.{sha}
        WHERE {source.stored_predicate}
          AND a.{size} >= %s
          AND COALESCE(runs.error_attempts, 0) < %s
          AND (
              lower(a.{mime}) LIKE ANY(%s)
              OR lower(a.{filename}) LIKE ANY(%s)
          )
          AND NOT EXISTS (
              SELECT 1
              FROM {ENRICHMENT_TABLE} done
              WHERE done.content_sha256 = a.{sha}
                AND done.ai_provider = %s
                AND done.ai_model = %s
                AND done.ai_prompt_version = %s
                AND done.text_extraction_status = ANY(%s)
          )
        {tail}
        """


def _audio_candidate_params(
    *,
    provider: str,
    model: str,
    prompt_version: str,
    max_error_attempts: int,
    error_window_params: list[Any],
) -> list[Any]:
    return [
        provider,
        model,
        prompt_version,
        STATUS_ERROR,
        *error_window_params,
        MIN_AUDIO_BYTES,
        int(max_error_attempts),
        list(AUDIO_MIME_LIKE_PATTERNS),
        [f"%{extension}" for extension in AUDIO_EXTENSIONS],
        provider,
        model,
        prompt_version,
        list(COMPLETED_STATUSES),
    ]


def load_audio_enrichment_candidates(
    warehouse,
    *,
    source: AudioEnrichmentSource,
    provider: str,
    model: str,
    prompt_version: str,
    limit: int | None,
    max_error_attempts: int = DEFAULT_AUDIO_ENRICHMENT_MAX_ERROR_ATTEMPTS,
    error_window_days: int = DEFAULT_AUDIO_ENRICHMENT_ERROR_WINDOW_DAYS,
) -> list[dict[str, Any]]:
    """Audio attachments stored in the object store that have no completed
    transcription+cleanup enrichment for this provider/model/prompt identity."""
    error_window_sql, error_window_params = _error_window_clause(error_window_days, column="updated_at")
    columns = (
        "account",
        "content_sha256",
        "filename",
        "mime_type",
        "size",
        "storage_backend",
        "storage_key",
        "storage_file_id",
        "storage_url",
    )
    limit_sql = "LIMIT %s" if limit is not None and limit > 0 else ""
    projection = (
        f"SELECT DISTINCT ON (a.{source.sha_column}) "
        f"a.account, "
        f"a.{source.sha_column} AS content_sha256, "
        f"a.{source.filename_column} AS filename, "
        f"a.{source.mime_column} AS mime_type, "
        f"a.{source.size_column} AS size, "
        f"a.storage_backend, a.storage_key, a.storage_file_id, a.storage_url, "
        f"a.{source.order_column} AS order_at"
    )
    inner = _audio_candidate_query(
        source,
        projection=projection,
        tail=f"ORDER BY a.{source.sha_column}, a.{source.order_column} DESC",
    ).format(error_window_sql=error_window_sql)
    params = _audio_candidate_params(
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        max_error_attempts=max_error_attempts,
        error_window_params=error_window_params,
    )
    if limit_sql:
        params.append(int(limit))
    rows = warehouse._query(
        f"""
        SELECT account, content_sha256, filename, mime_type, size,
               storage_backend, storage_key, storage_file_id, storage_url
        FROM ( {inner} ) AS candidates
        ORDER BY order_at DESC
        {limit_sql}
        """,
        tuple(params),
    )
    return [dict(zip(columns, row, strict=True)) for row in rows]


def has_audio_enrichment_candidate(
    warehouse,
    *,
    source: AudioEnrichmentSource,
    provider: str,
    model: str,
    prompt_version: str,
    max_error_attempts: int = DEFAULT_AUDIO_ENRICHMENT_MAX_ERROR_ATTEMPTS,
    error_window_days: int = DEFAULT_AUDIO_ENRICHMENT_ERROR_WINDOW_DAYS,
) -> bool:
    """Return whether at least one stored audio attachment needs transcription."""
    error_window_sql, error_window_params = _error_window_clause(error_window_days, column="updated_at")
    query = _audio_candidate_query(
        source,
        projection="SELECT 1",
        tail="LIMIT 1",
    ).format(error_window_sql=error_window_sql)
    params = _audio_candidate_params(
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        max_error_attempts=max_error_attempts,
        error_window_params=error_window_params,
    )
    rows = warehouse._query(query, tuple(params))
    return bool(rows)


def audio_transcript_cleanup_schema() -> dict[str, Any]:
    # Codex enforces OpenAI strict structured outputs: every property must be
    # listed in `required`, so uncertainties is required too (an empty array is fine).
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "is_useful",
            "summary",
            "cleaned_transcript",
            "entities",
            "search_keywords",
            "uncertainties",
        ],
        "properties": {
            "is_useful": {"type": "boolean"},
            "summary": {"type": "string"},
            "cleaned_transcript": {"type": "string"},
            "entities": {"type": "array", "items": {"type": "string"}},
            "search_keywords": {"type": "array", "items": {"type": "string"}},
            "uncertainties": {"type": "array", "items": {"type": "string"}},
        },
    }


def audio_transcript_cleanup_instructions(source_label: str) -> str:
    return f"""Clean up this raw speech-to-text transcript of a real {source_label}.

Fix obvious transcription errors (misheard words, missing punctuation, garbled phrases) using context, but never invent content that was not spoken. Preserve the speaker's actual words and meaning; only lightly clean formatting, capitalization, and punctuation. Write a one- or two-sentence summary of what the message is about. Extract named entities (people, places, organizations, dates, numbers) actually mentioned. Note anything you are uncertain you transcribed or cleaned correctly.

Use false for is_useful only when the transcript is empty, pure noise, or contains no discernible speech content. Do not invent details."""


def audio_transcript_cleanup_prompt(
    *, transcript: str, candidate: Mapping[str, Any], source_label: str = "voice message"
) -> str:
    payload = {
        "task": f"Clean up and summarize the raw transcript of one {source_label}.",
        "transcript": {
            "raw_text": transcript,
            "original_filename": str(candidate.get("filename", "")),
            "original_mime_type": str(candidate.get("mime_type", "")),
        },
        "instructions": audio_transcript_cleanup_instructions(source_label),
        "final_output_contract": {
            "format": "Return one JSON object and no prose.",
            "schema": audio_transcript_cleanup_schema(),
        },
        "agent_runtime_notes": [
            "You are running as a one-off CLI agent inside an isolated Docker container.",
            "Base your answer only on the provided raw_text; never invent spoken content.",
            "The final answer must be valid JSON matching final_output_contract.schema.",
        ],
    }
    return json.dumps(payload, sort_keys=True)


def validate_audio_transcript_cleanup_result(result: Mapping[str, Any]) -> list[str]:
    issues: list[str] = []
    if not isinstance(result.get("is_useful"), bool):
        issues.append("is_useful must be a boolean")
    for key in ("summary", "cleaned_transcript"):
        if not isinstance(result.get(key), str):
            issues.append(f"{key} must be a string")
    for key in ("entities", "search_keywords", "uncertainties"):
        value = result.get(key)
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            issues.append(f"{key} must be an array of strings")
    if result.get("is_useful") is True and not str(result.get("cleaned_transcript", "")).strip():
        issues.append("cleaned_transcript must not be empty when is_useful is true")
    return issues


def audio_transcript_enrichment_text(result: Mapping[str, Any]) -> str:
    """Compose the searchable text block stored in file_attachment_enrichments.

    Mirrors ``file_attachment_enrichment.attachment_enrichment_text``'s layout
    so audio-transcript rows read the same as vision-OCR rows.
    """
    if result.get("is_useful") is not True:
        return ""
    summary = _clean_str(result.get("summary"))
    cleaned_transcript = _clean_str(result.get("cleaned_transcript"))
    entities = _indexable_lines(result.get("entities"), separator=", ")
    search_keywords = _indexable_lines(result.get("search_keywords"), separator=", ")
    uncertainties = _indexable_lines(result.get("uncertainties"), separator="\n")
    return "\n\n".join(
        part
        for part in (
            "AI audio transcript extraction",
            f"Summary: {summary}".strip(),
            f"Transcript:\n{cleaned_transcript}".strip(),
            f"Entities: {entities}".strip(),
            f"Search keywords: {search_keywords}".strip(),
            f"Uncertainties:\n{uncertainties}".strip(),
        )
        if part and not part.endswith(":")
    )
