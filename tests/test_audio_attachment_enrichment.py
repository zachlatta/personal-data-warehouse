from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json

import pytest

from personal_data_warehouse.agent_runner import AgentRunRequest, AgentRunResult
from personal_data_warehouse.audio_attachment_enrichment import (
    APPLE_MESSAGES_AUDIO_SOURCE,
    WHATSAPP_AUDIO_SOURCE,
    AudioAttachmentTranscriptionRunner,
    audio_transcript_cleanup_prompt,
    audio_transcript_cleanup_schema,
    audio_transcript_enrichment_text,
    has_audio_enrichment_candidate,
    load_audio_enrichment_candidates,
    validate_audio_transcript_cleanup_result,
)
from personal_data_warehouse.file_attachment_enrichment import (
    COMPLETED_STATUSES,
    ENRICHMENT_TABLE,
    STATUS_ERROR,
    STATUS_NOT_USEFUL,
    STATUS_OK,
)


class FakeLogger:
    def __init__(self) -> None:
        self.infos: list[str] = []
        self.warnings: list[str] = []

    def info(self, message, *args) -> None:
        self.infos.append(message % args if args else message)

    def warning(self, message, *args) -> None:
        self.warnings.append(message % args if args else message)


class FakeWarehouse:
    def __init__(self, candidate_rows: list[tuple] | None = None) -> None:
        self.candidate_rows = list(candidate_rows or [])
        self.queries: list[tuple[str, tuple]] = []
        self.enrichment_rows: list[dict] = []
        self.agent_runs: list[dict] = []
        self.agent_run_events: list[dict] = []
        self.agent_run_tool_calls: list[dict] = []

    def ensure_file_attachment_enrichment_tables(self) -> None:
        pass

    def ensure_agent_tables(self) -> None:
        pass

    def _query(self, sql: str, params=None):
        self.queries.append((sql, params))
        return self.candidate_rows

    def insert_attachment_enrichments(self, rows) -> None:
        self.enrichment_rows.extend(rows)

    def insert_agent_runs(self, rows) -> None:
        self.agent_runs.extend(rows)

    def insert_agent_run_events(self, rows) -> None:
        self.agent_run_events.extend(rows)

    def insert_agent_run_tool_calls(self, rows) -> None:
        self.agent_run_tool_calls.extend(rows)


class FakeObjectStore:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.get_calls: list[dict] = []

    def get_object(self, ref) -> bytes:
        self.get_calls.append(dict(ref))
        return self.content


class FakeTranscriptionClient:
    def __init__(self, *, text: str = "", error: Exception | None = None) -> None:
        self.text = text
        self.error = error
        self.calls: list[dict] = []

    def transcribe_file(self, *, path, content_type: str):
        self.calls.append({"path": path, "content_type": content_type, "bytes": path.read_bytes()})
        if self.error is not None:
            raise self.error
        return {"id": "transcript-1", "status": "completed", "text": self.text, "speech_model_used": "universal-3-pro"}


class FakeAgent:
    def __init__(self, output: dict | None = None, *, status: str = "completed", error: str = "") -> None:
        self.output = output or {}
        self.status = status
        self.error = error
        self.requests: list[AgentRunRequest] = []

    def run(self, request: AgentRunRequest) -> AgentRunResult:
        self.requests.append(request)
        now = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)
        return AgentRunResult(
            run_id=request.run_id,
            provider=request.provider or "codex",
            model=request.model or "",
            task_type=request.task_type,
            subject_id=request.subject_id,
            prompt_version=request.prompt_version,
            input_sha256=request.input_sha256,
            status=self.status,
            final_output_json=self.output,
            error=self.error,
            exit_code=0 if self.status == "completed" else 1,
            started_at=now,
            completed_at=now,
            events=[],
        )


def candidate_row(content: bytes, *, filename: str = "voice.m4a", mime_type: str = "audio/x-m4a") -> tuple:
    return (
        "user@example.test",
        hashlib.sha256(content).hexdigest(),
        filename,
        mime_type,
        len(content),
        "google_drive",
        "apple-messages/library/voice.m4a",
        "drive-file-id",
        "https://drive.example/voice.m4a",
    )


def useful_output() -> dict:
    return {
        "is_useful": True,
        "summary": "A quick check-in about dinner plans.",
        "cleaned_transcript": "Hey, are we still on for dinner tonight?",
        "entities": [],
        "search_keywords": ["dinner", "plans"],
        "uncertainties": [],
    }


def make_runner(
    warehouse, agent, store, transcription_client, *, source=APPLE_MESSAGES_AUDIO_SOURCE, logger=None
) -> AudioAttachmentTranscriptionRunner:
    return AudioAttachmentTranscriptionRunner(
        source=source,
        warehouse=warehouse,
        agent=agent,
        object_store_factory=lambda account: store,
        transcription_client=transcription_client,
        logger=logger or FakeLogger(),
        provider="codex",
        model="",
    )


def test_sync_logs_summary_line_with_counts() -> None:
    content = b"fake m4a bytes"
    warehouse = FakeWarehouse([candidate_row(content)])
    logger = FakeLogger()
    runner = make_runner(
        warehouse,
        FakeAgent(useful_output()),
        FakeObjectStore(content),
        FakeTranscriptionClient(text="hey are we still on for dinner tonight"),
        logger=logger,
    )

    runner.sync(limit=10)

    assert (
        "iMessage voice message transcription: saw 1 attachments, enriched 1, "
        "not useful 0, failed 0" in logger.infos
    )


def test_sync_logs_summary_line_when_idle() -> None:
    logger = FakeLogger()
    runner = make_runner(
        FakeWarehouse([]),
        FakeAgent(useful_output()),
        FakeObjectStore(b""),
        FakeTranscriptionClient(text=""),
        logger=logger,
    )

    runner.sync(limit=10)

    assert (
        "iMessage voice message transcription: saw 0 attachments, enriched 0, "
        "not useful 0, failed 0" in logger.infos
    )


def test_runner_transcribes_and_cleans_up_voice_message() -> None:
    content = b"fake m4a bytes"
    warehouse = FakeWarehouse([candidate_row(content)])
    agent = FakeAgent(useful_output())
    store = FakeObjectStore(content)
    transcription = FakeTranscriptionClient(text="hey are we still on for dinner tonight")

    summary = make_runner(warehouse, agent, store, transcription).sync(limit=10)

    assert summary.attachments_seen == 1
    assert summary.attachments_enriched == 1
    assert summary.attachments_failed == 0

    assert store.get_calls == [
        {
            "storage_backend": "google_drive",
            "storage_key": "apple-messages/library/voice.m4a",
            "storage_file_id": "drive-file-id",
            "storage_url": "https://drive.example/voice.m4a",
        }
    ]
    assert transcription.calls[0]["content_type"] == "audio/x-m4a"
    assert transcription.calls[0]["bytes"] == content

    request = agent.requests[0]
    assert request.task_type == APPLE_MESSAGES_AUDIO_SOURCE.task_type == "apple_messages_audio_transcription"
    assert request.prompt_version == APPLE_MESSAGES_AUDIO_SOURCE.prompt_version == "apple-messages-audio-agent-v1"
    assert request.input_files == {}
    payload = json.loads(request.prompt)
    assert "dinner" in payload["transcript"]["raw_text"] or "hey are we still" in payload["transcript"]["raw_text"]

    row = warehouse.enrichment_rows[0]
    assert row["content_sha256"] == hashlib.sha256(content).hexdigest()
    # Keyed the same way vision rows are keyed: AssemblyAI is an invisible
    # intermediate step, not part of the stored identity.
    assert row["ai_provider"] == "agent_codex"
    assert row["ai_prompt_version"] == "apple-messages-audio-agent-v1"
    assert row["text_extraction_status"] == STATUS_OK
    assert "dinner tonight" in row["text"]


def test_enrich_candidate_never_joins_untrusted_filename_into_path() -> None:
    # filename comes straight from sender-supplied attachment metadata (iMessage
    # transfer name / WhatsApp document filename) and is never sanitized on
    # ingest. Path(tempdir) / filename is unsafe: an absolute filename silently
    # discards tempdir (arbitrary-file-write), so the raw filename must never be
    # joined into the on-disk path.
    content = b"fake audio bytes"
    warehouse = FakeWarehouse([candidate_row(content, filename="/etc/passwd", mime_type="audio/x-m4a")])
    agent = FakeAgent(useful_output())
    transcription = FakeTranscriptionClient(text="hello there")

    summary = make_runner(warehouse, agent, FakeObjectStore(content), transcription).sync(limit=None)

    assert summary.attachments_failed == 0
    call = transcription.calls[0]
    assert str(call["path"]) != "/etc/passwd"
    assert call["path"].name != "passwd"
    assert call["bytes"] == content


def test_enrich_candidate_handles_filename_with_path_separator() -> None:
    # A filename containing '/' (e.g. "voice/note.m4a") used to raise
    # FileNotFoundError because Path(tempdir) / filename tried to write into a
    # non-existent subdirectory of the temp dir.
    content = b"fake audio bytes"
    warehouse = FakeWarehouse([candidate_row(content, filename="voice/note.m4a", mime_type="audio/x-m4a")])
    agent = FakeAgent(useful_output())
    transcription = FakeTranscriptionClient(text="hello there")

    summary = make_runner(warehouse, agent, FakeObjectStore(content), transcription).sync(limit=None)

    assert summary.attachments_failed == 0
    assert summary.attachments_enriched == 1


def test_runner_uses_whatsapp_audio_source_identity() -> None:
    content = b"fake ogg bytes"
    warehouse = FakeWarehouse([candidate_row(content, filename="PTT.ogg", mime_type="audio/ogg; codecs=opus")])
    agent = FakeAgent(useful_output())
    transcription = FakeTranscriptionClient(text="hello there")

    summary = make_runner(
        warehouse, agent, FakeObjectStore(content), transcription, source=WHATSAPP_AUDIO_SOURCE
    ).sync(limit=10)

    assert summary.attachments_enriched == 1
    assert agent.requests[0].task_type == "whatsapp_audio_transcription"
    assert agent.requests[0].prompt_version == "whatsapp-audio-agent-v1"


def test_runner_short_circuits_empty_transcript_without_agent_call() -> None:
    content = b"silence"
    warehouse = FakeWarehouse([candidate_row(content)])
    agent = FakeAgent(useful_output())
    transcription = FakeTranscriptionClient(text="   ")

    summary = make_runner(warehouse, agent, FakeObjectStore(content), transcription).sync(limit=None)

    assert summary.attachments_not_useful == 1
    assert agent.requests == []  # nothing to summarize, so the agent never runs
    row = warehouse.enrichment_rows[0]
    assert row["text_extraction_status"] == STATUS_NOT_USEFUL
    assert row["text"] == ""


def test_runner_marks_non_useful_transcript_from_agent() -> None:
    content = b"fake bytes"
    warehouse = FakeWarehouse([candidate_row(content)])
    agent = FakeAgent(
        {
            "is_useful": False,
            "summary": "",
            "cleaned_transcript": "",
            "entities": [],
            "search_keywords": [],
            "uncertainties": [],
        }
    )
    transcription = FakeTranscriptionClient(text="unintelligible noise")

    summary = make_runner(warehouse, agent, FakeObjectStore(content), transcription).sync(limit=None)

    assert summary.attachments_not_useful == 1
    row = warehouse.enrichment_rows[0]
    assert row["text_extraction_status"] == STATUS_NOT_USEFUL
    assert row["text"] == ""


def test_runner_records_error_row_when_transcription_fails() -> None:
    content = b"fake bytes"
    warehouse = FakeWarehouse([candidate_row(content)])
    agent = FakeAgent(useful_output())
    transcription = FakeTranscriptionClient(error=RuntimeError("assemblyai upload failed"))

    summary = make_runner(warehouse, agent, FakeObjectStore(content), transcription).sync(limit=None)

    assert summary.attachments_failed == 1
    assert agent.requests == []  # never reached the cleanup stage
    row = warehouse.enrichment_rows[0]
    assert row["text_extraction_status"] == STATUS_ERROR
    assert "assemblyai upload failed" in row["text_extraction_error"]


def test_runner_records_error_row_when_agent_cleanup_fails() -> None:
    content = b"fake bytes"
    warehouse = FakeWarehouse([candidate_row(content)])
    agent = FakeAgent(status="error", error="container exploded")
    transcription = FakeTranscriptionClient(text="hello there")

    summary = make_runner(warehouse, agent, FakeObjectStore(content), transcription).sync(limit=None)

    assert summary.attachments_failed == 1
    row = warehouse.enrichment_rows[0]
    assert row["text_extraction_status"] == STATUS_ERROR
    assert "container exploded" in row["text_extraction_error"]


def test_runner_rejects_output_missing_required_fields() -> None:
    content = b"fake bytes"
    warehouse = FakeWarehouse([candidate_row(content)])
    agent = FakeAgent({"is_useful": True, "summary": 42})
    transcription = FakeTranscriptionClient(text="hello there")

    summary = make_runner(warehouse, agent, FakeObjectStore(content), transcription).sync(limit=None)

    assert summary.attachments_failed == 1
    assert warehouse.enrichment_rows[0]["text_extraction_status"] == STATUS_ERROR


@pytest.mark.parametrize("source", [APPLE_MESSAGES_AUDIO_SOURCE, WHATSAPP_AUDIO_SOURCE])
@pytest.mark.parametrize("error_window_days", [14, 0])
def test_candidate_query_placeholder_count_matches_params(source, error_window_days) -> None:
    warehouse = FakeWarehouse([])
    load_audio_enrichment_candidates(
        warehouse,
        source=source,
        provider="agent_codex",
        prompt_version=source.prompt_version,
        limit=5,
        error_window_days=error_window_days,
    )
    sql, params = warehouse.queries[0]
    assert sql.count("%s") == len(params)


def test_candidate_query_matches_whatsapp_mime_with_codec_suffix() -> None:
    # WhatsApp voice notes report mime_type='audio/ogg; codecs=opus' (a parameter
    # suffix); exact-equality matching would silently exclude every one of them.
    warehouse = FakeWarehouse([])
    load_audio_enrichment_candidates(
        warehouse,
        source=WHATSAPP_AUDIO_SOURCE,
        provider="agent_codex",
        prompt_version=WHATSAPP_AUDIO_SOURCE.prompt_version,
        limit=5,
    )
    sql, params = warehouse.queries[0]
    assert "LIKE ANY" in sql
    like_patterns = next(p for p in params if isinstance(p, list) and any("audio/ogg%" == v for v in p))
    assert "audio/ogg%" in like_patterns


def test_has_audio_enrichment_candidate_returns_bool() -> None:
    warehouse = FakeWarehouse([(1,)])
    assert has_audio_enrichment_candidate(
        warehouse,
        source=APPLE_MESSAGES_AUDIO_SOURCE,
        provider="agent_codex",
        prompt_version=APPLE_MESSAGES_AUDIO_SOURCE.prompt_version,
    ) is True

    warehouse_empty = FakeWarehouse([])
    assert has_audio_enrichment_candidate(
        warehouse_empty,
        source=APPLE_MESSAGES_AUDIO_SOURCE,
        provider="agent_codex",
        prompt_version=APPLE_MESSAGES_AUDIO_SOURCE.prompt_version,
    ) is False


def test_candidate_query_excludes_already_completed_rows() -> None:
    warehouse = FakeWarehouse([])
    load_audio_enrichment_candidates(
        warehouse,
        source=APPLE_MESSAGES_AUDIO_SOURCE,
        provider="agent_codex",
        prompt_version=APPLE_MESSAGES_AUDIO_SOURCE.prompt_version,
        limit=5,
    )
    sql, _params = warehouse.queries[0]
    assert "text_extraction_status = ANY(%s)" in sql


def test_candidate_completion_and_failure_identity_ignore_model() -> None:
    warehouse = FakeWarehouse([])

    load_audio_enrichment_candidates(
        warehouse,
        source=APPLE_MESSAGES_AUDIO_SOURCE,
        provider="agent_codex",
        prompt_version=APPLE_MESSAGES_AUDIO_SOURCE.prompt_version,
        limit=5,
    )

    sql, _params = warehouse.queries[0]
    assert "ai_model = %s" not in sql


def test_candidate_query_retry_cap_counts_transcription_stage_failures() -> None:
    # A transcription-stage failure (AssemblyAI upload/transcribe error) never
    # reaches self._agent.run(), so it can never be counted by an agent_runs-based
    # retry cap the way the vision pipeline's agent-stage failures are. The audio
    # retry cap must instead count STATUS_ERROR rows directly in
    # file_attachment_enrichments (which _record_failure always writes to,
    # regardless of which stage failed) - otherwise a permanently-broken audio
    # attachment is re-selected and re-transcribed forever.
    warehouse = FakeWarehouse([])
    load_audio_enrichment_candidates(
        warehouse,
        source=APPLE_MESSAGES_AUDIO_SOURCE,
        provider="agent_codex",
        prompt_version=APPLE_MESSAGES_AUDIO_SOURCE.prompt_version,
        limit=5,
        max_error_attempts=3,
    )
    sql, params = warehouse.queries[0]
    assert "agent_runs" not in sql
    assert f"FROM {ENRICHMENT_TABLE}" in sql
    assert "failed_runs" in sql and STATUS_ERROR in params
    assert 3 in params


def test_candidate_query_error_window_applies_to_failed_runs_cte() -> None:
    warehouse = FakeWarehouse([])
    load_audio_enrichment_candidates(
        warehouse,
        source=APPLE_MESSAGES_AUDIO_SOURCE,
        provider="agent_codex",
        prompt_version=APPLE_MESSAGES_AUDIO_SOURCE.prompt_version,
        limit=5,
        error_window_days=14,
    )
    sql, params = warehouse.queries[0]
    # The failed_runs CTE now reads file_attachment_enrichments.updated_at (the
    # only timestamp that table has), not agent_runs.started_at.
    assert sql.count("updated_at >") >= 1
    assert 14 in params
    assert list(COMPLETED_STATUSES) in params


def test_audio_transcript_cleanup_prompt_embeds_raw_transcript() -> None:
    prompt = audio_transcript_cleanup_prompt(
        transcript="hey are we still on for dinner",
        candidate={"filename": "voice.m4a", "mime_type": "audio/x-m4a"},
    )
    payload = json.loads(prompt)
    assert payload["transcript"]["raw_text"] == "hey are we still on for dinner"
    assert payload["final_output_contract"]["schema"] == audio_transcript_cleanup_schema()


def test_validate_audio_transcript_cleanup_result_flags_bad_shapes() -> None:
    assert validate_audio_transcript_cleanup_result(useful_output()) == []

    issues = validate_audio_transcript_cleanup_result({"is_useful": "yes", "entities": "SpaceX"})
    assert any("is_useful" in issue for issue in issues)
    assert any("entities" in issue for issue in issues)

    issues = validate_audio_transcript_cleanup_result({**useful_output(), "cleaned_transcript": "  "})
    assert any("cleaned_transcript must not be empty" in issue for issue in issues)


def test_audio_transcript_enrichment_text_empty_for_non_useful() -> None:
    assert audio_transcript_enrichment_text({**useful_output(), "is_useful": False}) == ""


def test_audio_transcript_enrichment_text_includes_cleaned_transcript() -> None:
    text = audio_transcript_enrichment_text(useful_output())
    assert "dinner tonight" in text
    assert "Summary: A quick check-in about dinner plans." in text
