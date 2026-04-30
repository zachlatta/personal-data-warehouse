from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest
import requests

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.apple_voice_memos_transcription import (
    ASSEMBLYAI_PROVIDER,
    AssemblyAIClient,
    VoiceMemosTranscriptionRunner,
    assemblyai_transcript_request,
    clean_transcript_text,
    transcription_segment_rows,
)


class FakeResponse:
    def __init__(self, payload, *, status_code: int = 200, text: str = "") -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error")

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self) -> None:
        self.posts = []
        self.gets = []
        self.get_payloads = [
            {"id": "tx1", "status": "processing"},
            {
                "id": "tx1",
                "status": "completed",
                "speech_model_used": "universal-3-pro",
                "text": "Hello there.",
                "utterances": [],
            },
        ]

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        if url.endswith("/v2/upload"):
            return FakeResponse({"upload_url": "https://cdn.example/audio"})
        return FakeResponse({"id": "tx1"})

    def get(self, url, **kwargs):
        self.gets.append((url, kwargs))
        return FakeResponse(self.get_payloads.pop(0))


class FakeLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, *args, **kwargs) -> None:
        self.messages.append(args[0] % args[1:] if len(args) > 1 else str(args[0]))

    def warning(self, *args, **kwargs) -> None:
        self.messages.append(args[0] % args[1:] if len(args) > 1 else str(args[0]))


class FakeWarehouse:
    def __init__(self) -> None:
        self.ensure_called = False
        self.recordings = [
            {
                "account": "zach@example.com",
                "recording_id": "20260427 100004-40DC0200",
                "filename": "memo.m4a",
                "content_type": "audio/mp4",
            }
        ]
        self.run_rows = []
        self.segment_rows = []

    def ensure_apple_voice_memos_tables(self) -> None:
        self.ensure_called = True

    def load_untranscribed_apple_voice_memos_files(self, *, provider: str, limit: int):
        assert provider == ASSEMBLYAI_PROVIDER
        return self.recordings[:limit]

    def insert_apple_voice_memos_transcription_runs(self, rows) -> None:
        self.run_rows.extend(rows)

    def insert_apple_voice_memos_transcript_segments(self, rows) -> None:
        self.segment_rows.extend(rows)


class FakeAudioSource:
    @contextmanager
    def audio_file(self, recording):
        path = Path("/tmp/fake-audio.m4a")
        path.write_bytes(b"audio")
        try:
            yield path
        finally:
            path.unlink(missing_ok=True)


class FakeTranscriptionClient:
    def transcribe_file(self, *, path: Path, content_type: str):
        return {
            "id": "tx1",
            "status": "completed",
            "speech_model_used": "universal-3-pro",
            "text": "Alice: hello. Bob: hi.",
            "utterances": [
                {"speaker": "A", "start": 0, "end": 900, "confidence": 0.98, "text": "Hello.", "words": []},
                {"speaker": "B", "start": 1000, "end": 1300, "confidence": 0.97, "text": "Hi.", "words": []},
            ],
        }


def test_load_settings_reads_assemblyai_config(monkeypatch) -> None:
    monkeypatch.setenv("ASSEMBLYAI_API_KEY", "test-key")

    settings = load_settings(require_clickhouse=False, require_gmail=False, require_assemblyai=True)

    assert settings.assemblyai is not None
    assert settings.assemblyai.api_key == "test-key"
    assert settings.assemblyai.base_url == "https://api.assemblyai.com"
    assert settings.assemblyai.min_speakers_expected == 1
    assert settings.assemblyai.max_speakers_expected == 8


def test_load_settings_reads_assemblyai_speaker_options(monkeypatch) -> None:
    monkeypatch.setenv("ASSEMBLYAI_API_KEY", "test-key")
    monkeypatch.setenv("ASSEMBLYAI_MIN_SPEAKERS_EXPECTED", "2")
    monkeypatch.setenv("ASSEMBLYAI_MAX_SPEAKERS_EXPECTED", "6")

    settings = load_settings(require_clickhouse=False, require_gmail=False, require_assemblyai=True)

    assert settings.assemblyai is not None
    assert settings.assemblyai.min_speakers_expected == 2
    assert settings.assemblyai.max_speakers_expected == 6


def test_assemblyai_request_enables_diarization_and_best_models() -> None:
    request = assemblyai_transcript_request(audio_url="https://cdn.example/audio")

    assert request["speaker_labels"] is True
    assert request["speaker_options"] == {"min_speakers_expected": 1, "max_speakers_expected": 8}
    assert request["language_detection"] is True
    assert request["speech_models"] == ["universal-3-pro", "universal-2"]
    assert "keyterms_prompt" in request
    assert "prompt" not in request
    assert "Pellegrino" in request["keyterms_prompt"]
    assert "Hackatime" in request["keyterms_prompt"]
    assert {"from": ["hackertime", "hacker time", "hacka time"], "to": "Hackatime"} in request["custom_spelling"]
    assert {"from": ["open router"], "to": "OpenRouter"} in request["custom_spelling"]
    assert {"from": ["stardance", "star dance", "start dance"], "to": "Stardance"} in request["custom_spelling"]


def test_assemblyai_client_uploads_submits_and_polls(tmp_path) -> None:
    audio = tmp_path / "memo.m4a"
    audio.write_bytes(b"audio")
    session = FakeSession()

    result = AssemblyAIClient(
        api_key="test-key",
        session=session,
        poll_interval_seconds=1,
        sleep=lambda _seconds: None,
    ).transcribe_file(path=audio, content_type="audio/mp4")

    assert result["status"] == "completed"
    assert session.posts[0][0].endswith("/v2/upload")
    assert session.posts[1][0].endswith("/v2/transcript")
    assert session.posts[1][1]["json"]["speaker_labels"] is True
    assert session.posts[1][1]["json"]["speaker_options"]["max_speakers_expected"] == 8
    assert len(session.gets) == 2


def test_assemblyai_client_submit_error_includes_response_body(tmp_path) -> None:
    class SubmitErrorSession(FakeSession):
        def post(self, url, **kwargs):
            self.posts.append((url, kwargs))
            if url.endswith("/v2/upload"):
                return FakeResponse({"upload_url": "https://cdn.example/audio"})
            return FakeResponse(
                {"error": "bad audio_url"},
                status_code=400,
                text='{"error":"unsupported audio format"}',
            )

    audio = tmp_path / "memo.m4a"
    audio.write_bytes(b"audio")

    with pytest.raises(RuntimeError, match="unsupported audio format"):
        AssemblyAIClient(api_key="test-key", session=SubmitErrorSession()).transcribe_file(
            path=audio,
            content_type="audio/mp4",
        )


def test_transcription_segment_rows_use_utterances() -> None:
    rows = transcription_segment_rows(
        {"account": "zach@example.com", "recording_id": "rec1"},
        {
            "id": "tx1",
            "utterances": [
                {"speaker": "A", "start": 10, "end": 20, "confidence": 0.5, "text": "Hello", "words": []}
            ],
        },
        created_at=datetime(2026, 4, 27, tzinfo=UTC),
    )

    assert rows[0]["speaker_label"] == "A"
    assert rows[0]["start_ms"] == 10
    assert rows[0]["text"] == "Hello"


def test_clean_transcript_text_removes_provider_speaker_markup() -> None:
    assert clean_transcript_text("[Speaker:JACKIE] Hello there.") == "Hello there."
    assert clean_transcript_text("[Speaker] Hey Jackie.") == "Hey Jackie."


def test_transcription_runner_writes_run_and_segments() -> None:
    warehouse = FakeWarehouse()

    summary = VoiceMemosTranscriptionRunner(
        warehouse=warehouse,
        audio_source=FakeAudioSource(),
        transcription_client=FakeTranscriptionClient(),
        logger=FakeLogger(),
        now=lambda: datetime(2026, 4, 27, tzinfo=UTC),
    ).sync(limit=3)

    assert warehouse.ensure_called
    assert summary.recordings_seen == 1
    assert summary.recordings_transcribed == 1
    assert summary.segments_written == 2
    assert warehouse.run_rows[0]["provider_transcript_id"] == "tx1"
    assert warehouse.segment_rows[0]["speaker_label"] == "A"
