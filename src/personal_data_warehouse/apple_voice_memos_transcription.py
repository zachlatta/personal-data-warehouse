from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json
import re
import tempfile
import time
from typing import Any

from googleapiclient.http import MediaIoBaseDownload
import requests


ASSEMBLYAI_PROVIDER = "assemblyai"
ASSEMBLYAI_SPEECH_MODELS = ("universal-3-pro", "universal-2")
DEFAULT_ASSEMBLYAI_SPEAKER_OPTIONS = {"min_speakers_expected": 1, "max_speakers_expected": 8}
MAX_ASSEMBLYAI_ERROR_BODY_CHARS = 2000
ASSEMBLYAI_KEYTERMS_PROMPT = (
    "Hack Club",
    "Congressional App Challenge",
    "OpenRouter",
    "Hackatime",
    "OpenAI",
    "Anthropic",
    "Gemma",
    "Code.org",
    "Stardance",
    "Challenger",
    "Spindrift",
    "Pellegrino",
    "Framework",
)
ASSEMBLYAI_CUSTOM_SPELLING = (
    {"from": ["hackertime", "hacker time", "hacka time"], "to": "Hackatime"},
    {"from": ["open router"], "to": "OpenRouter"},
    {"from": ["open ai"], "to": "OpenAI"},
    {"from": ["anthropic"], "to": "Anthropic"},
    {"from": ["stardance", "star dance", "start dance"], "to": "Stardance"},
    {"from": ["spindrift"], "to": "Spindrift"},
    {"from": ["pellegrino"], "to": "Pellegrino"},
)


@dataclass(frozen=True)
class VoiceMemosTranscriptionSummary:
    recordings_seen: int
    recordings_transcribed: int
    recordings_failed: int
    segments_written: int


class AssemblyAIClient:
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://api.assemblyai.com",
        poll_interval_seconds: int = 5,
        timeout_seconds: int = 1800,
        speaker_options: Mapping[str, int] | None = None,
        session=None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._poll_interval_seconds = poll_interval_seconds
        self._timeout_seconds = timeout_seconds
        self._speaker_options = dict(speaker_options or DEFAULT_ASSEMBLYAI_SPEAKER_OPTIONS)
        self._session = session or requests.Session()
        self._sleep = sleep
        self._headers = {"authorization": api_key}

    def transcribe_file(self, *, path: Path, content_type: str) -> Mapping[str, Any]:
        upload_url = self.upload_file(path=path, content_type=content_type)
        transcript_id = self.submit_transcript(audio_url=upload_url)
        return self.poll_transcript(transcript_id=transcript_id)

    def upload_file(self, *, path: Path, content_type: str) -> str:
        with path.open("rb") as file:
            response = self._session.post(
                f"{self._base_url}/v2/upload",
                headers={**self._headers, "content-type": content_type or "application/octet-stream"},
                data=file,
                timeout=self._timeout_seconds,
            )
        _raise_for_status_with_body(response)
        payload = response.json()
        upload_url = str(payload.get("upload_url", ""))
        if not upload_url:
            raise RuntimeError("AssemblyAI upload response did not include upload_url")
        return upload_url

    def submit_transcript(self, *, audio_url: str) -> str:
        response = self._session.post(
            f"{self._base_url}/v2/transcript",
            headers={**self._headers, "content-type": "application/json"},
            json=assemblyai_transcript_request(audio_url=audio_url, speaker_options=self._speaker_options),
            timeout=self._timeout_seconds,
        )
        _raise_for_status_with_body(response)
        payload = response.json()
        transcript_id = str(payload.get("id", ""))
        if not transcript_id:
            raise RuntimeError("AssemblyAI transcript response did not include id")
        return transcript_id

    def poll_transcript(self, *, transcript_id: str) -> Mapping[str, Any]:
        deadline = time.monotonic() + self._timeout_seconds
        while True:
            response = self._session.get(
                f"{self._base_url}/v2/transcript/{transcript_id}",
                headers=self._headers,
                timeout=self._timeout_seconds,
            )
            _raise_for_status_with_body(response)
            payload = response.json()
            status = str(payload.get("status", ""))
            if status == "completed":
                return payload
            if status == "error":
                raise RuntimeError(str(payload.get("error", "AssemblyAI transcription failed")))
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for AssemblyAI transcript {transcript_id}")
            self._sleep(self._poll_interval_seconds)


def _raise_for_status_with_body(response) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        body = str(getattr(response, "text", "") or "").strip()
        if body:
            if len(body) > MAX_ASSEMBLYAI_ERROR_BODY_CHARS:
                body = f"{body[:MAX_ASSEMBLYAI_ERROR_BODY_CHARS]}...<truncated>"
            raise RuntimeError(f"{exc}; response_body={body}") from exc
        raise


def assemblyai_transcript_request(
    *,
    audio_url: str,
    speaker_options: Mapping[str, int] | None = DEFAULT_ASSEMBLYAI_SPEAKER_OPTIONS,
) -> dict[str, object]:
    request: dict[str, object] = {
        "audio_url": audio_url,
        "speech_models": list(ASSEMBLYAI_SPEECH_MODELS),
        "language_detection": True,
        "speaker_labels": True,
        "format_text": True,
        "punctuate": True,
        "disfluencies": False,
        "entity_detection": True,
        "keyterms_prompt": list(ASSEMBLYAI_KEYTERMS_PROMPT),
        "custom_spelling": [dict(item) for item in ASSEMBLYAI_CUSTOM_SPELLING],
    }
    if speaker_options:
        request["speaker_options"] = dict(speaker_options)
    return request


class GoogleDriveVoiceMemoAudioSource:
    def __init__(self, *, service) -> None:
        self._service = service

    @contextmanager
    def audio_file(self, recording: Mapping[str, Any]) -> Iterator[Path]:
        file_id = str(recording.get("storage_file_id", ""))
        if not file_id:
            raise ValueError(f"Voice Memo {recording.get('recording_id', '')} is missing storage_file_id")
        filename = str(recording.get("filename", "")) or f"{recording.get('recording_id', 'recording')}.audio"
        with tempfile.TemporaryDirectory(prefix="voice-memo-audio-") as directory:
            path = Path(directory) / filename
            request = self._service.files().get_media(fileId=file_id, supportsAllDrives=True)
            with path.open("wb") as output:
                downloader = MediaIoBaseDownload(output, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk(num_retries=2)
            yield path


class VoiceMemosTranscriptionRunner:
    def __init__(
        self,
        *,
        warehouse,
        audio_source,
        transcription_client: AssemblyAIClient,
        logger,
        now: Callable[[], datetime] | None = None,
        provider: str = ASSEMBLYAI_PROVIDER,
    ) -> None:
        self._warehouse = warehouse
        self._audio_source = audio_source
        self._transcription_client = transcription_client
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._provider = provider

    def sync(self, *, limit: int) -> VoiceMemosTranscriptionSummary:
        self._warehouse.ensure_apple_voice_memos_tables()
        recordings = self._warehouse.load_untranscribed_apple_voice_memos_files(provider=self._provider, limit=limit)
        transcribed = 0
        failed = 0
        segments_written = 0
        for index, recording in enumerate(recordings, start=1):
            recording_id = str(recording.get("recording_id", ""))
            self._logger.info("[%s/%s] transcribing %s", index, len(recordings), recording_id)
            requested_at = self._now()
            try:
                with self._audio_source.audio_file(recording) as path:
                    result = self._transcription_client.transcribe_file(
                        path=path,
                        content_type=str(recording.get("content_type", "")),
                    )
                completed_at = self._now()
                self._warehouse.insert_apple_voice_memos_transcription_runs(
                    [transcription_run_row(recording, result, requested_at=requested_at, completed_at=completed_at)]
                )
                segment_rows = transcription_segment_rows(recording, result, created_at=completed_at)
                self._warehouse.insert_apple_voice_memos_transcript_segments(segment_rows)
                transcribed += 1
                segments_written += len(segment_rows)
                self._logger.info(
                    "[%s/%s] transcribed %s: %s segments",
                    index,
                    len(recordings),
                    recording_id,
                    len(segment_rows),
                )
            except Exception as exc:
                failed += 1
                completed_at = self._now()
                self._warehouse.insert_apple_voice_memos_transcription_runs(
                    [
                        failed_transcription_run_row(
                            recording,
                            provider=self._provider,
                            error=str(exc),
                            requested_at=requested_at,
                            completed_at=completed_at,
                        )
                    ]
                )
                self._logger.warning("[%s/%s] failed %s: %s", index, len(recordings), recording_id, exc)
        return VoiceMemosTranscriptionSummary(
            recordings_seen=len(recordings),
            recordings_transcribed=transcribed,
            recordings_failed=failed,
            segments_written=segments_written,
        )


def transcription_run_row(
    recording: Mapping[str, Any],
    result: Mapping[str, Any],
    *,
    requested_at: datetime,
    completed_at: datetime,
) -> dict[str, Any]:
    return {
        "account": str(recording.get("account", "")),
        "recording_id": str(recording.get("recording_id", "")),
        "content_sha256": str(recording.get("content_sha256", "")),
        "provider": ASSEMBLYAI_PROVIDER,
        "provider_transcript_id": str(result.get("id", "")),
        "model": str(result.get("speech_model_used") or ",".join(result.get("speech_models") or [])),
        "status": str(result.get("status", "")),
        "error": str(result.get("error", "") or ""),
        "transcript_text": clean_transcript_text(str(result.get("text", "") or "")),
        "raw_result_json": json.dumps(result, sort_keys=True, separators=(",", ":")),
        "requested_at": requested_at,
        "completed_at": completed_at,
        "sync_version": int(completed_at.timestamp() * 1_000_000),
    }


def failed_transcription_run_row(
    recording: Mapping[str, Any],
    *,
    provider: str,
    error: str,
    requested_at: datetime,
    completed_at: datetime,
) -> dict[str, Any]:
    return {
        "account": str(recording.get("account", "")),
        "recording_id": str(recording.get("recording_id", "")),
        "content_sha256": str(recording.get("content_sha256", "")),
        "provider": provider,
        "provider_transcript_id": "",
        "model": "",
        "status": "error",
        "error": error,
        "transcript_text": "",
        "raw_result_json": "{}",
        "requested_at": requested_at,
        "completed_at": completed_at,
        "sync_version": int(completed_at.timestamp() * 1_000_000),
    }


def transcription_segment_rows(
    recording: Mapping[str, Any],
    result: Mapping[str, Any],
    *,
    created_at: datetime,
) -> list[dict[str, Any]]:
    utterances = result.get("utterances")
    if not isinstance(utterances, list) or not utterances:
        utterances = [
            {
                "speaker": "",
                "start": 0,
                "end": 0,
                "confidence": result.get("confidence") or 0,
                "text": result.get("text") or "",
                "words": result.get("words") or [],
            }
        ]
    rows: list[dict[str, Any]] = []
    for index, utterance in enumerate(utterances):
        if not isinstance(utterance, Mapping):
            continue
        rows.append(
            {
                "account": str(recording.get("account", "")),
                "recording_id": str(recording.get("recording_id", "")),
                "provider": ASSEMBLYAI_PROVIDER,
                "provider_transcript_id": str(result.get("id", "")),
                "segment_index": index,
                "speaker_label": str(utterance.get("speaker", "") or ""),
                "start_ms": int(utterance.get("start", 0) or 0),
                "end_ms": int(utterance.get("end", 0) or 0),
                "confidence": float(utterance.get("confidence", 0) or 0),
                "text": clean_transcript_text(str(utterance.get("text", "") or "")),
                "words_json": json.dumps(utterance.get("words") or [], sort_keys=True, separators=(",", ":")),
                "created_at": created_at,
                "sync_version": int(created_at.timestamp() * 1_000_000),
            }
        )
    return rows


SPEAKER_MARKUP_RE = re.compile(r"\[Speaker(?::[^\]]+)?\]\s*", re.IGNORECASE)


def clean_transcript_text(text: str) -> str:
    cleaned = SPEAKER_MARKUP_RE.sub("", text)
    return re.sub(r"[ \t]{2,}", " ", cleaned).strip()
