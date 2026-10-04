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

import requests

from personal_data_warehouse.objectstore import ObjectStore
from personal_data_warehouse.schema import voice_memo_transcription_failure_status


ASSEMBLYAI_PROVIDER = "assemblyai"
# Universal-3.5 Pro is AssemblyAI's newest and most accurate model; Universal-2 is
# the fallback only for a language 3.5 Pro does not cover. This is AssemblyAI's
# own default chain. Universal-3 Pro is superseded and deliberately absent.
ASSEMBLYAI_SPEECH_MODELS = ("universal-3-5-pro", "universal-2")
DEFAULT_ASSEMBLYAI_SPEAKER_OPTIONS = {"min_speakers_expected": 1, "max_speakers_expected": 8}
# max_speakers_expected is a HARD ceiling: AssemblyAI merges every speaker past
# it into an existing label. The configured ceiling (8) suits a conversation,
# where a high ceiling over-splits one voice; a long recording is where a stage
# event, a panel or a conference session lives, and there the low ceiling
# guarantees mixed labels. 30 is AssemblyAI's own default for audio over ten
# minutes. The 5.7-hour stage-talks recording of 2026-10-03 -- ten named
# speakers, five labels -- is the case this exists for.
LONG_RECORDING_MIN_SECONDS = 30 * 60
LONG_RECORDING_MAX_SPEAKERS_EXPECTED = 30
# Diarization can return two people who share a microphone as one label and one
# multi-minute utterance (the 2026-10-03 stage talks: an interviewer and her
# guest, 12.5 minutes, one label under every speaker_options setting tried).
# Utterances longer than this are stored as sentence-bounded segments with the
# same label, so the enrichment agent's speaker_turns can split the people
# inside them.
LONG_UTTERANCE_SPLIT_MS = 60_000
SPLIT_SEGMENT_TARGET_MS = 8_000
SPLIT_SEGMENT_MAX_MS = 45_000
SENTENCE_END_CHARACTERS = (".", "?", "!")
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

    def transcribe_file(
        self, *, path: Path, content_type: str, duration_seconds: float | None = None
    ) -> Mapping[str, Any]:
        upload_url = self.upload_file(path=path, content_type=content_type)
        transcript_id = self.submit_transcript(
            audio_url=upload_url,
            speaker_options=assemblyai_speaker_options(self._speaker_options, duration_seconds=duration_seconds),
        )
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

    def submit_transcript(self, *, audio_url: str, speaker_options: Mapping[str, int]) -> str:
        response = self._session.post(
            f"{self._base_url}/v2/transcript",
            headers={**self._headers, "content-type": "application/json"},
            json=assemblyai_transcript_request(audio_url=audio_url, speaker_options=speaker_options),
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


def assemblyai_client_from_settings(settings) -> AssemblyAIClient:
    return AssemblyAIClient(
        api_key=settings.assemblyai.api_key,
        base_url=settings.assemblyai.base_url,
        poll_interval_seconds=settings.assemblyai.poll_interval_seconds,
        timeout_seconds=settings.assemblyai.timeout_seconds,
        speaker_options={
            key: value
            for key, value in {
                "min_speakers_expected": settings.assemblyai.min_speakers_expected,
                "max_speakers_expected": settings.assemblyai.max_speakers_expected,
            }.items()
            if value is not None
        },
    )


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


def assemblyai_speaker_options(
    configured: Mapping[str, int], *, duration_seconds: float | None
) -> dict[str, int]:
    """The diarization range for one recording: the configured one, widened when long."""
    options = dict(configured)
    if duration_seconds is None or float(duration_seconds) < LONG_RECORDING_MIN_SECONDS:
        return options
    options["max_speakers_expected"] = max(
        int(options.get("max_speakers_expected") or 0), LONG_RECORDING_MAX_SPEAKERS_EXPECTED
    )
    return options


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
    """Fetch voice audio for transcription, for EVERY voice source.

    A Drive file id is global, not folder-scoped, so one store serves every
    source whose bytes live in the same Drive account -- which is the default
    (base_alice_voice_recordings falls back to the Voice Memos account).
    ``object_stores`` overrides that per source for a source configured with
    its own credential, so adding one never means teaching the runner about it.
    """

    def __init__(
        self,
        *,
        object_store: ObjectStore,
        object_stores: Mapping[str, ObjectStore] | None = None,
    ) -> None:
        self._object_store = object_store
        self._object_stores = dict(object_stores or {})

    @contextmanager
    def audio_file(self, recording: Mapping[str, Any]) -> Iterator[Path]:
        file_id = str(recording.get("storage_file_id", ""))
        if not file_id:
            raise ValueError(f"Voice recording {recording.get('recording_id', '')} is missing storage_file_id")
        filename = str(recording.get("filename", "")) or f"{recording.get('recording_id', 'recording')}.audio"
        store = self._object_stores.get(str(recording.get("source", "")), self._object_store)
        with tempfile.TemporaryDirectory(prefix="voice-memo-audio-") as directory:
            path = Path(directory) / filename
            store.download_to_path(recording, path)
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
        recordings = self._warehouse.load_untranscribed_voice_recordings(provider=self._provider, limit=limit)
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
                        duration_seconds=recording_duration_seconds(recording),
                    )
                completed_at = self._now()
                self._warehouse.insert_apple_voice_memos_transcription_runs(
                    [transcription_run_row(recording, result, requested_at=requested_at, completed_at=completed_at)]
                )
                segment_rows = transcription_segment_rows(recording, result, created_at=completed_at)
                self._warehouse.replace_voice_recording_transcript_segments(
                    source=voice_recording_source(recording),
                    account=str(recording.get("account", "")),
                    recording_id=recording_id,
                    provider=self._provider,
                    provider_transcript_id=str(result.get("id", "")),
                    rows=segment_rows,
                )
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


DEFAULT_VOICE_RECORDING_SOURCE = "apple_voice_memos"


def voice_recording_source(recording: Mapping[str, Any]) -> str:
    """The voice source a candidate row came from.

    Every derived voice row is keyed by source first, so this is a key column,
    not a label. It falls back to Apple Voice Memos only because that is the
    one source that existed before the column did -- a row that reached here
    without a source can only have come from the pre-multi-source path.
    """
    return str(recording.get("source", "") or DEFAULT_VOICE_RECORDING_SOURCE)


def recording_duration_seconds(recording: Mapping[str, Any]) -> float | None:
    value = recording.get("duration_seconds")
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def transcription_run_row(
    recording: Mapping[str, Any],
    result: Mapping[str, Any],
    *,
    requested_at: datetime,
    completed_at: datetime,
) -> dict[str, Any]:
    return {
        "source": voice_recording_source(recording),
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
        "source": voice_recording_source(recording),
        "account": str(recording.get("account", "")),
        "recording_id": str(recording.get("recording_id", "")),
        "content_sha256": str(recording.get("content_sha256", "")),
        "provider": provider,
        "provider_transcript_id": "",
        "model": "",
        # 'error' only when a retry could plausibly succeed. A provider that
        # rejected the INPUT -- no speech, too short, not audio -- is recorded
        # as 'rejected', which is terminal for the candidate query but is NOT
        # in StateSource.error_statuses, so one impossible recording cannot pin
        # voice_memo_transcription to failing forever and drown out a real
        # provider outage.
        "status": voice_memo_transcription_failure_status(error),
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
    for utterance in utterances:
        if not isinstance(utterance, Mapping):
            continue
        for piece in split_long_utterance(utterance):
            rows.append(
                {
                    "source": voice_recording_source(recording),
                    "account": str(recording.get("account", "")),
                    "recording_id": str(recording.get("recording_id", "")),
                    "provider": ASSEMBLYAI_PROVIDER,
                    "provider_transcript_id": str(result.get("id", "")),
                    "segment_index": len(rows),
                    "speaker_label": str(piece.get("speaker", "") or ""),
                    "start_ms": int(piece.get("start", 0) or 0),
                    "end_ms": int(piece.get("end", 0) or 0),
                    "confidence": float(piece.get("confidence", 0) or 0),
                    "text": clean_transcript_text(str(piece.get("text", "") or "")),
                    "words_json": json.dumps(piece.get("words") or [], sort_keys=True, separators=(",", ":")),
                    "created_at": created_at,
                    "sync_version": int(created_at.timestamp() * 1_000_000),
                }
            )
    return rows


def split_long_utterance(utterance: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Split an over-long utterance into sentence-bounded pieces with its label.

    A piece closes at the first sentence end once it is SPLIT_SEGMENT_TARGET_MS
    long, and never grows past SPLIT_SEGMENT_MAX_MS. The pieces' words, in order,
    are exactly the utterance's words, so no text is lost or reordered.
    """
    start = int(utterance.get("start", 0) or 0)
    end = int(utterance.get("end", 0) or 0)
    words = [word for word in utterance.get("words") or [] if isinstance(word, Mapping)]
    if end - start <= LONG_UTTERANCE_SPLIT_MS or not words:
        return [utterance]
    chunks: list[list[Mapping[str, Any]]] = [[]]
    for word in words:
        current = chunks[-1]
        if current and int(word.get("end", 0) or 0) - int(current[0].get("start", 0) or 0) > SPLIT_SEGMENT_MAX_MS:
            chunks.append([word])
            continue
        current.append(word)
        duration = int(word.get("end", 0) or 0) - int(current[0].get("start", 0) or 0)
        if duration >= SPLIT_SEGMENT_TARGET_MS and str(word.get("text", "")).rstrip().endswith(SENTENCE_END_CHARACTERS):
            chunks.append([])
    chunks = [chunk for chunk in chunks if chunk]
    pieces: list[Mapping[str, Any]] = []
    for index, chunk in enumerate(chunks):
        confidences = [float(word.get("confidence", 0) or 0) for word in chunk]
        pieces.append(
            {
                "speaker": utterance.get("speaker", ""),
                "start": start if index == 0 else int(chunk[0].get("start", 0) or 0),
                "end": end if index == len(chunks) - 1 else int(chunk[-1].get("end", 0) or 0),
                "confidence": sum(confidences) / len(confidences),
                "text": " ".join(str(word.get("text", "")) for word in chunk),
                "words": chunk,
            }
        )
    return pieces


SPEAKER_MARKUP_RE = re.compile(r"\[Speaker(?::[^\]]+)?\]\s*", re.IGNORECASE)


def clean_transcript_text(text: str) -> str:
    cleaned = SPEAKER_MARKUP_RE.sub("", text)
    return re.sub(r"[ \t]{2,}", " ", cleaned).strip()
