"""Transcribe a piece of audio once per byte content, whatever passes see it.

AssemblyAI is billed per second of audio, and every pass that sends audio to it
used to decide "already transcribed" by its OWN key: the voice runner by
(source, account, recording_id), the attachment runners by (sha, agent
provider, agent model, prompt version). So the same bytes were paid for again
whenever they reached a pass under a new key -- a phone migration that copied
the iPhone memos into the Pixel's Recorder library re-transcribed every one of
them under google_recorder ids, and an agent model change re-transcribed every
WhatsApp and iMessage voice message although only the cleanup prompt had
changed.

The bytes are the identity. ``derived_enrichment.audio_transcripts`` holds the
provider's full result keyed by (content_sha256, provider), and every
transcribing pass goes through ``ContentAddressedTranscriber``: a hash already
on file is answered from the table without downloading the audio or calling
the provider, under any filename, source or prompt. Passes keep writing their
own per-recording / per-attachment rows from the result, so nothing downstream
changes; only the paid call is shared.

Only a byte-identical file is matched. A re-encoded copy (a voice memo
forwarded through a chat app that transcodes it) has new bytes and is
transcribed again; an acoustic fingerprint would be the layer for that, and
nothing measured so far says it is needed.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class TranscriptionOutcome:
    result: Mapping[str, Any]
    # True when the result came from the table and no audio was sent anywhere.
    reused: bool


def audio_transcript_row(
    *, content_sha256: str, provider: str, result: Mapping[str, Any], created_at: datetime
) -> dict[str, Any]:
    return {
        "content_sha256": content_sha256,
        "provider": provider,
        "provider_transcript_id": str(result.get("id", "") or ""),
        "model": str(result.get("speech_model_used") or ",".join(result.get("speech_models") or [])),
        "transcript_text": str(result.get("text", "") or ""),
        "raw_result_json": json.dumps(result, sort_keys=True, separators=(",", ":")),
        "created_at": created_at,
        "sync_version": int(created_at.timestamp() * 1_000_000),
    }


class ContentAddressedTranscriber:
    """A transcription client wrapped in the warehouse's per-hash result table."""

    def __init__(
        self,
        *,
        warehouse,
        client,
        provider: str,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._client = client
        self._provider = provider
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._ensured = False

    def transcribe(
        self,
        *,
        content_sha256: str,
        audio_file: Callable[[], AbstractContextManager[Path]],
        content_type: str,
        duration_seconds: float | None = None,
    ) -> TranscriptionOutcome:
        """The provider's result for these bytes, calling the provider only if none is on file.

        ``audio_file`` is only entered on a miss, so a hit costs neither the
        provider call nor the object-store download. A row without a hash
        cannot be matched and is always sent (and not stored).
        """
        if not self._ensured:
            self._warehouse.ensure_audio_transcript_tables()
            self._ensured = True
        if content_sha256:
            stored = self._warehouse.load_audio_transcript(content_sha256=content_sha256, provider=self._provider)
            if stored is not None:
                return TranscriptionOutcome(result=stored, reused=True)
        with audio_file() as path:
            kwargs: dict[str, Any] = {"path": path, "content_type": content_type}
            if duration_seconds is not None:
                kwargs["duration_seconds"] = duration_seconds
            result = self._client.transcribe_file(**kwargs)
        # Only a finished transcript describes the bytes. An error result is a
        # property of that attempt, and storing it would pin the failure to
        # the hash for every later pass.
        if content_sha256 and str(result.get("status", "")) == "completed":
            self._warehouse.insert_audio_transcripts(
                [
                    audio_transcript_row(
                        content_sha256=content_sha256,
                        provider=self._provider,
                        result=result,
                        created_at=self._now(),
                    )
                ]
            )
        return TranscriptionOutcome(result=result, reused=False)
