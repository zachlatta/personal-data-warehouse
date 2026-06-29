from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import threading

from personal_data_warehouse_voice_memos.scanner import (
    VoiceMemoFileCandidate,
    VoiceMemoRecording,
    recording_from_candidate,
    scan_voice_memo_file_candidates,
)
from personal_data_warehouse_voice_memos.state import VoiceMemosUploadState

PARTIAL_LOCAL_DURATION_TOLERANCE_SECONDS = 5.0


@dataclass(frozen=True)
class VoiceMemosUploadSummary:
    recordings_seen: int
    recordings_skipped: int
    recordings_uploaded: int
    metadata_uploaded: int
    bytes_seen: int = 0
    bytes_uploaded: int = 0
    bytes_skipped: int = 0
    recordings_selected: int = 0
    recordings_deferred: int = 0


@dataclass(frozen=True)
class _RecordingSyncResult:
    uploaded: int
    skipped: int
    metadata_uploaded: int
    bytes_uploaded: int
    bytes_skipped: int
    audio_present: bool = False
    metadata_present: bool = False


class VoiceMemosUploadRunner:
    def __init__(
        self,
        *,
        account: str,
        recordings_path: Path | str,
        extensions: tuple[str, ...],
        ingest_client,
        logger,
        now=None,
        limit: int | None = None,
        workers: int = 1,
        mode: str = "full",
        upload_state: VoiceMemosUploadState | None = None,
        min_file_age_seconds: int = 0,
        before_upload_check: Callable[[], str | None] | None = None,
        max_upload_bytes: int | None = None,
    ) -> None:
        # Uploads go through the app's ingest endpoints only.
        if ingest_client is None:
            raise ValueError("ingest_client is required")
        # Recordings larger than what the chosen upload route can deliver (the
        # app's cap, or Cloudflare's 100 MiB edge limit on the public host) are
        # deferred rather than retried forever: a single oversized memo would
        # otherwise 413 on every run and, because the failure re-raises, wedge
        # the whole sync so newer memos never upload. Default to the client's
        # advertised ceiling so the limit tracks the route actually in use.
        if max_upload_bytes is None:
            max_upload_bytes = getattr(ingest_client, "effective_max_upload_bytes", None)
        self._max_upload_bytes = max_upload_bytes if (max_upload_bytes and max_upload_bytes > 0) else None
        self._account = account
        self._recordings_path = Path(recordings_path).expanduser()
        self._extensions = extensions
        self._ingest_client = ingest_client
        self._state_lock = threading.Lock()
        self._logger = logger
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._limit = limit
        self._workers = max(1, workers)
        if mode not in {"full", "incremental"}:
            raise ValueError("mode must be 'full' or 'incremental'")
        self._mode = mode
        self._upload_state = upload_state
        self._min_file_age_seconds = max(0, min_file_age_seconds)
        self._before_upload_check = before_upload_check

    def sync(self) -> VoiceMemosUploadSummary:
        self._logger.info(
            "Scanning Voice Memos in %s for extensions: %s",
            self._recordings_path,
            ", ".join(self._extensions),
        )
        candidates = scan_voice_memo_file_candidates(self._recordings_path, extensions=self._extensions)
        if self._mode == "incremental":
            return self._sync_incremental(candidates)

        if self._limit is not None:
            candidates = candidates[: self._limit]
        recordings_seen = len(candidates)
        bytes_seen = sum(candidate.size_bytes for candidate in candidates)
        partial_candidates = [candidate for candidate in candidates if voice_memo_candidate_is_partially_materialized(candidate)]
        partial_candidate_set = set(partial_candidates)
        candidates = [candidate for candidate in candidates if candidate not in partial_candidate_set]
        for candidate in partial_candidates:
            self._logger.warning(
                "Deferring %s because Voice Memos reports %.2fs total but only %.2fs local audio",
                candidate.filename,
                candidate.duration_seconds or 0,
                candidate.local_duration_seconds or 0,
            )
        recordings = [(candidate, recording_from_candidate(candidate)) for candidate in candidates]
        self._logger.info(
            "Found %s Voice Memos recordings totaling %s",
            recordings_seen,
            format_bytes(bytes_seen),
        )
        if recordings and self._before_upload_check is not None:
            skip_reason = self._before_upload_check()
            if skip_reason:
                self._logger.warning("Skipping Voice Memos upload: %s", skip_reason)
                return VoiceMemosUploadSummary(
                    recordings_seen=recordings_seen,
                    recordings_skipped=0,
                    recordings_uploaded=0,
                    metadata_uploaded=0,
                    bytes_seen=bytes_seen,
                    recordings_selected=len(recordings),
                    recordings_deferred=len(recordings) + len(partial_candidates),
                )
        self._logger.info("Uploading with %s worker(s)", self._workers)

        if self._workers == 1 or len(recordings) <= 1:
            results = [
                self._sync_candidate(index=index, total=len(recordings), candidate=candidate, recording=recording)
                for index, (candidate, recording) in enumerate(recordings, start=1)
            ]
        else:
            with ThreadPoolExecutor(max_workers=self._workers, thread_name_prefix="voice-memos") as executor:
                futures = [
                    executor.submit(
                        self._sync_candidate,
                        index=index,
                        total=len(recordings),
                        candidate=candidate,
                        recording=recording,
                    )
                    for index, (candidate, recording) in enumerate(recordings, start=1)
                ]
                results = [future.result() for future in as_completed(futures)]

        uploaded = sum(result.uploaded for result in results)
        skipped = sum(result.skipped for result in results)
        metadata_uploaded = sum(result.metadata_uploaded for result in results)
        bytes_uploaded = sum(result.bytes_uploaded for result in results)
        bytes_skipped = sum(result.bytes_skipped for result in results)

        summary = VoiceMemosUploadSummary(
            recordings_seen=recordings_seen,
            recordings_skipped=skipped,
            recordings_uploaded=uploaded,
            metadata_uploaded=metadata_uploaded,
            bytes_seen=bytes_seen,
            bytes_uploaded=bytes_uploaded,
            bytes_skipped=bytes_skipped,
            recordings_selected=len(recordings),
            recordings_deferred=len(partial_candidates),
        )
        self._logger.info(
            "Voice Memos upload summary: seen=%s (%s), uploaded=%s (%s), skipped=%s (%s), metadata=%s",
            summary.recordings_seen,
            format_bytes(summary.bytes_seen),
            summary.recordings_uploaded,
            format_bytes(summary.bytes_uploaded),
            summary.recordings_skipped,
            format_bytes(summary.bytes_skipped),
            summary.metadata_uploaded,
        )
        return summary

    def _sync_incremental(self, candidates: list[VoiceMemoFileCandidate]) -> VoiceMemosUploadSummary:
        if self._limit is not None:
            candidates = candidates[: self._limit]
        bytes_seen = sum(candidate.size_bytes for candidate in candidates)
        self._logger.info(
            "Found %s Voice Memos recordings totaling %s",
            len(candidates),
            format_bytes(bytes_seen),
        )
        selected: list[VoiceMemoFileCandidate] = []
        state_skipped = 0
        age_deferred = 0
        partial_deferred = 0
        oversize_deferred = 0
        now = self._now()

        for candidate in candidates:
            if self._min_file_age_seconds and (now - candidate.file_modified_at).total_seconds() < self._min_file_age_seconds:
                age_deferred += 1
                continue
            if self._max_upload_bytes is not None and candidate.size_bytes > self._max_upload_bytes:
                oversize_deferred += 1
                self._logger.warning(
                    "Deferring %s (%s): exceeds the %s upload ceiling for the current route",
                    candidate.filename,
                    format_bytes(candidate.size_bytes),
                    format_bytes(self._max_upload_bytes),
                )
                continue
            if voice_memo_candidate_is_partially_materialized(candidate):
                partial_deferred += 1
                self._logger.warning(
                    "Deferring %s because Voice Memos reports %.2fs total but only %.2fs local audio",
                    candidate.filename,
                    candidate.duration_seconds or 0,
                    candidate.local_duration_seconds or 0,
                )
                continue
            entry = self._upload_state.entry_for(candidate) if self._upload_state is not None else None
            if entry is not None and entry.complete and entry.matches(candidate):
                state_skipped += 1
                continue
            selected.append(candidate)

        self._logger.info(
            "Incremental selection: selected=%s skipped=%s deferred=%s",
            len(selected),
            state_skipped,
            age_deferred + partial_deferred + oversize_deferred,
        )
        if not selected:
            return VoiceMemosUploadSummary(
                recordings_seen=len(candidates),
                recordings_skipped=state_skipped,
                recordings_uploaded=0,
                metadata_uploaded=0,
                bytes_seen=bytes_seen,
                bytes_uploaded=0,
                bytes_skipped=sum(candidate.size_bytes for candidate in candidates if self._is_state_complete(candidate)),
                recordings_selected=0,
                recordings_deferred=age_deferred + partial_deferred + oversize_deferred,
            )

        if self._before_upload_check is not None:
            skip_reason = self._before_upload_check()
            if skip_reason:
                self._logger.warning("Skipping Voice Memos upload: %s", skip_reason)
                return VoiceMemosUploadSummary(
                    recordings_seen=len(candidates),
                    recordings_skipped=state_skipped,
                    recordings_uploaded=0,
                    metadata_uploaded=0,
                    bytes_seen=bytes_seen,
                    bytes_uploaded=0,
                    bytes_skipped=sum(candidate.size_bytes for candidate in candidates if self._is_state_complete(candidate)),
                    recordings_selected=len(selected),
                    recordings_deferred=len(selected) + age_deferred + partial_deferred + oversize_deferred,
                )

        self._logger.info("Uploading with %s worker(s)", self._workers)
        recordings = [(candidate, recording_from_candidate(candidate)) for candidate in selected]
        if self._workers == 1 or len(recordings) <= 1:
            results = [
                self._sync_candidate(index=index, total=len(recordings), candidate=candidate, recording=recording)
                for index, (candidate, recording) in enumerate(recordings, start=1)
            ]
        else:
            with ThreadPoolExecutor(max_workers=self._workers, thread_name_prefix="voice-memos") as executor:
                futures = [
                    executor.submit(
                        self._sync_candidate,
                        index=index,
                        total=len(recordings),
                        candidate=candidate,
                        recording=recording,
                    )
                    for index, (candidate, recording) in enumerate(recordings, start=1)
                ]
                results = [future.result() for future in as_completed(futures)]

        uploaded = sum(result.uploaded for result in results)
        skipped = state_skipped + sum(result.skipped for result in results)
        metadata_uploaded = sum(result.metadata_uploaded for result in results)
        bytes_uploaded = sum(result.bytes_uploaded for result in results)
        bytes_skipped = sum(result.bytes_skipped for result in results) + sum(
            candidate.size_bytes for candidate in candidates if self._is_state_complete(candidate)
        )
        summary = VoiceMemosUploadSummary(
            recordings_seen=len(candidates),
            recordings_skipped=skipped,
            recordings_uploaded=uploaded,
            metadata_uploaded=metadata_uploaded,
            bytes_seen=bytes_seen,
            bytes_uploaded=bytes_uploaded,
            bytes_skipped=bytes_skipped,
            recordings_selected=len(selected),
            recordings_deferred=age_deferred + partial_deferred + oversize_deferred,
        )
        self._logger.info(
            "Voice Memos upload summary: seen=%s (%s), selected=%s, uploaded=%s (%s), skipped=%s (%s), deferred=%s, metadata=%s",
            summary.recordings_seen,
            format_bytes(summary.bytes_seen),
            summary.recordings_selected,
            summary.recordings_uploaded,
            format_bytes(summary.bytes_uploaded),
            summary.recordings_skipped,
            format_bytes(summary.bytes_skipped),
            summary.recordings_deferred,
            summary.metadata_uploaded,
        )
        return summary

    def _sync_candidate(
        self,
        *,
        index: int,
        total: int,
        candidate: VoiceMemoFileCandidate,
        recording: VoiceMemoRecording,
    ) -> _RecordingSyncResult:
        try:
            result = self._sync_recording(index=index, total=total, recording=recording)
            if self._upload_state is not None:
                with self._state_lock:
                    self._upload_state.mark_success(
                        candidate=candidate,
                        content_sha256=recording.content_sha256,
                        audio_uploaded=result.audio_present,
                        metadata_uploaded=result.metadata_present,
                        now=self._now(),
                    )
            return result
        except Exception as exc:
            if self._upload_state is not None:
                with self._state_lock:
                    self._upload_state.mark_failure(
                        candidate=candidate,
                        content_sha256=recording.content_sha256,
                        error=str(exc),
                        now=self._now(),
                    )
            raise

    def _is_state_complete(self, candidate: VoiceMemoFileCandidate) -> bool:
        entry = self._upload_state.entry_for(candidate) if self._upload_state is not None else None
        return bool(entry is not None and entry.complete and entry.matches(candidate))

    def _sync_recording(self, *, index: int, total: int, recording: VoiceMemoRecording) -> _RecordingSyncResult:
        # The app owns object keys, kinds, and pdw_* tags and dedups by content
        # sha, so we always send and let the app collapse duplicates.
        recorded_at = recording.recorded_at.isoformat()
        self._logger.info(
            "[%s/%s] upload %s (%s, sha256=%s) -> app",
            index,
            total,
            recording.filename,
            format_bytes(recording.size_bytes),
            short_sha256(recording.content_sha256),
        )
        self._ingest_client.upload_voice_memo_audio(
            recording.path.read_bytes(),
            recorded_at=recorded_at,
            extension=recording.extension,
            content_type=recording.content_type,
        )
        metadata_payload = build_metadata(
            account=self._account,
            recording=recording,
            uploaded_at=self._now(),
        )
        self._ingest_client.upload_voice_memo_metadata(
            metadata_payload,
            recorded_at=recorded_at,
            audio_content_sha256=recording.content_sha256,
        )
        return _RecordingSyncResult(
            uploaded=1,
            skipped=0,
            metadata_uploaded=1,
            bytes_uploaded=recording.size_bytes,
            bytes_skipped=0,
            audio_present=True,
            metadata_present=True,
        )


def build_metadata(
    *,
    account: str,
    recording: VoiceMemoRecording,
    uploaded_at: datetime,
) -> dict[str, object]:
    recording_payload = {
        "recording_id": recording.recording_id,
        "title": recording.title,
        "original_path": str(recording.path),
        "filename": recording.filename,
        "extension": recording.extension,
        "content_type": recording.content_type,
        "size_bytes": recording.size_bytes,
        "content_sha256": recording.content_sha256,
        "file_created_at": recording.file_created_at.isoformat(),
        "file_modified_at": recording.file_modified_at.isoformat(),
        "recorded_at": recording.recorded_at.isoformat(),
    }
    if recording.duration_seconds is not None:
        recording_payload["duration_seconds"] = recording.duration_seconds
    if recording.local_duration_seconds is not None:
        recording_payload["local_duration_seconds"] = recording.local_duration_seconds
    return {
        "schema_version": 1,
        "source": "apple_voice_memos",
        "account": account,
        "uploaded_at": uploaded_at.isoformat(),
        "recording": recording_payload,
    }


def voice_memo_candidate_is_partially_materialized(candidate: VoiceMemoFileCandidate) -> bool:
    if candidate.duration_seconds is None or candidate.local_duration_seconds is None:
        return False
    if candidate.duration_seconds > 0 and candidate.local_duration_seconds <= 0:
        return True
    return candidate.duration_seconds > candidate.local_duration_seconds + PARTIAL_LOCAL_DURATION_TOLERANCE_SECONDS


def short_sha256(value: str) -> str:
    return value[:12]


def format_bytes(size: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"
