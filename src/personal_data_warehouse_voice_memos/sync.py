from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import hashlib
import json
import threading

from personal_data_warehouse_voice_memos.scanner import (
    VoiceMemoFileCandidate,
    VoiceMemoRecording,
    recording_from_path,
    scan_voice_memo_file_candidates,
)
from personal_data_warehouse_voice_memos.state import VoiceMemosUploadState
from personal_data_warehouse_voice_memos.storage import ObjectPresence, ObjectStore

OBJECT_PREFIX = "apple-voice-memos"
INBOX_PREFIX = f"{OBJECT_PREFIX}/inbox"


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
        object_store: ObjectStore | None = None,
        object_store_factory: Callable[[], ObjectStore] | None = None,
        logger,
        now=None,
        limit: int | None = None,
        workers: int = 1,
        mode: str = "full",
        upload_state: VoiceMemosUploadState | None = None,
        min_file_age_seconds: int = 0,
        before_upload_check: Callable[[], str | None] | None = None,
    ) -> None:
        if object_store is None and object_store_factory is None:
            raise ValueError("object_store or object_store_factory must be provided")
        if object_store is not None and object_store_factory is not None:
            raise ValueError("pass only one of object_store or object_store_factory")
        self._account = account
        self._recordings_path = Path(recordings_path).expanduser()
        self._extensions = extensions
        self._object_store = object_store
        self._object_store_factory = object_store_factory
        self._thread_local = threading.local()
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
        recordings = [(candidate, recording_from_path(candidate.path)) for candidate in candidates]
        bytes_seen = sum(recording.size_bytes for _, recording in recordings)
        self._logger.info(
            "Found %s Voice Memos recordings totaling %s",
            len(recordings),
            format_bytes(bytes_seen),
        )
        if recordings and self._before_upload_check is not None:
            skip_reason = self._before_upload_check()
            if skip_reason:
                self._logger.warning("Skipping Voice Memos upload: %s", skip_reason)
                return VoiceMemosUploadSummary(
                    recordings_seen=len(recordings),
                    recordings_skipped=0,
                    recordings_uploaded=0,
                    metadata_uploaded=0,
                    bytes_seen=bytes_seen,
                    recordings_selected=len(recordings),
                    recordings_deferred=len(recordings),
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
            recordings_seen=len(recordings),
            recordings_skipped=skipped,
            recordings_uploaded=uploaded,
            metadata_uploaded=metadata_uploaded,
            bytes_seen=bytes_seen,
            bytes_uploaded=bytes_uploaded,
            bytes_skipped=bytes_skipped,
            recordings_selected=len(recordings),
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
        now = self._now()

        for candidate in candidates:
            if self._min_file_age_seconds and (now - candidate.file_modified_at).total_seconds() < self._min_file_age_seconds:
                age_deferred += 1
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
            age_deferred,
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
                recordings_deferred=age_deferred,
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
                    recordings_deferred=len(selected) + age_deferred,
                )

        self._logger.info("Uploading with %s worker(s)", self._workers)
        recordings = [(candidate, recording_from_path(candidate.path)) for candidate in selected]
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
            recordings_deferred=age_deferred,
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
        object_store = self._object_store_for_thread()
        presence = object_presence(object_store, content_sha256=recording.content_sha256)
        audio_exists = presence.audio_exists
        metadata_exists = presence.metadata_exists
        audio_key = audio_object_key(recording)
        metadata_key = metadata_object_key(recording)
        if audio_exists and metadata_exists:
            self._logger.info(
                "[%s/%s] skip %s (%s, sha256=%s): audio and metadata already present",
                index,
                total,
                recording.filename,
                format_bytes(recording.size_bytes),
                short_sha256(recording.content_sha256),
            )
            return _RecordingSyncResult(
                uploaded=0,
                skipped=1,
                metadata_uploaded=0,
                bytes_uploaded=0,
                bytes_skipped=recording.size_bytes,
                audio_present=True,
                metadata_present=True,
            )

        self._logger.info(
            "[%s/%s] upload %s (%s, sha256=%s) -> %s",
            index,
            total,
            recording.filename,
            format_bytes(recording.size_bytes),
            short_sha256(recording.content_sha256),
            audio_key,
        )
        if not audio_exists:
            object_store.put_file(
                path=recording.path,
                object_key=audio_key,
                content_sha256=recording.content_sha256,
                content_type=recording.content_type,
                skip_existing_check=True,
            )
        metadata_payload = build_metadata(
            account=self._account,
            recording=recording,
            uploaded_at=self._now(),
        )
        self._logger.info(
            "[%s/%s] metadata %s -> %s",
            index,
            total,
            recording.filename,
            metadata_key,
        )
        if not metadata_exists:
            object_store.put_json(
                object_key=metadata_key,
                payload=metadata_payload,
                content_sha256=json_sha256(metadata_payload),
                source_content_sha256=recording.content_sha256,
                skip_existing_check=True,
            )
        return _RecordingSyncResult(
            uploaded=1,
            skipped=0,
            metadata_uploaded=0 if metadata_exists else 1,
            bytes_uploaded=0 if audio_exists else recording.size_bytes,
            bytes_skipped=0,
            audio_present=True,
            metadata_present=True,
        )

    def _object_store_for_thread(self) -> ObjectStore:
        if self._object_store is not None:
            return self._object_store
        store = getattr(self._thread_local, "object_store", None)
        if store is None:
            if self._object_store_factory is None:
                raise RuntimeError("object_store_factory is not configured")
            store = self._object_store_factory()
            self._thread_local.object_store = store
        return store


def build_metadata(
    *,
    account: str,
    recording: VoiceMemoRecording,
    uploaded_at: datetime,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "source": "apple_voice_memos",
        "account": account,
        "uploaded_at": uploaded_at.isoformat(),
        "recording": {
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
        },
    }


def object_presence(object_store: ObjectStore, *, content_sha256: str) -> ObjectPresence:
    presence = getattr(object_store, "presence", None)
    if callable(presence):
        return presence(content_sha256=content_sha256)
    return ObjectPresence(
        audio_exists=object_store.has_blob(content_sha256=content_sha256),
        metadata_exists=object_store.has_metadata(content_sha256=content_sha256),
    )


def audio_object_key(recording: VoiceMemoRecording) -> str:
    year = f"{recording.recorded_at.year:04d}"
    month = f"{recording.recorded_at.month:02d}"
    return f"{INBOX_PREFIX}/{year}/{month}/{dated_object_basename(recording)}{recording.extension}"


def metadata_object_key(recording: VoiceMemoRecording) -> str:
    year = f"{recording.recorded_at.year:04d}"
    month = f"{recording.recorded_at.month:02d}"
    return f"{INBOX_PREFIX}/{year}/{month}/{dated_object_basename(recording)}.json"


def dated_object_basename(recording: VoiceMemoRecording) -> str:
    return f"{recording.recorded_at.date().isoformat()}-{recording.content_sha256}"


def json_sha256(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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
