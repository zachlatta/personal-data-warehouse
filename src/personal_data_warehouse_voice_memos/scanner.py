from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import hashlib
import mimetypes
import sqlite3
from typing import Mapping


@dataclass(frozen=True)
class VoiceMemoRecording:
    path: Path
    recording_id: str
    title: str
    filename: str
    extension: str
    content_type: str
    size_bytes: int
    content_sha256: str
    file_created_at: datetime
    file_modified_at: datetime
    recorded_at: datetime
    duration_seconds: float | None = None
    local_duration_seconds: float | None = None


@dataclass(frozen=True)
class VoiceMemoFileCandidate:
    path: Path
    recording_id: str
    filename: str
    extension: str
    size_bytes: int
    file_created_at: datetime
    file_modified_at: datetime
    birthtime_ns: int
    mtime_ns: int
    recorded_at: datetime
    duration_seconds: float | None = None
    local_duration_seconds: float | None = None


def scan_voice_memo_file_candidates(
    recordings_path: Path | str,
    *,
    extensions: tuple[str, ...],
) -> list[VoiceMemoFileCandidate]:
    root = Path(recordings_path).expanduser()
    normalized_extensions = {extension.lower() for extension in extensions}
    candidates: list[VoiceMemoFileCandidate] = []
    cloud_metadata = load_cloud_recording_metadata(root)

    for path in sorted(root.iterdir()):
        if not path.is_file() or path.suffix.lower() not in normalized_extensions:
            continue
        candidates.append(file_candidate_from_path(path, cloud_metadata=cloud_metadata.get(path.name)))

    return candidates


def scan_voice_memos(recordings_path: Path | str, *, extensions: tuple[str, ...]) -> list[VoiceMemoRecording]:
    return [recording_from_candidate(candidate) for candidate in scan_voice_memo_file_candidates(recordings_path, extensions=extensions)]


def file_candidate_from_path(path: Path, *, cloud_metadata: Mapping[str, float | None] | None = None) -> VoiceMemoFileCandidate:
    stat = path.stat()
    created_at = datetime.fromtimestamp(file_birthtime(stat), tz=UTC)
    modified_at = datetime.fromtimestamp(stat.st_mtime, tz=UTC)
    metadata = cloud_metadata or {}
    return VoiceMemoFileCandidate(
        path=path,
        recording_id=path.stem,
        filename=path.name,
        extension=path.suffix.lower(),
        size_bytes=stat.st_size,
        file_created_at=created_at,
        file_modified_at=modified_at,
        birthtime_ns=file_birthtime_ns(stat),
        mtime_ns=stat.st_mtime_ns,
        recorded_at=recorded_at_from_filename(path.stem) or created_at,
        duration_seconds=metadata.get("duration_seconds"),
        local_duration_seconds=metadata.get("local_duration_seconds"),
    )


def recording_from_path(path: Path) -> VoiceMemoRecording:
    stat = path.stat()
    extension = path.suffix.lower()
    created_at = datetime.fromtimestamp(file_birthtime(stat), tz=UTC)
    return VoiceMemoRecording(
        path=path,
        recording_id=path.stem,
        title=path.stem,
        filename=path.name,
        extension=extension,
        content_type=content_type_for_extension(extension),
        size_bytes=stat.st_size,
        content_sha256=file_sha256(path),
        file_created_at=created_at,
        file_modified_at=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
        recorded_at=recorded_at_from_filename(path.stem) or created_at,
    )


def recording_from_candidate(candidate: VoiceMemoFileCandidate) -> VoiceMemoRecording:
    recording = recording_from_path(candidate.path)
    return VoiceMemoRecording(
        path=recording.path,
        recording_id=recording.recording_id,
        title=recording.title,
        filename=recording.filename,
        extension=recording.extension,
        content_type=recording.content_type,
        size_bytes=recording.size_bytes,
        content_sha256=recording.content_sha256,
        file_created_at=recording.file_created_at,
        file_modified_at=recording.file_modified_at,
        recorded_at=recording.recorded_at,
        duration_seconds=candidate.duration_seconds,
        local_duration_seconds=candidate.local_duration_seconds,
    )


def load_cloud_recording_metadata(root: Path) -> dict[str, dict[str, float | None]]:
    database_path = root / "CloudRecordings.db"
    if not database_path.exists():
        return {}
    try:
        connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
    except sqlite3.Error:
        return {}
    try:
        rows = connection.execute(
            """
            SELECT ZPATH, ZDURATION, ZLOCALDURATION
            FROM ZCLOUDRECORDING
            WHERE ZPATH IS NOT NULL
            """
        ).fetchall()
    except sqlite3.Error:
        return {}
    finally:
        connection.close()
    metadata: dict[str, dict[str, float | None]] = {}
    for path, duration, local_duration in rows:
        filename = str(path or "")
        if not filename:
            continue
        metadata[filename] = {
            "duration_seconds": _optional_float(duration),
            "local_duration_seconds": _optional_float(local_duration),
        }
    return metadata


def _optional_float(value) -> float | None:
    if value is None:
        return None
    return float(value)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def content_type_for_extension(extension: str) -> str:
    if extension == ".qta":
        return "audio/quicktime"
    if extension == ".m4a":
        return "audio/mp4"
    return mimetypes.types_map.get(extension, "application/octet-stream")


def recorded_at_from_filename(stem: str) -> datetime | None:
    try:
        return datetime.strptime(stem[:15], "%Y%m%d %H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None


def file_birthtime(stat) -> float:
    return float(getattr(stat, "st_birthtime", stat.st_ctime))


def file_birthtime_ns(stat) -> int:
    if hasattr(stat, "st_birthtime_ns"):
        return int(stat.st_birthtime_ns)
    return int(file_birthtime(stat) * 1_000_000_000)
