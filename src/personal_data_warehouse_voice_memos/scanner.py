from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import hashlib
import mimetypes


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


def scan_voice_memo_file_candidates(
    recordings_path: Path | str,
    *,
    extensions: tuple[str, ...],
) -> list[VoiceMemoFileCandidate]:
    root = Path(recordings_path).expanduser()
    normalized_extensions = {extension.lower() for extension in extensions}
    candidates: list[VoiceMemoFileCandidate] = []

    for path in sorted(root.iterdir()):
        if not path.is_file() or path.suffix.lower() not in normalized_extensions:
            continue
        candidates.append(file_candidate_from_path(path))

    return candidates


def scan_voice_memos(recordings_path: Path | str, *, extensions: tuple[str, ...]) -> list[VoiceMemoRecording]:
    return [recording_from_path(candidate.path) for candidate in scan_voice_memo_file_candidates(recordings_path, extensions=extensions)]


def file_candidate_from_path(path: Path) -> VoiceMemoFileCandidate:
    stat = path.stat()
    created_at = datetime.fromtimestamp(file_birthtime(stat), tz=UTC)
    modified_at = datetime.fromtimestamp(stat.st_mtime, tz=UTC)
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
