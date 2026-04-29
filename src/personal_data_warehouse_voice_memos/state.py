from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
import json
import os
import tempfile
from typing import Any

from personal_data_warehouse_voice_memos.scanner import VoiceMemoFileCandidate

STATE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class VoiceMemoUploadStateEntry:
    path: str
    size_bytes: int
    mtime_ns: int
    birthtime_ns: int
    content_sha256: str
    audio_uploaded: bool
    metadata_uploaded: bool
    last_success_at: str
    last_failure_at: str = ""
    last_error: str = ""
    last_checked_at: str = ""

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> VoiceMemoUploadStateEntry | None:
        try:
            return cls(
                path=str(value.get("path", "")),
                size_bytes=int(value.get("size_bytes", 0) or 0),
                mtime_ns=int(value.get("mtime_ns", 0) or 0),
                birthtime_ns=int(value.get("birthtime_ns", 0) or 0),
                content_sha256=str(value.get("content_sha256", "")),
                audio_uploaded=bool(value.get("audio_uploaded", False)),
                metadata_uploaded=bool(value.get("metadata_uploaded", False)),
                last_success_at=str(value.get("last_success_at", "")),
                last_failure_at=str(value.get("last_failure_at", "")),
                last_error=str(value.get("last_error", "")),
                last_checked_at=str(value.get("last_checked_at", "")),
            )
        except (TypeError, ValueError):
            return None

    def matches(self, candidate: VoiceMemoFileCandidate) -> bool:
        return (
            self.size_bytes == candidate.size_bytes
            and self.mtime_ns == candidate.mtime_ns
            and self.birthtime_ns == candidate.birthtime_ns
        )

    @property
    def complete(self) -> bool:
        return self.audio_uploaded and self.metadata_uploaded and bool(self.content_sha256)


@dataclass
class VoiceMemosUploadState:
    account: str
    recordings_path: str
    entries: dict[str, VoiceMemoUploadStateEntry]

    @classmethod
    def empty(cls, *, account: str, recordings_path: Path) -> VoiceMemosUploadState:
        return cls(account=account, recordings_path=str(recordings_path), entries={})

    @classmethod
    def load(cls, path: Path, *, account: str, recordings_path: Path) -> VoiceMemosUploadState:
        try:
            payload = json.loads(path.read_text())
        except FileNotFoundError:
            return cls.empty(account=account, recordings_path=recordings_path)
        except (json.JSONDecodeError, OSError):
            return cls.empty(account=account, recordings_path=recordings_path)

        if not isinstance(payload, dict) or payload.get("schema_version") != STATE_SCHEMA_VERSION:
            return cls.empty(account=account, recordings_path=recordings_path)
        if str(payload.get("account", "")) != account:
            return cls.empty(account=account, recordings_path=recordings_path)
        if str(payload.get("recordings_path", "")) != str(recordings_path):
            return cls.empty(account=account, recordings_path=recordings_path)

        raw_entries = payload.get("entries", {})
        entries: dict[str, VoiceMemoUploadStateEntry] = {}
        if isinstance(raw_entries, dict):
            for key, raw_entry in raw_entries.items():
                if isinstance(raw_entry, dict):
                    entry = VoiceMemoUploadStateEntry.from_mapping(raw_entry)
                    if entry is not None:
                        entries[str(key)] = entry
        return cls(account=account, recordings_path=str(recordings_path), entries=entries)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": STATE_SCHEMA_VERSION,
            "account": self.account,
            "recordings_path": self.recordings_path,
            "entries": {key: asdict(entry) for key, entry in sorted(self.entries.items())},
        }
        encoded = json.dumps(payload, sort_keys=True, indent=2).encode("utf-8")
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as file:
            temp_name = file.name
            file.write(encoded)
            file.write(b"\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_name, path)

    def entry_for(self, candidate: VoiceMemoFileCandidate) -> VoiceMemoUploadStateEntry | None:
        return self.entries.get(state_key(candidate.path, root=Path(self.recordings_path)))

    def mark_success(
        self,
        *,
        candidate: VoiceMemoFileCandidate,
        content_sha256: str,
        audio_uploaded: bool,
        metadata_uploaded: bool,
        now: datetime,
    ) -> None:
        key = state_key(candidate.path, root=Path(self.recordings_path))
        self.entries[key] = VoiceMemoUploadStateEntry(
            path=key,
            size_bytes=candidate.size_bytes,
            mtime_ns=candidate.mtime_ns,
            birthtime_ns=candidate.birthtime_ns,
            content_sha256=content_sha256,
            audio_uploaded=audio_uploaded,
            metadata_uploaded=metadata_uploaded,
            last_success_at=now.astimezone(UTC).isoformat(),
            last_checked_at=now.astimezone(UTC).isoformat(),
        )

    def mark_failure(
        self,
        *,
        candidate: VoiceMemoFileCandidate,
        content_sha256: str = "",
        error: str,
        now: datetime,
    ) -> None:
        key = state_key(candidate.path, root=Path(self.recordings_path))
        existing = self.entries.get(key)
        self.entries[key] = VoiceMemoUploadStateEntry(
            path=key,
            size_bytes=candidate.size_bytes,
            mtime_ns=candidate.mtime_ns,
            birthtime_ns=candidate.birthtime_ns,
            content_sha256=content_sha256 or (existing.content_sha256 if existing else ""),
            audio_uploaded=existing.audio_uploaded if existing else False,
            metadata_uploaded=existing.metadata_uploaded if existing else False,
            last_success_at=existing.last_success_at if existing else "",
            last_failure_at=now.astimezone(UTC).isoformat(),
            last_error=error,
            last_checked_at=now.astimezone(UTC).isoformat(),
        )


def default_state_file() -> Path:
    return (
        Path.home()
        / "Library"
        / "Application Support"
        / "personal-data-warehouse"
        / "voice-memos-upload-state.json"
    )


def state_key(path: Path, *, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return path.name
