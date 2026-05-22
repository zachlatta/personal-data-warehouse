from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
import json
import os
import tempfile
from typing import Any

STATE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class AppleNoteUploadStateEntry:
    note_id: str
    fingerprint: str
    revision_id: str
    title: str
    modified_at: str
    is_deleted: bool
    metadata_uploaded: bool
    html_uploaded: bool
    attachments_uploaded: bool
    last_success_at: str
    last_failure_at: str = ""
    last_error: str = ""
    last_checked_at: str = ""

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> AppleNoteUploadStateEntry | None:
        try:
            return cls(
                note_id=str(value.get("note_id", "")),
                fingerprint=str(value.get("fingerprint", "")),
                revision_id=str(value.get("revision_id", "")),
                title=str(value.get("title", "")),
                modified_at=str(value.get("modified_at", "")),
                is_deleted=bool(value.get("is_deleted", False)),
                metadata_uploaded=bool(value.get("metadata_uploaded", False)),
                html_uploaded=bool(value.get("html_uploaded", False)),
                attachments_uploaded=bool(value.get("attachments_uploaded", False)),
                last_success_at=str(value.get("last_success_at", "")),
                last_failure_at=str(value.get("last_failure_at", "")),
                last_error=str(value.get("last_error", "")),
                last_checked_at=str(value.get("last_checked_at", "")),
            )
        except (TypeError, ValueError):
            return None

    @property
    def complete(self) -> bool:
        return self.metadata_uploaded and self.attachments_uploaded


@dataclass
class AppleNotesUploadState:
    account: str
    store_path: str
    entries: dict[str, AppleNoteUploadStateEntry]

    @classmethod
    def empty(cls, *, account: str, store_path: Path | str) -> AppleNotesUploadState:
        return cls(account=account, store_path=str(store_path), entries={})

    @classmethod
    def load(cls, path: Path, *, account: str, store_path: Path | str) -> AppleNotesUploadState:
        try:
            payload = json.loads(path.read_text())
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return cls.empty(account=account, store_path=store_path)
        if not isinstance(payload, dict) or payload.get("schema_version") != STATE_SCHEMA_VERSION:
            return cls.empty(account=account, store_path=store_path)
        if str(payload.get("account", "")) != account:
            return cls.empty(account=account, store_path=store_path)
        if str(payload.get("store_path", "")) != str(store_path):
            return cls.empty(account=account, store_path=store_path)
        entries: dict[str, AppleNoteUploadStateEntry] = {}
        raw_entries = payload.get("entries", {})
        if isinstance(raw_entries, dict):
            for key, raw_entry in raw_entries.items():
                if isinstance(raw_entry, dict):
                    entry = AppleNoteUploadStateEntry.from_mapping(raw_entry)
                    if entry is not None:
                        entries[str(key)] = entry
        return cls(account=account, store_path=str(store_path), entries=entries)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": STATE_SCHEMA_VERSION,
            "account": self.account,
            "store_path": self.store_path,
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

    def entry_for(self, note_id: str) -> AppleNoteUploadStateEntry | None:
        return self.entries.get(note_id)

    def mark_success(
        self,
        *,
        note_id: str,
        fingerprint: str,
        revision_id: str,
        title: str,
        modified_at: datetime,
        is_deleted: bool,
        metadata_uploaded: bool,
        html_uploaded: bool,
        attachments_uploaded: bool,
        now: datetime,
    ) -> None:
        timestamp = now.astimezone(UTC).isoformat()
        self.entries[note_id] = AppleNoteUploadStateEntry(
            note_id=note_id,
            fingerprint=fingerprint,
            revision_id=revision_id,
            title=title,
            modified_at=modified_at.astimezone(UTC).isoformat(),
            is_deleted=is_deleted,
            metadata_uploaded=metadata_uploaded,
            html_uploaded=html_uploaded,
            attachments_uploaded=attachments_uploaded,
            last_success_at=timestamp,
            last_checked_at=timestamp,
        )

    def mark_failure(
        self,
        *,
        note_id: str,
        error: str,
        now: datetime,
    ) -> None:
        existing = self.entries.get(note_id)
        timestamp = now.astimezone(UTC).isoformat()
        self.entries[note_id] = AppleNoteUploadStateEntry(
            note_id=note_id,
            fingerprint=existing.fingerprint if existing else "",
            revision_id=existing.revision_id if existing else "",
            title=existing.title if existing else "",
            modified_at=existing.modified_at if existing else "",
            is_deleted=existing.is_deleted if existing else False,
            metadata_uploaded=existing.metadata_uploaded if existing else False,
            html_uploaded=existing.html_uploaded if existing else False,
            attachments_uploaded=existing.attachments_uploaded if existing else False,
            last_success_at=existing.last_success_at if existing else "",
            last_failure_at=timestamp,
            last_error=error,
            last_checked_at=timestamp,
        )


def default_state_file() -> Path:
    return Path.home() / "Library" / "Application Support" / "personal-data-warehouse" / "apple-notes-upload-state.json"
