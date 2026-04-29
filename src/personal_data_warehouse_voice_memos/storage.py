from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Protocol, TypedDict


class StoredObject(TypedDict):
    storage_backend: str
    storage_key: str
    storage_file_id: str
    storage_url: str


class ObjectStore(Protocol):
    backend: str

    def has_blob(self, *, content_sha256: str) -> bool:
        pass

    def has_metadata(self, *, content_sha256: str) -> bool:
        pass

    def put_file(
        self,
        *,
        path: Path,
        object_key: str,
        content_sha256: str,
        content_type: str,
    ) -> StoredObject:
        pass

    def put_json(
        self,
        *,
        object_key: str,
        payload: dict[str, object],
        content_sha256: str,
        source_content_sha256: str | None = None,
    ) -> StoredObject:
        pass


def stored_object_from_mapping(value: Mapping[str, object]) -> StoredObject:
    return {
        "storage_backend": str(value.get("storage_backend", "")),
        "storage_key": str(value.get("storage_key", "")),
        "storage_file_id": str(value.get("storage_file_id", "")),
        "storage_url": str(value.get("storage_url", "")),
    }
