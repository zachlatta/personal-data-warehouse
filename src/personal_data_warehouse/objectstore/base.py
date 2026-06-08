"""Backend-neutral object storage interface.

This module defines the abstraction that the rest of the application uses to
store and retrieve binary/blob-like objects (Voice Memo audio, Apple Notes
attachments, Apple Messages exports, Gmail attachments, Alice recordings, ...).

The concrete backend (Google Drive today, S3-compatible later) lives behind the
:class:`ObjectStore` protocol and is selected through
:func:`personal_data_warehouse.objectstore.factory.build_object_store`.

Nothing in this module imports a backend SDK: application code depends only on
the neutral interface and the neutral :class:`StoredObject` reference, which is
what gets persisted in the warehouse.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TypedDict, runtime_checkable


class ObjectStoreError(RuntimeError):
    """Base error for object storage backends."""


class ObjectNotFoundError(ObjectStoreError):
    """Raised when an object referenced by a :class:`StoredObject` is missing."""


@dataclass(frozen=True)
class ObjectPresence:
    """Result of a content-addressed dedup probe.

    ``audio_exists``/``metadata_exists`` are historical field names; they map to
    "is the primary blob already stored" and "is the JSON sidecar already
    stored" respectively for any object kind, not just audio.
    """

    audio_exists: bool
    metadata_exists: bool


@dataclass(frozen=True)
class ObjectMetadata:
    """Backend-neutral metadata for a stored object."""

    backend: str
    storage_key: str
    storage_file_id: str
    content_type: str
    size_bytes: int | None = None
    content_sha256: str | None = None
    filename: str | None = None
    created_time: str | None = None
    modified_time: str | None = None
    storage_url: str = ""


@dataclass(frozen=True)
class ObjectListing:
    """A single object returned by a listing/find query.

    ``ref`` is the backend-neutral reference; ``app_properties`` are the
    ``pdw_*`` tags we attach to every object (these are our own metadata, stored
    by whatever backend, not a Drive-specific concept). ``filename`` is the
    object's display name where the backend tracks one.
    """

    ref: StoredObject
    app_properties: Mapping[str, str]
    filename: str = ""


class StoredObject(TypedDict):
    """Durable, backend-neutral reference to a stored object.

    This is what gets persisted in the warehouse (the ``storage_*`` columns).
    Its shape is intentionally stable across backends:

    - ``storage_backend``: backend type that owns the object (e.g. ``google_drive``).
    - ``storage_key``: logical object key/path within the backend namespace.
    - ``storage_file_id``: backend provider id (Drive file id, S3 version id, ...).
      For S3 this may simply repeat ``storage_key``.
    - ``storage_url``: a link to the object if the backend exposes one, else "".
    """

    storage_backend: str
    storage_key: str
    storage_file_id: str
    storage_url: str


@runtime_checkable
class ObjectStore(Protocol):
    """Pluggable object storage backend.

    Implementations must preserve the content-addressed dedup semantics used by
    the upload pipelines (``has_blob``/``has_metadata``/``presence``) and the
    write operations (``put_file``/``put_json``). Read/management operations
    (``get_object``/``object_exists``/``delete_object``/``get_metadata``/
    ``get_share_url``) operate on a previously returned :class:`StoredObject`.
    """

    backend: str

    # --- content-addressed dedup probes -------------------------------------
    def has_blob(self, *, content_sha256: str) -> bool:
        ...

    def has_metadata(self, *, content_sha256: str) -> bool:
        ...

    def has_object(self, *, kind: str, key: str, value: str) -> bool:
        ...

    def presence(self, *, content_sha256: str) -> ObjectPresence:
        ...

    # --- writes -------------------------------------------------------------
    def put_file(
        self,
        *,
        path: Path,
        object_key: str,
        content_sha256: str,
        content_type: str,
        skip_existing_check: bool = False,
        app_properties: dict[str, str] | None = None,
        kind: str | None = None,
    ) -> StoredObject:
        ...

    def put_json(
        self,
        *,
        object_key: str,
        payload: dict[str, object],
        content_sha256: str,
        source_content_sha256: str | None = None,
        skip_existing_check: bool = False,
        app_properties: dict[str, str] | None = None,
        kind: str | None = None,
    ) -> StoredObject:
        ...

    # --- reads / management -------------------------------------------------
    def get_object(self, ref: Mapping[str, object]) -> bytes:
        """Return the full object contents as bytes."""
        ...

    def download_to_path(self, ref: Mapping[str, object], path: Path) -> None:
        """Stream the object contents to ``path``."""
        ...

    def object_exists(self, ref: Mapping[str, object]) -> bool:
        """Return whether the referenced object still exists in the backend."""
        ...

    def delete_object(self, ref: Mapping[str, object]) -> None:
        """Delete the referenced object. Idempotent: missing objects are a no-op."""
        ...

    def get_metadata(self, ref: Mapping[str, object]) -> ObjectMetadata:
        """Return backend-neutral metadata for the referenced object."""
        ...

    # --- discovery / relocation --------------------------------------------
    def list_objects(
        self,
        *,
        kind: str,
        stage: str | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> list[ObjectListing]:
        """List stored objects of ``kind`` (optionally filtered by stage/tags)."""
        ...

    def find_object(
        self,
        *,
        kind: str,
        stage: str | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> ObjectListing | None:
        """Return the first matching object, or ``None``."""
        ...

    def move_object(
        self,
        ref: Mapping[str, object],
        *,
        new_object_key: str,
        app_properties: Mapping[str, str] | None = None,
    ) -> StoredObject:
        """Relocate an object to ``new_object_key`` (e.g. inbox -> library).

        Implementations update the object's stage tag to match the new key.
        """
        ...

    def get_share_url(self, ref: Mapping[str, object]) -> str | None:
        """Return a link to the object, or ``None`` if the backend exposes none.

        Implementations must not change object visibility (e.g. must not make a
        private object public) as a side effect of this call.
        """
        ...


def stored_object_from_mapping(value: Mapping[str, object]) -> StoredObject:
    return {
        "storage_backend": str(value.get("storage_backend", "")),
        "storage_key": str(value.get("storage_key", "")),
        "storage_file_id": str(value.get("storage_file_id", "")),
        "storage_url": str(value.get("storage_url", "")),
    }
