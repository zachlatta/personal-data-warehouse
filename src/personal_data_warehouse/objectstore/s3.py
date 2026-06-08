"""S3-compatible backend seam.

This module establishes the contract for an S3-compatible
:class:`~personal_data_warehouse.objectstore.base.ObjectStore` so the backend
can be added later without touching application call sites. It is intentionally
not yet implemented (see the project follow-ups); selecting the ``s3`` backend
raises a clear error rather than silently misbehaving.

Implementation notes for whoever fills this in:

- Use ``boto3`` configured from :class:`S3Connection` (endpoint_url, region,
  bucket, credentials, addressing_style, key_prefix). Add ``boto3`` to
  ``pyproject.toml`` dependencies at that point.
- Content-addressed dedup (``has_blob``/``has_metadata``/``has_object``/
  ``presence``) has no server-side metadata search on S3. Implement it with
  deterministic, content-addressed object keys (the ``content_sha256`` is
  already embedded in every ``object_key``) plus ``head_object`` existence
  checks, or by maintaining a small manifest object per namespace.
- ``put_file``/``put_json`` should set ``ContentType`` and store the neutral
  tags (``pdw_source``, ``pdw_kind``, ``content_sha256``, ...) as object
  metadata, returning a :class:`StoredObject` whose ``storage_file_id`` is the
  S3 key (or version id) and ``storage_url`` is "" (private) unless a CDN base
  is configured.
- ``get_share_url`` should return a presigned URL; it must not make objects
  public.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from personal_data_warehouse.objectstore.base import (
    ObjectListing,
    ObjectMetadata,
    ObjectPresence,
    ObjectStoreError,
    StoredObject,
)
from personal_data_warehouse.objectstore.spec import S3Connection

_NOT_IMPLEMENTED = (
    "The S3-compatible object storage backend is not implemented yet. "
    "See personal_data_warehouse/objectstore/s3.py for the contract, add boto3, "
    "and implement S3ObjectStore."
)


class S3ObjectStore:
    """Placeholder S3-compatible backend.

    Holds validated connection config and exposes the full
    :class:`ObjectStore` surface, but every operation raises
    :class:`ObjectStoreError` until implemented. This keeps the factory/registry
    and config plumbing honest and exercisable while deferring the actual
    implementation.
    """

    backend = "s3"

    def __init__(self, *, connection: S3Connection) -> None:
        if not connection.bucket:
            raise ObjectStoreError("S3Connection.bucket is required for the s3 backend")
        self._connection = connection

    @property
    def connection(self) -> S3Connection:
        return self._connection

    def has_blob(self, *, content_sha256: str) -> bool:
        raise ObjectStoreError(_NOT_IMPLEMENTED)

    def has_metadata(self, *, content_sha256: str) -> bool:
        raise ObjectStoreError(_NOT_IMPLEMENTED)

    def has_object(self, *, kind: str, key: str, value: str) -> bool:
        raise ObjectStoreError(_NOT_IMPLEMENTED)

    def presence(self, *, content_sha256: str) -> ObjectPresence:
        raise ObjectStoreError(_NOT_IMPLEMENTED)

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
        raise ObjectStoreError(_NOT_IMPLEMENTED)

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
        raise ObjectStoreError(_NOT_IMPLEMENTED)

    def get_object(self, ref: Mapping[str, object]) -> bytes:
        raise ObjectStoreError(_NOT_IMPLEMENTED)

    def download_to_path(self, ref: Mapping[str, object], path: Path) -> None:
        raise ObjectStoreError(_NOT_IMPLEMENTED)

    def object_exists(self, ref: Mapping[str, object]) -> bool:
        raise ObjectStoreError(_NOT_IMPLEMENTED)

    def delete_object(self, ref: Mapping[str, object]) -> None:
        raise ObjectStoreError(_NOT_IMPLEMENTED)

    def get_metadata(self, ref: Mapping[str, object]) -> ObjectMetadata:
        raise ObjectStoreError(_NOT_IMPLEMENTED)

    def list_objects(
        self,
        *,
        kind: str,
        stage: str | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> list[ObjectListing]:
        raise ObjectStoreError(_NOT_IMPLEMENTED)

    def find_object(
        self,
        *,
        kind: str,
        stage: str | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> ObjectListing | None:
        raise ObjectStoreError(_NOT_IMPLEMENTED)

    def move_object(
        self,
        ref: Mapping[str, object],
        *,
        new_object_key: str,
        app_properties: Mapping[str, str] | None = None,
    ) -> StoredObject:
        raise ObjectStoreError(_NOT_IMPLEMENTED)

    def get_share_url(self, ref: Mapping[str, object]) -> str | None:
        raise ObjectStoreError(_NOT_IMPLEMENTED)
