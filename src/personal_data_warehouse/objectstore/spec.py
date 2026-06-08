"""Construction config for object storage backends.

:class:`ObjectStoreSpec` is the backend-neutral description that application
call sites build and hand to
:func:`personal_data_warehouse.objectstore.factory.build_object_store`. It
carries the logical namespace and object tagging that every backend needs, plus
a single backend-specific connection object.

Adding a new backend means adding a connection dataclass here and a branch in
the factory -- no application call site changes.
"""

from __future__ import annotations

from dataclasses import dataclass

GOOGLE_DRIVE_BACKEND = "google_drive"
S3_BACKEND = "s3"
SUPPORTED_BACKENDS = (GOOGLE_DRIVE_BACKEND, S3_BACKEND)


@dataclass(frozen=True)
class GoogleDriveConnection:
    """Connection knobs for the Google Drive backend.

    The Drive ``service`` is built from these plus the loaded ``Settings`` (for
    credentials) inside the factory, so call sites never touch the Drive SDK.
    """

    account: str
    request_timeout_seconds: int = 30


@dataclass(frozen=True)
class S3Connection:
    """Connection knobs for a future S3-compatible backend.

    These map directly onto boto3 client configuration so the backend can target
    AWS S3 or any S3-compatible service (R2, MinIO, Backblaze B2, ...):

    - ``endpoint_url``: override for S3-compatible services; ``None`` for AWS.
    - ``region``: AWS region / region hint.
    - ``bucket``: target bucket.
    - ``access_key``/``secret_key``: credentials (omit to use the boto3 chain).
    - ``addressing_style``: ``"virtual"`` or ``"path"`` host addressing.
    - ``key_prefix``: prefix prepended to every object key.
    """

    bucket: str
    region: str = ""
    endpoint_url: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    addressing_style: str = "virtual"
    key_prefix: str = ""


@dataclass(frozen=True)
class ObjectStoreSpec:
    """Backend-neutral description of where/how to store a family of objects."""

    backend: str
    namespace: str
    source: str
    blob_kind: str
    metadata_kind: str
    legacy_sources: tuple[str, ...] = ()
    google_drive: GoogleDriveConnection | None = None
    s3: S3Connection | None = None


def google_drive_spec(
    *,
    folder_id: str,
    account: str,
    source: str,
    blob_kind: str,
    metadata_kind: str,
    legacy_sources: tuple[str, ...] = (),
    request_timeout_seconds: int = 30,
) -> ObjectStoreSpec:
    """Build a Google Drive :class:`ObjectStoreSpec`."""

    return ObjectStoreSpec(
        backend=GOOGLE_DRIVE_BACKEND,
        namespace=folder_id,
        source=source,
        blob_kind=blob_kind,
        metadata_kind=metadata_kind,
        legacy_sources=legacy_sources,
        google_drive=GoogleDriveConnection(
            account=account,
            request_timeout_seconds=request_timeout_seconds,
        ),
    )
