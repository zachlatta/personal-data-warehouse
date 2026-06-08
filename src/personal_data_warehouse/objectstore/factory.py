"""Backend selection for the object storage abstraction.

Application call sites build an :class:`ObjectStoreSpec` and call
:func:`build_object_store`; they never import a concrete backend. Adding a
backend means registering one builder in ``_BUILDERS`` -- no call site changes.
"""

from __future__ import annotations

from collections.abc import Callable

from personal_data_warehouse.objectstore.base import ObjectStore, ObjectStoreError
from personal_data_warehouse.objectstore.spec import (
    GOOGLE_DRIVE_BACKEND,
    S3_BACKEND,
    SUPPORTED_BACKENDS,
    ObjectStoreSpec,
)


def build_object_store(spec: ObjectStoreSpec, *, settings=None, service=None) -> ObjectStore:
    """Construct the object store described by ``spec``.

    ``settings`` is required for backends that build their own client from
    credentials (Google Drive). ``service`` lets callers (and tests) inject a
    pre-built backend client instead of constructing one.
    """

    builder = _BUILDERS.get(spec.backend)
    if builder is None:
        supported = ", ".join(SUPPORTED_BACKENDS)
        raise ObjectStoreError(
            f"Unsupported object storage backend: {spec.backend!r} (supported: {supported})"
        )
    return builder(spec, settings, service)


def _build_google_drive(spec: ObjectStoreSpec, settings, service) -> ObjectStore:
    # Imported lazily so that importing the factory does not require the Google
    # client libraries unless the Drive backend is actually used.
    from personal_data_warehouse.objectstore.google_drive import (
        GoogleDriveObjectStore,
        build_google_drive_service,
    )

    if not spec.namespace:
        raise ObjectStoreError("Google Drive object store requires a folder id (spec.namespace)")
    if service is None:
        connection = spec.google_drive
        if connection is None:
            raise ObjectStoreError("Google Drive object store requires spec.google_drive connection")
        if settings is None:
            raise ObjectStoreError("Google Drive object store requires settings to build a service")
        service = build_google_drive_service(
            account=connection.account,
            settings=settings,
            request_timeout_seconds=connection.request_timeout_seconds,
        )
    return GoogleDriveObjectStore(
        folder_id=spec.namespace,
        service=service,
        source=spec.source,
        legacy_sources=spec.legacy_sources,
        audio_kind=spec.blob_kind,
        metadata_kind=spec.metadata_kind,
    )


def _build_s3(spec: ObjectStoreSpec, settings, service) -> ObjectStore:
    from personal_data_warehouse.objectstore.s3 import S3ObjectStore

    if service is not None:
        return service
    if spec.s3 is None:
        raise ObjectStoreError("S3 object store requires spec.s3 connection")
    return S3ObjectStore(connection=spec.s3)


_BUILDERS: dict[str, Callable[[ObjectStoreSpec, object, object], ObjectStore]] = {
    GOOGLE_DRIVE_BACKEND: _build_google_drive,
    S3_BACKEND: _build_s3,
}
