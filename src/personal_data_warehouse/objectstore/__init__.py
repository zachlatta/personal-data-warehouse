"""Unified object storage abstraction for the personal data warehouse.

Application code stores and retrieves binary/blob objects through the
backend-neutral :class:`ObjectStore` interface, obtaining an instance from
:func:`build_object_store`. Google Drive is the initial backend; an
S3-compatible backend can be added behind the same interface without changing
call sites.
"""

from __future__ import annotations

from personal_data_warehouse.objectstore.base import (
    ObjectListing,
    ObjectMetadata,
    ObjectNotFoundError,
    ObjectPresence,
    ObjectStore,
    ObjectStoreError,
    StoredObject,
    stored_object_from_mapping,
)
from personal_data_warehouse.objectstore.factory import build_object_store
from personal_data_warehouse.objectstore.spec import (
    GOOGLE_DRIVE_BACKEND,
    S3_BACKEND,
    SUPPORTED_BACKENDS,
    GoogleDriveConnection,
    ObjectStoreSpec,
    S3Connection,
    google_drive_spec,
)

__all__ = [
    "GOOGLE_DRIVE_BACKEND",
    "GoogleDriveConnection",
    "ObjectListing",
    "ObjectMetadata",
    "ObjectNotFoundError",
    "ObjectPresence",
    "ObjectStore",
    "ObjectStoreError",
    "ObjectStoreSpec",
    "S3_BACKEND",
    "S3Connection",
    "StoredObject",
    "SUPPORTED_BACKENDS",
    "build_object_store",
    "google_drive_spec",
    "stored_object_from_mapping",
]
