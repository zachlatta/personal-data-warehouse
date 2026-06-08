from __future__ import annotations

import pytest

from personal_data_warehouse.objectstore import (
    GOOGLE_DRIVE_BACKEND,
    S3_BACKEND,
    SUPPORTED_BACKENDS,
    ObjectStoreError,
    ObjectStoreSpec,
    S3Connection,
    build_object_store,
    google_drive_spec,
)
from personal_data_warehouse.objectstore import google_drive as gd
from personal_data_warehouse.objectstore.google_drive import GoogleDriveObjectStore
from personal_data_warehouse.objectstore.s3 import S3ObjectStore


def test_google_drive_spec_shape() -> None:
    spec = google_drive_spec(
        folder_id="folder-1",
        account="drive@example.com",
        source="apple_notes",
        blob_kind="apple_note_body_html",
        metadata_kind="apple_note_revision_metadata",
        legacy_sources=("legacy",),
        request_timeout_seconds=45,
    )

    assert spec.backend == GOOGLE_DRIVE_BACKEND
    assert spec.namespace == "folder-1"
    assert spec.google_drive is not None
    assert spec.google_drive.account == "drive@example.com"
    assert spec.google_drive.request_timeout_seconds == 45
    assert spec.s3 is None


def test_build_google_drive_with_injected_service_maps_spec() -> None:
    spec = google_drive_spec(
        folder_id="folder-1",
        account="drive@example.com",
        source="apple_notes",
        blob_kind="apple_note_body_html",
        metadata_kind="apple_note_revision_metadata",
        legacy_sources=("legacy",),
    )
    sentinel_service = object()

    store = build_object_store(spec, service=sentinel_service)

    assert isinstance(store, GoogleDriveObjectStore)
    assert store.backend == GOOGLE_DRIVE_BACKEND
    assert store._service is sentinel_service
    assert store._folder_id == "folder-1"
    assert store._source == "apple_notes"
    assert store._audio_kind == "apple_note_body_html"
    assert store._metadata_kind == "apple_note_revision_metadata"
    assert store._legacy_sources == ("legacy",)


def test_build_google_drive_builds_service_from_settings(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_build_service(*, account, settings, request_timeout_seconds):
        captured["account"] = account
        captured["settings"] = settings
        captured["timeout"] = request_timeout_seconds
        return object()

    monkeypatch.setattr(gd, "build_google_drive_service", fake_build_service)

    spec = google_drive_spec(
        folder_id="folder-1",
        account="drive@example.com",
        source="src",
        blob_kind="bk",
        metadata_kind="mk",
        request_timeout_seconds=22,
    )
    settings = object()

    store = build_object_store(spec, settings=settings)

    assert isinstance(store, GoogleDriveObjectStore)
    assert captured == {"account": "drive@example.com", "settings": settings, "timeout": 22}


def test_build_google_drive_requires_settings_when_no_service() -> None:
    spec = google_drive_spec(
        folder_id="folder-1",
        account="drive@example.com",
        source="src",
        blob_kind="bk",
        metadata_kind="mk",
    )
    with pytest.raises(ObjectStoreError, match="settings"):
        build_object_store(spec)


def test_build_google_drive_requires_namespace() -> None:
    spec = ObjectStoreSpec(
        backend=GOOGLE_DRIVE_BACKEND,
        namespace="",
        source="src",
        blob_kind="bk",
        metadata_kind="mk",
    )
    with pytest.raises(ObjectStoreError, match="folder id"):
        build_object_store(spec, service=object())


def test_unknown_backend_raises() -> None:
    spec = ObjectStoreSpec(
        backend="bogus",
        namespace="x",
        source="s",
        blob_kind="b",
        metadata_kind="m",
    )
    with pytest.raises(ObjectStoreError, match="Unsupported object storage backend"):
        build_object_store(spec)


def test_supported_backends_constant() -> None:
    assert set(SUPPORTED_BACKENDS) == {GOOGLE_DRIVE_BACKEND, S3_BACKEND}


def test_build_s3_returns_store_that_defers_until_implemented() -> None:
    spec = ObjectStoreSpec(
        backend=S3_BACKEND,
        namespace="bucket/prefix",
        source="s",
        blob_kind="b",
        metadata_kind="m",
        s3=S3Connection(bucket="my-bucket", region="us-east-1"),
    )

    store = build_object_store(spec)

    assert isinstance(store, S3ObjectStore)
    assert store.connection.bucket == "my-bucket"
    with pytest.raises(ObjectStoreError, match="not implemented"):
        store.has_blob(content_sha256="abc")


def test_build_s3_requires_connection() -> None:
    spec = ObjectStoreSpec(
        backend=S3_BACKEND,
        namespace="bucket",
        source="s",
        blob_kind="b",
        metadata_kind="m",
    )
    with pytest.raises(ObjectStoreError, match="spec.s3 connection"):
        build_object_store(spec)


def test_s3_connection_requires_bucket() -> None:
    with pytest.raises(ObjectStoreError, match="bucket"):
        S3ObjectStore(connection=S3Connection(bucket=""))
