"""The cross-source photo envelope contract (personal_data_warehouse_photos.envelope)."""

from __future__ import annotations

import hashlib

import pytest

from personal_data_warehouse_photos.envelope import (
    PHOTO_ROLES,
    SCHEMA_VERSION,
    build_photo_metadata,
    provenance_dedup_sha256,
)


def _envelope(**overrides):
    kwargs = dict(
        source="apple_photos",
        account="z@x.test",
        native_id="UUID-1",
        role="original",
        filename="IMG_0001.HEIC",
        mime_type="image/heic",
        size_bytes=123,
        content_sha256="sha-still",
        uploaded_at="2026-06-01T14:31:00+00:00",
        width=4284,
        height=5712,
        captured_at="2026-06-01T14:30:00",
        capture_tz_offset="-07:00",
        camera_make="Apple",
        camera_model="iPhone 16 Pro",
        record_key="apple_record",
        record={"uuid": "UUID-1", "kind": 0},
    )
    kwargs.update(overrides)
    return build_photo_metadata(**kwargs)


def test_provenance_sha_is_deterministic_and_distinguishes_sources():
    sha = provenance_dedup_sha256(
        source="apple_photos", account="z@x.test", native_id="UUID-1", role="original",
        file_content_sha256="filesha",
    )
    # Matches the Go handler test's seed construction exactly.
    assert sha == hashlib.sha256(b"apple_photos|z@x.test|UUID-1|original|filesha").hexdigest()
    other_source = provenance_dedup_sha256(
        source="google_photos", account="z@x.test", native_id="UUID-1", role="original",
        file_content_sha256="filesha",
    )
    # The same bytes claimed by a second source is a distinct provenance claim.
    assert sha != other_source


def test_envelope_shape_and_record_block():
    env = _envelope()
    assert env["schema_version"] == SCHEMA_VERSION
    assert env["source"] == "apple_photos"
    assert env["account"] == "z@x.test"
    assert env["file"]["native_id"] == "UUID-1"
    assert env["file"]["role"] == "original"
    assert env["file"]["content_sha256"] == "sha-still"
    assert env["file"]["width"] == 4284
    assert env["apple_record"] == {"uuid": "UUID-1", "kind": 0}


def test_live_video_role_is_allowed():
    env = _envelope(role="live_video", mime_type="video/quicktime", filename="IMG_0001_3.mov")
    assert env["file"]["role"] == "live_video"
    assert "live_video" in PHOTO_ROLES


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("source", "", "source is required"),
        ("account", "", "account is required"),
        ("native_id", "", "native_id is required"),
        ("role", "thumbnail", "role must be one of"),
        ("content_sha256", "", "content_sha256 is required"),
    ],
)
def test_envelope_fails_fast_on_contract_violations(field, value, message):
    with pytest.raises(ValueError, match=message):
        _envelope(**{field: value})


def test_record_requires_source_named_key():
    with pytest.raises(ValueError, match="record_key is required"):
        _envelope(record_key="")
    with pytest.raises(ValueError, match="source-named"):
        _envelope(record_key="record")
