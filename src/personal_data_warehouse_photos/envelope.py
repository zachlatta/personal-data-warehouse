"""The cross-source photo metadata envelope.

This is THE contract between every photo uploader (Apple Photos now,
Google Photos / manual imports later) and the warehouse. A photo upload is
always two posts:

1. the file blob -> ``POST /ingest/photos/file`` (deduped by content sha), and
2. this JSON envelope -> ``POST /ingest/photos/metadata`` (deduped by the
   PROVENANCE sha, so the same bytes arriving from two sources keep both
   envelopes).

The envelope carries the ``source`` slug that the Dagster reader routes on
(``PHOTO_SOURCE_RELATIONS``), the normalized per-file facts every source must
provide (``file``), and the source's untouched native record under a
source-named key (``apple_record`` for Apple Photos; a future Takeout importer
adds e.g. ``takeout_sidecar``). Keep normalized facts BOTH places: the ``file``
block is what turns into raw table columns, the record block is the archival
raw payload (``raw_metadata_json``) that lets canonicalization be re-run
forever.

Adding a photo source clientside is: call :func:`build_photo_metadata` with
your ``source`` slug + ``record_key``/``record``, upload via
``IngestClient.upload_photo_file`` / ``upload_photo_metadata``, and pass
:func:`provenance_dedup_sha256` as the metadata dedup key. Nothing else.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

SCHEMA_VERSION = 1

# The only roles the identity layer understands. `original` is the shot as
# captured; `edited` is a source-rendered adjustment of the same photo;
# `live_video` is a Live Photo's motion component (uploaded under the SAME
# native id as its still, which is what attaches it to the still's asset).
PHOTO_ROLES = ("original", "edited", "live_video")


def provenance_dedup_sha256(
    *,
    source: str,
    account: str,
    native_id: str,
    role: str,
    file_content_sha256: str,
) -> str:
    """Stable dedup key for one (source, account, native_id, role, file) claim.

    Deliberately NOT the file sha: two sources holding identical bytes are two
    provenance claims and both envelopes must survive in the inbox, while a
    re-run of one source (same claim, envelope differing only in uploaded_at)
    must dedup away. Mirrors the Go handler's expectations in
    app/internal/server/ingest.go.
    """
    seed = "|".join((source, account, native_id, role, file_content_sha256))
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def build_photo_metadata(
    *,
    source: str,
    account: str,
    native_id: str,
    role: str,
    filename: str,
    mime_type: str,
    size_bytes: int,
    content_sha256: str,
    uploaded_at: str,
    width: int = 0,
    height: int = 0,
    captured_at: str = "",
    capture_tz_offset: str = "",
    camera_make: str = "",
    camera_model: str = "",
    file_modified_at: str = "",
    record_key: str = "",
    record: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the photo metadata envelope. Fails fast on contract violations."""
    if not source:
        raise ValueError("source is required")
    if not account:
        raise ValueError("account is required")
    if not native_id:
        raise ValueError("native_id is required")
    if role not in PHOTO_ROLES:
        raise ValueError(f"role must be one of {PHOTO_ROLES}, got {role!r}")
    if not content_sha256:
        raise ValueError("content_sha256 is required")
    if record is not None and not record_key:
        raise ValueError("record_key is required when a record is provided")
    if record_key and not record_key.endswith(("_record", "_sidecar", "_exif")):
        raise ValueError(
            "record_key must be source-named (e.g. 'apple_record', 'takeout_sidecar'), "
            f"got {record_key!r}"
        )
    envelope: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "source": source,
        "account": account,
        "uploaded_at": uploaded_at,
        "file": {
            "native_id": native_id,
            "role": role,
            "filename": filename,
            "mime_type": mime_type,
            "size_bytes": int(size_bytes),
            "content_sha256": content_sha256,
            "width": int(width),
            "height": int(height),
            "captured_at": captured_at,
            "capture_tz_offset": capture_tz_offset,
            "camera_make": camera_make,
            "camera_model": camera_model,
            "file_modified_at": file_modified_at,
        },
    }
    if record is not None:
        envelope[record_key] = dict(record)
    return envelope
