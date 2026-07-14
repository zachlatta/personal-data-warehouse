"""The manual-finance document metadata envelope.

A document upload is always two posts (photos pattern):

1. the file blob -> ``POST /ingest/manual-finance/file`` (deduped by content
   sha), and
2. this JSON envelope -> ``POST /ingest/manual-finance/metadata`` (deduped by
   the PROVENANCE sha, so a re-upload of the same document claim — an envelope
   differing only in ``uploaded_at`` or ``original_path`` — never dups).

A document IS its bytes: the native id is the content sha. ``original_path``
is deliberately excluded from the dedup sha — moving a file between folders
updates the stored row's path hint instead of creating a second document.
"""

from __future__ import annotations

import hashlib
from typing import Any

SCHEMA_VERSION = 1

DOCUMENT_SOURCE = "manual"


def provenance_dedup_sha256(
    *,
    source: str,
    account: str,
    native_id: str,
    file_content_sha256: str,
) -> str:
    seed = "|".join((source, account, native_id, file_content_sha256))
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def build_document_metadata(
    *,
    account: str,
    filename: str,
    original_path: str,
    mime_type: str,
    size_bytes: int,
    content_sha256: str,
    uploaded_at: str,
    file_modified_at: str = "",
    source: str = DOCUMENT_SOURCE,
) -> dict[str, Any]:
    """Build the document metadata envelope. Fails fast on contract violations."""
    if not source:
        raise ValueError("source is required")
    if not account:
        raise ValueError("account is required")
    if not content_sha256:
        raise ValueError("content_sha256 is required")
    if not filename:
        raise ValueError("filename is required")
    return {
        "schema_version": SCHEMA_VERSION,
        "source": source,
        "account": account,
        "uploaded_at": uploaded_at,
        "file": {
            "native_id": content_sha256,
            "filename": filename,
            "original_path": original_path,
            "mime_type": mime_type,
            "size_bytes": int(size_bytes),
            "content_sha256": content_sha256,
            "file_modified_at": file_modified_at,
        },
    }
