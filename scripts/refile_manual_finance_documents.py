"""Re-file already-uploaded manual-finance documents into account folders.

Some documents were uploaded as bare files with no account folder, so their
``original_path`` is just a filename. ``document_account_key`` has nothing to
key on but the institution, which is a counterparty rather than an account, so
the ledger withholds them (see AGENTS.md, "A document about an ENTITY is never
one of Zach's accounts"). The guard is right; the fix is to give the documents
the folder they should have had.

**This needs no local copy of the files.** A document upload is two posts and
only the second one carries the path: the blob is deduped by content sha and is
already in Drive, and ``provenance_dedup_sha256`` deliberately EXCLUDES
``original_path`` so re-posting the envelope updates the stored row's path hint
instead of creating a second document. Everything the envelope needs --
account, filename, mime type, size, content sha -- is already in
``base_manual_finance.documents``, so this reads the warehouse and re-posts.

Read-only by default. It prints the plan and exits; ``--apply`` is what writes,
and only ``--apply`` contacts the app at all.

    # see the plan
    uv run python scripts/refile_manual_finance_documents.py --map refile.json

    # then, once the plan reads right
    uv run python scripts/refile_manual_finance_documents.py --map refile.json --apply

``--map`` is a JSON object of ``{"<content_sha256 or filename substring>": "<folder>"}``.
A folder must be a single path segment; the uploader's convention is
``<institution>-<name>-<mask>``, and a mask in the folder name is one of the two
things that lets a mask become account identity (see `mask_is_corroborated`).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from typing import Any

from personal_data_warehouse_manual_finance.envelope import (
    build_document_metadata,
    provenance_dedup_sha256,
)


def pdw_json(intent: str, sql: str) -> list[dict[str, Any]]:
    out = subprocess.run(
        ["pdw", "sql", "--output", "json", "-q", intent, sql],
        capture_output=True, text=True, check=True,
    ).stdout
    return json.loads(out or "[]")


def unfoldered_documents() -> list[dict[str, Any]]:
    return pdw_json(
        "manual finance documents with no account folder",
        """
        SELECT account, content_sha256, filename, original_path, mime_type,
               size_bytes, file_modified_at
        FROM base_manual_finance.documents
        WHERE is_deleted = 0 AND original_path NOT LIKE '%/%'
        ORDER BY filename
        """,
    )


def resolve_folder(doc: dict[str, Any], mapping: dict[str, str]) -> str | None:
    sha = str(doc["content_sha256"])
    if sha in mapping:
        return mapping[sha]
    for needle, folder in mapping.items():
        if len(needle) != 64 and needle.lower() in str(doc["filename"]).lower():
            return folder
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--map", required=True, help="JSON file: {sha or filename substring: folder}")
    parser.add_argument("--apply", action="store_true", help="actually re-post the envelopes")
    args = parser.parse_args()

    mapping = json.loads(open(args.map).read())
    for folder in mapping.values():
        if "/" in folder or not folder.strip():
            raise SystemExit(f"folder must be one path segment, got {folder!r}")

    documents = unfoldered_documents()
    planned: list[tuple[dict[str, Any], str]] = []
    for doc in documents:
        folder = resolve_folder(doc, mapping)
        if folder is None:
            print(f"  UNMAPPED  {doc['filename']}")
            continue
        planned.append((doc, folder))
        print(f"  {doc['original_path']}\n      -> {folder}/{doc['filename']}")

    print(f"\n{len(planned)} of {len(documents)} unfoldered documents mapped.")
    if not args.apply:
        print("Dry run: nothing was sent. Re-run with --apply to write.")
        return 0

    # Imported here so a dry run needs no app credentials at all.
    from personal_data_warehouse.config import load_settings
    from personal_data_warehouse.ingest_client import IngestClient

    settings = load_settings(require_gmail=False, require_manual_finance=True)
    client = IngestClient.from_settings(settings)
    now = datetime.now(tz=UTC).isoformat()
    for doc, folder in planned:
        account = str(doc["account"])
        sha = str(doc["content_sha256"])
        payload = build_document_metadata(
            account=account,
            filename=str(doc["filename"]),
            original_path=f"{folder}/{doc['filename']}",
            mime_type=str(doc["mime_type"]),
            size_bytes=int(doc["size_bytes"]),
            content_sha256=sha,
            uploaded_at=now,
            file_modified_at=str(doc["file_modified_at"] or ""),
        )
        client.upload_manual_finance_metadata(
            payload,
            modified_at=str(doc["file_modified_at"] or now),
            account_folder=folder,
            file_content_sha256=sha,
            metadata_dedup_sha256=provenance_dedup_sha256(
                source="manual", account=account, native_id=sha, file_content_sha256=sha
            ),
        )
        print(f"  re-filed {doc['filename']} -> {folder}/")
    print("\nDone. The next manual_finance_drive_ingest promotes the updated rows;")
    print("the next finance_ledger run then groups them by their new folder.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
