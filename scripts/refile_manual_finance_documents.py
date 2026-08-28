"""Re-file already-uploaded manual-finance documents into account folders.

Some documents were uploaded as bare files with no account folder, so their
``original_path`` is just a filename. ``document_account_key`` has nothing to
key on but the institution, which is a counterparty rather than an account, so
the ledger withholds them (see AGENTS.md, "A document about an ENTITY is never
one of Zach's accounts"). The guard is right; the fix is to give the documents
the folder they should have had.

**This needs no local copy of the files.** A document upload is two posts and
only the second one carries the path: the blob is deduped by content sha and is
already in Drive. Everything the envelope needs -- account, filename, mime
type, size, content sha -- is already in ``base_manual_finance.documents``, so
this reads the warehouse and re-posts only the metadata.

**The metadata dedup sha must vary with the destination folder, and that is
not what ``provenance_dedup_sha256`` gives you.** The app dedups a metadata
envelope by the ``metadata_dedup_sha256`` it is handed, looking for an existing
object carrying it and returning that object WITHOUT writing (see
``PutJSON``/``findByAppProperty``). ``provenance_dedup_sha256`` excludes
``original_path`` by design, so a plain re-post is byte-for-byte the same
claim, gets deduped away, and writes nothing at all -- 19 re-posts produced
zero inbox objects and the sensor kept reporting an empty inbox. Excluding the
path prevents a DUPLICATE; it does not perform an UPDATE. So a re-file derives
its own dedup sha from the provenance sha plus the destination folder: a new
inbox object is written, the reader upserts it onto the same document row
(the PK is source/account/native_id/content_sha256, none of which move), and
``original_path`` changes. Re-running the same map is still a no-op, because
the same folder yields the same sha.

Read-only by default. It prints the plan and exits; ``--apply`` is what writes,
and only ``--apply`` contacts the app at all.

    # see the plan
    uv run python scripts/refile_manual_finance_documents.py --map refile.json

    # then, once the plan reads right. --apply posts to the app, so it needs
    # PDW_API_URL + PDW_SECRET_TOKEN: this runs OUTSIDE the pdw CLI, so it
    # inherits nothing from `pdw login` on its own. Source the shared resolver
    # rather than hand-rolling the config read (see bin/_pdw-upload-lib.sh):
    . bin/_pdw-upload-lib.sh && pdw_export_app_credentials
    uv run python scripts/refile_manual_finance_documents.py --map refile.json --apply

``--map`` is a JSON object of ``{"<content_sha256 or filename substring>": "<folder>"}``.
A folder must be a single path segment; the uploader's convention is
``<institution>-<name>-<mask>``, and a mask in the folder name is one of the two
things that lets a mask become account identity (see `mask_is_corroborated`).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import UTC, datetime
from typing import Any

from personal_data_warehouse_manual_finance.envelope import (
    build_document_metadata,
    provenance_dedup_sha256,
)


def refile_dedup_sha256(*, account: str, content_sha256: str, folder: str) -> str:
    """Metadata dedup sha for a re-file, stable per (document, destination).

    Folding the folder in is what makes the app write a new inbox object at
    all; see the module docstring. Deriving it FROM the provenance sha rather
    than replacing it keeps a re-file distinguishable from an ordinary upload
    of the same document, so the original envelope is never displaced.
    """
    seed = provenance_dedup_sha256(
        source="manual", account=account, native_id=content_sha256,
        file_content_sha256=content_sha256,
    )
    return hashlib.sha256(f"{seed}|refile|{folder}".encode("utf-8")).hexdigest()


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
    # `ingest_client_from_env` is the shared builder every uploader uses --
    # PDW_API_URL + PDW_SECRET_TOKEN, with the Tailscale-direct route resolved
    # for it. This posts a small JSON envelope, so the body cap never applies.
    from personal_data_warehouse.ingest_client import ingest_client_from_env

    client = ingest_client_from_env()
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
            metadata_dedup_sha256=refile_dedup_sha256(
                account=account, content_sha256=sha, folder=folder
            ),
        )
        print(f"  re-filed {doc['filename']} -> {folder}/")
    print("\nDone. The next manual_finance_drive_ingest promotes the updated rows;")
    print("the next finance_ledger run then groups them by their new folder.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
