#!/usr/bin/env python
"""End-to-end check of "who sent this image?" against real Slack data.

Hits the real Slack API and the real warehouse, so run it deliberately.

It proves the whole chain without writing to any production relation:

1. read the real ``base_slack.files`` row, its uploader and its conversation
   from the warehouse,
2. fetch the file's real bytes through the app's existing get_object tool,
3. fingerprint them with the production code path,
4. seed all of that into a throwaway ``pdw_test_*`` schema,
5. run the real ranking SQL there with a probe image and report what an agent
   would have been told.

With no ``--probe``, it derives probes by re-encoding the real bytes (the same
class of transformation that made Slack's copy and the pasted copy differ in
size). Pass ``--probe`` to use an actual second copy of the picture.

    uv run python scripts/verify_slack_image_lookup.py --file-id F0EXAMPLE123
    uv run python scripts/verify_slack_image_lookup.py --file-id F0EXAMPLE123 --probe ~/Desktop/poster.png
"""

from __future__ import annotations

import argparse
import hashlib
import io
import os
import sys
from datetime import UTC, datetime

from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from personal_data_warehouse.photo_fingerprint import HASH_VERSION, compute_dhash, hamming
from personal_data_warehouse.postgres import PostgresWarehouse
from personal_data_warehouse.relations import relation
from personal_data_warehouse.slack_file_fingerprints import (
    DEFAULT_MAX_PIXELS,
    NO_RETRY,
    STATUS_OK,
)
from personal_data_warehouse.slack_file_fingerprints import AppObjectFetcher, SlackFileRef
from personal_data_warehouse.slack_image_lookup import build_lookup_sql, format_matches, parse_matches
from tests.conftest import cleanup_test_warehouse, make_test_schema

NOW = datetime.now(tz=UTC)


def _app_credentials() -> tuple[str, str]:
    """Env first, then `pdw login`'s config -- the bin/*-launchd convention."""
    base_url = (os.environ.get("PDW_API_URL") or os.environ.get("MCP_BASE_URL") or "").strip()
    token = (os.environ.get("PDW_SECRET_TOKEN") or os.environ.get("MCP_SECRET_TOKEN") or "").strip()
    if base_url and token:
        return base_url, token
    config_path = os.path.expanduser("~/.config/pdw/config.json")
    try:
        import json as _json

        with open(config_path) as handle:
            config = _json.load(handle)
    except (OSError, ValueError):
        return base_url, token
    return base_url or str(config.get("base_url") or ""), token or str(config.get("token") or "")


def _fetch_source_rows(prod: PostgresWarehouse, file_id: str):
    files = prod._query_dicts(
        "SELECT * FROM @slack_files WHERE file_id = %s ORDER BY message_ts LIMIT 5", (file_id,)
    )
    if not files:
        raise SystemExit(f"no base_slack.files row for {file_id}")
    head = files[0]
    users = prod._query_dicts(
        "SELECT * FROM @slack_users WHERE account = %s AND team_id = %s AND user_id = %s",
        (head["account"], head["team_id"], head["user_id"]),
    )
    conversations = prod._query_dicts(
        "SELECT * FROM @slack_conversations WHERE account = %s AND team_id = %s AND conversation_id = %s",
        (head["account"], head["team_id"], head["conversation_id"]),
    )
    return files, users, conversations


def _reencoded_probes(content: bytes) -> list[tuple[str, bytes]]:
    from PIL import Image

    previous = Image.MAX_IMAGE_PIXELS
    Image.MAX_IMAGE_PIXELS = None
    try:
        image = Image.open(io.BytesIO(content))
        image.load()
        probes = []
        for label, scale, fmt, kwargs in (
            ("50% PNG", 0.5, "PNG", {}),
            ("25% JPEG q85", 0.25, "JPEG", {"quality": 85}),
            ("1200px JPEG q75", 1200 / image.width, "JPEG", {"quality": 75}),
        ):
            resized = image.resize(
                (max(1, int(image.width * scale)), max(1, int(image.height * scale))),
                Image.Resampling.LANCZOS,
            )
            if fmt == "JPEG":
                resized = resized.convert("RGB")
            buffer = io.BytesIO()
            resized.save(buffer, format=fmt, **kwargs)
            probes.append((label, buffer.getvalue()))
        return probes
    finally:
        Image.MAX_IMAGE_PIXELS = previous


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file-id", required=True, help="base_slack.files file_id to verify")
    parser.add_argument("--probe", default=None, help="a real second copy of the image")
    args = parser.parse_args()

    load_dotenv()
    postgres_url = os.environ.get("POSTGRES_DATABASE_URL")
    if not postgres_url:
        raise SystemExit("POSTGRES_DATABASE_URL must be set")

    prod = PostgresWarehouse(postgres_url)
    try:
        files, users, conversations = _fetch_source_rows(prod, args.file_id)
    finally:
        prod.close()

    head = files[0]
    print(f"warehouse row : {head['file_id']}  {head['name']}  {head['size']} bytes")
    print(f"                uploaded {head['created_at']} into {head['conversation_id']}")
    print(f"                uploader {head['user_id']} "
          f"({'resolved in base_slack.users' if users else 'NOT in base_slack.users'})")
    print(f"                shared into {len(files)} conversation row(s)")

    # Bytes come through the app's get_object tool, the same path the backfill
    # and any agent uses -- so this exercises the real fetch, not a copy of it.
    base_url, secret_token = _app_credentials()
    if not base_url or not secret_token:
        raise SystemExit("set PDW_API_URL + PDW_SECRET_TOKEN, or run `pdw login`")
    fetcher = AppObjectFetcher(base_url=base_url, secret_token=secret_token)
    content = fetcher.fetch(SlackFileRef.from_row(head))
    fingerprint = compute_dhash(content, max_pixels=DEFAULT_MAX_PIXELS)
    sha = hashlib.sha256(content).hexdigest()
    print(f"\nfetched       : {len(content)} bytes, {fingerprint.width}x{fingerprint.height} "
          f"({fingerprint.width * fingerprint.height / 1e6:.0f} MP)")
    print(f"                sha256 {sha}")
    print(f"                dhash  {fingerprint.dhash}")
    if len(content) != int(head["size"]):
        print(f"                WARNING: byte size differs from the stored row ({head['size']})")

    schema = make_test_schema("verify")
    scratch = PostgresWarehouse(postgres_url, schema=schema)
    try:
        scratch.ensure_slack_tables()
        scratch.ensure_slack_file_fingerprint_tables()
        scratch.insert_slack_files(files)
        if users:
            scratch.insert_slack_users(users)
        if conversations:
            scratch.insert_slack_conversations(conversations)
        scratch.insert_media_fingerprints([{
            "content_sha256": sha, "hash_version": HASH_VERSION, "dhash": fingerprint.dhash,
            "width": fingerprint.width, "height": fingerprint.height,
            "created_at": NOW, "sync_version": 1,
        }])
        scratch.upsert_slack_file_fingerprints([{
            "account": head["account"], "team_id": head["team_id"], "file_id": head["file_id"],
            "content_sha256": sha, "hash_version": HASH_VERSION, "status": STATUS_OK,
            "attempts": 1, "fetched_bytes": len(content), "last_error": "",
            "last_attempt_at": NOW, "next_attempt_at": NO_RETRY,
            "created_at": NOW, "updated_at": NOW, "sync_version": 1,
        }])

        if args.probe:
            probes = [(f"supplied copy ({args.probe})", open(args.probe, "rb").read())]
        else:
            print("\n(no --probe given; deriving probes by re-encoding the real bytes)")
            probes = _reencoded_probes(content)

        canonical = relation("slack_image_fingerprints")
        local = canonical.with_namespace(schema)
        failures = 0
        for label, data in probes:
            probe_fp = compute_dhash(data, max_pixels=DEFAULT_MAX_PIXELS)
            distance = hamming(fingerprint.dhash, probe_fp.dhash)
            same_sha = hashlib.sha256(data).hexdigest() == sha
            print(f"\n=== probe: {label} ===")
            print(f"    {len(data)} bytes, sha-identical to Slack's copy: {same_sha}, "
                  f"true distance {distance}/256")

            sql = build_lookup_sql(probe_fp.dhash).replace(
                f"{canonical.schema}.{canonical.name}", f"{local.schema}.{local.name}"
            )
            rows = scratch._query_dicts(sql)
            import json
            ndjson = "\n".join(
                json.dumps({k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in r.items()})
                for r in rows
            )
            matches = parse_matches(ndjson)
            print(format_matches(matches))
            top = matches[0] if matches else None
            if top is None or top.file_id != args.file_id:
                print(f"    FAIL: expected {args.file_id} at rank 1")
                failures += 1
            elif not top.uploader_user_id:
                print("    FAIL: uploader was not resolved")
                failures += 1
            else:
                print(f"    PASS: rank 1 is {args.file_id}, uploader resolved, "
                      f"distance {top.distance}/256")
        print(f"\n{'ALL PROBES PASSED' if failures == 0 else f'{failures} PROBE(S) FAILED'}")
        return 1 if failures else 0
    finally:
        cleanup_test_warehouse(scratch)


if __name__ == "__main__":
    raise SystemExit(main())
