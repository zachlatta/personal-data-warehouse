"""Benchmark photo-caption quality across agent models and reasoning efforts.

Runs the EXACT production enrichment path (same candidate shape, capture
context, prompt, image prep, and agent container pipeline) for a stratified
sample of real photos under several (model, reasoning_effort) configs, then
prints a per-config comparison and writes every caption to JSONL for
eyeballing / labeling. Results are deliberately NOT written to
file_attachment_enrichments — benchmark variants must never leak into the
timeline search documents.

Run where the agent container can run (the prod Dagster container on rotom,
which holds the provider keys, Docker socket, and Drive credential):

    uv run python scripts/photo_caption_benchmark.py \
        --sample 18 \
        --configs gpt-5.6-sol:high,gpt-5.6-terra:medium,gpt-5.6-luna:medium,gpt-5.6-luna:low \
        --output /tmp/photo-caption-benchmark.jsonl

Buckets (per --sample, split evenly): photos with a calendar event near
capture (does the caption name it?), photos with GPS but no event, and
plain photos (neither). Auto-scores are heuristics — the JSONL is the real
deliverable for picking the production config.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
import time
from collections import defaultdict
from typing import Any

from personal_data_warehouse.agent_resource import AgentResource
from personal_data_warehouse.agent_runner import AgentRunRequest
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.defs.photos_drive_ingest import photos_object_store
from personal_data_warehouse.file_attachment_enrichment import (
    PHOTOS_SOURCE,
    attachment_storage_ref,
    attachment_vision_prompt,
    attachment_vision_schema,
    prepare_attachment_image,
    validate_attachment_vision_result,
)
from personal_data_warehouse.photo_context import photo_enrichment_context
from personal_data_warehouse.warehouse import warehouse_from_settings

_SAMPLE_SQL = """
WITH scored AS (
    SELECT r.photo_id, r.account, r.content_sha256, r.filename, r.mime_type,
           r.size_bytes, r.storage_backend, r.storage_key, r.storage_file_id,
           r.storage_url, r.capture_ts,
           EXISTS (
               SELECT 1 FROM calendar_events c
               WHERE c.start_at <= r.capture_ts + INTERVAL '3 hours'
                 AND c.end_at >= r.capture_ts - INTERVAL '3 hours'
                 AND c.is_deleted = 0 AND c.status <> 'cancelled'
           ) AS has_calendar,
           (a.latitude <> 0 OR a.longitude <> 0) AS has_gps
    FROM photo_canonical_renditions r
    JOIN photo_assets a ON a.photo_id = r.photo_id
)
SELECT *,
       CASE WHEN has_calendar THEN 'calendar'
            WHEN has_gps THEN 'gps'
            ELSE 'plain' END AS bucket
FROM scored
ORDER BY bucket, capture_ts DESC
"""


def load_sample(warehouse, per_bucket: int) -> list[dict[str, Any]]:
    rows = warehouse._query_dicts(_SAMPLE_SQL)
    taken: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        bucket = str(row["bucket"])
        if len(taken[bucket]) < per_bucket:
            taken[bucket].append(dict(row))
    sample = [row for bucket_rows in taken.values() for row in bucket_rows]
    return sample


def parse_configs(raw: str) -> list[tuple[str, str]]:
    configs = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        model, _, effort = chunk.partition(":")
        configs.append((model.strip(), (effort or "medium").strip()))
    if not configs:
        raise SystemExit("--configs must name at least one model[:effort]")
    return configs


def event_terms_near_capture(context: dict[str, Any] | None) -> list[str]:
    if not context:
        return []
    return [
        str(event.get("summary", "")).strip()
        for event in context.get("calendar_events_near_capture", [])
        if str(event.get("summary", "")).strip()
    ]


def caption_mentions(result: dict[str, Any], terms: list[str]) -> bool:
    haystack = " ".join(
        [str(result.get("summary", "")), " ".join(map(str, result.get("entities", []) or []))]
    ).lower()
    return any(term.lower() in haystack for term in terms)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=int, default=18, help="Total photos (split across 3 buckets)")
    parser.add_argument("--photo-ids", default="", help="Comma-separated photo ids (overrides --sample)")
    parser.add_argument("--configs", required=True, help="model[:effort],model[:effort],...")
    parser.add_argument("--output", default="photo-caption-benchmark.jsonl")
    parser.add_argument("--dry-run", action="store_true", help="List the sample and exit")
    args = parser.parse_args()

    settings = load_settings(require_gmail=False, require_photos=True, require_agent=True)
    warehouse = warehouse_from_settings(settings)
    store = photos_object_store(settings)
    base_resource = AgentResource.from_config(settings.agent)

    if args.photo_ids:
        wanted = [photo_id.strip() for photo_id in args.photo_ids.split(",") if photo_id.strip()]
        rows = warehouse._query_dicts(_SAMPLE_SQL)
        sample = [dict(row) for row in rows if str(row["photo_id"]) in wanted]
    else:
        sample = load_sample(warehouse, per_bucket=max(1, args.sample // 3))
    if not sample:
        print("no benchmark candidates found (need identity-resolved photos with thumbnails)")
        return 1

    print(f"sample: {len(sample)} photos")
    for row in sample:
        print(f"  [{row['bucket']:8}] {row['photo_id']} {row['filename']} @ {row['capture_ts']}")
    if args.dry_run:
        return 0

    configs = parse_configs(args.configs)
    results: list[dict[str, Any]] = []
    with open(args.output, "w", encoding="utf-8") as sink:
        for photo_index, row in enumerate(sample, start=1):
            content = store.get_object(attachment_storage_ref(row))
            image, image_name = prepare_attachment_image(
                content=content,
                mime_type=str(row.get("mime_type", "")),
                filename=str(row.get("filename", "")),
            )
            context = photo_enrichment_context(warehouse, row)
            prompt = attachment_vision_prompt(
                image_name=image_name,
                candidate=row,
                source_label=PHOTOS_SOURCE.label,
                instructions=PHOTOS_SOURCE.vision_instructions or None,
                context=context,
            )
            event_terms = event_terms_near_capture(context)
            for model, effort in configs:
                config_label = f"{model}:{effort}"
                resource = dataclasses.replace(base_resource, model=model, reasoning_effort=effort)
                started = time.monotonic()
                error = ""
                output: dict[str, Any] = {}
                issues: list[str] = []
                try:
                    run = resource.run(
                        AgentRunRequest(
                            prompt=prompt,
                            schema=attachment_vision_schema(),
                            task_type="photo_caption_benchmark",
                            subject_id=str(row.get("content_sha256", "")),
                            prompt_version=PHOTOS_SOURCE.prompt_version,
                            input_files={image_name: image},
                        )
                    )
                    if run.status != "completed":
                        error = run.error or f"agent run {run.run_id} ended {run.status}"
                    else:
                        output = dict(run.final_output_json)
                        issues = validate_attachment_vision_result(output)
                except Exception as exc:  # noqa: BLE001 - benchmark keeps going
                    error = str(exc)
                elapsed = time.monotonic() - started
                record = {
                    "photo_id": row["photo_id"],
                    "bucket": row["bucket"],
                    "filename": row["filename"],
                    "config": config_label,
                    "latency_seconds": round(elapsed, 1),
                    "error": error,
                    "schema_issues": issues,
                    "event_terms_near_capture": event_terms,
                    "mentions_nearby_event": caption_mentions(output, event_terms) if output else False,
                    "result": output,
                }
                results.append(record)
                sink.write(json.dumps(record, default=str) + "\n")
                sink.flush()
                status = "ERROR" if error else ("issues" if issues else "ok")
                print(
                    f"[{photo_index}/{len(sample)}] {config_label:24} {row['filename']:24} "
                    f"{elapsed:6.1f}s {status}"
                )

    print("\n=== per-config summary ===")
    print(f"{'config':26} {'ok':>3} {'err':>4} {'schema':>6} {'p50 s':>6} {'event-hit':>9}")
    by_config: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in results:
        by_config[record["config"]].append(record)
    for config_label, records in by_config.items():
        ok = [record for record in records if not record["error"]]
        latencies = sorted(record["latency_seconds"] for record in ok) or [0.0]
        calendar_records = [
            record for record in ok if record["bucket"] == "calendar" and record["event_terms_near_capture"]
        ]
        event_hits = sum(1 for record in calendar_records if record["mentions_nearby_event"])
        event_rate = f"{event_hits}/{len(calendar_records)}" if calendar_records else "n/a"
        schema_ok = sum(1 for record in ok if not record["schema_issues"])
        print(
            f"{config_label:26} {len(ok):>3} {len(records) - len(ok):>4} "
            f"{schema_ok:>3}/{len(ok):<2} {latencies[len(latencies) // 2]:>6.1f} {event_rate:>9}"
        )
    print(f"\ncaptions written to {args.output} — eyeball them before picking a config.")
    warehouse.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
