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
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
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

# $/1M tokens (standard tier). input price applies to UNCACHED input
# (input_tokens - cached_input_tokens), cached price to cached reads; cache
# writes are not separately reported by the CLI stream, so real spend can sit
# slightly above these numbers.
PRICING_PER_MILLION = {
    "gpt-5.6-sol": {"input": 5.00, "cached": 0.50, "output": 30.00},
    "gpt-5.6-terra": {"input": 2.50, "cached": 0.25, "output": 15.00},
    "gpt-5.6-luna": {"input": 1.00, "cached": 0.10, "output": 6.00},
}


def usage_from_events(events) -> dict[str, int]:
    """Sum token usage across the run's turn.completed events."""
    totals = {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0}
    for event in events:
        payload = event.event_json if hasattr(event, "event_json") else event
        if not isinstance(payload, dict):
            continue
        usage = payload.get("usage")
        if isinstance(usage, dict) and str(payload.get("type", "")).endswith("turn.completed"):
            for key in totals:
                try:
                    totals[key] += int(usage.get(key, 0) or 0)
                except (TypeError, ValueError):
                    continue
    return totals


def run_cost_usd(model: str, usage: dict[str, int]) -> float | None:
    pricing = PRICING_PER_MILLION.get(model)
    if pricing is None or not any(usage.values()):
        return None
    uncached = max(0, usage["input_tokens"] - usage["cached_input_tokens"])
    return (
        uncached * pricing["input"]
        + usage["cached_input_tokens"] * pricing["cached"]
        + usage["output_tokens"] * pricing["output"]
    ) / 1_000_000


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


def load_sample(warehouse, total: int) -> list[dict[str, Any]]:
    """Exactly `total` photos, filled round-robin across buckets so thin
    buckets (e.g. only 3 'plain' photos) donate their slack to the others."""
    rows = warehouse._query_dicts(_SAMPLE_SQL)
    by_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_bucket[str(row["bucket"])].append(dict(row))
    order = ["calendar", "gps", "plain"]
    sample: list[dict[str, Any]] = []
    index = 0
    while len(sample) < total and any(by_bucket.get(bucket) for bucket in order):
        bucket = order[index % len(order)]
        index += 1
        if by_bucket.get(bucket):
            sample.append(by_bucket[bucket].pop(0))
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
    parser.add_argument("--workers", type=int, default=4, help="Concurrent agent runs")
    parser.add_argument("--dry-run", action="store_true", help="List the sample and exit")
    args = parser.parse_args()

    settings = load_settings(require_gmail=False, require_photos=True, require_agent=True)
    warehouse = warehouse_from_settings(settings)
    store = photos_object_store(settings)
    base_config = settings.agent  # frozen dataclass; per-config variants via replace()

    if args.photo_ids:
        wanted = [photo_id.strip() for photo_id in args.photo_ids.split(",") if photo_id.strip()]
        rows = warehouse._query_dicts(_SAMPLE_SQL)
        sample = [dict(row) for row in rows if str(row["photo_id"]) in wanted]
    else:
        sample = load_sample(warehouse, total=args.sample)
    if not sample:
        print("no benchmark candidates found (need identity-resolved photos with thumbnails)")
        return 1

    print(f"sample: {len(sample)} photos")
    for row in sample:
        print(f"  [{row['bucket']:8}] {row['photo_id']} {row['filename']} @ {row['capture_ts']}")
    if args.dry_run:
        return 0

    configs = parse_configs(args.configs)

    # Prepare each photo once (bytes, context, prompt), then fan the
    # (photo x config) grid across a small pool of concurrent agent
    # containers — the runs are independent, and serial execution at several
    # minutes per agent run would take the better part of a day. Latency
    # numbers therefore include some contention; cost and quality do not.
    prepared: list[dict[str, Any]] = []
    for row in sample:
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
        prepared.append(
            {
                "row": row,
                "image": image,
                "image_name": image_name,
                "prompt": prompt,
                "event_terms": event_terms_near_capture(context),
            }
        )

    # Resume support: deploys replace the container and kill this process, so
    # completed (photo, config) pairs are reloaded from the output file (kept
    # on a persistent volume) and skipped on relaunch.
    results: list[dict[str, Any]] = []
    done_pairs: set[tuple[str, str]] = set()
    try:
        with open(args.output, encoding="utf-8") as existing:
            for line in existing:
                try:
                    record = json.loads(line)
                except ValueError:
                    continue
                if not record.get("error"):
                    done_pairs.add((str(record.get("photo_id")), str(record.get("config"))))
                    results.append(record)
    except FileNotFoundError:
        pass

    grid = [
        (photo_index, item, model, effort)
        for photo_index, item in enumerate(prepared, start=1)
        for model, effort in configs
        if (str(item["row"]["photo_id"]), f"{model}:{effort}") not in done_pairs
    ]
    total = len(grid) + len(done_pairs)
    completed = len(done_pairs)
    if done_pairs:
        print(f"resuming: {len(done_pairs)} runs already complete, {len(grid)} to go", flush=True)
    sink_lock = threading.Lock()

    def run_one(job) -> dict[str, Any]:
        photo_index, item, model, effort = job
        row = item["row"]
        config_label = f"{model}:{effort}"
        resource = AgentResource.from_config(
            dataclasses.replace(base_config, model=model, reasoning_effort=effort)
        )
        started = time.monotonic()
        error = ""
        output: dict[str, Any] = {}
        issues: list[str] = []
        usage = {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0}
        try:
            run = resource.run(
                AgentRunRequest(
                    prompt=item["prompt"],
                    schema=attachment_vision_schema(),
                    task_type="photo_caption_benchmark",
                    subject_id=str(row.get("content_sha256", "")),
                    prompt_version=PHOTOS_SOURCE.prompt_version,
                    input_files={item["image_name"]: item["image"]},
                )
            )
            usage = usage_from_events(run.events)
            if run.status != "completed":
                error = run.error or f"agent run {run.run_id} ended {run.status}"
            else:
                output = dict(run.final_output_json)
                issues = validate_attachment_vision_result(output)
        except Exception as exc:  # noqa: BLE001 - benchmark keeps going
            error = str(exc)
        elapsed = time.monotonic() - started
        cost = run_cost_usd(model, usage)
        return {
            "photo_id": row["photo_id"],
            "bucket": row["bucket"],
            "filename": row["filename"],
            "config": config_label,
            "latency_seconds": round(elapsed, 1),
            "usage": usage,
            "cost_usd": round(cost, 5) if cost is not None else None,
            "error": error,
            "schema_issues": issues,
            "event_terms_near_capture": item["event_terms"],
            "mentions_nearby_event": caption_mentions(output, item["event_terms"]) if output else False,
        } | {"result": output, "_photo_index": photo_index}

    with open(args.output, "a", encoding="utf-8") as sink:
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            for record in pool.map(run_one, grid):
                record.pop("_photo_index")
                results.append(record)
                with sink_lock:
                    sink.write(json.dumps(record, default=str) + "\n")
                    sink.flush()
                completed += 1
                status = "ERROR" if record["error"] else ("issues" if record["schema_issues"] else "ok")
                print(
                    f"[{completed}/{total}] {record['config']:24} {record['filename']:24} "
                    f"{record['latency_seconds']:6.1f}s {status}",
                    flush=True,
                )

    print("\n=== per-config summary ===")
    print(
        f"{'config':22} {'ok':>3} {'err':>4} {'schema':>6} {'p50 s':>6} {'event-hit':>9} "
        f"{'$/photo':>8} {'$ / 11k':>8}"
    )
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
        costs = [record["cost_usd"] for record in ok if record["cost_usd"] is not None]
        mean_cost = sum(costs) / len(costs) if costs else 0.0
        print(
            f"{config_label:22} {len(ok):>3} {len(records) - len(ok):>4} "
            f"{schema_ok:>3}/{len(ok):<2} {latencies[len(latencies) // 2]:>6.1f} {event_rate:>9} "
            f"{mean_cost:>8.4f} {mean_cost * 11000:>8.0f}"
        )
    print(f"\ncaptions written to {args.output} — eyeball them before picking a config.")
    warehouse.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
