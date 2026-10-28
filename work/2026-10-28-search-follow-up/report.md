# Search landing follow-up

Checked 2026-10-28 after 20:27 UTC. Read-only production checks; no service changes.

The first complete post-deploy day, 2026-10-27 16:10 UTC through
2026-10-28 16:10 UTC, improved substantially but did not meet the 2,000 ms p90 target.

| Scope | Requests | Before p50 ms | After p50 ms | Before p90 ms | After p90 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| All real traffic | 808 | 2,148 | 1,558.5 | 8,888 | 5,623.7 |
| Priority filtered | 334 | 2,189 | 1,489 | 15,693 | 4,038.9 |

All-traffic p90 improved 36.7%; priority-filtered p90 improved 74.3%.
284/808 requests exceeded two seconds; three requests failed.
Before values come from the assigned baseline and the landed search incident documentation.
These are different traffic samples, not a controlled replay. Priority filtered means any
priority filter, matching the earlier attention comparison; it does not prove each call used
exactly self,direct,cc.

The live marts_ops.search_request_latency rolling 24h snapshot at the later check contained
938 requests, five errors, p50 1,482.5 ms and p90 4,475.6 ms, status attention.
Its window differs from the fixed first-day comparison above. Of its 297 slow requests,
158 were I/O bound and 139 idle; lexical was slowest in 105, semantic in 116,
exact in 22 and unmeasured in 54.

## Step timings and cancellation

695 first-day requests had measured leg timings (nonnegative milliseconds).

| Step | p50 ms | p90 ms |
| --- | ---: | ---: |
| Embedding | 18 | 34 |
| Lexical | 823 | 3,168.2 |
| Exact | 467 | 1,527.4 |
| Semantic | 978 | 2,841.4 |
| Fusion | 103 | 242.2 |

Legs overlap; percentiles cannot be added. Exact was degraded in 21/808 real requests
(2.60%), or 21/695 measured-leg requests (3.02%).
Priority-filtered requests accounted for 15 cuts; unfiltered requests for six.
The recorded degraded exact leg is the observable cut proxy, not an independent cancellation trace.
Benchmark and contract-audit clients are excluded with the same hyphenated client names
used by the live latency view.

## Memory

At 20:27:13 UTC, cache_residency reported 2,751,250,432 resident bytes out of
20,925,833,216 bytes: 13.15% of the combined search working set in PostgreSQL shared buffers.
The landing documentation's earlier snapshot was 5.7%. This is a point-in-time measurement,
not a 24-hour average. It includes chunk text and vectors as well as BM25/HNSW indexes.
It does not measure the separate OS page cache; 13.15% must not be presented as total RAM residency.

Lexical and semantic tail times plus low shared-buffer residency are consistent with continued
read pressure. They do not establish that buying RAM is the next fix. Measure OS cache residency
and competing readers before a host sizing decision; no host changes are part of this report.

## Voice enrichment verification

Recent apple_voice_memos_enrichment_job runs succeeded, including 20:17:07–20:17:18 UTC today.
pg_stat_statements shows the exact new SELECT h.occurred_at,h.who,h.text FROM
timeline.search_text(...) AS h shape used by the enrichment helper: 42 calls,
14,586 ms cumulative execution, 51,548 shared blocks read, first tracked at
2026-10-27 16:33:38 UTC.

Across the 23 normalized shapes of the old event/person Slack ILIKE lookup,
the cumulative call count remains 32, matching the landing baseline.
No statistics reset occurred since 2026-10-26 20:33 UTC and dealloc is zero.
This supports no additional legacy calls and confirms use of the replacement lookup;
pg_stat_statements is cumulative and does not attribute a specific call to a particular run.

## Reproduction

check.py performs bounded read-only queries through SSH and psql because the normal query
role cannot read ops.search_request_timings or the administrative statistics/Dagster store.
Set AUDIT_HOST, WAREHOUSE_CONTAINER and DAGSTER_CONTAINER to the current production placement.
The adjacent CSV files are the captured aggregate results. No query terms, message bodies,
credentials or session identifiers are collected.

Report delivery: replied successfully to the requested existing email conversation;
recipient verified as the requesting sender and response marked SENT.

Validation: uv run pytest completed with 3,166 passed and five skipped in 62.44 seconds.
Staged content reviewed for public disclosure; only aggregate operational metrics and
reproduction queries are included. No application code changed.
