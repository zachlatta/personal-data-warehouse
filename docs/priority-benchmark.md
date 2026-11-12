# Priority correctness benchmark

Labels and reports never go in git; see [search benchmark privacy](search-benchmark.md#privacy).
The editing surface is `.priority-eval/ground_truth.json`, mirrored to
`private.priority_benchmark_labels`. Aggregate measurements go to
`ops.priority_benchmark_results`; `marts_ops.timeline_priority_correctness` joins
the private requirements manifest to those measurements without exposing IDs.

The file has `requirements` (adapter → stratum → required count) and `labels`
(adapter, event_id, expected, stratum). Missing labels are rejected before
publication. Every declared stratum must be present in full. Missing events count
as errors, and each adapter must score at least 90% over its declared samples.
Independently labeled `self` events also form a gold stratum: at least 90% must
remain classified as self (recall). This floor applies per adapter with any gold
self labels, so background-heavy samples cannot hide a classifier dropping all
human acts. Self precision must also reach 90%: true human self acts divided by
all predicted-self events in `blind_self_precision` strata (including names with
that prefix followed by `_`), when declared, or all labels otherwise. Each
independent precision stratum must meet the floor; a passing older cohort cannot
hide a failing new one. An explicitly declared precision sample with no self
predictions fails. The mart exposes counts, precision and gold-self recall;
measurements lacking precision evidence fail until refreshed.
Empty/unpublished labels, absent strata, unmeasured labels, changed label versions
and measurements older than seven days all fail loudly. The C2 audit derives its
adapter coverage from this manifest, rather than maintaining another adapter list.

## Independent labels and coverage

The first sample uses the fixed window 2026-11-04 18:00 UTC through 2026-11-11
18:00 UTC, taking the first 25 events per adapter by `md5(event_id)`. No tier is
filtered or projected while labeling. A second sample takes ten events per emitted
tier, ordered by `md5(event_id || 'stratified-2026-11-11')` in the same window.
The selection query uses priority only to partition the draw; it projects an
opaque hash of the partition, not its assigned priority. Assigned tiers are
revealed only after expected labels are locked.

Read content, recipients, channel/thread context and provider provenance before
labeling. Store expected tiers in the private file before scoring; never relabel a
failure to agree with the classifier. Fix rules when an adapter fails. The strata
include genuine self acts, direct correspondence and peripheral conversation where
those tiers exist. Evaluate an always-background baseline to check that each
agent adapter fails the 90% bar. A small sample is not a corpus-wide guarantee;
supplement it with a fresh hash-ordered review of surviving self turns.
The SQL classifiers are measured deterministic baselines; agents make the
independent labeling judgments.

The self-precision draw uses the October 26–November 12, 2026 UTC window (upper
bound exclusive). For each agent adapter, take the first 40 events classified
as self by the baseline SQL, ordered by
`md5(event_id || 'self-precision-2026-11-12')`. Project source content, parent
context and provenance, without assigned or revised tiers; lock judgments before
evaluating revised SQL. Keep this stratum separate from the blind all-tier draw,
tier-stratified coverage and targeted regressions. After changes, audit a fresh
40-session self sample with a different hash seed and report every machine case.

The delivery-focused replication uses the same fixed window and salt
`delivery-blind-2026-11-12-7c4f2a`: draw 50 baseline-self sessions and 80 baseline-self
user turns by hash order, excluding already labeled events. Lock source/context
judgments before evaluating revised classifications. Store this cohort separately
as `blind_self_precision_delivery`. Sender-tool evidence supplies the independent
`targeted_agent_deliveries` stratum; its labels remain event IDs only.
The resulting-self holdout uses salt `post-delivery-self-2026-11-12-r9`, drawing
40 sessions and 60 user turns while excluding all earlier labels. Preserve its
independent judgments in `blind_self_precision_holdout` and enforce its own floor.

Paseo's retained local agent records contain parent-agent labels, but PDW has no
corresponding registry or launch-source columns. Provider JSON in the evaluated
window lacks those Paseo keys. Ingesting parent-agent and schedule/workflow launch
provenance, keyed by provider session ID, is a follow-up; it would replace the
opening role/ownership and probe heuristics with structural evidence.

Targeted regression labels are a separate `targeted_human_duplicates` stratum,
not part of either blind draw. They cover human requests sent to multiple agents,
the human originals of Codex subagent copies, and their typed follow-ups. Label
these from source context before measuring the proposed SQL. Their expected self
labels contribute to the same per-adapter gold-self recall floor. Preserve the
blind sample and its judgments when extending this stratum.

The separate `targeted_automation_envelopes` stratum covers schedule run
declarations (including one-shot runs), automatic request dispatch, repository
routing, reviewer transitions and temporary-root helpers. Label these from their
source provenance, not the currently assigned tier. When changing an adapter,
audit both self-to-background and background-to-self changes across the full
evaluation window. Review every changed row and scan surviving self openers for
recurring automation. Preserve the repeated-opener baseline unless a labeled
human counterexample justifies narrowing it.

## Rules

Untargeted public community Slack chatter is noise, regardless of membership;
private channels/group DMs, public name mentions, broadcast replies and addressing
use the cc/direct rules. Attachment shells are background.

Automated agent prompts are background. The detailed provenance, envelope and
repetition rules are in the [agent sessions guide](../app/internal/guide/topics/agent-sessions.md).
The fan-out probe examines at most 16 matching deliveries and requires four
distinct non-subagent sessions. Repeated deliveries within one session do not
establish fan-out; a limited probe can conservatively miss automation.

User-channel turns delivered by another session are machinery. Resolve parent
priorities after the materialized source-page limit, then collect candidate
delivery tool calls only across that page's human-session time range. Partial
`agent_delivery_idx` source indexes cover the shared delivery predicate and
timestamp. Calls include shell `paseo send`/`run` and agent-prompt MCP tools.
Require a different source/session and a preceding call within ten minutes.
Normalize JSON/shell escaping and whitespace; compare the entire message at
48–127 characters, or at least three of four disjoint 32-character fingerprints
for longer messages. This tolerates interpolated paths, timestamps and ports
without evaluating shell code. Shorter common text is insufficient without a
recipient/launch registry. Explicit parenthesized agent-relay markers are also
background. Human text with no matching earlier call stays self.

Marketplace human messages are direct; buyer offers and app-install requests are
cc relays. Generic marketplace promotions and CI notices are noise.
Outbound numbered email sequences to at least ten distinct recipients with the
same subject within ±3 days are bulk noise; a reply in an established human
thread stays self. Numbered sequences include week/day/lesson/part/step/episode
counters, rather than a particular campaign name.

## Commands

```sh
uv run python scripts/priority_benchmark.py sql --proposed
uv run python scripts/priority_benchmark.py sql --proposed --output details
uv run python scripts/priority_benchmark.py publish-labels
uv run python scripts/priority_benchmark.py pull-labels
uv run python scripts/priority_benchmark.py measure
```

`sql` is read-only: put its SQL in ignored scratch and use `pdw sql --file` after
reading columns. The evaluator executes the adapter SELECTs, checks every SQL
rewrite anchor, and recomputes parent sessions before turns. Gmail's query-role
evaluation rebuilds its inaccessible caches from public rows using the engine's
shared correspondent aggregation; it tests fresh-cache semantics, not daily cache
timing. Tests execute the proposed SQL against a fictional warehouse.

`publish-labels` and `measure` require a trusted database URL and write production
when pointed there. Publishing invalidates previous measurements. Publish first,
then measure stored output after adapter reclassification; deploying code alone
never creates labels or manufactures a passing grade. `pull-labels` restores the
private editing file. Use the same approved writer environment as search/answer
benchmark publication; keep its connection URL out of commands, logs and git.

## Scheduled measurement

The Dagster `priority_benchmark` asset runs in `priority_benchmark_job`, scheduled
by `priority_benchmark_daily` at **05:37 UTC every day**, enabled by default.
It measures stored timeline classifications against the published private labels;
it neither labels events nor invokes an agent. An active-job guard and exclusive
lock prevent overlapping scheduled runs. The job has a 15-minute runtime cap and
one retry after a transient failure.

Before labels are published, the asset succeeds with `skipped: labels unpublished`
at informational log level. It writes no measurement, and the mart remains
`unpublished` / `failing`. Invalid published labels or database errors still fail
the job. Pipeline health tracks the measurement timestamp with a two-day interval;
the correctness mart independently fails measurements older than seven days.

Measurements describe stored classifications. After adapter rules change, measure
once history catch-up covers the labeled window and its parent sessions. The daily
schedule refreshes the measurement against the same independently labeled set.
