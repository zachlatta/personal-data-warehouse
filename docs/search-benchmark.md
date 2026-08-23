# Labeled retrieval benchmark

`search_benchmark` measures **where a known-correct answer actually ranks**. Labels are
produced independently of the ranker, so unlike the replay evaluator it can measure a
retriever that is *better* than the incumbent, not only one that regressed.

Use it alongside, not instead of, [the replay evaluator](search-retrieval-evaluation.md):

| | replay evaluator | this benchmark |
| --- | --- | --- |
| relevance comes from | the previous retriever's own top-10 | independent human/agent labels |
| answers | "did we lose a result we used to return?" | "is the right answer near the top?" |
| can detect an improvement | no — a better result scores as a miss | yes |
| label cost | free (mined) | expensive (manual) |

## Privacy

**Labels and reports never go in git.** They contain private queries and timeline
references, and this repository is public. They live under the gitignored `.search-eval/`,
and `assert_private_path` refuses to write an artifact anywhere else. Only this harness is
version-controlled.

The label file is the expensive artifact and it is deliberately *not* backed up by the
repo. Copy it somewhere private if the machine holding it matters.

## Running it

```bash
# Score every labeled query in hybrid, keyword and exact mode.
uv run python scripts/search_benchmark.py run

# Deeper, more parallel, and excluding the sessions that produced the labels.
uv run python scripts/search_benchmark.py run \
  --depth 100 --workers 12 \
  --exclude-agent-sessions-since 2026-08-22 \
  --exclude-session <session-id>

# Serial latency sample (the only comparable latency number).
uv run python scripts/search_benchmark.py latency --sample 8
```

**Run it in parallel.** A single hybrid call takes tens of seconds against the production
corpus, so a serial pass over a few dozen queries takes half an hour. `--workers` fans the
calls out and identical `(query, mode)` pairs are executed once. Wall clock drops roughly
an order of magnitude.

The cost of that: **timings collected under concurrency are not single-user latency**. The
report labels them `latency_under_concurrency` and says so. When you need a latency number
to compare against a budget, use the `latency` subcommand, which runs strictly serially.

## The label file

`.search-eval/ground_truth.json` is a list of cases:

```json
[
  {
    "query": "the exact query text to issue",
    "stratum": "natural_language",
    "verdict": "FOUND",
    "ambiguous": false,
    "truth_refs": ["gmail_email:<account>|<message-id>"],
    "truth_predicate": {"sources": ["gmail"], "since": "2025-01-01", "text_regex": "renew"},
    "note": "why this is the answer, and what the decoys are"
  }
]
```

- `truth_refs` — timeline references, built as `adapter || ':' || event_id`. The harness
  resolves every one against `timeline.events` before scoring and reports any that no
  longer exist: **an unresolved ref is a stale label, not a retrieval failure.**
- `truth_predicate` — for a query with hundreds of equally-correct answers (*any*
  out-of-office notice), enumerating five refs understates recall, because the ranker may
  have returned a sixth perfectly good one. A predicate accepts any result matching all of
  its conditions (`sources`, `since`, `until`, `text_regex`). An empty predicate matches
  nothing — that is a labeling mistake, not a wildcard.
- `verdict` — `FOUND`, `WEAK` (only tangential matches exist), or `NOT_IN_CORPUS` (skipped
  by default). Recording a query the corpus genuinely cannot answer is a useful label.
- `stratum` — scores are broken out by it. Retrieval quality differs enormously between
  natural-language questions and identifier lookups, and a single aggregate hides that.
- `ambiguous` — flags a query with no unique target, printed with `~` in the table so a
  miss is not over-read.

### Producing labels

Labels must be found **without the ranker under test**, or they are circular. Use
`timeline.search_text_exact()` (literal substring), `ILIKE`/trigram SQL on `base_*` tables,
and targeted SQL on `timeline.events`. Never use the `search` tool to decide what the right
answer is.

### Contamination

Agent sessions are indexed into the timeline. A session that *discusses* a benchmark query
quotes it verbatim and then ranks for it, which both fakes hits and displaces real answers.
Exclude the sessions that produced the labels with `--exclude-session`, and everything
recent with `--exclude-agent-sessions-since`. The report records how many rows were dropped
per query so the exclusion is auditable rather than silent.

## Metrics

`hit@1`, `hit@5`, `hit@10`, `found@depth`, `MRR`, and median rank when found — overall and
per stratum, per mode. A result counts as relevant when it matches a labeled ref, satisfies
the predicate, or is a neighbouring event in the same `(source, context)` window within an
hour. That last allowance exists because the semantic branch chunks chat into per-hour
windows and can legitimately return a different message from the window holding the answer;
scoring it as a miss would penalise the retriever for the indexer's granularity.

## Reading a report

Every report stamps the environment it measured — git sha, `pdw version`, chunk and
embedding counts, and `timeline.events` max `seq`. Two reports whose corpus counts differ
materially are not directly comparable: the semantic corpus converges continuously, so a
score can move because the index grew rather than because the ranker changed.
