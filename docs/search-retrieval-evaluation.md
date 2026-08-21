# Timeline retrieval replay evaluation

The retrieval evaluator replays real `timeline.search_text()` calls mined from
agent transcripts. Generated query corpora and reports contain private data and
must stay under the gitignored `.search-eval/` directory.

```bash
# Recreate the original Stage-3 30-day corpus.
uv run python scripts/search_retrieval_eval.py mine \
  --since 2026-07-20 --until 2026-08-20 \
  --output .search-eval/queries.jsonl

# Run judged cases first, then the full replay after the semantic backfill is caught up.
uv run python scripts/search_retrieval_eval.py run --judged-only
uv run python scripts/search_retrieval_eval.py run
```

Each JSONL case includes the literal query, its original mode/scope, and up to
10 result keys returned to the historical agent. A key is the stable `ref` when
the old query selected it, otherwise the result's source + event time. Those
keys are regression labels: they measure whether a new retriever preserves
previously returned candidates, not whether those candidates were useful or
whether every relevant corpus item was found. The
format records that caveat as
`relevance_provenance=historical_search_results`. Replace `relevant_keys` (and
optionally `relevant_refs`) and set `relevance_provenance=human` as cases are
manually judged; the evaluator then reports ordinary recall@k and MRR without
changing the harness.

The fixed 2026-07-20 through 2026-08-20 snapshot currently yields 451 literal,
actually executed searches after excluding unexpanded shell variables and
tool prompts/code edits that merely mention `timeline.search_text`. The earlier
exploratory count of roughly 465 included those ambiguous transcript matches.

Exact identifier/phrase searches replay only in `exact` mode. Ranked keyword
queries run in both `keyword` and `hybrid` modes and report recall@k, MRR,
hit@1, hit@5, fallbacks, errors, and hybrid candidates that were not in the
keyword top-k. Hit rates answer the practical question "did the user get at
least one useful result near the top?" without penalizing a retriever for not
returning every duplicate passage that contains the same answer.
Do not interpret hybrid numbers while `derived_search.chunks` or
`derived_search.chunk_embeddings` is still backfilling: the lexical half sees
the full timeline while the semantic half does not.
