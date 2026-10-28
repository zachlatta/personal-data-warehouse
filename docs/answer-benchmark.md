# Answer benchmark (contract C16)

The other contracts measure the machine: adapters current, search ranking, latency. This
one measures what they exist for: **does an agent with only PDW give Zach the right
answer?** It asks a fixed set of real questions Zach asked agents before, each with a gold
answer checked at its source, to a fresh agent whose only tool is the `pdw` CLI, and grades
each on four things:

| measure | how | target |
| --- | --- | --- |
| correct | every `key_facts` entry matches the answer (case-insensitive regex alternatives), and the run is not contaminated | 90% of questions |
| right source | a cited ref is a gold ref, a neighbouring event in the same thread within an hour, or matches `gold_predicate`, using the [search benchmark](search-benchmark.md)'s matcher | reported |
| PDW calls | `pdw` invocations in the agent's trace, plus invented commands and SQL errors | median under 6 |
| time | the agent run's wall clock, timeouts included | median under 60 s |

`marts_ops.answer_benchmark` is one row for the latest run: `ok`, `attention` (below a target,
a contaminated answer or an errored question), `failing` (under 70% correct, or nothing
answered), `no_data` (no run in ten days). `scripts/contract_audit.py` grades C16 from it.

## Privacy

Questions, gold answers and the agent's answers are Zach's private data and never go in git:

- the editing file is `.search-eval/answer_questions.json` (gitignored, and
  `assert_private_path` refuses any other location);
- `private.answer_benchmark_questions` mirrors it (`publish` / `pull`);
- `private.answer_benchmark_results` holds each question's answer, grade and `pdw` trace;
- only counts reach `ops.answer_benchmark_runs` and the mart.

Runs do not write `ops.ai_processing_agent_runs`, so nothing about them reaches the timeline.

## Copying from the session that first answered

Every question came from a past agent session, and that session holds the answer. Three
layers keep it from being copied:

1. **The tool proxy hides it.** The run asks `run_with_pdw` to exclude the `agent_session`,
   `agent_session_turn` and `muse_file` (Muse's agent-written memory) adapters, plus the question's `exclude_refs` (a non-agent copy of the
   answer, such as a zrl-bot reply email or the WhatsApp message an assistant mirrored it
   to). Search and context hits are dropped and SQL rows
   carrying an excluded id are removed. A dropped row is a **prevented** leak: it is counted
   (`proxy_drops`) and never held against the run.
2. **Leak markers catch what the proxy cannot.** SQL that returns only text (an `ILIKE` over
   `timeline.events.search_text`) carries no id to filter. Each question stores
   `leak_markers`: Zach's original wording and distinctive sentences of the original agent's
   answer that the gold source does not contain. A marker anywhere in the agent's trace marks
   the run **contaminated**.
3. **Citations.** Citing an agent session or an `exclude_refs` entry is contaminated too.

A contaminated answer is graded wrong and counted in `contaminated`.

**If you read the gold answers, do not copy them anywhere searchable that is not an agent
session.** Agent sessions are excluded as a whole, so reviewing this file from Claude Code or
Codex is safe. An email, note or Slack message quoting a gold answer must be added to that
question's `exclude_refs` and republished.

## The question file

A JSON list; every field is validated on load and a bad entry is refused, not scored:

```json
{
  "question_id": "cc1-04",
  "question": "self-contained, absolute dates, in Zach's voice",
  "original_question": "Zach's wording when he first asked",
  "as_of": "2026-09-10",
  "gold_answer": "short correct answer",
  "key_facts": [["sept?(ember)? 3", "2026-09-03", "9/3"]],
  "gold_refs": ["gmail_email:<account>|<message-id>"],
  "gold_predicate": null,
  "gold_source": "gmail",
  "category": "email",
  "origin_source": "claude_code",
  "origin_session": "<session id>",
  "origin_asked_at": "2026-09-10T14:00:00Z",
  "checked_evidence": "how we know the original answer was right",
  "leak_markers": ["at least 20 chars, not in the question or gold answer"],
  "exclude_refs": [],
  "verification_note": "how the gold was re-checked at its source",
  "active": true
}
```

Rules: a gold source is a record, never an agent session or agent memory; the answer must stay true (a past
fact, not "what's on today"); verify gold answers at the source with `search_text_exact` or
SQL on `base_*`, not with the ranker under test. Set `active: false` to retire a question
whose answer changed, rather than editing history.

## Running it

```bash
# A run that writes nothing: questions from the private file, refs resolved
# read-only through `pdw sql`, the report under .search-eval/answer_reports/.
uv run python scripts/answer_benchmark.py run --limit 3

# Publish the edited file, then a run that reads questions from and writes
# results to the warehouse at POSTGRES_DATABASE_URL.
uv run python scripts/answer_benchmark.py publish
uv run python scripts/answer_benchmark.py run --write

# Restore the private file in a fresh checkout.
uv run python scripts/answer_benchmark.py pull
```

The agent is the managed container agent (`agent_runner.ContainerAgentRunner`, the one
enrichment uses), one question at a time, ten minutes each. Its model is the deployment's
`AGENT_PROVIDER` / `AGENT_MODEL` unless `--provider` / `--model` say otherwise; Zach chose
GPT-6.1 Sol (`codex` / `gpt-6.1-sol`) on 2026-10-28, and every result row records the model
that answered. The prompt frame is `PROMPT_VERSION` in `answer_benchmark.py`; change it
only with a new version, because a new frame is a new baseline.
