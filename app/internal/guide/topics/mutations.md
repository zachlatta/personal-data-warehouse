# Mutations: reviewed writes

Nothing in PDW writes to an upstream service directly. A mutation is **proposed**, lands
in a review queue, and runs only after a human approves it in the web or phone review UI.
Prefer this path for any change to Zach's Gmail, Calendar, Contacts, Slack read state or
Apple Notes; use a direct connector only for something PDW has no mutation type for, and
say why.

## The two calls

1. {{if .CLI}}`pdw call propose_mutation_help`{{else}}`propose_mutation_help` (no arguments){{end}} — the catalog of supported types with
   field-by-field descriptions and a worked example for each. Read it first; the payload
   shapes are strict.
2. {{if .CLI}}`pdw call propose_mutation --data '<json>'`{{else}}`propose_mutation`{{end}} — `mutations` is an array of `{type, ...payload}`
   entries. It returns a `request_id` and an `approval_url`; it executes nothing.

Supported types: `gmail.send_email`, `gmail.archive_threads`, `gmail.unarchive_threads`,
`gmail.modify_thread_labels`, `calendar.create_event`, `calendar.update_event`,
`calendar.delete_event`, `google_people.contacts`, `contacts.batch_mutation`, `slack.mark_conversation_read`,
`apple_notes.create_note`, `apple_notes.update_note`. The help call is authoritative when
this list and it disagree.

## Rules

- **A batch is reviewed as the thing it is about**, so propose one request per intent
  (one archive batch, one reply) rather than many single-item requests, and give each
  entry the context the reviewer needs. Gmail thread batches render as an inbox; Slack
  mark-read batches as conversations with permalinks; calendar proposals on a day grid
  with conflicts flagged.
- **Email as Zach:** preserve CC lists on replies, reply in the thread you found, and
  never send from a guessed account. The reviewer can edit an email before approving it,
  drop one item from a batch without denying the rest, or mark a dead request superseded.
- **Apple Notes:** `body` replaces the whole note; `append_body` adds to it. Prefer
  `append_body` — the executor cannot tell an intentional rewrite from a stale read. A
  note is addressed by the bare UUID `base_apple_notes.notes.note_id`; a title is the
  note's first line, not a separate property. These run on a Mac, not in the cloud, so
  they wait for that Mac's next uploader cycle after approval.
- **Status lives in the warehouse.** The request's state, result and error are in
  `ops.upstream_mutation_operations` (readable by the query role for exactly this), and
  every proposal is a `mutation_requests` event on the timeline (search scope `mutations`).
  `blocked_missing_credentials` means a person must re-grant something; it is not
  retried.
- Do not retry one mutation across both surfaces{{if .CLI}} (CLI and MCP){{end}}; check the queue first.
