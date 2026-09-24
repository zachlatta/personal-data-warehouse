# Mutations: reviewed writes

Nothing in PDW writes to an upstream service directly. A mutation is **proposed**, lands
in a review queue, and runs only after a human approves it in the web or phone review UI.
Prefer this path for any change to Zach's Gmail, Calendar, Google or Apple Contacts, Slack
(a message sent as him, or his read state) or Apple Notes; use a direct connector only for
something PDW has no mutation type for, and say why.

## The two calls

1. {{if .CLI}}`pdw call propose_mutation_help`{{else}}`propose_mutation_help` (no arguments){{end}} — the catalog of supported types with
   field-by-field descriptions and a worked example for each. Read it first; the payload
   shapes are strict.
2. {{if .CLI}}`pdw call propose_mutation --data '<json>'`{{else}}`propose_mutation`{{end}} — `mutations` is an array of `{type, ...payload}`
   entries. It returns a `request_id` and an `approval_url`; it executes nothing.

Supported types: `gmail.send_email`, `gmail.archive_threads`, `gmail.unarchive_threads`,
`gmail.modify_thread_labels`, `calendar.create_event`, `calendar.update_event`,
`calendar.delete_event`, `google_people.contacts`, `contacts.batch_mutation`, `slack.mark_conversation_read`,
`slack.send_message`, `apple_notes.create_note`, `apple_notes.update_note`, `apple_contacts.create_contact`,
`apple_contacts.update_contact`, `apple_contacts.merge_contacts`. The help call is
authoritative when this list and it disagree.

## Rules

- **A batch is reviewed as the thing it is about**, so propose one request per intent
  (one archive batch, one reply) rather than many single-item requests, and give each
  entry the context the reviewer needs. Gmail thread batches render as an inbox; Slack
  mark-read batches as conversations with permalinks; calendar proposals on a day grid
  with conflicts flagged.
- **Email as Zach:** preserve CC lists on replies, reply in the thread you found, and
  never send from a guessed account. The reviewer can edit an email before approving it,
  drop one item from a batch without denying the rest, or mark a dead request superseded.
- **A proposal is never edited after it is proposed; it is replaced or withdrawn.** To
  correct one, propose the corrected request with `replaces_request_id` and
  `replaces_reason`: while the old request is still pending it is withdrawn in the same
  transaction, so the reviewer can never approve both and send the same thing twice; if it
  failed or was denied it is linked as superseded; if it was approved or has already run
  the proposal is refused with its status — do not re-propose it, say what is wrong. To
  take a pending request back with no replacement (done by hand, wrong account, overtaken
  by events), {{if .CLI}}`pdw call withdraw_mutation --data '{"request_id": "...", "reason": "..."}'`{{else}}`withdraw_mutation` with `request_id` and `reason`{{end}}.
  A reason is required on both paths and is shown to the reviewer. Once a reviewer has
  edited a request its `revision` moves, and you must pass the revision you read
  (`replaces_revision` / `expected_revision`) or the call is refused: never take back a
  version a human is still changing. Withdrawing removes work from the queue and never
  runs anything; approval and execution stay with the human and the workers.
- **Slack as Zach:** `slack.send_message` posts through his own Slack session (never a
  bot) to a `conversation_id` from `base_slack.conversations`, to one person by `user_id`
  from `base_slack.users` (the executor reuses or opens the DM), or under `thread_ts` (a
  synced `base_slack.messages.message_ts`; needs `conversation_id`). Write Slack mrkdwn
  and `<@U…>` mentions, at most 4,000 characters, and reply where the conversation is —
  read it with `timeline.context()` first. The reviewer sees the recipient by name, the
  thread, and the warnings (not synced, archived, not a member), and can edit the words
  before approving; recipient and thread are not editable. One approval sends one
  message: a retry after a lost response finds its own `client_msg_id` before posting.
- **Apple Notes:** `body` replaces the whole note; `append_body` adds to it. Prefer
  `append_body` — the executor cannot tell an intentional rewrite from a stale read. A
  note is addressed by the bare UUID `base_apple_notes.notes.note_id`; a title is the
  note's first line, not a separate property. These run on a Mac, not in the cloud, so
  they wait for that Mac's next uploader cycle after approval.
- **Apple Contacts:** `marts_contacts.contacts` unions the Google and iCloud books
  (`source` says which). Write to Google with `google_people.contacts`; write to iCloud
  with `apple_contacts.*` (a card's `card_id`, `<UUID>:ABPerson`). Nothing copies one
  book to the other, so a contact that must exist in both needs a proposal for each.
  `update_contact` is additive — scalars are set, emails/phones/urls are added, nothing is
  dropped unless named in `remove`, and `append_note` is preferred over `note`.
  `merge_contacts` deletes the merged cards after copying their values: propose it only
  for cards that share an email or phone in `marts_contacts.contact_points`, never on a
  name match alone, and keep the card with the photo or hand-typed note. The review shows
  each card's current contents; the executor records the pre-edit cards in the result.
  An `update_contact` naming a card an earlier approved merge already deleted is applied
  to the surviving card (`redirected_from` in the result), so batching merges and updates
  from one snapshot is safe. Like Notes these run on a Mac and iCloud syncs the change to
  every device.
- **Status lives in the warehouse.** The request's state, result and error are in
  `ops.upstream_mutation_operations` (readable by the query role for exactly this), and
  every proposal is a `mutation_requests` event on the timeline (search scope `mutations`).
  `blocked_missing_credentials` means a person must re-grant something; it is not
  retried.
- Do not retry one mutation across both surfaces{{if .CLI}} (CLI and MCP){{end}}; check the queue first.
