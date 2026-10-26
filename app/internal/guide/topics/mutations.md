# Mutations: reviewed writes

Nothing in PDW writes to an upstream service directly. A mutation is **proposed**, lands
in a review queue, and runs only after a human approves it in the web or phone review UI.
Prefer this path for any change to Zach's Gmail (mail, labels and filters), Calendar, Google or Apple Contacts, Slack
(a message sent as him, or his read state), Apple Notes, or a text message (RCS/SMS from his Pixel); use a direct connector only for
something PDW has no mutation type for, and say why. Deleting a screen capture from the warehouse is a mutation too
(`screen_timelapse.purge_moment`); never delete warehouse rows or stored objects by hand.

## The two calls

1. {{if .CLI}}`pdw call propose_mutation_help`{{else}}`propose_mutation_help` (no arguments){{end}} — the catalog of supported types with
   field-by-field descriptions and a worked example for each. Read it first; the payload
   shapes are strict.
2. {{if .CLI}}`pdw call propose_mutation --data '<json>'`{{else}}`propose_mutation`{{end}} — `mutations` is an array of `{type, ...payload}`
   entries. It returns a `request_id` and an `approval_url`; it executes nothing.

Supported types: `gmail.send_email`, `gmail.archive_threads`, `gmail.unarchive_threads`,
`gmail.modify_thread_labels`, `gmail.create_filter`, `gmail.replace_filter`, `gmail.delete_filter`,
`gmail.create_label`, `gmail.update_label`, `gmail.delete_label`, `calendar.create_event`, `calendar.update_event`,
`calendar.delete_event`, `google_people.contacts`, `contacts.batch_mutation`, `slack.mark_conversation_read`,
`slack.send_message`, `apple_notes.create_note`, `apple_notes.update_note`, `apple_contacts.create_contact`,
`apple_contacts.update_contact`, `apple_contacts.merge_contacts`, `android_messages.send_message`,
`apple_messages.mark_read`, `screen_timelapse.purge_moment`. The help call is
authoritative when this list and it disagree.

## Rules

- **A batch is reviewed as the thing it is about**, so propose one request per intent
  (one archive batch, one reply) rather than many single-item requests, and give each
  entry the context the reviewer needs. Gmail thread batches render as an inbox; Slack
  mark-read batches as conversations with permalinks; calendar proposals on a day grid
  with conflicts flagged.
- **Email as Zach:** preserve CC lists on replies, reply in the thread you found, and
  never send from a guessed account. To forward, set `message.forward_message_id` to the
  original's `base_gmail.messages.message_id` and write only your note: the `Fwd:`
  subject, Gmail's forwarded block and the original's attachments are added for you. The reviewer can edit an email before approving it,
  drop one item from a batch without denying the rest, or mark a dead request superseded.
- **Email attachments:** set `message.attachments` to an array of
  `{filename, content_type, data_base64}` (standard base64 bytes, not a path or URL).
  Maximum 100 files / 20 MiB decoded total per message. The proposal snapshots the
  files for review and sends the approved bytes with either a send or draft.
  {{if .CLI}}For large payloads, use `pdw call propose_mutation < proposal.json` rather than a shell argument.{{end}} Variants
  inherit the base attachments; set a variant's `message.attachments` to override, or
  `[]` to remove. Reviewers can download, add or remove files in the web editor.
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
- **Gmail filters and labels:** read what exists first in `base_gmail.labels` and
  `base_gmail.filters` (`is_deleted = 0`). A filter is `criteria` (from, to, subject, query,
  negated_query, has_attachment, size) plus `action` (label names in `add_labels` /
  `remove_labels`, `create_and_add_labels` to create one, and the booleans `skip_inbox`,
  `mark_read`, `star`, `mark_important`, `never_important`, `never_spam`, `trash`, plus
  `category`); label names resolve to ids at execution. Forwarding is refused. A filter only
  touches **new** mail: to move what is already there, add a `gmail.modify_thread_labels` /
  `gmail.archive_threads` batch for those threads in the same request (the review shows how
  many recent threads match; label and filter changes run first, in order). Gmail has no
  filter edit: `gmail.replace_filter` names the old `filter_id` and creates the new rule
  before deleting the old one. `delete_*` and `replace_filter` snapshot what they change,
  and the executor refuses if it changed before approval.
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
- **Texts as Zach:** `android_messages.send_message` sends one text from Google Messages
  on his Pixel (RCS, or SMS when the recipient has none; Messages decides) to ONE
  `recipient`: an E.164 phone (`+13105550123`), taken from `marts_contacts.contact_points`
  or a one-to-one `chat_key` in `base_android_messages.messages` / a handle in
  `base_apple_messages.handles` — never a number guessed from a name, and confirm the
  number belongs to the person before proposing. Emails (old iMessage handles) are
  refused. `text` is plain, at most 4,000 characters. The review shows the number, the
  contact card(s) it is on, the latest messages with it from either phone, warns on a
  stranger or a first text, and lets Zach edit the words before he approves. No group
  chats. One approval sends one message: the phone checks Messages' database for the same
  text since approval before sending. `apple_messages.send_message` is retired (he left
  the iPhone on 2026-10-23).
- **Mark texts read:** `apple_messages.mark_read` marks ONE one-to-one conversation read;
  `chat` is its `base_apple_messages.chats.chat_identifier` (E.164 phone, email, or short
  code like `72975`). No group chats. The Mac opens the conversation in Messages, so it
  runs only while porygon's screen is unlocked (it waits as `failed_retryable` until then),
  marks the whole conversation read, and sends read receipts where the sender has them on.
- **Delete a leaked screen capture:** `screen_timelapse.purge_moment` with `device`,
  `captured_at` (the exact `base_screen_timelapse.screenshots.captured_at`), optional
  `display_index` (omit for every display) and `reason`. Name the KIND of thing the frame
  showed in `reason` ("full card number on screen"), never the thing itself: the request
  is a timeline event. A moment that matches no row is refused. After approval the worker
  deletes the screenshots, the moment's accessibility tree, their readings, the
  `screen_capture` timeline event (when display 1 goes) and each stored file in Drive.
  The review counts other frames with the same bytes: propose those too. Files on the
  device and the backups on slowking are not touched.
- **Status lives in the warehouse.** The request's state, result and error are in
  `ops.upstream_mutation_operations` (readable by the query role for exactly this), and
  every proposal is a `mutation_requests` event on the timeline (search scope `mutations`).
  `blocked_missing_credentials` means a person must re-grant something; it is not
  retried.
- Do not retry one mutation across both surfaces{{if .CLI}} (CLI and MCP){{end}}; check the queue first.
