package mutations

import (
	"context"
	"errors"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

const proposeMutationDescription = "Create a pending upstream mutation request for human review in the Personal Data Warehouse. Does not execute the mutation; returns a request_id and approval_url for the web review UI. Mutations is an array — each entry specifies a type (e.g. gmail.send_email) plus that type's payload fields. Call propose_mutation_help first to see the supported types and the exact payload schema for each. To correct an earlier proposal, pass replaces_request_id (with replaces_reason): the old request is withdrawn in the same transaction if still pending, so a reviewer can never approve both; if it was already approved or has run, the proposal is refused rather than repeating the change."

const withdrawMutationDescription = "Withdraw a pending mutation request you no longer stand behind — the change was made by hand, the recipient or account was wrong, events overtook it, or another request replaces it. Requires a reason; optionally names the request that replaces it. Only a request still pending review can be withdrawn: an approved, executing, finished, or denied request is refused with its status, and if a reviewer has edited the request you must pass the revision you read (expected_revision). Withdrawing never runs anything; it only removes work from the review queue and is recorded in the audit log with your client identity."

const proposeMutationHelpDescription = "Return the catalog of mutation types supported by propose_mutation, with field-by-field descriptions and a worked example for each. With no arguments it returns every type (~36 KB); pass type (e.g. gmail.send_email) to get one type's fields and example, or list_only: true for just the type names and summaries. Call this before propose_mutation to see how to shape each mutation entry."

// Tools returns the propose_mutation MCP tools backed by the given service.
// Returns nil when service is nil so callers can pass the result straight into
// a tool.Registry without a nil guard.
func Tools(service *Service) []tool.Tool {
	if service == nil {
		return nil
	}
	return []tool.Tool{
		&tool.Typed[ProposeMutationInput, ProposalResponse]{
			NameStr:        "propose_mutation",
			TitleStr:       "Propose Mutation",
			DescriptionStr: proposeMutationDescription,
			Handle: func(ctx context.Context, in ProposeMutationInput) (ProposalResponse, error) {
				response, err := service.ProposeMutation(ctx, in)
				return response, agentFacingError(err)
			},
		},
		&tool.Typed[WithdrawMutationInput, WithdrawResponse]{
			NameStr:        "withdraw_mutation",
			TitleStr:       "Withdraw Mutation",
			DescriptionStr: withdrawMutationDescription,
			Handle: func(ctx context.Context, in WithdrawMutationInput) (WithdrawResponse, error) {
				response, err := service.WithdrawMutation(ctx, in)
				return response, agentFacingError(err)
			},
		},
		&tool.Typed[ProposeMutationHelpInput, MutationHelpDocument]{
			NameStr:        "propose_mutation_help",
			TitleStr:       "Propose Mutation Help",
			DescriptionStr: proposeMutationHelpDescription,
			Handle: func(_ context.Context, in ProposeMutationHelpInput) (MutationHelpDocument, error) {
				return MutationHelpFor(in)
			},
		},
	}
}

// agentFacingError turns the two outcomes a caller can act on — a rejected
// input and a request whose state refuses the action — into the tool layer's
// invalid-input error (HTTP 400, an MCP error result with the message), and
// leaves storage failures as what they are. Without this a "request X was
// already approved" answer would surface as an origin 502 that an edge proxy
// may replace with a generic body the agent cannot read.
func agentFacingError(err error) error {
	if err == nil {
		return nil
	}
	var inputErr *proposalInputError
	if errors.As(err, &inputErr) {
		return &tool.InvalidInputError{Message: inputErr.Error()}
	}
	var stateErr *RequestStateError
	if errors.As(err, &stateErr) {
		return &tool.InvalidInputError{Message: stateErr.Error()}
	}
	return err
}

type ProposeMutationHelpInput struct {
	Type     string `json:"type,omitempty" jsonschema:"one mutation type to document in full, e.g. gmail.send_email; omit for every type"`
	ListOnly bool   `json:"list_only,omitempty" jsonschema:"return only each type's name and summary, without fields or examples"`
}

// MutationHelpFor narrows the catalog. Agents were grepping the 27 KB full
// document for one type (and once for a type name that does not exist, twice,
// getting nothing back either time); an unknown type is answered with the
// list of real ones.
func MutationHelpFor(in ProposeMutationHelpInput) (MutationHelpDocument, error) {
	doc := MutationHelp()
	want := strings.TrimSpace(in.Type)
	if want != "" {
		var names []string
		for _, entry := range doc.Mutations {
			if entry.Type == want {
				doc.Mutations = []MutationHelpType{entry}
				return doc, nil
			}
			names = append(names, entry.Type)
		}
		return MutationHelpDocument{}, &tool.InvalidInputError{Message: "no mutation type " + want + "; supported types: " + strings.Join(names, ", ")}
	}
	if in.ListOnly {
		for i := range doc.Mutations {
			doc.Mutations[i] = MutationHelpType{Type: doc.Mutations[i].Type, Summary: doc.Mutations[i].Summary, RequiresEnv: doc.Mutations[i].RequiresEnv}
		}
	}
	return doc, nil
}

type MutationHelpDocument struct {
	Overview  string             `json:"overview"`
	Common    []MutationHelpArg  `json:"common_request_fields"`
	Mutations []MutationHelpType `json:"mutation_types"`
}

type MutationHelpType struct {
	Type        string            `json:"type"`
	Summary     string            `json:"summary"`
	Fields      []MutationHelpArg `json:"fields"`
	Example     map[string]any    `json:"example"`
	ExtraNotes  string            `json:"notes,omitempty"`
	RequiresEnv string            `json:"account_must_be_in_env,omitempty"`
}

type MutationHelpArg struct {
	Name        string `json:"name"`
	JSONType    string `json:"json_type"`
	Required    bool   `json:"required"`
	Description string `json:"description"`
}

func MutationHelp() MutationHelpDocument {
	return MutationHelpDocument{
		Overview: "propose_mutation creates a pending mutation request for human review. " +
			"The top-level fields (title, reason, mutations, context) wrap one or more mutations. " +
			"Each entry in mutations is an object whose `type` selects one of the supported operations below; the remaining fields are the payload for that type. " +
			"A request is never edited by an agent after it is proposed: to correct one, propose the corrected request with replaces_request_id and replaces_reason (the old request is withdrawn in the same transaction while it is still pending, linked as superseded if it failed or was denied, and the proposal is refused if it was approved or has run), " +
			"or call withdraw_mutation with a reason to take a pending request back without a replacement. Reviewers edit, trim, approve, and deny; agents propose and withdraw.",
		Common: []MutationHelpArg{
			{Name: "title", JSONType: "string", Required: true, Description: "short human-readable title for the reviewed request"},
			{Name: "reason", JSONType: "string", Required: true, Description: "why these mutations should be reviewed and approved"},
			{Name: "mutations", JSONType: "array<object>", Required: true, Description: "one or more mutation entries; each must include type plus that type's fields"},
			{Name: "context", JSONType: "object", Required: false, Description: "optional source context for the human reviewer"},
			{Name: "replaces_request_id", JSONType: "string", Required: false, Description: "id of an earlier request this proposal replaces; withdrawn atomically while pending, linked when failed or denied, refused when approved or already run"},
			{Name: "replaces_reason", JSONType: "string", Required: false, Description: "required with replaces_request_id: why the earlier request is being replaced"},
			{Name: "replaces_revision", JSONType: "integer", Required: false, Description: "the earlier request's revision as last read; required once a reviewer has edited it"},
		},
		Mutations: []MutationHelpType{
			{
				Type:        GmailArchiveOperation,
				Summary:     "Remove INBOX from one or more Gmail threads.",
				RequiresEnv: "GMAIL_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + GmailArchiveOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Gmail account email address"},
					{Name: "thread_ids", JSONType: "array<string>", Required: true, Description: "Gmail thread IDs to archive"},
				},
				Example: map[string]any{
					"type":       GmailArchiveOperation,
					"account":    "you@example.com",
					"thread_ids": []string{"1899e1d3a4f0aaaa", "1899e1d3a4f0bbbb"},
				},
			},
			{
				Type:        GmailUnarchiveOperation,
				Summary:     "Add INBOX back to one or more Gmail threads.",
				RequiresEnv: "GMAIL_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + GmailUnarchiveOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Gmail account email address"},
					{Name: "thread_ids", JSONType: "array<string>", Required: true, Description: "Gmail thread IDs to return to the inbox"},
				},
				Example: map[string]any{
					"type":       GmailUnarchiveOperation,
					"account":    "you@example.com",
					"thread_ids": []string{"1899e1d3a4f0aaaa"},
				},
			},
			{
				Type:        GmailModifyThreadLabelsOperation,
				Summary:     "Add existing labels, explicitly create-and-add new labels, and/or remove labels on Gmail threads.",
				RequiresEnv: "GMAIL_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + GmailModifyThreadLabelsOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Gmail account email address"},
					{Name: "thread_ids", JSONType: "array<string>", Required: true, Description: "Gmail thread IDs whose labels should change"},
					{Name: "add_labels", JSONType: "array<string>", Required: false, Description: "exact Gmail label display names or immutable label IDs to add"},
					{Name: "create_and_add_labels", JSONType: "array<string>", Required: false, Description: "Gmail user-label display names to create if missing and then add; never interpreted as immutable label IDs"},
					{Name: "remove_labels", JSONType: "array<string>", Required: false, Description: "exact Gmail label display names or immutable label IDs to remove"},
				},
				Example: map[string]any{
					"type":                  GmailModifyThreadLabelsOperation,
					"account":               "you@example.com",
					"thread_ids":            []string{"1899e1d3a4f0aaaa"},
					"add_labels":            []string{"Receipts", "STARRED"},
					"create_and_add_labels": []string{"Projects/Launch"},
					"remove_labels":         []string{"UNREAD"},
				},
				ExtraNotes: "At least one label list is required, and the same label cannot appear in more than one list. " +
					"add_labels and remove_labels accept a Gmail label's exact display name (for example Receipts) or immutable label ID (for example Label_42 or STARRED), and stay strict when a label is unknown. " +
					"Labels are created only when explicitly listed in create_and_add_labels; the executor reuses an existing same-name label on retries before applying it to the reviewed threads.",
			},
			{
				Type:        GmailCreateFilterOperation,
				Summary:     "Create a Gmail filter: which new mail it matches, and what happens to it (labels, skip inbox, mark read, star, importance, spam, category, trash).",
				RequiresEnv: "GMAIL_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + GmailCreateFilterOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Gmail account email address"},
					{Name: "criteria", JSONType: "object", Required: true, Description: "at least one of: from, to, subject, query (Gmail search syntax), negated_query (string); has_attachment (bool); size (bytes) with size_comparison \"larger\"|\"smaller\"; exclude_chats (bool, not a criterion on its own). Gmail's camelCase names (negatedQuery, hasAttachment, sizeComparison) are accepted too; anything else is refused."},
					{Name: "action", JSONType: "object", Required: true, Description: "at least one of: add_labels / remove_labels (existing label names from base_gmail.labels), create_and_add_labels (names to create if missing), and the booleans skip_inbox, mark_read, star, mark_important, never_important, never_spam, trash, plus category (primary|social|updates|forums|promotions). System labels (INBOX, UNREAD, STARRED...) go through the booleans, never the label lists. forward is refused."},
				},
				Example: map[string]any{
					"type":     GmailCreateFilterOperation,
					"account":  "you@example.com",
					"criteria": map[string]any{"from": "newsletter@example.com"},
					"action":   map[string]any{"skip_inbox": true, "create_and_add_labels": []string{"Newsletters"}},
				},
				ExtraNotes: "Gmail applies a filter only to new mail. To also move what is already in the mailbox, add a gmail.modify_thread_labels (or gmail.archive_threads) mutation for those thread_ids in the same request; the review shows how many recent threads the criteria match. " +
					"Read current labels and filters in base_gmail.labels and base_gmail.filters (snapshotted every 30 minutes and right after one of these mutations runs). add_labels/remove_labels must name labels that exist there or are created earlier in the same request (gmail.create_label, or create_and_add_labels); mutations in a request run in order, and label/filter changes run before thread label changes. " +
					"A filter that forwards mail is refused: forwarding needs a verified address and a separate decision. An identical existing filter is refused at proposal and treated as done at execution.",
			},
			{
				Type:        GmailReplaceFilterOperation,
				Summary:     "Change an existing Gmail filter (Gmail has no filter update: creates the new rule, then deletes the old one).",
				RequiresEnv: "GMAIL_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + GmailReplaceFilterOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Gmail account email address"},
					{Name: "filter_id", JSONType: "string", Required: true, Description: "base_gmail.filters.filter_id of the live filter to replace"},
					{Name: "criteria", JSONType: "object", Required: true, Description: "the complete new criteria, as for " + GmailCreateFilterOperation},
					{Name: "action", JSONType: "object", Required: true, Description: "the complete new action, as for " + GmailCreateFilterOperation},
				},
				Example: map[string]any{
					"type":      GmailReplaceFilterOperation,
					"account":   "you@example.com",
					"filter_id": "ANe1BmjExample",
					"criteria":  map[string]any{"from": "newsletter@example.com"},
					"action":    map[string]any{"skip_inbox": true, "mark_read": true, "add_labels": []string{"Newsletters"}},
				},
				ExtraNotes: "The old filter is snapshotted at proposal and the review shows before → after. The executor creates the new filter first and deletes the old one second, so the mailbox is never without the rule; if Gmail's copy of the old filter changed since review, nothing is changed.",
			},
			{
				Type:        GmailDeleteFilterOperation,
				Summary:     "Delete a Gmail filter.",
				RequiresEnv: "GMAIL_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + GmailDeleteFilterOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Gmail account email address"},
					{Name: "filter_id", JSONType: "string", Required: true, Description: "base_gmail.filters.filter_id of the live filter to delete"},
				},
				Example: map[string]any{
					"type":      GmailDeleteFilterOperation,
					"account":   "you@example.com",
					"filter_id": "ANe1BmjExample",
				},
				ExtraNotes: "The rule is snapshotted at proposal and shown in the review; the executor refuses if it changed since, and treats an already-deleted filter as done.",
			},
			{
				Type:        GmailCreateLabelOperation,
				Summary:     "Create a Gmail label, optionally with a color and visibility.",
				RequiresEnv: "GMAIL_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + GmailCreateLabelOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Gmail account email address"},
					{Name: "name", JSONType: "string", Required: true, Description: "label name; nest with a slash (Parent/Child)"},
					{Name: "color", JSONType: "object", Required: false, Description: "{background_color, text_color}, each one of Gmail's palette colors (for example #16a766, #4a86e8, #fb4c2f, #ffad47, #a479e2, #000000, #ffffff)"},
					{Name: "label_list_visibility", JSONType: "string", Required: false, Description: "labelShow, labelShowIfUnread or labelHide"},
					{Name: "message_list_visibility", JSONType: "string", Required: false, Description: "show or hide"},
				},
				Example: map[string]any{
					"type":    GmailCreateLabelOperation,
					"account": "you@example.com",
					"name":    "Receipts",
					"color":   map[string]any{"background_color": "#16a766", "text_color": "#ffffff"},
				},
				ExtraNotes: "A name already in base_gmail.labels is refused at proposal; at execution an existing same-name label counts as done.",
			},
			{
				Type:        GmailUpdateLabelOperation,
				Summary:     "Rename a Gmail label or change its color or visibility.",
				RequiresEnv: "GMAIL_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + GmailUpdateLabelOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Gmail account email address"},
					{Name: "label", JSONType: "string", Required: true, Description: "the label's current name or label_id (base_gmail.labels)"},
					{Name: "new_name", JSONType: "string", Required: false, Description: "new name; must not clash with another label"},
					{Name: "color", JSONType: "object", Required: false, Description: "as for " + GmailCreateLabelOperation},
					{Name: "label_list_visibility", JSONType: "string", Required: false, Description: "labelShow, labelShowIfUnread or labelHide"},
					{Name: "message_list_visibility", JSONType: "string", Required: false, Description: "show or hide"},
				},
				Example: map[string]any{
					"type":     GmailUpdateLabelOperation,
					"account":  "you@example.com",
					"label":    "Receipts",
					"new_name": "Finance/Receipts",
				},
				ExtraNotes: "System labels cannot be changed. Renaming a parent does not rename its nested labels; propose each. Filters reference labels by id, so a rename keeps them working.",
			},
			{
				Type:        GmailDeleteLabelOperation,
				Summary:     "Delete a Gmail label (the label comes off its messages; the messages stay).",
				RequiresEnv: "GMAIL_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + GmailDeleteLabelOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Gmail account email address"},
					{Name: "label", JSONType: "string", Required: true, Description: "the label's current name or label_id (base_gmail.labels)"},
				},
				Example: map[string]any{
					"type":    GmailDeleteLabelOperation,
					"account": "you@example.com",
					"label":   "Old Project",
				},
				ExtraNotes: "System labels cannot be deleted. The review shows how many messages carry the label and which filters use it.",
			},
			{
				Type:        GmailSendEmailOperation,
				Summary:     "Send or draft a Gmail message — new, a reply, or a forward. Supports multiple titled variants for human reviewer to pick from.",
				RequiresEnv: "GMAIL_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + GmailSendEmailOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Gmail account email address"},
					{Name: "delivery_mode", JSONType: "string", Required: false, Description: `"send" or "draft"; defaults to "send"`},
					{Name: "message", JSONType: "object", Required: true, Description: "base message; fields: to[], cc[], bcc[], subject, body_text, body_html, reply_to_thread_id, forward_message_id, in_reply_to, references[], attachments[]. Each attachment is {filename, content_type, data_base64}: a plain filename (no paths/control characters, max 255 UTF-8 bytes), a non-multipart MIME type without parameters (e.g. application/pdf), and canonical standard base64 file bytes (not base64url, a URL or a local path). Up to 100 files and 20 MiB total decoded bytes per message. Bytes are snapshotted for human review; variants inherit base attachments, message.attachments overrides them, [] removes them. Must include at least one recipient, a subject, and body_text or body_html; a forward needs only recipients."},
					{Name: "variants", JSONType: "array<object>", Required: false, Description: "optional alternate proposals; each needs a two-word `title` for the review tab. Variant fields override the base message; omitted fields inherit from message."},
				},
				Example: map[string]any{
					"type":          GmailSendEmailOperation,
					"account":       "you@example.com",
					"delivery_mode": "send",
					"message": map[string]any{
						"to":          []string{"friend@example.com"},
						"subject":     "Re: lunch",
						"body_text":   "Sounds good — see you at 1pm.",
						"attachments": []map[string]any{{"filename": "agenda.txt", "content_type": "text/plain", "data_base64": "THVuY2gK"}},
					},
				},
				ExtraNotes: "When variants is non-empty, every variant must have a two-word title (max 32 chars), and each fully-resolved variant must independently pass the same recipient/subject/body validation. " +
					"To reply, set reply_to_thread_id to the Gmail thread_id; the thread headers and quoted history are added for you. " +
					"To forward, set forward_message_id to the Gmail message_id of a synced message (base_gmail.messages.message_id, a gmail search hit's source_pk.message_id) and write only your note in body_text: the subject defaults to \"Fwd: <original subject>\", Gmail's forwarded-message block with the original is added below the signature, and the original's attachments are re-sent from Gmail at execution, alongside any attachments[] you add. " +
					"A forward is filed in the original's conversation, cannot also be a reply, and every variant must forward the same message.",
			},
			{
				Type:        GooglePeopleContactsOperation,
				Summary:     "Create, update, or delete Google Contacts. Multiple operations can be batched in one mutation entry.",
				RequiresEnv: "CONTACT_GOOGLE_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + GooglePeopleContactsOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Google Contacts account email address"},
					{Name: "operations", JSONType: "array<object>", Required: true, Description: `each operation has op = "create_contact", "update_contact", or "delete_contact"; see notes for the required fields of each`},
				},
				Example: map[string]any{
					"type":    GooglePeopleContactsOperation,
					"account": "you@example.com",
					"operations": []map[string]any{
						{
							"op": "create_contact",
							"person": map[string]any{
								"names":          []map[string]any{{"givenName": "Ada", "familyName": "Lovelace"}},
								"emailAddresses": []map[string]any{{"value": "ada@example.com"}},
							},
						},
						{
							"op":            "update_contact",
							"resource_name": "people/c1234567890123456789",
							"etag":          "%EhQBAgMFBgcJCgsMEBUWLjU3PT4/QBoBAiIMdGhpc2lzYW5ldGFn",
							"person": map[string]any{
								"organizations": []map[string]any{{"name": "Hack Club", "title": "Gap Year Fellow"}},
							},
						},
						{
							"op":            "delete_contact",
							"resource_name": "people/c9876543210987654321",
							"etag":          "%EhQBAgMFBgcJCgsMEBUWLjU3PT4/QBoBAiIMYW5vdGhlcmV0YWc=",
						},
					},
				},
				ExtraNotes: "create_contact needs `person` (a Google People `Person`) and must NOT set person.resourceName. " +
					"update_contact and delete_contact both need `resource_name` plus the contact's current `etag` — read both from " +
					"base_google_contacts.cards (columns card_id and etag). The etag is what lets the executor prove the contact still " +
					"looks the way the reviewer saw it; an update without one is rejected by the People API. " +
					"For update_contact, `update_person_fields` is optional: when omitted it is derived from the keys of `person`, which " +
					"updates exactly what you sent and cannot delete anything. Set it explicitly only to CLEAR a field — naming a field " +
					"that `person` omits deletes that field's current value, and the review UI flags that in red. " +
					"Only these fields are updatable: " + contactUpdatablePersonFieldList() + ".",
			},
			{
				Type:        CalendarCreateEventOperation,
				Summary:     "Create a Google Calendar event.",
				RequiresEnv: "CALENDAR_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + CalendarCreateEventOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Google Calendar account email address"},
					{Name: "calendar_id", JSONType: "string", Required: false, Description: `calendar to insert into; defaults to "primary"`},
					{Name: "event", JSONType: "object", Required: true, Description: "standard Google Calendar event resource (summary, start, end, attendees, recurrence, …). Must include start and end."},
					{Name: "send_updates", JSONType: "string", Required: false, Description: `"all", "externalOnly", or "none"; defaults to "all"`},
				},
				Example: map[string]any{
					"type":         CalendarCreateEventOperation,
					"account":      "you@example.com",
					"calendar_id":  "primary",
					"send_updates": "all",
					"event": map[string]any{
						"summary": "Lunch with Ada",
						"start":   map[string]any{"dateTime": "2026-06-01T12:00:00", "timeZone": "America/Los_Angeles"},
						"end":     map[string]any{"dateTime": "2026-06-01T13:00:00", "timeZone": "America/Los_Angeles"},
						"attendees": []map[string]any{
							{"email": "ada@example.com"},
						},
					},
				},
			},
			{
				Type:        CalendarUpdateEventOperation,
				Summary:     "Patch fields on an existing Google Calendar event. Only keys present in `patch` are replaced.",
				RequiresEnv: "CALENDAR_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + CalendarUpdateEventOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Google Calendar account email address"},
					{Name: "calendar_id", JSONType: "string", Required: false, Description: `calendar that owns the event; defaults to "primary"`},
					{Name: "event_id", JSONType: "string", Required: true, Description: "Google Calendar event ID (instance IDs are supported for recurring overrides)"},
					{Name: "patch", JSONType: "object", Required: true, Description: "partial event resource; only included keys are replaced"},
					{Name: "expected_etag", JSONType: "string", Required: false, Description: "etag captured at proposal time; if the live event changes before execution the mutation is rejected"},
					{Name: "send_updates", JSONType: "string", Required: false, Description: `"all", "externalOnly", or "none"; defaults to "all"`},
				},
				Example: map[string]any{
					"type":          CalendarUpdateEventOperation,
					"account":       "you@example.com",
					"calendar_id":   "primary",
					"event_id":      "abc123",
					"expected_etag": `"3392140000000000"`,
					"send_updates":  "all",
					"patch": map[string]any{
						"summary": "Lunch with Ada (moved)",
					},
				},
			},
			{
				Type:        AppleNotesCreateNoteOperation,
				Summary:     "Create a note in Apple Notes. Executed on Zach's Mac through Notes.app, not in the cloud.",
				RequiresEnv: "APPLE_NOTES_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + AppleNotesCreateNoteOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Apple Notes account label"},
					{Name: "folder", JSONType: "string", Required: false, Description: `Notes folder to create in; defaults to "` + appleNotesDefaultFolder + `" and is created if missing`},
					{Name: "name", JSONType: "string", Required: false, Description: "note title; when omitted Notes uses the body's first line"},
					{Name: "body", JSONType: "string", Required: true, Description: "note body. Plain text is converted to paragraphs; HTML (<div>, <b>, <ul>) is passed through, which is how Notes stores rich text."},
				},
				Example: map[string]any{
					"type":    AppleNotesCreateNoteOperation,
					"account": "you@example.com",
					"folder":  "PDW Agent",
					"name":    "Runway",
					"body":    "12 months of cash remaining at the current burn.",
				},
				ExtraNotes: "Executed by the local apple-notes worker on a Mac running Notes.app, so it completes on that Mac's next run rather than within seconds of approval. " +
					"The result JSON carries the created note's x-coredata:// id. base_apple_notes.notes.note_id holds a different identifier for the same note (ZIDENTIFIER, a UUID) once the uploader syncs it; apple_notes.update_note accepts either form.",
			},
			{
				Type:        AppleNotesUpdateNoteOperation,
				Summary:     "Retitle, rewrite, or append to an existing Apple note.",
				RequiresEnv: "APPLE_NOTES_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + AppleNotesUpdateNoteOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Apple Notes account label"},
					{Name: "note_id", JSONType: "string", Required: true, Description: "base_apple_notes.notes.note_id (a UUID), or Notes' own x-coredata:// id; the executor accepts either and resolves the UUID against the local store"},
					{Name: "name", JSONType: "string", Required: false, Description: "new title"},
					{Name: "body", JSONType: "string", Required: false, Description: "REPLACES the whole body; mutually exclusive with append_body"},
					{Name: "append_body", JSONType: "string", Required: false, Description: "appends to the end of the existing body, leaving it otherwise untouched"},
				},
				Example: map[string]any{
					"type":        AppleNotesUpdateNoteOperation,
					"account":     "you@example.com",
					"note_id":     "0A1B2C3D-4E5F-6071-8293-A4B5C6D7E8F9",
					"append_body": "Updated 2026-08-24: burn is down 8%.",
				},
				ExtraNotes: "Prefer append_body over body. body discards whatever the note currently holds, and the executor cannot tell an intentional rewrite from a stale read — " +
					"it records the pre-edit body in the result JSON so an unwanted replacement is recoverable, but that is a repair, not a guard.",
			},
			{
				Type:        AppleContactsCreateContactOperation,
				Summary:     "Create a card in Apple (iCloud) Contacts. Executed on Zach's Mac through Contacts.app, not in the cloud.",
				RequiresEnv: "APPLE_CONTACTS_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + AppleContactsCreateContactOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Apple Contacts account label"},
					{Name: "contact", JSONType: "object", Required: true, Description: "given_name, family_name, middle_name, nickname, organization, job_title, department, note (strings) and emails, phones, urls (arrays of {label, value}; labels such as work, home, mobile, other). At least one of given_name, family_name, organization."},
				},
				Example: map[string]any{
					"type":    AppleContactsCreateContactOperation,
					"account": "you@example.com",
					"contact": map[string]any{
						"given_name": "Ada", "family_name": "Lovelace", "organization": "Hack Club", "job_title": "Deputy to the Founder",
						"emails": []map[string]any{{"label": "work", "value": "ada@example.com"}},
						"phones": []map[string]any{{"label": "mobile", "value": "+1 802 555 0100"}},
					},
				},
				ExtraNotes: "Check marts_contacts.contacts first: a person who already has a card should get update_contact, and two cards for one person should get merge_contacts. " +
					"Executed by the local apple-contacts worker on a Mac signed in to iCloud, so it completes on that Mac rather than within seconds of approval; iCloud then syncs the card to every device. " +
					"The result JSON carries the new card_id, which is also what base_apple_contacts.cards.card_id will hold once the uploader syncs it.",
			},
			{
				Type:        AppleContactsUpdateContactOperation,
				Summary:     "Set fields on, add values to, or remove values from an existing Apple Contacts card.",
				RequiresEnv: "APPLE_CONTACTS_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + AppleContactsUpdateContactOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Apple Contacts account label"},
					{Name: "card_id", JSONType: "string", Required: true, Description: "base_apple_contacts.cards.card_id (<UUID>:ABPerson); it is also Contacts' own id for the card"},
					{Name: "contact", JSONType: "object", Required: false, Description: "same shape as create_contact. Scalars are SET; emails/phones/urls are ADDED when the card lacks the value (never replaced); note REPLACES the note; append_note adds to it."},
					{Name: "remove", JSONType: "object", Required: false, Description: "emails, phones, urls: arrays of exact values to delete from the card"},
				},
				Example: map[string]any{
					"type":    AppleContactsUpdateContactOperation,
					"account": "you@example.com",
					"card_id": "8537DF38-BF0D-4468-9061-D2D41468E05A:ABPerson",
					"contact": map[string]any{
						"organization": "Hack Club", "job_title": "Director of Operations",
						"emails": []map[string]any{{"label": "work", "value": "ada@example.com"}},
					},
				},
				ExtraNotes: "Additive by default: nothing on the card is dropped unless it is named in remove, and the executor records the pre-edit card in the result JSON (previous_card). " +
					"Prefer append_note over note. The review shows the card's current contents beside the proposed change.",
			},
			{
				Type:        AppleContactsMergeContactsOperation,
				Summary:     "Fold duplicate Apple Contacts cards into one: union every email/phone/url, fill the kept card's empty fields, delete the rest.",
				RequiresEnv: "APPLE_CONTACTS_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + AppleContactsMergeContactsOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Apple Contacts account label"},
					{Name: "keep_card_id", JSONType: "string", Required: true, Description: "the card that survives (base_apple_contacts.cards.card_id)"},
					{Name: "merge_card_ids", JSONType: "array<string>", Required: true, Description: "the cards folded into keep_card_id and then DELETED"},
					{Name: "contact", JSONType: "object", Required: false, Description: "overrides applied on top of the union, same shape as create_contact (e.g. the correct family_name when the cards disagree)"},
				},
				Example: map[string]any{
					"type":           AppleContactsMergeContactsOperation,
					"account":        "you@example.com",
					"keep_card_id":   "8537DF38-BF0D-4468-9061-D2D41468E05A:ABPerson",
					"merge_card_ids": []string{"44C1F82A-1F0E-4C2B-9E1B-2A5E4F7B8C9D:ABPerson"},
					"contact":        map[string]any{"family_name": "Lovelace"},
				},
				ExtraNotes: "Destructive: the merged cards are deleted after their values are copied. Only propose it for cards you have confirmed are the same person through a shared email or phone in marts_contacts.contact_points, never on a name match alone. " +
					"Every pre-merge card is recorded in the result JSON (previous_cards). Keep the card with the photo or the hand-typed note as keep_card_id.",
			},
			{
				Type:        SlackMarkConversationReadOperation,
				Summary:     "Mark a Slack conversation read through one exact, already-synced message.",
				RequiresEnv: "SLACK_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + SlackMarkConversationReadOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Slack account label"},
					{Name: "conversation_id", JSONType: "string", Required: true, Description: "Slack C, D, or G conversation ID; use container_id from marts_inbox.slack_items"},
					{Name: "message_ts", JSONType: "string", Required: true, Description: "exact Slack message timestamp; use message_id from marts_inbox.slack_items"},
				},
				Example: map[string]any{
					"type":            SlackMarkConversationReadOperation,
					"account":         "zrl",
					"conversation_id": "D012ABCDEF",
					"message_ts":      "1593473566.000200",
				},
				ExtraNotes: "This moves the whole conversation read cursor through message_ts; it does not mark only one message or only one thread. " +
					"The executor requires the Slack web session Zach pastes into `pdw slack publish-session`, verifies the stored user/workspace identity, and refuses a target that is not already synced exactly.",
			},
			{
				Type:        AppleMessagesSendMessageOperation,
				Summary:     "Send a text (iMessage or SMS) as Zach to one phone number or iMessage email address. Executed on Zach's Mac through Messages.app, not in the cloud.",
				RequiresEnv: "APPLE_MESSAGES_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + AppleMessagesSendMessageOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Apple Messages account label (base_apple_messages.messages.account)"},
					{Name: "recipient", JSONType: "string", Required: true, Description: "ONE phone number in E.164 form (+13105550123; a bare 10-digit number is read as +1) or one iMessage email address. Take it from marts_contacts.contact_points or base_apple_messages.handles.address, never from a name alone."},
					{Name: "text", JSONType: "string", Required: true, Description: "the message, plain text, at most 4000 characters"},
					{Name: "service", JSONType: "string", Required: false, Description: `"auto" (default: reply in the conversation Zach already has with this handle, whatever its service; iMessage when there is none), "iMessage", or "SMS"`},
				},
				Example: map[string]any{
					"type":      AppleMessagesSendMessageOperation,
					"account":   "you@example.com",
					"recipient": "+13105550123",
					"text":      "Running 10 minutes late, sorry!",
				},
				ExtraNotes: "Nothing is sent until a human approves the request; the review shows the exact handle, the contact card(s) it belongs to, and the latest messages exchanged with it, and warns when the handle is on no card or has never been texted. " +
					"Executed by the resident apple-messages worker on the Mac that syncs Apple Messages, so it normally sends within seconds of approval. " +
					"One approval sends one message: before every attempt the executor looks in the Mac's chat.db for an outgoing message with the same text to the same handle since approval, and records an already-sent match as success instead of sending again. " +
					"The result JSON carries the sent message's guid and service. Group chats are not supported: one recipient per mutation.",
			},
			{
				Type:        AppleMessagesMarkReadOperation,
				Summary:     "Mark one one-to-one iMessage/SMS conversation read, as if Zach had opened it. Executed on Zach's Mac through Messages.app, not in the cloud.",
				RequiresEnv: "APPLE_MESSAGES_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + AppleMessagesMarkReadOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Apple Messages account label (base_apple_messages.messages.account)"},
					{Name: "chat", JSONType: "string", Required: true, Description: "the conversation's handle, as base_apple_messages.chats.chat_identifier holds it for a one-to-one chat (style 45): an E.164 phone (+13105550123; a bare 10-digit number is read as +1), an email address, or a short code (72975). Group chats (chat123...) are refused."},
				},
				Example: map[string]any{
					"type":    AppleMessagesMarkReadOperation,
					"account": "you@example.com",
					"chat":    "72975",
				},
				ExtraNotes: "Messages has no API for read state, so the executor opens the conversation in Messages on the Mac that syncs Apple Messages, waits until chat.db shows no unread incoming message in it, and closes the window. That needs the Mac's screen unlocked: while it is locked the mutation waits as failed_retryable and runs once it is unlocked. " +
					"It marks the whole conversation read, including texts that arrive after review; senders with read receipts on see it as read, and iCloud syncs the read state to Zach's iPhone. " +
					"The review lists what the warehouse shows unread in that conversation now. One mutation per conversation; batch several conversations in one request.",
			},
			{
				Type:        SlackSendMessageOperation,
				Summary:     "Send a Slack message as Zach: to a channel or group, as a DM to one person, or as a reply in a thread.",
				RequiresEnv: "SLACK_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + SlackSendMessageOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Slack account label"},
					{Name: "conversation_id", JSONType: "string", Required: false, Description: "Slack C, D, or G conversation ID to post in (base_slack.conversations.conversation_id). Exactly one of conversation_id or user_id."},
					{Name: "user_id", JSONType: "string", Required: false, Description: "Slack U or W user ID to message directly (base_slack.users.user_id); the executor opens or reuses the DM. Exactly one of conversation_id or user_id."},
					{Name: "text", JSONType: "string", Required: true, Description: "the message, in Slack mrkdwn (write a mention as <@U…>); at most 4000 characters"},
					{Name: "thread_ts", JSONType: "string", Required: false, Description: "exact Slack timestamp of the thread's parent message to reply under (base_slack.messages.message_ts); needs conversation_id"},
					{Name: "reply_broadcast", JSONType: "boolean", Required: false, Description: "with thread_ts: also post the reply to the conversation; defaults to false"},
				},
				Example: map[string]any{
					"type":            SlackSendMessageOperation,
					"account":         "zrl",
					"conversation_id": "C012ABCDEF",
					"thread_ts":       "1593473566.000200",
					"text":            "Thanks — I'll take a look this afternoon.",
				},
				ExtraNotes: "Nothing is sent until a human approves the request; the reviewer can edit the text before approving, so propose the message you would actually send rather than a draft to be fixed. " +
					"Sent as Zach through the Slack web session Zach pastes into `pdw slack publish-session` (never a bot). The executor re-checks the account, the workspace (auth.test against the stored session) and the recipient: a conversation must be synced in base_slack.conversations for that workspace, not archived, and one Zach can post in; a user_id must be a live, non-bot base_slack.users row; a thread_ts must be a synced message in that conversation. " +
					"One approval sends one message: every attempt carries a client_msg_id derived from the mutation id and looks for it in the warehouse and in Slack before posting, so a retry after a timeout cannot post twice. " +
					"Prefer conversation_id when you found the conversation on the timeline; use user_id only for a person you have no DM with yet.",
			},
			{
				Type:        CalendarDeleteEventOperation,
				Summary:     "Delete a Google Calendar event. Use an instance event_id to cancel a single occurrence of a recurring series.",
				RequiresEnv: "CALENDAR_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + CalendarDeleteEventOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Google Calendar account email address"},
					{Name: "calendar_id", JSONType: "string", Required: false, Description: `calendar that owns the event; defaults to "primary"`},
					{Name: "event_id", JSONType: "string", Required: true, Description: "Google Calendar event ID"},
					{Name: "expected_etag", JSONType: "string", Required: false, Description: "etag captured at proposal time; fails fast if the event changed"},
					{Name: "send_updates", JSONType: "string", Required: false, Description: `"all", "externalOnly", or "none"; defaults to "all"`},
				},
				Example: map[string]any{
					"type":         CalendarDeleteEventOperation,
					"account":      "you@example.com",
					"calendar_id":  "primary",
					"event_id":     "abc123",
					"send_updates": "all",
				},
			},
		},
	}
}
