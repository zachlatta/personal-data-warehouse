package mutations

import (
	"context"
	"errors"

	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

const proposeMutationDescription = "Create a pending upstream mutation request for human review in the Personal Data Warehouse. Does not execute the mutation; returns a request_id and approval_url for the web review UI. Mutations is an array — each entry specifies a type (e.g. gmail.send_email) plus that type's payload fields. Call propose_mutation_help first to see the supported types and the exact payload schema for each."

const proposeMutationHelpDescription = "Return the catalog of mutation types supported by propose_mutation, with field-by-field descriptions and a worked example for each. Takes no arguments. Call this before propose_mutation to see how to shape each mutation entry."

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
				var inputErr *proposalInputError
				if errors.As(err, &inputErr) {
					return response, &tool.InvalidInputError{Message: inputErr.Error()}
				}
				return response, err
			},
		},
		&tool.Typed[ProposeMutationHelpInput, MutationHelpDocument]{
			NameStr:        "propose_mutation_help",
			TitleStr:       "Propose Mutation Help",
			DescriptionStr: proposeMutationHelpDescription,
			Handle: func(_ context.Context, _ ProposeMutationHelpInput) (MutationHelpDocument, error) {
				return MutationHelp(), nil
			},
		},
	}
}

type ProposeMutationHelpInput struct{}

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
			"Each entry in mutations is an object whose `type` selects one of the supported operations below; the remaining fields are the payload for that type.",
		Common: []MutationHelpArg{
			{Name: "title", JSONType: "string", Required: true, Description: "short human-readable title for the reviewed request"},
			{Name: "reason", JSONType: "string", Required: true, Description: "why these mutations should be reviewed and approved"},
			{Name: "mutations", JSONType: "array<object>", Required: true, Description: "one or more mutation entries; each must include type plus that type's fields"},
			{Name: "context", JSONType: "object", Required: false, Description: "optional source context for the human reviewer"},
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
				Type:        GmailSendEmailOperation,
				Summary:     "Send or draft a Gmail message. Supports multiple titled variants for human reviewer to pick from.",
				RequiresEnv: "GMAIL_ACCOUNTS",
				Fields: []MutationHelpArg{
					{Name: "type", JSONType: "string", Required: true, Description: "literal " + GmailSendEmailOperation},
					{Name: "account", JSONType: "string", Required: true, Description: "configured Gmail account email address"},
					{Name: "delivery_mode", JSONType: "string", Required: false, Description: `"send" or "draft"; defaults to "send"`},
					{Name: "message", JSONType: "object", Required: true, Description: "base message; fields: to[], cc[], bcc[], subject, body_text, body_html, reply_to_thread_id, in_reply_to, references[]. Must include at least one recipient, a subject, and body_text or body_html."},
					{Name: "variants", JSONType: "array<object>", Required: false, Description: "optional alternate proposals; each needs a two-word `title` for the review tab. Variant fields override the base message; omitted fields inherit from message."},
				},
				Example: map[string]any{
					"type":          GmailSendEmailOperation,
					"account":       "you@example.com",
					"delivery_mode": "send",
					"message": map[string]any{
						"to":        []string{"friend@example.com"},
						"subject":   "Re: lunch",
						"body_text": "Sounds good — see you at 1pm.",
					},
				},
				ExtraNotes: "When variants is non-empty, every variant must have a two-word title (max 32 chars), and each fully-resolved variant must independently pass the same recipient/subject/body validation.",
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
					"The executor requires an xoxc token and d cookie published with `pdw slack publish-session`, verifies the stored user/workspace identity, and refuses a target that is not already synced exactly.",
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
