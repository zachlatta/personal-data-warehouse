package mutations

import (
	"context"
	"errors"
	"time"
)

const (
	ReviewPath = "/mutation-review"

	GmailArchiveOperation               = "gmail.archive_threads"
	GmailUnarchiveOperation             = "gmail.unarchive_threads"
	GmailModifyThreadLabelsOperation    = "gmail.modify_thread_labels"
	GmailSendEmailOperation             = "gmail.send_email"
	GooglePeopleContactsOperation       = "google_people.contacts"
	ContactsBatchMutationOperation      = "contacts.batch_mutation"
	CalendarProvider                    = "google_calendar"
	CalendarCreateEventOperation        = "calendar.create_event"
	CalendarUpdateEventOperation        = "calendar.update_event"
	CalendarDeleteEventOperation        = "calendar.delete_event"
	AppleNotesProvider                  = "apple_notes"
	AppleNotesCreateNoteOperation       = "apple_notes.create_note"
	AppleNotesUpdateNoteOperation       = "apple_notes.update_note"
	AppleContactsProvider               = "apple_contacts"
	AppleContactsCreateContactOperation = "apple_contacts.create_contact"
	AppleContactsUpdateContactOperation = "apple_contacts.update_contact"
	AppleContactsMergeContactsOperation = "apple_contacts.merge_contacts"
	SlackProvider                       = "slack"
	SlackMarkConversationReadOperation  = "slack.mark_conversation_read"

	defaultRequestedBy = "mcp"
	// reviewerActorID is the actor recorded when a reviewer surface passes
	// none; the JSON API always names its client (app:<name>).
	reviewerActorID = "web-ui"
)

var ErrNotFound = errors.New("mutation request not found")

type Config struct {
	BaseURL               string
	GmailAccounts         []string
	ContactGoogleAccounts []string
	CalendarAccounts      []string
	AppleNotesAccounts    []string
	AppleContactsAccounts []string
	SlackAccounts         []string
	Now                   func() time.Time
	// RequestCreated is called after a request lands in pending_review — the
	// hook the push notifier hangs off. It must not block: the proposal has
	// already been stored, and a slow provider must not slow the proposer.
	RequestCreated func(context.Context, Request)
	// RequestWithdrawn is called after an agent withdraws a pending request,
	// directly or by proposing its replacement, so the alert that asked for a
	// review of it can be replaced on the phone. Same contract as
	// RequestCreated: already stored, must not block.
	RequestWithdrawn func(context.Context, Request)
}

type Store interface {
	CreateRequest(ctx context.Context, input CreateRequestInput) (Request, error)
	ListRequests(ctx context.Context, filter RequestFilter) ([]Request, error)
	GetRequest(ctx context.Context, id string) (Request, error)
	UpdateGmailEmailMutation(ctx context.Context, requestID string, mutationID string, input UpdateGmailEmailMutationInput, actor string) (Mutation, error)
	RemoveMutation(ctx context.Context, requestID string, mutationID string, actor string) (Mutation, error)
	ApproveRequest(ctx context.Context, id string, actor string) (Request, error)
	RejectRequest(ctx context.Context, id string, actor string, reason string) (Request, error)
	SupersedeRequest(ctx context.Context, id string, supersededBy string, actor string) (Request, error)
	WithdrawRequest(ctx context.Context, id string, input WithdrawInput) (Request, error)
}

type RequestFilter struct {
	Statuses []string
	Limit    int
}

type CreateRequestInput struct {
	Title       string
	Reason      string
	Context     map[string]any
	Mutations   []MutationInput
	RequestedBy string
	// Replaces names the earlier request this proposal stands in for. The
	// store closes that request out in the same transaction — withdrawing it
	// if it is still pending, linking it if it is dead — or refuses the whole
	// proposal when it has already been approved or has run.
	Replaces *RequestReplacement
}

// RequestReplacement is the agent's statement about the request a proposal
// replaces: which one, why, and (when a reviewer has edited it) the revision
// the agent read.
type RequestReplacement struct {
	RequestID        string
	ExpectedRevision int64
	Reason           string
}

// WithdrawInput is an agent taking a pending request back. Reason is
// required; ReplacedBy optionally links the request that stands in for it;
// ExpectedRevision (0 = not stated) is required once a reviewer has changed
// the request, and must match.
type WithdrawInput struct {
	Reason           string
	ReplacedBy       string
	ExpectedRevision int64
	Actor            string
}

type UpdateGmailEmailMutationInput struct {
	DeliveryMode      string
	Message           map[string]any
	SelectedVariantID string
}

type MutationInput struct {
	Type               string
	Account            string
	Title              string
	Reason             string
	ThreadIDs          []string
	AddLabels          []string
	CreateAndAddLabels []string
	RemoveLabels       []string
	DeliveryMode       string
	Message            map[string]any
	EmailVariants      []GmailEmailVariantInput
	Operations         []map[string]any
	CalendarID         string
	EventID            string
	ExpectedEtag       string
	SendUpdates        string
	Event              map[string]any
	Patch              map[string]any
	Folder             string
	NoteID             string
	Name               string
	Body               string
	AppendBody         string
	CardID             string
	KeepCardID         string
	MergeCardIDs       []string
	Contact            map[string]any
	Remove             map[string]any
	ConversationID     string
	MessageTS          string
}

type GmailEmailVariantInput struct {
	Title           string         `json:"title" jsonschema:"short two-word title shown on the review tab, like Direct Reply or Softer Ask"`
	Message         map[string]any `json:"message,omitempty" jsonschema:"optional full email message object for this variant"`
	To              []string       `json:"to,omitempty" jsonschema:"primary recipients for this variant; inherits top-level recipients when omitted"`
	CC              []string       `json:"cc,omitempty" jsonschema:"carbon-copy recipients for this variant; inherits top-level cc when omitted"`
	BCC             []string       `json:"bcc,omitempty" jsonschema:"blind-copy recipients for this variant; inherits top-level bcc when omitted"`
	Subject         string         `json:"subject,omitempty" jsonschema:"subject for this variant; inherits top-level subject when omitted"`
	BodyText        string         `json:"body_text,omitempty" jsonschema:"plain-text body for this variant"`
	BodyHTML        string         `json:"body_html,omitempty" jsonschema:"HTML body for this variant"`
	ReplyToThreadID string         `json:"reply_to_thread_id,omitempty" jsonschema:"Gmail thread ID to reply in; inherits top-level reply thread when omitted"`
	InReplyTo       string         `json:"in_reply_to,omitempty" jsonschema:"optional RFC822 message id this variant replies to"`
	References      []string       `json:"references,omitempty" jsonschema:"optional RFC822 References header values"`
}

type Request struct {
	ID             string
	Status         string
	Title          string
	Reason         string
	Context        map[string]any
	Result         map[string]any
	Error          string
	IdempotencyKey string
	SupersededBy   string
	// ReplacesRequestID is the earlier request this one was proposed to
	// replace (the reverse of SupersededBy on that request).
	ReplacesRequestID string
	Revision          int64
	RequestedBy       string
	ApprovedBy        string
	// WithdrawnBy is the agent identity that withdrew the request; the
	// reason is in Error, exactly where a reviewer's denial reason lives.
	WithdrawnBy   string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	ApprovedAt    time.Time
	ExecutedAt    time.Time
	ObservedAt    time.Time
	WithdrawnAt   time.Time
	MutationCount int
	Mutations     []Mutation
}

type Mutation struct {
	ID             string
	RequestID      string
	RequestIndex   int64
	Provider       string
	Operation      string
	Account        string
	Status         string
	Title          string
	Reason         string
	Payload        map[string]any
	Preview        map[string]any
	Result         map[string]any
	Error          string
	IdempotencyKey string
	Revision       int64
	AttemptCount   int64
	RequestedBy    string
	ApprovedBy     string
	ClaimedBy      string
	ClaimedAt      time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	ApprovedAt     time.Time
	ExecutedAt     time.Time
	ObservedAt     time.Time
}
