package mutations

import (
	"context"
	"errors"
	"time"
)

const (
	ReviewPath = "/mutation-review"

	GmailArchiveOperation          = "gmail.archive_threads"
	GmailUnarchiveOperation        = "gmail.unarchive_threads"
	GmailSendEmailOperation        = "gmail.send_email"
	GooglePeopleContactsOperation  = "google_people.contacts"
	ContactsBatchMutationOperation = "contacts.batch_mutation"
	CalendarProvider               = "google_calendar"
	CalendarCreateEventOperation   = "calendar.create_event"
	CalendarUpdateEventOperation   = "calendar.update_event"
	CalendarDeleteEventOperation   = "calendar.delete_event"

	defaultRequestedBy = "mcp"
	reviewerActorID    = "web-ui"
)

var ErrNotFound = errors.New("mutation request not found")

type Config struct {
	BaseURL               string
	UIPassword            string
	SessionSecret         string
	SessionTTL            time.Duration
	GmailAccounts         []string
	ContactGoogleAccounts []string
	CalendarAccounts      []string
	Now                   func() time.Time
}

type Store interface {
	CreateRequest(ctx context.Context, input CreateRequestInput) (Request, error)
	ListRequests(ctx context.Context, filter RequestFilter) ([]Request, error)
	GetRequest(ctx context.Context, id string) (Request, error)
	UpdateGmailEmailMutation(ctx context.Context, requestID string, mutationID string, input UpdateGmailEmailMutationInput, actor string) (Mutation, error)
	RemoveMutation(ctx context.Context, requestID string, mutationID string, actor string) (Mutation, error)
	ApproveRequest(ctx context.Context, id string, actor string) (Request, error)
	RejectRequest(ctx context.Context, id string, actor string, reason string) (Request, error)
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
}

type UpdateGmailEmailMutationInput struct {
	DeliveryMode      string
	Message           map[string]any
	SelectedVariantID string
}

type MutationInput struct {
	Type          string
	Account       string
	Title         string
	Reason        string
	ThreadIDs     []string
	DeliveryMode  string
	Message       map[string]any
	EmailVariants []GmailEmailVariantInput
	Operations    []map[string]any
	CalendarID    string
	EventID       string
	ExpectedEtag  string
	SendUpdates   string
	Event         map[string]any
	Patch         map[string]any
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
	Revision       int64
	RequestedBy    string
	ApprovedBy     string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	ApprovedAt     time.Time
	ExecutedAt     time.Time
	ObservedAt     time.Time
	MutationCount  int
	Mutations      []Mutation
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
