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
	Now                   func() time.Time
}

type Store interface {
	CreateRequest(ctx context.Context, input CreateRequestInput) (Request, error)
	ListRequests(ctx context.Context, filter RequestFilter) ([]Request, error)
	GetRequest(ctx context.Context, id string) (Request, error)
	UpdateGmailEmailMutation(ctx context.Context, requestID string, mutationID string, input UpdateGmailEmailMutationInput, actor string) (Mutation, error)
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
	DeliveryMode string
	Message      map[string]any
}

type MutationInput struct {
	Type         string
	Account      string
	Title        string
	Reason       string
	ThreadIDs    []string
	DeliveryMode string
	Message      map[string]any
	Operations   []map[string]any
	Raw          map[string]any
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
