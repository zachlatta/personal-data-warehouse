// Package queue is the Mac-side half of the reviewed-mutation queue: claiming
// approved rows from ops.upstream_mutation_operations for a local-only
// provider (Apple Notes, Apple Contacts), running them through an app-specific
// executor, and writing the outcome back. It is the Go twin of the Python
// personal_data_warehouse_apple_{notes,contacts}.mutation_worker modules and
// of the warehouse methods they called.
package queue

import (
	"context"
	"fmt"
	"time"
)

// Failure statuses fail_upstream_mutation accepts. Anything else is a bug in
// the executor, not a row state.
const (
	StatusSucceeded                 = "succeeded"
	StatusFailedRetryable           = "failed_retryable"
	StatusFailedTerminal            = "failed_terminal"
	StatusBlockedMissingCredentials = "blocked_missing_credentials"
)

// ClaimableStatuses mirrors UPSTREAM_MUTATION_CLAIMABLE_STATUSES.
var ClaimableStatuses = []string{"approved", "failed_retryable"}

// Mutation is one claimed row of ops.upstream_mutation_operations.
type Mutation struct {
	ID           string
	RequestID    string
	Provider     string
	Operation    string
	Account      string
	Status       string
	AttemptCount int64
	Payload      map[string]any
}

// Result is what an executor reports for one mutation.
type Result struct {
	Status     string
	Error      string
	ResultJSON map[string]any
}

// Executor applies one claimed mutation through the local app.
type Executor interface {
	Execute(mutation Mutation) Result
}

// ExecutorFunc adapts a function to Executor.
type ExecutorFunc func(mutation Mutation) Result

// Execute implements Executor.
func (f ExecutorFunc) Execute(mutation Mutation) Result { return f(mutation) }

// Operation names a (provider, operation) pair, used for the reclaim
// allow-list of idempotent operations.
type Operation struct {
	Provider  string
	Operation string
}

// Store is the queue's persistence, behind an interface so the worker can be
// unit-tested with a fake. The real implementation is PostgresStore.
type Store interface {
	EnsureTables(ctx context.Context) error
	// ReclaimStaleExecuting resets `executing` rows claimed before now-staleAfter
	// back to `approved`, for the listed idempotent operations only. Only safe
	// while holding the provider's worker lock.
	ReclaimStaleExecuting(ctx context.Context, staleAfter time.Duration, idempotent []Operation, actorID string) (int, error)
	// ClaimApproved claims up to limit rows for the given providers.
	ClaimApproved(ctx context.Context, limit int, claimedBy string, providers []string) ([]Mutation, error)
	Complete(ctx context.Context, mutationID string, resultJSON map[string]any, actorID string) error
	Fail(ctx context.Context, mutationID string, status string, errorText string, resultJSON map[string]any, actorID string) error
	Close() error
}

// Summary is the outcome of one processing pass.
type Summary struct {
	Claimed                   int
	Succeeded                 int
	FailedRetryable           int
	FailedTerminal            int
	BlockedMissingCredentials int
	SkippedDueToLock          bool
}

// Describe renders the summary the way the Python worker logged it, with the
// provider's display name (e.g. "Apple Notes").
func (s Summary) Describe(displayName string) string {
	if s.SkippedDueToLock {
		return fmt.Sprintf("%s mutations skipped: another worker holds the lock", displayName)
	}
	return fmt.Sprintf("%s mutations: claimed=%d succeeded=%d retryable=%d terminal=%d blocked=%d",
		displayName, s.Claimed, s.Succeeded, s.FailedRetryable, s.FailedTerminal, s.BlockedMissingCredentials)
}

// Epoch is the warehouse-wide "absent" timestamp sentinel.
var Epoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
