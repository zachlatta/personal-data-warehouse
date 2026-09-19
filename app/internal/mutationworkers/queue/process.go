package queue

import (
	"context"
	"fmt"
	"os"
	"time"
)

// DefaultBatchSize and DefaultReclaimAfter mirror the Python worker
// constants: writes are serialized through one app on one Mac, so a stuck
// claim should return to the queue quickly.
const (
	DefaultBatchSize    = 25
	DefaultReclaimAfter = 600 * time.Second
)

// Options configures one processing pass.
type Options struct {
	Store        Store
	Executor     Executor
	Lock         Lock
	Provider     string
	Idempotent   []Operation
	Limit        int
	ClaimedBy    string
	ReclaimAfter time.Duration
	EnsureTables bool
}

// Process is process_apple_{notes,contacts}_mutations: under the worker lock,
// reclaim stale idempotent claims, claim a batch for the provider, execute
// each row, and record the outcome. A lost lock is reported as a skip rather
// than an empty success.
func Process(ctx context.Context, opts Options) (Summary, error) {
	if opts.Store == nil || opts.Executor == nil || opts.Lock == nil {
		return Summary{}, fmt.Errorf("queue.Process needs a store, an executor and a lock")
	}
	if opts.Limit <= 0 {
		opts.Limit = DefaultBatchSize
	}
	if opts.ReclaimAfter <= 0 {
		opts.ReclaimAfter = DefaultReclaimAfter
	}
	release, acquired, err := opts.Lock.Acquire(ctx)
	if err != nil {
		return Summary{}, err
	}
	if !acquired {
		return Summary{SkippedDueToLock: true}, nil
	}
	defer release()

	if opts.EnsureTables {
		if err := opts.Store.EnsureTables(ctx); err != nil {
			return Summary{}, err
		}
	}
	if _, err := opts.Store.ReclaimStaleExecuting(ctx, opts.ReclaimAfter, opts.Idempotent, opts.ClaimedBy); err != nil {
		return Summary{}, err
	}
	claimed, err := opts.Store.ClaimApproved(ctx, opts.Limit, opts.ClaimedBy, []string{opts.Provider})
	if err != nil {
		return Summary{}, err
	}
	summary := Summary{Claimed: len(claimed)}
	for _, mutation := range claimed {
		result := opts.Executor.Execute(mutation)
		if result.Status == StatusSucceeded {
			if err := opts.Store.Complete(ctx, mutation.ID, result.ResultJSON, opts.ClaimedBy); err != nil {
				return summary, err
			}
			summary.Succeeded++
			continue
		}
		if err := opts.Store.Fail(ctx, mutation.ID, result.Status, result.Error, result.ResultJSON, opts.ClaimedBy); err != nil {
			return summary, err
		}
		switch result.Status {
		case StatusFailedRetryable:
			summary.FailedRetryable++
		case StatusFailedTerminal:
			summary.FailedTerminal++
		case StatusBlockedMissingCredentials:
			summary.BlockedMissingCredentials++
		}
	}
	return summary, nil
}

// Hostname is socket.gethostname() for the claimed_by actor id.
func Hostname() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return "unknown"
	}
	return name
}
