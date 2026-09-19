package applecontacts

import (
	"context"
	"io"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/applescript"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

// Spec is the Apple Contacts worker. Only update_contact is reclaimed from a
// stale `executing` claim: a replayed create duplicates a card and a
// replayed merge tries to delete cards that are already gone.
var Spec = queue.Spec{
	DisplayName: "Apple Contacts",
	Command:     "apple-contacts",
	Provider:    Provider,
	WorkerName:  "apple_contacts_mutation_worker",
	LockID:      LockID,
	EnvPrefix:   "APPLE_CONTACTS",
	Idempotent:  []queue.Operation{{Provider: Provider, Operation: UpdateContactOperation}},
	NewExecutor: func(store queue.Store) queue.Executor { return newLedgerExecutor(nil, store) },
}

// mergedCardLedger is the slice of the ledger store the executor needs:
// queue.PostgresStore.AppleContactsMergedCardTarget.
type mergedCardLedger interface {
	AppleContactsMergedCardTarget(ctx context.Context, cardID string) (string, error)
}

// newLedgerExecutor builds the worker's executor with its merged-card resolver
// backed by the mutation ledger, so an update whose card an earlier merge
// deleted lands on the surviving card instead of dying failed_terminal.
func newLedgerExecutor(runner applescript.Runner, store queue.Store) *Executor {
	executor := NewExecutor(runner)
	if ledger, ok := store.(mergedCardLedger); ok {
		executor.WithMergedInto(func(cardID string) (string, error) {
			return ledger.AppleContactsMergedCardTarget(context.Background(), cardID)
		})
	}
	return executor
}

// Enabled is the APPLE_CONTACTS_MUTATIONS_ENABLED kill switch.
func Enabled(getenv func(string) string) bool { return Spec.Enabled(getenv) }

// RunApplyOnce applies approved Apple Contacts mutations once, for the
// apple-contacts ingest uploader to call BEFORE its scan. It honours the kill
// switch and never fails the caller.
func RunApplyOnce(ctx context.Context, getenv func(string) string) (message string, applied bool) {
	return Spec.RunApplyOnce(ctx, getenv)
}

// Run is the `pdw mutations apple-contacts` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	return Spec.Run(args, stdin, stdout, stderr, getenv, cfg)
}
