package applenotes

import (
	"context"
	"io"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

// Spec is the Apple Notes worker: provider apple_notes, its own lock id, and
// update_note as the only reclaimable operation. A create is NOT idempotent
// (replaying it makes a second note), so a stale create is left executing
// for a human to look at rather than silently duplicated.
var Spec = queue.Spec{
	DisplayName: "Apple Notes",
	Command:     "apple-notes",
	Provider:    Provider,
	WorkerName:  "apple_notes_mutation_worker",
	LockID:      LockID,
	EnvPrefix:   "APPLE_NOTES",
	Idempotent:  []queue.Operation{{Provider: Provider, Operation: UpdateNoteOperation}},
	NewExecutor: func(queue.Store) queue.Executor { return NewExecutor(nil, nil) },
}

// Enabled is the APPLE_NOTES_MUTATIONS_ENABLED kill switch.
func Enabled(getenv func(string) string) bool { return Spec.Enabled(getenv) }

// RunApplyOnce applies approved Apple Notes mutations once, for the
// apple-notes ingest uploader to call BEFORE its scan so a note this run
// writes is shipped back in the same cycle. It honours the kill switch and
// never fails the caller: the returned message is for the run log.
func RunApplyOnce(ctx context.Context, getenv func(string) string) (message string, applied bool) {
	return Spec.RunApplyOnce(ctx, getenv)
}

// Run is the `pdw mutations apple-notes` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	return Spec.Run(args, stdin, stdout, stderr, getenv, cfg)
}
