package applemessages

import (
	"io"
	"os"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/applescript"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

// Spec is the Apple Messages worker. Nothing is reclaimed from a stale
// `executing` claim: a send may be mid-flight in Messages, and the chat.db
// check cannot see a message that has not been written yet, so a stuck send
// waits for a human exactly like gmail.send_email.
var Spec = queue.Spec{
	DisplayName: "Apple Messages",
	Command:     "apple-messages",
	Provider:    Provider,
	WorkerName:  "apple_messages_mutation_worker",
	LockID:      LockID,
	EnvPrefix:   "APPLE_MESSAGES",
	Idempotent:  nil,
	// The allowlist is read through the same layered environment as the rest of
	// the worker, so it can live in the repo .env the LaunchAgent cds into.
	NewExecutor: func(queue.Store) queue.Executor { return newWorkerExecutor(nil, nil, queue.Getenv(os.Getenv)) },
}

func newWorkerExecutor(runner applescript.Runner, store Store, getenv func(string) string) *Executor {
	return NewExecutor(runner, store).
		WithReadiness(func() error { return applescript.Launch(MessagesBundleID) }).
		WithAllowedRecipients(AllowedRecipientsFromEnv(getenv))
}

// Enabled is the APPLE_MESSAGES_MUTATIONS_ENABLED kill switch.
func Enabled(getenv func(string) string) bool { return Spec.Enabled(getenv) }

// Run is the `pdw mutations apple-messages` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	return Spec.Run(args, stdin, stdout, stderr, getenv, cfg)
}
