package queue

import (
	"context"
	"fmt"
	"io"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultResidentRetrySeconds mirrors DEFAULT_RESIDENT_RETRY_SECONDS.
const DefaultResidentRetrySeconds = 10.0

// Spec describes one local-only provider's worker: everything that differs
// between the Apple Notes and Apple Contacts workers.
type Spec struct {
	// DisplayName is the human name in log lines ("Apple Notes").
	DisplayName string
	// Command is the pdw subcommand name ("apple-notes"), for --help.
	Command string
	// Provider is the ops.upstream_mutation_operations provider value.
	Provider string
	// WorkerName keys the sync lock and the claimed_by actor id
	// ("apple_notes_mutation_worker").
	WorkerName string
	// LockID is the provider's own advisory-lock id.
	LockID int64
	// EnvPrefix is the provider's environment prefix ("APPLE_NOTES").
	EnvPrefix string
	// Idempotent lists the operations a stale `executing` claim may be
	// reclaimed for.
	Idempotent []Operation
	// NewExecutor builds the app executor. It is handed the open ledger store so
	// an executor that needs to ask the ledger a question (Apple Contacts: which
	// card was this one merged into?) can.
	NewExecutor func(store Store) Executor
}

// Enabled is apple_<x>_mutations_enabled: the <PREFIX>_MUTATIONS_ENABLED
// kill switch, on unless set to 0/false/no.
func (s Spec) Enabled(getenv func(string) string) bool {
	value := strings.TrimSpace(getenv(s.EnvPrefix + "_MUTATIONS_ENABLED"))
	switch value {
	case "0", "false", "no":
		return false
	}
	return true
}

// Getenv layers the project .env under the process environment, the way
// load_settings() called load_dotenv(): the environment wins, the file fills
// gaps. The file is <PDW_INGEST_PROJECT_DIR>/.env (default: the current
// directory), exactly as the ingest uploaders resolve it, so the LaunchAgent
// wrapper's `cd "$REPO_DIR"` makes the repo .env visible to both.
func Getenv(getenv func(string) string) func(string) string {
	layered, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		return getenv
	}
	return layered
}

// DatabaseURL is the warehouse URL the worker writes to.
func DatabaseURL(getenv func(string) string) string {
	return NormalizePostgresURL(getenv("POSTGRES_DATABASE_URL"))
}

func (s Spec) batchSize(getenv func(string) string) int {
	if raw := strings.TrimSpace(getenv(s.EnvPrefix + "_MUTATION_BATCH_SIZE")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			return n
		}
	}
	return DefaultBatchSize
}

func (s Spec) floatSeconds(getenv func(string) string, suffix string, fallback float64) time.Duration {
	value := fallback
	if raw := strings.TrimSpace(getenv(s.EnvPrefix + suffix)); raw != "" {
		if parsed, err := strconv.ParseFloat(raw, 64); err == nil {
			value = parsed
		}
	}
	return time.Duration(value * float64(time.Second))
}

func (s Spec) lock(getenv func(string) string) Lock {
	return SyncLock{Name: s.WorkerName, LockID: s.LockID, Getenv: getenv}
}

// ApplyOnce is run_apple_<x>_mutations from the uploader CLI: open the
// warehouse, process one batch under the lock, and describe the outcome. It
// never fails the caller: an unreachable warehouse or a refused edit must not
// stop the upload that follows, so every failure is a message, not an error.
// It does not consult the kill switch; see RunApplyOnce.
func (s Spec) ApplyOnce(ctx context.Context, getenv func(string) string) string {
	getenv = Getenv(getenv)
	store, err := OpenPostgres(ctx, DatabaseURL(getenv), 30*time.Second)
	if err != nil {
		return fmt.Sprintf("%s mutations skipped: warehouse unavailable (%v)", s.DisplayName, err)
	}
	defer store.Close()
	summary, err := Process(ctx, Options{
		Store:        store,
		Executor:     s.NewExecutor(store),
		Lock:         s.lock(getenv),
		Provider:     s.Provider,
		Idempotent:   s.Idempotent,
		Limit:        DefaultBatchSize,
		ClaimedBy:    fmt.Sprintf("mac:%s:%s", Hostname(), s.WorkerName),
		ReclaimAfter: DefaultReclaimAfter,
		EnsureTables: true,
	})
	if err != nil {
		return fmt.Sprintf("%s mutations failed: %v", s.DisplayName, err)
	}
	return summary.Describe(s.DisplayName)
}

// RunApplyOnce is what an ingest uploader calls before its scan: apply
// approved mutations unless the provider's kill switch is off. The returned
// message is meant for the run log; applied is false when the switch is off.
func (s Spec) RunApplyOnce(ctx context.Context, getenv func(string) string) (message string, applied bool) {
	if !s.Enabled(Getenv(getenv)) {
		return fmt.Sprintf("%s mutations disabled (%s_MUTATIONS_ENABLED=0)", s.DisplayName, s.EnvPrefix), false
	}
	return s.ApplyOnce(ctx, getenv), true
}

// Deps are the resident worker's injectable pieces, so tests never open a
// database or drive an app.
type Deps struct {
	OpenStore   func(ctx context.Context, url string) (Store, error)
	NewListener func(ctx context.Context, url string) (Listener, error)
	NewExecutor func(store Store) Executor
	Lock        Lock
	Hostname    string
}

func (s Spec) defaultDeps(getenv func(string) string) Deps {
	return Deps{
		OpenStore: func(ctx context.Context, url string) (Store, error) {
			return OpenPostgres(ctx, url, 30*time.Second)
		},
		NewListener: func(ctx context.Context, url string) (Listener, error) {
			return NewPostgresListener(ctx, url)
		},
		NewExecutor: s.NewExecutor,
		Lock:        s.lock(getenv),
		Hostname:    Hostname(),
	}
}

// RunResident is run_resident_apple_<x>_worker: connect, LISTEN, drain, and
// reconnect after every failure until ctx is cancelled.
func (s Spec) RunResident(ctx context.Context, getenv func(string) string, deps Deps, logger common.Logger) {
	pollInterval := s.floatSeconds(getenv, "_MUTATION_POLL_SECONDS", DefaultQueuePollSeconds)
	retryInterval := s.floatSeconds(getenv, "_MUTATION_RETRY_SECONDS", DefaultResidentRetrySeconds)
	claimedBy := fmt.Sprintf("mac-resident:%s:%s", deps.Hostname, s.WorkerName)
	for ctx.Err() == nil {
		err := s.residentSession(ctx, getenv, deps, logger, claimedBy, pollInterval)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			// The loop only returns cleanly on cancellation; a nil error with
			// a live context is a listener that closed under us, so reconnect
			// on the same schedule rather than spinning.
			err = fmt.Errorf("notification loop ended")
		}
		logger.Warningf("%s resident mutation worker failed; reconnecting: %v", s.DisplayName, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(retryInterval):
		}
	}
}

func (s Spec) residentSession(ctx context.Context, getenv func(string) string, deps Deps, logger common.Logger, claimedBy string, pollInterval time.Duration) error {
	url := DatabaseURL(getenv)
	if url == "" {
		return fmt.Errorf("POSTGRES_DATABASE_URL is required by the %s mutation worker", s.DisplayName)
	}
	store, err := deps.OpenStore(ctx, url)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.EnsureTables(ctx); err != nil {
		return err
	}
	executor := deps.NewExecutor(store)
	listener, err := deps.NewListener(ctx, url)
	if err != nil {
		return err
	}
	processPending := func() (bool, error) {
		summary, err := Process(ctx, Options{
			Store:        store,
			Executor:     executor,
			Lock:         deps.Lock,
			Provider:     s.Provider,
			Idempotent:   s.Idempotent,
			Limit:        s.batchSize(getenv),
			ClaimedBy:    claimedBy,
			ReclaimAfter: DefaultReclaimAfter,
			EnsureTables: false,
		})
		if err != nil {
			return false, err
		}
		if summary.Claimed > 0 {
			logger.Infof("%s", summary.Describe(s.DisplayName))
		}
		return summary.Claimed > 0, nil
	}
	return RunNotificationLoop(ctx, listener, processPending, pollInterval)
}

// Run is the `pdw mutations <command>` entry point. `--once` applies one
// batch and exits (what the uploader wrappers do); without it the process is
// the resident worker, stopping on SIGTERM/SIGINT. cfg is accepted for parity
// with the other local commands; the worker talks to Postgres, not the app.
func (s Spec) Run(args []string, _ io.Reader, stdout, stderr io.Writer, getenv func(string) string, _ ingestclient.Config) int {
	return s.RunWithDeps(args, stdout, stderr, getenv, nil)
}

// RunWithDeps is Run with injectable dependencies (nil = production).
func (s Spec) RunWithDeps(args []string, stdout, stderr io.Writer, getenv func(string) string, deps *Deps) int {
	once := false
	for _, arg := range args {
		switch arg {
		case "--once":
			once = true
		case "-h", "--help":
			s.usage(stdout)
			return 0
		default:
			fmt.Fprintf(stderr, "pdw mutations %s: unknown flag %q\n\n", s.Command, arg)
			s.usage(stderr)
			return 2
		}
	}
	getenv = Getenv(getenv)
	logger := common.NewWriterLogger(stdout)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if once {
		message, _ := s.RunApplyOnce(ctx, getenv)
		fmt.Fprintln(stdout, message)
		if strings.Contains(message, " failed: ") || strings.Contains(message, "warehouse unavailable") {
			return 1
		}
		return 0
	}
	if DatabaseURL(getenv) == "" {
		fmt.Fprintf(stderr, "POSTGRES_DATABASE_URL is required by the %s mutation worker\n", s.DisplayName)
		return 2
	}
	resolved := s.defaultDeps(getenv)
	if deps != nil {
		resolved = *deps
	}
	logger.Infof("starting resident %s mutation worker", s.DisplayName)
	s.RunResident(ctx, getenv, resolved, logger)
	return 0
}

func (s Spec) usage(out io.Writer) {
	fmt.Fprintf(out, `Usage: pdw mutations %s [--once]

Apply approved %s mutations from ops.upstream_mutation_operations through
the local app over AppleScript. Without flags the process stays resident:
it LISTENs for approvals, drains the queue, and polls every
%s_MUTATION_POLL_SECONDS (default %.0f) seconds until SIGTERM/SIGINT.

Flags:
  --once      apply one batch and exit (what the ingest uploaders do first)
  -h, --help  show this help

Environment:
  POSTGRES_DATABASE_URL                 the warehouse (also read from PDW_INGEST_PROJECT_DIR/.env, default ./.env)
  %s_MUTATIONS_ENABLED           0/false/no pauses --once (default on)
  %s_MUTATION_BATCH_SIZE         rows per pass (default %d)
  %s_MUTATION_POLL_SECONDS       resident fallback poll (default %.0f)
  %s_MUTATION_RETRY_SECONDS      resident reconnect delay (default %.0f)
  %s_SYNC_LOCK_POSTGRES_URL      advisory-lock database (else DAGSTER_POSTGRES_URL, DATABASE_URL, else a flock at %s_SYNC_LOCK_PATH)
`, s.Command, s.DisplayName, s.EnvPrefix, DefaultQueuePollSeconds,
		s.EnvPrefix, s.EnvPrefix, DefaultBatchSize, s.EnvPrefix, DefaultQueuePollSeconds, s.EnvPrefix, DefaultResidentRetrySeconds,
		LockEnvPrefix(s.WorkerName), LockEnvPrefix(s.WorkerName))
}
