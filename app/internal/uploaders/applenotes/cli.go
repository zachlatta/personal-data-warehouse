package applenotes

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	notesmutations "github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/applenotes"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest apple-notes --help`.
const Usage = `pdw ingest apple-notes - upload local Apple Notes revisions.

USAGE
  pdw ingest apple-notes [--mode incremental|full] [--limit N] [--workers N]
                         [--state-file PATH] [--lock-file PATH]
                         [--no-mutations | --mutations-only] [--network-diagnostics]

Ensures Notes.app is running (so iCloud keeps delivering other devices' edits),
snapshots APPLE_NOTES_STORE_PATH (default the group.com.apple.notes NoteStore),
and posts each changed note's HTML body, attachments and revision sidecar to
the app's /ingest/apple-notes/{body,attachment,revision} endpoints, plus a
tombstone revision for every note that vanished.

FLAGS
  --mode MODE            incremental (default; skip unchanged notes) or full.
  --limit N              Maximum changed or deleted notes to upload; 0 (default)
                         is unlimited.
  --workers N            Parallel upload workers; 0 (default) uses
                         APPLE_NOTES_UPLOAD_WORKERS or 4.
  --state-file PATH      Incremental upload state (default: the Application Support
                         state file).
  --lock-file PATH       Nonblocking lock used to avoid overlapping LaunchAgent runs
                         (default: beside the state file).
  --no-mutations         Skip the approved-mutations stage in this run.
  --mutations-only       Run only the approved-mutations stage and skip the upload.
  --network-diagnostics  Print the network guard's verdict and exit.

Approved apple_notes mutations (ops.upstream_mutation_operations) are applied
through Notes.app BEFORE the scan, so a note this run writes is shipped back in
the same cycle. A mutation failure is reported and never fails the upload.

ENVIRONMENT
  APPLE_NOTES_ACCOUNT (else GMAIL_ACCOUNTS[0])   Account the uploads are keyed by.
  APPLE_NOTES_STORE_PATH                         The NoteStore.sqlite to snapshot.
  APPLE_NOTES_OPEN_NOTES_APP=0                   Do not launch Notes.app before the scan.
  APPLE_NOTES_MUTATIONS_ENABLED=0                Disables the mutation stage.
  PDW_INGEST_PROJECT_DIR                         Directory whose .env is loaded
                                                 (default: current directory).
`

const (
	skipMessage           = "Apple Notes upload skipped: another uploader run is active"
	defaultUploadWorkers  = 4
	uploadWorkersVariable = "APPLE_NOTES_UPLOAD_WORKERS"
	networkPolicyPrefix   = "APPLE_NOTES"
)

// Run is the `pdw ingest apple-notes` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest apple-notes")
	mode := fs.String("mode", "incremental", "")
	stateFile := fs.String("state-file", DefaultStateFile(), "")
	lockFile := fs.String("lock-file", common.LockFileFor(DefaultStateFile()), "")
	diagnostics := fs.Bool("network-diagnostics", false, "")
	limit := fs.Int("limit", 0, "")
	noMutations := fs.Bool("no-mutations", false, "")
	mutationsOnly := fs.Bool("mutations-only", false, "")
	workers := fs.Int("workers", 0, "")
	if _, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr); done {
		return code
	}
	if err := common.ValidateMode(*mode); err != nil {
		return common.UsageError(fs, stderr, "%v", err)
	}
	if *limit < 0 {
		return common.UsageError(fs, stderr, "--limit must be greater than or equal to 0")
	}
	if *workers < 0 {
		return common.UsageError(fs, stderr, "--workers must be greater than or equal to 0")
	}

	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest apple-notes: %v\n", err)
		return common.ExitFailure
	}
	if *diagnostics {
		fmt.Fprint(stdout, common.NetworkDiagnostics(common.NetworkPolicyFromEnv(env, networkPolicyPrefix+"_UPLOAD", "VOICE_MEMOS_UPLOAD")))
		return 0
	}
	cfg = cfg.WithEnvFallback(env)
	account := env.AppleNotesAccount()
	if account == "" {
		fmt.Fprintln(stderr, "pdw ingest apple-notes: APPLE_NOTES_ACCOUNT or GMAIL_ACCOUNTS must be set for Apple Notes sync")
		return common.ExitFailure
	}
	storePath := env.Path("APPLE_NOTES_STORE_PATH", DefaultStorePath)
	if *workers == 0 {
		*workers = defaultUploadWorkers
		if raw := env.Env(uploadWorkersVariable); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				fmt.Fprintf(stderr, "pdw ingest apple-notes: %s must be an integer\n", uploadWorkersVariable)
				return common.ExitFailure
			}
			*workers = parsed
		}
	}
	if *workers < 1 {
		*workers = 1
	}
	logger := common.NewWriterLogger(stdout)

	// Before the snapshot, as the Python uploader did: with Notes.app quit
	// the local store silently freezes while runs report healthy.
	EnsureNotesAppRunning(storePath, logger, env, "", nil)

	state := LoadState(*stateFile, account, storePath)
	lock, code, done := common.AcquireRunLock(*lockFile, skipMessage, stdout, stderr)
	if done {
		return code
	}
	defer lock.Release()

	// Mutations run BEFORE the upload stage on purpose: a note this run
	// writes is then picked up by the same run's scan, so an approved edit
	// reaches the warehouse in one cycle instead of waiting five minutes for
	// the next one. The stage never fails the upload: an unreachable
	// warehouse or a refused edit is a line in the run log.
	if !*noMutations {
		if message, applied := notesmutations.RunApplyOnce(context.Background(), env); applied {
			fmt.Fprintln(stdout, message)
		}
	}
	if *mutationsOnly {
		return 0
	}

	client, err := ingestclient.FromEnv(env, cfg, logger)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest apple-notes: %v\n", err)
		return common.ExitFailure
	}
	saveState := func() {
		if err := state.Save(*stateFile); err != nil {
			logger.Warningf("Failed to save Apple Notes upload state to %s: %v", *stateFile, err)
		}
	}
	summary, err := (&Runner{
		Account:           account,
		StorePath:         storePath,
		Client:            client,
		Logger:            logger,
		State:             state,
		Mode:              *mode,
		Limit:             *limit,
		Workers:           *workers,
		SaveState:         saveState,
		BeforeUploadCheck: common.UploadGuard(env, networkPolicyPrefix, cfg.BaseURL),
	}).Sync()
	saveState()
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest apple-notes: %v\n", err)
		return common.ExitFailure
	}
	fmt.Fprintf(stdout, "Apple Notes upload complete: seen=%d selected=%d revisions=%d skipped=%d deferred=%d deleted=%d attachments=%d missing=%d\n",
		summary.NotesSeen, summary.NotesSelected, summary.RevisionsUploaded, summary.NotesSkipped, summary.NotesDeferred, summary.NotesDeleted, summary.AttachmentsUploaded, summary.AttachmentsMissing)
	return 0
}
