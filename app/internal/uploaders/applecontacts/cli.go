package applecontacts

import (
	"context"
	"fmt"
	"io"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	contactsmutations "github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/applecontacts"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest apple-contacts --help`.
const Usage = `pdw ingest apple-contacts - upload local Apple/iCloud Contacts.

USAGE
  pdw ingest apple-contacts [--mode incremental|full] [--limit N]
                            [--state-file PATH] [--lock-file PATH]
                            [--no-mutations | --mutations-only]

Snapshots every AddressBook-v22.abcddb under APPLE_CONTACTS_STORE_PATH
(default ~/Library/Application Support/AddressBook), including account/iCloud
stores, and posts changed cards and tombstones as one gzipped batch to the
app's /ingest/apple-contacts/batch endpoint.

FLAGS
  --mode MODE         incremental (default; skip unchanged cards) or full.
  --limit N           Maximum changed contacts to upload; 0 (default) is unlimited.
  --state-file PATH   Incremental upload state (default: the Application Support
                      state database).
  --lock-file PATH    Nonblocking lock used to avoid overlapping LaunchAgent runs
                      (default: beside the state file).
  --no-mutations      Skip the approved-mutations stage in this run.
  --mutations-only    Run only the approved-mutations stage and skip the upload.

Approved apple_contacts mutations (ops.upstream_mutation_operations) are
applied through Contacts.app BEFORE the scan, so a card this run writes is
shipped back in the same cycle; the resident apple-contacts-mutation-worker
LaunchAgent applies them between runs. A mutation failure is reported and
never fails the upload.

ENVIRONMENT
  APPLE_CONTACTS_ACCOUNT (else GMAIL_ACCOUNTS[0])   Account the uploads are keyed by.
  APPLE_CONTACTS_STORE_PATH                         Address Book root.
  APPLE_CONTACTS_MUTATIONS_ENABLED=0                Disables the mutation stage.
  PDW_INGEST_PROJECT_DIR                            Directory whose .env is loaded
                                                    (default: current directory).
`

const skipMessage = "Apple Contacts upload skipped: another uploader run is active"

// Run is the `pdw ingest apple-contacts` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest apple-contacts")
	mode := fs.String("mode", "incremental", "")
	stateFile := fs.String("state-file", DefaultStateFile(), "")
	lockFile := fs.String("lock-file", common.LockFileFor(DefaultStateFile()), "")
	limit := fs.Int("limit", 0, "")
	noMutations := fs.Bool("no-mutations", false, "")
	mutationsOnly := fs.Bool("mutations-only", false, "")
	if _, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr); done {
		return code
	}
	if err := common.ValidateMode(*mode); err != nil {
		return common.UsageError(fs, stderr, "%v", err)
	}
	if *limit < 0 {
		return common.UsageError(fs, stderr, "--limit must be greater than or equal to 0")
	}

	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest apple-contacts: %v\n", err)
		return common.ExitFailure
	}
	cfg = cfg.WithEnvFallback(env)
	account := env.AppleContactsAccount()
	if account == "" {
		fmt.Fprintln(stderr, "pdw ingest apple-contacts: APPLE_CONTACTS_ACCOUNT or GMAIL_ACCOUNTS must be set for Apple Contacts sync")
		return common.ExitFailure
	}
	storePath := env.Path("APPLE_CONTACTS_STORE_PATH", DefaultStorePath)
	logger := common.NewWriterLogger(stdout)

	lock, code, done := common.AcquireRunLock(*lockFile, skipMessage, stdout, stderr)
	if done {
		return code
	}
	defer lock.Release()

	// Mutations run BEFORE the upload stage on purpose: a card this run
	// writes is then picked up by the same run's scan, so an approved edit
	// reaches the warehouse in one cycle instead of waiting five minutes for
	// the next one. The stage never fails the upload: an unreachable
	// warehouse or a refused edit is a line in the run log.
	if !*noMutations {
		if message, applied := contactsmutations.RunApplyOnce(context.Background(), env); applied {
			fmt.Fprintln(stdout, message)
		}
	}
	if *mutationsOnly {
		return 0
	}

	state, err := OpenState(*stateFile, account, storePath)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest apple-contacts: %v\n", err)
		return common.ExitFailure
	}
	defer state.Close()

	client, err := ingestclient.FromEnv(env, cfg, logger)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest apple-contacts: %v\n", err)
		return common.ExitFailure
	}
	summary, err := (&Runner{
		Account:           account,
		StorePath:         storePath,
		Client:            client,
		Logger:            logger,
		State:             state,
		Mode:              *mode,
		Limit:             *limit,
		BeforeUploadCheck: common.UploadGuard(env, "APPLE_CONTACTS", cfg.BaseURL),
	}).Sync()
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest apple-contacts: %v\n", err)
		return common.ExitFailure
	}
	fmt.Fprintf(stdout, "Apple Contacts upload complete: seen=%d selected=%d skipped=%d deleted=%d deferred=%d batches=%d\n",
		summary.ContactsSeen, summary.ContactsSelected, summary.ContactsSkipped, summary.ContactsDeleted, summary.ContactsDeferred, summary.BatchesUploaded)
	return 0
}
