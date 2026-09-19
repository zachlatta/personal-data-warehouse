package manualfinance

import (
	"fmt"
	"io"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest manual-finance --help`.
const Usage = `pdw ingest manual-finance - upload finance documents (statements, valuations, exports).

USAGE
  pdw ingest manual-finance [flags] <files-or-directories>...

Folder organization is preserved: each file's path relative to the upload root
becomes its original_path hint and its top folder the object key's account
segment, so keep one folder per account (<institution>-<name>-<mask>/). Files
post to /ingest/manual-finance/file, then their envelope to
/ingest/manual-finance/metadata; content-sha dedup makes re-runs cheap.

FLAGS
  --evidence-only     Archive and extract searchable evidence (tax returns,
                      payroll, supporting documents) without booking it into the
                      finance ledger; folders need not identify accounts.
  --root DIR          Base directory original_path is computed against (default:
                      each directory argument, or a bare file's parent).
  --limit N           Maximum files to upload this run; 0 (default) is no limit.
  --mode MODE         incremental (default; skip uploaded content) or full.
  --state-file PATH   Incremental upload state (default: the Application Support
                      state database).
  --lock-file PATH    Nonblocking lock used to avoid overlapping runs
                      (default: beside the state file).

ENVIRONMENT
  MANUAL_FINANCE_ACCOUNT (else GMAIL_ACCOUNTS[0])   Account the uploads are keyed by.
  PDW_INGEST_PROJECT_DIR                            Directory whose .env is loaded
                                                    (default: current directory).
`

const skipMessage = "Manual finance upload skipped: another uploader run is active"

// Run is the `pdw ingest manual-finance` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest manual-finance")
	evidenceOnly := fs.Bool("evidence-only", false, "")
	root := fs.String("root", "", "")
	limit := fs.Int("limit", 0, "")
	mode := fs.String("mode", "incremental", "")
	stateFile := fs.String("state-file", DefaultStateFile(), "")
	lockFile := fs.String("lock-file", common.LockFileFor(DefaultStateFile()), "")
	paths, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr)
	if done {
		return code
	}
	if len(paths) == 0 {
		return common.UsageError(fs, stderr, "the following arguments are required: paths")
	}
	if err := common.ValidateMode(*mode); err != nil {
		return common.UsageError(fs, stderr, "%v", err)
	}
	if *limit < 0 {
		return common.UsageError(fs, stderr, "--limit must be greater than or equal to 0")
	}
	for i, path := range paths {
		paths[i] = common.ExpandUser(path)
	}

	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest manual-finance: %v\n", err)
		return common.ExitFailure
	}
	cfg = cfg.WithEnvFallback(env)
	account := env.ManualFinanceAccount()
	if account == "" {
		fmt.Fprintln(stderr, "pdw ingest manual-finance: MANUAL_FINANCE_ACCOUNT or GMAIL_ACCOUNTS must be set for manual finance uploads")
		return common.ExitFailure
	}
	logger := common.NewWriterLogger(stdout)

	lock, code, done := common.AcquireRunLock(*lockFile, skipMessage, stdout, stderr)
	if done {
		return code
	}
	defer lock.Release()

	state, err := OpenState(*stateFile, account)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest manual-finance: %v\n", err)
		return common.ExitFailure
	}
	defer state.Close()

	client, err := ingestclient.FromEnv(env, cfg, logger)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest manual-finance: %v\n", err)
		return common.ExitFailure
	}
	summary, err := (&Runner{
		Account:      account,
		Paths:        paths,
		Root:         common.ExpandUser(*root),
		Client:       client,
		Logger:       logger,
		Limit:        *limit,
		Mode:         *mode,
		State:        state,
		EvidenceOnly: *evidenceOnly,
	}).Sync()
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest manual-finance: %v\n", err)
		return common.ExitFailure
	}
	fmt.Fprintf(stdout, "Manual finance upload complete: seen=%d ignored=%d selected=%d uploaded=%d skipped=%d\n",
		summary.FilesSeen, summary.FilesIgnored, summary.FilesSelected, summary.FilesUploaded, summary.FilesSkipped)
	return 0
}
