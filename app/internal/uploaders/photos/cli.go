package photos

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultLibraryPath is APPLE_PHOTOS_LIBRARY_PATH's default.
const DefaultLibraryPath = "~/Pictures/Photos Library.photoslibrary"

// AuthorizeTimeout bounds the one-time `--authorize` prompt.
const AuthorizeTimeout = 300 * time.Second

// Hooks let the CLI tests substitute the pieces that need macOS, Photos, a
// TCC grant or a network: the exporter and the network guard.
var (
	newExporter = func(timeout time.Duration) *PhotoKitExporter {
		return &PhotoKitExporter{Timeout: timeout}
	}
	// runnerExporterOverride replaces the runner's PhotoKit exporter (tests).
	runnerExporterOverride Exporter
	newBeforeUploadCheck   = func(getenv common.Getenv, baseURL string) func() string {
		policy := common.NetworkPolicyFromEnv(getenv, "VOICE_MEMOS_UPLOAD", "")
		return common.BeforeUploadCheck(policy, baseURL, common.PreflightTimeout(getenv, "PHOTOS"))
	}
)

// Usage is the `--help` text (printed to stdout, exit 0, like argparse).
const Usage = `usage: pdw ingest apple-photos [--limit N] [--mode incremental|full] [--state-file PATH] [--lock-file PATH]
                               [--library-path PATH] [--authorize] [--retry-failed] [--network-diagnostics]

Download and upload full Apple Photos originals through the app ingest API.

FLAGS
  --limit N                 Maximum files to upload this run; 0 means no limit (default 0).
  --mode incremental|full   Upload mode (default incremental).
  --state-file PATH         Incremental upload state path
                            (default ~/Library/Application Support/personal-data-warehouse/photos-upload-state.sqlite).
  --lock-file PATH          Nonblocking lock path used to avoid overlapping scheduled runs
                            (default: the state file with a .lock suffix).
  --library-path PATH       Apple Photos library (.photoslibrary) to scan
                            (default $APPLE_PHOTOS_LIBRARY_PATH, else ~/Pictures/Photos Library.photoslibrary).
  --authorize               Request one-time macOS Photos library access and exit.
  --retry-failed            Clear the retry backoff on previously failed files so they are attempted this run.
  --network-diagnostics     Print network guard diagnostics and exit.

ENVIRONMENT
  PHOTOS_ACCOUNT (else APPLE_MESSAGES_ACCOUNT, APPLE_NOTES_ACCOUNT, VOICE_MEMOS_ACCOUNT, GMAIL_ACCOUNTS[0])
                                          Account the uploads are keyed by.
  APPLE_PHOTOS_LIBRARY_PATH               The .photoslibrary to scan.
  PHOTOS_UPLOAD_PREFLIGHT_TIMEOUT_SECONDS App reachability preflight timeout (default 5).
  VOICE_MEMOS_UPLOAD_*                    Network guard settings (shared with every uploader).
  PDW_INGEST_PROJECT_DIR                  Directory whose .env is loaded (default: current directory).
`

// Run is `pdw ingest apple-photos`: parse the uploader's flags, resolve its
// settings the way personal_data_warehouse.config.load_settings did (the
// project .env, then the environment), take the nonblocking run lock, and
// run one upload pass. It returns the process exit code: 0 on success or a
// deliberate skip, 1 on a failed run, 2 on a usage error.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	_ = stdin
	fs := common.NewFlagSet("pdw ingest apple-photos")
	limit := fs.Int("limit", 0, "")
	mode := fs.String("mode", "incremental", "")
	stateFile := fs.String("state-file", DefaultStateFile(), "")
	lockFile := fs.String("lock-file", common.LockFileFor(DefaultStateFile()), "")
	libraryPath := fs.String("library-path", "", "")
	authorize := fs.Bool("authorize", false, "")
	retryFailed := fs.Bool("retry-failed", false, "")
	networkDiagnostics := fs.Bool("network-diagnostics", false, "")
	positionals, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr)
	if done {
		return code
	}
	if len(positionals) > 0 {
		return common.UsageError(fs, stderr, "unrecognized arguments: %s", strings.Join(positionals, " "))
	}
	if err := common.ValidateMode(*mode); err != nil {
		return common.UsageError(fs, stderr, "%v", err)
	}
	if *limit < 0 {
		return common.UsageError(fs, stderr, "--limit must be greater than or equal to 0")
	}

	if *authorize {
		status, err := newExporter(AuthorizeTimeout).RequestAuthorization()
		if err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return common.ExitFailure
		}
		if status != PhotosAuthorizedStatus {
			fmt.Fprintln(stderr, "error: Full Photos library access was not granted. Allow Full Access in System Settings → Privacy & Security → Photos.")
			return common.ExitFailure
		}
		fmt.Fprintln(stdout, "Photos library access granted")
		return 0
	}

	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest apple-photos: %v\n", err)
		return common.ExitFailure
	}
	if *networkDiagnostics {
		fmt.Fprint(stdout, common.NetworkDiagnostics(common.NetworkPolicyFromEnv(env, "VOICE_MEMOS_UPLOAD", "")))
		return 0
	}

	account := env.PhotosAccount()
	if account == "" {
		fmt.Fprintln(stderr, "error: PHOTOS_ACCOUNT or GMAIL_ACCOUNTS must be set for photo sync")
		return common.ExitFailure
	}
	library := env.Path("APPLE_PHOTOS_LIBRARY_PATH", DefaultLibraryPath)
	if strings.TrimSpace(*libraryPath) != "" {
		library = common.ExpandUser(*libraryPath)
	}
	cfg = cfg.WithEnvFallback(env)
	logger := common.NewWriterLogger(stdout)
	client, err := ingestclient.FromEnv(env, cfg, logger)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return common.ExitFailure
	}

	state, err := OpenState(common.ExpandUser(*stateFile), account, library)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return common.ExitFailure
	}
	defer state.Close()

	lock, acquired, err := common.TryRunLock(common.ExpandUser(*lockFile))
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return common.ExitFailure
	}
	if !acquired {
		fmt.Fprintln(stdout, "Photo upload skipped: another uploader run is active")
		return 0
	}
	defer lock.Release()

	if *retryFailed {
		cleared, err := state.ClearFailures()
		if err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return common.ExitFailure
		}
		fmt.Fprintf(stdout, "Cleared the retry backoff on %d previously failed file(s)\n", cleared)
	}
	runner := &Runner{
		Account:           account,
		LibraryPath:       library,
		Client:            client,
		Logger:            logger,
		Limit:             *limit,
		Mode:              *mode,
		State:             state,
		BeforeUploadCheck: newBeforeUploadCheck(env, cfg.BaseURL),
		Exporter:          runnerExporter(),
	}
	summary, err := runner.Sync()
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return common.ExitFailure
	}
	fmt.Fprintf(stdout, "Photo upload complete: assets=%d files=%d selected=%d exported=%d uploaded=%d skipped=%d deferred=%d failed=%d\n",
		summary.AssetsSeen, summary.FilesSeen, summary.FilesSelected, summary.FilesExported, summary.FilesUploaded,
		summary.FilesSkipped, summary.FilesDeferred, summary.FilesFailed)
	return 0
}

func runnerExporter() Exporter {
	if runnerExporterOverride != nil {
		return runnerExporterOverride
	}
	return newExporter(DefaultExportTimeout)
}
