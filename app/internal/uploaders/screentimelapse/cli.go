package screentimelapse

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest screen-timelapse --help`.
const Usage = `pdw ingest screen-timelapse - upload the periodic whole-screen captures.

USAGE
  pdw ingest screen-timelapse [--sources device=folder,...] [--limit N]
                              [--dry-run] [--state-file PATH] [--lock-file PATH]

Sends every capture file the screen timelapse loops wrote -- each periodic
screenshot (one per display) and each accessibility tree -- to the app's
/ingest/screen-timelapse/file endpoint, exactly as written, newest first. The
app stores the bytes and writes the file's row before it answers; a file is
then remembered and not sent again unless it changes. Files younger than two
minutes are left for the next run, since a capture loop may still be writing
them. A run stops at the first failed upload and exits non-zero.

FLAGS
  --sources SPEC      Comma-separated device=folder pairs: whose screen each
                      folder holds. Overrides SCREEN_TIMELAPSE_SOURCES.
  --limit N           Files to send this run, newest first (default 300), so a
                      backlog drains behind the live minute over several runs.
  --dry-run           List what would be sent; sends nothing and records nothing.
  --state-file PATH   Upload ledger (default: the Application Support state database).
  --lock-file PATH    Nonblocking lock used to avoid overlapping LaunchAgent runs
                      (default: beside the state file).

ENVIRONMENT
  SCREEN_TIMELAPSE_SOURCES   device=folder pairs (default: this Mac's short
                             hostname = ~/Pictures/Screenshots/periodic).
                             crobat: crobat=~/Pictures/Screenshots/periodic,
                             joltik=~/Pictures/PhoneScreenshots/joltik
  PDW_INGEST_PROJECT_DIR     Directory whose .env is loaded (default: current directory).
`

const skipMessage = "Screen timelapse upload skipped: another uploader run is active"

// Run is the `pdw ingest screen-timelapse` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest screen-timelapse")
	sourcesFlag := fs.String("sources", "", "")
	stateFile := fs.String("state-file", DefaultStateFile(), "")
	lockFile := fs.String("lock-file", common.LockFileFor(DefaultStateFile()), "")
	limit := fs.Int("limit", DefaultLimit, "")
	dryRun := fs.Bool("dry-run", false, "")
	if _, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr); done {
		return code
	}
	if *limit <= 0 {
		return common.UsageError(fs, stderr, "--limit must be greater than 0")
	}
	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest screen-timelapse: %v\n", err)
		return common.ExitFailure
	}
	cfg = cfg.WithEnvFallback(env)
	spec := common.FirstNonEmpty(*sourcesFlag, env.Env("SCREEN_TIMELAPSE_SOURCES"),
		common.LocalDeviceName()+"="+filepath.Join(common.HomeDir(), "Pictures", "Screenshots", "periodic"))
	sources, err := ParseSources(spec)
	if err != nil {
		return common.UsageError(fs, stderr, "%v", err)
	}
	logger := common.NewWriterLogger(stdout)

	lock, code, done := common.AcquireRunLock(*lockFile, skipMessage, stdout, stderr)
	if done {
		return code
	}
	defer lock.Release()

	statePath := *stateFile
	var client Uploader
	guard := func() string { return "" }
	if *dryRun {
		// A scratch ledger: the dry run sees every file as unsent.
		scratch, err := os.MkdirTemp("", "pdw-screen-timelapse-dry-run-")
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest screen-timelapse: %v\n", err)
			return common.ExitFailure
		}
		defer os.RemoveAll(scratch)
		statePath = filepath.Join(scratch, "state.sqlite")
		client = &dryRunUploader{out: stdout}
	} else {
		ingest, err := ingestclient.FromEnv(env, cfg, logger)
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest screen-timelapse: %v\n", err)
			return common.ExitFailure
		}
		client = ingest
		guard = common.UploadGuard(env, "SCREEN_TIMELAPSE", cfg.BaseURL)
	}
	state, err := OpenState(statePath)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest screen-timelapse: %v\n", err)
		return common.ExitFailure
	}
	defer state.Close()
	summary, err := (&Runner{
		Sources:           sources,
		Client:            client,
		State:             state,
		Logger:            logger,
		Limit:             *limit,
		BeforeUploadCheck: guard,
	}).Sync()
	fmt.Fprintf(stdout, "Screen timelapse upload complete: sources=%d uploaded=%d already_stored=%d unsettled=%d remaining=%d deferred=%t dry_run=%t\n",
		len(sources), summary.Uploaded, summary.Skipped, summary.Unsettled, summary.Remaining, summary.Deferred, *dryRun)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest screen-timelapse: %v\n", err)
		return common.ExitFailure
	}
	return 0
}

// dryRunUploader prints what a real run would send.
type dryRunUploader struct {
	out io.Writer
}

func (d *dryRunUploader) UploadScreenTimelapseFile(content []byte, f ingestclient.ScreenTimelapseFile) (ingestclient.ScreenTimelapseStored, error) {
	fmt.Fprintf(d.out, "would send %s/%s (%d bytes, captured_at %s)\n", f.Device, f.SourceFilename, len(content), f.CapturedAt)
	return ingestclient.ScreenTimelapseStored{}, nil
}
