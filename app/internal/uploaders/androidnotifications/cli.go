package androidnotifications

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest android-notifications --help`.
const Usage = `pdw ingest android-notifications - upload the notifications the phone receives.

USAGE
  pdw ingest android-notifications tail-system-log --spool-dir DIR   (see its --help)
  pdw ingest android-notifications [--spool-dir DIR] [--system-log-spool-dir DIR]
                                   [--device NAME] [--account EMAIL]
                                   [--limit N] [--max-run-seconds N] [--dry-run]
                                   [--lock-file PATH]

Runs ON the phone, as root (the spool is private to the listener app). The
PDW notification listener writes one JSON file per notification event --
posted or visibly updated, removed (with Android's reason: a tap, a swipe,
the app), or already showing when it connected -- and this sends them, oldest
first, as gzipped batches to the app's /ingest/android-notifications/batch
endpoint, deleting each file once its batch is stored. A file that is not a
listener event is moved to the spool's rejected/ directory. The system-log
spool (written by tail-system-log: shown, hidden, expanded, tapped, action
pressed, alerted, and the shade, screen and lock state) is sent in the same
run, merged by time. A run stops at
the first failed upload and exits non-zero; what was not sent stays.

FLAGS
  --spool-dir DIR        The listener's spool
                         (default /data/data/com.zachlatta.pdw.notifications/files/spool).
  --system-log-spool-dir DIR
                         tail-system-log's spool, sent with the listener's
                         (default ANDROID_NOTIFICATIONS_SYSTEM_LOG_SPOOL_DIR;
                         none when unset).
  --device NAME          The phone's name in the warehouse (default
                         ANDROID_NOTIFICATIONS_DEVICE, else the hostname).
  --account EMAIL        Account the events are keyed by (default
                         ANDROID_NOTIFICATIONS_ACCOUNT).
  --limit N              Events sent per run, oldest first; 0 is all (default 0).
  --max-run-seconds N    Exit 124 when a run takes longer; 0 disables (default 600).
  --dry-run              Read the spool and print what would be sent; sends
                         nothing and deletes nothing.
  --lock-file PATH       Nonblocking lock against overlapping runs
                         (default: the state directory).

ENVIRONMENT
  ANDROID_NOTIFICATIONS_SPOOL_DIR, ANDROID_NOTIFICATIONS_SYSTEM_LOG_SPOOL_DIR,
  ANDROID_NOTIFICATIONS_DEVICE,
  ANDROID_NOTIFICATIONS_ACCOUNT
  PDW_API_URL, PDW_SECRET_TOKEN   (or pdw's own config file)
`

const skipMessage = "Android notification upload skipped: another uploader run is active"

// ExitCodeRunDeadline is the exit code of a run that hit --max-run-seconds.
const ExitCodeRunDeadline = 124

const defaultMaxRunSeconds = 600

// exitProcess is replaced in tests.
var exitProcess = os.Exit

// Run is the `pdw ingest android-notifications` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	if len(args) > 0 && args[0] == "tail-system-log" {
		return RunTail(args[1:], stdout, stderr)
	}
	fs := common.NewFlagSet("pdw ingest android-notifications")
	spoolDir := fs.String("spool-dir", "", "")
	systemLogSpoolDir := fs.String("system-log-spool-dir", "", "")
	device := fs.String("device", "", "")
	account := fs.String("account", "", "")
	lockFile := fs.String("lock-file", common.DefaultStateFile("android-notifications-upload.lock"), "")
	limit := fs.Int("limit", 0, "")
	maxRunSeconds := fs.Int("max-run-seconds", defaultMaxRunSeconds, "")
	dryRun := fs.Bool("dry-run", false, "")
	if _, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr); done {
		return code
	}
	if *limit < 0 {
		return common.UsageError(fs, stderr, "--limit must be 0 or more")
	}
	if *maxRunSeconds < 0 {
		return common.UsageError(fs, stderr, "--max-run-seconds must be 0 or more")
	}
	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest android-notifications: %v\n", err)
		return common.ExitFailure
	}
	cfg = cfg.WithEnvFallback(env)
	dir := common.FirstNonEmpty(*spoolDir, env.Env("ANDROID_NOTIFICATIONS_SPOOL_DIR"), DefaultSpoolDir)
	systemLogDir := common.FirstNonEmpty(*systemLogSpoolDir, env.Env("ANDROID_NOTIFICATIONS_SYSTEM_LOG_SPOOL_DIR"))
	deviceName := common.FirstNonEmpty(*device, env.Env("ANDROID_NOTIFICATIONS_DEVICE"), common.LocalDeviceName())
	accountName := common.FirstNonEmpty(*account, env.Env("ANDROID_NOTIFICATIONS_ACCOUNT"))
	if accountName == "" {
		fmt.Fprintln(stderr, "pdw ingest android-notifications: --account or ANDROID_NOTIFICATIONS_ACCOUNT must be set")
		return common.ExitFailure
	}
	logger := common.NewWriterLogger(stdout)

	lock, code, done := common.AcquireRunLock(*lockFile, skipMessage, stdout, stderr)
	if done {
		return code
	}
	defer lock.Release()
	if *maxRunSeconds > 0 {
		limitDuration := time.Duration(*maxRunSeconds) * time.Second
		timer := time.AfterFunc(limitDuration, func() {
			fmt.Fprintf(stderr, "pdw ingest android-notifications: run exceeded its %s deadline; exiting so the next run can start\n", limitDuration)
			exitProcess(ExitCodeRunDeadline)
		})
		defer timer.Stop()
	}

	var client Uploader
	guard := func() string { return "" }
	if *dryRun {
		client = &dryRunUploader{out: stdout}
	} else {
		ingest, err := ingestclient.FromEnv(env, cfg, logger)
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest android-notifications: %v\n", err)
			return common.ExitFailure
		}
		client = ingest
		// Not UploadGuard: this runs on the phone (see common.PreflightGuard).
		guard = common.PreflightGuard(env, "ANDROID_NOTIFICATIONS", cfg.BaseURL)
	}
	summary, err := (&Runner{
		SpoolDir:          dir,
		ExtraSpoolDirs:    []string{systemLogDir},
		Account:           accountName,
		Device:            deviceName,
		Client:            client,
		Logger:            logger,
		Limit:             *limit,
		DryRun:            *dryRun,
		BeforeUploadCheck: guard,
	}).Sync()
	fmt.Fprintf(stdout, "Android notification upload complete: pending=%d events=%d batches=%d rejected=%d deferred=%t dry_run=%t\n",
		summary.Pending, summary.Uploaded, summary.Batches, summary.Rejected, summary.Deferred, *dryRun)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest android-notifications: %v\n", err)
		return common.ExitFailure
	}
	return 0
}

// dryRunUploader prints what a real run would send.
type dryRunUploader struct {
	out     io.Writer
	batches int
}

func (d *dryRunUploader) UploadAndroidNotificationsBatch(gzipBytes []byte, exportedAt string) (ingestclient.StoredObject, error) {
	d.batches++
	return ingestclient.StoredObject{StorageKey: fmt.Sprintf("dry-run-%d (%d gzipped bytes)", d.batches, len(gzipBytes))}, nil
}
