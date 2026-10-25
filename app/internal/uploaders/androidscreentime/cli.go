package androidscreentime

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest android-screen-time --help`.
const Usage = `pdw ingest android-screen-time - upload an Android phone's screen time.

USAGE
  pdw ingest android-screen-time [--device NAME] [--account EMAIL]
                                 [--state-file PATH] [--max-run-seconds N]
                                 [--dry-run] [--lock-file PATH]

Runs ON the phone, as root. It reads what Android's usage stats service
records, through the PDW usage exporter app (com.zachlatta.pdw.usage, which
must have usage access): every usage event (an app coming to the foreground
or leaving it, the screen on or off, the phone locked or unlocked, a
notification interrupting, shutdown and startup) since the last run, and
Android's daily totals per app and for the device (screen-on and unlocked
time) for the last 12 days, sending only the totals that changed. They go as
gzipped batches to the app's /ingest/android-screen-time/batch endpoint.

Android keeps events and daily totals for about ten days, so the first run on
a device sends that much history and nothing older exists. Rows are keyed by
--device, so each phone or tablet sends under its own name.

FLAGS
  --device NAME          The device's name in the warehouse (default
                         ANDROID_SCREEN_TIME_DEVICE, else the hostname).
  --account EMAIL        Account the rows are keyed by (default
                         ANDROID_SCREEN_TIME_ACCOUNT).
  --state-file PATH      What was sent (default: the state directory).
  --max-run-seconds N    Exit 124 when a run takes longer; 0 disables (default 300).
  --dry-run              Read and print what would be sent; sends nothing and
                         records nothing.
  --lock-file PATH       Nonblocking lock against overlapping runs
                         (default: the state directory).

ENVIRONMENT
  ANDROID_SCREEN_TIME_DEVICE, ANDROID_SCREEN_TIME_ACCOUNT
  PDW_API_URL, PDW_SECRET_TOKEN   (or pdw's own config file)
`

const skipMessage = "Android screen time upload skipped: another uploader run is active"

// ExitCodeRunDeadline is the exit code of a run that hit --max-run-seconds.
const ExitCodeRunDeadline = 124

const defaultMaxRunSeconds = 300

// exitProcess is replaced in tests.
var exitProcess = os.Exit

// Run is the `pdw ingest android-screen-time` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest android-screen-time")
	stateFile := fs.String("state-file", common.DefaultStateFile("android-screen-time-upload.json"), "")
	device := fs.String("device", "", "")
	account := fs.String("account", "", "")
	lockFile := fs.String("lock-file", common.DefaultStateFile("android-screen-time-upload.lock"), "")
	maxRunSeconds := fs.Int("max-run-seconds", defaultMaxRunSeconds, "")
	dryRun := fs.Bool("dry-run", false, "")
	if _, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr); done {
		return code
	}
	if *maxRunSeconds < 0 {
		return common.UsageError(fs, stderr, "--max-run-seconds must be 0 or more")
	}
	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest android-screen-time: %v\n", err)
		return common.ExitFailure
	}
	cfg = cfg.WithEnvFallback(env)
	accountName := common.FirstNonEmpty(*account, env.Env("ANDROID_SCREEN_TIME_ACCOUNT"))
	if accountName == "" {
		fmt.Fprintln(stderr, "pdw ingest android-screen-time: --account or ANDROID_SCREEN_TIME_ACCOUNT must be set")
		return common.ExitFailure
	}
	deviceName := common.FirstNonEmpty(*device, env.Env("ANDROID_SCREEN_TIME_DEVICE"), common.LocalDeviceName())
	logger := common.NewWriterLogger(stdout)

	lock, code, done := common.AcquireRunLock(*lockFile, skipMessage, stdout, stderr)
	if done {
		return code
	}
	defer lock.Release()
	if *maxRunSeconds > 0 {
		limitDuration := time.Duration(*maxRunSeconds) * time.Second
		timer := time.AfterFunc(limitDuration, func() {
			fmt.Fprintf(stderr, "pdw ingest android-screen-time: run exceeded its %s deadline; exiting so the next run can start\n", limitDuration)
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
			fmt.Fprintf(stderr, "pdw ingest android-screen-time: %v\n", err)
			return common.ExitFailure
		}
		client = ingest
		// Not UploadGuard: this runs on the phone (see common.PreflightGuard).
		guard = common.PreflightGuard(env, "ANDROID_SCREEN_TIME", cfg.BaseURL)
	}
	summary, err := (&Runner{
		StatePath:         *stateFile,
		Account:           accountName,
		Device:            deviceName,
		Client:            client,
		Logger:            logger,
		DryRun:            *dryRun,
		BeforeUploadCheck: guard,
	}).Sync()
	fmt.Fprintf(stdout, "Android screen time upload complete: events_read=%d events=%d daily_read=%d daily=%d daily_unchanged=%d batches=%d deferred=%t dry_run=%t\n",
		summary.EventsRead, summary.EventsSent, summary.DailyRead, summary.DailySent, summary.DailyUnchanged, summary.Batches, summary.Deferred, *dryRun)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest android-screen-time: %v\n", err)
		return common.ExitFailure
	}
	return 0
}

// dryRunUploader prints what a real run would send.
type dryRunUploader struct {
	out     io.Writer
	batches int
}

func (d *dryRunUploader) UploadAndroidScreenTimeBatch(gzipBytes []byte, exportedAt string) (ingestclient.StoredObject, error) {
	d.batches++
	return ingestclient.StoredObject{StorageKey: fmt.Sprintf("dry-run-%d (%d gzipped bytes)", d.batches, len(gzipBytes))}, nil
}
