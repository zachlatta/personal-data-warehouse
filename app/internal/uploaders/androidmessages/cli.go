package androidmessages

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest android-messages --help`.
const Usage = `pdw ingest android-messages - upload the texts the phone sends and receives.

USAGE
  pdw ingest android-messages --since TIME [--database PATH] [--state-file PATH]
                              [--device NAME] [--account EMAIL]
                              [--max-run-seconds N] [--dry-run] [--lock-file PATH]

Runs ON the phone, as root (Google Messages' database is private to it). It
reads Messages' own database (bugle_db: every SMS, MMS and RCS message)
read-only and sends each message received at or after --since, with its
text, attachment details (type, name, size; not the file), sender and
conversation, as gzipped batches to the app's /ingest/android-messages/batch
endpoint. Each run sends new messages and re-sends any from the last three
days whose status, read state or parts changed. A run that finds Messages'
database closed (no -wal/-shm beside it) sends nothing and exits 0.

--since is required: messages before it are never read. On the Pixel it is
the moment the phone took over the line, so the iPhone history imported into
Messages (PDW already has it from the Mac) is not sent again.

FLAGS
  --since TIME           RFC 3339 start time (default ANDROID_MESSAGES_SINCE).
  --database PATH        Messages' database
                         (default /data/data/com.google.android.apps.messaging/databases/bugle_db).
  --state-file PATH      What was sent (default: the state directory).
  --device NAME          The phone's name in the warehouse (default
                         ANDROID_MESSAGES_DEVICE, else the hostname).
  --account EMAIL        Account the messages are keyed by (default
                         ANDROID_MESSAGES_ACCOUNT).
  --max-run-seconds N    Exit 124 when a run takes longer; 0 disables (default 600).
  --dry-run              Read and print what would be sent; sends nothing and
                         records nothing.
  --lock-file PATH       Nonblocking lock against overlapping runs
                         (default: the state directory).

ENVIRONMENT
  ANDROID_MESSAGES_SINCE, ANDROID_MESSAGES_DATABASE, ANDROID_MESSAGES_DEVICE,
  ANDROID_MESSAGES_ACCOUNT
  PDW_API_URL, PDW_SECRET_TOKEN   (or pdw's own config file)
`

const skipMessage = "Android messages upload skipped: another uploader run is active"

// ExitCodeRunDeadline is the exit code of a run that hit --max-run-seconds.
const ExitCodeRunDeadline = 124

const defaultMaxRunSeconds = 600

// exitProcess is replaced in tests.
var exitProcess = os.Exit

// Run is the `pdw ingest android-messages` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest android-messages")
	since := fs.String("since", "", "")
	database := fs.String("database", "", "")
	stateFile := fs.String("state-file", common.DefaultStateFile("android-messages-upload.json"), "")
	device := fs.String("device", "", "")
	account := fs.String("account", "", "")
	lockFile := fs.String("lock-file", common.DefaultStateFile("android-messages-upload.lock"), "")
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
		fmt.Fprintf(stderr, "pdw ingest android-messages: %v\n", err)
		return common.ExitFailure
	}
	cfg = cfg.WithEnvFallback(env)
	sinceText := common.FirstNonEmpty(*since, env.Env("ANDROID_MESSAGES_SINCE"))
	if sinceText == "" {
		fmt.Fprintln(stderr, "pdw ingest android-messages: --since or ANDROID_MESSAGES_SINCE must be set (messages before it are never sent)")
		return common.ExitFailure
	}
	sinceTime, err := time.Parse(time.RFC3339, sinceText)
	if err != nil {
		return common.UsageError(fs, stderr, "--since must be an RFC 3339 time: %v", err)
	}
	accountName := common.FirstNonEmpty(*account, env.Env("ANDROID_MESSAGES_ACCOUNT"))
	if accountName == "" {
		fmt.Fprintln(stderr, "pdw ingest android-messages: --account or ANDROID_MESSAGES_ACCOUNT must be set")
		return common.ExitFailure
	}
	deviceName := common.FirstNonEmpty(*device, env.Env("ANDROID_MESSAGES_DEVICE"), common.LocalDeviceName())
	dbPath := common.FirstNonEmpty(*database, env.Env("ANDROID_MESSAGES_DATABASE"), DefaultDatabasePath)
	logger := common.NewWriterLogger(stdout)

	lock, code, done := common.AcquireRunLock(*lockFile, skipMessage, stdout, stderr)
	if done {
		return code
	}
	defer lock.Release()
	if *maxRunSeconds > 0 {
		limitDuration := time.Duration(*maxRunSeconds) * time.Second
		timer := time.AfterFunc(limitDuration, func() {
			fmt.Fprintf(stderr, "pdw ingest android-messages: run exceeded its %s deadline; exiting so the next run can start\n", limitDuration)
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
			fmt.Fprintf(stderr, "pdw ingest android-messages: %v\n", err)
			return common.ExitFailure
		}
		client = ingest
		// Not UploadGuard: this runs on the phone (see common.PreflightGuard).
		guard = common.PreflightGuard(env, "ANDROID_MESSAGES", cfg.BaseURL)
	}
	summary, err := (&Runner{
		DatabasePath:      dbPath,
		StatePath:         *stateFile,
		Since:             sinceTime,
		Account:           accountName,
		Device:            deviceName,
		Client:            client,
		Logger:            logger,
		DryRun:            *dryRun,
		BeforeUploadCheck: guard,
	}).Sync()
	fmt.Fprintf(stdout, "Android messages upload complete: read=%d unchanged=%d messages=%d batches=%d deferred=%t dry_run=%t\n",
		summary.Read, summary.Unchanged, summary.Uploaded, summary.Batches, summary.Deferred, *dryRun)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest android-messages: %v\n", err)
		return common.ExitFailure
	}
	return 0
}

// dryRunUploader prints what a real run would send.
type dryRunUploader struct {
	out     io.Writer
	batches int
}

func (d *dryRunUploader) UploadAndroidMessagesBatch(gzipBytes []byte, exportedAt string) (ingestclient.StoredObject, error) {
	d.batches++
	return ingestclient.StoredObject{StorageKey: fmt.Sprintf("dry-run-%d (%d gzipped bytes)", d.batches, len(gzipBytes))}, nil
}
