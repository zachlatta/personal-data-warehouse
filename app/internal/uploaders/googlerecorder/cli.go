package googlerecorder

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest google-recorder --help`.
const Usage = `pdw ingest google-recorder - upload Google Recorder's recordings from the Pixel.

USAGE
  pdw ingest google-recorder [--recorder-dir DIR] [--device NAME] [--account EMAIL]
                             [--limit N] [--max-run-seconds N] [--dry-run]
                             [--state-file PATH] [--lock-file PATH]

Runs ON the phone, as root (Recorder's recordings and database are private to
the app). Reads a copy of Recorder's recording_database, and sends every
finished recording -- the .m4a Recorder wrote, unchanged, and a JSON sidecar
with Recorder's title, time, duration and place -- to the app's
/ingest/google-recorder endpoints, newest first. A recording is then
remembered and sent again only when Recorder changes it: a rename sends a new
sidecar, an edit sends the new audio. Recordings still in progress, and files
changed in the last few seconds, wait for the next run. A run stops at the
first failed upload and exits non-zero.

FLAGS
  --recorder-dir DIR     Recorder's data directory
                         (default /data/data/com.google.android.apps.recorder).
  --device NAME          The phone's name in the warehouse (default
                         GOOGLE_RECORDER_DEVICE, else the hostname).
  --account EMAIL        Account for recordings made while Recorder had no
                         Google account signed in (default GOOGLE_RECORDER_ACCOUNT).
  --limit N              Recordings sent per run, newest first (default 50).
  --max-run-seconds N    Exit 124 when a run takes longer; 0 disables (default 900).
  --dry-run              List what would be sent; sends nothing and records nothing.
  --state-file PATH      Upload ledger (default: the state directory).
  --lock-file PATH       Nonblocking lock against overlapping runs
                         (default: beside the state file).

ENVIRONMENT
  GOOGLE_RECORDER_DIR, GOOGLE_RECORDER_DEVICE, GOOGLE_RECORDER_ACCOUNT
  PDW_API_URL, PDW_SECRET_TOKEN   (or pdw's own config file)
`

const skipMessage = "Google Recorder upload skipped: another uploader run is active"

// ExitCodeRunDeadline is the exit code of a run that hit --max-run-seconds.
const ExitCodeRunDeadline = 124

const defaultMaxRunSeconds = 900

// exitProcess is replaced in tests.
var exitProcess = os.Exit

// Run is the `pdw ingest google-recorder` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest google-recorder")
	recorderDir := fs.String("recorder-dir", "", "")
	device := fs.String("device", "", "")
	account := fs.String("account", "", "")
	stateFile := fs.String("state-file", DefaultStateFile(), "")
	lockFile := fs.String("lock-file", "", "")
	limit := fs.Int("limit", DefaultLimit, "")
	maxRunSeconds := fs.Int("max-run-seconds", defaultMaxRunSeconds, "")
	dryRun := fs.Bool("dry-run", false, "")
	if _, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr); done {
		return code
	}
	if *limit <= 0 {
		return common.UsageError(fs, stderr, "--limit must be greater than 0")
	}
	if *maxRunSeconds < 0 {
		return common.UsageError(fs, stderr, "--max-run-seconds must be 0 or more")
	}
	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest google-recorder: %v\n", err)
		return common.ExitFailure
	}
	cfg = cfg.WithEnvFallback(env)
	dir := common.FirstNonEmpty(*recorderDir, env.Env("GOOGLE_RECORDER_DIR"), DefaultRecorderDir)
	deviceName := common.FirstNonEmpty(*device, env.Env("GOOGLE_RECORDER_DEVICE"), common.LocalDeviceName())
	fallbackAccount := common.FirstNonEmpty(*account, env.Env("GOOGLE_RECORDER_ACCOUNT"))
	logger := common.NewWriterLogger(stdout)

	lockPath := *lockFile
	if lockPath == "" {
		lockPath = common.LockFileFor(*stateFile)
	}
	lock, code, done := common.AcquireRunLock(lockPath, skipMessage, stdout, stderr)
	if done {
		return code
	}
	defer lock.Release()
	if *maxRunSeconds > 0 {
		limitDuration := time.Duration(*maxRunSeconds) * time.Second
		timer := time.AfterFunc(limitDuration, func() {
			fmt.Fprintf(stderr, "pdw ingest google-recorder: run exceeded its %s deadline; exiting so the next run can start\n", limitDuration)
			exitProcess(ExitCodeRunDeadline)
		})
		defer timer.Stop()
	}

	statePath := *stateFile
	var client Uploader
	guard := func() string { return "" }
	if *dryRun {
		scratch, err := os.MkdirTemp("", "pdw-google-recorder-dry-run-")
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest google-recorder: %v\n", err)
			return common.ExitFailure
		}
		defer os.RemoveAll(scratch)
		statePath = filepath.Join(scratch, "state.sqlite")
		client = &dryRunUploader{out: stdout}
	} else {
		ingest, err := ingestclient.FromEnv(env, cfg, logger)
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest google-recorder: %v\n", err)
			return common.ExitFailure
		}
		client = ingest
		guard = common.UploadGuard(env, "GOOGLE_RECORDER", cfg.BaseURL)
	}
	state, err := OpenState(statePath)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest google-recorder: %v\n", err)
		return common.ExitFailure
	}
	defer state.Close()
	summary, err := (&Runner{
		RecorderDir:       dir,
		Device:            deviceName,
		Account:           fallbackAccount,
		Client:            client,
		State:             state,
		Logger:            logger,
		Limit:             *limit,
		BeforeUploadCheck: guard,
	}).Sync()
	fmt.Fprintf(stdout, "Google Recorder upload complete: seen=%d audio=%d sidecars=%d already_landed=%d in_progress=%d unsettled=%d missing_audio=%d remaining=%d deferred=%t dry_run=%t\n",
		summary.Seen, summary.AudioUploaded, summary.MetadataUploaded, summary.Skipped, summary.InProgress,
		summary.Unsettled, summary.Missing, summary.Remaining, summary.Deferred, *dryRun)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest google-recorder: %v\n", err)
		return common.ExitFailure
	}
	return 0
}

// dryRunUploader prints what a real run would send.
type dryRunUploader struct {
	out io.Writer
}

func (d *dryRunUploader) UploadGoogleRecorderAudio(path, recordedAt, extension, contentType, contentSHA256 string) (ingestclient.StoredObject, error) {
	fmt.Fprintf(d.out, "would send audio %s (recorded_at %s, sha256=%s)\n", path, recordedAt, common.ShortSHA256(contentSHA256))
	return ingestclient.StoredObject{}, nil
}

func (d *dryRunUploader) UploadGoogleRecorderMetadata(payload map[string]any, recordedAt, audioContentSHA256, metadataDedupSHA256 string) (ingestclient.StoredObject, error) {
	recording, _ := payload["recording"].(map[string]any)
	fmt.Fprintf(d.out, "would send sidecar for %v %q\n", recording["recording_id"], recording["title"])
	return ingestclient.StoredObject{}, nil
}
