package androidcallrecordings

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest android-call-recordings --help`.
const Usage = `pdw ingest android-call-recordings - upload finished BCR calls.

USAGE
  pdw ingest android-call-recordings [--recorder-dir DIR] [--device NAME]
      [--account EMAIL] [--limit N] [--max-run-seconds N] [--dry-run]
      [--state-file PATH] [--lock-file PATH]

Runs as root on Android. Uploads audio/JSON pairs after both files have settled
for ten seconds, newest first, on any network. The default directory is
/storage/emulated/0/Recordings/Calls. Never deletes or tombstones missing files.
The default device is pichu and account is Zach's warehouse account.
ANDROID_CALL_RECORDINGS_DIR, ANDROID_CALL_RECORDINGS_DEVICE and
ANDROID_CALL_RECORDINGS_ACCOUNT override these defaults.
--limit defaults to 50; --max-run-seconds defaults to 900 (0 disables).
--dry-run sends nothing. State and lock paths default to the state directory.
`

const skipMessage = "Android call recordings upload skipped: another uploader run is active"

// ExitCodeRunDeadline is the exit code of a run that hit --max-run-seconds.
const ExitCodeRunDeadline = 124

const defaultMaxRunSeconds = 900

// exitProcess is replaced in tests.
var exitProcess = os.Exit

// Run is the `pdw ingest android-call-recordings` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest android-call-recordings")
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
		fmt.Fprintf(stderr, "pdw ingest android-call-recordings: %v\n", err)
		return common.ExitFailure
	}
	cfg = cfg.WithEnvFallback(env)
	dir := common.FirstNonEmpty(*recorderDir, env.Env("ANDROID_CALL_RECORDINGS_DIR"), DefaultRecorderDir)
	deviceName := common.FirstNonEmpty(*device, env.Env("ANDROID_CALL_RECORDINGS_DEVICE"), "pichu")
	fallbackAccount := common.FirstNonEmpty(*account, env.Env("ANDROID_CALL_RECORDINGS_ACCOUNT"), "zach@zachlatta.com")
	fmt.Fprintf(stdout, "heartbeat_device=%s\n", deviceName)
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
			fmt.Fprintf(stderr, "pdw ingest android-call-recordings: run exceeded its %s deadline; exiting so the next run can start\n", limitDuration)
			exitProcess(ExitCodeRunDeadline)
		})
		defer timer.Stop()
	}

	statePath := *stateFile
	var client Uploader
	guard := func() string { return "" }
	if *dryRun {
		scratch, err := os.MkdirTemp("", "pdw-android-call-recordings-dry-run-")
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest android-call-recordings: %v\n", err)
			return common.ExitFailure
		}
		defer os.RemoveAll(scratch)
		statePath = filepath.Join(scratch, "state.sqlite")
		client = &dryRunUploader{out: stdout}
	} else {
		ingest, err := ingestclient.FromEnv(env, cfg, logger)
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest android-call-recordings: %v\n", err)
			return common.ExitFailure
		}
		client = ingest
		// Not UploadGuard: its tethering policy is about a Mac on a phone's
		// hotspot, and this runs on the phone (see common.PreflightGuard).
		guard = common.PreflightGuard(env, "ANDROID_CALL_RECORDINGS", cfg.BaseURL)
	}
	state, err := OpenState(statePath)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest android-call-recordings: %v\n", err)
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
	fmt.Fprintf(stdout, "newest_finished_at=%s\n", summary.NewestFinishedAt)
	fmt.Fprintf(stdout, "Android call recordings upload complete: seen=%d audio=%d sidecars=%d already_landed=%d in_progress=%d unsettled=%d missing_audio=%d remaining=%d deferred=%t dry_run=%t\n",
		summary.Seen, summary.AudioUploaded, summary.MetadataUploaded, summary.Skipped, summary.InProgress,
		summary.Unsettled, summary.Missing, summary.Remaining, summary.Deferred, *dryRun)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest android-call-recordings: %v\n", err)
		return common.ExitFailure
	}
	return 0
}

// dryRunUploader reports hashes without logging phone numbers or filenames.
type dryRunUploader struct{ out io.Writer }

func (d *dryRunUploader) UploadAndroidCallRecordingsAudio(path, recordedAt, extension, contentType, sha string) (ingestclient.StoredObject, error) {
	fmt.Fprintf(d.out, "would send audio sha256=%s\n", common.ShortSHA256(sha))
	return ingestclient.StoredObject{}, nil
}
func (d *dryRunUploader) UploadAndroidCallRecordingsMetadata(payload map[string]any, recordedAt, sha, dedup string) (ingestclient.StoredObject, error) {
	fmt.Fprintf(d.out, "would send sidecar sha256=%s\n", common.ShortSHA256(sha))
	return ingestclient.StoredObject{}, nil
}
