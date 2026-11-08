package androidphotos

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest android-photos --help`.
const Usage = `pdw ingest android-photos - upload the Pixel's photos and videos.

USAGE
  pdw ingest android-photos [--media-root DIR] [--device NAME] [--account EMAIL]
                            [--limit N] [--max-run-seconds N] [--dry-run]
                            [--state-file PATH] [--lock-file PATH]

Runs ON the phone, as root. Asks MediaStore (Android's media index) for every
finished image and video under DCIM/, Pictures/ and Movies/ -- the camera,
screenshots and images apps save -- and sends each file, unchanged, to the
app's /ingest/photos endpoints with a photo envelope (source android_photos):
MediaStore's capture time, size and owner app, and the camera, UTC offset and
GPS from the JPEG's EXIF. A motion photo's video, which Pixel Camera appends to
the JPEG, is cut out and sent as the same photo's live_video rendition (an
.mp4). Newest first. A file is remembered by its path and
sent again only when its bytes change. Uploads go over Wi-Fi only: on any other
network (or when dumpsys cannot say) the run defers, before reading a file. A
run stops at the first failed upload and exits non-zero.

FLAGS
  --media-root DIR       Backing directory of the primary volume
                         (default /data/media/0).
  --device NAME          The phone's name in the warehouse (default
                         ANDROID_PHOTOS_DEVICE, else the hostname).
  --account EMAIL        Account the photos are keyed by (default
                         ANDROID_PHOTOS_ACCOUNT, else PHOTOS_ACCOUNT).
  --limit N              Files sent per run, newest first (default 25).
  --max-run-seconds N    Exit 124 when a run takes longer; 0 disables (default 900).
  --dry-run              List what would be sent; sends nothing and records nothing.
  --state-file PATH      Upload ledger (default: the state directory).
  --lock-file PATH       Nonblocking lock against overlapping runs
                         (default: beside the state file).

ENVIRONMENT
  ANDROID_PHOTOS_MEDIA_ROOT, ANDROID_PHOTOS_DEVICE, ANDROID_PHOTOS_ACCOUNT
  PDW_API_URL, PDW_SECRET_TOKEN   (or pdw's own config file)
`

const skipMessage = "Android photo upload skipped: another uploader run is active"

// ExitCodeRunDeadline is the exit code of a run that hit --max-run-seconds.
const ExitCodeRunDeadline = 124

const defaultMaxRunSeconds = 900

// exitProcess is replaced in tests.
var exitProcess = os.Exit

// Run is the `pdw ingest android-photos` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	_ = stdin
	fs := common.NewFlagSet("pdw ingest android-photos")
	mediaRoot := fs.String("media-root", "", "")
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
		fmt.Fprintf(stderr, "pdw ingest android-photos: %v\n", err)
		return common.ExitFailure
	}
	cfg = cfg.WithEnvFallback(env)
	root := common.FirstNonEmpty(*mediaRoot, env.Env("ANDROID_PHOTOS_MEDIA_ROOT"), DefaultMediaRoot)
	deviceName := common.FirstNonEmpty(*device, env.Env("ANDROID_PHOTOS_DEVICE"), common.LocalDeviceName())
	accountName := common.FirstNonEmpty(*account, env.Env("ANDROID_PHOTOS_ACCOUNT"), env.PhotosAccount())
	if accountName == "" {
		fmt.Fprintln(stderr, "pdw ingest android-photos: --account or ANDROID_PHOTOS_ACCOUNT must be set")
		return common.ExitFailure
	}
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
			fmt.Fprintf(stderr, "pdw ingest android-photos: run exceeded its %s deadline; exiting so the next run can start\n", limitDuration)
			exitProcess(ExitCodeRunDeadline)
		})
		defer timer.Stop()
	}

	statePath := *stateFile
	var client Uploader
	guard := func() string { return "" }
	// Photos go over Wi-Fi only (Zach, 2026-11-08), and a phone that cannot
	// tell which network it is on defers rather than risk cellular data.
	network := func() string { return common.AndroidWiFiGuard(true) }
	if *dryRun {
		network = nil
		scratch, err := os.MkdirTemp("", "pdw-android-photos-dry-run-")
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest android-photos: %v\n", err)
			return common.ExitFailure
		}
		defer os.RemoveAll(scratch)
		statePath = filepath.Join(scratch, "state.sqlite")
		client = &dryRunUploader{out: stdout}
	} else {
		ingest, err := ingestclient.FromEnv(env, cfg, logger)
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest android-photos: %v\n", err)
			return common.ExitFailure
		}
		client = ingest
		// Not UploadGuard: its tethering policy is about a Mac on a phone's
		// hotspot, and this runs on the phone (see common.PreflightGuard).
		guard = common.PreflightGuard(env, "ANDROID_PHOTOS", cfg.BaseURL)
	}
	state, err := OpenState(statePath)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest android-photos: %v\n", err)
		return common.ExitFailure
	}
	defer state.Close()
	summary, err := (&Runner{
		MediaRoot:         root,
		Device:            deviceName,
		Account:           accountName,
		Client:            client,
		State:             state,
		Logger:            logger,
		Limit:             *limit,
		BeforeUploadCheck: guard,
		NetworkCheck:      network,
	}).Sync()
	fmt.Fprintf(stdout, "Android photo upload complete: seen=%d uploaded=%d motion=%d motion_unreadable=%d already_landed=%d unsettled=%d missing=%d remaining=%d deferred=%t dry_run=%t\n",
		summary.Seen, summary.Uploaded, summary.Motion, summary.MotionUnreadable, summary.Skipped, summary.Unsettled, summary.Missing, summary.Remaining, summary.Deferred, *dryRun)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest android-photos: %v\n", err)
		return common.ExitFailure
	}
	return 0
}

// dryRunUploader prints what a real run would send.
type dryRunUploader struct {
	out io.Writer
}

func (d *dryRunUploader) UploadPhotoFile(path, capturedAt, extension, contentType, contentSHA256 string) (ingestclient.StoredObject, error) {
	fmt.Fprintf(d.out, "would send %s (%s, captured_at %s, sha256=%s)\n", path, contentType, capturedAt, common.ShortSHA256(contentSHA256))
	return ingestclient.StoredObject{}, nil
}

func (d *dryRunUploader) UploadPhotoMetadata(payload map[string]any, capturedAt, fileContentSHA256, metadataDedupSHA256 string) (ingestclient.StoredObject, error) {
	file, _ := payload["file"].(map[string]any)
	fmt.Fprintf(d.out, "would send %v envelope for %v (camera %v %v, offset %q)\n", file["role"], file["native_id"], file["camera_make"], file["camera_model"], file["capture_tz_offset"])
	return ingestclient.StoredObject{}, nil
}
