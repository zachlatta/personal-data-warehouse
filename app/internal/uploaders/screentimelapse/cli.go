package screentimelapse

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest screen-timelapse --help`.
const Usage = `pdw ingest screen-timelapse - upload the periodic whole-screen captures.

USAGE
  pdw ingest screen-timelapse [--sources device=folder,...] [--limit N]
                              [--compress-png] [--quality Q] [--max-edge PX]
                              [--delete-after-upload] [--wifi-only]
                              [--dry-run] [--state-file PATH] [--lock-file PATH]

Sends every capture file the screen timelapse loops wrote -- each periodic
screenshot (one per display) and each accessibility tree -- to the app's
/ingest/screen-timelapse/file endpoint, newest first, one moment (a device's
screenshots and tree of one instant) at a time. The app stores the bytes and
writes the file's row before it answers; a file is then remembered and not
sent again unless it changes. Files younger than two minutes are left for the
next run, since a capture loop may still be writing them, and a moment's
screenshots wait for its tree to settle (at most ten minutes more when it has
none). A run stops at the first failed upload and exits non-zero.

Before a moment is sent its tree is scanned for payment card numbers (13-19
digits, Luhn, a Visa, Mastercard, Amex or Discover prefix). On a hit the tree
goes up with the number replaced, a screenshot the number's screen can be
matched to exactly goes up with a red box over it and the card's other fields
(HEIC and AVIF are redrawn with sips), and the moment's other screenshots are
withheld: recorded, never sent, until the file is edited. Files are sent
exactly as written otherwise.

FLAGS
  --sources SPEC          Comma-separated device=folder pairs: whose screen each
                          folder holds. Overrides SCREEN_TIMELAPSE_SOURCES.
  --limit N               Files to send this run, newest first (default 300), so a
                          backlog drains behind the live minute over several runs.
  --compress-png          Re-encode raw PNG screenshots in memory (after any
                          card-number redaction) before upload (WebP via
                          Android's native Skia encoder when running on Android,
                          pure-Go JPEG fallback elsewhere).
  --quality Q             Lossy compression quality 1-100 for --compress-png
                          (default 75).
  --max-edge PX           Maximum long edge in pixels for --compress-png
                          (default 0: keep native screen resolution).
  --delete-after-upload   Unlink each local file after the server acknowledges
                          it (never unlinks a screenshot withheld by the card
                          guard).
  --wifi-only             On Android, defer uploads when the active default
                          network is not Wi-Fi (exits 0 so frames wait on disk).
  --dry-run               List what would be sent; sends nothing and records nothing.
  --state-file PATH       Upload ledger (default: the Application Support state database).
  --lock-file PATH        Nonblocking lock used to avoid overlapping LaunchAgent runs
                          (default: beside the state file).

ENVIRONMENT
  SCREEN_TIMELAPSE_SOURCES               device=folder pairs (default: this Mac's short
                                         hostname = ~/Pictures/Screenshots/periodic).
                                         crobat: crobat=~/Pictures/Screenshots/periodic,
                                         joltik=~/Pictures/PhoneScreenshots/joltik
                                         pichu: pichu=/data/adb/screen-timelapse/out
  SCREEN_TIMELAPSE_COMPRESS_PNG          1/true to enable --compress-png.
  SCREEN_TIMELAPSE_QUALITY               Lossy quality 1-100 (default 75).
  SCREEN_TIMELAPSE_MAX_EDGE              Max long edge in pixels (default 0 = full size).
  SCREEN_TIMELAPSE_DELETE_AFTER_UPLOAD   1/true to enable --delete-after-upload.
  SCREEN_TIMELAPSE_WIFI_ONLY             1/true to enable --wifi-only.
  PDW_INGEST_PROJECT_DIR                 Directory whose .env is loaded (default: current directory).
`

const skipMessage = "Screen timelapse upload skipped: another uploader run is active"

// Run is the `pdw ingest screen-timelapse` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest screen-timelapse")
	sourcesFlag := fs.String("sources", "", "")
	stateFile := fs.String("state-file", DefaultStateFile(), "")
	lockFile := fs.String("lock-file", common.LockFileFor(DefaultStateFile()), "")
	limit := fs.Int("limit", DefaultLimit, "")
	compressPNG := fs.Bool("compress-png", false, "")
	quality := fs.Int("quality", 0, "")
	maxEdge := fs.Int("max-edge", -1, "")
	deleteAfterUpload := fs.Bool("delete-after-upload", false, "")
	wifiOnly := fs.Bool("wifi-only", false, "")
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
	enableCompress := *compressPNG || parseBoolEnv(env.Env("SCREEN_TIMELAPSE_COMPRESS_PNG"))
	enableDelete := *deleteAfterUpload || parseBoolEnv(env.Env("SCREEN_TIMELAPSE_DELETE_AFTER_UPLOAD"))
	enableWiFiOnly := *wifiOnly || parseBoolEnv(env.Env("SCREEN_TIMELAPSE_WIFI_ONLY"))
	compressQuality := *quality
	if compressQuality == 0 {
		compressQuality = parseIntEnv(env.Env("SCREEN_TIMELAPSE_QUALITY"), DefaultCompressQuality)
	}
	if compressQuality < 1 || compressQuality > 100 {
		return common.UsageError(fs, stderr, "--quality must be between 1 and 100")
	}
	compressMaxEdge := *maxEdge
	if compressMaxEdge < 0 {
		compressMaxEdge = parseIntEnv(env.Env("SCREEN_TIMELAPSE_MAX_EDGE"), DefaultCompressMaxEdge)
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
		baseGuard := networkGuard(env, cfg.BaseURL)
		guard = func() string {
			if enableWiFiOnly {
				if reason := AndroidWiFiGuard(); reason != "" {
					return reason
				}
			}
			return baseGuard()
		}
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
		CompressPNG:       enableCompress,
		CompressQuality:   compressQuality,
		CompressMaxEdge:   compressMaxEdge,
		DeleteAfterUpload: enableDelete && !*dryRun,
		BeforeUploadCheck: guard,
	}).Sync()
	fmt.Fprintf(stdout, "Screen timelapse upload complete: sources=%d uploaded=%d already_stored=%d compressed=%d deleted=%d redacted=%d withheld=%d unsettled=%d held=%d remaining=%d deferred=%t dry_run=%t\n",
		len(sources), summary.Uploaded, summary.Skipped, summary.Compressed, summary.Deleted, summary.Redacted, summary.Withheld, summary.Unsettled, summary.Held, summary.Remaining, summary.Deferred, *dryRun)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest screen-timelapse: %v\n", err)
		return common.ExitFailure
	}
	return 0
}

func parseBoolEnv(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func parseIntEnv(raw string, fallback int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

// dryRunUploader prints what a real run would send.
type dryRunUploader struct {
	out io.Writer
}

func (d *dryRunUploader) UploadScreenTimelapseFile(content []byte, f ingestclient.ScreenTimelapseFile) (ingestclient.ScreenTimelapseStored, error) {
	fmt.Fprintf(d.out, "would send %s/%s (%d bytes, captured_at %s)\n", f.Device, f.SourceFilename, len(content), f.CapturedAt)
	return ingestclient.ScreenTimelapseStored{}, nil
}

// onAndroid reports whether this run is on the phone. A var for tests.
var onAndroid = func() bool { return isExecutable(defaultAndroidDumpsys) }

// networkGuard is UploadGuard on a Mac. On Android it is PreflightGuard, like
// every other phone uploader: the tethering policy guards a Mac on a phone's
// hotspot, and Android routes through policy tables, so /proc/net/route has
// no default route and UploadGuard deferred every run on pichu ("no default
// network route", 2026-10-27 to 2026-10-29). --wifi-only still applies.
func networkGuard(env common.Getenv, baseURL string) func() string {
	if onAndroid() {
		return common.PreflightGuard(env, "SCREEN_TIMELAPSE", baseURL)
	}
	return common.UploadGuard(env, "SCREEN_TIMELAPSE", baseURL)
}
