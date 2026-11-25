package agentsessions

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest agent-sessions --help`.
const Usage = `pdw ingest agent-sessions - upload local AI agent CLI session transcripts.

USAGE
  pdw ingest agent-sessions [--mode incremental|full] [--limit N] [--batch-size N]
                            [--watch DURATION] [--interval DURATION]
                            [--max-interval DURATION] [--idle-after DURATION]
                            [--state-file PATH] [--lock-file PATH]

Tails ~/.claude/projects, ~/.codex/sessions, ~/.openclaw/agents/main/sessions,
~/.pi/agent/sessions, Muse Code's ~/.local/share/muse/sessions and Argon's
~/.gemini/antigravity-cli/brain (AGENT_SESSIONS_*_DIR override each; set one
empty to disable that tool on this host), coalesces new lines into gzipped JSONL
batches and posts them to the app's /ingest/agent-sessions/batch endpoint.

FLAGS
  --mode MODE              incremental (default; resume from the state file) or full.
  --limit N                Maximum lines to upload this run; 0 (default) is unlimited.
                           Bounds a first backfill.
  --batch-size N           Maximum lines per uploaded batch (default 20000).
  --watch DURATION         Stay resident for DURATION (e.g. 300s), polling every
                           --interval while any transcript was modified within
                           --idle-after; transcripts whose size and mtime did not
                           move are skipped. Once no transcript has been
                           modified for --idle-after (checked from on-disk mtimes
                           so idle hosts back off from pass 1 after each restart),
                           the pause backs off up to --max-interval and resets to
                           --interval immediately on new activity (first line
                           after a long idle waits at most --max-interval). A host
                           with an OpenClaw SQLite store never backs off.
                           The run then exits so its wrapper can post the heartbeat
                           and the supervisor (systemd Restart=always, launchd
                           KeepAlive) starts the next one. A failed pass ends the
                           window with a failure.
  --interval DURATION      Base pause between --watch passes while active (default 2s).
  --max-interval DURATION  Maximum pause between idle --watch passes (default 30s).
  --idle-after DURATION    Transcript inactivity duration before backing off (default
                           10m); no effect on a host with an OpenClaw SQLite store.
  --state-file PATH        Incremental upload state (default: the Application Support
                           state database).
  --lock-file PATH         Nonblocking lock used to avoid overlapping LaunchAgent runs
                           (default: beside the state file).

ENVIRONMENT
  AGENT_SESSIONS_ACCOUNT (else GMAIL_ACCOUNTS[0])   Account the uploads are keyed by.
  AGENT_SESSIONS_DEVICE                             Device label (default: hostname).
  PDW_INGEST_PROJECT_DIR                            Directory whose .env is loaded
                                                    (default: current directory).
`

const skipMessage = "Agent sessions upload skipped: another uploader run is active"

// Run is the `pdw ingest agent-sessions` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest agent-sessions")
	mode := fs.String("mode", "incremental", "")
	stateFile := fs.String("state-file", DefaultStateFile(), "")
	lockFile := fs.String("lock-file", common.LockFileFor(DefaultStateFile()), "")
	limit := fs.Int("limit", 0, "")
	batchSize := fs.Int("batch-size", DefaultBatchSize, "")
	watch := fs.Duration("watch", 0, "")
	interval := fs.Duration("interval", DefaultWatchInterval, "")
	maxInterval := fs.Duration("max-interval", DefaultMaxWatchInterval, "")
	idleAfter := fs.Duration("idle-after", DefaultIdleAfter, "")
	if _, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr); done {
		return code
	}
	if err := common.ValidateMode(*mode); err != nil {
		return common.UsageError(fs, stderr, "%v", err)
	}
	if *limit < 0 {
		return common.UsageError(fs, stderr, "--limit must be greater than or equal to 0")
	}
	if *batchSize < 1 {
		return common.UsageError(fs, stderr, "--batch-size must be at least 1")
	}
	if *watch < 0 {
		return common.UsageError(fs, stderr, "--watch must not be negative")
	}
	if *interval <= 0 {
		return common.UsageError(fs, stderr, "--interval must be positive")
	}
	if *maxInterval <= 0 {
		return common.UsageError(fs, stderr, "--max-interval must be positive")
	}
	if *idleAfter < 0 {
		return common.UsageError(fs, stderr, "--idle-after must not be negative")
	}

	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest agent-sessions: %v\n", err)
		return common.ExitFailure
	}
	cfg = cfg.WithEnvFallback(env)
	account := env.AgentSessionsAccount()
	if account == "" {
		fmt.Fprintln(stderr, "pdw ingest agent-sessions: AGENT_SESSIONS_ACCOUNT or GMAIL_ACCOUNTS must be set for agent session sync")
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
		fmt.Fprintf(stderr, "pdw ingest agent-sessions: %v\n", err)
		return common.ExitFailure
	}
	defer state.Close()

	// Uploads always go through the app, which owns the Drive credential,
	// folder, object keys, kinds, and pdw_* tags. The device holds none of
	// that: only the app URL and signing key.
	client, err := ingestclient.FromEnv(env, cfg, logger)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest agent-sessions: %v\n", err)
		return common.ExitFailure
	}
	runner := &Runner{
		Account: account,
		Device:  env.DeviceName(),
		Dirs:    DirsFromEnv(env),
		Upload: func(gzipBytes []byte, exportedAt time.Time) (ingestclient.StoredObject, error) {
			return client.UploadAgentSessionsBatch(gzipBytes, common.ISOFormat(exportedAt))
		},
		Logger:            logger,
		State:             state,
		Mode:              *mode,
		Limit:             *limit,
		BatchSize:         *batchSize,
		BeforeUploadCheck: common.UploadGuard(env, "AGENT_SESSIONS", cfg.BaseURL),
		Observe:           client.PostSourceObservation,
	}
	if *watch > 0 {
		return runWatch(runner, *watch, *interval, *maxInterval, *idleAfter, stdout, stderr)
	}
	summary, err := runner.Sync()
	if err != nil {
		var blocked *ErrUploadBlocked
		if errors.As(err, &blocked) {
			fmt.Fprintf(stdout, "Agent sessions upload skipped: %s\n", blocked.Reason)
			return 0
		}
		fmt.Fprintf(stderr, "pdw ingest agent-sessions: %v\n", err)
		return common.ExitFailure
	}
	fmt.Fprintf(stdout, "Agent sessions upload complete: files=%d new=%d lines=%d skipped=%d batches=%d\n",
		summary.FilesSeen, summary.FilesWithNewLines, summary.LinesSelected, summary.LinesSkipped, summary.BatchesUploaded)
	return 0
}

// DefaultWatchInterval is the base pause between --watch passes while any
// transcript was modified within DefaultIdleAfter (always, on a host with an
// OpenClaw SQLite store), keeping the device's share of active-session landing
// latency to about a second.
const DefaultWatchInterval = 2 * time.Second

// DefaultMaxWatchInterval caps the pause between passes once a host without an
// OpenClaw SQLite store has had no transcript modifications for
// DefaultIdleAfter. On an idle host this runs ~10 passes per five-minute window
// (well under 1% CPU on porygon); the first line written after a long idle
// waits at most 30 s before resetting to DefaultWatchInterval.
const DefaultMaxWatchInterval = 30 * time.Second

// DefaultIdleAfter is how long all transcripts must be unmodified before
// --watch backs off from DefaultWatchInterval. Long enough that ordinary
// model-thinking gaps (40–120 s) stay at 2 s with zero added latency; judged
// from on-disk mtimes so an already-idle host backs off from pass 1 of every
// five-minute window. A host with an OpenClaw SQLite store never backs off.
const DefaultIdleAfter = 10 * time.Minute

// defaultMtimeSettleWindow guards the OpenClaw store against a write in the
// same mtime tick as the read.
const defaultMtimeSettleWindow = 2 * time.Second

// runWatch is `--watch`: Sync until the window closes, staying at interval
// while any transcript has been modified within idleAfter and backing off up to
// maxInterval once the host has been quiet for idleAfter (including from pass 1
// after a restart when on-disk mtimes are already older than idleAfter or no
// transcripts exist). Any new lines or fresh mtime resets the pause to interval
// immediately. A host with an OpenClaw SQLite store never backs off (see
// hasOpenClawStore). A failed pass ends the window non-zero (the wrapper records
// it; the supervisor restarts the uploader); a blocked network ends it as a
// deliberate skip, exactly as a one-shot run reports one.
func runWatch(runner *Runner, window, interval, maxInterval, idleAfter time.Duration, stdout, stderr io.Writer) int {
	if maxInterval < interval {
		maxInterval = interval
	}
	runner.SkipUnchanged = true
	runner.MtimeSettleWindow = defaultMtimeSettleWindow
	deadline := time.Now().Add(window)
	var total Summary
	passes := 0
	var lastActivity time.Time
	currentInterval := interval
	for {
		summary, err := runner.Sync()
		passes++
		total.LinesSelected += summary.LinesSelected
		total.LinesSkipped += summary.LinesSkipped
		total.BatchesUploaded += summary.BatchesUploaded
		total.FilesSeen = summary.FilesSeen
		if err != nil {
			var blocked *ErrUploadBlocked
			if errors.As(err, &blocked) {
				fmt.Fprintf(stdout, "Agent sessions upload skipped: %s\n", blocked.Reason)
				return 0
			}
			fmt.Fprintf(stderr, "pdw ingest agent-sessions: %v\n", err)
			return common.ExitFailure
		}
		now := time.Now().UTC()
		if summary.LatestModTime.After(lastActivity) {
			lastActivity = summary.LatestModTime
		}
		active := summary.LinesSelected > 0 || summary.LinesSkipped > 0 || summary.LimitReached || summary.FilesWithNewLines > 0
		if active || runner.hasOpenClawStore() {
			if active {
				lastActivity = now
			}
			currentInterval = interval
		} else if lastActivity.IsZero() || now.Sub(lastActivity) >= idleAfter {
			// Already idle past idleAfter (including pass 1 after a 5-minute
			// restart on an idle host or a host with zero transcripts).
			if passes == 1 {
				currentInterval = maxInterval
			} else if currentInterval < maxInterval {
				currentInterval *= 2
				if currentInterval > maxInterval {
					currentInterval = maxInterval
				}
			}
		} else {
			currentInterval = interval
		}
		rem := time.Until(deadline)
		if rem < interval {
			break
		}
		sleep := currentInterval
		if sleep > rem {
			sleep = rem
		}
		time.Sleep(sleep)
	}
	fmt.Fprintf(stdout, "Agent sessions watch complete: passes=%d files=%d lines=%d skipped=%d batches=%d\n",
		passes, total.FilesSeen, total.LinesSelected, total.LinesSkipped, total.BatchesUploaded)
	return 0
}
