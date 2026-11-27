package x

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest x --help`.
const Usage = `pdw ingest x - upload Zach's own X (Twitter) account: his posts, likes, bookmarks and mentions.

USAGE
  pdw ingest x [--mode incremental|full] [--streams posts,likes,bookmarks,mentions]
               [--pages N] [--backfill-pages N] [--browser NAME] [--dry-run]
               [--state-file PATH] [--lock-file PATH]

Reads the x.com login from this Mac's Chrome-family browser (one keychain
prompt, "Always Allow"), then reads the account through the same read-only
GraphQL queries the x.com web client uses -- his posts and replies, his likes,
his bookmarks and his mentions -- and posts them as gzipped batches to the
app's /ingest/x/batch endpoint. Every post a page shows rides along: the post
he replied to, the post he quoted or reposted. Nothing is ever written to X.

Each run reads the newest pages of every stream until it reaches posts it has
already shipped, then walks a few more pages back into history from where the
last run stopped, so a first sync of years of likes takes many runs rather
than one burst. A rate limit stops that stream until the next run. The cookie
never leaves this Mac and never reaches stdout.

FLAGS
  --mode MODE            incremental (default) or full (restart every stream's
                         history walk; the warehouse upserts).
  --streams LIST         Comma-separated subset of posts, likes, bookmarks, mentions.
  --pages N              Newest pages read per stream per run (default 3).
  --backfill-pages N     History pages read per stream per run (default 5; 0 stops the walk).
  --browser NAME         Force a browser (chrome|brave|edge|arc|chromium|vivaldi).
  --dry-run              Read and count without uploading or recording state.
  --state-file PATH      Upload state (default: the Application Support state database).
  --lock-file PATH       Nonblocking lock against overlapping LaunchAgent runs.

ENVIRONMENT
  X_ACCOUNT              Required: the X handle (e.g. zachlatta). The run refuses
                         a browser signed in to a different account.
  X_USER_AGENT           User-Agent for x.com (default: desktop Chrome on macOS).
  X_REQUEST_DELAY_MS     Pause between x.com requests (default 2000).
  PDW_INGEST_PROJECT_DIR Directory whose .env is loaded (default: current directory).
`

const skipMessage = "X upload skipped: another uploader run is active"

// Deps are the seams Run wires to the real machine; tests inject fakes.
type Deps struct {
	Capture func(browser string) (Session, error)
	NewAPI  func(Session, common.Getenv) API
}

func defaultDeps() Deps {
	host := chromium.DefaultHost()
	return Deps{
		Capture: func(browser string) (Session, error) { return Capture(host, browser) },
		NewAPI: func(session Session, env common.Getenv) API {
			return NewClient(session, env.Env("X_BASE_URL"), env.Env("X_USER_AGENT"))
		},
	}
}

// Run is the `pdw ingest x` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	return RunWith(args, stdout, stderr, getenv, cfg, defaultDeps())
}

// RunWith is Run with injectable dependencies.
func RunWith(args []string, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config, deps Deps) int {
	fs := common.NewFlagSet("pdw ingest x")
	mode := fs.String("mode", "incremental", "")
	streamList := fs.String("streams", "", "")
	pages := fs.Int("pages", DefaultIncrementalPages, "")
	backfillPages := fs.Int("backfill-pages", DefaultBackfillPages, "")
	browser := fs.String("browser", "", "")
	dryRun := fs.Bool("dry-run", false, "")
	stateFile := fs.String("state-file", DefaultStateFile(), "")
	lockFile := fs.String("lock-file", common.LockFileFor(DefaultStateFile()), "")
	if _, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr); done {
		return code
	}
	if err := common.ValidateMode(*mode); err != nil {
		return common.UsageError(fs, stderr, "%v", err)
	}
	if *pages < 1 || *backfillPages < 0 {
		return common.UsageError(fs, stderr, "--pages must be at least 1 and --backfill-pages at least 0")
	}
	streams, err := SelectStreams(*streamList)
	if err != nil {
		return common.UsageError(fs, stderr, "%v", err)
	}

	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest x: %v\n", err)
		return common.ExitFailure
	}
	cfg = cfg.WithEnvFallback(env)
	account := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(env.Env("X_ACCOUNT")), "@"))
	if account == "" {
		fmt.Fprintln(stderr, "pdw ingest x: X_ACCOUNT must be set to the X handle to sync (e.g. zachlatta)")
		return common.ExitFailure
	}
	logger := common.NewWriterLogger(stdout)

	lock, code, done := common.AcquireRunLock(*lockFile, skipMessage, stdout, stderr)
	if done {
		return code
	}
	defer lock.Release()

	session, err := deps.Capture(*browser)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest x: %v\n", err)
		return common.ExitFailure
	}
	logger.Infof("Using the x.com session from %s (login %s)", session.Browser, session.Fingerprint())
	api := deps.NewAPI(session, env)
	if err := api.Discover(); err != nil {
		fmt.Fprintf(stderr, "pdw ingest x: %v\n", err)
		return common.ExitFailure
	}
	user, err := api.Query("UserByScreenName", map[string]any{"screen_name": account})
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest x: could not look up @%s: %v\n", account, err)
		return common.ExitFailure
	}
	userID := UserID(user)
	if userID == "" {
		fmt.Fprintf(stderr, "pdw ingest x: x.com has no account @%s\n", account)
		return common.ExitFailure
	}
	if session.UserID != "" && session.UserID != userID {
		fmt.Fprintf(stderr, "pdw ingest x: the browser is signed in to a different X account than @%s; refusing to read another account's likes and bookmarks as this one's\n", account)
		return common.ExitFailure
	}

	runner := &Runner{
		Account:       account,
		UserID:        userID,
		API:           api,
		Logger:        logger,
		Streams:       streams,
		Mode:          *mode,
		Pages:         *pages,
		BackfillPages: *backfillPages,
		Delay:         requestDelay(env),
	}
	if *dryRun {
		runner.Client = &dryRunUploader{}
		// No state: a dry run reads only the newest pages and records nothing.
		runner.BackfillPages = 0
	} else {
		state, err := OpenState(*stateFile, account)
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest x: %v\n", err)
			return common.ExitFailure
		}
		defer state.Close()
		ingest, err := ingestclient.FromEnv(env, cfg, logger)
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest x: %v\n", err)
			return common.ExitFailure
		}
		runner.State = state
		runner.Client = ingest
		runner.Observe = ingest.PostSourceObservation
		runner.BeforeUploadCheck = common.UploadGuard(env, "X", cfg.BaseURL)
	}
	summary, err := runner.Sync()
	fmt.Fprintf(stdout, "X upload complete: pages=%d posts=%d relations=%d batches=%d rate_limited=%s deferred=%t failed=%d dry_run=%t\n",
		summary.Pages, summary.Posts, summary.Relations, summary.Batches, strings.Join(summary.RateLimited, ","),
		summary.Deferred, len(summary.Failed), *dryRun)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest x: %v\n", err)
		var auth *AuthError
		if errors.As(err, &auth) {
			fmt.Fprintln(stderr, "pdw ingest x: open x.com in Chrome on this Mac, sign in as @"+account+", then kickstart the LaunchAgent")
		}
		return common.ExitFailure
	}
	return 0
}

func requestDelay(env common.Getenv) time.Duration {
	raw := env.Env("X_REQUEST_DELAY_MS")
	if raw == "" {
		return DefaultRequestDelay
	}
	var ms int
	if _, err := fmt.Sscanf(raw, "%d", &ms); err != nil || ms < 0 {
		return DefaultRequestDelay
	}
	return time.Duration(ms) * time.Millisecond
}

// dryRunUploader counts what a real run would post.
type dryRunUploader struct{ batches int }

func (d *dryRunUploader) UploadXBatch(gzipBytes []byte, _ string) (ingestclient.StoredObject, error) {
	d.batches++
	return ingestclient.StoredObject{StorageKey: fmt.Sprintf("dry-run-%d (%d gzipped bytes)", d.batches, len(gzipBytes))}, nil
}

// SelectStreams resolves --streams (empty means every stream).
func SelectStreams(list string) ([]Stream, error) {
	if strings.TrimSpace(list) == "" {
		return Streams, nil
	}
	var out []Stream
	for _, name := range strings.Split(list, ",") {
		name = strings.ToLower(strings.TrimSpace(name))
		stream, ok := StreamByName(name)
		if !ok {
			return nil, fmt.Errorf("unknown stream %q (want posts, likes, bookmarks or mentions)", name)
		}
		out = append(out, stream)
	}
	return out, nil
}
