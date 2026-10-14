package browserhistory

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest browser-history --help`.
const Usage = `pdw ingest browser-history - upload this Mac's Chrome, Brave and Safari history.

USAGE
  pdw ingest browser-history [--mode incremental|full] [--limit N]
                             [--browser chrome,brave,safari] [--dry-run]
                             [--state-file PATH] [--lock-file PATH]

Copies every profile's history database (with its WAL/journal) to a temporary
directory, reads the visits added since the last run, and posts them as
gzipped batches to the app's /ingest/browser-history/batch endpoint. Each
visit carries its URL, title, time, browser, profile, device, transition type
and, for a web search, the search term (Chromium's omnibox search terms, or
the query of a search-engine results URL).

Safari's History.db is readable only with Full Disk Access for the launching
executable chain (/bin/zsh and ~/.local/bin/pdw). A profile that cannot be
read fails the run AFTER every readable profile has uploaded.

FLAGS
  --mode MODE         incremental (default; visits above each profile's upload
                      mark) or full (re-send every visit; the warehouse upserts).
  --limit N           Maximum visits to upload this run; 0 (default) is unlimited.
  --browser LIST      Comma-separated subset of chrome, brave, safari.
  --dry-run           Read every profile and print what would upload; sends
                      nothing and leaves the upload state untouched.
  --state-file PATH   Upload state (default: the Application Support state database).
  --lock-file PATH    Nonblocking lock used to avoid overlapping LaunchAgent runs
                      (default: beside the state file).

ENVIRONMENT
  BROWSER_HISTORY_ACCOUNT (else GMAIL_ACCOUNTS[0])  Account the uploads are keyed by.
  BROWSER_HISTORY_DEVICE (else the short hostname)  Device the visits are attributed to.
  BROWSER_HISTORY_HOME (else $HOME)                 Home directory the browsers live under.
  PDW_INGEST_PROJECT_DIR                            Directory whose .env is loaded
                                                    (default: current directory).
`

const skipMessage = "Browser history upload skipped: another uploader run is active"

// Run is the `pdw ingest browser-history` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest browser-history")
	mode := fs.String("mode", "incremental", "")
	stateFile := fs.String("state-file", DefaultStateFile(), "")
	lockFile := fs.String("lock-file", common.LockFileFor(DefaultStateFile()), "")
	limit := fs.Int("limit", 0, "")
	browserList := fs.String("browser", "", "")
	dryRun := fs.Bool("dry-run", false, "")
	if _, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr); done {
		return code
	}
	if err := common.ValidateMode(*mode); err != nil {
		return common.UsageError(fs, stderr, "%v", err)
	}
	if *limit < 0 {
		return common.UsageError(fs, stderr, "--limit must be greater than or equal to 0")
	}
	browsers, err := SelectBrowsers(*browserList)
	if err != nil {
		return common.UsageError(fs, stderr, "%v", err)
	}

	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest browser-history: %v\n", err)
		return common.ExitFailure
	}
	cfg = cfg.WithEnvFallback(env)
	account := env.BrowserHistoryAccount()
	if account == "" {
		fmt.Fprintln(stderr, "pdw ingest browser-history: BROWSER_HISTORY_ACCOUNT or GMAIL_ACCOUNTS must be set for browser history sync")
		return common.ExitFailure
	}
	device := common.FirstNonEmpty(env.Env("BROWSER_HISTORY_DEVICE"), common.LocalDeviceName())
	home := common.FirstNonEmpty(env.Env("BROWSER_HISTORY_HOME"), common.HomeDir())
	logger := common.NewWriterLogger(stdout)

	lock, code, done := common.AcquireRunLock(*lockFile, skipMessage, stdout, stderr)
	if done {
		return code
	}
	defer lock.Release()

	var state *State
	var client Uploader
	guard := func() string { return "" }
	if *dryRun {
		// No state: a dry run reads every profile from the start.
		client = &dryRunUploader{out: stdout}
	} else {
		state, err = OpenState(*stateFile, account, device)
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest browser-history: %v\n", err)
			return common.ExitFailure
		}
		defer state.Close()
		ingest, err := ingestclient.FromEnv(env, cfg, logger)
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest browser-history: %v\n", err)
			return common.ExitFailure
		}
		client = ingest
		guard = common.UploadGuard(env, "BROWSER_HISTORY", cfg.BaseURL)
	}
	summary, err := (&Runner{
		Account:           account,
		Device:            device,
		Home:              home,
		Browsers:          browsers,
		Client:            client,
		Logger:            logger,
		State:             state,
		Mode:              *mode,
		Limit:             *limit,
		BeforeUploadCheck: guard,
	}).Sync()
	fmt.Fprintf(stdout, "Browser history upload complete: profiles=%d visits=%d searches=%d batches=%d deferred=%t failed=%d\n",
		summary.Profiles, summary.VisitsUploaded, summary.Searches, summary.BatchesUploaded, summary.Deferred, len(summary.Failed))
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest browser-history: %v\n", err)
		var permission *common.PermissionError
		if errors.As(err, &permission) || strings.Contains(err.Error(), "Full Disk Access") {
			fmt.Fprintln(stderr, "pdw ingest browser-history: grant Full Disk Access to /bin/zsh and ~/.local/bin/pdw in System Settings > Privacy & Security > Full Disk Access")
		}
		return common.ExitFailure
	}
	return 0
}

// dryRunUploader counts what a real run would post.
type dryRunUploader struct {
	out     io.Writer
	batches int
}

func (d *dryRunUploader) UploadBrowserHistoryBatch(gzipBytes []byte, exportedAt string) (ingestclient.StoredObject, error) {
	d.batches++
	return ingestclient.StoredObject{StorageKey: fmt.Sprintf("dry-run-%d (%d gzipped bytes)", d.batches, len(gzipBytes))}, nil
}

// SelectBrowsers resolves --browser (empty means every default browser).
func SelectBrowsers(list string) ([]Browser, error) {
	if strings.TrimSpace(list) == "" {
		return DefaultBrowsers, nil
	}
	known := map[string]Browser{}
	for _, browser := range DefaultBrowsers {
		known[browser.Name] = browser
	}
	var out []Browser
	for _, name := range strings.Split(list, ",") {
		name = strings.ToLower(strings.TrimSpace(name))
		browser, ok := known[name]
		if !ok {
			return nil, fmt.Errorf("unknown browser %q (want chrome, brave or safari)", name)
		}
		out = append(out, browser)
	}
	return out, nil
}
