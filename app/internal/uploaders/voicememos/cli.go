package voicememos

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultRecordingsPath and DefaultExtensions live in scanner.go; these are
// the CLI's remaining defaults (personal_data_warehouse_voice_memos.cli).
const (
	defaultMinFileAgeSeconds = 120
	writebackQueryTimeout    = 30 * time.Second
)

// Options are the parsed `pdw ingest voice-memos` flags.
type Options struct {
	Limit              int
	Workers            int
	Mode               string
	StateFile          string
	MinFileAgeSeconds  int
	LockFile           string
	RecordingsPath     string
	NetworkDiagnostics bool
	NoWriteback        bool
	WritebackOnly      bool
	WritebackDryRun    bool
	WritebackLimit     int
}

// Settings is what the Python load_settings(require_voice_memos=True)
// resolved for this uploader.
type Settings struct {
	Account        string
	RecordingsPath string
	Extensions     []string
}

// SettingsFromEnv mirrors config.py: VOICE_MEMOS_ACCOUNT (else the first
// GMAIL_ACCOUNTS entry), VOICE_MEMOS_RECORDINGS_PATH (expanded) and
// VOICE_MEMOS_EXTENSIONS (comma-separated, dot-prefixed, lower-cased).
func SettingsFromEnv(getenv common.Getenv) (Settings, error) {
	account := getenv.VoiceMemosAccount()
	if account == "" {
		return Settings{}, errors.New("VOICE_MEMOS_ACCOUNT or GMAIL_ACCOUNTS must be set for Voice Memos sync")
	}
	extensions := getenv.CSV("VOICE_MEMOS_EXTENSIONS")
	if len(extensions) == 0 {
		extensions = append([]string(nil), DefaultExtensions...)
	}
	for i, extension := range extensions {
		if !strings.HasPrefix(extension, ".") {
			extension = "." + extension
		}
		extensions[i] = strings.ToLower(extension)
	}
	return Settings{
		Account:        account,
		RecordingsPath: getenv.Path("VOICE_MEMOS_RECORDINGS_PATH", DefaultRecordingsPath),
		Extensions:     extensions,
	}, nil
}

// WritebackEnabledFromEnv is the VOICE_MEMOS_WRITEBACK_ENABLED kill switch
// (default on; 0/false/no/off disable it).
func WritebackEnabledFromEnv(getenv common.Getenv) bool {
	return getenv.Enabled("VOICE_MEMOS_WRITEBACK_ENABLED")
}

// ParseArgs parses the uploader's flags (the argparse surface of cli.py).
func ParseArgs(args []string, helpOut io.Writer) (Options, error) {
	defaultState := DefaultStateFile()
	opts := Options{}
	fs := flag.NewFlagSet("voice-memos", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.IntVar(&opts.Limit, "limit", 0, "Maximum recordings to upload; 0 means no limit")
	fs.IntVar(&opts.Workers, "workers", 0, "Number of parallel upload workers; 0 picks a mode-specific default")
	fs.StringVar(&opts.Mode, "mode", "incremental", "Upload mode: incremental or full")
	fs.StringVar(&opts.StateFile, "state-file", defaultState, "Incremental upload state path")
	fs.IntVar(&opts.MinFileAgeSeconds, "min-file-age-seconds", defaultMinFileAgeSeconds, "Do not upload recordings modified more recently than this")
	fs.StringVar(&opts.LockFile, "lock-file", common.WithSuffix(defaultState, ".lock"), "Nonblocking lock path used to avoid overlapping cron runs")
	fs.StringVar(&opts.RecordingsPath, "recordings-path", "", "Voice Memos recordings directory (default: VOICE_MEMOS_RECORDINGS_PATH or the system store)")
	fs.BoolVar(&opts.NetworkDiagnostics, "network-diagnostics", false, "Print network guard diagnostics and exit")
	fs.BoolVar(&opts.NoWriteback, "no-writeback", false, "Skip the enriched-title write-back into the Voice Memos app after uploading")
	fs.BoolVar(&opts.WritebackOnly, "writeback-only", false, "Run only the enriched-title write-back, skipping the upload phase")
	fs.BoolVar(&opts.WritebackDryRun, "writeback-dry-run", false, "Log the renames write-back would apply without writing to the Voice Memos store")
	fs.IntVar(&opts.WritebackLimit, "writeback-limit", 0, "Maximum renames to apply per run; 0 means no limit")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(helpOut, Usage())
			return opts, flag.ErrHelp
		}
		return opts, err
	}
	if fs.NArg() > 0 {
		return opts, fmt.Errorf("unrecognized arguments: %s", strings.Join(fs.Args(), " "))
	}
	if opts.Mode != "incremental" && opts.Mode != "full" {
		return opts, fmt.Errorf("argument --mode: invalid choice: '%s' (choose from 'incremental', 'full')", opts.Mode)
	}
	return opts, nil
}

// Usage is the --help text.
func Usage() string {
	return `usage: pdw ingest voice-memos [-h] [--limit LIMIT] [--workers WORKERS]
                              [--mode {incremental,full}] [--state-file STATE_FILE]
                              [--min-file-age-seconds MIN_FILE_AGE_SECONDS]
                              [--lock-file LOCK_FILE] [--recordings-path RECORDINGS_PATH]
                              [--network-diagnostics] [--no-writeback]
                              [--writeback-only] [--writeback-dry-run]
                              [--writeback-limit WRITEBACK_LIMIT]

Upload local macOS Voice Memos files through the app ingest API.

options:
  -h, --help            show this help message and exit
  --limit LIMIT         Maximum recordings to upload; 0 means no limit
  --workers WORKERS     Number of parallel upload workers; 0 picks a mode-specific default
  --mode {incremental,full}
                        Upload mode
  --state-file STATE_FILE
                        Incremental upload state path
  --min-file-age-seconds MIN_FILE_AGE_SECONDS
                        Do not upload recordings modified more recently than this
  --lock-file LOCK_FILE
                        Nonblocking lock path used to avoid overlapping cron runs
  --recordings-path RECORDINGS_PATH
                        Voice Memos recordings directory (default: VOICE_MEMOS_RECORDINGS_PATH or the system store)
  --network-diagnostics
                        Print network guard diagnostics and exit
  --no-writeback        Skip the enriched-title write-back into the Voice Memos app after uploading
  --writeback-only      Run only the enriched-title write-back, skipping the upload phase
  --writeback-dry-run   Log the renames write-back would apply without writing to the Voice Memos store
  --writeback-limit WRITEBACK_LIMIT
                        Maximum renames to apply per run; 0 means no limit
`
}

// GetenvWithDotenv overlays the repo .env (python-dotenv semantics: the
// process environment wins) under getenv. The Python uploader loaded it
// through load_settings(); dir is the project directory (PDW_INGEST_PROJECT_DIR
// when set, else the working directory).
func GetenvWithDotenv(getenv func(string) string, dir string) func(string) string {
	if dir == "" {
		dir = strings.TrimSpace(getenv("PDW_INGEST_PROJECT_DIR"))
	}
	if dir == "" {
		if cwd, err := os.Getwd(); err == nil {
			dir = cwd
		}
	}
	values, err := common.LoadDotenv(filepath.Join(dir, ".env"))
	if err != nil || len(values) == 0 {
		return getenv
	}
	return func(name string) string {
		if value := getenv(name); value != "" {
			return value
		}
		return values[name]
	}
}

// Run is the `pdw ingest voice-memos` entry point. Exit codes follow the
// Python CLI: 0 on success or a deliberate skip (lock held, network guard),
// 1 when a run failed after it started (so launchd sees the failure), 2 on a
// usage error.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	_ = stdin
	opts, err := ParseArgs(args, stdout)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "%s\nerror: %v\n", Usage(), err)
		return 2
	}
	env := common.Getenv(GetenvWithDotenv(getenv, ""))
	if opts.NetworkDiagnostics {
		fmt.Fprint(stdout, common.NetworkDiagnostics(newNetworkPolicy(env)))
		return 0
	}
	settings, err := SettingsFromEnv(env)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	if opts.RecordingsPath != "" {
		settings.RecordingsPath = common.ExpandUser(opts.RecordingsPath)
	}
	logger := common.NewWriterLogger(stdout)
	state := LoadState(opts.StateFile, settings.Account, settings.RecordingsPath)
	workers := DefaultWorkers(opts.Mode, opts.Workers)
	runUpload := !opts.WritebackOnly
	runWriteback := opts.WritebackOnly || (!opts.NoWriteback && WritebackEnabledFromEnv(env))

	lock, acquired, err := common.TryRunLock(opts.LockFile)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	if !acquired {
		fmt.Fprintln(stdout, "Voice Memos upload skipped: another uploader run is active")
		return 0
	}
	var summary *Summary
	var writebackSummary *WritebackSummary
	code := func() int {
		defer lock.Release()
		if runUpload {
			result, err := runUploadPhase(opts, settings, state, workers, env, cfg, logger)
			// The state is saved whether or not the run succeeded, so the
			// next run resumes past what landed.
			if saveErr := state.Save(opts.StateFile); saveErr != nil && err == nil {
				err = saveErr
			}
			if err != nil {
				fmt.Fprintf(stderr, "error: %v\n", err)
				return 1
			}
			summary = &result
		}
		if runWriteback {
			result, err := runWritebackPhase(opts, settings, state, env, cfg, logger)
			if err != nil {
				fmt.Fprintf(stderr, "error: %v\n", err)
				return 1
			}
			writebackSummary = &result
		}
		return 0
	}()
	if code != 0 {
		return code
	}
	if summary != nil {
		fmt.Fprintf(stdout, "Voice Memos upload complete: seen=%d selected=%d uploaded=%d skipped=%d deferred=%d metadata=%d\n",
			summary.RecordingsSeen, summary.RecordingsSelected, summary.RecordingsUploaded, summary.RecordingsSkipped, summary.RecordingsDeferred, summary.MetadataUploaded)
	}
	if writebackSummary != nil {
		suffix := ""
		if writebackSummary.DryRun {
			suffix = " (dry run)"
		}
		fmt.Fprintf(stdout, "Voice Memos write-back complete: local=%d auto_named=%d titles=%d planned=%d renamed=%d skipped=%d%s\n",
			writebackSummary.LocalRecordings, writebackSummary.AutoNamed, writebackSummary.EnrichedTitles, writebackSummary.Planned, writebackSummary.Renamed, writebackSummary.Skipped, suffix)
	}
	return 0
}

// DefaultWorkers resolves --workers: 0 picks the mode's default, one
// worker for an incremental run and eight for a full backfill.
func DefaultWorkers(mode string, workers int) int {
	if workers != 0 {
		return workers
	}
	if mode == "incremental" {
		return 1
	}
	return 8
}

// beforeUploadCheck builds the network guard; tests swap it so a run does not
// probe the host's real default route.
var beforeUploadCheck = common.BeforeUploadCheck

// newNetworkPolicy reads the VOICE_MEMOS_UPLOAD_* guard settings (the prefix
// every other uploader falls back to); tests swap it for a policy whose
// commands are canned.
var newNetworkPolicy = func(env common.Getenv) *common.NetworkPolicy {
	return common.NetworkPolicyFromEnv(env, "VOICE_MEMOS_UPLOAD", "")
}

func runUploadPhase(opts Options, settings Settings, state *State, workers int, env common.Getenv, cfg ingestclient.Config, logger common.Logger) (Summary, error) {
	client, err := ingestclient.FromEnv(env, cfg, logger)
	if err != nil {
		return Summary{}, err
	}
	minAge := opts.MinFileAgeSeconds
	if opts.Mode != "incremental" {
		minAge = 0
	}
	policy := newNetworkPolicy(env)
	runner := &Runner{
		Account:           settings.Account,
		RecordingsPath:    settings.RecordingsPath,
		Extensions:        settings.Extensions,
		Client:            client,
		Logger:            logger,
		Limit:             opts.Limit,
		Workers:           workers,
		Mode:              opts.Mode,
		State:             state,
		MinFileAgeSeconds: minAge,
		BeforeUploadCheck: beforeUploadCheck(policy, cfg.BaseURL, common.PreflightTimeout(env, "VOICE_MEMOS")),
		MaxUploadBytes:    client.EffectiveMaxUploadBytes(),
		EnsureApp: func(path string, logger common.Logger) AppKick {
			return EnsureVoiceMemosAppRunning(path, logger, env, "", nil)
		},
	}
	return runner.Sync()
}

func runWritebackPhase(opts Options, settings Settings, state *State, env common.Getenv, cfg ingestclient.Config, logger common.Logger) (WritebackSummary, error) {
	baseURL := strings.TrimSpace(cfg.BaseURL)
	token := strings.TrimSpace(cfg.Token)
	if baseURL == "" || token == "" {
		return WritebackSummary{}, errors.New("Voice Memos write-back requires PDW_API_URL and PDW_SECRET_TOKEN (or their MCP_* aliases)")
	}
	clientName := env.Env("PDW_CLIENT_NAME")
	if clientName == "" {
		clientName = "pdw"
	}
	runner := &WritebackRunner{
		RecordingsPath: settings.RecordingsPath,
		Account:        settings.Account,
		Query:          SQLToolQuerier(baseURL, clientName, token, writebackQueryTimeout),
		Logger:         logger,
		Limit:          opts.WritebackLimit,
		DryRun:         opts.WritebackDryRun,
		// The upload state caches each filename's audio sha; the write-back
		// uses it to re-identify memos whose filenames drifted.
		SHAByFilename: state.SHAByFilename(),
	}
	return runner.Run()
}
