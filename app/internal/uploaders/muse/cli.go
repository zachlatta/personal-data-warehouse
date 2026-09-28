package muse

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/agentsessions"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultHome is the Muse agent's home directory on its VM.
const DefaultHome = "/home/hatch"

// Usage is `pdw ingest muse --help`.
const Usage = `pdw ingest muse - upload a Muse agent's transcripts and workspace.

USAGE
  pdw ingest muse [--home DIR] [--mode incremental|full] [--limit N]
                  [--no-transcripts] [--no-files] [--state-dir DIR]

Runs ON the Muse VM (Meta's hosted personal agent). Tails every transcript
under <home>/agents/*/sessions/ into /ingest/agent-sessions/batch (source
"muse"), then snapshots the agent's persistent workspace -- memory, goals,
feed, podcasts, deliverables, uploads -- into the same batches, with any
binary or large file posted to /ingest/muse/file. Credentials, the browser
profile, Meta's product docs and runtime plumbing are never read.

FLAGS
  --home DIR          The Muse home (default $MUSE_HOME, else /home/hatch).
  --mode MODE         incremental (default) or full (re-send everything).
  --limit N           Maximum transcript lines, and separately changed files,
                      to upload this run; 0 (default) is unlimited.
  --no-transcripts    Skip the transcripts.
  --no-files          Skip the workspace files.
  --state-dir DIR     Incremental state and the run lock (default
                      <home>/.local/state/pdw, which survives a VM restart).

ENVIRONMENT
  MUSE_ACCOUNT (else AGENT_SESSIONS_ACCOUNT, GMAIL_ACCOUNTS[0])  Account label.
  MUSE_DEVICE                           Device label (default: muse).
  PDW_API_URL / PDW_SECRET_TOKEN        Warehouse app (else "pdw login").
`

const skipMessage = "Muse upload skipped: another uploader run is active"

// Run is the `pdw ingest muse` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest muse")
	home := fs.String("home", "", "")
	mode := fs.String("mode", "incremental", "")
	limit := fs.Int("limit", 0, "")
	noTranscripts := fs.Bool("no-transcripts", false, "")
	noFiles := fs.Bool("no-files", false, "")
	stateDir := fs.String("state-dir", "", "")
	if _, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr); done {
		return code
	}
	if err := common.ValidateMode(*mode); err != nil {
		return common.UsageError(fs, stderr, "%v", err)
	}
	if *limit < 0 {
		return common.UsageError(fs, stderr, "--limit must be greater than or equal to 0")
	}
	if *noTranscripts && *noFiles {
		return common.UsageError(fs, stderr, "--no-transcripts and --no-files together upload nothing")
	}

	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest muse: %v\n", err)
		return common.ExitFailure
	}
	cfg = cfg.WithEnvFallback(env)
	root := strings.TrimSpace(*home)
	if root == "" {
		root = strings.TrimSpace(env("MUSE_HOME"))
	}
	if root == "" {
		root = DefaultHome
	}
	root = filepath.Clean(common.ExpandUser(root))
	account := strings.TrimSpace(env("MUSE_ACCOUNT"))
	if account == "" {
		account = env.AgentSessionsAccount()
	}
	if account == "" {
		fmt.Fprintln(stderr, "pdw ingest muse: MUSE_ACCOUNT (or AGENT_SESSIONS_ACCOUNT / GMAIL_ACCOUNTS) must be set")
		return common.ExitFailure
	}
	device := strings.TrimSpace(env("MUSE_DEVICE"))
	if device == "" {
		device = "muse"
	}
	states := strings.TrimSpace(*stateDir)
	if states == "" {
		states = filepath.Join(root, ".local", "state", "pdw")
	}
	logger := common.NewWriterLogger(stdout)

	lock, code, done := common.AcquireRunLock(filepath.Join(states, "muse-upload.lock"), skipMessage, stdout, stderr)
	if done {
		return code
	}
	defer lock.Release()

	client, err := ingestclient.FromEnv(env, cfg, logger)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest muse: %v\n", err)
		return common.ExitFailure
	}
	uploadBatch := func(gzipBytes []byte, exportedAt time.Time) (ingestclient.StoredObject, error) {
		return client.UploadAgentSessionsBatch(gzipBytes, common.ISOFormat(exportedAt))
	}
	guard := common.UploadGuard(env, "MUSE", cfg.BaseURL)

	transcripts := agentsessions.Summary{}
	if !*noTranscripts {
		state, err := agentsessions.OpenState(filepath.Join(states, "muse-transcripts-state.sqlite"), account)
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest muse: %v\n", err)
			return common.ExitFailure
		}
		runner := &agentsessions.Runner{
			Account:           account,
			Device:            device,
			Dirs:              agentsessions.Dirs{MuseAgents: filepath.Join(root, "agents")},
			Upload:            uploadBatch,
			Logger:            logger,
			State:             state,
			Mode:              *mode,
			Limit:             *limit,
			BatchSize:         agentsessions.DefaultBatchSize,
			BeforeUploadCheck: guard,
		}
		transcripts, err = runner.Sync()
		state.Close()
		if err != nil {
			var blocked *agentsessions.ErrUploadBlocked
			if errors.As(err, &blocked) {
				fmt.Fprintf(stdout, "Muse upload skipped: %s\n", blocked.Reason)
				return 0
			}
			fmt.Fprintf(stderr, "pdw ingest muse: transcripts: %v\n", err)
			return common.ExitFailure
		}
	}

	workspace := WorkspaceSummary{}
	if !*noFiles {
		state, err := OpenWorkspaceState(filepath.Join(states, "muse-workspace-state.sqlite"), account, root)
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest muse: %v\n", err)
			return common.ExitFailure
		}
		ws := &Workspace{
			Home:        root,
			Account:     account,
			Device:      device,
			UploadBatch: uploadBatch,
			UploadBlob: func(content []byte, extension, contentType string) (ingestclient.StoredObject, error) {
				return client.UploadMuseFile(content, extension, contentType)
			},
			Logger:       logger,
			State:        state,
			Mode:         *mode,
			Limit:        *limit,
			MaxBlobBytes: client.EffectiveMaxUploadBytes(),
		}
		workspace, err = ws.Sync()
		state.Close()
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest muse: workspace: %v\n", err)
			return common.ExitFailure
		}
	}

	fmt.Fprintf(stdout, "Muse upload complete: transcripts=%d lines=%d batches=%d; files seen=%d changed=%d blobs=%d deleted=%d too_large=%d batches=%d\n",
		transcripts.FilesSeen, transcripts.LinesSelected, transcripts.BatchesUploaded,
		workspace.FilesSeen, workspace.FilesChanged, workspace.FilesBlob, workspace.FilesDeleted, workspace.FilesTooLarge, workspace.BatchesUploaded)
	return 0
}
