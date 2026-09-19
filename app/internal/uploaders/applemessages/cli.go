package applemessages

import (
	"fmt"
	"io"
	"strconv"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest apple-messages --help`.
const Usage = `pdw ingest apple-messages - upload local Apple Messages (iMessage/SMS/RCS).

USAGE
  pdw ingest apple-messages [--mode incremental|full] [--limit N] [--workers N]
                            [--state-file PATH] [--lock-file PATH]
                            [--network-diagnostics]

Snapshots APPLE_MESSAGES_STORE_PATH (default ~/Library/Messages/chat.db), posts
changed handles, chats, messages and attachment manifests as gzipped batches to
/ingest/apple-messages/batch, and a bounded slice of attachment bytes per run
to /ingest/apple-messages/attachment.

FLAGS
  --mode MODE            incremental (default; skip unchanged records) or full.
  --limit N              Maximum manifest records to upload; 0 (default) is unlimited.
  --workers N            Parallel attachment upload workers; 0 (default) uses
                         APPLE_MESSAGES_UPLOAD_WORKERS or 4.
  --state-file PATH      Incremental upload state (default: the Application Support
                         state database).
  --lock-file PATH       Nonblocking lock used to avoid overlapping LaunchAgent runs
                         (default: beside the state file).
  --network-diagnostics  Print the network guard's verdict and exit.

ENVIRONMENT
  APPLE_MESSAGES_ACCOUNT (else GMAIL_ACCOUNTS[0])   Account the uploads are keyed by.
  APPLE_MESSAGES_STORE_PATH                         The chat.db to snapshot.
  APPLE_MESSAGES_ATTACHMENT_BYTES_PER_RUN           Attachment byte budget (default 512 MiB).
  APPLE_MESSAGES_ATTACHMENT_COUNT_PER_RUN           Attachment count budget (default 200).
  PDW_INGEST_PROJECT_DIR                            Directory whose .env is loaded
                                                    (default: current directory).
`

const (
	skipMessage                    = "Apple Messages upload skipped: another uploader run is active"
	defaultAttachmentBytesPerRun   = int64(512 * 1024 * 1024)
	defaultAttachmentCountPerRun   = 200
	defaultUploadWorkers           = 4
	attachmentBytesPerRunVariable  = "APPLE_MESSAGES_ATTACHMENT_BYTES_PER_RUN"
	attachmentCountPerRunVariable  = "APPLE_MESSAGES_ATTACHMENT_COUNT_PER_RUN"
	uploadWorkersVariable          = "APPLE_MESSAGES_UPLOAD_WORKERS"
	networkPolicyPrefix            = "APPLE_MESSAGES"
	networkPolicyFallbackPrefix    = "VOICE_MEMOS_UPLOAD"
	networkPolicyPrefixWithSection = networkPolicyPrefix + "_UPLOAD"
)

// Run is the `pdw ingest apple-messages` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest apple-messages")
	mode := fs.String("mode", "incremental", "")
	stateFile := fs.String("state-file", DefaultStateFile(), "")
	lockFile := fs.String("lock-file", common.LockFileFor(DefaultStateFile()), "")
	limit := fs.Int("limit", 0, "")
	workers := fs.Int("workers", 0, "")
	diagnostics := fs.Bool("network-diagnostics", false, "")
	if _, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr); done {
		return code
	}
	if err := common.ValidateMode(*mode); err != nil {
		return common.UsageError(fs, stderr, "%v", err)
	}
	if *limit < 0 {
		return common.UsageError(fs, stderr, "--limit must be greater than or equal to 0")
	}
	if *workers < 0 {
		return common.UsageError(fs, stderr, "--workers must be greater than or equal to 0")
	}

	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest apple-messages: %v\n", err)
		return common.ExitFailure
	}
	if *diagnostics {
		fmt.Fprint(stdout, common.NetworkDiagnostics(common.NetworkPolicyFromEnv(env, networkPolicyPrefixWithSection, networkPolicyFallbackPrefix)))
		return 0
	}
	cfg = cfg.WithEnvFallback(env)
	account := env.AppleMessagesAccount()
	if account == "" {
		fmt.Fprintln(stderr, "pdw ingest apple-messages: APPLE_MESSAGES_ACCOUNT or GMAIL_ACCOUNTS must be set for Apple Messages sync")
		return common.ExitFailure
	}
	storePath := env.Path("APPLE_MESSAGES_STORE_PATH", DefaultStorePath)
	bytesPerRun, err := envInt64(env, attachmentBytesPerRunVariable, defaultAttachmentBytesPerRun)
	if err != nil || bytesPerRun < 0 {
		fmt.Fprintf(stderr, "pdw ingest apple-messages: %s must be greater than or equal to 0\n", attachmentBytesPerRunVariable)
		return common.ExitFailure
	}
	countPerRun, err := envInt64(env, attachmentCountPerRunVariable, int64(defaultAttachmentCountPerRun))
	if err != nil || countPerRun < 0 {
		fmt.Fprintf(stderr, "pdw ingest apple-messages: %s must be greater than or equal to 0\n", attachmentCountPerRunVariable)
		return common.ExitFailure
	}
	envWorkers, err := envInt64(env, uploadWorkersVariable, int64(defaultUploadWorkers))
	if err != nil || envWorkers < 1 {
		fmt.Fprintf(stderr, "pdw ingest apple-messages: %s must be at least 1\n", uploadWorkersVariable)
		return common.ExitFailure
	}
	if *workers == 0 {
		*workers = int(envWorkers)
	}
	logger := common.NewWriterLogger(stdout)

	lock, code, done := common.AcquireRunLock(*lockFile, skipMessage, stdout, stderr)
	if done {
		return code
	}
	defer lock.Release()

	state, err := OpenState(*stateFile, account, storePath)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest apple-messages: %v\n", err)
		return common.ExitFailure
	}
	defer state.Close()

	client, err := ingestclient.FromEnv(env, cfg, logger)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest apple-messages: %v\n", err)
		return common.ExitFailure
	}
	summary, err := (&Runner{
		Account:               account,
		StorePath:             storePath,
		Client:                client,
		Logger:                logger,
		State:                 state,
		Mode:                  *mode,
		Limit:                 *limit,
		AttachmentBytesPerRun: bytesPerRun,
		AttachmentCountPerRun: int(countPerRun),
		Workers:               *workers,
		BeforeUploadCheck:     common.UploadGuard(env, networkPolicyPrefix, cfg.BaseURL),
	}).Sync()
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest apple-messages: %v\n", err)
		return common.ExitFailure
	}
	fmt.Fprintf(stdout, "Apple Messages upload complete: messages=%d attachments=%d selected=%d skipped=%d batches=%d attachment_uploads=%d attachment_deferred=%d\n",
		summary.MessagesSeen, summary.AttachmentsSeen, summary.RecordsSelected, summary.RecordsSkipped, summary.BatchesUploaded, summary.AttachmentsUploaded, summary.AttachmentsDeferred)
	return 0
}

func envInt64(env common.Getenv, name string, fallback int64) (int64, error) {
	raw := env.Env(name)
	if raw == "" {
		return fallback, nil
	}
	return strconv.ParseInt(raw, 10, 64)
}
