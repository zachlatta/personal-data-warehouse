package main

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/agentsessions"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/applecontacts"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/applemessages"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/applenotes"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/manualfinance"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/photos"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/plaid"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/voicememos"
)

// localCommand is the shared entry point of every local (non-/api/tools)
// command pdw hosts natively: the uploaders, the browser-session publishers
// and the mutation workers. Each parses its own flags from args, so pdw
// forwards everything after the source/verb verbatim, and each receives the
// warehouse URL + token pdw resolved (cfg) and applies its own .env fallback.
type localCommand func(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int

// ingestSources maps a `pdw ingest <source>` name to the Go uploader that
// implements it. It is a package var so tests can swap in a fake that records
// the invocation instead of scanning a Mac.
//
// WhatsApp is deliberately absent: it runs in-process inside the production
// Dagster deployment, not as a launchd/systemd uploader.
//
// "claude-desktop" is also absent: it is the clientside *auth* pusher
// (claudedesktop.go) and runIngestWithConfig dispatches it before this table.
// (Its actual conversation polling runs serverside in Dagster.)
var ingestSources = map[string]localCommand{
	"agent-sessions": agentsessions.Run,
	"apple-contacts": applecontacts.Run,
	"apple-messages": applemessages.Run,
	"apple-notes":    applenotes.Run,
	"apple-photos":   photos.Run,
	"manual-finance": manualfinance.Run,
	"plaid":          plaid.Run,
	"voice-memos":    voicememos.Run,
}

// resolveIngestSource returns the uploader for a source, or ok=false when the
// source is unknown.
func resolveIngestSource(source string) (localCommand, bool) {
	run, ok := ingestSources[source]
	return run, ok
}

// ingestSourceNames returns the known source names in stable, sorted order for
// help and error messages.
func ingestSourceNames() []string {
	names := make([]string, 0, len(ingestSources)+1)
	for name := range ingestSources {
		names = append(names, name)
	}
	names = append(names, claudeDesktopSource)
	sort.Strings(names)
	return names
}

const ingestUsage = `pdw ingest - run a local data-warehouse uploader.

USAGE
  pdw ingest <source> [uploader flags...]

Every uploader is built into this binary; nothing here runs Python. Every flag
after <source> is forwarded verbatim to the uploader (e.g. --mode
incremental|full, --limit N). Run "pdw ingest <source> --help" to see a
source's own flags.

SOURCES
  voice-memos      Upload local macOS Voice Memos recordings
  apple-notes      Upload local Apple Notes
  apple-messages   Upload local Apple Messages (iMessage/SMS/RCS)
  apple-contacts   Upload local Apple/iCloud Contacts
  agent-sessions   Upload AI agent CLI session transcripts
  apple-photos     Upload local Apple Photos originals + metadata
  claude-desktop   Push the Claude Desktop (claude.ai) session credential
  plaid            Link, repair, list and unlink Plaid Items (sync runs in Dagster)
  manual-finance   Upload finance documents (statements, valuations, exports)

The uploader posts to the warehouse over the same URL + token pdw uses for
everything else: run "pdw login" once (or set PDW_API_URL + PDW_SECRET_TOKEN)
and uploads are configured too; there is no separate ingest URL.

ENVIRONMENT
  PDW_INGEST_PROJECT_DIR  Directory whose .env the uploader loads for its own
                          settings (accounts, direct-origin hosts, ...); the
                          environment wins over the file. Default: the current
                          directory.
  PDW_API_URL             Warehouse URL the uploader posts to (else "pdw login").
  PDW_SECRET_TOKEN        App secret token used to sign uploads (else "pdw login").

EXAMPLES
  pdw ingest voice-memos --mode incremental
  pdw ingest apple-notes --mode full
  pdw ingest agent-sessions --limit 1000
  pdw ingest plaid link              # genuinely new institution only
  pdw ingest plaid update <item-id>   # repair an existing Item, preserving its identity
  pdw ingest plaid items              # list what is linked
  pdw ingest plaid unlink <item-id>   # retire an item a re-link left behind
  pdw ingest manual-finance ~/Desktop/accounts
`

// runIngest parses `pdw ingest` arguments and runs the matching uploader. It
// never talks to /api/tools, so it must be dispatched before the API-config
// resolution in run().
func runIngest(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string) int {
	return runIngestWithConfig(args, stdin, stdout, stderr, getenv, "", "")
}

func runIngestWithConfig(
	args []string,
	stdin io.Reader,
	stdout,
	stderr io.Writer,
	getenv func(string) string,
	flagBaseURL,
	flagToken string,
) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "pdw ingest: a source is required")
		fmt.Fprint(stderr, ingestUsage)
		return 2
	}
	source := args[0]
	if source == "-h" || source == "--help" {
		fmt.Fprint(stdout, ingestUsage)
		return 0
	}
	// claude-desktop is the credential pusher, with its own flag set, so it is
	// dispatched before the uploader table.
	if source == claudeDesktopSource {
		return runClaudeDesktopAuth(args[1:], stdout, stderr, getenv, flagBaseURL, flagToken)
	}
	run, ok := resolveIngestSource(source)
	if !ok {
		fmt.Fprintf(stderr, "pdw ingest: unknown source %q; valid sources: %s\n", source, strings.Join(ingestSourceNames(), ", "))
		return 2
	}
	return run(args[1:], stdin, stdout, stderr, getenv, resolveLocalConfig(getenv, flagBaseURL, flagToken))
}

// resolveLocalConfig returns the warehouse base URL and token with the same
// precedence pdw uses everywhere else (root flags, then PDW_API_URL /
// PDW_SECRET_TOKEN, then the `pdw login` config file). The ingest signing key
// is the app secret token, which is exactly the token pdw already holds, so a
// single login configures uploads too. Either value may be empty when nothing
// is configured; each local command applies its own .env fallback and names
// what is missing.
func resolveLocalConfig(getenv func(string) string, flagBaseURL, flagToken string) ingestclient.Config {
	return ingestclient.ResolveConfig(getenv, strings.TrimSpace(flagBaseURL), strings.TrimSpace(flagToken))
}

// resolveIngestWarehouse is resolveLocalConfig as the (baseURL, token) pair
// the claude-desktop pusher reads.
func resolveIngestWarehouse(getenv func(string) string, flagBaseURL, flagToken string) (baseURL, token string) {
	cfg := resolveLocalConfig(getenv, flagBaseURL, flagToken)
	return cfg.BaseURL, cfg.Token
}
