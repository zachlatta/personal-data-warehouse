package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliconfig"
)

// ingestModules maps a `pdw ingest <source>` name to the Python module that
// implements that uploader. Each module is runnable as `python -m <module>`
// (it has an `if __name__ == "__main__"` guard) and parses its own flags, so
// pdw forwards every flag after the source verbatim.
//
// Only the four scheduled, device-side client uploaders live here. WhatsApp is
// deliberately absent: it runs in-process inside the production Dagster
// deployment, not as a launchd/systemd uploader.
var ingestModules = map[string]string{
	"voice-memos":    "personal_data_warehouse_voice_memos.cli",
	"apple-notes":    "personal_data_warehouse_apple_notes.cli",
	"apple-messages": "personal_data_warehouse_apple_messages.cli",
	"agent-sessions": "personal_data_warehouse_agent_sessions.cli",
}

// ingestExec runs the resolved uploader command. It's a package var so tests
// can swap in a stub that records the invocation instead of spawning uv.
var ingestExec = runIngestProcess

// resolveIngestModule returns the Python module for a source, or ok=false when
// the source is unknown.
func resolveIngestModule(source string) (string, bool) {
	module, ok := ingestModules[source]
	return module, ok
}

// ingestSourceNames returns the known source names in stable, sorted order for
// help and error messages.
func ingestSourceNames() []string {
	names := make([]string, 0, len(ingestModules))
	for name := range ingestModules {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ingestArgv builds the argument vector passed to uv: it always runs the
// uploader module with `python -m`, then forwards the caller's passthrough
// flags unchanged.
func ingestArgv(module string, passthrough []string) []string {
	argv := []string{"run", "python", "-m", module}
	return append(argv, passthrough...)
}

// ingestUvBin resolves the uv launcher. PDW_UV_BIN lets deployment wrappers
// pin the absolute path (e.g. /opt/homebrew/bin/uv) since launchd/systemd run
// with a minimal PATH; otherwise we rely on uv being on PATH.
func ingestUvBin(getenv func(string) string) string {
	if v := strings.TrimSpace(getenv("PDW_UV_BIN")); v != "" {
		return v
	}
	return "uv"
}

// ingestProjectDir resolves the directory uv runs in. PDW_INGEST_PROJECT_DIR
// pins the repo checkout so uv discovers the right pyproject.toml and the
// uploader loads the repo's .env regardless of the caller's cwd. Empty means
// inherit the current working directory.
func ingestProjectDir(getenv func(string) string) string {
	return strings.TrimSpace(getenv("PDW_INGEST_PROJECT_DIR"))
}

const ingestUsage = `pdw ingest - run a local data-warehouse uploader through pdw.

USAGE
  pdw ingest <source> [uploader flags...]

Every flag after <source> is forwarded verbatim to the uploader (e.g.
--mode incremental|full, --limit N). Run "pdw ingest <source> --help" to see a
source's own flags.

SOURCES
  voice-memos      Upload local macOS Voice Memos recordings
  apple-notes      Upload local Apple Notes
  apple-messages   Upload local Apple Messages (iMessage/SMS/RCS)
  agent-sessions   Upload AI agent CLI session transcripts

The uploader posts to the warehouse over the same URL + token pdw uses for
everything else: run "pdw login" once (or set PDW_API_URL + PDW_SECRET_TOKEN)
and uploads are configured too; there is no separate ingest URL.

ENVIRONMENT
  PDW_UV_BIN              uv launcher path (default: uv on PATH).
  PDW_INGEST_PROJECT_DIR  Repo checkout uv runs in (default: current directory).
  PDW_API_URL            Warehouse URL the uploader posts to (else "pdw login").
  PDW_SECRET_TOKEN       App secret token used to sign uploads (else "pdw login").

EXAMPLES
  pdw ingest voice-memos --mode incremental
  pdw ingest apple-notes --mode full
  pdw ingest agent-sessions --limit 1000
`

// runIngest parses `pdw ingest` arguments and runs the matching uploader. It
// never talks to the warehouse API, so it must be dispatched before the
// API-config resolution in run().
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
	module, ok := resolveIngestModule(source)
	if !ok {
		fmt.Fprintf(stderr, "pdw ingest: unknown source %q; valid sources: %s\n", source, strings.Join(ingestSourceNames(), ", "))
		return 2
	}
	passthrough := args[1:]
	argv := ingestArgv(module, passthrough)
	return ingestExec(
		ingestUvBin(getenv),
		argv,
		ingestProjectDir(getenv),
		ingestEnvAdditions(getenv, flagBaseURL, flagToken),
		stdin,
		stdout,
		stderr,
	)
}

// resolveIngestWarehouse returns the warehouse base URL and token using the
// same precedence pdw uses everywhere else (flags, env, then the `pdw login`
// config file). The ingest signing key is the app secret token, which is
// exactly the token pdw already holds, so a single login configures uploads
// too. Either value may be empty when nothing is configured.
func resolveIngestWarehouse(getenv func(string) string, flagBaseURL, flagToken string) (baseURL, token string) {
	var fileCfg cliconfig.Config
	if loaded, _, rerr := cliconfig.Resolve(getenv); rerr == nil {
		fileCfg = loaded
	}
	baseURL = firstNonEmpty(strings.TrimSpace(flagBaseURL), getenv("PDW_API_URL"), fileCfg.BaseURL)
	token = firstNonEmpty(strings.TrimSpace(flagToken), getenv("PDW_SECRET_TOKEN"), fileCfg.Token)
	return baseURL, token
}

// ingestEnvAdditions feeds pdw's own warehouse config to the uploader so there
// is no separate ingest base URL to manage: the uploader's app URL is the main
// API URL (PDW_API_URL), and its HMAC signing key is the app secret token
// (PDW_SECRET_TOKEN) pdw already holds — the exact names the Python client
// reads. Values pulled from env are already inherited by the child, so we only
// append a value when it came from a root flag or the login config file. A root
// flag is always appended so it wins over an inherited env value (Go's exec
// dedups duplicate env keys keeping the last entry).
func ingestEnvAdditions(getenv func(string) string, flagBaseURL, flagToken string) []string {
	baseURL, token := resolveIngestWarehouse(getenv, flagBaseURL, flagToken)
	flagBaseURL = strings.TrimSpace(flagBaseURL)
	flagToken = strings.TrimSpace(flagToken)
	var add []string
	if flagBaseURL != "" {
		add = append(add, "PDW_API_URL="+baseURL)
	} else if baseURL != "" && getenv("PDW_API_URL") == "" && getenv("MCP_BASE_URL") == "" {
		add = append(add, "PDW_API_URL="+baseURL)
	}
	if flagToken != "" {
		add = append(add, "PDW_SECRET_TOKEN="+token)
	} else if token != "" &&
		getenv("PDW_SECRET_TOKEN") == "" &&
		getenv("MCP_SECRET_TOKEN") == "" {
		add = append(add, "PDW_SECRET_TOKEN="+token)
	}
	return add
}

// runIngestProcess executes uv as a child process, wiring through stdio so the
// uploader's output and any interactive prompts reach the user, and returns
// the child's exit code.
func runIngestProcess(uvBin string, argv []string, dir string, extraEnv []string, stdin io.Reader, stdout, stderr io.Writer) int {
	cmd := exec.Command(uvBin, argv...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), extraEnv...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			// The uploader ran and exited non-zero; surface its exit code so
			// launchd/systemd see the real status.
			return exitErr.ExitCode()
		}
		// uv itself failed to start (e.g. not on PATH, bad project dir).
		fmt.Fprintf(stderr, "pdw ingest: failed to run uploader: %v\n", err)
		return 1
	}
	return 0
}
