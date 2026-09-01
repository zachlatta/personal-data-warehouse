package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/cliconfig"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

var usage = `pdw — talk to the personal data warehouse /api/tools surface.

USAGE
  pdw [--base-url URL] [--token TOKEN] [--client NAME] [<command> [args]]

With no command, pdw runs schema_overview and prints the warehouse
schema, so callers always see what tables and columns are available before
writing SQL.

COMMANDS
  login                      Save warehouse URL + token to a per-user config file
                             so future runs need no env vars or flags.
                               --base-url URL  Warehouse URL (else prompted; defaults
                                               to https://personal-data-warehouse.zachlatta.com/).
                               --token TOKEN   Bearer token (else prompted).
                               --client NAME   Client identifier (else prompted; default pdw).
  logout                     Remove the saved configuration.
  config show                Print the resolved configuration with the token redacted.
  list                       List every tool the server exposes.
                               --json   Emit the raw tool list as a JSON array.
  describe <name>            Print one tool's title, description, and input schema.
  call <name> [--data JSON]  Invoke a NON-SQL tool. With --data, send that JSON
                             as the request body. Without --data, read JSON from
                             stdin. To run SQL, use the "sql" command below, not
                             "call sql" / "call query".
                               --data JSON   Inline JSON input
                                             (aliases: --args, --input, --json).
  search [flags] QUERY...    Hybrid search across every synced source. This is
                             the normal CLI search path; no JSON or SQL required.
                             Run "pdw search --help" for the full flag list.
                               --mode MODE       hybrid (default), keyword, or exact
                               -n, --max-results N  Maximum hits (default 20)
                               --source NAMES    Source aliases, comma-separated; repeatable
                               --priority TIERS  Timeline priority tiers, comma-separated.
` + warehouse.TimelinePriorityHelpLines("                                                 ") + `
                                                 Run "pdw search --help" for the generated
                                                 scope-selection guide.
                               --since TIME      Lower event-time bound (e.g. 2026-08-01)
                               --output FMT      text (default) or json
  sql [--output FMT] [-q QUESTION] [--file PATH] [--no-timeout] [SQL]
                             The one way to run read-only SQL. The SQL is the
                             single positional argument; it may instead be read
                             from --file or piped on stdin (which avoids
                             shell-quoting multi-line SQL). The server stops a
                             statement after its 60s budget; the client waits
                             75s so that error (with its hint) is what you see.
                               -q, --question TEXT  Concise plain-English
                                             description of what the SQL answers,
                                             logged server-side as the caller's
                                             intent. Optional; when omitted a
                                             generic "no intent given" marker is
                                             logged instead.
                               --output FMT  csv, json, or nd-json. If omitted,
                                             defaults to csv and prints a note.
                               --file PATH   Read the SQL statement from a file.
                               --no-timeout  Wait indefinitely for the response (the server still bounds statement execution).
  columns <table>            Describe one relation: every column with its exact
                             Postgres type, plus indexes and row estimate. Takes
                             base_gmail.messages or a bare table name. Run this for each
                             relation before writing SQL — schema_overview lists
                             relations and keys but NOT columns, so this is the only
                             authoritative column list. Guessing column names is the
                             single largest source of failed queries.
  schema                     Run schema_overview and print the warehouse schema
                             (same as running pdw with no command).
  ingest <source> [flags]    Run a local data-warehouse uploader through pdw.
                             Sources: voice-memos, apple-notes, apple-messages,
                             agent-sessions. Flags after <source> are forwarded
                             to the uploader (e.g. --mode incremental|full,
                             --limit N). See "pdw ingest --help".
  slack publish-session      Publish this Mac's Slack client session to the
                             warehouse, so the sync can ask Slack what changed
                             in one request instead of polling every
                             conversation. See "pdw slack --help".
  chatgpt publish-session    Publish this Mac's chatgpt.com browser session so
                             the server-side poller can keep syncing ChatGPT
                             conversations. See "pdw chatgpt --help".
  whoop publish-session      Publish this Mac's app.whoop.com browser session so
                             the private-API WHOOP sync (heart rate, hypnogram,
                             journal) keeps running. See "pdw whoop --help".
  version                    Print the build version.
  update                     Replace this binary with the latest GitHub release.
                               --check  Only report whether an update is available.
                               --force  Reinstall even if already on the latest version.
                               --repo OWNER/NAME  GitHub repo to pull from (default
                                                  $PDW_REPO or zachlatta/personal-data-warehouse).
  help                       Show this message.

AUTO-UPDATE
  Every invocation also kicks off a throttled background self-update (at most
  once every 5 minutes) so the binary stays current without running "update"
  by hand. It runs detached and never blocks or fails the command; the new
  binary is picked up on the next invocation. Set PDW_NO_AUTO_UPDATE=1 to
  disable it.

CONFIGURATION
  Values resolve in this order: --flag > environment > config file > default.
  Run "pdw login" once to write $XDG_CONFIG_HOME/pdw/config.json
  (defaults to ~/.config/pdw/config.json) with mode 0600.

ENVIRONMENT
  PDW_API_URL        Base URL of the warehouse app (e.g. http://localhost:8080).
  PDW_SECRET_TOKEN   Shared secret matching the server's PDW_SECRET_TOKEN.
  PDW_CLIENT_NAME    Client identifier sent on every request. Default: pdw.
  PDW_REPO           GitHub repo for self-update. Default: zachlatta/personal-data-warehouse.
                     (The legacy PDW_CLI_REPO name is still honored.)
  PDW_NO_AUTO_UPDATE Set to 1/true to disable the background auto-update.
  XDG_CONFIG_HOME    Overrides the config directory root.

EXAMPLES
  pdw login                          # one-time setup; persists URL + token
  pdw list
  pdw describe sql
  pdw search 'runway burn rate months cash remaining'
  pdw search --priority self,direct 'budget approval'
  pdw search --source gmail,slack --since 2026-08-01 'budget approval'
  pdw search --mode exact --output json 'admin/api-keys'
  pdw columns base_gmail.messages         # every column + type, before writing SQL
  pdw sql -q 'Search for an offer letter' \
    "SELECT * FROM timeline.search_text('offer letter', 20)"
  pdw sql -q 'Find every literal API-key path' \
    "SELECT * FROM timeline.search_text_exact('admin/api-keys', 20)"
  pdw sql 'SELECT 1'
  pdw sql -q 'How many rows?' 'SELECT count(*) FROM base_gmail.messages'
  pdw sql --output json -q 'What time is it?' 'SELECT now()'
  pdw sql --no-timeout -q 'Run a long query' 'SELECT ...'
  pdw sql -q 'Find calendar transcripts mentioning Vercel' --file query.sql
  pdw sql -q 'Recent Slack messages in a channel' < query.sql
  pdw config show
  pdw version
  pdw update --check
  pdw update
  pdw logout
`

// version is overridden at build time via -ldflags "-X main.version=v1.2.3".
var version = "dev"

// defaultRepo is the GitHub repo this CLI updates from when --repo and
// PDW_CLI_REPO are unset.
const defaultRepo = "zachlatta/personal-data-warehouse"

// defaultBaseURL is the warehouse URL the login prompt offers when the user
// has no saved config and doesn't type one in.
const defaultBaseURL = "https://personal-data-warehouse.zachlatta.com/"

// run is the testable entry point. It returns the process exit code rather
// than calling os.Exit so it can be driven from tests.
func run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string) int {
	rootFlags := flag.NewFlagSet("pdw", flag.ContinueOnError)
	rootFlags.SetOutput(io.Discard)
	baseURL := rootFlags.String("base-url", "", "base URL of the warehouse app (overrides PDW_API_URL)")
	token := rootFlags.String("token", "", "PDW_SECRET_TOKEN value (overrides PDW_SECRET_TOKEN)")
	clientName := rootFlags.String("client", "", "client name reported in server logs (overrides PDW_CLIENT_NAME)")
	if err := rootFlags.Parse(args); err != nil {
		// `pdw --help` / `-h` surfaces as flag.ErrHelp; treat it as the
		// help command (stdout, exit 0) rather than a usage error.
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stdout, usage)
			return 0
		}
		// `pdw --version` is the recurring one: ten invocations in thirty days,
		// each answered with a bare flag error plus the whole usage blob. One
		// line naming the real command is the whole fix.
		if redirect := rootFlagRedirect(args); redirect != "" {
			fmt.Fprint(stderr, redirect)
			return 2
		}
		fmt.Fprintln(stderr, err)
		fmt.Fprint(stderr, usage)
		return 2
	}
	rest := rootFlags.Args()
	var cmd string
	if len(rest) == 0 {
		// No command: default to schema_overview so callers always see the
		// schema first. Falls through to the auth/client setup below.
		cmd = "schema"
	} else {
		cmd, rest = rest[0], rest[1:]
	}

	// The hidden background worker performs the throttled self-update and
	// exits. Dispatch it before maybeAutoUpdate so it never spawns another.
	if cmd == autoUpdateCommand {
		return runAutoUpdateWorker(rest, getenv)
	}
	// Best-effort, non-blocking: kick a debounced background self-update so
	// the binary keeps itself current without a manual `pdw update`.
	maybeAutoUpdate(cmd, getenv)

	if cmd == "help" || cmd == "-h" || cmd == "--help" {
		fmt.Fprint(stdout, usage)
		return 0
	}
	// ingest runs local uploaders and never talks to /api/tools, so dispatch
	// it here: before the generic help check (so `pdw ingest <src> --help`
	// forwards --help to the uploader) and before the API-config resolution
	// (so it needs no warehouse URL/token).
	if cmd == "ingest" {
		return runIngestWithConfig(rest, stdin, stdout, stderr, getenv, *baseURL, *token)
	}
	// chatgpt runs a local setup helper (read browser cookie, publish session)
	// and posts to the app's signed endpoint, like ingest. Dispatch it here so
	// it forwards --help to the uploader and needs no /api/tools client.
	if cmd == "chatgpt" {
		return runChatGPT(rest, stdin, stdout, stderr, getenv, *baseURL, *token)
	}
	// whoop publish-session is the same shape: a local browser-cookie capture
	// posted to the app's signed endpoint, with no /api/tools client needed.
	if cmd == "whoop" {
		return runWhoop(rest, stdin, stdout, stderr, getenv, *baseURL, *token)
	}
	// slack publish-session captures the Slack desktop app's client session and
	// posts it to the app's signed endpoint -- same shape again.
	if cmd == "slack" {
		return runSlack(rest, stdin, stdout, stderr, getenv, *baseURL, *token)
	}
	// `pdw search --help` must print the search FlagSet's own help, not the
	// global usage: the flags this command accepts (notably --priority) are
	// otherwise undiscoverable from the command itself.
	if cmd == "search" && hasHelpArg(rest) {
		fmt.Fprint(stdout, searchUsage)
		return 0
	}
	if hasHelpArg(rest) {
		fmt.Fprint(stdout, usage)
		return 0
	}
	// These commands don't talk to /api/tools, so they must not require
	// PDW_API_URL / PDW_SECRET_TOKEN.
	if cmd == "version" {
		return runVersion(rest, stdout, stderr)
	}
	if cmd == "update" {
		return runUpdate(rest, stdout, stderr, getenv)
	}
	if cmd == "login" {
		return runLogin(rest, stdin, stdout, stderr, getenv)
	}
	if cmd == "logout" {
		return runLogout(rest, stdout, stderr, getenv)
	}
	if cmd == "config" {
		return runConfig(rest, stdout, stderr, getenv)
	}

	// A tool name typed as a command is answered before the config check: the
	// caller's mistake is the command, and reporting a missing token instead
	// sends them to fix the wrong thing.
	if redirect, ok := commandRedirects[cmd]; ok {
		fmt.Fprintf(stderr, "pdw: there is no %q command; %s\n", cmd, redirect)
		return 2
	}

	resolved, err := resolveConfig(*baseURL, *clientName, *token, getenv)
	if err != nil {
		fmt.Fprintln(stderr, "pdw:", err)
		return 2
	}
	client, err := cliclient.New(resolved.baseURL, resolved.clientName, resolved.token)
	if err != nil {
		fmt.Fprintln(stderr, "pdw:", err)
		return 2
	}

	switch cmd {
	case "list":
		return runList(client, rest, stdout, stderr)
	case "describe":
		return runDescribe(client, rest, stdout, stderr)
	case "call":
		return runCall(client, rest, stdin, stdout, stderr)
	case "search":
		return runSearch(client, rest, stdout, stderr)
	case "sql":
		return runSQL(client, rest, stdin, stdout, stderr)
	case "columns":
		return runColumns(client, rest, stdout, stderr)
	case "schema":
		return runSchema(client, rest, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "pdw: unknown command %q\n", cmd)
		fmt.Fprint(stderr, usage)
		return 2
	}
}

// schemaSearchFirstNudge is printed to stderr before every schema dump.
var schemaSearchFirstNudge = "pdw schema: this is the relation list for writing SQL. For any text, topic, person or identifier question, start with `pdw search '<terms>'` (add --priority " + strings.Join(warehouse.TimelineAttentionPriorities(), ",") + " for attention questions) — it needs no schema."

// runSchema calls schema_overview and prints its CSV result blocks as plain
// text so the no-args invocation is human-readable. Any extra args are
// rejected to keep the command's contract narrow.
func runSchema(client *cliclient.Client, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 {
		fmt.Fprintln(stderr, "pdw schema: unexpected arguments")
		return 2
	}
	// Schema discovery is step 2's prerequisite, not step 1: measured over
	// 14 days to 2026-08-28, 31% of PDW sessions opened with this command and
	// 21% with a search, against a 60% search-first target. The nudge goes to
	// stderr so scripted consumers keep clean rows on stdout.
	fmt.Fprintln(stderr, schemaSearchFirstNudge)
	out, err := client.CallTool(context.Background(), "schema_overview", nil)
	if err != nil {
		var apiErr *cliclient.APIError
		if errors.As(err, &apiErr) {
			fmt.Fprintf(stderr, "pdw schema: %s (http %d): %s\n", apiErr.Code, apiErr.Status, apiErr.Message)
			return 1
		}
		fmt.Fprintln(stderr, "pdw schema:", err)
		return 1
	}
	var payload struct {
		Results []struct {
			SQL   string `json:"sql"`
			CSV   string `json:"csv"`
			Error string `json:"error"`
		} `json:"results"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		// Server gave us something we can't parse; surface the raw payload.
		fmt.Fprintln(stdout, string(out))
		return 0
	}
	for _, r := range payload.Results {
		if r.Error != "" {
			fmt.Fprintln(stderr, "pdw schema:", r.Error)
			return 1
		}
		if r.CSV != "" {
			fmt.Fprintln(stdout, r.CSV)
		}
	}
	return 0
}

const sqlOutputHint = "note: use --output [csv|json|nd-json] to specify output format"

// Slightly above the server's 60s statement budget (config.QueryTimeout), so a
// slow query surfaces the server's SQL timeout error — which carries a rewrite
// hint — instead of a client-side abort that leaves the statement burning
// server-side and invites a blind retry.
const defaultSQLTimeout = 75 * time.Second

type sqlCommandInput struct {
	Question string `json:"question"`
	SQL      string `json:"sql"`
	Format   string `json:"format"`
}

type sqlCommandResponse struct {
	Rows  json.RawMessage `json:"rows"`
	Error string          `json:"error"`
	Hint  string          `json:"hint"`
}

func runSQL(client *cliclient.Client, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sql", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	output := fs.String("output", "", "output format: csv, json, or nd-json")
	file := fs.String("file", "", "read the SQL statement from this file instead of an argument")
	questionFlag := fs.String("question", "", "plain-English description of what the SQL answers, logged server-side as intent")
	questionShort := fs.String("q", "", "alias for --question")
	noTimeout := fs.Bool("no-timeout", false, "wait indefinitely for the query")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stdout, usage)
			return 0
		}
		fmt.Fprintln(stderr, "pdw sql:", err)
		return 2
	}
	formatSpecified := strings.TrimSpace(*output) != ""
	format, err := normalizeSQLOutputFormat(*output)
	if err != nil {
		fmt.Fprintln(stderr, "pdw sql:", err)
		return 2
	}
	question, sql, code := resolveSQLInput(fs.Args(), firstNonEmpty(*questionFlag, *questionShort), *file, stdin, stderr)
	if code != 0 {
		return code
	}
	input, err := json.Marshal(sqlCommandInput{Question: question, SQL: sql, Format: format})
	if err != nil {
		fmt.Fprintln(stderr, "pdw sql:", err)
		return 1
	}
	ctx := context.Background()
	cancel := func() {}
	if !*noTimeout {
		ctx, cancel = context.WithTimeout(ctx, defaultSQLTimeout)
	}
	defer cancel()

	out, err := client.CallTool(ctx, "sql", input)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			fmt.Fprintf(stderr, "pdw sql: no response after %s (the server itself stops statements after its own budget); narrow the query, or rerun with --no-timeout if you are waiting on a large result download\n", defaultSQLTimeout)
			return 1
		}
		var apiErr *cliclient.APIError
		if errors.As(err, &apiErr) {
			fmt.Fprintf(stderr, "pdw sql: %s (http %d): %s\n", apiErr.Code, apiErr.Status, apiErr.Message)
			return 1
		}
		fmt.Fprintln(stderr, "pdw sql:", err)
		return 1
	}
	var payload sqlCommandResponse
	if err := json.Unmarshal(out, &payload); err != nil {
		fmt.Fprintln(stdout, string(out))
		return 0
	}
	if payload.Error != "" {
		fmt.Fprintln(stderr, "pdw sql:", payload.Error)
		return 1
	}
	if payload.Hint != "" {
		// Advice about the statement's shape, on stderr so scripted --output
		// consumers still get clean rows on stdout.
		fmt.Fprintln(stderr, "pdw sql:", payload.Hint)
	}
	if !formatSpecified {
		fmt.Fprintln(stdout, sqlOutputHint)
	}
	return printSQLRows(payload.Rows, format, stdout)
}

// Copy-pasteable examples embedded in sql error messages so a failed call
// recovers in one line instead of sending the caller back to `pdw --help`.
const (
	sqlExample      = `pdw sql -q "why you're asking" "SELECT 1"`
	sqlStdinExample = `pdw sql -q "why you're asking" < query.sql`
	sqlFileExample  = `pdw sql -q "why you're asking" --file query.sql`
)

// defaultSQLQuestion is logged as the caller's intent when -q/--question is
// omitted. It keeps the server-side intent field populated (the server rejects
// an empty question) while honestly flagging that no intent was stated.
const defaultSQLQuestion = "(ad-hoc pdw CLI query; no -q intent given)"

// resolveSQLInput determines the question and SQL for the sql command. The SQL
// is the single positional argument, but may instead come from --file or
// stdin so callers can avoid wrapping multi-line, quote-heavy SQL in shell
// quotes. The question is the optional -q/--question flag; when blank it falls
// back to defaultSQLQuestion so server logs always carry an intent field.
func resolveSQLInput(positional []string, questionFlag, file string, stdin io.Reader, stderr io.Writer) (question, sql string, code int) {
	question = strings.TrimSpace(questionFlag)
	if question == "" {
		question = defaultSQLQuestion
	}
	file = strings.TrimSpace(file)
	switch {
	case file != "":
		if len(positional) > 0 {
			fmt.Fprintf(stderr, "pdw sql: SQL came from --file, so don't also pass it as an argument. Example: %s\n", sqlFileExample)
			return "", "", 2
		}
		b, err := os.ReadFile(file)
		if err != nil {
			fmt.Fprintln(stderr, "pdw sql: read --file:", err)
			return "", "", 2
		}
		sql = strings.TrimSpace(string(b))
	case len(positional) > 1:
		fmt.Fprintf(stderr, "pdw sql: too many arguments; SQL is the single positional arg now and the question moved to -q. Example: %s\n", sqlExample)
		return "", "", 2
	case len(positional) == 1:
		sql = strings.TrimSpace(positional[0])
	default:
		b, err := io.ReadAll(stdin)
		if err != nil {
			fmt.Fprintln(stderr, "pdw sql: read stdin:", err)
			return "", "", 2
		}
		sql = strings.TrimSpace(string(b))
	}
	if sql == "" {
		fmt.Fprintf(stderr, "pdw sql: no SQL given; pass it as an argument, via --file, or on stdin. Example: %s  (or pipe it: %s)\n", sqlExample, sqlStdinExample)
		return "", "", 2
	}
	return question, sql, 0
}

// runColumns describes one relation so callers can confirm exact column names
// before writing SQL instead of guessing them. It calls the server's
// describe_table tool rather than hand-rolling an information_schema query, so
// the CLI and MCP surfaces return the identical catalog — same exact
// format_type() types (text[] and bigint, not information_schema's "ARRAY"),
// same indexes, same row estimate, and the same actionable error naming real
// candidates when the relation does not exist.
func runColumns(client *cliclient.Client, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "pdw columns: table name is required (usage: pdw columns <table>)")
		return 2
	}
	if len(args) > 1 {
		fmt.Fprintln(stderr, "pdw columns: unexpected extra arguments; pass a single table name")
		return 2
	}
	relation := strings.TrimSpace(args[0])
	if !isRelationIdentifier(relation) {
		fmt.Fprintln(stderr, "pdw columns: table name must be an identifier like base_gmail.messages (or a bare table name)")
		return 2
	}
	input, err := json.Marshal(map[string]string{"relation": relation})
	if err != nil {
		fmt.Fprintln(stderr, "pdw columns:", err)
		return 1
	}
	out, err := client.CallTool(context.Background(), "describe_table", input)
	if err != nil {
		var apiErr *cliclient.APIError
		if errors.As(err, &apiErr) {
			fmt.Fprintf(stderr, "pdw columns: %s (http %d): %s\n", apiErr.Code, apiErr.Status, apiErr.Message)
			return 1
		}
		fmt.Fprintln(stderr, "pdw columns:", err)
		return 1
	}
	return printCatalogResults(out, "pdw columns", stdout, stderr)
}

// isRelationIdentifier accepts `table` and `schema.table`. The server validates
// too; this keeps an obviously malformed name from making a round trip.
func isRelationIdentifier(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) > 2 {
		return false
	}
	for _, part := range parts {
		if !validIdentifier(part) {
			return false
		}
	}
	return true
}

// printCatalogResults renders the {results:[{csv,error}]} payload shared by the
// schema_overview and describe_table tools.
func printCatalogResults(out []byte, prefix string, stdout, stderr io.Writer) int {
	var payload struct {
		Results []struct {
			CSV   string `json:"csv"`
			Error string `json:"error"`
		} `json:"results"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		fmt.Fprintln(stdout, string(out))
		return 0
	}
	for _, r := range payload.Results {
		if r.Error != "" {
			fmt.Fprintln(stderr, prefix+":", r.Error)
			return 1
		}
		if r.CSV != "" {
			fmt.Fprintln(stdout, r.CSV)
		}
	}
	return 0
}

func validIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func normalizeSQLOutputFormat(output string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(output)) {
	case "", "csv":
		return "csv", nil
	case "json":
		return "json", nil
	case "nd-json", "ndjson":
		return "ndjson", nil
	default:
		return "", fmt.Errorf("invalid --output %q; use csv, json, or nd-json", output)
	}
}

func printSQLRows(rows json.RawMessage, format string, stdout io.Writer) int {
	if len(rows) == 0 || string(rows) == "null" {
		return 0
	}
	if format == "json" {
		if pretty, err := prettyJSON(rows); err == nil {
			fmt.Fprintln(stdout, pretty)
			return 0
		}
		fmt.Fprintln(stdout, string(rows))
		return 0
	}
	var text string
	if err := json.Unmarshal(rows, &text); err != nil {
		fmt.Fprintln(stdout, string(rows))
		return 0
	}
	fmt.Fprintln(stdout, text)
	return 0
}

type resolvedConfig struct {
	baseURL    string
	clientName string
	token      string
}

func resolveConfig(flagBase, flagClient, flagToken string, getenv func(string) string) (resolvedConfig, error) {
	// Config file lookup is best-effort — a missing or corrupt file should
	// not stop a fully-flagged or fully-env'd invocation from working.
	// Resolve transparently falls back to the legacy pdw-cli config path.
	var fileCfg cliconfig.Config
	if loaded, _, rerr := cliconfig.Resolve(getenv); rerr == nil {
		fileCfg = loaded
	}
	rc := resolvedConfig{
		baseURL:    firstNonEmpty(flagBase, getenv("PDW_API_URL"), fileCfg.BaseURL),
		clientName: firstNonEmpty(flagClient, getenv("PDW_CLIENT_NAME"), fileCfg.ClientName, "pdw"),
		token:      firstNonEmpty(flagToken, getenv("PDW_SECRET_TOKEN"), fileCfg.Token),
	}
	var missing []string
	if rc.baseURL == "" {
		missing = append(missing, "warehouse URL (--base-url, PDW_API_URL, or `pdw login`)")
	}
	if rc.token == "" {
		missing = append(missing, "bearer token (--token, PDW_SECRET_TOKEN, or `pdw login`)")
	}
	if len(missing) > 0 {
		return rc, fmt.Errorf("not configured: %s", strings.Join(missing, "; "))
	}
	return rc, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func runList(client *cliclient.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "emit the tool list as a JSON array")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stdout, usage)
			return 0
		}
		fmt.Fprintln(stderr, "pdw list:", err)
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintln(stderr, "pdw list: unexpected positional arguments")
		return 2
	}
	tools, err := client.ListTools(context.Background())
	if err != nil {
		fmt.Fprintln(stderr, "pdw list:", err)
		return 1
	}
	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(tools); err != nil {
			fmt.Fprintln(stderr, "pdw list:", err)
			return 1
		}
		return 0
	}
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tTITLE\tDESCRIPTION")
	for _, t := range tools {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", t.Name, t.Title, firstLine(t.Description))
	}
	tw.Flush()
	return 0
}

func runDescribe(client *cliclient.Client, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "pdw describe: tool name is required")
		return 2
	}
	if len(args) > 1 {
		fmt.Fprintln(stderr, "pdw describe: unexpected extra arguments")
		return 2
	}
	name := args[0]
	tools, err := client.ListTools(context.Background())
	if err != nil {
		fmt.Fprintln(stderr, "pdw describe:", err)
		return 1
	}
	for _, t := range tools {
		if t.Name == name {
			fmt.Fprintf(stdout, "name: %s\ntitle: %s\n\ndescription:\n%s\n\ninput_schema:\n", t.Name, t.Title, t.Description)
			pretty, perr := prettyJSON(t.InputSchema)
			if perr != nil {
				fmt.Fprintln(stdout, string(t.InputSchema))
			} else {
				fmt.Fprintln(stdout, pretty)
			}
			return 0
		}
	}
	fmt.Fprintf(stderr, "pdw describe: no tool named %q (try 'pdw list')\n", name)
	return 1
}

func runCall(client *cliclient.Client, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if hasHelpArg(args) {
		fmt.Fprint(stdout, usage)
		return 0
	}
	name, rest, err := extractToolName(args)
	if err != nil {
		fmt.Fprintln(stderr, "pdw call:", err)
		return 2
	}
	// There is exactly one supported way to run SQL: the `sql` command. The
	// CLI/HTTP tool is named sql and the MCP tool is named query, so callers
	// arrive here under either name. Redirect both to the single path instead
	// of accepting a second, quoting-prone JSON route through `call`.
	if name == "sql" || name == "query" {
		fmt.Fprintln(stderr, "pdw call: run SQL with the dedicated `sql` command, not `call`:")
		fmt.Fprintln(stderr, "  pdw sql -q '<question>' '<sql>'")
		fmt.Fprintln(stderr, "  pdw sql -q '<question>' --file query.sql   # SQL from a file")
		fmt.Fprintln(stderr, "  pdw sql -q '<question>' < query.sql        # SQL from stdin")
		fmt.Fprintln(stderr, "This avoids JSON/shell quoting; `call` is only for non-SQL tools.")
		return 2
	}
	// schema_overview and describe_table have first-class commands that render
	// their CSV as readable text; reaching them through `call` returns the same
	// answer as raw JSON, so it is a second path to a worse result. C8 says
	// there is one way to do each thing.
	if redirect, ok := callToolRedirects[name]; ok {
		fmt.Fprintf(stderr, "pdw call: %s does not go through `call`:\n", name)
		fmt.Fprintf(stderr, "  %s\n", redirect)
		return 2
	}
	if name == "search" {
		fmt.Fprintln(stderr, "pdw call: use the first-class search command instead of JSON:")
		fmt.Fprintln(stderr, "  pdw search '<terms>'")
		fmt.Fprintln(stderr, "  pdw search --source gmail,slack --since 2026-08-01 '<terms>'")
		fmt.Fprintln(stderr, "  pdw search --mode exact --output json '<literal>'")
		return 2
	}
	fs := flag.NewFlagSet("call", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	data := fs.String("data", "", "inline JSON request body")
	// Accept the flag names agents commonly reach for as aliases instead of
	// failing hard at parse time.
	dataArgs := fs.String("args", "", "alias for --data")
	dataInput := fs.String("input", "", "alias for --data")
	dataJSON := fs.String("json", "", "alias for --data")
	if err := fs.Parse(rest); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stdout, usage)
			return 0
		}
		fmt.Fprintln(stderr, "pdw call:", err)
		return 2
	}
	if extra := fs.Args(); len(extra) > 0 {
		if looksLikeKeyValue(extra) {
			fmt.Fprintln(stderr, "pdw call: pass tool input as JSON via --data '{\"key\":\"value\"}' or on stdin, not key=value arguments")
			return 2
		}
		fmt.Fprintln(stderr, "pdw call: unexpected extra arguments")
		return 2
	}

	input, err := loadCallInput(firstNonEmpty(*data, *dataArgs, *dataInput, *dataJSON), stdin)
	if err != nil {
		fmt.Fprintln(stderr, "pdw call:", err)
		return 2
	}
	out, err := client.CallTool(context.Background(), name, input)
	if err != nil {
		var apiErr *cliclient.APIError
		if errors.As(err, &apiErr) {
			if apiErr.Code == "tool_not_found" {
				if s := suggestTool(client, name); s != "" {
					fmt.Fprintf(stderr, "pdw call: %s (http %d): %s; did you mean %q? (run 'pdw list')\n", apiErr.Code, apiErr.Status, apiErr.Message, s)
					return 1
				}
			}
			fmt.Fprintf(stderr, "pdw call: %s (http %d): %s\n", apiErr.Code, apiErr.Status, apiErr.Message)
			return 1
		}
		fmt.Fprintln(stderr, "pdw call:", err)
		return 1
	}
	// The HTTP tool API returns domain-level errors as 200 with data so it can
	// preserve partial-result semantics and match MCP. A top-level error string,
	// however, means this individual call failed. Reflect that in the process
	// exit status instead of making shell agents treat an error-shaped response
	// as success (notably search statement timeouts).
	if message := topLevelToolError(out); message != "" {
		fmt.Fprintln(stderr, "pdw call:", message)
		return 1
	}
	pretty, perr := prettyJSON(out)
	if perr != nil {
		fmt.Fprintln(stdout, string(out))
		return 0
	}
	fmt.Fprintln(stdout, pretty)
	return 0
}

func topLevelToolError(raw json.RawMessage) string {
	var envelope struct {
		Error json.RawMessage `json:"error"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &envelope) != nil || len(envelope.Error) == 0 {
		return ""
	}
	var message string
	if json.Unmarshal(envelope.Error, &message) == nil {
		return strings.TrimSpace(message)
	}
	return ""
}

func loadCallInput(data string, stdin io.Reader) (json.RawMessage, error) {
	raw := strings.TrimSpace(data)
	if raw == "" {
		buf, err := io.ReadAll(stdin)
		if err != nil {
			return nil, fmt.Errorf("read stdin: %w", err)
		}
		raw = strings.TrimSpace(string(buf))
	}
	if raw == "" {
		return nil, nil
	}
	if !json.Valid([]byte(raw)) {
		return nil, fmt.Errorf("invalid JSON input")
	}
	return json.RawMessage(raw), nil
}

func prettyJSON(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// extractToolName pulls the tool name out of the call args while leaving
// flag arguments (which may appear before or after it) intact. This lets
// `call query --data '{}'` and `call --data '{}' query` both work.
func extractToolName(args []string) (string, []string, error) {
	for i, a := range args {
		if a == "--" {
			break
		}
		if strings.HasPrefix(a, "-") {
			continue
		}
		// Skip the value of the previous flag if it was the separated form
		// (e.g. --data {json}). The flag pkg accepts both --flag=val and
		// --flag val, so detect the latter by looking back.
		if i > 0 && strings.HasPrefix(args[i-1], "-") && !strings.Contains(args[i-1], "=") {
			continue
		}
		rest := append([]string{}, args[:i]...)
		rest = append(rest, args[i+1:]...)
		return a, rest, nil
	}
	return "", nil, fmt.Errorf("tool name is required")
}

func firstLine(s string) string {
	if i := strings.IndexAny(s, "\r\n"); i >= 0 {
		return s[:i]
	}
	return s
}

// commandRedirects answer a command name that does not exist with the one that
// does, in a single line. Every key is a server-side TOOL name an agent typed
// as a command: `pdw query` and `pdw schema_overview` were both observed in
// real sessions, and both got "unknown command" plus a ~100-line help dump.
var commandRedirects = map[string]string{
	"query":           "run SQL with `pdw sql -q '<question>' '<sql>'`",
	"sql_query":       "run SQL with `pdw sql -q '<question>' '<sql>'`",
	"schema_overview": "print the warehouse schema with `pdw schema`",
	"describe_table":  "list one relation's columns with `pdw columns <table>`",
	"tools":           "list the server's tools with `pdw list`",
}

// callToolRedirects are the tools `call` refuses, because each already has a
// first-class command whose output is readable rather than raw JSON.
var callToolRedirects = map[string]string{
	"schema_overview": "pdw schema",
	"describe_table":  "pdw columns <table>",
}

// rootFlagRedirect answers a root-level flag that does not exist with the
// command that does. Returns "" when the flag is not one we recognize, so an
// unknown flag still falls through to the ordinary usage error.
func rootFlagRedirect(args []string) string {
	for _, arg := range args {
		if arg == "--" {
			return ""
		}
		if !strings.HasPrefix(arg, "-") {
			continue
		}
		switch strings.ToLower(strings.TrimLeft(arg, "-")) {
		case "version", "v":
			return "pdw: there is no --version flag; run `pdw version`.\n"
		}
	}
	return ""
}

// hasHelpArg reports whether a help flag appears before a "--" terminator.
func hasHelpArg(args []string) bool {
	for _, a := range args {
		if a == "--" {
			return false
		}
		if a == "-h" || a == "--help" || a == "-help" {
			return true
		}
	}
	return false
}

// looksLikeKeyValue reports whether any leftover positional arg looks like a
// shell-style key=value pair, which agents sometimes pass to `call` instead of
// a JSON body.
func looksLikeKeyValue(args []string) bool {
	for _, a := range args {
		if i := strings.IndexByte(a, '='); i > 0 {
			return true
		}
	}
	return false
}

// suggestTool returns the closest existing tool name to want, or "" if none is
// close enough to be worth suggesting.
func suggestTool(client *cliclient.Client, want string) string {
	tools, err := client.ListTools(context.Background())
	if err != nil {
		return ""
	}
	best := ""
	bestDist := -1
	for _, t := range tools {
		d := levenshtein(strings.ToLower(want), strings.ToLower(t.Name))
		if bestDist < 0 || d < bestDist {
			bestDist, best = d, t.Name
		}
	}
	if best != "" && bestDist <= 3 {
		return best
	}
	return ""
}

func levenshtein(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	prev := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		cur := make([]int, len(rb)+1)
		cur[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			cur[j] = min3(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev = cur
	}
	return prev[len(rb)]
}

func min3(a, b, c int) int {
	if b < a {
		a = b
	}
	if c < a {
		a = c
	}
	return a
}
