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

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/cliconfig"
)

const usage = `pdw — talk to the personal data warehouse /api/tools surface.

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
  sql [--output FMT] [-q QUESTION] [--file PATH] [SQL]
                             The one way to run read-only SQL. The SQL is the
                             single positional argument; it may instead be read
                             from --file or piped on stdin (which avoids
                             shell-quoting multi-line SQL).
                               -q, --question TEXT  Concise plain-English
                                             description of what the SQL answers,
                                             logged server-side as the caller's
                                             intent. Optional; when omitted a
                                             generic "no intent given" marker is
                                             logged instead.
                               --output FMT  csv, json, or nd-json. If omitted,
                                             defaults to csv and prints a note.
                               --file PATH   Read the SQL statement from a file.
  columns <table>            List a table's column names and types. Use this (or
                             schema) before writing SQL so you don't guess column
                             names.
  schema                     Run schema_overview and print the warehouse schema
                             (same as running pdw with no command).
  ingest <source> [flags]    Run a local data-warehouse uploader through pdw.
                             Sources: voice-memos, apple-notes, apple-messages,
                             agent-sessions. Flags after <source> are forwarded
                             to the uploader (e.g. --mode incremental|full,
                             --limit N). See "pdw ingest --help".
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
  pdw call schema_overview
  pdw columns gmail_messages
  pdw sql 'SELECT 1'
  pdw sql -q 'How many rows?' 'SELECT count(*) FROM gmail_messages'
  pdw sql --output json -q 'What time is it?' 'SELECT now()'
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

// runSchema calls schema_overview and prints its CSV result blocks as plain
// text so the no-args invocation is human-readable. Any extra args are
// rejected to keep the command's contract narrow.
func runSchema(client *cliclient.Client, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 {
		fmt.Fprintln(stderr, "pdw schema: unexpected arguments")
		return 2
	}
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

type sqlCommandInput struct {
	Question string `json:"question"`
	SQL      string `json:"sql"`
	Format   string `json:"format"`
}

type sqlCommandResponse struct {
	Rows  json.RawMessage `json:"rows"`
	Error string          `json:"error"`
}

func runSQL(client *cliclient.Client, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sql", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	output := fs.String("output", "", "output format: csv, json, or nd-json")
	file := fs.String("file", "", "read the SQL statement from this file instead of an argument")
	questionFlag := fs.String("question", "", "plain-English description of what the SQL answers, logged server-side as intent")
	questionShort := fs.String("q", "", "alias for --question")
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
	out, err := client.CallTool(context.Background(), "sql", input)
	if err != nil {
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

// runColumns lists a table's columns via the sql tool so callers can confirm
// exact column names before writing SQL instead of guessing them.
func runColumns(client *cliclient.Client, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "pdw columns: table name is required (usage: pdw columns <table>)")
		return 2
	}
	if len(args) > 1 {
		fmt.Fprintln(stderr, "pdw columns: unexpected extra arguments; pass a single table name")
		return 2
	}
	table := strings.TrimSpace(args[0])
	if !validIdentifier(table) {
		fmt.Fprintln(stderr, "pdw columns: table name must be a bare identifier (letters, digits, underscores)")
		return 2
	}
	sql := "SELECT column_name, data_type, is_nullable FROM information_schema.columns " +
		"WHERE table_schema = current_schema() AND table_name = '" + table + "' ORDER BY ordinal_position"
	input, err := json.Marshal(sqlCommandInput{Question: "What columns does the " + table + " table have?", SQL: sql, Format: "csv"})
	if err != nil {
		fmt.Fprintln(stderr, "pdw columns:", err)
		return 1
	}
	out, err := client.CallTool(context.Background(), "sql", input)
	if err != nil {
		var apiErr *cliclient.APIError
		if errors.As(err, &apiErr) {
			fmt.Fprintf(stderr, "pdw columns: %s (http %d): %s\n", apiErr.Code, apiErr.Status, apiErr.Message)
			return 1
		}
		fmt.Fprintln(stderr, "pdw columns:", err)
		return 1
	}
	var payload sqlCommandResponse
	if err := json.Unmarshal(out, &payload); err != nil {
		fmt.Fprintln(stdout, string(out))
		return 0
	}
	if payload.Error != "" {
		fmt.Fprintln(stderr, "pdw columns:", payload.Error)
		return 1
	}
	return printSQLRows(payload.Rows, "csv", stdout)
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
	pretty, perr := prettyJSON(out)
	if perr != nil {
		fmt.Fprintln(stdout, string(out))
		return 0
	}
	fmt.Fprintln(stdout, pretty)
	return 0
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
