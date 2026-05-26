package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/cliconfig"
)

const usage = `pdw-cli — talk to the personal data warehouse /api/tools surface.

USAGE
  pdw-cli [--base-url URL] [--token TOKEN] [--client NAME] <command> [args]

COMMANDS
  login                      Save warehouse URL + token to a per-user config file
                             so future runs need no env vars or flags.
                               --base-url URL  Warehouse URL (else prompted; defaults
                                               to https://personal-data-warehouse.zachlatta.com/).
                               --token TOKEN   Bearer token (else prompted).
                               --client NAME   Client identifier (else prompted; default pdw-cli).
  logout                     Remove the saved configuration.
  config show                Print the resolved configuration with the token redacted.
  list                       List every tool the server exposes.
                               --json   Emit the raw tool list as a JSON array.
  describe <name>            Print one tool's title, description, and input schema.
  call <name> [--data JSON]  Invoke a tool. With --data, send that JSON as the
                             request body. Without --data, read JSON from stdin.
                               --data JSON   Inline JSON input.
  version                    Print the build version.
  update                     Replace this binary with the latest GitHub release.
                               --check  Only report whether an update is available.
                               --force  Reinstall even if already on the latest version.
                               --repo OWNER/NAME  GitHub repo to pull from (default
                                                  $PDW_CLI_REPO or zachlatta/personal-data-warehouse).
  help                       Show this message.

CONFIGURATION
  Values resolve in this order: --flag > environment > config file > default.
  Run "pdw-cli login" once to write $XDG_CONFIG_HOME/pdw-cli/config.json
  (defaults to ~/.config/pdw-cli/config.json) with mode 0600.

ENVIRONMENT
  PDW_API_URL        Base URL of the warehouse app (e.g. http://localhost:8080).
  PDW_SECRET_TOKEN   Shared secret matching the server's PDW_SECRET_TOKEN.
  PDW_CLIENT_NAME    Client identifier sent on every request. Default: pdw-cli.
  PDW_CLI_REPO       GitHub repo for self-update. Default: zachlatta/personal-data-warehouse.
  XDG_CONFIG_HOME    Overrides the config directory root.

EXAMPLES
  pdw-cli login                          # one-time setup; persists URL + token
  pdw-cli list
  pdw-cli describe query
  pdw-cli call schema_overview
  pdw-cli call query --data '{"queries":[{"question":"row count","sql":"SELECT 1"}]}'
  echo '{"queries":[{"question":"q","sql":"SELECT 1"}]}' | pdw-cli call query
  pdw-cli config show
  pdw-cli version
  pdw-cli update --check
  pdw-cli update
  pdw-cli logout
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
	rootFlags := flag.NewFlagSet("pdw-cli", flag.ContinueOnError)
	rootFlags.SetOutput(io.Discard)
	baseURL := rootFlags.String("base-url", "", "base URL of the warehouse app (overrides PDW_API_URL)")
	token := rootFlags.String("token", "", "PDW_SECRET_TOKEN value (overrides PDW_SECRET_TOKEN)")
	clientName := rootFlags.String("client", "", "client name reported in server logs (overrides PDW_CLIENT_NAME)")
	if err := rootFlags.Parse(args); err != nil {
		fmt.Fprintln(stderr, err)
		fmt.Fprint(stderr, usage)
		return 2
	}
	rest := rootFlags.Args()
	if len(rest) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	cmd, rest := rest[0], rest[1:]

	if cmd == "help" || cmd == "-h" || cmd == "--help" {
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
		fmt.Fprintln(stderr, "pdw-cli:", err)
		return 2
	}
	client, err := cliclient.New(resolved.baseURL, resolved.clientName, resolved.token)
	if err != nil {
		fmt.Fprintln(stderr, "pdw-cli:", err)
		return 2
	}

	switch cmd {
	case "list":
		return runList(client, rest, stdout, stderr)
	case "describe":
		return runDescribe(client, rest, stdout, stderr)
	case "call":
		return runCall(client, rest, stdin, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "pdw-cli: unknown command %q\n", cmd)
		fmt.Fprint(stderr, usage)
		return 2
	}
}

type resolvedConfig struct {
	baseURL    string
	clientName string
	token      string
}

func resolveConfig(flagBase, flagClient, flagToken string, getenv func(string) string) (resolvedConfig, error) {
	// Config file lookup is best-effort — a missing or corrupt file should
	// not stop a fully-flagged or fully-env'd invocation from working.
	var fileCfg cliconfig.Config
	if path, perr := cliconfig.Path(getenv); perr == nil {
		if loaded, lerr := cliconfig.Load(path); lerr == nil {
			fileCfg = loaded
		}
	}
	rc := resolvedConfig{
		baseURL:    firstNonEmpty(flagBase, getenv("PDW_API_URL"), fileCfg.BaseURL),
		clientName: firstNonEmpty(flagClient, getenv("PDW_CLIENT_NAME"), fileCfg.ClientName, "pdw-cli"),
		token:      firstNonEmpty(flagToken, getenv("PDW_SECRET_TOKEN"), fileCfg.Token),
	}
	var missing []string
	if rc.baseURL == "" {
		missing = append(missing, "warehouse URL (--base-url, PDW_API_URL, or `pdw-cli login`)")
	}
	if rc.token == "" {
		missing = append(missing, "bearer token (--token, PDW_SECRET_TOKEN, or `pdw-cli login`)")
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
		fmt.Fprintln(stderr, "pdw-cli list:", err)
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintln(stderr, "pdw-cli list: unexpected positional arguments")
		return 2
	}
	tools, err := client.ListTools(context.Background())
	if err != nil {
		fmt.Fprintln(stderr, "pdw-cli list:", err)
		return 1
	}
	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(tools); err != nil {
			fmt.Fprintln(stderr, "pdw-cli list:", err)
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
		fmt.Fprintln(stderr, "pdw-cli describe: tool name is required")
		return 2
	}
	if len(args) > 1 {
		fmt.Fprintln(stderr, "pdw-cli describe: unexpected extra arguments")
		return 2
	}
	name := args[0]
	tools, err := client.ListTools(context.Background())
	if err != nil {
		fmt.Fprintln(stderr, "pdw-cli describe:", err)
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
	fmt.Fprintf(stderr, "pdw-cli describe: no tool named %q (try 'pdw-cli list')\n", name)
	return 1
}

func runCall(client *cliclient.Client, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	name, rest, err := extractToolName(args)
	if err != nil {
		fmt.Fprintln(stderr, "pdw-cli call:", err)
		return 2
	}
	fs := flag.NewFlagSet("call", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	data := fs.String("data", "", "inline JSON request body")
	if err := fs.Parse(rest); err != nil {
		fmt.Fprintln(stderr, "pdw-cli call:", err)
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintln(stderr, "pdw-cli call: unexpected extra arguments")
		return 2
	}

	input, err := loadCallInput(*data, stdin)
	if err != nil {
		fmt.Fprintln(stderr, "pdw-cli call:", err)
		return 2
	}
	out, err := client.CallTool(context.Background(), name, input)
	if err != nil {
		var apiErr *cliclient.APIError
		if errors.As(err, &apiErr) {
			fmt.Fprintf(stderr, "pdw-cli call: %s (http %d): %s\n", apiErr.Code, apiErr.Status, apiErr.Message)
			return 1
		}
		fmt.Fprintln(stderr, "pdw-cli call:", err)
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
