package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/cliconfig"
)

func runLogin(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string) int {
	fs := flag.NewFlagSet("login", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	flagBase := fs.String("base-url", "", "warehouse base URL")
	flagToken := fs.String("token", "", "bearer token")
	flagClient := fs.String("client", "", "client name reported in server logs")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintln(stderr, "pdw login:", err)
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintln(stderr, "pdw login: unexpected positional arguments")
		return 2
	}

	// Writes always go to the canonical pdw path, but prefill the prompts
	// from whatever Resolve finds — including a legacy pdw-cli config — so an
	// existing login migrates forward on the next `pdw login`.
	path, err := cliconfig.Path(getenv)
	if err != nil {
		fmt.Fprintln(stderr, "pdw login:", err)
		return 2
	}
	existing, _, err := cliconfig.Resolve(getenv)
	if err != nil {
		fmt.Fprintln(stderr, "pdw login:", err)
		return 1
	}

	reader := bufio.NewReader(stdin)
	baseDefault := existing.BaseURL
	if baseDefault == "" {
		baseDefault = defaultBaseURL
	}
	baseURL, err := promptValue(*flagBase, baseDefault, "Warehouse API URL", false, reader, stdout)
	if err != nil {
		fmt.Fprintln(stderr, "pdw login:", err)
		return 2
	}
	token, err := promptValue(*flagToken, existing.Token, "Bearer token", true, reader, stdout)
	if err != nil {
		fmt.Fprintln(stderr, "pdw login:", err)
		return 2
	}
	clientDefault := existing.ClientName
	if clientDefault == "" {
		clientDefault = "pdw"
	}
	clientName, err := promptValue(*flagClient, clientDefault, "Client name", false, reader, stdout)
	if err != nil {
		fmt.Fprintln(stderr, "pdw login:", err)
		return 2
	}

	cfg := cliconfig.Config{
		BaseURL:    strings.TrimSpace(baseURL),
		Token:      strings.TrimSpace(token),
		ClientName: strings.TrimSpace(clientName),
	}
	// Validate before touching disk — surface bad URL/empty token early.
	if _, err := cliclient.New(cfg.BaseURL, cfg.ClientName, cfg.Token); err != nil {
		fmt.Fprintln(stderr, "pdw login:", err)
		return 2
	}
	if err := cliconfig.Save(path, cfg); err != nil {
		fmt.Fprintln(stderr, "pdw login:", err)
		return 1
	}
	fmt.Fprintf(stdout, "Saved configuration for %s as %s to %s\n", cfg.BaseURL, cfg.ClientName, path)
	return 0
}

func runLogout(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) > 0 {
		fmt.Fprintln(stderr, "pdw logout: unexpected arguments")
		return 2
	}
	// Remove both the canonical pdw config and any legacy pdw-cli config so
	// logout fully de-authenticates regardless of which one a prior login wrote.
	removed, err := cliconfig.DeleteAll(getenv)
	if err != nil {
		fmt.Fprintln(stderr, "pdw logout:", err)
		return 1
	}
	if len(removed) == 0 {
		fmt.Fprintln(stdout, "Nothing to remove (not configured)")
		return 0
	}
	for _, path := range removed {
		fmt.Fprintf(stdout, "Removed %s\n", path)
	}
	return 0
}

func runConfig(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) == 0 || args[0] != "show" {
		fmt.Fprintln(stderr, `pdw config: only "show" is supported (e.g. pdw config show)`)
		return 2
	}
	if len(args) > 1 {
		fmt.Fprintln(stderr, "pdw config show: unexpected arguments")
		return 2
	}
	cfg, path, err := cliconfig.Resolve(getenv)
	if err != nil {
		fmt.Fprintln(stderr, "pdw config:", err)
		return 1
	}
	if cfg == (cliconfig.Config{}) {
		fmt.Fprintf(stdout, "not configured (no config at %s)\nrun `pdw login` to set up\n", path)
		return 0
	}
	fmt.Fprintf(stdout, "path: %s\n", path)
	body, err := json.MarshalIndent(cfg.Redacted(), "", "  ")
	if err != nil {
		fmt.Fprintln(stderr, "pdw config:", err)
		return 1
	}
	fmt.Fprintln(stdout, string(body))
	return 0
}

// promptValue resolves a single login field. Order: explicit flag, then a
// single line from stdin. If stdin returns nothing and there's an existing
// value, that's the new value; otherwise the field is required.
//
// secret is reserved for fields where we may later want to disable terminal
// echo; today it only controls the default-hint visibility.
func promptValue(flagValue, existing, label string, secret bool, reader *bufio.Reader, stdout io.Writer) (string, error) {
	if v := strings.TrimSpace(flagValue); v != "" {
		return v, nil
	}
	hint := ""
	if existing != "" {
		if secret {
			hint = " [keep existing]"
		} else {
			hint = " [" + existing + "]"
		}
	}
	fmt.Fprintf(stdout, "%s%s: ", label, hint)
	line, err := reader.ReadString('\n')
	if err != nil && line == "" {
		// EOF with no data: fall back to existing, or fail.
		if existing != "" {
			return existing, nil
		}
		return "", fmt.Errorf("%s is required", strings.ToLower(label))
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		if existing != "" {
			return existing, nil
		}
		return "", fmt.Errorf("%s is required", strings.ToLower(label))
	}
	return line, nil
}
