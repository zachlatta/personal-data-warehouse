package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliconfig"
)

// envWithHome returns a getenv stub backed by the given temp dir as $HOME
// (so cliconfig.Path resolves to <tmp>/.config/pdw-cli/config.json) plus
// any caller-supplied overrides.
func envWithHome(tmp string, extra ...map[string]string) func(string) string {
	m := map[string]string{"HOME": tmp}
	for _, e := range extra {
		for k, v := range e {
			m[k] = v
		}
	}
	return func(k string) string { return m[k] }
}

func configPathFor(t *testing.T, home string) string {
	t.Helper()
	return filepath.Join(home, ".config", "pdw-cli", "config.json")
}

func TestLoginWritesConfigFromFlags(t *testing.T) {
	home := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"login", "--base-url", "https://warehouse.example", "--token", "tok-from-flag", "--client", "laptop"},
		strings.NewReader(""), &stdout, &stderr, envWithHome(home),
	)
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	cfg, err := cliconfig.Load(configPathFor(t, home))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := cliconfig.Config{BaseURL: "https://warehouse.example", Token: "tok-from-flag", ClientName: "laptop"}
	if cfg != want {
		t.Fatalf("config = %#v, want %#v", cfg, want)
	}
	// User-facing confirmation must not echo the token.
	if strings.Contains(stdout.String(), "tok-from-flag") {
		t.Fatalf("login output leaked token: %s", stdout.String())
	}
	if !strings.Contains(stdout.String(), configPathFor(t, home)) {
		t.Fatalf("login output should mention the config path; got: %s", stdout.String())
	}
}

func TestLoginInteractivelyPromptsForMissingFields(t *testing.T) {
	home := t.TempDir()
	var stdout, stderr bytes.Buffer
	stdin := strings.NewReader("https://prompt.example\nprompt-token\nfrom-prompt\n")
	code := run([]string{"login"}, stdin, &stdout, &stderr, envWithHome(home))
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s, stdout=%s", code, stderr.String(), stdout.String())
	}
	cfg, err := cliconfig.Load(configPathFor(t, home))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := cliconfig.Config{BaseURL: "https://prompt.example", Token: "prompt-token", ClientName: "from-prompt"}
	if cfg != want {
		t.Fatalf("config = %#v, want %#v", cfg, want)
	}
}

func TestLoginPreservesExistingValuesWhenBlankSubmitted(t *testing.T) {
	home := t.TempDir()
	path := configPathFor(t, home)
	if err := cliconfig.Save(path, cliconfig.Config{
		BaseURL: "https://old.example", Token: "old-token", ClientName: "old-client",
	}); err != nil {
		t.Fatal(err)
	}
	// Blank lines should keep existing values.
	stdin := strings.NewReader("\n\n\n")
	var stdout, stderr bytes.Buffer
	code := run([]string{"login"}, stdin, &stdout, &stderr, envWithHome(home))
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	cfg, err := cliconfig.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	want := cliconfig.Config{BaseURL: "https://old.example", Token: "old-token", ClientName: "old-client"}
	if cfg != want {
		t.Fatalf("config = %#v, want %#v", cfg, want)
	}
}

func TestLoginUsesDefaultBaseURLWhenPromptBlank(t *testing.T) {
	home := t.TempDir()
	var stdout, stderr bytes.Buffer
	// Blank line for URL (should accept default) + token + client.
	stdin := strings.NewReader("\nprompt-token\nlaptop\n")
	code := run([]string{"login"}, stdin, &stdout, &stderr, envWithHome(home))
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s, stdout=%s", code, stderr.String(), stdout.String())
	}
	cfg, err := cliconfig.Load(configPathFor(t, home))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.BaseURL != defaultBaseURL {
		t.Fatalf("base url = %q, want default %q", cfg.BaseURL, defaultBaseURL)
	}
	// The prompt should advertise the default so the user knows what Enter accepts.
	if !strings.Contains(stdout.String(), defaultBaseURL) {
		t.Fatalf("prompt should show default URL hint; stdout=%s", stdout.String())
	}
}

func TestLoginRejectsMissingRequiredFieldsInNonInteractiveMode(t *testing.T) {
	home := t.TempDir()
	var stdout, stderr bytes.Buffer
	// EOF immediately: no flags + no stdin = no way to fill base_url/token.
	code := run([]string{"login"}, strings.NewReader(""), &stdout, &stderr, envWithHome(home))
	if code == 0 {
		t.Fatal("expected non-zero exit when required fields can't be supplied")
	}
	low := strings.ToLower(stderr.String())
	if !strings.Contains(low, "url") && !strings.Contains(low, "token") {
		t.Fatalf("stderr should mention missing field: %s", stderr.String())
	}
	if _, err := os.Stat(configPathFor(t, home)); !os.IsNotExist(err) {
		t.Fatalf("config should not be written on failure (err = %v)", err)
	}
}

func TestLoginValidatesURLBeforeWriting(t *testing.T) {
	home := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"login", "--base-url", "ftp://nope", "--token", "tok", "--client", "x"},
		strings.NewReader(""), &stdout, &stderr, envWithHome(home),
	)
	if code == 0 {
		t.Fatal("expected non-zero exit for invalid scheme")
	}
	if _, err := os.Stat(configPathFor(t, home)); !os.IsNotExist(err) {
		t.Fatalf("config should not be written on validation failure")
	}
}

func TestLogoutRemovesConfig(t *testing.T) {
	home := t.TempDir()
	path := configPathFor(t, home)
	if err := cliconfig.Save(path, cliconfig.Config{BaseURL: "http://x", Token: "t", ClientName: "c"}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{"logout"}, strings.NewReader(""), &stdout, &stderr, envWithHome(home))
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("config still present: %v", err)
	}
}

func TestLogoutWhenAbsentSucceeds(t *testing.T) {
	home := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"logout"}, strings.NewReader(""), &stdout, &stderr, envWithHome(home))
	if code != 0 {
		t.Fatalf("exit = %d (logout when not configured should be no-op)", code)
	}
}

func TestConfigShowRedactsToken(t *testing.T) {
	home := t.TempDir()
	if err := cliconfig.Save(configPathFor(t, home), cliconfig.Config{
		BaseURL: "https://x", Token: "shhh-do-not-print", ClientName: "me",
	}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{"config", "show"}, strings.NewReader(""), &stdout, &stderr, envWithHome(home))
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	if strings.Contains(stdout.String(), "shhh-do-not-print") {
		t.Fatalf("config show leaked token: %s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "https://x") {
		t.Fatalf("config show missing base_url: %s", stdout.String())
	}
}

func TestConfigShowWhenNotConfigured(t *testing.T) {
	home := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"config", "show"}, strings.NewReader(""), &stdout, &stderr, envWithHome(home))
	if code != 0 {
		t.Fatalf("exit = %d (config show with no file should still succeed)", code)
	}
	if !strings.Contains(stdout.String(), "not configured") && !strings.Contains(stdout.String(), "no config") {
		t.Fatalf("config show should indicate empty state: %s", stdout.String())
	}
}

// --- resolution-order tests via list ---

func TestListUsesConfigFileWhenEnvAbsent(t *testing.T) {
	home := t.TempDir()
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[]}`)
	})
	if err := cliconfig.Save(configPathFor(t, home), cliconfig.Config{
		BaseURL: srv.URL, Token: cliTestToken, ClientName: "from-config",
	}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{"list"}, strings.NewReader(""), &stdout, &stderr, envWithHome(home))
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	if srv.lastAuth != "Bearer from-config:"+cliTestToken {
		t.Fatalf("server auth = %q; expected the config-file client+token", srv.lastAuth)
	}
}

func TestEnvWinsOverConfig(t *testing.T) {
	home := t.TempDir()
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[]}`)
	})
	if err := cliconfig.Save(configPathFor(t, home), cliconfig.Config{
		BaseURL: "http://wrong-from-config", Token: "wrong-token", ClientName: "from-config",
	}); err != nil {
		t.Fatal(err)
	}
	getenv := envWithHome(home, map[string]string{
		"PDW_API_URL":      srv.URL,
		"PDW_SECRET_TOKEN": cliTestToken,
		"PDW_CLIENT_NAME":  "from-env",
	})
	var stdout, stderr bytes.Buffer
	code := run([]string{"list"}, strings.NewReader(""), &stdout, &stderr, getenv)
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	if srv.lastAuth != "Bearer from-env:"+cliTestToken {
		t.Fatalf("server auth = %q; env should have won", srv.lastAuth)
	}
}

func TestFlagWinsOverEnvAndConfig(t *testing.T) {
	home := t.TempDir()
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[]}`)
	})
	if err := cliconfig.Save(configPathFor(t, home), cliconfig.Config{
		BaseURL: "http://wrong", Token: "wrong", ClientName: "config",
	}); err != nil {
		t.Fatal(err)
	}
	getenv := envWithHome(home, map[string]string{
		"PDW_API_URL":      "http://also-wrong",
		"PDW_SECRET_TOKEN": "still-wrong",
		"PDW_CLIENT_NAME":  "env",
	})
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"--base-url", srv.URL, "--token", cliTestToken, "--client", "from-flag", "list"},
		strings.NewReader(""), &stdout, &stderr, getenv,
	)
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	if srv.lastAuth != "Bearer from-flag:"+cliTestToken {
		t.Fatalf("server auth = %q; flag should have won", srv.lastAuth)
	}
}

func TestListGivesActionableErrorWhenNotConfigured(t *testing.T) {
	home := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"list"}, strings.NewReader(""), &stdout, &stderr, envWithHome(home))
	if code == 0 {
		t.Fatal("expected non-zero exit")
	}
	// The error must point users at the new option, not just env vars.
	if !strings.Contains(stderr.String(), "pdw-cli login") {
		t.Fatalf("stderr should suggest `pdw-cli login`: %s", stderr.String())
	}
}

func TestHelpMentionsLoginCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"help"}, strings.NewReader(""), &stdout, &stderr, func(string) string { return "" })
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	for _, want := range []string{"login", "logout", "config show"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("help missing %q:\n%s", want, stdout.String())
		}
	}
}

// sanity: the helper still produces parseable JSON in TestConfigShow scenarios
var _ = json.Unmarshal
