package main

import (
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliconfig"
)

// withStubIngestExec swaps the ingest executor seam for the duration of a test
// so we can assert on the command pdw would run without spawning uv/python.
type capturedIngest struct {
	called   bool
	uvBin    string
	argv     []string
	dir      string
	extraEnv []string
}

func withStubIngestExec(t *testing.T, code int) *capturedIngest {
	t.Helper()
	cap := &capturedIngest{}
	prev := ingestExec
	ingestExec = func(uvBin string, argv []string, dir string, extraEnv []string, _ io.Reader, _, _ io.Writer) int {
		cap.called = true
		cap.uvBin = uvBin
		cap.argv = argv
		cap.dir = dir
		cap.extraEnv = extraEnv
		return code
	}
	t.Cleanup(func() { ingestExec = prev })
	return cap
}

// envValue returns the value of name in a "KEY=VALUE" slice, and whether it was
// present.
func envValue(env []string, name string) (string, bool) {
	for _, kv := range env {
		if k, v, ok := strings.Cut(kv, "="); ok && k == name {
			return v, true
		}
	}
	return "", false
}

func TestIngestResolvesKnownSources(t *testing.T) {
	want := map[string]string{
		"voice-memos":    "personal_data_warehouse_voice_memos.cli",
		"apple-notes":    "personal_data_warehouse_apple_notes.cli",
		"apple-messages": "personal_data_warehouse_apple_messages.cli",
		"agent-sessions": "personal_data_warehouse_agent_sessions.cli",
	}
	for source, module := range want {
		got, ok := resolveIngestModule(source)
		if !ok {
			t.Fatalf("source %q should be known", source)
		}
		if got != module {
			t.Fatalf("source %q -> module %q, want %q", source, got, module)
		}
	}
	if _, ok := resolveIngestModule("whatsapp"); ok {
		t.Fatalf("whatsapp is server-side and must not be an ingest source")
	}
	if _, ok := resolveIngestModule("bogus"); ok {
		t.Fatalf("unknown source must not resolve")
	}
}

func TestIngestArgvIncludesModuleAndPassthrough(t *testing.T) {
	argv := ingestArgv("personal_data_warehouse_voice_memos.cli", []string{"--mode", "incremental", "--limit", "5"})
	want := []string{"run", "python", "-m", "personal_data_warehouse_voice_memos.cli", "--mode", "incremental", "--limit", "5"}
	if !reflect.DeepEqual(argv, want) {
		t.Fatalf("ingestArgv = %#v, want %#v", argv, want)
	}
}

func TestIngestUvBinResolution(t *testing.T) {
	if got := ingestUvBin(func(string) string { return "" }); got != "uv" {
		t.Fatalf("default uv bin = %q, want uv", got)
	}
	env := map[string]string{"PDW_UV_BIN": "/opt/homebrew/bin/uv"}
	if got := ingestUvBin(func(k string) string { return env[k] }); got != "/opt/homebrew/bin/uv" {
		t.Fatalf("PDW_UV_BIN not honored, got %q", got)
	}
}

func TestIngestProjectDirResolution(t *testing.T) {
	if got := ingestProjectDir(func(string) string { return "" }); got != "" {
		t.Fatalf("default project dir should be empty (inherit cwd), got %q", got)
	}
	env := map[string]string{"PDW_INGEST_PROJECT_DIR": "/repo"}
	if got := ingestProjectDir(func(k string) string { return env[k] }); got != "/repo" {
		t.Fatalf("PDW_INGEST_PROJECT_DIR not honored, got %q", got)
	}
}

func TestIngestRunsResolvedCommand(t *testing.T) {
	cap := withStubIngestExec(t, 0)
	env := map[string]string{
		"PDW_UV_BIN":             "/opt/homebrew/bin/uv",
		"PDW_INGEST_PROJECT_DIR": "/repo",
	}
	var out, errOut bytes.Buffer
	code := runIngest([]string{"agent-sessions", "--limit", "5"}, strings.NewReader(""), &out, &errOut, func(k string) string { return env[k] })
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, errOut.String())
	}
	if !cap.called {
		t.Fatal("ingest executor was not invoked")
	}
	if cap.uvBin != "/opt/homebrew/bin/uv" {
		t.Fatalf("uvBin = %q", cap.uvBin)
	}
	if cap.dir != "/repo" {
		t.Fatalf("dir = %q", cap.dir)
	}
	want := []string{"run", "python", "-m", "personal_data_warehouse_agent_sessions.cli", "--limit", "5"}
	if !reflect.DeepEqual(cap.argv, want) {
		t.Fatalf("argv = %#v, want %#v", cap.argv, want)
	}
}

func TestIngestPropagatesExitCode(t *testing.T) {
	withStubIngestExec(t, 7)
	var out, errOut bytes.Buffer
	code := runIngest([]string{"voice-memos"}, strings.NewReader(""), &out, &errOut, func(string) string { return "" })
	if code != 7 {
		t.Fatalf("exit = %d, want 7 (uploader exit code must propagate)", code)
	}
}

func TestIngestUnknownSourceErrors(t *testing.T) {
	cap := withStubIngestExec(t, 0)
	var out, errOut bytes.Buffer
	code := runIngest([]string{"bogus"}, strings.NewReader(""), &out, &errOut, func(string) string { return "" })
	if code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if cap.called {
		t.Fatal("must not exec for an unknown source")
	}
	if !strings.Contains(errOut.String(), "bogus") {
		t.Fatalf("stderr should name the bad source: %s", errOut.String())
	}
	for _, s := range []string{"voice-memos", "apple-notes", "apple-messages", "agent-sessions"} {
		if !strings.Contains(errOut.String(), s) {
			t.Fatalf("stderr should list valid source %q: %s", s, errOut.String())
		}
	}
}

func TestIngestNoSourcePrintsUsage(t *testing.T) {
	cap := withStubIngestExec(t, 0)
	var out, errOut bytes.Buffer
	code := runIngest(nil, strings.NewReader(""), &out, &errOut, func(string) string { return "" })
	if code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if cap.called {
		t.Fatal("must not exec when no source is given")
	}
	if !strings.Contains(errOut.String(), "voice-memos") {
		t.Fatalf("usage should list sources: %s", errOut.String())
	}
}

func TestIngestHelpFlagPrintsUsage(t *testing.T) {
	for _, flag := range []string{"-h", "--help"} {
		cap := withStubIngestExec(t, 0)
		var out, errOut bytes.Buffer
		code := runIngest([]string{flag}, strings.NewReader(""), &out, &errOut, func(string) string { return "" })
		if code != 0 {
			t.Fatalf("%s exit = %d, want 0", flag, code)
		}
		if cap.called {
			t.Fatalf("%s must not exec", flag)
		}
		if !strings.Contains(out.String(), "pdw ingest") {
			t.Fatalf("%s should print ingest usage to stdout: %s", flag, out.String())
		}
	}
}

func TestIngestPassesUploaderHelpThrough(t *testing.T) {
	// `pdw ingest voice-memos --help` must forward --help to the uploader's
	// own argparse, not print pdw's ingest usage.
	cap := withStubIngestExec(t, 0)
	var out, errOut bytes.Buffer
	code := runIngest([]string{"voice-memos", "--help"}, strings.NewReader(""), &out, &errOut, func(string) string { return "" })
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	if !cap.called {
		t.Fatal("expected exec so uploader handles --help")
	}
	want := []string{"run", "python", "-m", "personal_data_warehouse_voice_memos.cli", "--help"}
	if !reflect.DeepEqual(cap.argv, want) {
		t.Fatalf("argv = %#v, want %#v", cap.argv, want)
	}
}

func TestIngestInheritsBaseURLFromEnv(t *testing.T) {
	// When PDW_API_URL is already in the environment the child inherits it
	// directly, so pdw must not re-inject a duplicate.
	cap := withStubIngestExec(t, 0)
	env := map[string]string{"PDW_API_URL": "https://warehouse.example"}
	var out, errOut bytes.Buffer
	runIngest([]string{"voice-memos"}, strings.NewReader(""), &out, &errOut, func(k string) string { return env[k] })
	if _, ok := envValue(cap.extraEnv, "PDW_API_URL"); ok {
		t.Fatalf("must not re-inject PDW_API_URL already present in env: %#v", cap.extraEnv)
	}
}

func TestIngestInheritsTokenFromEnv(t *testing.T) {
	// PDW_SECRET_TOKEN is already in the Python client's signing-key chain, so
	// pdw need not re-inject it; it just inherits via the process environment.
	cap := withStubIngestExec(t, 0)
	env := map[string]string{"PDW_API_URL": "https://warehouse.example", "PDW_SECRET_TOKEN": "tok"}
	var out, errOut bytes.Buffer
	runIngest([]string{"voice-memos"}, strings.NewReader(""), &out, &errOut, func(k string) string { return env[k] })
	if _, ok := envValue(cap.extraEnv, "PDW_SECRET_TOKEN"); ok {
		t.Fatalf("must not re-inject a token when PDW_SECRET_TOKEN is already set: %#v", cap.extraEnv)
	}
}

func TestIngestRootFlagsOverrideEnvForUploaderConfig(t *testing.T) {
	cap := withStubIngestExec(t, 0)
	env := map[string]string{
		"PDW_API_URL":      "https://env.example",
		"PDW_SECRET_TOKEN": "env-token",
	}
	var out, errOut bytes.Buffer
	code := runIngestWithConfig(
		[]string{"voice-memos"},
		strings.NewReader(""),
		&out,
		&errOut,
		func(k string) string { return env[k] },
		"https://flag.example",
		"flag-token",
	)
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, errOut.String())
	}
	if got, ok := envValue(cap.extraEnv, "PDW_API_URL"); !ok || got != "https://flag.example" {
		t.Fatalf("base URL flag not passed to uploader: %#v", cap.extraEnv)
	}
	if got, ok := envValue(cap.extraEnv, "PDW_SECRET_TOKEN"); !ok || got != "flag-token" {
		t.Fatalf("token flag not passed to uploader: %#v", cap.extraEnv)
	}
}

func TestIngestInjectsFromLoginConfigFile(t *testing.T) {
	// The core of the unification: `pdw login` writes a config file with the
	// warehouse URL + token; `pdw ingest` must reuse it so uploads work with no
	// separate ingest configuration.
	dir := t.TempDir()
	env := map[string]string{"XDG_CONFIG_HOME": dir}
	getenv := func(k string) string { return env[k] }
	path, err := cliconfig.Path(getenv)
	if err != nil {
		t.Fatal(err)
	}
	if err := cliconfig.Save(path, cliconfig.Config{BaseURL: "https://login.example", Token: "login-token"}); err != nil {
		t.Fatal(err)
	}
	cap := withStubIngestExec(t, 0)
	var out, errOut bytes.Buffer
	runIngest([]string{"agent-sessions"}, strings.NewReader(""), &out, &errOut, getenv)
	if got, ok := envValue(cap.extraEnv, "PDW_API_URL"); !ok || got != "https://login.example" {
		t.Fatalf("base URL not taken from login config: %#v", cap.extraEnv)
	}
	if got, ok := envValue(cap.extraEnv, "PDW_SECRET_TOKEN"); !ok || got != "login-token" {
		t.Fatalf("signing key not taken from login config: %#v", cap.extraEnv)
	}
}

func TestRunDispatchesIngestWithoutAPIConfig(t *testing.T) {
	// ingest execs local uploaders; it must not require PDW_API_URL/token.
	cap := withStubIngestExec(t, 0)
	var out, errOut bytes.Buffer
	code := run([]string{"ingest", "apple-notes", "--mode", "full"}, strings.NewReader(""), &out, &errOut, func(string) string { return "" })
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, errOut.String())
	}
	if !cap.called {
		t.Fatal("run() did not dispatch ingest")
	}
	want := []string{"run", "python", "-m", "personal_data_warehouse_apple_notes.cli", "--mode", "full"}
	if !reflect.DeepEqual(cap.argv, want) {
		t.Fatalf("argv = %#v, want %#v", cap.argv, want)
	}
}

func TestRunDispatchesIngestWithRootConfigFlags(t *testing.T) {
	cap := withStubIngestExec(t, 0)
	var out, errOut bytes.Buffer
	code := run(
		[]string{"--base-url", "https://flag.example", "--token", "flag-token", "ingest", "agent-sessions"},
		strings.NewReader(""),
		&out,
		&errOut,
		func(string) string { return "" },
	)
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, errOut.String())
	}
	if !cap.called {
		t.Fatal("run() did not dispatch ingest")
	}
	if got, ok := envValue(cap.extraEnv, "PDW_API_URL"); !ok || got != "https://flag.example" {
		t.Fatalf("base URL flag not passed to uploader: %#v", cap.extraEnv)
	}
	if got, ok := envValue(cap.extraEnv, "PDW_SECRET_TOKEN"); !ok || got != "flag-token" {
		t.Fatalf("token flag not passed to uploader: %#v", cap.extraEnv)
	}
}
