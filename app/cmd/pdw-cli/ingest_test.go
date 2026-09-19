package main

import (
	"bytes"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliconfig"
	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
)

// capturedLocal records one dispatch into a fake localCommand so tests can
// assert on the args and config pdw handed a native uploader without running
// one.
type capturedLocal struct {
	called bool
	args   []string
	cfg    ingestclient.Config
	getenv func(string) string
}

func fakeLocal(cap *capturedLocal, code int) localCommand {
	return func(args []string, _ io.Reader, stdout, _ io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
		cap.called = true
		cap.args = append([]string(nil), args...)
		cap.cfg = cfg
		cap.getenv = getenv
		io.WriteString(stdout, "fake ran\n")
		return code
	}
}

// withFakeIngestSource swaps one ingest source's Run for a fake for the
// duration of a test.
func withFakeIngestSource(t *testing.T, source string, code int) *capturedLocal {
	t.Helper()
	prev, ok := ingestSources[source]
	if !ok {
		t.Fatalf("source %q is not in the ingest table", source)
	}
	cap := &capturedLocal{}
	ingestSources[source] = fakeLocal(cap, code)
	t.Cleanup(func() { ingestSources[source] = prev })
	return cap
}

// isolatedEnv returns a getenv with no login config reachable (a fresh
// XDG_CONFIG_HOME) and no project .env, so config assertions are exact.
func isolatedEnv(t *testing.T, extra map[string]string) func(string) string {
	t.Helper()
	env := map[string]string{
		"XDG_CONFIG_HOME":        t.TempDir(),
		"PDW_INGEST_PROJECT_DIR": t.TempDir(),
	}
	for k, v := range extra {
		env[k] = v
	}
	return func(k string) string { return env[k] }
}

func TestIngestSourceTableIsExactlyTheNativeUploaders(t *testing.T) {
	want := []string{
		"agent-sessions", "apple-contacts", "apple-messages", "apple-notes",
		"apple-photos", "manual-finance", "plaid", "voice-memos",
	}
	var got []string
	for name, run := range ingestSources {
		if run == nil {
			t.Fatalf("source %q has a nil runner", name)
		}
		got = append(got, name)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ingest sources = %v, want %v", got, want)
	}
	if _, ok := resolveIngestSource("whatsapp"); ok {
		t.Fatal("whatsapp is server-side and must not be an ingest source")
	}
	if _, ok := resolveIngestSource("claude-desktop"); ok {
		t.Fatal("claude-desktop is dispatched before the table, not through it")
	}
	names := ingestSourceNames()
	if !sort.StringsAreSorted(names) || names[len(names)-1] != "voice-memos" || names[0] != "agent-sessions" {
		t.Fatalf("ingestSourceNames = %v", names)
	}
	found := false
	for _, n := range names {
		if n == "claude-desktop" {
			found = true
		}
	}
	if !found {
		t.Fatal("help/error source list must include claude-desktop")
	}
}

func TestIngestForwardsArgsToTheNativeUploader(t *testing.T) {
	cap := withFakeIngestSource(t, "agent-sessions", 0)
	var out, errOut bytes.Buffer
	code := runIngest([]string{"agent-sessions", "--limit", "5", "--mode", "full"}, strings.NewReader(""), &out, &errOut, isolatedEnv(t, nil))
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, errOut.String())
	}
	if !cap.called {
		t.Fatal("uploader was not invoked")
	}
	if want := []string{"--limit", "5", "--mode", "full"}; !reflect.DeepEqual(cap.args, want) {
		t.Fatalf("args = %#v, want %#v", cap.args, want)
	}
	if !strings.Contains(out.String(), "fake ran") {
		t.Fatalf("uploader stdout not wired through: %q", out.String())
	}
}

func TestIngestPropagatesExitCode(t *testing.T) {
	withFakeIngestSource(t, "voice-memos", 7)
	var out, errOut bytes.Buffer
	code := runIngest([]string{"voice-memos"}, strings.NewReader(""), &out, &errOut, isolatedEnv(t, nil))
	if code != 7 {
		t.Fatalf("exit = %d, want 7 (uploader exit code must propagate)", code)
	}
}

func TestIngestUnknownSourceErrors(t *testing.T) {
	cap := withFakeIngestSource(t, "voice-memos", 0)
	var out, errOut bytes.Buffer
	code := runIngest([]string{"bogus"}, strings.NewReader(""), &out, &errOut, isolatedEnv(t, nil))
	if code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if cap.called {
		t.Fatal("must not run an uploader for an unknown source")
	}
	if !strings.Contains(errOut.String(), "bogus") {
		t.Fatalf("stderr should name the bad source: %s", errOut.String())
	}
	for _, s := range []string{"voice-memos", "apple-notes", "apple-messages", "agent-sessions", "claude-desktop", "plaid", "apple-photos", "manual-finance", "apple-contacts"} {
		if !strings.Contains(errOut.String(), s) {
			t.Fatalf("stderr should list valid source %q: %s", s, errOut.String())
		}
	}
}

func TestIngestNoSourcePrintsUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	code := runIngest(nil, strings.NewReader(""), &out, &errOut, isolatedEnv(t, nil))
	if code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(errOut.String(), "voice-memos") {
		t.Fatalf("usage should list sources: %s", errOut.String())
	}
}

func TestIngestHelpFlagPrintsUsage(t *testing.T) {
	for _, flag := range []string{"-h", "--help"} {
		cap := withFakeIngestSource(t, "voice-memos", 0)
		var out, errOut bytes.Buffer
		code := runIngest([]string{flag}, strings.NewReader(""), &out, &errOut, isolatedEnv(t, nil))
		if code != 0 {
			t.Fatalf("%s exit = %d, want 0", flag, code)
		}
		if cap.called {
			t.Fatalf("%s must not run an uploader", flag)
		}
		if !strings.Contains(out.String(), "pdw ingest") {
			t.Fatalf("%s should print ingest usage to stdout: %s", flag, out.String())
		}
	}
	for _, gone := range []string{"PDW_UV_BIN", "uv ", "python", "pdw ingest plaid sync"} {
		if strings.Contains(ingestUsage, gone) {
			t.Fatalf("ingest usage still mentions %q", gone)
		}
	}
	if !strings.Contains(ingestUsage, "PDW_INGEST_PROJECT_DIR") || !strings.Contains(ingestUsage, ".env") {
		t.Fatal("ingest usage must document PDW_INGEST_PROJECT_DIR as the .env directory")
	}
	if !strings.Contains(ingestUsage, "pdw ingest plaid update <item-id>") {
		t.Fatal("help must name the existing-Item repair command")
	}
}

func TestIngestPassesUploaderHelpThrough(t *testing.T) {
	// `pdw ingest voice-memos --help` must reach the uploader's own flag set,
	// not print pdw's ingest usage.
	cap := withFakeIngestSource(t, "voice-memos", 0)
	var out, errOut bytes.Buffer
	code := runIngest([]string{"voice-memos", "--help"}, strings.NewReader(""), &out, &errOut, isolatedEnv(t, nil))
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	if !cap.called || !reflect.DeepEqual(cap.args, []string{"--help"}) {
		t.Fatalf("expected --help forwarded to the uploader, got called=%v args=%v", cap.called, cap.args)
	}
	if strings.Contains(out.String(), "SOURCES") {
		t.Fatal("printed pdw's ingest usage instead of forwarding --help")
	}
}

func TestEveryRealUploaderAnswersHelpOnItsOwn(t *testing.T) {
	// The real Run functions, not fakes: --help must print usage to stdout and
	// exit 0 without touching the machine or the network.
	for name, run := range ingestSources {
		var out, errOut bytes.Buffer
		code := run([]string{"--help"}, strings.NewReader(""), &out, &errOut, isolatedEnv(t, nil), ingestclient.Config{})
		if code != 0 {
			t.Fatalf("%s --help exit = %d, stderr=%s", name, code, errOut.String())
		}
		// apple-photos prints its help to stderr; every other uploader to
		// stdout. Either is the uploader's own usage rather than pdw's.
		if help := out.String() + errOut.String(); !strings.Contains(help, "pdw ingest "+name) {
			t.Fatalf("%s --help did not print its own usage: %q", name, help)
		}
	}
}

func TestIngestConfigFromEnv(t *testing.T) {
	cap := withFakeIngestSource(t, "voice-memos", 0)
	env := isolatedEnv(t, map[string]string{"PDW_API_URL": "https://warehouse.example", "PDW_SECRET_TOKEN": "tok"})
	var out, errOut bytes.Buffer
	runIngest([]string{"voice-memos"}, strings.NewReader(""), &out, &errOut, env)
	if cap.cfg.BaseURL != "https://warehouse.example" || cap.cfg.Token != "tok" {
		t.Fatalf("cfg = %+v", cap.cfg)
	}
}

func TestIngestRootFlagsOverrideEnvForUploaderConfig(t *testing.T) {
	cap := withFakeIngestSource(t, "voice-memos", 0)
	env := isolatedEnv(t, map[string]string{"PDW_API_URL": "https://env.example", "PDW_SECRET_TOKEN": "env-token"})
	var out, errOut bytes.Buffer
	code := runIngestWithConfig([]string{"voice-memos"}, strings.NewReader(""), &out, &errOut, env, "https://flag.example", "flag-token")
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, errOut.String())
	}
	if cap.cfg.BaseURL != "https://flag.example" || cap.cfg.Token != "flag-token" {
		t.Fatalf("root flags must win: cfg = %+v", cap.cfg)
	}
}

func TestIngestInjectsFromLoginConfigFile(t *testing.T) {
	// The core of the unification: `pdw login` writes a config file with the
	// warehouse URL + token; `pdw ingest` must reuse it so uploads work with no
	// separate ingest configuration.
	getenv := isolatedEnv(t, nil)
	path, err := cliconfig.Path(getenv)
	if err != nil {
		t.Fatal(err)
	}
	if err := cliconfig.Save(path, cliconfig.Config{BaseURL: "https://login.example", Token: "login-token"}); err != nil {
		t.Fatal(err)
	}
	cap := withFakeIngestSource(t, "agent-sessions", 0)
	var out, errOut bytes.Buffer
	runIngest([]string{"agent-sessions"}, strings.NewReader(""), &out, &errOut, getenv)
	if cap.cfg.BaseURL != "https://login.example" || cap.cfg.Token != "login-token" {
		t.Fatalf("config not taken from login file: %+v", cap.cfg)
	}
}

func TestIngestLeavesConfigEmptyForTheUploaderToFillFromDotenv(t *testing.T) {
	// A machine that never ran `pdw login` keeps PDW_API_URL in the repo .env;
	// pdw hands an empty config through and the uploader applies its own
	// .env fallback, so pdw must not refuse up front.
	cap := withFakeIngestSource(t, "apple-notes", 0)
	var out, errOut bytes.Buffer
	code := runIngest([]string{"apple-notes"}, strings.NewReader(""), &out, &errOut, isolatedEnv(t, nil))
	if code != 0 || !cap.called {
		t.Fatalf("exit=%d called=%v stderr=%s", code, cap.called, errOut.String())
	}
	if cap.cfg != (ingestclient.Config{}) {
		t.Fatalf("expected empty config, got %+v", cap.cfg)
	}
}

func TestRunDispatchesIngestWithoutAPIConfig(t *testing.T) {
	cap := withFakeIngestSource(t, "apple-notes", 0)
	var out, errOut bytes.Buffer
	code := run([]string{"ingest", "apple-notes", "--mode", "full"}, strings.NewReader(""), &out, &errOut, isolatedEnv(t, nil))
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, errOut.String())
	}
	if !cap.called || !reflect.DeepEqual(cap.args, []string{"--mode", "full"}) {
		t.Fatalf("run() did not dispatch ingest: called=%v args=%v", cap.called, cap.args)
	}
}

func TestRunDispatchesIngestWithRootConfigFlags(t *testing.T) {
	cap := withFakeIngestSource(t, "agent-sessions", 0)
	var out, errOut bytes.Buffer
	code := run([]string{"--base-url", "https://flag.example", "--token", "flag-token", "ingest", "agent-sessions"}, strings.NewReader(""), &out, &errOut, isolatedEnv(t, nil))
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, errOut.String())
	}
	if cap.cfg.BaseURL != "https://flag.example" || cap.cfg.Token != "flag-token" {
		t.Fatalf("root flags not passed to uploader: %+v", cap.cfg)
	}
}

func TestPlaidUpdateCanonicalCommandForwards(t *testing.T) {
	cap := withFakeIngestSource(t, "plaid", 0)
	var out, errOut bytes.Buffer
	args := []string{"ingest", "plaid", "update", "item-existing", "--no-browser", "--host", "127.0.0.1", "--port", "8765"}
	code := run(args, strings.NewReader(""), &out, &errOut, isolatedEnv(t, nil))
	if code != 0 || !reflect.DeepEqual(cap.args, args[2:]) {
		t.Fatalf("update dispatch: code=%d args=%v", code, cap.args)
	}
}
