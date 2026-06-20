package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestDueForAutoUpdate(t *testing.T) {
	base := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	debounce := 5 * time.Minute
	cases := []struct {
		name   string
		now    time.Time
		last   time.Time
		stamp  bool
		expect bool
	}{
		{"no stamp yet fires", base, time.Time{}, false, true},
		{"just stamped is debounced", base, base, true, false},
		{"inside window is debounced", base.Add(4 * time.Minute), base, true, false},
		{"exactly at window fires", base.Add(5 * time.Minute), base, true, true},
		{"past window fires", base.Add(6 * time.Minute), base, true, true},
		{"clock skew (last in future) is debounced", base, base.Add(time.Hour), true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dueForAutoUpdate(tc.now, tc.last, tc.stamp, debounce); got != tc.expect {
				t.Fatalf("dueForAutoUpdate = %v, want %v", got, tc.expect)
			}
		})
	}
}

func TestAutoUpdateEnabled(t *testing.T) {
	cases := []struct {
		name    string
		cmd     string
		version string
		env     map[string]string
		expect  bool
	}{
		{"normal command on a real build", "sql", "v0.0.5", nil, true},
		{"dev build skips", "sql", "dev", nil, false},
		{"empty version skips", "sql", "", nil, false},
		{"update command skips", "update", "v0.0.5", nil, false},
		{"worker command skips", autoUpdateCommand, "v0.0.5", nil, false},
		{"version command skips", "version", "v0.0.5", nil, false},
		{"help command skips", "help", "v0.0.5", nil, false},
		{"opt-out env (1) skips", "sql", "v0.0.5", map[string]string{"PDW_NO_AUTO_UPDATE": "1"}, false},
		{"opt-out env (true) skips", "sql", "v0.0.5", map[string]string{"PDW_NO_AUTO_UPDATE": "true"}, false},
		{"opt-out env (0) stays on", "sql", "v0.0.5", map[string]string{"PDW_NO_AUTO_UPDATE": "0"}, true},
		{"opt-out env (empty) stays on", "sql", "v0.0.5", map[string]string{"PDW_NO_AUTO_UPDATE": ""}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := autoUpdateEnabled(tc.cmd, tc.version, envMap(tc.env)); got != tc.expect {
				t.Fatalf("autoUpdateEnabled(%q, %q) = %v, want %v", tc.cmd, tc.version, got, tc.expect)
			}
		})
	}
}

func TestAutoUpdateStampRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "auto-update.json")

	if _, ok := readAutoUpdateStamp(path); ok {
		t.Fatal("expected no stamp before any write")
	}

	want := time.Date(2026, 6, 20, 9, 30, 0, 0, time.UTC)
	if err := writeAutoUpdateStamp(path, want); err != nil {
		t.Fatalf("writeAutoUpdateStamp: %v", err)
	}
	got, ok := readAutoUpdateStamp(path)
	if !ok {
		t.Fatal("expected a stamp after writing")
	}
	if !got.Equal(want) {
		t.Fatalf("stamp = %v, want %v", got, want)
	}
}

func TestAutoUpdateStatePathSitsBesideConfig(t *testing.T) {
	home := t.TempDir()
	path, err := autoUpdateStatePath(envMap(map[string]string{"HOME": home}))
	if err != nil {
		t.Fatalf("autoUpdateStatePath: %v", err)
	}
	want := filepath.Join(home, ".config", "pdw", "auto-update.json")
	if path != want {
		t.Fatalf("autoUpdateStatePath = %q, want %q", path, want)
	}
}

// withAutoUpdateStubs swaps the package-level clock and spawn hook for the
// duration of a test, returning a pointer to a spawn-call counter.
func withAutoUpdateStubs(t *testing.T, now time.Time) *int {
	t.Helper()
	prevNow, prevSpawn, prevVersion := nowFunc, spawnUpdate, version
	t.Cleanup(func() { nowFunc, spawnUpdate, version = prevNow, prevSpawn, prevVersion })
	calls := 0
	nowFunc = func() time.Time { return now }
	spawnUpdate = func() error { calls++; return nil }
	return &calls
}

func TestMaybeAutoUpdateSpawnsAndStampsWhenDue(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	calls := withAutoUpdateStubs(t, now)
	version = "v0.0.5"
	home := t.TempDir()

	maybeAutoUpdate("sql", envMap(map[string]string{"HOME": home}))

	if *calls != 1 {
		t.Fatalf("spawn calls = %d, want 1", *calls)
	}
	path := filepath.Join(home, ".config", "pdw", "auto-update.json")
	stamp, ok := readAutoUpdateStamp(path)
	if !ok {
		t.Fatal("expected a stamp to be written")
	}
	if !stamp.Equal(now) {
		t.Fatalf("stamp = %v, want %v", stamp, now)
	}
}

func TestMaybeAutoUpdateDebouncedWithinWindow(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	calls := withAutoUpdateStubs(t, now)
	version = "v0.0.5"
	home := t.TempDir()
	path := filepath.Join(home, ".config", "pdw", "auto-update.json")
	if err := writeAutoUpdateStamp(path, now.Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}

	maybeAutoUpdate("sql", envMap(map[string]string{"HOME": home}))

	if *calls != 0 {
		t.Fatalf("spawn calls = %d, want 0 (debounced)", *calls)
	}
	// The recent stamp must be left untouched so the window keeps counting
	// from the real last attempt.
	stamp, _ := readAutoUpdateStamp(path)
	if !stamp.Equal(now.Add(-2 * time.Minute)) {
		t.Fatalf("stamp = %v, want it left at the earlier attempt", stamp)
	}
}

func TestMaybeAutoUpdateFiresAfterWindow(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	calls := withAutoUpdateStubs(t, now)
	version = "v0.0.5"
	home := t.TempDir()
	path := filepath.Join(home, ".config", "pdw", "auto-update.json")
	if err := writeAutoUpdateStamp(path, now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}

	maybeAutoUpdate("sql", envMap(map[string]string{"HOME": home}))

	if *calls != 1 {
		t.Fatalf("spawn calls = %d, want 1", *calls)
	}
	stamp, _ := readAutoUpdateStamp(path)
	if !stamp.Equal(now) {
		t.Fatalf("stamp = %v, want refreshed to %v", stamp, now)
	}
}

func TestMaybeAutoUpdateSkipsDevBuild(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	calls := withAutoUpdateStubs(t, now)
	version = "dev"
	home := t.TempDir()

	maybeAutoUpdate("sql", envMap(map[string]string{"HOME": home}))

	if *calls != 0 {
		t.Fatalf("spawn calls = %d, want 0 for dev build", *calls)
	}
	if _, ok := readAutoUpdateStamp(filepath.Join(home, ".config", "pdw", "auto-update.json")); ok {
		t.Fatal("dev build should not write a stamp")
	}
}

func TestMaybeAutoUpdateSkipsUpdateCommand(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	calls := withAutoUpdateStubs(t, now)
	version = "v0.0.5"
	home := t.TempDir()

	maybeAutoUpdate("update", envMap(map[string]string{"HOME": home}))

	if *calls != 0 {
		t.Fatalf("spawn calls = %d, want 0 for the manual update command", *calls)
	}
}

func TestMaybeAutoUpdateSkipsWhenOptedOut(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	calls := withAutoUpdateStubs(t, now)
	version = "v0.0.5"
	home := t.TempDir()

	maybeAutoUpdate("sql", envMap(map[string]string{"HOME": home, "PDW_NO_AUTO_UPDATE": "1"}))

	if *calls != 0 {
		t.Fatalf("spawn calls = %d, want 0 when opted out", *calls)
	}
}

// TestRunDispatchesAutoUpdateWorker exercises the full run() entry point for
// the hidden worker command: it must replace the target binary via the same
// machinery as `pdw update`, and must NOT recursively spawn another updater.
func TestRunDispatchesAutoUpdateWorker(t *testing.T) {
	prevSpawn := spawnUpdate
	t.Cleanup(func() { spawnUpdate = prevSpawn })
	spawned := 0
	spawnUpdate = func() error { spawned++; return nil }

	target := filepath.Join(t.TempDir(), "pdw")
	if err := os.WriteFile(target, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}
	newBinary := []byte("NEW BACKGROUND BINARY")
	api := newGitHubStub(t, "octo/repo", "v1.0.0", newBinary, "")

	var stdout, stderr bytes.Buffer
	code := run(
		[]string{autoUpdateCommand, "--github-api", api.URL, "--repo", "octo/repo", "--target", target},
		strings.NewReader(""), &stdout, &stderr, func(string) string { return "" },
	)
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	got, _ := os.ReadFile(target)
	if !bytes.Equal(got, newBinary) {
		t.Fatalf("worker did not replace target: %q", got)
	}
	if spawned != 0 {
		t.Fatalf("worker should not spawn another updater, spawned=%d", spawned)
	}
	// Worker is silent: nothing leaks to the caller's stdout/stderr.
	if stdout.String() != "" || stderr.String() != "" {
		t.Fatalf("worker should be quiet, got stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

// TestRunTriggersBackgroundUpdateForNormalCommand proves the foreground path
// in run() kicks the debounced background update for an ordinary command.
func TestRunTriggersBackgroundUpdateForNormalCommand(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	calls := withAutoUpdateStubs(t, now)
	version = "v0.0.5"
	home := t.TempDir()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[]}`)
	}))
	t.Cleanup(srv.Close)

	env := map[string]string{
		"HOME":             home,
		"PDW_API_URL":      srv.URL,
		"PDW_SECRET_TOKEN": cliTestToken,
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{"list"}, strings.NewReader(""), &stdout, &stderr, envMap(env))
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, stderr.String())
	}
	if *calls != 1 {
		t.Fatalf("background update spawn calls = %d, want 1", *calls)
	}
}

func TestMaybeAutoUpdateSkipsWhenNoConfigDir(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	calls := withAutoUpdateStubs(t, now)
	version = "v0.0.5"

	// Neither HOME nor XDG_CONFIG_HOME set: no place for a stamp, so we must
	// not spawn (a spawn with no debounce would refire on every invocation).
	maybeAutoUpdate("sql", envMap(nil))

	if *calls != 0 {
		t.Fatalf("spawn calls = %d, want 0 when no config dir resolves", *calls)
	}
}
