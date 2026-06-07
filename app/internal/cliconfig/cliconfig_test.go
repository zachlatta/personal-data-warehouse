package cliconfig_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliconfig"
)

func envFromMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestPathRespectsXDGConfigHome(t *testing.T) {
	xdg := t.TempDir()
	got, err := cliconfig.Path(envFromMap(map[string]string{"XDG_CONFIG_HOME": xdg, "HOME": "/tmp/should-not-be-used"}))
	if err != nil {
		t.Fatalf("Path: %v", err)
	}
	want := filepath.Join(xdg, "pdw", "config.json")
	if got != want {
		t.Fatalf("Path = %q, want %q", got, want)
	}
}

func TestPathDefaultsToHomeDotConfig(t *testing.T) {
	home := t.TempDir()
	got, err := cliconfig.Path(envFromMap(map[string]string{"HOME": home}))
	if err != nil {
		t.Fatalf("Path: %v", err)
	}
	want := filepath.Join(home, ".config", "pdw", "config.json")
	if got != want {
		t.Fatalf("Path = %q, want %q", got, want)
	}
}

func TestPathFailsWhenHomeUnknown(t *testing.T) {
	_, err := cliconfig.Path(envFromMap(map[string]string{}))
	if err == nil {
		t.Fatal("expected error when neither XDG_CONFIG_HOME nor HOME is set")
	}
}

func TestLoadReturnsNotFoundForMissingFile(t *testing.T) {
	dir := t.TempDir()
	cfg, err := cliconfig.Load(filepath.Join(dir, "absent.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg != (cliconfig.Config{}) {
		t.Fatalf("expected zero config, got %#v", cfg)
	}
}

func TestLoadParsesJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	body := `{"base_url":"http://x","token":"abc","client_name":"laptop"}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := cliconfig.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := cliconfig.Config{BaseURL: "http://x", Token: "abc", ClientName: "laptop"}
	if cfg != want {
		t.Fatalf("Load = %#v, want %#v", cfg, want)
	}
}

func TestLoadRejectsMalformedJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{not json`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := cliconfig.Load(path)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "parse") {
		t.Fatalf("err = %v, want parse error", err)
	}
}

func TestSaveCreatesDirectoryAndWritesJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pdw-cli", "config.json")
	cfg := cliconfig.Config{BaseURL: "https://example", Token: "tok", ClientName: "me"}
	if err := cliconfig.Save(path, cfg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var got cliconfig.Config
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got != cfg {
		t.Fatalf("round-trip = %#v, want %#v", got, cfg)
	}
}

func TestSaveUsesRestrictivePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permissions not meaningful on Windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "pdw-cli", "config.json")
	if err := cliconfig.Save(path, cliconfig.Config{BaseURL: "http://x", Token: "secret", ClientName: "me"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := st.Mode().Perm(); perm != 0o600 {
		t.Fatalf("file mode = %o, want 600", perm)
	}
	dirSt, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if perm := dirSt.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("dir mode = %o, must not be group/other readable", perm)
	}
}

func TestSaveOverwritesExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := cliconfig.Save(path, cliconfig.Config{BaseURL: "http://a", Token: "old", ClientName: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := cliconfig.Save(path, cliconfig.Config{BaseURL: "http://b", Token: "new", ClientName: "y"}); err != nil {
		t.Fatal(err)
	}
	cfg, err := cliconfig.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Token != "new" || cfg.BaseURL != "http://b" || cfg.ClientName != "y" {
		t.Fatalf("after overwrite cfg = %#v", cfg)
	}
}

func TestDeleteRemovesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cliconfig.Delete(path); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("file still present: %v", err)
	}
}

func TestDeleteWhenMissingIsNoOp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := cliconfig.Delete(path); err != nil {
		t.Fatalf("Delete on missing: %v", err)
	}
}

func TestRedactPreservesShapeButHidesToken(t *testing.T) {
	cfg := cliconfig.Config{BaseURL: "http://x", Token: "supersecrettokenvalue", ClientName: "me"}
	red := cfg.Redacted()
	if red.BaseURL != cfg.BaseURL || red.ClientName != cfg.ClientName {
		t.Fatalf("non-secret fields changed: %#v", red)
	}
	if strings.Contains(red.Token, "supersecret") {
		t.Fatalf("token not redacted: %q", red.Token)
	}
	if red.Token == "" {
		t.Fatalf("redacted token should still be non-empty to signal presence")
	}
}

func TestRedactKeepsEmptyTokenEmpty(t *testing.T) {
	cfg := cliconfig.Config{BaseURL: "http://x"}
	if cfg.Redacted().Token != "" {
		t.Fatalf("empty token must remain empty after redaction")
	}
}

func TestLegacyPathPointsAtPDWCLIDir(t *testing.T) {
	home := t.TempDir()
	got, err := cliconfig.LegacyPath(envFromMap(map[string]string{"HOME": home}))
	if err != nil {
		t.Fatalf("LegacyPath: %v", err)
	}
	want := filepath.Join(home, ".config", "pdw-cli", "config.json")
	if got != want {
		t.Fatalf("LegacyPath = %q, want %q", got, want)
	}
}

func TestResolvePrefersCanonicalPath(t *testing.T) {
	home := t.TempDir()
	env := envFromMap(map[string]string{"HOME": home})
	canonical, _ := cliconfig.Path(env)
	legacy, _ := cliconfig.LegacyPath(env)
	if err := cliconfig.Save(canonical, cliconfig.Config{BaseURL: "http://new", Token: "t1", ClientName: "c1"}); err != nil {
		t.Fatal(err)
	}
	if err := cliconfig.Save(legacy, cliconfig.Config{BaseURL: "http://old", Token: "t2", ClientName: "c2"}); err != nil {
		t.Fatal(err)
	}
	cfg, path, err := cliconfig.Resolve(env)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if cfg.BaseURL != "http://new" || path != canonical {
		t.Fatalf("Resolve = %#v from %q, want canonical config", cfg, path)
	}
}

func TestResolveFallsBackToLegacyConfig(t *testing.T) {
	home := t.TempDir()
	env := envFromMap(map[string]string{"HOME": home})
	legacy, _ := cliconfig.LegacyPath(env)
	want := cliconfig.Config{BaseURL: "http://old", Token: "tok", ClientName: "laptop"}
	if err := cliconfig.Save(legacy, want); err != nil {
		t.Fatal(err)
	}
	cfg, path, err := cliconfig.Resolve(env)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if cfg != want {
		t.Fatalf("Resolve = %#v, want %#v (legacy config)", cfg, want)
	}
	if path != legacy {
		t.Fatalf("Resolve path = %q, want legacy %q", path, legacy)
	}
}

func TestResolveReportsMalformedLegacyConfig(t *testing.T) {
	home := t.TempDir()
	env := envFromMap(map[string]string{"HOME": home})
	legacy, _ := cliconfig.LegacyPath(env)
	if err := os.MkdirAll(filepath.Dir(legacy), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacy, []byte(`{not json`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, path, err := cliconfig.Resolve(env)
	if err == nil {
		t.Fatal("expected malformed legacy config to be reported")
	}
	if path != legacy {
		t.Fatalf("Resolve error path = %q, want legacy path %q", path, legacy)
	}
	if !strings.Contains(err.Error(), legacy) {
		t.Fatalf("Resolve error should mention legacy path, got: %v", err)
	}
}

func TestResolveReturnsCanonicalPathWhenNothingExists(t *testing.T) {
	home := t.TempDir()
	env := envFromMap(map[string]string{"HOME": home})
	canonical, _ := cliconfig.Path(env)
	cfg, path, err := cliconfig.Resolve(env)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if cfg != (cliconfig.Config{}) {
		t.Fatalf("expected zero config, got %#v", cfg)
	}
	if path != canonical {
		t.Fatalf("Resolve path = %q, want canonical %q so a future Save lands there", path, canonical)
	}
}

func TestDeleteAllRemovesBothLocations(t *testing.T) {
	home := t.TempDir()
	env := envFromMap(map[string]string{"HOME": home})
	canonical, _ := cliconfig.Path(env)
	legacy, _ := cliconfig.LegacyPath(env)
	if err := cliconfig.Save(canonical, cliconfig.Config{BaseURL: "http://a", Token: "t", ClientName: "c"}); err != nil {
		t.Fatal(err)
	}
	if err := cliconfig.Save(legacy, cliconfig.Config{BaseURL: "http://b", Token: "t", ClientName: "c"}); err != nil {
		t.Fatal(err)
	}
	removed, err := cliconfig.DeleteAll(env)
	if err != nil {
		t.Fatalf("DeleteAll: %v", err)
	}
	if len(removed) != 2 {
		t.Fatalf("DeleteAll removed %v, want both config files", removed)
	}
	for _, p := range []string{canonical, legacy} {
		if _, statErr := os.Stat(p); !os.IsNotExist(statErr) {
			t.Fatalf("config still present at %s: %v", p, statErr)
		}
	}
}

func TestDeleteAllWhenNothingConfiguredIsNoOp(t *testing.T) {
	home := t.TempDir()
	removed, err := cliconfig.DeleteAll(envFromMap(map[string]string{"HOME": home}))
	if err != nil {
		t.Fatalf("DeleteAll: %v", err)
	}
	if len(removed) != 0 {
		t.Fatalf("DeleteAll removed %v, want nothing", removed)
	}
}
