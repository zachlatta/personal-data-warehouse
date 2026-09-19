package common

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseInterleavedAcceptsFlagsAfterPositionals(t *testing.T) {
	fs := NewFlagSet("x")
	evidence := fs.Bool("evidence-only", false, "")
	limit := fs.Int("limit", 0, "")
	positionals, err := ParseInterleaved(fs, []string{"a", "--evidence-only", "b", "--limit", "3", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(positionals, ",") != "a,b,c" || !*evidence || *limit != 3 {
		t.Fatalf("positionals=%v evidence=%v limit=%d", positionals, *evidence, *limit)
	}
}

func TestParseInterleavedStopsAtDoubleDash(t *testing.T) {
	fs := NewFlagSet("x")
	fs.Bool("flag", false, "")
	positionals, err := ParseInterleaved(fs, []string{"a", "--", "--flag", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(positionals, ",") != "a,--flag,b" {
		t.Fatalf("positionals=%v", positionals)
	}
}

func TestParseArgsExitCodes(t *testing.T) {
	var out, errOut bytes.Buffer
	fs := NewFlagSet("pdw ingest x")
	fs.Int("limit", 0, "")
	if _, code, done := ParseArgs(fs, []string{"--help"}, "USAGE\n", &out, &errOut); !done || code != 0 || out.String() != "USAGE\n" {
		t.Fatalf("help: done=%v code=%d out=%q", done, code, out.String())
	}
	out.Reset()
	fs = NewFlagSet("pdw ingest x")
	if _, code, done := ParseArgs(fs, []string{"--bogus"}, "USAGE\n", &out, &errOut); !done || code != ExitUsage {
		t.Fatalf("bad flag: done=%v code=%d", done, code)
	}
	if strings.Count(errOut.String(), "bogus") != 1 {
		t.Fatalf("the flag error should be reported exactly once: %q", errOut.String())
	}
	fs = NewFlagSet("pdw ingest x")
	if _, code, done := ParseArgs(fs, nil, "", &out, &errOut); done || code != 0 {
		t.Fatalf("clean parse: done=%v code=%d", done, code)
	}
}

func TestValidateMode(t *testing.T) {
	if err := ValidateMode("incremental"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateMode("full"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateMode("weekly"); err == nil {
		t.Fatal("weekly should be rejected")
	}
}

func TestProjectDirPrefersPDWIngestProjectDir(t *testing.T) {
	env := map[string]string{"PDW_INGEST_PROJECT_DIR": "/repo"}
	if got := ProjectDir(func(k string) string { return env[k] }); got != "/repo" {
		t.Fatalf("ProjectDir = %q", got)
	}
	cwd, _ := os.Getwd()
	if got := ProjectDir(func(string) string { return "" }); got != cwd {
		t.Fatalf("default ProjectDir = %q, want cwd %q", got, cwd)
	}
}

func TestLoadProjectDotenvLayersBeneathTheEnvironment(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("PDW_CLI_TEST_DOTENV_A=from-file\nPDW_CLI_TEST_DOTENV_B=from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PDW_CLI_TEST_DOTENV_B", "from-env")
	env := map[string]string{"PDW_INGEST_PROJECT_DIR": dir, "PDW_CLI_TEST_DOTENV_B": "from-env"}
	getenv, err := LoadProjectDotenv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if got := getenv("PDW_CLI_TEST_DOTENV_A"); got != "from-file" {
		t.Fatalf("A = %q", got)
	}
	if got := getenv("PDW_CLI_TEST_DOTENV_B"); got != "from-env" {
		t.Fatalf("the environment must win over .env, got %q", got)
	}
	if got := os.Getenv("PDW_CLI_TEST_DOTENV_A"); got != "from-file" {
		t.Fatalf("the file's values must also reach the process environment, got %q", got)
	}
	os.Unsetenv("PDW_CLI_TEST_DOTENV_A")
}

func TestLoadProjectDotenvWithoutAFileIsNotAnError(t *testing.T) {
	env := map[string]string{"PDW_INGEST_PROJECT_DIR": t.TempDir()}
	if _, err := LoadProjectDotenv(func(k string) string { return env[k] }); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireRunLockSkipsWhenHeld(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.lock")
	var out, errOut bytes.Buffer
	lock, code, done := AcquireRunLock(path, "skipped", &out, &errOut)
	if done || code != 0 || lock == nil {
		t.Fatalf("first acquire: done=%v code=%d", done, code)
	}
	defer lock.Release()
	second, code, done := AcquireRunLock(path, "skipped: another run", &out, &errOut)
	if !done || code != 0 || second != nil {
		t.Fatalf("second acquire: done=%v code=%d lock=%v", done, code, second)
	}
	if !strings.Contains(out.String(), "skipped: another run") {
		t.Fatalf("skip message missing: %q", out.String())
	}
}

func TestUploadGuardHonorsTheTestOverride(t *testing.T) {
	OverrideBeforeUploadCheck = func() string { return "override" }
	defer func() { OverrideBeforeUploadCheck = nil }()
	if got := UploadGuard(func(string) string { return "" }, "X", "http://x")(); got != "override" {
		t.Fatalf("guard = %q", got)
	}
}
