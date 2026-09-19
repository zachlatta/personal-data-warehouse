package agentsessions

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/testfixtures"
)

// cliEnv is a self-contained uploader environment: transcripts under a temp
// root, every other tool disabled, state and lock in the temp dir, and no
// project .env.
func cliEnv(t *testing.T) (map[string]string, string) {
	t.Helper()
	root := t.TempDir()
	claude := filepath.Join(root, "claude", "project-a")
	if err := os.MkdirAll(claude, 0o755); err != nil {
		t.Fatal(err)
	}
	transcript := filepath.Join(claude, "11111111-2222-3333-4444-555555555555.jsonl")
	if err := os.WriteFile(transcript, []byte("{\"type\":\"user\",\"n\":1}\n{\"type\":\"assistant\",\"n\":2}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"AGENT_SESSIONS_ACCOUNT":               "zach@example.com",
		"AGENT_SESSIONS_DEVICE":                "test-device",
		"AGENT_SESSIONS_CLAUDE_PROJECTS_DIR":   filepath.Join(root, "claude"),
		"AGENT_SESSIONS_CODEX_SESSIONS_DIR":    filepath.Join(root, "none-codex"),
		"AGENT_SESSIONS_OPENCLAW_SESSIONS_DIR": filepath.Join(root, "none-openclaw"),
		"AGENT_SESSIONS_PI_SESSIONS_DIR":       filepath.Join(root, "none-pi"),
		"PDW_INGEST_PROJECT_DIR":               root,
	}
	return env, root
}

func runCLI(t *testing.T, args []string, env map[string]string, cfg ingestclient.Config) (int, string, string) {
	t.Helper()
	common.OverrideBeforeUploadCheck = func() string { return "" }
	t.Cleanup(func() { common.OverrideBeforeUploadCheck = nil })
	var out, errOut bytes.Buffer
	code := Run(args, strings.NewReader(""), &out, &errOut, func(k string) string { return env[k] }, cfg)
	return code, out.String(), errOut.String()
}

func stateArgs(root string) []string {
	return []string{"--state-file", filepath.Join(root, "state.sqlite"), "--lock-file", filepath.Join(root, "state.lock")}
}

func TestRunHelpPrintsUsageAndExitsZero(t *testing.T) {
	for _, flag := range []string{"-h", "--help"} {
		code, out, _ := runCLI(t, []string{flag}, map[string]string{}, ingestclient.Config{})
		if code != 0 || !strings.Contains(out, "pdw ingest agent-sessions") || !strings.Contains(out, "--limit") {
			t.Fatalf("%s: code=%d out=%q", flag, code, out)
		}
	}
}

func TestRunRejectsBadArgumentsWithArgparseExitCode(t *testing.T) {
	for _, args := range [][]string{{"--mode", "weekly"}, {"--limit", "-1"}, {"--batch-size", "0"}, {"--bogus"}} {
		code, _, errOut := runCLI(t, args, map[string]string{}, ingestclient.Config{})
		if code != 2 || !strings.Contains(errOut, "error") {
			t.Fatalf("%v: code=%d stderr=%q", args, code, errOut)
		}
	}
}

func TestRunRequiresAnAccount(t *testing.T) {
	env, root := cliEnv(t)
	delete(env, "AGENT_SESSIONS_ACCOUNT")
	code, _, errOut := runCLI(t, stateArgs(root), env, ingestclient.Config{BaseURL: "http://x", Token: "t"})
	if code != 1 || !strings.Contains(errOut, "AGENT_SESSIONS_ACCOUNT") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}

func TestRunRequiresWarehouseConfig(t *testing.T) {
	env, root := cliEnv(t)
	code, _, errOut := runCLI(t, stateArgs(root), env, ingestclient.Config{})
	if code != 1 || !strings.Contains(errOut, "PDW_API_URL") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}

func TestRunSkipsWhenTheLockIsHeld(t *testing.T) {
	env, root := cliEnv(t)
	lock, acquired, err := common.TryRunLock(filepath.Join(root, "state.lock"))
	if err != nil || !acquired {
		t.Fatalf("lock: %v %v", acquired, err)
	}
	defer lock.Release()
	app := testfixtures.NewFakeApp(t)
	code, out, _ := runCLI(t, stateArgs(root), env, ingestclient.Config{BaseURL: app.URL(), Token: "secret"})
	if code != 0 || !strings.Contains(out, "another uploader run is active") {
		t.Fatalf("code=%d out=%q", code, out)
	}
	if len(app.Calls()) != 0 {
		t.Fatal("a skipped run must not upload")
	}
}

func TestRunUploadsThroughTheAppAndResumesFromState(t *testing.T) {
	env, root := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	code, out, errOut := runCLI(t, append([]string{"--mode", "incremental"}, stateArgs(root)...), env, cfg)
	if code != 0 {
		t.Fatalf("code=%d stderr=%q out=%q", code, errOut, out)
	}
	if !strings.Contains(out, "Agent sessions upload complete: files=1 new=1 lines=2 skipped=0 batches=1") {
		t.Fatalf("summary line missing: %q", out)
	}
	if paths := app.Paths(); len(paths) != 1 || paths[0] != "/ingest/agent-sessions/batch" {
		t.Fatalf("app calls = %v", paths)
	}
	records := testfixtures.DecodeGzipJSONL(app.Calls()[0].Body)
	if len(records) != 2 || records[0]["account"] != "zach@example.com" || records[0]["device"] != "test-device" {
		t.Fatalf("records = %v", records)
	}
	code, out, _ = runCLI(t, stateArgs(root), env, cfg)
	if code != 0 || !strings.Contains(out, "lines=0 skipped=0 batches=0") {
		t.Fatalf("second run should find nothing new: code=%d out=%q", code, out)
	}
}

func TestRunHonorsLimitAndDefers(t *testing.T) {
	env, root := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	code, out, _ := runCLI(t, append([]string{"--limit", "1"}, stateArgs(root)...), env, ingestclient.Config{BaseURL: app.URL(), Token: "secret"})
	if code != 0 || !strings.Contains(out, "lines=1") || !strings.Contains(out, "run limit reached") {
		t.Fatalf("code=%d out=%q", code, out)
	}
}

func TestRunReportsABlockedNetworkAsADeliberateSkip(t *testing.T) {
	env, root := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	var out, errOut bytes.Buffer
	common.OverrideBeforeUploadCheck = func() string { return "blocked hardware port: iPhone USB" }
	defer func() { common.OverrideBeforeUploadCheck = nil }()
	code := Run(stateArgs(root), strings.NewReader(""), &out, &errOut, func(k string) string { return env[k] }, ingestclient.Config{BaseURL: app.URL(), Token: "secret"})
	if code != 0 || !strings.Contains(out.String(), "Agent sessions upload skipped: blocked hardware port: iPhone USB") {
		t.Fatalf("code=%d out=%q", code, out.String())
	}
	if len(app.Calls()) != 0 {
		t.Fatal("a blocked run must not upload")
	}
}

func TestRunFailsLoudlyWhenTheAppRefuses(t *testing.T) {
	env, root := cliEnv(t)
	code, _, errOut := runCLI(t, stateArgs(root), env, ingestclient.Config{BaseURL: "http://127.0.0.1:1", Token: "secret"})
	if code != 1 || errOut == "" {
		t.Fatalf("a mid-run transport failure must exit non-zero: code=%d stderr=%q", code, errOut)
	}
}

func TestRunLoadsTheProjectDotenv(t *testing.T) {
	env, root := cliEnv(t)
	delete(env, "AGENT_SESSIONS_ACCOUNT")
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte("AGENT_SESSIONS_ACCOUNT=dotenv@example.com\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	app := testfixtures.NewFakeApp(t)
	code, out, errOut := runCLI(t, stateArgs(root), env, ingestclient.Config{BaseURL: app.URL(), Token: "secret"})
	if code != 0 {
		t.Fatalf("code=%d stderr=%q out=%q", code, errOut, out)
	}
	records := testfixtures.DecodeGzipJSONL(app.Calls()[0].Body)
	if records[0]["account"] != "dotenv@example.com" {
		t.Fatalf("account should come from the project .env: %v", records[0]["account"])
	}
}
