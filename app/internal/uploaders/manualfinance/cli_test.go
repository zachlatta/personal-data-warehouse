package manualfinance

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

func cliEnv(t *testing.T) (map[string]string, string, string) {
	t.Helper()
	root := t.TempDir()
	corpus := filepath.Join(root, "accounts")
	folder := filepath.Join(corpus, "acme-checking-1234")
	if err := os.MkdirAll(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "statement.pdf"), []byte("%PDF-1.4 statement"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, ".DS_Store"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	return map[string]string{
		"MANUAL_FINANCE_ACCOUNT": "zach@example.com",
		"PDW_INGEST_PROJECT_DIR": root,
	}, root, corpus
}

func runCLI(t *testing.T, args []string, env map[string]string, cfg ingestclient.Config) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := Run(args, strings.NewReader(""), &out, &errOut, func(k string) string { return env[k] }, cfg)
	return code, out.String(), errOut.String()
}

func stateArgs(root string) []string {
	return []string{"--state-file", filepath.Join(root, "state.sqlite"), "--lock-file", filepath.Join(root, "state.lock")}
}

func TestRunHelpAndUsageErrors(t *testing.T) {
	code, out, _ := runCLI(t, []string{"--help"}, nil, ingestclient.Config{})
	if code != 0 || !strings.Contains(out, "pdw ingest manual-finance") || !strings.Contains(out, "--evidence-only") {
		t.Fatalf("help: code=%d out=%q", code, out)
	}
	for _, args := range [][]string{{}, {"--mode", "x", "/tmp/a"}, {"--limit", "-1", "/tmp/a"}, {"--nope", "/tmp/a"}} {
		if code, _, errOut := runCLI(t, args, nil, ingestclient.Config{}); code != 2 || errOut == "" {
			t.Fatalf("%v: code=%d stderr=%q", args, code, errOut)
		}
	}
}

func TestRunSkipsWhenTheLockIsHeld(t *testing.T) {
	env, root, corpus := cliEnv(t)
	lock, _, err := common.TryRunLock(filepath.Join(root, "state.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	code, out, _ := runCLI(t, append([]string{corpus}, stateArgs(root)...), env, ingestclient.Config{BaseURL: "http://x", Token: "t"})
	if code != 0 || !strings.Contains(out, "Manual finance upload skipped: another uploader run is active") {
		t.Fatalf("code=%d out=%q", code, out)
	}
}

func TestRunUploadsDocumentsWithTheirAccountFolder(t *testing.T) {
	env, root, corpus := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	// Flags after the positional, as argparse allowed.
	code, out, errOut := runCLI(t, append([]string{corpus, "--mode", "incremental"}, stateArgs(root)...), env, cfg)
	if code != 0 {
		t.Fatalf("code=%d stderr=%q out=%q", code, errOut, out)
	}
	calls := app.Calls()
	if len(calls) != 2 || calls[0].Path != "/ingest/manual-finance/file" || calls[1].Path != "/ingest/manual-finance/metadata" {
		t.Fatalf("calls = %v", app.Paths())
	}
	if calls[0].Query["account_folder"] != "acme-checking-1234" {
		t.Fatalf("account folder not preserved: %v", calls[0].Query)
	}
	if !strings.Contains(out, "Manual finance upload complete: seen=1 ignored=1 selected=1 uploaded=1 skipped=0") {
		t.Fatalf("summary missing: %q", out)
	}
	code, out, _ = runCLI(t, append([]string{corpus}, stateArgs(root)...), env, cfg)
	if code != 0 || !strings.Contains(out, "uploaded=0 skipped=1") {
		t.Fatalf("second run should skip: code=%d out=%q", code, out)
	}
}

func TestRunEvidenceOnlyMarksTheProvenance(t *testing.T) {
	env, root, corpus := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	code, _, errOut := runCLI(t, append([]string{"--evidence-only", corpus}, stateArgs(root)...), env, ingestclient.Config{BaseURL: app.URL(), Token: "secret"})
	if code != 0 {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
	metadata := string(app.Calls()[1].Body)
	if !strings.Contains(metadata, `"source":"manual_evidence"`) {
		t.Fatalf("evidence provenance missing: %s", metadata)
	}
}

func TestRunFailsWhenAnUploadFails(t *testing.T) {
	env, root, corpus := cliEnv(t)
	if code, _, errOut := runCLI(t, append([]string{corpus}, stateArgs(root)...), env, ingestclient.Config{BaseURL: "http://127.0.0.1:1", Token: "secret"}); code != 1 || errOut == "" {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}

func TestRunRequiresAccount(t *testing.T) {
	env, root, corpus := cliEnv(t)
	delete(env, "MANUAL_FINANCE_ACCOUNT")
	if code, _, errOut := runCLI(t, append([]string{corpus}, stateArgs(root)...), env, ingestclient.Config{BaseURL: "http://x", Token: "t"}); code != 1 || !strings.Contains(errOut, "MANUAL_FINANCE_ACCOUNT") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}
