package applemessages

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

func cliEnv(t *testing.T) (map[string]string, string) {
	t.Helper()
	root := t.TempDir()
	store := filepath.Join(root, "Messages", "chat.db")
	if err := os.MkdirAll(filepath.Join(root, "Messages", "Attachments"), 0o755); err != nil {
		t.Fatal(err)
	}
	testfixtures.CopyFile(t, filepath.Join("testdata", "chat.db"), store)
	for _, name := range []string{"photo.txt", "clip.mov"} {
		testfixtures.CopyFile(t, filepath.Join("testdata", "Attachments", name), filepath.Join(root, "Messages", "Attachments", name))
	}
	return map[string]string{
		"APPLE_MESSAGES_ACCOUNT":    "zach@example.com",
		"APPLE_MESSAGES_STORE_PATH": store,
		"PDW_INGEST_PROJECT_DIR":    root,
	}, root
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

func TestRunHelpAndUsageErrors(t *testing.T) {
	code, out, _ := runCLI(t, []string{"-h"}, nil, ingestclient.Config{})
	if code != 0 || !strings.Contains(out, "pdw ingest apple-messages") || !strings.Contains(out, "--workers") {
		t.Fatalf("help: code=%d out=%q", code, out)
	}
	for _, args := range [][]string{{"--mode", "x"}, {"--limit", "-1"}, {"--workers", "-1"}, {"--nope"}} {
		if code, _, errOut := runCLI(t, args, nil, ingestclient.Config{}); code != 2 || errOut == "" {
			t.Fatalf("%v: code=%d stderr=%q", args, code, errOut)
		}
	}
}

func TestRunRejectsABadWorkerCountFromTheEnvironment(t *testing.T) {
	env, root := cliEnv(t)
	env["APPLE_MESSAGES_UPLOAD_WORKERS"] = "0"
	if code, _, errOut := runCLI(t, stateArgs(root), env, ingestclient.Config{BaseURL: "http://x", Token: "t"}); code != 1 || !strings.Contains(errOut, "APPLE_MESSAGES_UPLOAD_WORKERS") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}

func TestRunSkipsWhenTheLockIsHeld(t *testing.T) {
	env, root := cliEnv(t)
	lock, _, err := common.TryRunLock(filepath.Join(root, "state.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	code, out, _ := runCLI(t, stateArgs(root), env, ingestclient.Config{BaseURL: "http://x", Token: "t"})
	if code != 0 || !strings.Contains(out, "Apple Messages upload skipped: another uploader run is active") {
		t.Fatalf("code=%d out=%q", code, out)
	}
}

func TestRunUploadsManifestAndAttachmentsThroughTheApp(t *testing.T) {
	env, root := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	code, out, errOut := runCLI(t, append([]string{"--workers", "2"}, stateArgs(root)...), env, cfg)
	if code != 0 {
		t.Fatalf("code=%d stderr=%q out=%q", code, errOut, out)
	}
	paths := app.Paths()
	var batches, attachments int
	for _, path := range paths {
		switch path {
		case "/ingest/apple-messages/batch":
			batches++
		case "/ingest/apple-messages/attachment":
			attachments++
		default:
			t.Fatalf("unexpected endpoint %s", path)
		}
	}
	if batches != 1 || attachments == 0 {
		t.Fatalf("batches=%d attachments=%d (%v)", batches, attachments, paths)
	}
	if !strings.Contains(out, "Apple Messages upload complete: messages=") || !strings.Contains(out, "batches=1") {
		t.Fatalf("summary missing: %q", out)
	}
	code, out, _ = runCLI(t, stateArgs(root), env, cfg)
	if code != 0 || !strings.Contains(out, "selected=0") {
		t.Fatalf("second run should skip everything: code=%d out=%q", code, out)
	}
}

func TestRunFailsWhenTheStoreIsMissing(t *testing.T) {
	env, root := cliEnv(t)
	env["APPLE_MESSAGES_STORE_PATH"] = filepath.Join(root, "missing", "chat.db")
	app := testfixtures.NewFakeApp(t)
	if code, _, errOut := runCLI(t, stateArgs(root), env, ingestclient.Config{BaseURL: app.URL(), Token: "secret"}); code != 1 || errOut == "" {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}
