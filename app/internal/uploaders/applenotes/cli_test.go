package applenotes

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
	store := filepath.Join(root, "notes", "NoteStore.sqlite")
	if err := os.MkdirAll(filepath.Join(root, "notes", "files"), 0o755); err != nil {
		t.Fatal(err)
	}
	testfixtures.CopyFile(t, filepath.Join("testdata", "synthetic", "NoteStore.sqlite"), store)
	testfixtures.CopyFile(t, filepath.Join("testdata", "synthetic", "files", "photo.txt"), filepath.Join(root, "notes", "files", "photo.txt"))
	return map[string]string{
		"APPLE_NOTES_ACCOUNT":        "zach@example.com",
		"APPLE_NOTES_STORE_PATH":     store,
		"APPLE_NOTES_OPEN_NOTES_APP": "0",
		"PDW_INGEST_PROJECT_DIR":     root,
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
	return []string{"--state-file", filepath.Join(root, "state.json"), "--lock-file", filepath.Join(root, "state.lock")}
}

func TestRunHelpAndUsageErrors(t *testing.T) {
	code, out, _ := runCLI(t, []string{"--help"}, nil, ingestclient.Config{})
	if code != 0 || !strings.Contains(out, "pdw ingest apple-notes") || !strings.Contains(out, "--mutations-only") {
		t.Fatalf("help: code=%d out=%q", code, out)
	}
	for _, args := range [][]string{{"--mode", "x"}, {"--limit", "-1"}, {"--workers", "-1"}, {"--nope"}} {
		if code, _, errOut := runCLI(t, args, nil, ingestclient.Config{}); code != 2 || errOut == "" {
			t.Fatalf("%v: code=%d stderr=%q", args, code, errOut)
		}
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
	if code != 0 || !strings.Contains(out, "Apple Notes upload skipped: another uploader run is active") {
		t.Fatalf("code=%d out=%q", code, out)
	}
}

func TestRunUploadsRevisionsAndPersistsState(t *testing.T) {
	env, root := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	code, out, errOut := runCLI(t, stateArgs(root), env, cfg)
	if code != 0 {
		t.Fatalf("code=%d stderr=%q out=%q", code, errOut, out)
	}
	seen := map[string]int{}
	for _, path := range app.Paths() {
		seen[path]++
	}
	if seen["/ingest/apple-notes/body"] == 0 || seen["/ingest/apple-notes/revision"] == 0 {
		t.Fatalf("expected body and revision uploads, got %v", seen)
	}
	if !strings.Contains(out, "Apple Notes upload complete: seen=") || strings.Contains(out, "selected=0") {
		t.Fatalf("summary missing: %q", out)
	}
	if _, err := os.Stat(filepath.Join(root, "state.json")); err != nil {
		t.Fatalf("state file not written: %v", err)
	}
	code, out, _ = runCLI(t, stateArgs(root), env, cfg)
	if code != 0 || !strings.Contains(out, "selected=0") || !strings.Contains(out, "revisions=0") {
		t.Fatalf("second run should skip every note: code=%d out=%q", code, out)
	}
}

// The mutation stage runs BEFORE the scan and never fails the run. With no
// POSTGRES_DATABASE_URL in the injected environment the stage reports the
// warehouse as unavailable, which is exactly what the Python uploader printed
// on a Mac without warehouse access, and the run continues.
func TestRunMutationsOnlyAppliesMutationsAndSkipsTheUpload(t *testing.T) {
	env, root := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	code, out, _ := runCLI(t, append([]string{"--mutations-only"}, stateArgs(root)...), env, ingestclient.Config{BaseURL: app.URL(), Token: "secret"})
	if code != 0 || !strings.Contains(out, "Apple Notes mutations skipped: warehouse unavailable") || len(app.Calls()) != 0 {
		t.Fatalf("code=%d out=%q calls=%d", code, out, len(app.Calls()))
	}
	if strings.Contains(out, "upload complete") {
		t.Fatalf("--mutations-only must not upload: %q", out)
	}
	env["APPLE_NOTES_MUTATIONS_ENABLED"] = "0"
	code, out, _ = runCLI(t, append([]string{"--mutations-only"}, stateArgs(root)...), env, ingestclient.Config{BaseURL: app.URL(), Token: "secret"})
	if code != 0 || strings.Contains(out, "mutations") {
		t.Fatalf("disabled mutations must print nothing: code=%d out=%q", code, out)
	}
}

func TestRunAppliesMutationsBeforeTheUploadAndNoMutationsSkipsTheStage(t *testing.T) {
	env, root := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	code, out, _ := runCLI(t, stateArgs(root), env, ingestclient.Config{BaseURL: app.URL(), Token: "secret"})
	if code != 0 || len(app.Calls()) == 0 {
		t.Fatalf("code=%d out=%q calls=%d", code, out, len(app.Calls()))
	}
	stage, upload := strings.Index(out, "Apple Notes mutations skipped: warehouse unavailable"), strings.Index(out, "Apple Notes upload complete")
	if stage < 0 || upload < 0 || stage > upload {
		t.Fatalf("mutations must run before the upload: %q", out)
	}
	env2, root2 := cliEnv(t)
	code, out, _ = runCLI(t, append([]string{"--no-mutations"}, stateArgs(root2)...), env2, ingestclient.Config{BaseURL: app.URL(), Token: "secret"})
	if code != 0 || strings.Contains(out, "mutations") || !strings.Contains(out, "upload complete") {
		t.Fatalf("--no-mutations: code=%d out=%q", code, out)
	}
}

func TestRunRequiresAccount(t *testing.T) {
	env, root := cliEnv(t)
	delete(env, "APPLE_NOTES_ACCOUNT")
	if code, _, errOut := runCLI(t, stateArgs(root), env, ingestclient.Config{BaseURL: "http://x", Token: "t"}); code != 1 || !strings.Contains(errOut, "APPLE_NOTES_ACCOUNT") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}
