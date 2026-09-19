package applecontacts

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
	if err := os.MkdirAll(filepath.Join(root, "book", "Sources", "src"), 0o755); err != nil {
		t.Fatal(err)
	}
	testfixtures.CopyFile(t, filepath.Join("testdata", "synthetic.abcddb"), filepath.Join(root, "book", "Sources", "src", StoreFilename))
	return map[string]string{
		"APPLE_CONTACTS_ACCOUNT":    "zach@example.com",
		"APPLE_CONTACTS_STORE_PATH": filepath.Join(root, "book"),
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
	code, out, _ := runCLI(t, []string{"--help"}, nil, ingestclient.Config{})
	if code != 0 || !strings.Contains(out, "pdw ingest apple-contacts") || !strings.Contains(out, "--mutations-only") {
		t.Fatalf("help: code=%d out=%q", code, out)
	}
	for _, args := range [][]string{{"--mode", "x"}, {"--limit", "-2"}, {"--nope"}} {
		if code, _, errOut := runCLI(t, args, nil, ingestclient.Config{}); code != 2 || errOut == "" {
			t.Fatalf("%v: code=%d stderr=%q", args, code, errOut)
		}
	}
}

func TestRunRequiresAccount(t *testing.T) {
	env, root := cliEnv(t)
	delete(env, "APPLE_CONTACTS_ACCOUNT")
	if code, _, errOut := runCLI(t, stateArgs(root), env, ingestclient.Config{BaseURL: "http://x", Token: "t"}); code != 1 || !strings.Contains(errOut, "APPLE_CONTACTS_ACCOUNT") {
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
	if code != 0 || !strings.Contains(out, "Apple Contacts upload skipped: another uploader run is active") {
		t.Fatalf("code=%d out=%q", code, out)
	}
}

func TestRunUploadsCardsThroughTheApp(t *testing.T) {
	env, root := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	code, out, errOut := runCLI(t, stateArgs(root), env, cfg)
	if code != 0 {
		t.Fatalf("code=%d stderr=%q out=%q", code, errOut, out)
	}
	if paths := app.Paths(); len(paths) != 1 || paths[0] != "/ingest/apple-contacts/batch" {
		t.Fatalf("app calls = %v", paths)
	}
	if !strings.Contains(out, "Apple Contacts upload complete: seen=") || !strings.Contains(out, "batches=1") {
		t.Fatalf("summary missing: %q", out)
	}
	records := testfixtures.DecodeGzipJSONL(app.Calls()[0].Body)
	if len(records) == 0 || records[0]["account"] != "zach@example.com" {
		t.Fatalf("records = %v", records)
	}
	code, out, _ = runCLI(t, stateArgs(root), env, cfg)
	if code != 0 || !strings.Contains(out, "selected=0") || !strings.Contains(out, "batches=0") {
		t.Fatalf("second run should skip every card: code=%d out=%q", code, out)
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
	if code != 0 || !strings.Contains(out, "Apple Contacts mutations skipped: warehouse unavailable") {
		t.Fatalf("code=%d out=%q", code, out)
	}
	if len(app.Calls()) != 0 {
		t.Fatal("--mutations-only must not upload")
	}
	env["APPLE_CONTACTS_MUTATIONS_ENABLED"] = "0"
	code, out, _ = runCLI(t, append([]string{"--mutations-only"}, stateArgs(root)...), env, ingestclient.Config{BaseURL: app.URL(), Token: "secret"})
	if code != 0 || strings.Contains(out, "mutations") {
		t.Fatalf("disabled mutations must print nothing: code=%d out=%q", code, out)
	}
}

func TestRunAppliesMutationsBeforeTheUploadAndNoMutationsSkipsTheStage(t *testing.T) {
	env, root := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	code, out, _ := runCLI(t, stateArgs(root), env, ingestclient.Config{BaseURL: app.URL(), Token: "secret"})
	if code != 0 || len(app.Calls()) != 1 {
		t.Fatalf("code=%d out=%q calls=%d", code, out, len(app.Calls()))
	}
	stage, upload := strings.Index(out, "Apple Contacts mutations skipped: warehouse unavailable"), strings.Index(out, "Apple Contacts upload complete")
	if stage < 0 || upload < 0 || stage > upload {
		t.Fatalf("mutations must run before the upload: %q", out)
	}
	env2, root2 := cliEnv(t)
	code, out, _ = runCLI(t, append([]string{"--no-mutations"}, stateArgs(root2)...), env2, ingestclient.Config{BaseURL: app.URL(), Token: "secret"})
	if code != 0 || strings.Contains(out, "mutations") || len(app.Calls()) != 2 {
		t.Fatalf("--no-mutations: code=%d out=%q calls=%d", code, out, len(app.Calls()))
	}
}
