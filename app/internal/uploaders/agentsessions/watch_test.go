package agentsessions

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/testfixtures"
)

// A resident watcher passes every couple of seconds over thousands of
// transcripts; only the ones that grew may cost a state lookup and a read.
func TestRunnerSkipsTranscriptsWhoseSizeAndMtimeDidNotMove(t *testing.T) {
	root := t.TempDir()
	transcript := filepath.Join(root, "6f1a2b3c-1111-2222-3333-444444444444.jsonl")
	os.WriteFile(transcript, []byte("{\"a\":1}\n"), 0o644)
	os.WriteFile(filepath.Join(root, "idle.jsonl"), []byte("{\"a\":0}\n"), 0o644)
	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	uploads := &fakeUploads{}
	runner := newRunner(t, root, uploads, state)
	runner.SkipUnchanged = true

	if summary, err := runner.Sync(); err != nil || summary.LinesSelected != 2 || summary.FilesUnchanged != 0 {
		t.Fatalf("first pass = %+v, %v", summary, err)
	}
	summary, err := runner.Sync()
	if err != nil || summary.FilesUnchanged != 2 || summary.LinesSelected != 0 || len(uploads.batches) != 1 {
		t.Fatalf("an idle pass must skip both files: %+v, %v, batches=%d", summary, err, len(uploads.batches))
	}

	f, _ := os.OpenFile(transcript, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString("{\"a\":2}\n")
	f.Close()
	summary, err = runner.Sync()
	if err != nil || summary.LinesSelected != 1 || summary.FilesUnchanged != 1 {
		t.Fatalf("the grown file must be read, the idle one skipped: %+v, %v", summary, err)
	}
	if got := uploads.batches[1][0]["record"].(map[string]any)["seq"].(float64); got != 1 {
		t.Fatalf("appended line seq = %v", got)
	}
}

func TestRunnerRereadsAFileWhoseUploadFailed(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "a.jsonl"), []byte("{\"a\":1}\n"), 0o644)
	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	uploads := &fakeUploads{}
	fail := true
	runner := newRunner(t, root, uploads, state)
	runner.SkipUnchanged = true
	runner.Upload = func(gz []byte, at time.Time) (ingestclient.StoredObject, error) {
		if fail {
			return ingestclient.StoredObject{}, errors.New("app unreachable")
		}
		return uploads.upload(gz, at)
	}
	if _, err := runner.Sync(); err == nil {
		t.Fatal("the failed upload must surface")
	}
	fail = false
	summary, err := runner.Sync()
	if err != nil || summary.LinesSelected != 1 || len(uploads.batches) != 1 {
		t.Fatalf("a file whose lines never uploaded must not be remembered as unchanged: %+v, %v", summary, err)
	}
}

func TestRunnerRereadsAFileItDeferredAtTheLimit(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "a.jsonl"), []byte("{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n"), 0o644)
	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	uploads := &fakeUploads{}
	runner := newRunner(t, root, uploads, state)
	runner.SkipUnchanged = true
	runner.Limit = 2
	runner.Sync()
	summary, err := runner.Sync()
	if err != nil || summary.LinesSelected != 1 {
		t.Fatalf("the deferred line must ship on the next pass: %+v, %v", summary, err)
	}
}

func TestRunnerSkipsAnUnchangedOpenClawStore(t *testing.T) {
	db, dirs := newOpenClawStore(t, t.TempDir())
	addEvent(t, db, "sess-1", 0, `{"type":"message","n":0}`)
	addEvent(t, db, "sess-1", 1, `{"type":"message","n":1}`)
	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	runner := storeRunner(t, dirs, &fakeUploads{}, state)
	runner.SkipUnchanged = true
	first, err := runner.Sync()
	if err != nil || first.LinesSelected != 2 {
		t.Fatalf("first pass = %+v, %v", first, err)
	}
	second, err := runner.Sync()
	if err != nil || second.LinesSelected != 0 || second.FilesUnchanged != 1 {
		t.Fatalf("an untouched store must be skipped whole: %+v, %v", second, err)
	}
	addEvent(t, db, "sess-1", 2, `{"type":"message","n":2}`)
	third, err := runner.Sync()
	if err != nil || third.LinesSelected != 1 {
		t.Fatalf("a store that changed must be read: %+v, %v", third, err)
	}
}

func TestRunnerWatchQuietWhenIdle(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "a.jsonl"), []byte("{\"a\":1}\n"), 0o644)
	logger := &common.RecordingLogger{}
	runner := newRunner(t, root, &fakeUploads{}, nil)
	runner.Logger = logger
	runner.SkipUnchanged = true
	runner.Sync()
	before := len(logger.Infos)
	runner.Sync()
	if len(logger.Infos) != before {
		t.Fatalf("an idle watch pass must not log: %v", logger.Infos[before:])
	}
}

func TestRunWatchShipsLinesAppendedDuringTheWindowThenExits(t *testing.T) {
	env, root := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	transcript := filepath.Join(root, "claude", "project-a", "11111111-2222-3333-4444-555555555555.jsonl")
	go func() {
		for len(app.Calls()) == 0 {
			time.Sleep(10 * time.Millisecond)
		}
		f, _ := os.OpenFile(transcript, os.O_APPEND|os.O_WRONLY, 0o644)
		f.WriteString("{\"type\":\"user\",\"n\":3}\n")
		f.Close()
	}()
	args := append([]string{"--watch", "1s", "--interval", "20ms"}, stateArgs(root)...)
	started := time.Now()
	code, out, errOut := runCLI(t, args, env, cfg)
	if code != 0 {
		t.Fatalf("code=%d stderr=%q out=%q", code, errOut, out)
	}
	if elapsed := time.Since(started); elapsed < 900*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("the watch window must run for --watch, ran %s", elapsed)
	}
	if calls := app.Calls(); len(calls) != 2 {
		t.Fatalf("want the initial batch and the appended line, got %d uploads", len(calls))
	}
	if !strings.Contains(out, "Agent sessions watch complete:") || !strings.Contains(out, "lines=3") {
		t.Fatalf("watch summary missing: %q", out)
	}
}

func TestRunWatchFailsLoudlyWhenTheAppRefuses(t *testing.T) {
	env, root := cliEnv(t)
	args := append([]string{"--watch", "10m", "--interval", "20ms"}, stateArgs(root)...)
	started := time.Now()
	code, _, errOut := runCLI(t, args, env, ingestclient.Config{BaseURL: "http://127.0.0.1:1", Token: "secret"})
	if code != 1 || errOut == "" {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
	// The ingest client's own retries take seconds; the window is minutes.
	if time.Since(started) > time.Minute {
		t.Fatal("a failed pass must end the window so the wrapper records the failure")
	}
}

func TestRunWatchRejectsANonPositiveInterval(t *testing.T) {
	env, root := cliEnv(t)
	code, _, _ := runCLI(t, append([]string{"--watch", "1s", "--interval", "0s"}, stateArgs(root)...), env, ingestclient.Config{BaseURL: "http://x", Token: "t"})
	if code != 2 {
		t.Fatalf("code=%d", code)
	}
}
