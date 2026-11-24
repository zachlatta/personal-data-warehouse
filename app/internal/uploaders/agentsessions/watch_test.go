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

func TestRunWatchRejectsANonPositiveMaxInterval(t *testing.T) {
	env, root := cliEnv(t)
	code, _, _ := runCLI(t, append([]string{"--watch", "1s", "--max-interval", "0s"}, stateArgs(root)...), env, ingestclient.Config{BaseURL: "http://x", Token: "t"})
	if code != 2 {
		t.Fatalf("code=%d", code)
	}
}

func TestRunnerReuploadsTruncatedFile(t *testing.T) {
	root := t.TempDir()
	transcript := filepath.Join(root, "trunc.jsonl")
	os.WriteFile(transcript, []byte("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n"), 0o644)
	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	uploads := &fakeUploads{}
	runner := newRunner(t, root, uploads, state)
	runner.SkipUnchanged = true

	if s, err := runner.Sync(); err != nil || s.LinesSelected != 3 {
		t.Fatalf("first = %+v, %v", s, err)
	}
	os.WriteFile(transcript, []byte("{\"a\":9}\n"), 0o644)
	s, err := runner.Sync()
	if err != nil || s.LinesSelected != 1 || len(uploads.batches) != 2 {
		t.Fatalf("truncated file must be re-read from start: %+v, %v, batches=%d", s, err, len(uploads.batches))
	}
	if got := uploads.batches[1][0]["record"].(map[string]any)["line"].(map[string]any)["a"].(float64); got != 9 {
		t.Fatalf("got line a=%v, want 9", got)
	}
}

func TestRunnerReuploadsSameSizeRenamedReplacementEvenWithIdenticalMtime(t *testing.T) {
	root := t.TempDir()
	transcript := filepath.Join(root, "replaced.jsonl")
	origTime := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	os.WriteFile(transcript, []byte("{\"a\":1}\n"), 0o644)
	os.Chtimes(transcript, origTime, origTime)

	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	uploads := &fakeUploads{}
	runner := newRunner(t, root, uploads, state)
	runner.SkipUnchanged = true

	if s, err := runner.Sync(); err != nil || s.LinesSelected != 1 {
		t.Fatalf("first = %+v, %v", s, err)
	}

	// Atomically replace with a different file of the EXACT same size and mtime.
	tmp := filepath.Join(root, "tmp.new")
	os.WriteFile(tmp, []byte("{\"a\":2}\n"), 0o644)
	os.Chtimes(tmp, origTime, origTime)
	if err := os.Rename(tmp, transcript); err != nil {
		t.Fatal(err)
	}

	s, err := runner.Sync()
	if err != nil || s.LinesSelected != 1 || len(uploads.batches) != 2 {
		t.Fatalf("same-size renamed replacement must be uploaded: %+v, %v, batches=%d", s, err, len(uploads.batches))
	}
	if got := uploads.batches[1][0]["record"].(map[string]any)["line"].(map[string]any)["a"].(float64); got != 2 {
		t.Fatalf("got line a=%v, want 2", got)
	}
}

func TestRunnerReuploadsSameSizeInPlaceRewrite(t *testing.T) {
	root := t.TempDir()
	transcript := filepath.Join(root, "rewrite.jsonl")
	t1 := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(5 * time.Second)
	os.WriteFile(transcript, []byte("{\"a\":1}\n"), 0o644)
	os.Chtimes(transcript, t1, t1)

	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	uploads := &fakeUploads{}
	runner := newRunner(t, root, uploads, state)
	runner.SkipUnchanged = true

	if s, err := runner.Sync(); err != nil || s.LinesSelected != 1 {
		t.Fatalf("first = %+v, %v", s, err)
	}

	// Rewrite in place (same inode, same size, newer mtime).
	f, err := os.OpenFile(transcript, os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("{\"a\":7}\n")
	f.Close()
	os.Chtimes(transcript, t2, t2)

	s, err := runner.Sync()
	if err != nil || s.LinesSelected != 1 || len(uploads.batches) != 2 {
		t.Fatalf("same-size in-place rewrite must be uploaded: %+v, %v, batches=%d", s, err, len(uploads.batches))
	}
	if got := uploads.batches[1][0]["record"].(map[string]any)["line"].(map[string]any)["a"].(float64); got != 7 {
		t.Fatalf("got line a=%v, want 7", got)
	}
}

func TestRunnerMtimeSettleWindowPreventsRacySkip(t *testing.T) {
	root := t.TempDir()
	transcript := filepath.Join(root, "recent.jsonl")
	now := time.Date(2026, 5, 21, 12, 0, 10, 0, time.UTC)
	os.WriteFile(transcript, []byte("{\"a\":1}\n"), 0o644)
	// File mtime is only 500ms old — inside the 2s settle window.
	os.Chtimes(transcript, now.Add(-500*time.Millisecond), now.Add(-500*time.Millisecond))

	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	uploads := &fakeUploads{}
	runner := newRunner(t, root, uploads, state)
	runner.SkipUnchanged = true
	runner.MtimeSettleWindow = 2 * time.Second
	runner.Now = func() time.Time { return now }

	if s, err := runner.Sync(); err != nil || s.LinesSelected != 1 {
		t.Fatalf("first = %+v, %v", s, err)
	}
	// Still inside settle window: must not be skipped yet.
	s2, err := runner.Sync()
	if err != nil || s2.FilesUnchanged != 0 {
		t.Fatalf("unsettled file must not be marked unchanged: %+v, %v", s2, err)
	}
	// Advance clock past settle window: pass 3 marks it caught up, pass 4 skips it.
	now = now.Add(5 * time.Second)
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	s4, err := runner.Sync()
	if err != nil || s4.FilesUnchanged != 1 {
		t.Fatalf("settled file must be skipped: %+v, %v", s4, err)
	}
}

func TestRunnerSkipsUnchangedFilesAcrossRestartsUsingPersistedState(t *testing.T) {
	root := t.TempDir()
	transcript := filepath.Join(root, "persist.jsonl")
	oldTime := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	os.WriteFile(transcript, []byte("{\"a\":1}\n"), 0o644)
	os.Chtimes(transcript, oldTime, oldTime)

	statePath := filepath.Join(t.TempDir(), "s.sqlite")
	state1, _ := OpenState(statePath, "z@x")
	uploads := &fakeUploads{}
	r1 := newRunner(t, root, uploads, state1)
	r1.SkipUnchanged = true
	if s, err := r1.Sync(); err != nil || s.LinesSelected != 1 {
		t.Fatalf("run 1 = %+v, %v", s, err)
	}
	state1.Close()

	// Simulate a 5-minute restart: fresh State and fresh Runner.
	state2, _ := OpenState(statePath, "z@x")
	defer state2.Close()
	r2 := newRunner(t, root, uploads, state2)
	r2.SkipUnchanged = true
	s2, err := r2.Sync()
	if err != nil || s2.FilesUnchanged != 1 || s2.LinesSelected != 0 {
		t.Fatalf("fresh Runner after restart must skip unchanged file on pass 1: %+v, %v", s2, err)
	}

	// Now append while running; must still be picked up.
	f, _ := os.OpenFile(transcript, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString("{\"a\":2}\n")
	f.Close()
	s3, err := r2.Sync()
	if err != nil || s3.LinesSelected != 1 || len(uploads.batches) != 2 {
		t.Fatalf("appended file after restart must upload: %+v, %v", s3, err)
	}
}

func TestRunWatchBacksOffWhenIdleAndResetsOnActivity(t *testing.T) {
	env, root := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	transcript := filepath.Join(root, "claude", "project-a", "11111111-2222-3333-4444-555555555555.jsonl")
	// Backdate the transcript past idle-after BEFORE seeding so the committed
	// signature matches the old mtime and pass 1 starts genuinely idle (like
	// porygon after a 5-minute restart).
	oldTime := time.Now().Add(-20 * time.Minute)
	if err := os.Chtimes(transcript, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	if code, _, errOut := runCLI(t, stateArgs(root), env, cfg); code != 0 {
		t.Fatalf("seed run failed: code=%d stderr=%q", code, errOut)
	}

	// Idle watch for 600ms with 20ms interval and 250ms max-interval:
	// already idle past 10m so it backs off from pass 1 (<= 5 passes in 600ms).
	args := append([]string{"--watch", "600ms", "--interval", "20ms", "--max-interval", "250ms"}, stateArgs(root)...)
	started := time.Now()
	code, out, errOut := runCLI(t, args, env, cfg)
	elapsed := time.Since(started)
	if code != 0 {
		t.Fatalf("code=%d stderr=%q out=%q", code, errOut, out)
	}
	if elapsed < 500*time.Millisecond {
		t.Fatalf("idle watch must not exit early, ran %s", elapsed)
	}
	var passes int
	for _, field := range strings.Fields(out) {
		if strings.HasPrefix(field, "passes=") {
			for _, ch := range strings.TrimPrefix(field, "passes=") {
				passes = passes*10 + int(ch-'0')
			}
		}
	}
	if passes == 0 || passes > 5 {
		t.Fatalf("idle watch must back off from pass 1: got passes=%d in %s (out=%q)", passes, elapsed, out)
	}
}

// M4: verify backoff resets to base interval on activity; fails if reset is removed.
func TestRunWatchResetsToBaseIntervalAfterBackoffOnActivity(t *testing.T) {
	env, root := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	transcript := filepath.Join(root, "claude", "project-a", "11111111-2222-3333-4444-555555555555.jsonl")
	oldTime := time.Now().Add(-20 * time.Minute)
	if err := os.Chtimes(transcript, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	if code, _, errOut := runCLI(t, stateArgs(root), env, cfg); code != 0 {
		t.Fatalf("seed run failed: code=%d stderr=%q", code, errOut)
	}

	// max-interval=450ms, interval=20ms: pass 1 sleeps 450ms (backed off).
	// Append line 1 while backed off; once it lands, immediately append line 2
	// and assert line 2 lands within 150ms (only possible if interval reset to 20ms).
	var secondLatency time.Duration
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		time.Sleep(50 * time.Millisecond)
		f, _ := os.OpenFile(transcript, os.O_APPEND|os.O_WRONLY, 0o644)
		f.WriteString("{\"type\":\"user\",\"n\":10}\n")
		f.Close()
		deadline := time.Now().Add(2 * time.Second)
		for len(app.Calls()) < 2 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if len(app.Calls()) < 2 {
			return
		}
		// Immediately append another line; watcher must now be at 20ms, not 450ms.
		tAppend := time.Now()
		f, _ = os.OpenFile(transcript, os.O_APPEND|os.O_WRONLY, 0o644)
		f.WriteString("{\"type\":\"user\",\"n\":11}\n")
		f.Close()
		deadline = time.Now().Add(2 * time.Second)
		for len(app.Calls()) < 3 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if len(app.Calls()) >= 3 {
			secondLatency = time.Since(tAppend)
		}
	}()

	args := append([]string{"--watch", "1200ms", "--interval", "20ms", "--max-interval", "450ms"}, stateArgs(root)...)
	code, out, errOut := runCLI(t, args, env, cfg)
	<-doneCh
	if code != 0 {
		t.Fatalf("code=%d stderr=%q out=%q", code, errOut, out)
	}
	if len(app.Calls()) < 3 {
		t.Fatalf("expected 3 upload calls (seed + 2 appends), got %d", len(app.Calls()))
	}
	if secondLatency == 0 || secondLatency > 200*time.Millisecond {
		t.Fatalf("after activity woke a backed-off watcher, next line took %s (want <200ms at 20ms base interval)", secondLatency)
	}
}

// M2: empty directories back off from pass 1, and far-future mtimes are
// ignored when computing LatestModTime so they cannot defeat backoff.
func TestRunWatchStaysFastWhenActiveAndHandlesFutureMtimesAndZeroFiles(t *testing.T) {
	// 1) Zero files: backs off immediately from pass 1.
	env, root := cliEnv(t)
	os.Remove(filepath.Join(root, "claude", "project-a", "11111111-2222-3333-4444-555555555555.jsonl"))
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	args := append([]string{"--watch", "450ms", "--interval", "20ms", "--max-interval", "200ms"}, stateArgs(root)...)
	code, out, errOut := runCLI(t, args, env, cfg)
	if code != 0 {
		t.Fatalf("zero-file watch failed: code=%d stderr=%q", code, errOut)
	}
	if !strings.Contains(out, "passes=2 ") && !strings.Contains(out, "passes=3 ") && !strings.Contains(out, "passes=4 ") {
		t.Fatalf("zero-file host must back off from pass 1: %q", out)
	}

	// 2) Future-dated mtime must not pin LatestModTime and defeat backoff.
	futurePath := filepath.Join(root, "claude", "project-a", "future.jsonl")
	os.WriteFile(futurePath, []byte("{\"n\":1}\n"), 0o644)
	farFuture := time.Now().Add(24 * time.Hour)
	os.Chtimes(futurePath, farFuture, farFuture)
	runner := newRunner(t, filepath.Join(root, "claude"), &fakeUploads{}, nil)
	s, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if !s.LatestModTime.IsZero() {
		t.Fatalf("far-future mtime must be ignored for LatestModTime, got %s", s.LatestModTime)
	}
}

func parseWatchPasses(t *testing.T, out string) int {
	t.Helper()
	for _, field := range strings.Fields(out) {
		if strings.HasPrefix(field, "passes=") {
			n := 0
			for _, ch := range strings.TrimPrefix(field, "passes=") {
				if ch < '0' || ch > '9' {
					t.Fatalf("unexpected passes token %q in %q", field, out)
				}
				n = n*10 + int(ch-'0')
			}
			return n
		}
	}
	t.Fatalf("passes= not found in %q", out)
	return 0
}

// M2(a): a transcript modified seconds ago (< idle-after) and already uploaded
// must keep runWatch at the base interval across multiple idle passes.
func TestRunWatchStaysAtBaseIntervalForRecentlyModifiedTranscript(t *testing.T) {
	env, root := cliEnv(t)
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	transcript := filepath.Join(root, "claude", "project-a", "11111111-2222-3333-4444-555555555555.jsonl")
	recent := time.Now().Add(-10 * time.Second)
	if err := os.Chtimes(transcript, recent, recent); err != nil {
		t.Fatal(err)
	}
	if code, _, errOut := runCLI(t, stateArgs(root), env, cfg); code != 0 {
		t.Fatalf("seed run failed: code=%d stderr=%q", code, errOut)
	}

	// Pass 1 uploads 0 lines; only the on-disk recent mtime keeps the watcher
	// at 15ms instead of backing off to 250ms (which would yield <= 3 passes).
	args := append([]string{"--watch", "300ms", "--interval", "15ms", "--max-interval", "250ms", "--idle-after", "10m"}, stateArgs(root)...)
	code, out, errOut := runCLI(t, args, env, cfg)
	if code != 0 {
		t.Fatalf("code=%d stderr=%q out=%q", code, errOut, out)
	}
	if !strings.Contains(out, "lines=0 ") {
		t.Fatalf("expected 0 new lines on seeded watch, got %q", out)
	}
	if passes := parseWatchPasses(t, out); passes < 8 {
		t.Fatalf("recently modified transcript must stay at base interval: got passes=%d (want >=8, out=%q)", passes, out)
	}
}

// M2(b): a recently modified OpenClaw store (< idle-after) with no transcripts
// and already uploaded must keep runWatch at the base interval.
func TestRunWatchStaysAtBaseIntervalForRecentlyModifiedOpenClawStore(t *testing.T) {
	root := t.TempDir()
	db, dirs := newOpenClawStore(t, root)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	addEvent(t, db, "sess-1", 0, `{"type":"message","n":0}`)
	recent := time.Now().Add(-10 * time.Second)
	os.Chtimes(dirs.OpenClawStore, recent, recent)
	os.Chtimes(dirs.OpenClawStore+"-wal", recent, recent)

	env := map[string]string{
		"AGENT_SESSIONS_ACCOUNT":                "zach@example.com",
		"AGENT_SESSIONS_DEVICE":                 "test-device",
		"AGENT_SESSIONS_CLAUDE_PROJECTS_DIR":    filepath.Join(root, "none-claude"),
		"AGENT_SESSIONS_CODEX_SESSIONS_DIR":     filepath.Join(root, "none-codex"),
		"AGENT_SESSIONS_OPENCLAW_SESSIONS_DIR":  dirs.OpenClawSessions,
		"AGENT_SESSIONS_PI_SESSIONS_DIR":        filepath.Join(root, "none-pi"),
		"AGENT_SESSIONS_MUSE_CODE_SESSIONS_DIR": filepath.Join(root, "none-muse-code"),
		"AGENT_SESSIONS_ARGON_SESSIONS_DIR":     filepath.Join(root, "none-argon"),
		"PDW_INGEST_PROJECT_DIR":                root,
	}
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	if code, _, errOut := runCLI(t, stateArgs(root), env, cfg); code != 0 {
		t.Fatalf("seed run failed: code=%d stderr=%q", code, errOut)
	}

	// Pass 1 uploads 0 lines and discovers 0 transcript files; only the OpenClaw
	// store's recent mtime keeps the watcher at 15ms instead of 250ms.
	args := append([]string{"--watch", "300ms", "--interval", "15ms", "--max-interval", "250ms", "--idle-after", "10m"}, stateArgs(root)...)
	code, out, errOut := runCLI(t, args, env, cfg)
	if code != 0 {
		t.Fatalf("code=%d stderr=%q out=%q", code, errOut, out)
	}
	if !strings.Contains(out, "lines=0 ") {
		t.Fatalf("expected 0 new lines on seeded watch, got %q", out)
	}
	if passes := parseWatchPasses(t, out); passes < 8 {
		t.Fatalf("recently modified OpenClaw store must stay at base interval: got passes=%d (want >=8, out=%q)", passes, out)
	}
}

// M1: OpenClaw store with a live WAL file must be skipped under the 2s settle window
// (signatureOf combines db + wal mtimes; isSettled must check real max mtime, not hash).
func TestRunnerSkipsOpenClawStoreWithWALUnderSettleWindow(t *testing.T) {
	db, dirs := newOpenClawStore(t, t.TempDir())
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	addEvent(t, db, "sess-1", 0, `{"type":"message","n":0}`)
	addEvent(t, db, "sess-1", 1, `{"type":"message","n":1}`)
	walPath := dirs.OpenClawStore + "-wal"
	if _, err := os.Stat(walPath); err != nil {
		t.Fatalf("expected WAL file %s to exist: %v", walPath, err)
	}
	// Backdate both DB and WAL past the 2s settle window so pass 1 settles immediately.
	settled := time.Now().Add(-10 * time.Second)
	os.Chtimes(dirs.OpenClawStore, settled, settled)
	os.Chtimes(walPath, settled, settled)

	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	uploads := &fakeUploads{}
	runner := storeRunner(t, dirs, uploads, state)
	runner.SkipUnchanged = true
	runner.MtimeSettleWindow = defaultMtimeSettleWindow
	runner.Now = func() time.Time { return time.Now().UTC() }

	first, err := runner.Sync()
	if err != nil || first.LinesSelected != 2 {
		t.Fatalf("first pass = %+v, %v", first, err)
	}
	second, err := runner.Sync()
	if err != nil || second.LinesSelected != 0 || second.FilesUnchanged != 1 {
		t.Fatalf("WAL OpenClaw store must be skipped under settle window: %+v, %v", second, err)
	}
	addEvent(t, db, "sess-1", 2, `{"type":"message","n":2}`)
	third, err := runner.Sync()
	if err != nil || third.LinesSelected != 1 {
		t.Fatalf("WAL OpenClaw store must be re-read after new event: %+v, %v", third, err)
	}
}

// M3: same-size rewrite inside the 2s settle window must still upload its new line,
// both within the same Runner and across a restart + after settling.
func TestRunnerReuploadsSameSizeRewriteInsideSettleWindow(t *testing.T) {
	root := t.TempDir()
	transcript := filepath.Join(root, "racy-rewrite.jsonl")
	baseNow := time.Date(2026, 5, 21, 12, 0, 10, 0, time.UTC)
	t1 := baseNow.Add(-500 * time.Millisecond)
	t2 := baseNow.Add(-100 * time.Millisecond)
	os.WriteFile(transcript, []byte("{\"a\":1}\n"), 0o644)
	os.Chtimes(transcript, t1, t1)

	statePath := filepath.Join(t.TempDir(), "s.sqlite")
	state1, _ := OpenState(statePath, "z@x")
	uploads := &fakeUploads{}
	now := baseNow
	r1 := newRunner(t, root, uploads, state1)
	r1.SkipUnchanged = true
	r1.MtimeSettleWindow = 2 * time.Second
	r1.Now = func() time.Time { return now }

	if s, err := r1.Sync(); err != nil || s.LinesSelected != 1 {
		t.Fatalf("pass 1 = %+v, %v", s, err)
	}

	// Same-size in-place rewrite while STILL inside the 2s settle window (LastSize is -1).
	f, err := os.OpenFile(transcript, os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("{\"a\":2}\n")
	f.Close()
	os.Chtimes(transcript, t2, t2)

	// 1) Same runner must upload {"a":2}.
	s2, err := r1.Sync()
	if err != nil || s2.LinesSelected != 1 || len(uploads.batches) != 2 {
		t.Fatalf("same-size rewrite inside settle window must upload: %+v, %v, batches=%d", s2, err, len(uploads.batches))
	}
	if got := uploads.batches[1][0]["record"].(map[string]any)["line"].(map[string]any)["a"].(float64); got != 2 {
		t.Fatalf("got a=%v, want 2", got)
	}

	// 2) Rewrite again to {"a":3} inside settle window, then simulate restart AFTER settling.
	t3 := baseNow.Add(-50 * time.Millisecond)
	f, _ = os.OpenFile(transcript, os.O_WRONLY|os.O_TRUNC, 0o644)
	f.WriteString("{\"a\":3}\n")
	f.Close()
	os.Chtimes(transcript, t3, t3)
	state1.Close()

	state2, _ := OpenState(statePath, "z@x")
	defer state2.Close()
	r2 := newRunner(t, root, uploads, state2)
	r2.SkipUnchanged = true
	r2.MtimeSettleWindow = 2 * time.Second
	r2.Now = func() time.Time { return baseNow.Add(10 * time.Second) }
	s3, err := r2.Sync()
	if err != nil || s3.LinesSelected != 1 || len(uploads.batches) != 3 {
		t.Fatalf("same-size rewrite across restart must upload (never masked by backfill): %+v, %v, batches=%d", s3, err, len(uploads.batches))
	}
	if got := uploads.batches[2][0]["record"].(map[string]any)["line"].(map[string]any)["a"].(float64); got != 3 {
		t.Fatalf("got a=%v, want 3", got)
	}
}

// F4: argonMetaCache must not cache a failed SQLite read.
func TestArgonMetaCacheDoesNotCacheFailedSQLiteRead(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(root, "conversation_summaries.db")
	// Write a corrupt/unreadable SQLite file so the first read fails.
	if err := os.WriteFile(dbPath, []byte("not a sqlite db"), 0o644); err != nil {
		t.Fatal(err)
	}
	var cache argonMetaCache
	if got := cache.summariesFor(root); len(got) != 0 {
		t.Fatalf("corrupt db returned %v", got)
	}
	if _, cached := cache.byDB[dbPath]; cached {
		t.Fatal("failed SQLite read must not be cached")
	}
}

// F5: symlinked .jsonl files must be skipped (matching lstat semantics).
func TestDiscoverSkipsSymlinkedJSONL(t *testing.T) {
	root := t.TempDir()
	realFile := filepath.Join(t.TempDir(), "real.jsonl")
	os.WriteFile(realFile, []byte("{\"n\":1}\n"), 0o644)
	link := filepath.Join(root, "linked.jsonl")
	if err := os.Symlink(realFile, link); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if files := Discover(Dirs{ClaudeProjects: root}); len(files) != 0 {
		t.Fatalf("symlinked .jsonl must be skipped, got %v", files)
	}
}

func TestRunnerStateLossReuploadsCleanly(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "loss.jsonl"), []byte("{\"a\":1}\n{\"a\":2}\n"), 0o644)
	statePath := filepath.Join(t.TempDir(), "s.sqlite")
	state1, _ := OpenState(statePath, "z@x")
	uploads := &fakeUploads{}
	r1 := newRunner(t, root, uploads, state1)
	r1.SkipUnchanged = true
	if s, err := r1.Sync(); err != nil || s.LinesSelected != 2 {
		t.Fatalf("run 1 = %+v, %v", s, err)
	}
	state1.Close()

	// Simulate state loss: remove the state DB completely.
	if err := os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	state2, err := OpenState(statePath, "z@x")
	if err != nil {
		t.Fatal(err)
	}
	defer state2.Close()
	r2 := newRunner(t, root, uploads, state2)
	r2.SkipUnchanged = true
	s2, err := r2.Sync()
	if err != nil || s2.LinesSelected != 2 || len(uploads.batches) != 2 {
		t.Fatalf("after state loss, all lines must re-upload: %+v, %v, batches=%d", s2, err, len(uploads.batches))
	}
}

func TestRunnerUpgradesLegacyStateAndBackfillsSignatures(t *testing.T) {
	root := t.TempDir()
	transcript := filepath.Join(root, "legacy.jsonl")
	oldTime := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	content := []byte("{\"a\":1}\n")
	os.WriteFile(transcript, content, 0o644)
	os.Chtimes(transcript, oldTime, oldTime)

	// Build a pre-upgrade state DB without last_size/last_mtime_ns/last_inode.
	statePath := filepath.Join(t.TempDir(), "legacy.sqlite")
	rawDB, err := common.OpenSQLite(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rawDB.Exec(`
		CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '');
		INSERT INTO metadata VALUES ('schema_version', '1'), ('account', 'z@x');
		CREATE TABLE file_state (
			path TEXT PRIMARY KEY,
			uploaded_offset INTEGER NOT NULL DEFAULT 0,
			uploaded_lines INTEGER NOT NULL DEFAULT 0,
			last_uploaded_at TEXT NOT NULL DEFAULT ''
		);
		INSERT INTO file_state VALUES (?, ?, 1, '2026-05-20T12:00:00+00:00');
	`, transcript, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	rawDB.Close()

	// First run after upgrade must NOT re-upload, and must backfill signature.
	state1, err := OpenState(statePath, "z@x")
	if err != nil {
		t.Fatal(err)
	}
	uploads := &fakeUploads{}
	r1 := newRunner(t, root, uploads, state1)
	r1.SkipUnchanged = true
	s1, err := r1.Sync()
	if err != nil || s1.LinesSelected != 0 || len(uploads.batches) != 0 {
		t.Fatalf("upgraded run must not re-upload already-shipped file: %+v, %v, batches=%d", s1, err, len(uploads.batches))
	}
	state1.Close()

	// Second run after restart must immediately skip using the backfilled signature.
	state2, _ := OpenState(statePath, "z@x")
	defer state2.Close()
	r2 := newRunner(t, root, uploads, state2)
	r2.SkipUnchanged = true
	s2, err := r2.Sync()
	if err != nil || s2.FilesUnchanged != 1 || s2.LinesSelected != 0 {
		t.Fatalf("restart after backfill must skip immediately: %+v, %v", s2, err)
	}
}

func TestRunnerAttachesArgonSessionMetaLazilyOnChangedTranscript(t *testing.T) {
	root := t.TempDir()
	brain := filepath.Join(root, "brain")
	logs := filepath.Join(brain, "sess-1", ".system_generated", "logs")
	ann := filepath.Join(root, "annotations")
	os.MkdirAll(logs, 0o755)
	os.MkdirAll(ann, 0o755)
	os.WriteFile(filepath.Join(logs, "transcript_full.jsonl"), []byte("{\"step_index\":0,\"type\":\"USER_INPUT\"}\n"), 0o644)
	os.WriteFile(filepath.Join(ann, "sess-1.pbtxt"), []byte("title: \"Lazy Meta Test\"\n"), 0o644)

	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	uploads := &fakeUploads{}
	runner := &Runner{
		Account: "z@x", Device: "mew-dev",
		Dirs:          Dirs{ArgonSessions: brain},
		Upload:        uploads.upload,
		Logger:        &common.RecordingLogger{},
		State:         state,
		SkipUnchanged: true,
		Now:           func() time.Time { return time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC) },
	}
	s, err := runner.Sync()
	if err != nil || s.LinesSelected != 1 || len(uploads.batches) != 1 {
		t.Fatalf("argon sync = %+v, %v, batches=%d", s, err, len(uploads.batches))
	}
	rec := uploads.batches[0][0]["record"].(map[string]any)
	sess, ok := rec["session"].(map[string]any)
	if !ok || sess["title"] != "Lazy Meta Test" {
		t.Fatalf("argon session meta missing from uploaded record: %v", rec)
	}
}
