package agentsessions

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

func TestSessionIDs(t *testing.T) {
	if got := ClaudeSessionID("6f1a2b3c-1111-2222-3333-444444444444.jsonl"); got != "6f1a2b3c-1111-2222-3333-444444444444" {
		t.Fatalf("claude = %q", got)
	}
	if got := CodexSessionID("rollout-2025-06-01T12-00-00-6F1A2B3C-1111-2222-3333-444444444444.jsonl"); got != "6f1a2b3c-1111-2222-3333-444444444444" {
		t.Fatalf("codex = %q", got)
	}
	if got := CodexSessionID("rollout-weird.jsonl"); got != "weird" {
		t.Fatalf("codex fallback = %q", got)
	}
	if got := OpenClawSessionID("abc-123.jsonl"); got != "abc-123" {
		t.Fatalf("openclaw = %q", got)
	}
	if got := PiSessionID("2025-06-01T12-00-00_6f1a2b3c-1111-2222-3333-444444444444.jsonl"); got != "6f1a2b3c-1111-2222-3333-444444444444" {
		t.Fatalf("pi = %q", got)
	}
	if got := PiSessionID("2025_stem.jsonl"); got != "stem" {
		t.Fatalf("pi fallback = %q", got)
	}
}

func TestDiscoverFindsEveryToolAndIgnoresSidecars(t *testing.T) {
	root := t.TempDir()
	claude := filepath.Join(root, "claude", "proj-a")
	codex := filepath.Join(root, "codex", "2025", "06")
	openclaw := filepath.Join(root, "openclaw")
	pi := filepath.Join(root, "pi", "2025")
	for _, dir := range []string{claude, codex, openclaw, pi} {
		os.MkdirAll(dir, 0o755)
	}
	write := func(path string) { os.WriteFile(path, []byte("{}\n"), 0o644) }
	write(filepath.Join(claude, "6f1a2b3c-1111-2222-3333-444444444444.jsonl"))
	write(filepath.Join(claude, "journal.jsonl"))
	write(filepath.Join(claude, "notes.txt"))
	write(filepath.Join(codex, "rollout-2025-06-01T12-00-00-6f1a2b3c-1111-2222-3333-444444444444.jsonl"))
	write(filepath.Join(codex, "other.jsonl"))
	write(filepath.Join(openclaw, "sess-1.jsonl"))
	write(filepath.Join(openclaw, "sess-1.trajectory.jsonl"))
	write(filepath.Join(openclaw, "sess-1.meta.json"))
	write(filepath.Join(pi, "2025-06-01T12-00-00_6f1a2b3c-1111-2222-3333-444444444444.jsonl"))

	files := Discover(Dirs{ClaudeProjects: claude, CodexSessions: filepath.Join(root, "codex"), OpenClawSessions: openclaw, PiSessions: filepath.Join(root, "pi")})
	var got []string
	for _, f := range files {
		got = append(got, f.Tool+":"+f.SessionID)
	}
	want := []string{
		"claude_code:6f1a2b3c-1111-2222-3333-444444444444",
		"codex:6f1a2b3c-1111-2222-3333-444444444444",
		"openclaw:sess-1",
		"pi:6f1a2b3c-1111-2222-3333-444444444444",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("Discover = %v, want %v", got, want)
	}
	if len(Discover(Dirs{ClaudeProjects: filepath.Join(root, "missing")})) != 0 {
		t.Fatal("a missing root must be skipped")
	}
}

func TestStateRoundTripsAndResetsOnAccountChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	state, err := OpenState(path, "z@x")
	if err != nil {
		t.Fatal(err)
	}
	if err := state.RecordProgress("/a.jsonl", 42, 3, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	progress, _ := state.ProgressFor("/a.jsonl")
	if progress.UploadedOffset != 42 || progress.UploadedLines != 3 {
		t.Fatalf("progress = %+v", progress)
	}
	state.Close()
	state, err = OpenState(path, "other@x")
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	progress, _ = state.ProgressFor("/a.jsonl")
	if progress.UploadedOffset != 0 {
		t.Fatalf("an account change must wipe the state, got %+v", progress)
	}
}

type fakeUploads struct {
	batches [][]map[string]any
}

func (f *fakeUploads) upload(gz []byte, exportedAt time.Time) (ingestclient.StoredObject, error) {
	reader, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return ingestclient.StoredObject{}, err
	}
	data, _ := io.ReadAll(reader)
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return ingestclient.StoredObject{}, err
		}
		records = append(records, record)
	}
	f.batches = append(f.batches, records)
	return ingestclient.StoredObject{StorageKey: "agent-sessions/inbox/batch"}, nil
}

func newRunner(t *testing.T, root string, uploads *fakeUploads, state *State) *Runner {
	t.Helper()
	return &Runner{
		Account: "z@x", Device: "crobat",
		Dirs:   Dirs{ClaudeProjects: root},
		Upload: uploads.upload, Logger: &common.RecordingLogger{}, State: state,
		Now: func() time.Time { return time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC) },
	}
}

func TestRunnerUploadsNewLinesThenOnlyAppendedLines(t *testing.T) {
	root := t.TempDir()
	transcript := filepath.Join(root, "6f1a2b3c-1111-2222-3333-444444444444.jsonl")
	os.WriteFile(transcript, []byte("{\"a\":1}\n{\"a\":2}\n"), 0o644)
	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	uploads := &fakeUploads{}
	summary, err := newRunner(t, root, uploads, state).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.LinesSelected != 2 || summary.BatchesUploaded != 1 || summary.FilesWithNewLines != 1 {
		t.Fatalf("summary = %+v", summary)
	}
	first := uploads.batches[0][0]
	if first["source"] != "agent_sessions" || first["device"] != "crobat" || first["record_type"] != "claude_code_event" {
		t.Fatalf("envelope = %v", first)
	}
	record := first["record"].(map[string]any)
	if record["seq"].(float64) != 0 || record["session_id"] != "6f1a2b3c-1111-2222-3333-444444444444" || record["line"].(map[string]any)["a"].(float64) != 1 {
		t.Fatalf("record = %v", record)
	}
	if first["exported_at"] != "2026-05-21T12:00:00+00:00" {
		t.Fatalf("exported_at = %v", first["exported_at"])
	}

	f, _ := os.OpenFile(transcript, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString("{\"a\":3}\n")
	f.Close()
	summary, err = newRunner(t, root, uploads, state).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.LinesSelected != 1 || len(uploads.batches) != 2 {
		t.Fatalf("second summary = %+v batches=%d", summary, len(uploads.batches))
	}
	if uploads.batches[1][0]["record"].(map[string]any)["seq"].(float64) != 2 {
		t.Fatalf("appended line must carry the absolute sequence number")
	}
}

func TestRunnerIgnoresTrailingPartialLineAndSkipsBlankOrBadLines(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "s.jsonl"), []byte("{\"a\":1}\n\nnot json\n{\"a\":2"), 0o644)
	uploads := &fakeUploads{}
	runner := newRunner(t, root, uploads, nil)
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.LinesSelected != 1 || summary.LinesSkipped != 2 {
		t.Fatalf("summary = %+v", summary)
	}
	if !strings.Contains(strings.Join(runner.Logger.(*common.RecordingLogger).Warnings, "\n"), "Skipping unparseable line 2") {
		t.Fatalf("warnings = %v", runner.Logger.(*common.RecordingLogger).Warnings)
	}
}

func TestRunnerRespectsLimitAndDefersRemainder(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "a.jsonl"), []byte("{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n"), 0o644)
	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	uploads := &fakeUploads{}
	runner := newRunner(t, root, uploads, state)
	runner.Limit = 2
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.LinesSelected != 2 || !summary.LimitReached {
		t.Fatalf("summary = %+v", summary)
	}
	runner = newRunner(t, root, uploads, state)
	runner.Limit = 2
	summary, _ = runner.Sync()
	if summary.LinesSelected != 1 || summary.LimitReached {
		t.Fatalf("second summary = %+v", summary)
	}
}

func TestRunnerFullModeReuploadsEverything(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "a.jsonl"), []byte("{\"n\":1}\n"), 0o644)
	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	uploads := &fakeUploads{}
	newRunner(t, root, uploads, state).Sync()
	runner := newRunner(t, root, uploads, state)
	runner.Mode = "full"
	summary, _ := runner.Sync()
	if summary.LinesSelected != 1 || len(uploads.batches) != 2 {
		t.Fatalf("full mode summary = %+v", summary)
	}
}

func TestRunnerCoalescesFilesAndCommitsEachAtBatchBoundaries(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "a.jsonl"), []byte("{\"n\":1}\n{\"n\":2}\n"), 0o644)
	os.WriteFile(filepath.Join(root, "b.jsonl"), []byte("{\"n\":3}\n{\"n\":4}\n{\"n\":5}\n"), 0o644)
	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	uploads := &fakeUploads{}
	runner := newRunner(t, root, uploads, state)
	runner.BatchSize = 3
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.BatchesUploaded != 2 || len(uploads.batches[0]) != 3 || len(uploads.batches[1]) != 2 {
		t.Fatalf("summary = %+v batches=%d", summary, len(uploads.batches))
	}
	a, _ := state.ProgressFor(filepath.Join(root, "a.jsonl"))
	b, _ := state.ProgressFor(filepath.Join(root, "b.jsonl"))
	if a.UploadedLines != 2 || b.UploadedLines != 3 {
		t.Fatalf("progress a=%+v b=%+v", a, b)
	}
}

func TestRunnerBlockedByNetworkGuardBeforeAnyUpload(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "a.jsonl"), []byte("{\"n\":1}\n"), 0o644)
	uploads := &fakeUploads{}
	runner := newRunner(t, root, uploads, nil)
	runner.BeforeUploadCheck = func() string { return "blocked hardware port: iPhone USB" }
	_, err := runner.Sync()
	var blocked *ErrUploadBlocked
	if err == nil || !errorsAs(err, &blocked) || len(uploads.batches) != 0 {
		t.Fatalf("err = %v batches=%d", err, len(uploads.batches))
	}
}

func errorsAs(err error, target **ErrUploadBlocked) bool {
	e, ok := err.(*ErrUploadBlocked)
	if ok {
		*target = e
	}
	return ok
}
