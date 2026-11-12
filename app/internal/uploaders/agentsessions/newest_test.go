package agentsessions

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// The event time is the field agent_sessions_drive_ingest reads as
// occurred_at for that tool, so a healthy device reads `landed` in
// marts_ops.source_upstream_health.
func TestEventTimeReadsEachToolsOccurredAtField(t *testing.T) {
	at := time.Date(2026, 11, 11, 23, 43, 41, 442_000_000, time.UTC)
	cases := []struct {
		tool string
		line map[string]any
		want time.Time
	}{
		{ClaudeCodeTool, map[string]any{"timestamp": "2026-11-11T23:43:41.442Z"}, at},
		{CodexTool, map[string]any{"timestamp": "2026-11-11T23:43:41.442Z"}, at},
		{OpenClawTool, map[string]any{"timestamp": "2026-11-11T23:43:41.442Z"}, at},
		{PiTool, map[string]any{"timestamp": "2026-11-11T23:43:41.442Z"}, at},
		{ArgonTool, map[string]any{"created_at": "2026-11-11T23:43:41.442Z"}, at},
		{MuseTool, map[string]any{"created_at": float64(at.UnixMilli()) / 1000}, at},
		{MuseTool, map[string]any{"created_at": float64(at.UnixMilli())}, at}, // milliseconds
		{MuseCodeTool, map[string]any{"recorded_at": float64(at.UnixMicro())}, at},
		{MuseCodeTool, map[string]any{"retained_frame": true, "children": []any{
			map[string]any{"record_json": `{"recorded_at": ` + strconv.FormatInt(at.UnixMicro(), 10) + `}`},
		}}, at},
	}
	for _, c := range cases {
		got, ok := eventTime(c.tool, c.line)
		if !ok || got.Sub(c.want).Abs() > time.Millisecond {
			t.Errorf("%s %v: got %v %v, want %v", c.tool, c.line, got, ok, c.want)
		}
	}
	if _, ok := eventTime(ClaudeCodeTool, map[string]any{"type": "mode"}); ok {
		t.Error("a line with no time has no event time")
	}
}

// The observation is the device's own newest event, read from the transcripts
// themselves, so a tail that stopped advancing cannot hide the lines it never
// read: on 2026-11-11 one Claude Code session on mew-dev had lines 51 minutes
// newer than anything PDW held for it.
func TestRunnerObservesTheDevicesNewestEventPerToolBeforeUploading(t *testing.T) {
	root := t.TempDir()
	claude := filepath.Join(root, "claude")
	codex := filepath.Join(root, "codex")
	os.MkdirAll(claude, 0o755)
	os.MkdirAll(codex, 0o755)
	older := filepath.Join(claude, "11111111-1111-1111-1111-111111111111.jsonl")
	newer := filepath.Join(claude, "22222222-2222-2222-2222-222222222222.jsonl")
	os.WriteFile(older, []byte(`{"timestamp":"2026-11-11T20:00:00Z"}`+"\n"), 0o644)
	os.WriteFile(newer, []byte(`{"timestamp":"2026-11-11T23:00:00Z"}`+"\n"+`{"type":"mode"}`+"\n"+`{"timestamp":"2026-11-11T23:43:41.442Z"}`+"\n"), 0o644)
	os.WriteFile(filepath.Join(codex, "rollout-2026-11-11T13-31-31-33333333-3333-3333-3333-333333333333.jsonl"),
		[]byte(`{"timestamp":"2026-11-11T22:00:00Z"}`+"\n"), 0o644)

	var observed []ingestclient.SourceObservation
	var calls []string
	uploads := &fakeUploads{}
	runner := &Runner{
		Account: "z@x", Device: "crobat",
		Dirs: Dirs{ClaudeProjects: claude, CodexSessions: codex},
		Upload: func(gz []byte, at time.Time) (ingestclient.StoredObject, error) {
			calls = append(calls, "upload")
			return uploads.upload(gz, at)
		},
		Observe: func(o ingestclient.SourceObservation) (map[string]any, error) {
			calls = append(calls, "observe")
			observed = append(observed, o)
			return nil, nil
		},
		Logger: &common.RecordingLogger{},
		Now:    func() time.Time { return time.Date(2026, 11, 11, 23, 45, 0, 0, time.UTC) },
	}
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	byPipeline := map[string]ingestclient.SourceObservation{}
	for _, o := range observed {
		byPipeline[o.Pipeline] = o
	}
	claudeObs := byPipeline[ClaudeCodeTool]
	if claudeObs.Scope != "z@x/crobat" || claudeObs.NewestAt != "2026-11-11T23:43:41.442000+00:00" ||
		claudeObs.NewestID != "22222222-2222-2222-2222-222222222222" {
		t.Fatalf("claude_code observation = %+v", claudeObs)
	}
	if byPipeline[CodexTool].NewestID != "33333333-3333-3333-3333-333333333333" {
		t.Fatalf("codex observation = %+v", byPipeline[CodexTool])
	}
	if calls[0] != "observe" {
		t.Fatalf("calls = %v: observe before the first upload", calls)
	}

	// A watcher pass every two seconds observes at most every five minutes.
	runner.Observe = func(o ingestclient.SourceObservation) (map[string]any, error) {
		t.Fatalf("observed again within the interval: %+v", o)
		return nil, nil
	}
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestAFailedObservationNeverFailsTheUpload(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "44444444-4444-4444-4444-444444444444.jsonl"),
		[]byte(`{"timestamp":"2026-11-11T23:00:00Z"}`+"\n"), 0o644)
	uploads := &fakeUploads{}
	runner := newRunner(t, root, uploads, nil)
	runner.Observe = func(ingestclient.SourceObservation) (map[string]any, error) {
		return nil, errors.New("app unreachable")
	}
	summary, err := runner.Sync()
	if err != nil || summary.BatchesUploaded != 1 {
		t.Fatalf("summary = %+v, err = %v", summary, err)
	}
}
