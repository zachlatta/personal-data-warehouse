package agentsessions

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	_ "modernc.org/sqlite"
)

func procCPUTime() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond +
		time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
}

// buildSyntheticPorygonTree builds a ~2,528-transcript tree matching the
// real directory counts observed on porygon (Claude projects with tool-results,
// Pi, Muse Code with sidecars, Argon with chunks/steps/conversation_summaries.db
// and annotation pbtxt files).
func buildSyntheticPorygonTree(tb testing.TB, root string) Dirs {
	tb.Helper()
	claudeRoot := filepath.Join(root, "claude")
	piRoot := filepath.Join(root, "pi")
	museRoot := filepath.Join(root, "muse")
	argonRoot := filepath.Join(root, "argon")
	argonBrain := filepath.Join(argonRoot, "brain")
	argonAnn := filepath.Join(argonRoot, "annotations")
	if err := os.MkdirAll(argonAnn, 0o755); err != nil {
		tb.Fatal(err)
	}

	for i := 0; i < 2176; i++ {
		proj := filepath.Join(claudeRoot, fmt.Sprintf("proj-%03d", i%200))
		if err := os.MkdirAll(proj, 0o755); err != nil {
			tb.Fatal(err)
		}
		p := filepath.Join(proj, fmt.Sprintf("sess-%04d.jsonl", i))
		if err := os.WriteFile(p, []byte("{\"type\":\"user\",\"timestamp\":\"2026-10-20T12:00:00Z\",\"n\":1}\n"), 0o644); err != nil {
			tb.Fatal(err)
		}
		if i < 680 {
			tr := filepath.Join(proj, fmt.Sprintf("sess-%04d", i), "tool-results")
			if err := os.MkdirAll(tr, 0o755); err != nil {
				tb.Fatal(err)
			}
			for j := 0; j < 10; j++ {
				if err := os.WriteFile(filepath.Join(tr, fmt.Sprintf("tool-%d.txt", j)), []byte("output\n"), 0o644); err != nil {
					tb.Fatal(err)
				}
			}
		}
	}

	for i := 0; i < 59; i++ {
		d := filepath.Join(piRoot, fmt.Sprintf("proj-%02d", i%20))
		if err := os.MkdirAll(d, 0o755); err != nil {
			tb.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, fmt.Sprintf("2026-07-09T01-16-52-697Z_00000000-0000-0000-0000-%012d.jsonl", i)),
			[]byte("{\"timestamp\":\"2026-07-09T01:16:52Z\",\"n\":1}\n"), 0o644); err != nil {
			tb.Fatal(err)
		}
	}

	for i := 0; i < 110; i++ {
		d := filepath.Join(museRoot, "2026", "10", fmt.Sprintf("%02d", (i%28)+1), fmt.Sprintf("ms-%04d", i))
		if err := os.MkdirAll(filepath.Join(d, ".msp-view-v1"), 0o755); err != nil {
			tb.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "session.jsonl"), []byte("{\"recorded_at\":1793000000000000,\"n\":1}\n"), 0o644); err != nil {
			tb.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "session.peer-history.jsonl"), []byte("{}\n"), 0o644); err != nil {
			tb.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, ".msp-view-v1", "index.json"), []byte("{}\n"), 0o644); err != nil {
			tb.Fatal(err)
		}
	}

	dbPath := filepath.Join(argonRoot, "conversation_summaries.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE conversation_summaries (
		conversation_id TEXT PRIMARY KEY,
		title TEXT,
		workspace_uris TEXT,
		parent_conversation_id TEXT
	)`); err != nil {
		tb.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		tb.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO conversation_summaries VALUES (?, ?, ?, ?)`)
	if err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < 183; i++ {
		id := fmt.Sprintf("argon-%04d", i)
		logs := filepath.Join(argonBrain, id, ".system_generated", "logs")
		chunks := filepath.Join(logs, "chunks")
		if err := os.MkdirAll(chunks, 0o755); err != nil {
			tb.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(logs, "transcript_full.jsonl"), []byte("{\"created_at\":\"2026-10-20T12:00:00Z\",\"n\":1}\n"), 0o644); err != nil {
			tb.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(logs, "transcript.jsonl"), []byte("{}\n"), 0o644); err != nil {
			tb.Fatal(err)
		}
		for j := 0; j < 3; j++ {
			if err := os.WriteFile(filepath.Join(chunks, fmt.Sprintf("%d.jsonl", j)), []byte("{}\n"), 0o644); err != nil {
				tb.Fatal(err)
			}
		}
		for s := 0; s < 12; s++ {
			sd := filepath.Join(argonBrain, id, ".system_generated", "steps", fmt.Sprintf("%d", s))
			if err := os.MkdirAll(sd, 0o755); err != nil {
				tb.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(sd, "output.txt"), []byte("step\n"), 0o644); err != nil {
				tb.Fatal(err)
			}
		}
		title := fmt.Sprintf("Session %d", i)
		if i < 53 {
			title = ""
			if err := os.WriteFile(filepath.Join(argonAnn, id+".pbtxt"), []byte(fmt.Sprintf("title: \"Ann %d\"\n", i)), 0o644); err != nil {
				tb.Fatal(err)
			}
		}
		if _, err := stmt.Exec(id, title, `["file:///work/repo"]`, ""); err != nil {
			tb.Fatal(err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
	_ = db.Close()

	return Dirs{
		ClaudeProjects:   claudeRoot,
		PiSessions:       piRoot,
		MuseCodeSessions: museRoot,
		ArgonSessions:    argonBrain,
	}
}

func BenchmarkIdleWatchPass(b *testing.B) {
	root := b.TempDir()
	dirs := buildSyntheticPorygonTree(b, root)
	state, err := OpenState(filepath.Join(root, "state.sqlite"), "bench@example.com")
	if err != nil {
		b.Fatal(err)
	}
	defer state.Close()

	runner := &Runner{
		Account:       "bench@example.com",
		Device:        "bench",
		Dirs:          dirs,
		State:         state,
		SkipUnchanged: true,
		Logger:        &common.RecordingLogger{},
		Upload: func(_ []byte, _ time.Time) (ingestclient.StoredObject, error) {
			return ingestclient.StoredObject{StorageKey: "k"}, nil
		},
	}
	if _, err := runner.Sync(); err != nil {
		b.Fatal(err)
	}

	// Report per-phase breakdown once before timing b.N idle passes.
	t0, c0 := time.Now(), procCPUTime()
	cFiles := Discover(Dirs{ClaudeProjects: dirs.ClaudeProjects})
	b.Logf("Discover claude=%d wall=%s cpu=%s", len(cFiles), time.Since(t0), procCPUTime()-c0)

	t0, c0 = time.Now(), procCPUTime()
	aFiles := Discover(Dirs{ArgonSessions: dirs.ArgonSessions})
	b.Logf("Discover argon=%d wall=%s cpu=%s", len(aFiles), time.Since(t0), procCPUTime()-c0)

	t0, c0 = time.Now(), procCPUTime()
	mFiles := Discover(Dirs{MuseCodeSessions: dirs.MuseCodeSessions})
	b.Logf("Discover muse=%d wall=%s cpu=%s", len(mFiles), time.Since(t0), procCPUTime()-c0)

	t0, c0 = time.Now(), procCPUTime()
	pFiles := Discover(Dirs{PiSessions: dirs.PiSessions})
	b.Logf("Discover pi=%d wall=%s cpu=%s", len(pFiles), time.Since(t0), procCPUTime()-c0)

	// Measure cold start pass (fresh Runner with warm SQLite state, as on every 5m restart)
	cold := &Runner{
		Account:       "bench@example.com",
		Device:        "bench",
		Dirs:          dirs,
		State:         state,
		SkipUnchanged: true,
		Logger:        &common.RecordingLogger{},
		Upload: func(_ []byte, _ time.Time) (ingestclient.StoredObject, error) {
			return ingestclient.StoredObject{StorageKey: "k"}, nil
		},
	}
	t0, c0 = time.Now(), procCPUTime()
	sCold, err := cold.Sync()
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("Cold pass 1: files=%d unchanged=%d lines=%d wall=%s cpu=%s",
		sCold.FilesSeen, sCold.FilesUnchanged, sCold.LinesSelected, time.Since(t0), procCPUTime()-c0)

	b.ResetTimer()
	cStart := procCPUTime()
	for i := 0; i < b.N; i++ {
		if _, err := runner.Sync(); err != nil {
			b.Fatal(err)
		}
	}
	cpuElapsed := procCPUTime() - cStart
	b.ReportMetric(float64(cpuElapsed.Microseconds())/float64(b.N)/1000.0, "cpu-ms/op")
}
