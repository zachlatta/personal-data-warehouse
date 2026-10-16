package voicememos

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A memo that just finished downloading is a few seconds old when launchd
// fires on the store change. The run waits out the short settle and uploads
// it in the same run, instead of leaving it for the next five-minute tick.
func TestARunWaitsOutTheSettleForAJustArrivedMemo(t *testing.T) {
	root := t.TempDir()
	name := "20261015 205541-03A81A2D.m4a"
	writeRecordings(t, root, name)
	clock := testNow
	modified := clock.Add(-2 * time.Second)
	os.Chtimes(filepath.Join(root, name), modified, modified)

	client := &fakeUploader{}
	runner := newRunner(root, client, EmptyState("zach@example.com", root))
	runner.MinFileAgeSeconds = 5
	runner.Now = func() time.Time { return clock }
	var slept time.Duration
	runner.Sleep = func(d time.Duration) {
		slept += d
		clock = clock.Add(d)
	}
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if len(client.audio) != 1 || summary.RecordingsUploaded != 1 {
		t.Fatalf("the memo must land in this run: audio=%d summary=%+v", len(client.audio), summary)
	}
	if slept < 3*time.Second || slept > 4*time.Second {
		t.Fatalf("slept %s, want the ~3s left of the settle", slept)
	}
}

func TestARunDoesNotWaitForAFileStillBeingWrittenPastTheSettleBudget(t *testing.T) {
	root := t.TempDir()
	name := "20261015 205541-03A81A2D.m4a"
	writeRecordings(t, root, name)
	clock := testNow
	os.Chtimes(filepath.Join(root, name), clock, clock)

	runner := newRunner(root, &fakeUploader{}, EmptyState("zach@example.com", root))
	runner.MinFileAgeSeconds = 120
	runner.Now = func() time.Time { return clock }
	runner.Sleep = func(d time.Duration) { t.Fatalf("must not sleep %s for a gate longer than the settle budget", d) }
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.RecordingsDeferred != 1 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestTheDefaultSettleIsSecondsNotMinutes(t *testing.T) {
	opts, err := ParseArgs(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if opts.MinFileAgeSeconds > 10 {
		t.Fatalf("min file age = %ds; two minutes of it was most of a short memo's landing time", opts.MinFileAgeSeconds)
	}
	if opts.MaxRunSeconds != 900 {
		t.Fatalf("incremental max run = %d", opts.MaxRunSeconds)
	}
	full, _ := ParseArgs([]string{"--mode", "full"}, &bytes.Buffer{})
	if full.MaxRunSeconds != 0 {
		t.Fatalf("a full backfill has no deadline by default, got %d", full.MaxRunSeconds)
	}
	explicit, _ := ParseArgs([]string{"--max-run-seconds", "60"}, &bytes.Buffer{})
	if explicit.MaxRunSeconds != 60 {
		t.Fatalf("explicit max run = %d", explicit.MaxRunSeconds)
	}
}

// 2026-10-14: one run hung at "Scanning" for 10.9 hours, and launchd never
// starts a job that is still running, so two memos waited ~13 hours. The
// deadline turns that into one missed cycle and a failed run.
func TestTheRunDeadlineExitsAHungRun(t *testing.T) {
	var out bytes.Buffer
	exited := make(chan int, 1)
	stop := startRunDeadline(20*time.Millisecond, &out, func(code int) { exited <- code })
	defer stop()
	select {
	case code := <-exited:
		if code != ExitCodeRunDeadline {
			t.Fatalf("exit code = %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the deadline never fired")
	}
	if !strings.Contains(out.String(), "deadline") {
		t.Fatalf("message = %q", out.String())
	}
}

func TestAStoppedRunDeadlineNeverFires(t *testing.T) {
	exited := make(chan int, 1)
	stop := startRunDeadline(20*time.Millisecond, &bytes.Buffer{}, func(code int) { exited <- code })
	stop()
	select {
	case code := <-exited:
		t.Fatalf("a finished run was killed with %d", code)
	case <-time.After(100 * time.Millisecond):
	}
	if stop := startRunDeadline(0, &bytes.Buffer{}, func(int) { t.Fatal("a zero deadline is off") }); stop == nil {
		t.Fatal("stop must be callable")
	}
}
