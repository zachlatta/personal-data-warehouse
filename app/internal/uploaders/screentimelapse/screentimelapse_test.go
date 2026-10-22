package screentimelapse

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

type recordedUpload struct {
	file    ingestclient.ScreenTimelapseFile
	content string
}

type fakeUploader struct {
	uploads []recordedUpload
	failOn  string
}

func (f *fakeUploader) UploadScreenTimelapseFile(content []byte, file ingestclient.ScreenTimelapseFile) (ingestclient.ScreenTimelapseStored, error) {
	if f.failOn != "" && file.SourceFilename == f.failOn {
		return ingestclient.ScreenTimelapseStored{}, errors.New("HTTP 502")
	}
	f.uploads = append(f.uploads, recordedUpload{file: file, content: string(content)})
	return ingestclient.ScreenTimelapseStored{StoredObject: ingestclient.StoredObject{StorageKey: "k"}}, nil
}

func (f *fakeUploader) names() []string {
	var out []string
	for _, u := range f.uploads {
		out = append(out, u.file.Device+"/"+u.file.SourceFilename)
	}
	return out
}

var testNow = time.Date(2026, 10, 22, 18, 30, 0, 0, time.UTC)

func newYork(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

// writeCapture writes a file whose mtime is ageAgo before testNow.
func writeCapture(t *testing.T, dir, name, content string, ageAgo time.Duration) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	at := testNow.Add(-ageAgo)
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatal(err)
	}
	return path
}

func newRunner(t *testing.T, client Uploader, sources ...Source) *Runner {
	t.Helper()
	state, err := OpenState(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	return &Runner{
		Sources:  sources,
		Client:   client,
		State:    state,
		Logger:   common.NewWriterLogger(&strings.Builder{}),
		Now:      func() time.Time { return testNow },
		Location: newYork(t),
	}
}

func TestSyncUploadsEverySettledCaptureFileFromEachDevice(t *testing.T) {
	mac, phone := t.TempDir(), t.TempDir()
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-00-54-display-1.heic", "pixels", 20*time.Minute)
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-00-54.ax.json.gz", "tree", 20*time.Minute)
	writeCapture(t, phone, "phone-screenshot-2026-10-22_14-01-03.png", "phone pixels", 20*time.Minute)
	// Not capture files, or not finished being written.
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-29-30.ax.json", "being gzipped", 10*time.Minute)
	writeCapture(t, mac, ".DS_Store", "", time.Hour)
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-29-30-display-1.heic", "just written", 30*time.Second)

	client := &fakeUploader{}
	summary, err := newRunner(t, client, Source{Device: "crobat", Dir: mac}, Source{Device: "joltik", Dir: phone}).Sync()
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(client.names(), ",")
	for _, want := range []string{
		"crobat/periodic-screenshot-2026-10-22_14-00-54-display-1.heic",
		"crobat/periodic-screenshot-2026-10-22_14-00-54.ax.json.gz",
		"joltik/phone-screenshot-2026-10-22_14-01-03.png",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("uploads %s missing %s", got, want)
		}
	}
	if len(client.uploads) != 3 || summary.Uploaded != 3 || summary.Unsettled != 1 {
		t.Fatalf("uploads = %v, summary = %+v", got, summary)
	}
	for _, u := range client.uploads {
		if u.file.SourceFilename == "periodic-screenshot-2026-10-22_14-00-54-display-1.heic" {
			// 14:00:54 EDT, the capture host's zone.
			if u.file.CapturedAt != "2026-10-22T18:00:54Z" || u.file.ContentType != "image/heic" || u.content != "pixels" {
				t.Fatalf("upload = %+v", u)
			}
		}
	}
}

func TestSyncSendsAFileOnceAndAgainOnlyWhenItChanges(t *testing.T) {
	mac := t.TempDir()
	path := writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-00-54-display-1.heic", "pixels", 20*time.Minute)
	client := &fakeUploader{}
	runner := newRunner(t, client, Source{Device: "crobat", Dir: mac})
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 1 {
		t.Fatalf("an acknowledged file was sent again: %v", client.names())
	}
	if err := os.WriteFile(path, []byte("re-encoded"), 0o644); err != nil {
		t.Fatal(err)
	}
	at := testNow.Add(-15 * time.Minute)
	_ = os.Chtimes(path, at, at)
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 2 || client.uploads[1].content != "re-encoded" {
		t.Fatalf("a changed file must be sent again: %v", client.uploads)
	}
}

func TestSyncStopsAtAFailedUploadAndLeavesItUnacknowledged(t *testing.T) {
	mac := t.TempDir()
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-00-54-display-1.heic", "a", 20*time.Minute)
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_13-59-52-display-1.heic", "b", 21*time.Minute)
	client := &fakeUploader{failOn: "periodic-screenshot-2026-10-22_14-00-54-display-1.heic"}
	runner := newRunner(t, client, Source{Device: "crobat", Dir: mac})
	if _, err := runner.Sync(); err == nil {
		t.Fatal("a failed upload must fail the run, so the heartbeat says so")
	}
	// Newest first, and nothing after the failure was attempted.
	if len(client.uploads) != 0 {
		t.Fatalf("uploads after a failure: %v", client.names())
	}
	client.failOn = ""
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 2 {
		t.Fatalf("the failed file must be retried next run: %v", client.names())
	}
}

func TestSyncSendsTheNewestFilesFirstAndStopsAtTheLimit(t *testing.T) {
	mac := t.TempDir()
	writeCapture(t, mac, "periodic-screenshot-2026-05-20_21-52-22.heic", "old", 150*24*time.Hour)
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_13-59-52-display-1.heic", "newer", 31*time.Minute)
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-00-54-display-1.heic", "newest", 30*time.Minute)
	client := &fakeUploader{}
	runner := newRunner(t, client, Source{Device: "crobat", Dir: mac})
	runner.Limit = 2
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	// A backfill drains behind the live minute instead of in front of it.
	if got := strings.Join(client.names(), ","); got != "crobat/periodic-screenshot-2026-10-22_14-00-54-display-1.heic,crobat/periodic-screenshot-2026-10-22_13-59-52-display-1.heic" {
		t.Fatalf("uploads = %s", got)
	}
	if summary.Remaining != 1 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestSyncFailsOnAMissingSourceFolder(t *testing.T) {
	client := &fakeUploader{}
	_, err := newRunner(t, client, Source{Device: "joltik", Dir: filepath.Join(t.TempDir(), "absent")}).Sync()
	if err == nil {
		t.Fatal("a configured folder that does not exist is a misconfiguration, not an empty source")
	}
}

func TestSyncDefersWhenTheNetworkGuardSaysNo(t *testing.T) {
	mac := t.TempDir()
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-00-54-display-1.heic", "a", 20*time.Minute)
	client := &fakeUploader{}
	runner := newRunner(t, client, Source{Device: "crobat", Dir: mac})
	runner.BeforeUploadCheck = func() string { return "tethered to a phone" }
	summary, err := runner.Sync()
	if err != nil || !summary.Deferred || len(client.uploads) != 0 {
		t.Fatalf("summary = %+v err = %v uploads = %v", summary, err, client.names())
	}
}

func TestParseSources(t *testing.T) {
	got, err := ParseSources("crobat=/Users/z/Pictures/Screenshots/periodic, joltik=/Users/z/Pictures/PhoneScreenshots/joltik")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != (Source{Device: "crobat", Dir: "/Users/z/Pictures/Screenshots/periodic"}) ||
		got[1] != (Source{Device: "joltik", Dir: "/Users/z/Pictures/PhoneScreenshots/joltik"}) {
		t.Fatalf("sources = %+v", got)
	}
	for _, bad := range []string{"crobat", "=/x", "Crobat=/x", "crobat=/x,crobat=/y"} {
		if _, err := ParseSources(bad); err == nil {
			t.Fatalf("ParseSources(%q) should fail", bad)
		}
	}
}
