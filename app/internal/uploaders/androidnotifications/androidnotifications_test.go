package androidnotifications

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

type fakeUploader struct {
	batches [][]map[string]any
	fail    error
}

func (f *fakeUploader) UploadAndroidNotificationsBatch(body []byte, exportedAt string) (ingestclient.StoredObject, error) {
	if f.fail != nil {
		return ingestclient.StoredObject{}, f.fail
	}
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return ingestclient.StoredObject{}, err
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return ingestclient.StoredObject{}, err
	}
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return ingestclient.StoredObject{}, err
		}
		records = append(records, record)
	}
	f.batches = append(f.batches, records)
	return ingestclient.StoredObject{StorageKey: fmt.Sprintf("batch-%d", len(f.batches))}, nil
}

// spool lays out a listener spool: finished events, a half-written one in
// .tmp/ that must be left alone, and the names the listener gives files.
func spool(t *testing.T, events ...map[string]any) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".tmp"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".tmp", "1792863299999-partial.json"), []byte(`{"event_id":`), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		writeEvent(t, dir, event)
	}
	return dir
}

func writeEvent(t *testing.T, dir string, event map[string]any) {
	t.Helper()
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("%013d-%s.json", int64(event["event_at_ms"].(int)), event["event_id"])
	if err := os.WriteFile(filepath.Join(dir, name), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func event(id string, atMS int, kind string) map[string]any {
	return map[string]any{
		"event_id": id, "event_type": kind, "event_at_ms": atMS,
		"notification_key": "0|com.example.chat|7|null|10123", "package_name": "com.example.chat",
		"title": "Alex Example", "text": "lunch?",
	}
}

func runner(dir string, client Uploader) *Runner {
	return &Runner{
		SpoolDir: dir, Account: "zach@example.test", Device: "pichu", Client: client,
		Logger: &common.RecordingLogger{},
		Now:    func() time.Time { return time.Date(2026, 10, 24, 17, 40, 0, 0, time.UTC) },
	}
}

func remaining(t *testing.T, dir string) []string {
	t.Helper()
	names, err := spoolFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func TestSyncSendsTheSpoolOldestFirstAndDeletesWhatWasStored(t *testing.T) {
	dir := spool(t, event("b", 1792863236900, "removed"), event("a", 1792863236787, "posted"))
	client := &fakeUploader{}

	summary, err := runner(dir, client).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Uploaded != 2 || summary.Batches != 1 || summary.Pending != 2 {
		t.Fatalf("summary = %+v", summary)
	}
	got := client.batches[0]
	if got[0]["record"].(map[string]any)["event_id"] != "a" || got[1]["record"].(map[string]any)["event_id"] != "b" {
		t.Fatalf("events not oldest first: %v", got)
	}
	envelope := got[0]
	if envelope["source"] != "android_notifications" || envelope["record_type"] != "notification_event" ||
		envelope["device"] != "pichu" || envelope["account"] != "zach@example.test" ||
		envelope["exported_at"] != "2026-10-24T17:40:00+00:00" {
		t.Fatalf("envelope = %v", envelope)
	}
	if record := envelope["record"].(map[string]any); record["text"] != "lunch?" {
		t.Fatalf("the listener's event must ride unchanged: %v", record)
	}
	if left := remaining(t, dir); len(left) != 0 {
		t.Fatalf("sent files left in the spool: %v", left)
	}
	if _, err := os.Stat(filepath.Join(dir, ".tmp", "1792863299999-partial.json")); err != nil {
		t.Fatalf("a file still being written must not be touched: %v", err)
	}
}

func TestSyncBatchesAndHonoursTheLimit(t *testing.T) {
	var events []map[string]any
	for i := range 5 {
		events = append(events, event(fmt.Sprintf("e%d", i), 1792863236000+i, "posted"))
	}
	dir := spool(t, events...)
	client := &fakeUploader{}
	r := runner(dir, client)
	r.BatchSize = 2
	r.Limit = 3

	summary, err := r.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Batches != 2 || summary.Uploaded != 3 || summary.Pending != 5 {
		t.Fatalf("summary = %+v", summary)
	}
	if left := remaining(t, dir); len(left) != 2 || !strings.Contains(left[0], "e3") {
		t.Fatalf("the newest two should wait for the next run: %v", left)
	}
}

func TestAFailedUploadKeepsTheSpool(t *testing.T) {
	dir := spool(t, event("a", 1792863236787, "posted"))
	_, err := runner(dir, &fakeUploader{fail: errors.New("502")}).Sync()
	if err == nil {
		t.Fatal("want the upload error")
	}
	if left := remaining(t, dir); len(left) != 1 {
		t.Fatalf("an unsent event must stay in the spool: %v", left)
	}
}

func TestAFileThatIsNotAnEventIsSetAside(t *testing.T) {
	dir := spool(t, event("a", 1792863236787, "posted"))
	if err := os.WriteFile(filepath.Join(dir, "1792863236000-junk.json"), []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &fakeUploader{}
	summary, err := runner(dir, client).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Rejected != 1 || summary.Uploaded != 1 {
		t.Fatalf("summary = %+v", summary)
	}
	if _, err := os.Stat(filepath.Join(dir, rejectedDir, "1792863236000-junk.json")); err != nil {
		t.Fatalf("the bad file should be in rejected/: %v", err)
	}
}

func TestDryRunAndDeferredSendNothingAndDeleteNothing(t *testing.T) {
	dir := spool(t, event("a", 1792863236787, "posted"))
	r := runner(dir, &fakeUploader{})
	r.DryRun = true
	if _, err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	if left := remaining(t, dir); len(left) != 1 {
		t.Fatalf("a dry run deleted the spool: %v", left)
	}

	client := &fakeUploader{}
	r = runner(dir, client)
	r.BeforeUploadCheck = func() string { return "app unreachable" }
	summary, err := r.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if !summary.Deferred || len(client.batches) != 0 || len(remaining(t, dir)) != 1 {
		t.Fatalf("a deferred run must send nothing: %+v", summary)
	}
}

func TestAMissingSpoolIsAnEmptyRun(t *testing.T) {
	summary, err := runner(filepath.Join(t.TempDir(), "absent"), &fakeUploader{}).Sync()
	if err != nil || summary.Pending != 0 {
		t.Fatalf("summary = %+v, err = %v", summary, err)
	}
}

func TestRunRequiresAnAccount(t *testing.T) {
	var out, errOut bytes.Buffer
	env := func(key string) string {
		if key == "HOME" {
			return t.TempDir()
		}
		return ""
	}
	code := Run([]string{"--spool-dir", t.TempDir(), "--dry-run"}, nil, &out, &errOut, env, ingestclient.Config{})
	if code == 0 || !strings.Contains(errOut.String(), "ANDROID_NOTIFICATIONS_ACCOUNT") {
		t.Fatalf("code = %d, stderr = %q", code, errOut.String())
	}
}

func TestRunDryRunReportsWhatItWouldSend(t *testing.T) {
	dir := spool(t, event("a", 1792863236787, "posted"))
	var out, errOut bytes.Buffer
	code := Run([]string{"--spool-dir", dir, "--account", "zach@example.test", "--device", "pichu", "--dry-run",
		"--lock-file", filepath.Join(t.TempDir(), "run.lock")}, nil, &out, &errOut, func(string) string { return "" }, ingestclient.Config{})
	if code != 0 {
		t.Fatalf("code = %d, stderr = %q", code, errOut.String())
	}
	if !strings.Contains(out.String(), "pending=1 events=1 batches=1") || len(remaining(t, dir)) != 1 {
		t.Fatalf("stdout = %q", out.String())
	}
}
