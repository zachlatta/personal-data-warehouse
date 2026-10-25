package androidscreentime

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

type fakeUploader struct {
	batches [][]map[string]any
}

func (f *fakeUploader) UploadAndroidScreenTimeBatch(body []byte, exportedAt string) (ingestclient.StoredObject, error) {
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

func (f *fakeUploader) all() []map[string]any {
	var out []map[string]any
	for _, b := range f.batches {
		out = append(out, b...)
	}
	return out
}

// fakeExporter answers like the usage exporter: the events and daily rows it
// holds, filtered to the requested range, and records the ranges asked for.
type fakeExporter struct {
	events []string
	daily  []string
	asked  []string
}

func (f *fakeExporter) read(path string, beginMS, endMS int64) (io.ReadCloser, error) {
	f.asked = append(f.asked, fmt.Sprintf("%s %d", path, beginMS))
	var lines []string
	if path == "events" {
		for _, line := range f.events {
			var e map[string]any
			_ = json.Unmarshal([]byte(line), &e)
			ms := int64(e["event_at_ms"].(float64))
			if ms >= beginMS && ms < endMS {
				lines = append(lines, line)
			}
		}
	} else {
		lines = f.daily
	}
	return io.NopCloser(strings.NewReader(strings.Join(lines, "\n") + "\n")), nil
}

func event(ms int64, kind int, name, pkg, class string) string {
	return fmt.Sprintf(`{"record_type":"usage_event","event_at_ms":%d,"event_type":%d,"event_type_name":%q,"package_name":%q,"class_name":%q,"exporter_version":1}`, ms, kind, name, pkg, class)
}

func appDay(pkg string, startMS, foregroundMS int64) string {
	return fmt.Sprintf(`{"record_type":"daily_app_usage","package_name":%q,"app_label":"App","bucket_start_ms":%d,"bucket_end_ms":%d,"total_time_in_foreground_ms":%d,"time_zone":"America/New_York","exporter_version":1}`, pkg, startMS, startMS+1000, foregroundMS)
}

func runner(t *testing.T, exp *fakeExporter, up *fakeUploader, statePath string, now time.Time) *Runner {
	t.Helper()
	return &Runner{
		Read:      exp.read,
		StatePath: statePath,
		Account:   "zach@example.com",
		Device:    "pixel",
		Client:    up,
		Logger:    common.NewWriterLogger(io.Discard),
		Now:       func() time.Time { return now },
	}
}

func TestSyncSendsEventsOnceAndOnlyChangedDailyTotals(t *testing.T) {
	now := time.UnixMilli(1_792_882_000_000).UTC()
	base := now.Add(-time.Hour).UnixMilli()
	exp := &fakeExporter{
		events: []string{
			event(base, 15, "SCREEN_INTERACTIVE", "android", ""),
			event(base+10, 1, "ACTIVITY_RESUMED", "com.example.app", "com.example.app.Main"),
			// Two identical events in one millisecond get different keys.
			event(base+10, 1, "ACTIVITY_RESUMED", "com.example.app", "com.example.app.Main"),
		},
		daily: []string{appDay("com.example.app", base-1000, 5000), appDay("com.example.other", base-1000, 0)},
	}
	up := &fakeUploader{}
	statePath := filepath.Join(t.TempDir(), "state.json")
	summary, err := runner(t, exp, up, statePath, now).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.EventsSent != 3 || summary.DailySent != 2 {
		t.Fatalf("first run summary = %+v", summary)
	}
	sent := up.all()
	if sent[0]["source"] != "android_screen_time" || sent[0]["device"] != "pixel" || sent[0]["account"] != "zach@example.com" {
		t.Fatalf("envelope = %v", sent[0])
	}
	keys := map[string]bool{}
	for _, env := range sent[:3] {
		rec := env["record"].(map[string]any)
		keys[rec["event_key"].(string)] = true
	}
	if len(keys) != 3 {
		t.Fatalf("event keys should be distinct, got %v", keys)
	}
	if exp.asked[0] != "events 0" {
		t.Fatalf("a first run reads everything Android holds, asked %v", exp.asked)
	}

	// Second run: one new event, one app's total changed.
	exp.events = append(exp.events, event(base+20, 2, "ACTIVITY_PAUSED", "com.example.app", "com.example.app.Main"))
	exp.daily[0] = appDay("com.example.app", base-1000, 9000)
	up2 := &fakeUploader{}
	summary, err = runner(t, exp, up2, statePath, now.Add(time.Minute)).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.DailySent != 1 || summary.DailyUnchanged != 1 {
		t.Fatalf("second run should send only the changed total: %+v", summary)
	}
	wantBegin := fmt.Sprintf("events %d", base+10-eventOverlap.Milliseconds())
	if exp.asked[2] != wantBegin {
		t.Fatalf("second run should re-read from the overlap: asked %v, want %q", exp.asked, wantBegin)
	}
	// The re-read events keep the keys they had.
	resent := map[string]bool{}
	for _, env := range up2.all() {
		rec := env["record"].(map[string]any)
		if k, ok := rec["event_key"].(string); ok {
			resent[k] = true
		}
	}
	for k := range keys {
		if !resent[k] {
			t.Fatalf("event key %s changed between runs", k)
		}
	}
	if summary.EventsSent != 4 {
		t.Fatalf("second run events = %d, want the 3 overlapped plus 1 new", summary.EventsSent)
	}
}

func TestDailyKeySeparatesAppsDeviceTotalsAndBuckets(t *testing.T) {
	a := DailyKey(map[string]any{"record_type": "daily_app_usage", "package_name": "x", "bucket_start_ms": float64(1)})
	b := DailyKey(map[string]any{"record_type": "daily_app_usage", "package_name": "x", "bucket_start_ms": float64(2)})
	c := DailyKey(map[string]any{"record_type": "daily_device_usage", "event_type": float64(15), "bucket_start_ms": float64(1)})
	d := DailyKey(map[string]any{"record_type": "daily_device_usage", "event_type": float64(18), "bucket_start_ms": float64(1)})
	if a == b || a == c || c == d {
		t.Fatalf("keys collide: %s %s %s %s", a, b, c, d)
	}
}

func TestDryRunRecordsNothing(t *testing.T) {
	now := time.UnixMilli(1_792_882_000_000).UTC()
	exp := &fakeExporter{events: []string{event(now.Add(-time.Minute).UnixMilli(), 15, "SCREEN_INTERACTIVE", "android", "")}}
	statePath := filepath.Join(t.TempDir(), "state.json")
	r := runner(t, exp, &fakeUploader{}, statePath, now)
	r.DryRun = true
	if _, err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	st, err := loadState(statePath)
	if err != nil || st.EventsCursorMS != 0 {
		t.Fatalf("dry run wrote state: %+v %v", st, err)
	}
}
