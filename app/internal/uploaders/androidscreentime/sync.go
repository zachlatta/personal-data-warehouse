// Package androidscreentime uploads what Android records about how a phone
// is used: its screen time.
//
// Android's usage stats service keeps, for about ten days, an event for every
// app coming to the foreground or leaving it, the screen turning on or off,
// the phone locking and unlocking, notifications interrupting, shutdowns and
// startups; and daily totals per app (time in the foreground, time visible)
// and for the device (time with the screen on, time unlocked), later rolled
// into weekly, monthly and yearly buckets. Digital Wellbeing's screen time is
// computed from the same service.
//
// The PDW usage exporter (ops/android/pdw-usage-exporter, an APK with usage
// access) streams both as JSON lines through a content provider. This
// uploader runs as root on the phone, reads them with `content read`, and
// sends them as gzipped JSONL batches to /ingest/android-screen-time/batch.
//
// Events carry no id of their own, so each gets a key from its time, type,
// package and class, plus its position among identical events in the same
// millisecond. A state file keeps the newest event time sent; each run reads
// from ten minutes before it (the warehouse upserts by key, so the overlap
// only re-sends), and a new phone or a lost state file starts from whatever
// Android still holds. Daily totals for today change all day: the state keeps
// a hash per bucket and sends only buckets that changed.
//
// Every row is keyed by (account, device), so any number of Android devices
// can send into the same tables.
package androidscreentime

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Authority is the usage exporter's content provider.
const Authority = "com.zachlatta.pdw.usage"

// DefaultBatchSize is records per batch. An event is ~300 bytes of JSON.
const DefaultBatchSize = 2000

// eventOverlap is how far before the newest event sent each run reads again.
const eventOverlap = 10 * time.Minute

// dailyLookback is how far back each run reads daily totals; Android keeps
// daily buckets for about ten days.
const dailyLookback = 12 * 24 * time.Hour

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadAndroidScreenTimeBatch(gzipBytes []byte, exportedAt string) (ingestclient.StoredObject, error)
}

// Reader returns the exporter's JSON lines for a path ("events" or "daily")
// and a [begin, end) range in epoch milliseconds.
type Reader func(path string, beginMS, endMS int64) (io.ReadCloser, error)

// ContentReader reads the exporter with Android's `content read` command.
func ContentReader(path string, beginMS, endMS int64) (io.ReadCloser, error) {
	uri := fmt.Sprintf("content://%s/%s?begin=%d&end=%d", Authority, path, beginMS, endMS)
	var stderr bytes.Buffer
	cmd := exec.Command("content", "read", "--uri", uri)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("content read %s: %w: %s", uri, err, bytes.TrimSpace(stderr.Bytes()))
	}
	// `content read` prints a provider error on stderr and still exits 0.
	if msg := bytes.TrimSpace(stderr.Bytes()); len(msg) > 0 {
		return nil, fmt.Errorf("content read %s: %s", uri, msg)
	}
	return io.NopCloser(bytes.NewReader(out)), nil
}

// Summary is one run's counts.
type Summary struct {
	EventsRead     int
	EventsSent     int
	DailyRead      int
	DailySent      int
	DailyUnchanged int
	Batches        int
	Deferred       bool
}

// Runner sends one run.
type Runner struct {
	Read              Reader
	StatePath         string
	Account           string
	Device            string
	Client            Uploader
	Logger            common.Logger
	Now               func() time.Time
	BatchSize         int
	DryRun            bool
	BeforeUploadCheck func() string
}

type state struct {
	// EventsCursorMS is the newest event time sent.
	EventsCursorMS int64 `json:"events_cursor_ms"`
	// Daily is a content hash per daily bucket sent, by its key.
	Daily map[string]dailySent `json:"daily"`
}

type dailySent struct {
	Hash          string `json:"hash"`
	BucketStartMS int64  `json:"bucket_start_ms"`
}

// Sync reads the exporter and sends what is new.
func (r *Runner) Sync() (Summary, error) {
	if r.Client == nil {
		return Summary{}, errors.New("ingest client is required")
	}
	if r.Read == nil {
		r.Read = ContentReader
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
	if r.BatchSize <= 0 {
		r.BatchSize = DefaultBatchSize
	}
	var summary Summary
	st, err := loadState(r.StatePath)
	if err != nil {
		return summary, err
	}
	now := r.Now()
	endMS := now.UnixMilli()

	beginMS := int64(0)
	if st.EventsCursorMS > 0 {
		beginMS = st.EventsCursorMS - eventOverlap.Milliseconds()
	}
	events, err := r.readJSONL("events", beginMS, endMS)
	if err != nil {
		return summary, err
	}
	KeyEvents(events)
	summary.EventsRead = len(events)
	newestMS := st.EventsCursorMS
	for _, e := range events {
		if ms := int64Value(e["event_at_ms"]); ms > newestMS {
			newestMS = ms
		}
	}

	daily, err := r.readJSONL("daily", now.Add(-dailyLookback).UnixMilli(), endMS)
	if err != nil {
		return summary, err
	}
	summary.DailyRead = len(daily)
	seen := map[string]dailySent{}
	var changed []map[string]any
	for _, d := range daily {
		key := DailyKey(d)
		d["usage_key"] = key
		hash := contentHash(d)
		seen[key] = dailySent{Hash: hash, BucketStartMS: int64Value(d["bucket_start_ms"])}
		if st.Daily[key].Hash == hash {
			summary.DailyUnchanged++
			continue
		}
		changed = append(changed, d)
	}

	records := make([]map[string]any, 0, len(events)+len(changed))
	records = append(records, events...)
	records = append(records, changed...)
	if len(records) > 0 && r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			r.Logger.Warningf("Skipping Android screen time upload: %s", reason)
			summary.Deferred = true
			return summary, nil
		}
	}
	for start := 0; start < len(records); start += r.BatchSize {
		end := min(start+r.BatchSize, len(records))
		exportedAt := r.Now()
		envelopes := make([]map[string]any, 0, end-start)
		for _, rec := range records[start:end] {
			envelopes = append(envelopes, Envelope(r.Account, exportedAt, r.Device, rec))
		}
		body, err := common.GzipJSONL(envelopes)
		if err != nil {
			return summary, err
		}
		stored, err := r.Client.UploadAndroidScreenTimeBatch(body, common.ISOFormat(exportedAt))
		if err != nil {
			return summary, err
		}
		summary.Batches++
		for _, rec := range records[start:end] {
			if rec["record_type"] == "usage_event" {
				summary.EventsSent++
			} else {
				summary.DailySent++
			}
		}
		r.Logger.Infof("Uploaded Android screen time batch %s with %d records", stored.StorageKey, end-start)
	}
	if r.DryRun {
		return summary, nil
	}
	// Keep hashes for buckets still inside the lookback, so a bucket that
	// left Android's daily window is forgotten instead of growing the file.
	cutoff := now.Add(-dailyLookback - 24*time.Hour).UnixMilli()
	for key, sent := range st.Daily {
		if _, ok := seen[key]; !ok && sent.BucketStartMS >= cutoff {
			seen[key] = sent
		}
	}
	st.Daily = seen
	st.EventsCursorMS = newestMS
	return summary, writeState(r.StatePath, st)
}

func (r *Runner) readJSONL(path string, beginMS, endMS int64) ([]map[string]any, error) {
	rc, err := r.Read(path, beginMS, endMS)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	var out []map[string]any
	scanner := bufio.NewScanner(rc)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		// Epoch milliseconds fit a float64 exactly (below 2^53).
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("exporter %s: bad line %q: %w", path, truncate(line, 200), err)
		}
		out = append(out, rec)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("exporter %s: %w", path, err)
	}
	return out, nil
}

// KeyEvents sets each event's event_key: a hash of its time, type, package,
// class and position among identical events in the same millisecond. A run
// reads every event at or after its start time, so a millisecond's events
// are always read together and the positions are stable across runs.
func KeyEvents(events []map[string]any) {
	sort.SliceStable(events, func(i, j int) bool {
		return int64Value(events[i]["event_at_ms"]) < int64Value(events[j]["event_at_ms"])
	})
	counts := map[string]int{}
	for _, e := range events {
		base := fmt.Sprintf("%d|%d|%s|%s", int64Value(e["event_at_ms"]), int64Value(e["event_type"]), str(e["package_name"]), str(e["class_name"]))
		n := counts[base]
		counts[base] = n + 1
		e["event_key"] = shortHash(base + "|" + strconv.Itoa(n))
	}
}

// DailyKey names a daily bucket: an app's or a device-level event type's
// total for the bucket that starts at bucket_start_ms.
func DailyKey(d map[string]any) string {
	what := str(d["package_name"])
	if str(d["record_type"]) == "daily_device_usage" {
		what = "event:" + strconv.FormatInt(int64Value(d["event_type"]), 10)
	}
	return shortHash(fmt.Sprintf("%s|%s|%d", str(d["record_type"]), what, int64Value(d["bucket_start_ms"])))
}

// Envelope wraps one exporter record for the batch.
func Envelope(account string, exportedAt time.Time, device string, record map[string]any) map[string]any {
	return map[string]any{
		"schema_version": int64(1),
		"source":         "android_screen_time",
		"account":        account,
		"device":         device,
		"exported_at":    common.ISOFormat(exportedAt),
		"record_type":    str(record["record_type"]),
		"record":         record,
	}
}

func contentHash(d map[string]any) string {
	raw, _ := json.Marshal(d)
	return shortHash(string(raw))
}

func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:16])
}

func int64Value(v any) int64 {
	switch x := v.(type) {
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			f, _ := x.Float64()
			return int64(f)
		}
		return n
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	}
	return 0
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}

func loadState(path string) (state, error) {
	st := state{Daily: map[string]dailySent{}}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		return st, fmt.Errorf("state file %s: %w", path, err)
	}
	if st.Daily == nil {
		st.Daily = map[string]dailySent{}
	}
	return st, nil
}

func writeState(path string, st state) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return common.WriteFileAtomic(path, raw, 0o600)
}
