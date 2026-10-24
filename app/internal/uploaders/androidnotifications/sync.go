// Package androidnotifications uploads the notifications the phone receives.
//
// The PDW notification listener (ops/android/pdw-notification-listener, an
// APK with notification access) writes one JSON file per event into its
// private files/spool/ directory: a notification posted or visibly updated,
// one removed (with Android's reason), and the ones already showing when it
// connected. This uploader runs as root on the phone, sends the spool as
// gzipped JSONL batches to /ingest/android-notifications/batch, and deletes
// each file once its batch is stored. The spool is the ledger: a file that is
// still there has not been sent, and the warehouse upserts by the listener's
// event id, so a batch re-sent after a crash never duplicates.
//
// A second spool, written by `pdw ingest android-notifications
// tail-system-log` (systemlog.go), holds what the listener API cannot see:
// Android's own log of what happened to each notification on screen -- shown
// and hidden, expanded, tapped, an action button pressed, whether it buzzed or
// rang -- and the shade, heads-up, screen and lock state around it. The same
// run sends both, merged oldest first.
package androidnotifications

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultSpoolDir is the listener's spool on the phone.
const DefaultSpoolDir = "/data/data/com.zachlatta.pdw.notifications/files/spool"

// DefaultBatchSize is events per batch. An event is ~1-3 KB of JSON, so a
// batch is well under a megabyte gzipped.
const DefaultBatchSize = 500

// rejectedDir holds spool files that are not a listener event, kept for a
// person to look at instead of being sent or silently deleted.
const rejectedDir = "rejected"

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadAndroidNotificationsBatch(gzipBytes []byte, exportedAt string) (ingestclient.StoredObject, error)
}

// Summary is one run's counts.
type Summary struct {
	Pending  int
	Uploaded int
	Batches  int
	Rejected int
	Deferred bool
}

// Runner sends the spool.
type Runner struct {
	SpoolDir string
	// ExtraSpoolDirs are sent with SpoolDir (the system-log spool).
	ExtraSpoolDirs    []string
	Account           string
	Device            string
	Client            Uploader
	Logger            common.Logger
	Now               func() time.Time
	BatchSize         int
	Limit             int
	DryRun            bool
	BeforeUploadCheck func() string
}

// Sync sends every settled spool file, oldest first.
func (r *Runner) Sync() (Summary, error) {
	if r.Client == nil {
		return Summary{}, errors.New("ingest client is required")
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
	if r.BatchSize <= 0 {
		r.BatchSize = DefaultBatchSize
	}
	var names []spoolFile
	for _, dir := range append([]string{r.SpoolDir}, r.ExtraSpoolDirs...) {
		if dir == "" {
			continue
		}
		found, err := spoolFiles(dir)
		if err != nil {
			return Summary{}, err
		}
		for _, name := range found {
			names = append(names, spoolFile{dir: dir, name: name})
		}
	}
	// Every spool names its files by the event's epoch milliseconds first.
	sort.SliceStable(names, func(i, j int) bool { return names[i].name < names[j].name })
	summary := Summary{Pending: len(names)}
	if r.Limit > 0 && len(names) > r.Limit {
		names = names[:r.Limit]
	}
	if len(names) == 0 {
		return summary, nil
	}
	if r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			r.Logger.Warningf("Skipping Android notification upload: %s", reason)
			summary.Deferred = true
			return summary, nil
		}
	}
	for start := 0; start < len(names); start += r.BatchSize {
		end := min(start+r.BatchSize, len(names))
		exportedAt := r.Now()
		var records []map[string]any
		var sent []string
		for _, file := range names[start:end] {
			path := filepath.Join(file.dir, file.name)
			event, err := readEvent(path)
			if err != nil {
				r.Logger.Warningf("Setting aside spool file %s: %v", path, err)
				if !r.DryRun {
					if err := reject(file.dir, file.name); err != nil {
						return summary, err
					}
				}
				summary.Rejected++
				continue
			}
			records = append(records, Envelope(r.Account, exportedAt, r.Device, event))
			sent = append(sent, path)
		}
		if len(records) == 0 {
			continue
		}
		body, err := common.GzipJSONL(records)
		if err != nil {
			return summary, err
		}
		stored, err := r.Client.UploadAndroidNotificationsBatch(body, common.ISOFormat(exportedAt))
		if err != nil {
			return summary, err
		}
		summary.Batches++
		summary.Uploaded += len(records)
		r.Logger.Infof("Uploaded Android notification batch %s with %d events", stored.StorageKey, len(records))
		if r.DryRun {
			continue
		}
		for _, path := range sent {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return summary, fmt.Errorf("remove sent spool file: %w", err)
			}
		}
	}
	return summary, nil
}

// Envelope wraps one listener event for the batch. The event rides
// unchanged; the warehouse maps its fields to columns and keeps the whole of
// it as raw_json.
func Envelope(account string, exportedAt time.Time, device string, event map[string]any) map[string]any {
	return map[string]any{
		"schema_version": int64(1),
		"source":         "android_notifications",
		"account":        account,
		"device":         device,
		"exported_at":    common.ISOFormat(exportedAt),
		"record_type":    "notification_event",
		"record":         event,
	}
}

type spoolFile struct{ dir, name string }

// spoolFiles lists the finished event files, oldest first (their names start
// with the event's epoch milliseconds, zero-padded). The listener writes into
// .tmp/ and renames, so a name ending in .json is always a whole file.
func spoolFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.Type().IsRegular() && strings.HasSuffix(name, ".json") && !strings.HasPrefix(name, ".") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func readEvent(path string) (map[string]any, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		return nil, err
	}
	if id, _ := event["event_id"].(string); id == "" {
		return nil, errors.New("no event_id")
	}
	if kind, _ := event["event_type"].(string); kind == "" {
		return nil, errors.New("no event_type")
	}
	return event, nil
}

func reject(dir, name string) error {
	target := filepath.Join(dir, rejectedDir)
	if err := os.MkdirAll(target, 0o700); err != nil {
		return err
	}
	return os.Rename(filepath.Join(dir, name), filepath.Join(target, name))
}
