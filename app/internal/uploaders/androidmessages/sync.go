// Package androidmessages uploads the texts the phone sends and receives.
//
// Google Messages keeps every SMS, MMS and RCS message in its own SQLite
// database (bugle_db); RCS chats are nowhere else on the phone. This
// uploader runs as root on the phone, reads that database read-only, and
// sends each message received at or after a fixed start time (--since) as
// gzipped JSONL batches to /ingest/android-messages/batch.
//
// The start time is required, not defaulted. The phone also holds Zach's
// whole iPhone history (imported into Messages when he switched), which PDW
// already has from the Mac's chat.db; a message older than --since is never
// read, so that history never leaves the phone a second time.
//
// A small state file remembers the highest message row sent and a content
// hash per recently sent message. Each run reads the new rows plus the last
// few days again (a message's status, read state or parts change after it
// arrives), and sends only the messages whose content changed. The warehouse
// upserts by message key, so a batch re-sent after a crash lands once.
package androidmessages

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultDatabasePath is Google Messages' database on the phone.
const DefaultDatabasePath = "/data/data/com.google.android.apps.messaging/databases/bugle_db"

// DefaultBatchSize is messages per batch: each is well under a kilobyte of
// JSON, so a batch stays small even on cellular.
const DefaultBatchSize = 500

// DefaultRecentWindow is how far back each run re-reads for changes.
const DefaultRecentWindow = 3 * 24 * time.Hour

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadAndroidMessagesBatch(gzipBytes []byte, exportedAt string) (ingestclient.StoredObject, error)
}

// Summary is one run's counts.
type Summary struct {
	Read                int
	Unchanged           int
	Uploaded            int
	Batches             int
	AttachmentsUploaded int
	AttachmentBytes     int64
	Deferred            bool
}

// Runner reads the database and sends what changed.
type Runner struct {
	DatabasePath string
	StatePath    string
	Since        time.Time
	Account      string
	Device       string
	Client       Uploader
	Files        AttachmentUploader
	Logger       common.Logger
	Now          func() time.Time
	BatchSize    int
	RecentWindow time.Duration
	// AttachmentBytesPerRun caps the file bytes one run sends (0: the
	// default); the rest are sent by the next runs.
	AttachmentBytesPerRun int64
	DryRun                bool
	BeforeUploadCheck     func() string
}

type state struct {
	SinceMS int64             `json:"since_ms"`
	MaxID   int64             `json:"max_id"`
	Sent    map[string]string `json:"sent"`
	// Files is each file part sent, by part key, with its stored pointer.
	Files map[string]storedFile `json:"files,omitempty"`
}

// Sync sends every message since r.Since whose content is new or changed.
func (r *Runner) Sync() (Summary, error) {
	if r.Client == nil {
		return Summary{}, errors.New("ingest client is required")
	}
	if r.Since.IsZero() {
		return Summary{}, errors.New("a start time (--since) is required")
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
	if r.BatchSize <= 0 {
		r.BatchSize = DefaultBatchSize
	}
	if r.RecentWindow <= 0 {
		r.RecentWindow = DefaultRecentWindow
	}
	var summary Summary
	// Messages keeps the database open in WAL mode. Opening it read-only
	// while Messages has it open is an ordinary second reader; opening it
	// when its -wal/-shm files are absent would have root create them, owned
	// by root, and lock Messages out of its own database. So wait for
	// Messages to have it open.
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(r.DatabasePath + suffix); err != nil {
			r.Logger.Warningf("Skipping Android messages upload: %s%s is absent (Messages has not opened its database)", r.DatabasePath, suffix)
			summary.Deferred = true
			return summary, nil
		}
	}
	st, err := loadState(r.StatePath)
	if err != nil {
		return summary, err
	}
	sinceMS := r.Since.UnixMilli()
	if st.SinceMS != sinceMS {
		// A new start time is a fresh ledger: everything since it is sent
		// once more and the warehouse upserts it.
		st = state{SinceMS: sinceMS, Sent: map[string]string{}, Files: map[string]storedFile{}}
	}
	recentMS := max(sinceMS, r.Now().Add(-r.RecentWindow).UnixMilli())

	db, err := common.OpenSQLiteReadOnly(r.DatabasePath)
	if err != nil {
		return summary, err
	}
	messages, locations, err := readMessages(db, sinceMS, st.MaxID, recentMS)
	db.Close()
	if err != nil {
		return summary, err
	}
	summary.Read = len(messages)
	// Files first: a message is sent with its files' pointers, so a file
	// sent now changes its message, which is then sent again.
	keptFiles := map[string]storedFile{}
	summary.AttachmentsUploaded, summary.AttachmentBytes, err = r.attachFiles(messages, locations, &st, keptFiles)
	if err != nil {
		return summary, err
	}

	type pending struct {
		message map[string]any
		key     string
		sha     string
		id      int64
	}
	var changed []pending
	seen := map[string]string{}
	for _, message := range messages {
		key := stringOf(message["message_key"])
		sha := common.JSONSHA256(message)
		seen[key] = sha
		if st.Sent[key] == sha {
			summary.Unchanged++
			continue
		}
		changed = append(changed, pending{message: message, key: key, sha: sha, id: intOf(message["bugle_id"])})
	}
	if len(changed) == 0 {
		return summary, r.saveState(st, seen, messages, keptFiles)
	}
	if r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			r.Logger.Warningf("Skipping Android messages upload: %s", reason)
			summary.Deferred = true
			return summary, nil
		}
	}
	for start := 0; start < len(changed); start += r.BatchSize {
		end := min(start+r.BatchSize, len(changed))
		exportedAt := r.Now()
		records := make([]map[string]any, 0, end-start)
		for _, item := range changed[start:end] {
			records = append(records, Envelope(r.Account, exportedAt, r.Device, item.message))
		}
		body, err := common.GzipJSONL(records)
		if err != nil {
			return summary, err
		}
		stored, err := r.Client.UploadAndroidMessagesBatch(body, common.ISOFormat(exportedAt))
		if err != nil {
			return summary, err
		}
		summary.Batches++
		summary.Uploaded += len(records)
		r.Logger.Infof("Uploaded Android messages batch %s with %d messages", stored.StorageKey, len(records))
		if r.DryRun {
			continue
		}
		for _, item := range changed[start:end] {
			st.Sent[item.key] = item.sha
			st.MaxID = max(st.MaxID, item.id)
		}
		if err := writeState(r.StatePath, st); err != nil {
			return summary, err
		}
	}
	if r.DryRun {
		return summary, nil
	}
	return summary, r.saveState(st, seen, messages, keptFiles)
}

// saveState keeps hashes and file pointers only for what this run read
// (older messages are not re-read, so theirs are no longer needed) and
// advances the row cursor past every message read.
func (r *Runner) saveState(st state, seen map[string]string, messages []map[string]any, files map[string]storedFile) error {
	if r.DryRun {
		return nil
	}
	st.Files = files
	kept := map[string]string{}
	for key := range seen {
		if sha, ok := st.Sent[key]; ok {
			kept[key] = sha
		}
	}
	st.Sent = kept
	for _, message := range messages {
		key := stringOf(message["message_key"])
		if st.Sent[key] == seen[key] {
			st.MaxID = max(st.MaxID, intOf(message["bugle_id"]))
		}
	}
	return writeState(r.StatePath, st)
}

// Envelope wraps one message for the batch; the warehouse maps its fields to
// columns and keeps the whole of it as raw_json.
func Envelope(account string, exportedAt time.Time, device string, message map[string]any) map[string]any {
	return map[string]any{
		"schema_version": int64(1),
		"source":         "android_messages",
		"account":        account,
		"device":         device,
		"exported_at":    common.ISOFormat(exportedAt),
		"record_type":    "message",
		"record":         message,
	}
}

func loadState(path string) (state, error) {
	st := state{Sent: map[string]string{}, Files: map[string]storedFile{}}
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
	if st.Sent == nil {
		st.Sent = map[string]string{}
	}
	if st.Files == nil {
		st.Files = map[string]storedFile{}
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
