package applemessages

import (
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// StateEntry is one recorded (source_type, source_id).
type StateEntry struct {
	SourceType    string
	SourceID      string
	Fingerprint   string
	Complete      bool
	ContentSHA256 string
	StorageKey    string
	LastSuccessAt string
	LastFailureAt string
	LastError     string
	LastCheckedAt string
}

// State is the per-record upload state.
type State struct {
	db *common.StateDB
}

// DefaultStateFile is the state path.
func DefaultStateFile() string {
	return common.DefaultStateFile("apple-messages-upload-state.sqlite")
}

// OpenState opens the state for (account, store path).
func OpenState(path, account, storePath string) (*State, error) {
	db, err := common.OpenStateDB(path, []string{
		`CREATE TABLE IF NOT EXISTS upload_state (
			source_type TEXT NOT NULL,
			source_id TEXT NOT NULL,
			fingerprint TEXT NOT NULL DEFAULT '',
			complete INTEGER NOT NULL DEFAULT 0,
			content_sha256 TEXT NOT NULL DEFAULT '',
			storage_key TEXT NOT NULL DEFAULT '',
			last_success_at TEXT NOT NULL DEFAULT '',
			last_failure_at TEXT NOT NULL DEFAULT '',
			last_error TEXT NOT NULL DEFAULT '',
			last_checked_at TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (source_type, source_id)
		)`,
	}, map[string]string{"schema_version": "1", "account": account, "store_path": storePath}, []string{"upload_state"})
	if err != nil {
		return nil, err
	}
	return &State{db: db}, nil
}

// Close releases the database.
func (s *State) Close() error { return s.db.Close() }

// EntryFor returns the recorded entry, or nil.
func (s *State) EntryFor(sourceType, sourceID string) (*StateEntry, error) {
	rows, err := common.Query(s.db.DB, `SELECT source_type, source_id, fingerprint, complete, content_sha256, storage_key,
		last_success_at, last_failure_at, last_error, last_checked_at FROM upload_state WHERE source_type = ? AND source_id = ?`, sourceType, sourceID)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	row := rows[0]
	return &StateEntry{
		SourceType: row.String("source_type"), SourceID: row.String("source_id"), Fingerprint: row.String("fingerprint"),
		Complete: row.Bool("complete"), ContentSHA256: row.String("content_sha256"), StorageKey: row.String("storage_key"),
		LastSuccessAt: row.String("last_success_at"), LastFailureAt: row.String("last_failure_at"),
		LastError: row.String("last_error"), LastCheckedAt: row.String("last_checked_at"),
	}, nil
}

// IsComplete reports whether the record was uploaded with this fingerprint.
func (s *State) IsComplete(sourceType, sourceID, fingerprint string) (bool, error) {
	entry, err := s.EntryFor(sourceType, sourceID)
	if err != nil || entry == nil {
		return false, err
	}
	return entry.Complete && entry.Fingerprint == fingerprint, nil
}

// MarkSuccess records an uploaded record.
func (s *State) MarkSuccess(sourceType, sourceID, fingerprint, contentSHA256, storageKey string, now time.Time) error {
	timestamp := common.ISOFormat(now)
	_, err := s.db.DB.Exec(`INSERT INTO upload_state (
			source_type, source_id, fingerprint, complete, content_sha256, storage_key,
			last_success_at, last_failure_at, last_error, last_checked_at)
		VALUES (?, ?, ?, 1, ?, ?, ?, '', '', ?)
		ON CONFLICT(source_type, source_id) DO UPDATE SET
			fingerprint = excluded.fingerprint, complete = excluded.complete,
			content_sha256 = excluded.content_sha256, storage_key = excluded.storage_key,
			last_success_at = excluded.last_success_at, last_failure_at = '', last_error = '',
			last_checked_at = excluded.last_checked_at`,
		sourceType, sourceID, fingerprint, contentSHA256, storageKey, timestamp, timestamp)
	return err
}

// MarkFailure records a failed attempt.
func (s *State) MarkFailure(sourceType, sourceID, fingerprint, errText string, now time.Time) error {
	existing, err := s.EntryFor(sourceType, sourceID)
	if err != nil {
		return err
	}
	contentSHA, storageKey, lastSuccess := "", "", ""
	if existing != nil {
		contentSHA, storageKey, lastSuccess = existing.ContentSHA256, existing.StorageKey, existing.LastSuccessAt
	}
	timestamp := common.ISOFormat(now)
	_, err = s.db.DB.Exec(`INSERT INTO upload_state (
			source_type, source_id, fingerprint, complete, content_sha256, storage_key,
			last_success_at, last_failure_at, last_error, last_checked_at)
		VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_type, source_id) DO UPDATE SET
			fingerprint = excluded.fingerprint, complete = 0,
			last_failure_at = excluded.last_failure_at, last_error = excluded.last_error,
			last_checked_at = excluded.last_checked_at`,
		sourceType, sourceID, fingerprint, contentSHA, storageKey, lastSuccess, timestamp, errText, timestamp)
	return err
}
