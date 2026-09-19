package photos

import (
	"fmt"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// StateSchemaVersion is the metadata `schema_version` the state file carries.
const StateSchemaVersion = "1"

// SourceTypeAssetFile is the upload_state source_type for a PhotoKit resource.
const SourceTypeAssetFile = "asset_file"

// StateEntry is one file's recorded upload.
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
	// Consecutive failed attempts for this fingerprint, and when the current
	// failing streak started. They drive the runner's retry backoff, so a
	// file PhotoKit will never export cannot hold the whole schedule hostage.
	FailureCount   int64
	FirstFailureAt string
}

// State is the SQLite upload state (photos-upload-state.sqlite), keyed by
// (source_type, source_id) with a stable Photos-metadata fingerprint. The
// metadata table wipes state whenever the account/library/schema changes so
// stale completeness never suppresses uploads against a different library.
type State struct {
	db          *common.StateDB
	Account     string
	LibraryPath string
}

// DefaultStateFile is ~/Library/Application Support/personal-data-warehouse/photos-upload-state.sqlite.
func DefaultStateFile() string {
	return common.DefaultStateFile("photos-upload-state.sqlite")
}

const uploadStateDDL = `CREATE TABLE IF NOT EXISTS upload_state (
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
    failure_count INTEGER NOT NULL DEFAULT 0,
    first_failure_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (source_type, source_id)
)`

// OpenState opens (creating when absent) the state file for one account and
// library, migrating a state file written before the retry columns existed
// in place: bumping the schema version instead would wipe every completed
// row and re-upload the whole library.
func OpenState(path, account, libraryPath string) (*State, error) {
	db, err := common.OpenStateDB(path, []string{uploadStateDDL}, map[string]string{
		"schema_version": StateSchemaVersion,
		"account":        account,
		"library_path":   libraryPath,
	}, []string{"upload_state"})
	if err != nil {
		return nil, err
	}
	present, err := db.ColumnsOf("upload_state")
	if err != nil {
		db.Close()
		return nil, err
	}
	for _, column := range []struct{ name, definition string }{
		{"failure_count", "INTEGER NOT NULL DEFAULT 0"},
		{"first_failure_at", "TEXT NOT NULL DEFAULT ''"},
	} {
		if present[column.name] {
			continue
		}
		if _, err := db.DB.Exec("ALTER TABLE upload_state ADD COLUMN " + column.name + " " + column.definition); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &State{db: db, Account: account, LibraryPath: libraryPath}, nil
}

// Close closes the state file.
func (s *State) Close() error {
	if s == nil {
		return nil
	}
	return s.db.Close()
}

// EntryFor returns the recorded entry for one file.
func (s *State) EntryFor(sourceType, sourceID string) (StateEntry, bool, error) {
	rows, err := common.Query(s.db.DB, `SELECT source_type, source_id, fingerprint, complete, content_sha256, storage_key,
       last_success_at, last_failure_at, last_error, last_checked_at, failure_count, first_failure_at
FROM upload_state WHERE source_type = ? AND source_id = ?`, sourceType, sourceID)
	if err != nil {
		return StateEntry{}, false, err
	}
	if len(rows) == 0 {
		return StateEntry{}, false, nil
	}
	row := rows[0]
	return StateEntry{
		SourceType:     row.String("source_type"),
		SourceID:       row.String("source_id"),
		Fingerprint:    row.String("fingerprint"),
		Complete:       row.Bool("complete"),
		ContentSHA256:  row.String("content_sha256"),
		StorageKey:     row.String("storage_key"),
		LastSuccessAt:  row.String("last_success_at"),
		LastFailureAt:  row.String("last_failure_at"),
		LastError:      row.String("last_error"),
		LastCheckedAt:  row.String("last_checked_at"),
		FailureCount:   row.Int("failure_count"),
		FirstFailureAt: row.String("first_failure_at"),
	}, true, nil
}

// LatestSuccessAt is when this uploader last proved the whole export path
// works; ok is false when nothing has ever succeeded.
func (s *State) LatestSuccessAt() (time.Time, bool, error) {
	rows, err := common.Query(s.db.DB, "SELECT MAX(last_success_at) AS moment FROM upload_state WHERE last_success_at != ''")
	if err != nil {
		return time.Time{}, false, err
	}
	if len(rows) == 0 {
		return time.Time{}, false, nil
	}
	moment := rows[0].String("moment")
	if moment == "" {
		return time.Time{}, false, nil
	}
	parsed, ok := common.TryParseISO(moment)
	return parsed, ok, nil
}

// ClearFailures forgets every failing streak so backed-off files retry
// immediately, returning how many rows it touched.
func (s *State) ClearFailures() (int64, error) {
	result, err := s.db.DB.Exec("UPDATE upload_state SET failure_count = 0, first_failure_at = '' WHERE failure_count != 0")
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, nil
	}
	return affected, nil
}

// IsComplete reports whether the file was uploaded at this fingerprint.
func (s *State) IsComplete(sourceType, sourceID, fingerprint string) (bool, error) {
	entry, ok, err := s.EntryFor(sourceType, sourceID)
	if err != nil || !ok {
		return false, err
	}
	return entry.Complete && entry.Fingerprint == fingerprint, nil
}

// MarkSuccess records a completed upload, clearing any failure streak.
func (s *State) MarkSuccess(sourceType, sourceID, fingerprint string, now time.Time, contentSHA256, storageKey string) error {
	timestamp := common.ISOFormat(now)
	_, err := s.db.DB.Exec(`INSERT INTO upload_state (
    source_type, source_id, fingerprint, complete, content_sha256, storage_key,
    last_success_at, last_failure_at, last_error, last_checked_at, failure_count, first_failure_at
) VALUES (?, ?, ?, 1, ?, ?, ?, '', '', ?, 0, '')
ON CONFLICT(source_type, source_id) DO UPDATE SET
    fingerprint = excluded.fingerprint,
    complete = excluded.complete,
    content_sha256 = excluded.content_sha256,
    storage_key = excluded.storage_key,
    last_success_at = excluded.last_success_at,
    last_failure_at = '',
    last_error = '',
    last_checked_at = excluded.last_checked_at,
    failure_count = 0,
    first_failure_at = ''`, sourceType, sourceID, fingerprint, contentSHA256, storageKey, timestamp, timestamp)
	return err
}

// MarkFailure records one failed attempt and returns the updated entry.
//
// Attempts accumulate only while the fingerprint is unchanged: a re-edited
// asset is a different file to upload and starts a fresh streak.
func (s *State) MarkFailure(sourceType, sourceID, fingerprint, errText string, now time.Time) (StateEntry, error) {
	timestamp := common.ISOFormat(now)
	existing, exists, err := s.EntryFor(sourceType, sourceID)
	if err != nil {
		return StateEntry{}, err
	}
	sameFile := exists && existing.Fingerprint == fingerprint
	failureCount := int64(1)
	firstFailureAt := timestamp
	if sameFile {
		failureCount = existing.FailureCount + 1
		if existing.FirstFailureAt != "" {
			firstFailureAt = existing.FirstFailureAt
		}
	}
	contentSHA, storageKey, lastSuccess := "", "", ""
	if exists {
		contentSHA, storageKey, lastSuccess = existing.ContentSHA256, existing.StorageKey, existing.LastSuccessAt
	}
	if _, err := s.db.DB.Exec(`INSERT INTO upload_state (
    source_type, source_id, fingerprint, complete, content_sha256, storage_key,
    last_success_at, last_failure_at, last_error, last_checked_at, failure_count, first_failure_at
) VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(source_type, source_id) DO UPDATE SET
    fingerprint = excluded.fingerprint,
    complete = 0,
    last_failure_at = excluded.last_failure_at,
    last_error = excluded.last_error,
    last_checked_at = excluded.last_checked_at,
    failure_count = excluded.failure_count,
    first_failure_at = excluded.first_failure_at`,
		sourceType, sourceID, fingerprint, contentSHA, storageKey, lastSuccess,
		timestamp, errText, timestamp, failureCount, firstFailureAt); err != nil {
		return StateEntry{}, err
	}
	entry, ok, err := s.EntryFor(sourceType, sourceID)
	if err != nil {
		return StateEntry{}, err
	}
	if !ok {
		return StateEntry{}, fmt.Errorf("upload_state row for %s/%s vanished after write", sourceType, sourceID)
	}
	return entry, nil
}
