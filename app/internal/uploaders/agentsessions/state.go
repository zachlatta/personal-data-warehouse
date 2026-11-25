package agentsessions

import (
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const stateSchemaVersion = "1"

// FileProgress is how much of one transcript has been shipped.
type FileProgress struct {
	UploadedOffset int64
	UploadedLines  int64
}

// State is the per-file byte/line offsets, in the same SQLite file the
// Python uploader kept so an upgraded host resumes where it was.
type State struct {
	db *common.StateDB
}

// DefaultStateFile is ~/Library/Application Support/personal-data-warehouse/agent-sessions-upload-state.sqlite.
func DefaultStateFile() string {
	return common.DefaultStateFile("agent-sessions-upload-state.sqlite")
}

// OpenState opens (or creates) the state file for account. Works with both the
// 4-column table and databases that already carry extra columns (such as
// last_mtime_ns/last_inode from an earlier release) without touching them.
func OpenState(path, account string) (*State, error) {
	db, err := common.OpenStateDB(path, []string{
		`CREATE TABLE IF NOT EXISTS file_state (
			path TEXT PRIMARY KEY,
			uploaded_offset INTEGER NOT NULL DEFAULT 0,
			uploaded_lines INTEGER NOT NULL DEFAULT 0,
			last_uploaded_at TEXT NOT NULL DEFAULT ''
		)`,
	}, map[string]string{"schema_version": stateSchemaVersion, "account": account}, []string{"file_state"})
	if err != nil {
		return nil, err
	}
	return &State{db: db}, nil
}

// Close releases the database.
func (s *State) Close() error { return s.db.Close() }

// LoadAll reads every persisted file_state row in one query so a restarted
// watcher does not execute thousands of per-file SELECTs on pass 1.
func (s *State) LoadAll() (map[string]FileProgress, error) {
	rows, err := common.Query(s.db.DB,
		"SELECT path, uploaded_offset, uploaded_lines FROM file_state")
	if err != nil {
		return nil, err
	}
	out := make(map[string]FileProgress, len(rows))
	for _, row := range rows {
		out[row.String("path")] = FileProgress{
			UploadedOffset: row.Int("uploaded_offset"),
			UploadedLines:  row.Int("uploaded_lines"),
		}
	}
	return out, nil
}

// RecordProgress commits a file's offset after its lines were durably uploaded.
func (s *State) RecordProgress(path string, offset, lines int64, now time.Time) error {
	_, err := s.db.DB.Exec(`INSERT INTO file_state (path, uploaded_offset, uploaded_lines, last_uploaded_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
			uploaded_offset = excluded.uploaded_offset,
			uploaded_lines = excluded.uploaded_lines,
			last_uploaded_at = excluded.last_uploaded_at`,
		path, offset, lines, common.ISOFormat(now))
	return err
}

// Reset forgets every file.
func (s *State) Reset() error {
	_, err := s.db.DB.Exec("DELETE FROM file_state")
	return err
}
