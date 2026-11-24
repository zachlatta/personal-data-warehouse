package agentsessions

import (
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const stateSchemaVersion = "1"

// FileProgress is how much of one transcript has been shipped, plus its mtime
// and inode as of that read (zero in a row that predates them).
type FileProgress struct {
	UploadedOffset int64
	UploadedLines  int64
	LastMtimeNs    int64
	LastInode      uint64
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

// OpenState opens (or creates) the state file for account.
func OpenState(path, account string) (*State, error) {
	db, err := common.OpenStateDB(path, []string{
		`CREATE TABLE IF NOT EXISTS file_state (
			path TEXT PRIMARY KEY,
			uploaded_offset INTEGER NOT NULL DEFAULT 0,
			uploaded_lines INTEGER NOT NULL DEFAULT 0,
			last_uploaded_at TEXT NOT NULL DEFAULT '',
			last_mtime_ns INTEGER NOT NULL DEFAULT 0,
			last_inode INTEGER NOT NULL DEFAULT 0
		)`,
	}, map[string]string{"schema_version": stateSchemaVersion, "account": account}, []string{"file_state"})
	if err != nil {
		return nil, err
	}
	// Added 2026-11-24, in place: a schema_version bump would wipe the offsets
	// and re-ship every transcript.
	cols, err := db.ColumnsOf("file_state")
	if err != nil {
		db.Close()
		return nil, err
	}
	for _, alter := range []struct {
		col string
		ddl string
	}{
		{"last_mtime_ns", `ALTER TABLE file_state ADD COLUMN last_mtime_ns INTEGER NOT NULL DEFAULT 0`},
		{"last_inode", `ALTER TABLE file_state ADD COLUMN last_inode INTEGER NOT NULL DEFAULT 0`},
	} {
		if !cols[alter.col] {
			if _, err := db.DB.Exec(alter.ddl); err != nil {
				db.Close()
				return nil, err
			}
		}
	}
	return &State{db: db}, nil
}

// Close releases the database.
func (s *State) Close() error { return s.db.Close() }

// LoadAll reads every persisted file_state row in one query so a restarted
// watcher does not execute thousands of per-file SELECTs on pass 1.
func (s *State) LoadAll() (map[string]FileProgress, error) {
	rows, err := common.Query(s.db.DB,
		"SELECT path, uploaded_offset, uploaded_lines, last_mtime_ns, last_inode FROM file_state")
	if err != nil {
		return nil, err
	}
	out := make(map[string]FileProgress, len(rows))
	for _, row := range rows {
		out[row.String("path")] = FileProgress{
			UploadedOffset: row.Int("uploaded_offset"),
			UploadedLines:  row.Int("uploaded_lines"),
			LastMtimeNs:    row.Int("last_mtime_ns"),
			LastInode:      uint64(row.Int("last_inode")),
		}
	}
	return out, nil
}

// RecordProgress commits a file's offset after its lines were durably uploaded,
// with the signature taken before they were read.
func (s *State) RecordProgress(path string, offset, lines int64, sig fileSignature, now time.Time) error {
	_, err := s.db.DB.Exec(`INSERT INTO file_state (path, uploaded_offset, uploaded_lines, last_uploaded_at, last_mtime_ns, last_inode)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
			uploaded_offset = excluded.uploaded_offset,
			uploaded_lines = excluded.uploaded_lines,
			last_uploaded_at = excluded.last_uploaded_at,
			last_mtime_ns = excluded.last_mtime_ns,
			last_inode = excluded.last_inode`,
		path, offset, lines, common.ISOFormat(now), sig.modTime, int64(sig.inode))
	return err
}

// BackfillSignatures persists signatures for already-uploaded rows created
// before signature tracking was added, in a single transaction.
func (s *State) BackfillSignatures(sigs map[string]fileSignature) error {
	tx, err := s.db.DB.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`UPDATE file_state SET last_mtime_ns = ?, last_inode = ? WHERE path = ?`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()
	for path, sig := range sigs {
		if _, err := stmt.Exec(sig.modTime, int64(sig.inode), path); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// Reset forgets every file.
func (s *State) Reset() error {
	_, err := s.db.DB.Exec("DELETE FROM file_state")
	return err
}
