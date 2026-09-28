package muse

import (
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const workspaceStateSchemaVersion = "1"

// FileProgress is what the last successful upload knew about one path.
type FileProgress struct {
	SizeBytes     int64
	MtimeNS       int64
	ContentSHA256 string
}

// WorkspaceState remembers, per workspace path, the stat and content hash the
// warehouse last received, so an unchanged file is neither hashed nor sent and
// a path that vanished is tombstoned exactly once.
type WorkspaceState struct {
	db *common.StateDB
}

// OpenWorkspaceState opens (or creates) the state file. A different account
// or home wipes it, so one Muse's state never suppresses another's upload.
func OpenWorkspaceState(path, account, home string) (*WorkspaceState, error) {
	db, err := common.OpenStateDB(path, []string{
		`CREATE TABLE IF NOT EXISTS workspace_files (
			path TEXT PRIMARY KEY,
			size_bytes INTEGER NOT NULL,
			mtime_ns INTEGER NOT NULL,
			content_sha256 TEXT NOT NULL,
			uploaded_at TEXT NOT NULL
		)`,
	}, map[string]string{"schema_version": workspaceStateSchemaVersion, "account": account, "home": home}, []string{"workspace_files"})
	if err != nil {
		return nil, err
	}
	return &WorkspaceState{db: db}, nil
}

// Close closes the state file.
func (s *WorkspaceState) Close() error { return s.db.Close() }

// All returns every recorded path.
func (s *WorkspaceState) All() (map[string]FileProgress, error) {
	rows, err := common.Query(s.db.DB, "SELECT path, size_bytes, mtime_ns, content_sha256 FROM workspace_files")
	if err != nil {
		return nil, err
	}
	out := make(map[string]FileProgress, len(rows))
	for _, row := range rows {
		out[row.String("path")] = FileProgress{
			SizeBytes:     row.Int("size_bytes"),
			MtimeNS:       row.Int("mtime_ns"),
			ContentSHA256: row.String("content_sha256"),
		}
	}
	return out, nil
}

// Record stores what the warehouse now holds for path.
func (s *WorkspaceState) Record(path string, size, mtimeNS int64, sha string, at time.Time) error {
	_, err := s.db.DB.Exec(`INSERT INTO workspace_files (path, size_bytes, mtime_ns, content_sha256, uploaded_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET size_bytes = excluded.size_bytes, mtime_ns = excluded.mtime_ns,
			content_sha256 = excluded.content_sha256, uploaded_at = excluded.uploaded_at`,
		path, size, mtimeNS, sha, common.ISOFormat(at))
	return err
}

// Forget drops a path whose tombstone the warehouse has received.
func (s *WorkspaceState) Forget(path string) error {
	_, err := s.db.DB.Exec("DELETE FROM workspace_files WHERE path = ?", path)
	return err
}
