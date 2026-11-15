package muse

import (
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/agentsessions"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const workspaceStateSchemaVersion = "1"

// manifestCacheRuleVersion names the rule the cached manifest counts were
// made by (agentsessions.MuseManifest: line parsing and event keys). Bump it
// when that rule changes: opening the state then drops manifest_files alone,
// so every transcript is recounted and workspace_files is kept.
var manifestCacheRuleVersion = "1"

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
		// The session manifest's per-transcript counts, keyed by stat.
		// Added beside workspace_files without a schema_version bump: a
		// bump would wipe workspace_files and re-send the whole workspace.
		`CREATE TABLE IF NOT EXISTS manifest_files (
			path TEXT PRIMARY KEY,
			size_bytes INTEGER NOT NULL,
			mtime_ns INTEGER NOT NULL,
			event_count INTEGER NOT NULL,
			last_timestamp TEXT NOT NULL
		)`,
	}, map[string]string{"schema_version": workspaceStateSchemaVersion, "account": account, "home": home}, []string{"workspace_files", "manifest_files"})
	if err != nil {
		return nil, err
	}
	if err := resetManifestCacheOnNewRule(db); err != nil {
		db.Close()
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

// ManifestCache is the session manifest's per-transcript cache in this state
// file (agentsessions.MuseManifestCache).
func (s *WorkspaceState) ManifestCache() agentsessions.MuseManifestCache {
	return manifestCache{s.db}
}

// resetManifestCacheOnNewRule empties manifest_files when it was filled under
// another manifestCacheRuleVersion (or none, as before the version existed).
func resetManifestCacheOnNewRule(db *common.StateDB) error {
	rows, err := common.Query(db.DB, "SELECT value FROM metadata WHERE key = 'manifest_rule_version'")
	if err != nil {
		return err
	}
	if len(rows) == 1 && rows[0].String("value") == manifestCacheRuleVersion {
		return nil
	}
	if _, err := db.DB.Exec("DELETE FROM manifest_files"); err != nil {
		return err
	}
	_, err = db.DB.Exec(`INSERT INTO metadata (key, value) VALUES ('manifest_rule_version', ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, manifestCacheRuleVersion)
	return err
}

type manifestCache struct{ db *common.StateDB }

func (c manifestCache) Lookup(path string, size, mtimeNS int64) (agentsessions.MuseManifestCounts, bool) {
	rows, err := common.Query(c.db.DB, "SELECT event_count, last_timestamp FROM manifest_files WHERE path = ? AND size_bytes = ? AND mtime_ns = ?", path, size, mtimeNS)
	if err != nil || len(rows) == 0 {
		return agentsessions.MuseManifestCounts{}, false
	}
	return agentsessions.MuseManifestCounts{EventCount: rows[0].Int("event_count"), LastTimestamp: rows[0].String("last_timestamp")}, true
}

func (c manifestCache) Store(path string, size, mtimeNS int64, counts agentsessions.MuseManifestCounts) error {
	_, err := c.db.DB.Exec(`INSERT INTO manifest_files (path, size_bytes, mtime_ns, event_count, last_timestamp)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET size_bytes = excluded.size_bytes, mtime_ns = excluded.mtime_ns,
			event_count = excluded.event_count, last_timestamp = excluded.last_timestamp`,
		path, size, mtimeNS, counts.EventCount, counts.LastTimestamp)
	return err
}

func (c manifestCache) Retain(keep map[string]bool) error {
	rows, err := common.Query(c.db.DB, "SELECT path FROM manifest_files")
	if err != nil {
		return err
	}
	for _, row := range rows {
		if path := row.String("path"); !keep[path] {
			if _, err := c.db.DB.Exec("DELETE FROM manifest_files WHERE path = ?", path); err != nil {
				return err
			}
		}
	}
	return nil
}
