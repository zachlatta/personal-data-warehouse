package androidphotos

import (
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// State is the ledger of landed files: per relative path (the native id),
// the bytes the app acknowledged and the file stat they were hashed at, so
// an unchanged file is never read again. The app dedups each claim on
// (source, account, native id, role, sha), so a lost ledger costs a resend,
// never a duplicate row.
type State struct {
	db *common.StateDB
}

type stateEntry struct {
	SizeBytes     int64
	ModifiedNS    int64
	ContentSHA256 string
	// MotionChecked is set once the file's motion-photo video (if any) has
	// landed too. Rows written before videos were sent have it unset, so
	// those files send their video alone, never the still again.
	MotionChecked bool
	MotionSHA256  string
}

// DefaultStateFile is the ledger path.
func DefaultStateFile() string {
	return common.DefaultStateFile("android-photos-upload-state.sqlite")
}

// OpenState opens the ledger.
func OpenState(path string) (*State, error) {
	db, err := common.OpenStateDB(path, []string{
		`CREATE TABLE IF NOT EXISTS landed (
			native_id TEXT PRIMARY KEY,
			size_bytes INTEGER NOT NULL,
			mtime_ns INTEGER NOT NULL,
			content_sha256 TEXT NOT NULL,
			landed_at TEXT NOT NULL DEFAULT ''
		)`,
	}, map[string]string{"schema_version": "1"}, []string{"landed"})
	if err != nil {
		return nil, err
	}
	// Added 2026-11-08 after the first ledgers existed: added in place, so a
	// landed still is not sent again (a schema_version bump would wipe them).
	columns, err := db.ColumnsOf("landed")
	if err != nil {
		db.Close()
		return nil, err
	}
	for name, ddl := range map[string]string{
		"motion_checked": "ALTER TABLE landed ADD COLUMN motion_checked INTEGER NOT NULL DEFAULT 0",
		"motion_sha256":  "ALTER TABLE landed ADD COLUMN motion_sha256 TEXT NOT NULL DEFAULT ''",
	} {
		if !columns[name] {
			if _, err := db.DB.Exec(ddl); err != nil {
				db.Close()
				return nil, err
			}
		}
	}
	return &State{db: db}, nil
}

// Close releases the database.
func (s *State) Close() error { return s.db.Close() }

func (s *State) entry(nativeID string) (stateEntry, bool, error) {
	rows, err := common.Query(s.db.DB, "SELECT size_bytes, mtime_ns, content_sha256, motion_checked, motion_sha256 FROM landed WHERE native_id = ?", nativeID)
	if err != nil || len(rows) == 0 {
		return stateEntry{}, false, err
	}
	row := rows[0]
	return stateEntry{
		SizeBytes:     row.Int("size_bytes"),
		ModifiedNS:    row.Int("mtime_ns"),
		ContentSHA256: row.String("content_sha256"),
		MotionChecked: row.Int("motion_checked") != 0,
		MotionSHA256:  row.String("motion_sha256"),
	}, true, nil
}

// record remembers a landed file: its stat and sha, and whether its motion
// video was checked (motionSHA is the video's sha, or "" for a plain still).
func (s *State) record(nativeID string, size, mtimeNS int64, sha string, motionChecked bool, motionSHA string, now time.Time) error {
	checked := 0
	if motionChecked {
		checked = 1
	}
	_, err := s.db.DB.Exec(`INSERT INTO landed (native_id, size_bytes, mtime_ns, content_sha256, motion_checked, motion_sha256, landed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(native_id) DO UPDATE SET size_bytes = excluded.size_bytes, mtime_ns = excluded.mtime_ns,
			content_sha256 = excluded.content_sha256, motion_checked = excluded.motion_checked,
			motion_sha256 = excluded.motion_sha256, landed_at = excluded.landed_at`,
		nativeID, size, mtimeNS, sha, checked, motionSHA, common.ISOFormat(now))
	return err
}
