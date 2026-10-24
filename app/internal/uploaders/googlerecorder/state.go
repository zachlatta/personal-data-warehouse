package googlerecorder

import (
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// State is the ledger of landed recordings: per Recorder uuid, the audio and
// sidecar the app acknowledged, and the file stat the audio sha was taken
// at (so an unchanged file is not re-hashed every run). The app dedups both
// objects too, so a lost ledger costs a resend, never a duplicate.
type State struct {
	db *common.StateDB
}

type stateEntry struct {
	SizeBytes      int64
	ModifiedNS     int64
	AudioSHA256    string
	MetadataSHA256 string
}

// DefaultStateFile is the ledger path.
func DefaultStateFile() string {
	return common.DefaultStateFile("google-recorder-upload-state.sqlite")
}

// OpenState opens the ledger.
func OpenState(path string) (*State, error) {
	db, err := common.OpenStateDB(path, []string{
		`CREATE TABLE IF NOT EXISTS landed (
			uuid TEXT PRIMARY KEY,
			size_bytes INTEGER NOT NULL,
			mtime_ns INTEGER NOT NULL,
			audio_sha256 TEXT NOT NULL,
			metadata_sha256 TEXT NOT NULL,
			landed_at TEXT NOT NULL DEFAULT ''
		)`,
	}, map[string]string{"schema_version": "1"}, []string{"landed"})
	if err != nil {
		return nil, err
	}
	return &State{db: db}, nil
}

// Close releases the database.
func (s *State) Close() error { return s.db.Close() }

func (s *State) entry(uuid string) (stateEntry, bool, error) {
	rows, err := common.Query(s.db.DB, "SELECT size_bytes, mtime_ns, audio_sha256, metadata_sha256 FROM landed WHERE uuid = ?", uuid)
	if err != nil || len(rows) == 0 {
		return stateEntry{}, false, err
	}
	row := rows[0]
	return stateEntry{
		SizeBytes:      row.Int("size_bytes"),
		ModifiedNS:     row.Int("mtime_ns"),
		AudioSHA256:    row.String("audio_sha256"),
		MetadataSHA256: row.String("metadata_sha256"),
	}, true, nil
}

func (s *State) record(r Recording, audioSHA, metadataSHA string, now time.Time) error {
	_, err := s.db.DB.Exec(`INSERT INTO landed (uuid, size_bytes, mtime_ns, audio_sha256, metadata_sha256, landed_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(uuid) DO UPDATE SET size_bytes = excluded.size_bytes, mtime_ns = excluded.mtime_ns,
			audio_sha256 = excluded.audio_sha256, metadata_sha256 = excluded.metadata_sha256, landed_at = excluded.landed_at`,
		r.UUID, r.SizeBytes, r.ModifiedAt.UnixNano(), audioSHA, metadataSHA, common.ISOFormat(now))
	return err
}
