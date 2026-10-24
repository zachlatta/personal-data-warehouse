// Package googlerecorder is `pdw ingest google-recorder`: it ships the
// recordings Google Recorder makes on the Pixel to the app, from the phone.
//
// It runs as root on the rooted Pixel (a Magisk service starts it, see
// bin/google-recorder-upload-android), because Recorder keeps its recordings
// and its recording_database in its private data directory. Everything the
// warehouse records about a recording comes from that database: the uuid,
// Recorder's title, created_time (epoch milliseconds, UTC), the duration and
// the place. The audio is the .m4a Recorder wrote, sent unchanged.
package googlerecorder

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultRecorderDir is Recorder's data directory on Android (user 0).
const DefaultRecorderDir = "/data/data/com.google.android.apps.recorder"

// CompletedState is recording_meta.recording_state once Recorder has
// finished and saved a recording. A recording in progress is never sent.
const CompletedState = 3

const databaseName = "recording_database"

// Recording is one row of Recorder's recording_meta with its audio file.
type Recording struct {
	UUID            string
	Account         string
	Title           string
	CreatedAt       time.Time
	DurationMS      int64
	LocationName    string
	Latitude        *float64
	Longitude       *float64
	PlaceID         string
	State           int64
	ParentUUID      string
	RecordingConfig string
	StoredPath      string // the path Recorder recorded, inside its own sandbox
	Path            string // where the file is under the configured directory
	SizeBytes       int64
	ModifiedAt      time.Time
	FileExists      bool
}

// ReadRecordings reads every recording, newest first, from a copy of
// Recorder's database made in scratchDir.
//
// The live database is never opened. Recorder holds it in WAL mode under its
// own uid, and a root reader that creates the -shm or -wal file beside it
// leaves a file Recorder cannot write; copying the database and its WAL (not
// the -shm, which SQLite rebuilds from the WAL) reads the last committed
// state without touching Recorder's files. A copy torn by a concurrent
// checkpoint fails its integrity check and is taken again.
func ReadRecordings(recorderDir, scratchDir string) ([]Recording, error) {
	databases := filepath.Join(recorderDir, "databases")
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		recordings, err := readCopy(databases, filepath.Join(scratchDir, fmt.Sprintf("copy-%d", attempt)), recorderDir)
		if err == nil {
			return recordings, nil
		}
		var notFound *os.PathError
		if errors.As(err, &notFound) && errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("Recorder's database is not at %s (is Recorder installed and has it been opened once?): %w", databases, err)
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	return nil, lastErr
}

func readCopy(databases, copyDir, recorderDir string) ([]Recording, error) {
	if err := os.MkdirAll(copyDir, 0o700); err != nil {
		return nil, err
	}
	defer os.RemoveAll(copyDir)
	copied := filepath.Join(copyDir, databaseName)
	if err := copyFile(filepath.Join(databases, databaseName), copied); err != nil {
		return nil, err
	}
	if err := copyFile(filepath.Join(databases, databaseName+"-wal"), copied+"-wal"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	db, err := common.OpenSQLite(copied)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	check, err := common.Query(db, "PRAGMA quick_check")
	if err != nil {
		return nil, fmt.Errorf("check copy of Recorder's database: %w", err)
	}
	if len(check) != 1 || check[0].String("quick_check") != "ok" {
		return nil, fmt.Errorf("copy of Recorder's database failed its integrity check: %v", check)
	}
	tables, err := common.TableNames(db)
	if err != nil {
		return nil, err
	}
	if !tables["recording_meta"] {
		return nil, fmt.Errorf("Recorder's database has no recording_meta table; its schema changed")
	}
	rows, err := common.Query(db, `SELECT uuid, COALESCE(account, '') AS account, title, loc_lat, loc_lng,
		COALESCE(loc_name, '') AS loc_name, COALESCE(loc_place_id, '') AS loc_place_id, created_time, duration,
		file_path, recording_config, recording_state, COALESCE(parent_uuid, '') AS parent_uuid
		FROM recording_meta ORDER BY created_time DESC, uuid`)
	if err != nil {
		return nil, fmt.Errorf("read Recorder's recording_meta: %w", err)
	}
	recordings := make([]Recording, 0, len(rows))
	for _, row := range rows {
		r := Recording{
			UUID:            row.String("uuid"),
			Account:         row.String("account"),
			Title:           row.String("title"),
			CreatedAt:       time.UnixMilli(row.Int("created_time")).UTC(),
			DurationMS:      row.Int("duration"),
			LocationName:    row.String("loc_name"),
			Latitude:        floatOrNil(row.Get("loc_lat")),
			Longitude:       floatOrNil(row.Get("loc_lng")),
			PlaceID:         row.String("loc_place_id"),
			State:           row.Int("recording_state"),
			ParentUUID:      row.String("parent_uuid"),
			RecordingConfig: row.String("recording_config"),
			StoredPath:      row.String("file_path"),
		}
		r.Path = filepath.Join(recorderDir, "files", "recordings", filepath.Base(r.StoredPath))
		if info, err := os.Stat(r.Path); err == nil && info.Mode().IsRegular() {
			r.FileExists = true
			r.SizeBytes = info.Size()
			r.ModifiedAt = info.ModTime().UTC()
		}
		recordings = append(recordings, r)
	}
	return recordings, nil
}

func floatOrNil(value any) *float64 {
	switch v := value.(type) {
	case float64:
		return &v
	case int64:
		f := float64(v)
		return &f
	}
	return nil
}

func copyFile(source, destination string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
