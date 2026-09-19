package applenotes

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultStorePath is the Notes database, the same store the apple-notes
// uploader snapshots. It is spelled here rather than imported from
// internal/uploaders/applenotes because that uploader calls RunApplyOnce
// before its scan, and importing it back would be a cycle.
const DefaultStorePath = "~/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite"

// ErrStoreUnavailable wraps a failure to read the local NoteStore snapshot
// (missing store, Full Disk Access denied, corrupt copy). It is retryable:
// the note may well exist, we just could not look.
var ErrStoreUnavailable = errors.New("apple notes store unavailable")

// NotePrimaryKeyFromStore reads (store uuid, Z_PK) for a note UUID out of a
// snapshot of NoteStore.sqlite. A snapshot, not the live file: the uploader
// holds the same rule, because Notes writes through a WAL and reading it live
// races the app. storePath "" means the default Notes store.
func NotePrimaryKeyFromStore(uuid string, storePath string) (string, int64, bool, error) {
	if storePath == "" {
		storePath = DefaultStorePath
	}
	tempDir, err := os.MkdirTemp("", "pdw-notes-mutation-")
	if err != nil {
		return "", 0, false, fmt.Errorf("%w: %v", ErrStoreUnavailable, err)
	}
	defer os.RemoveAll(tempDir)
	snapshot := filepath.Join(tempDir, "NoteStore.sqlite")
	if err := common.SnapshotSQLite(common.ExpandUser(storePath), snapshot); err != nil {
		return "", 0, false, fmt.Errorf("%w: %v", ErrStoreUnavailable, err)
	}
	db, err := common.OpenSQLiteReadOnly(snapshot)
	if err != nil {
		return "", 0, false, fmt.Errorf("%w: %v", ErrStoreUnavailable, err)
	}
	defer db.Close()
	storeUUID, primaryKey, ok, err := lookupInStore(db, uuid)
	if err != nil {
		return "", 0, false, fmt.Errorf("%w: %v", ErrStoreUnavailable, err)
	}
	return storeUUID, primaryKey, ok, nil
}

func lookupInStore(db *sql.DB, uuid string) (string, int64, bool, error) {
	var storeUUID string
	if err := db.QueryRow(`SELECT Z_UUID FROM Z_METADATA`).Scan(&storeUUID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, false, nil
		}
		return "", 0, false, err
	}
	var primaryKey int64
	if err := db.QueryRow(`SELECT Z_PK FROM ZICCLOUDSYNCINGOBJECT WHERE ZIDENTIFIER = ?`, uuid).Scan(&primaryKey); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, false, nil
		}
		return "", 0, false, err
	}
	return storeUUID, primaryKey, true, nil
}
