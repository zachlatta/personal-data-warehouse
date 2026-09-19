package common

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// StateDB is the SQLite-backed incremental state the uploaders keep under
// ~/Library/Application Support/personal-data-warehouse. Every one has a
// `metadata` table recording the schema version, account and store identity;
// a mismatch wipes the state so completeness recorded against one library can
// never suppress uploads from another.
type StateDB struct {
	DB   *sql.DB
	Path string
}

// OpenStateDB creates the file's directory, opens it, runs the schema DDL,
// then reconciles the metadata table against expected, in the order the
// Python state classes did.
func OpenStateDB(path string, schema []string, expected map[string]string, stateTables []string) (*StateDB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := OpenSQLite(path)
	if err != nil {
		return nil, err
	}
	state := &StateDB{DB: db, Path: path}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '')`); err != nil {
		db.Close()
		return nil, err
	}
	for _, statement := range schema {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("state schema: %w", err)
		}
	}
	if err := state.ensureMetadata(expected, stateTables); err != nil {
		db.Close()
		return nil, err
	}
	return state, nil
}

func (s *StateDB) ensureMetadata(expected map[string]string, stateTables []string) error {
	rows, err := Query(s.DB, "SELECT key, value FROM metadata")
	if err != nil {
		return err
	}
	current := map[string]string{}
	for _, row := range rows {
		current[row.String("key")] = row.String("value")
	}
	mismatch := false
	if len(current) > 0 {
		for key, value := range expected {
			if current[key] != value {
				mismatch = true
				break
			}
		}
	}
	if mismatch {
		for _, table := range stateTables {
			quoted, err := QuoteIdentifier(table)
			if err != nil {
				return err
			}
			if _, err := s.DB.Exec("DELETE FROM " + quoted); err != nil {
				return err
			}
		}
		if _, err := s.DB.Exec("DELETE FROM metadata"); err != nil {
			return err
		}
	}
	for key, value := range expected {
		if _, err := s.DB.Exec(`INSERT INTO metadata (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the database.
func (s *StateDB) Close() error {
	if s == nil || s.DB == nil {
		return nil
	}
	return s.DB.Close()
}

// ColumnsOf lists a table's columns (PRAGMA table_info).
func (s *StateDB) ColumnsOf(table string) (map[string]bool, error) {
	quoted, err := QuoteIdentifier(table)
	if err != nil {
		return nil, err
	}
	rows, err := Query(s.DB, "PRAGMA table_info("+quoted+")")
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, row := range rows {
		out[row.String("name")] = true
	}
	return out, nil
}

// DefaultStateFile is the state path for one uploader.
func DefaultStateFile(name string) string {
	return filepath.Join(ApplicationSupportDir(), name)
}

// LockFileFor is the lock path beside a state file (pathlib with_suffix(".lock")).
func LockFileFor(stateFile string) string {
	base := filepath.Base(stateFile)
	ext := filepath.Ext(base)
	if ext == "" || strings.HasPrefix(base, ".") {
		return stateFile + ".lock"
	}
	return strings.TrimSuffix(stateFile, ext) + ".lock"
}
