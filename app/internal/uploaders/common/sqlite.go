package common

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	_ "modernc.org/sqlite"
)

// OpenSQLite opens a writable SQLite database, creating it when absent.
func OpenSQLite(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// The uploaders are single-process; a second connection only ever races
	// the first on the same state file, so keep the pool at one.
	db.SetMaxOpenConns(1)
	return db, nil
}

// OpenSQLiteReadOnly opens an existing database read-only (mode=ro), which is
// how a live Apple store is read: Messages, Notes, Contacts and Photos keep
// their files open in WAL mode and must never be written to by us.
func OpenSQLiteReadOnly(path string) (*sql.DB, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	dsn := "file:" + url.PathEscape(path) + "?mode=ro"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// SnapshotSQLite copies a live store to destination as a consistent,
// self-contained database (WAL contents included), the way the Python
// uploaders used sqlite3's backup API. A store this process cannot open is
// reported as a permission problem, because on macOS that is what it is:
// Full Disk Access has not been granted to the launching executable chain.
func SnapshotSQLite(source, destination string) error {
	src, err := OpenSQLiteReadOnly(source)
	if err != nil {
		return &PermissionError{Path: source, Err: err}
	}
	defer src.Close()
	if _, err := src.Exec("SELECT count(*) FROM sqlite_master"); err != nil {
		return &PermissionError{Path: source, Err: err}
	}
	_ = os.Remove(destination)
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	if _, err := src.Exec("VACUUM INTO " + SQLiteLiteral(destination)); err != nil {
		return fmt.Errorf("snapshot %s: %w", source, err)
	}
	return nil
}

// PermissionError is raised when a system store cannot be opened, and carries
// the repair a person can act on.
type PermissionError struct {
	Path string
	Err  error
}

func (e *PermissionError) Error() string {
	return fmt.Sprintf("Could not open %s. Grant Full Disk Access to the launching executable chain and retry. (%v)", e.Path, e.Err)
}

func (e *PermissionError) Unwrap() error { return e.Err }

// SQLiteLiteral quotes a string as a SQL literal.
func SQLiteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

var identifierRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// QuoteIdentifier double-quotes a SQLite identifier, refusing anything that is
// not a plain name (the table names come from sqlite_master, never from users).
func QuoteIdentifier(value string) (string, error) {
	if !identifierRe.MatchString(value) {
		return "", fmt.Errorf("invalid SQLite identifier: %q", value)
	}
	return `"` + value + `"`, nil
}

// Row is one scanned SQLite row: the column names in query order plus the
// values, with SQLite INTEGER as int64, REAL as float64, TEXT as string,
// BLOB as []byte and NULL as nil.
type Row struct {
	Columns []string
	Values  map[string]any
}

// Has reports whether the column was selected (NULL counts as present).
func (r Row) Has(column string) bool {
	_, ok := r.Values[column]
	return ok
}

// Get returns the raw value (nil when absent or NULL).
func (r Row) Get(column string) any {
	return r.Values[column]
}

// String mirrors the scanners' string_value: "" for absent/NULL, the text of
// numbers as Python's str() would print them, and "<N bytes>" for a BLOB.
func (r Row) String(column string) string {
	return PyStr(r.Values[column])
}

// First returns the first present, non-NULL column's string value.
func (r Row) First(columns ...string) string {
	for _, column := range columns {
		if v, ok := r.Values[column]; ok && v != nil {
			return PyStr(v)
		}
	}
	return ""
}

// FirstValue returns the first present, non-NULL raw value.
func (r Row) FirstValue(columns ...string) (any, bool) {
	for _, column := range columns {
		if v, ok := r.Values[column]; ok && v != nil {
			return v, true
		}
	}
	return nil, false
}

// Int mirrors int_value: 0 for absent/NULL/unparseable.
func (r Row) Int(column string) int64 {
	return ToInt(r.Values[column])
}

// Bool mirrors bool_value: the int value is non-zero.
func (r Row) Bool(column string) bool {
	return r.Int(column) != 0
}

// Bytes returns the BLOB value, or nil when the column holds anything else.
func (r Row) Bytes(column string) []byte {
	if b, ok := r.Values[column].([]byte); ok {
		return b
	}
	return nil
}

// Public mirrors public_row: every column, with BLOBs rendered as
// "<N bytes>" placeholders.
func (r Row) Public() map[string]any {
	out := make(map[string]any, len(r.Columns))
	for _, column := range r.Columns {
		out[column] = PublicValue(r.Values[column])
	}
	return out
}

// PublicWithoutBlobs mirrors public_row_without_blobs.
func (r Row) PublicWithoutBlobs() map[string]any {
	out := make(map[string]any, len(r.Columns))
	for _, column := range r.Columns {
		if _, isBlob := r.Values[column].([]byte); isBlob {
			continue
		}
		out[column] = PublicValue(r.Values[column])
	}
	return out
}

// PublicSkippingBlobs mirrors the Contacts scanner's _public_row: BLOB
// columns are dropped rather than placeholdered.
func (r Row) PublicSkippingBlobs() map[string]any {
	return r.PublicWithoutBlobs()
}

// PublicValue mirrors public_value: a BLOB becomes "<N bytes>"; scalars pass
// through as their JSON-able selves.
func PublicValue(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return fmt.Sprintf("<%d bytes>", len(v))
	case string, int64, float64, bool:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case float32:
		return float64(v)
	default:
		return fmt.Sprint(v)
	}
}

// PyStr renders a scalar as Python's str() would for the SQLite types.
func PyStr(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return fmt.Sprintf("<%d bytes>", len(v))
	case int64:
		return fmt.Sprintf("%d", v)
	case int:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case float64:
		return FloatRepr(v)
	case float32:
		return FloatRepr(float64(v))
	case bool:
		if v {
			return "True"
		}
		return "False"
	default:
		return fmt.Sprint(v)
	}
}

// Query runs a statement and scans every row.
func Query(db *sql.DB, statement string, args ...any) ([]Row, error) {
	rows, err := db.Query(statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []Row
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		row := Row{Columns: append([]string(nil), columns...), Values: make(map[string]any, len(columns))}
		for i, column := range columns {
			row.Values[column] = normalizeScanned(values[i])
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func normalizeScanned(value any) any {
	switch v := value.(type) {
	case []byte:
		// modernc returns TEXT as string and BLOB as []byte; copy the blob
		// because the driver may reuse the buffer.
		return append([]byte(nil), v...)
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case float32:
		return float64(v)
	default:
		return v
	}
}

// TableNames lists the database's tables.
func TableNames(db *sql.DB) (map[string]bool, error) {
	rows, err := Query(db, "SELECT name FROM sqlite_master WHERE type = 'table'")
	if err != nil {
		return nil, err
	}
	names := map[string]bool{}
	for _, row := range rows {
		names[row.String("name")] = true
	}
	return names, nil
}

// SelectAll mirrors select_all(connection, table): every column of a table,
// with ROWID selected explicitly first when withRowID is set.
func SelectAll(db *sql.DB, table string, withRowID bool) ([]Row, error) {
	quoted, err := QuoteIdentifier(table)
	if err != nil {
		return nil, err
	}
	if withRowID {
		return Query(db, "SELECT ROWID AS ROWID, * FROM "+quoted)
	}
	return Query(db, "SELECT * FROM "+quoted)
}

// ErrNoRows re-exports sql.ErrNoRows for callers that never import database/sql.
var ErrNoRows = sql.ErrNoRows

// IsNoRows reports whether err is sql.ErrNoRows.
func IsNoRows(err error) bool { return errors.Is(err, sql.ErrNoRows) }
