// Package testfixtures holds helpers shared by the uploader tests: reading
// the golden files generated from the Python implementations, decoding gzip
// batches, and building the synthetic stores the tests scan.
package testfixtures

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// Golden is one record of a golden.jsonl file: the canonical payload bytes the
// Python encoder produced and the fingerprint over them.
type Golden struct {
	Store       string `json:"store"`
	Kind        string `json:"kind"`
	Fingerprint string `json:"fingerprint"`
	Payload     string `json:"payload"`
	Extra       map[string]any
}

// LoadGoldens reads a golden.jsonl grouped by store.
func LoadGoldens(t *testing.T, path string) map[string][]Golden {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("golden file: %v", err)
	}
	defer file.Close()
	out := map[string][]Golden{}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var golden Golden
		if err := json.Unmarshal([]byte(line), &golden); err != nil {
			t.Fatalf("golden line: %v", err)
		}
		if err := json.Unmarshal([]byte(line), &golden.Extra); err != nil {
			t.Fatalf("golden line: %v", err)
		}
		out[golden.Store] = append(out[golden.Store], golden)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// DecodeGzipJSONL decodes a batch body into its records.
func DecodeGzipJSONL(gz []byte) []map[string]any {
	reader, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		panic(err)
	}
	data, _ := io.ReadAll(reader)
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			panic(err)
		}
		records = append(records, record)
	}
	return records
}

// CopyFile copies a fixture into a scratch location.
func CopyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// ExecScript runs SQL statements against a fresh SQLite file.
func ExecScript(t *testing.T, path string, statements ...string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("exec %q: %v", statement, err)
		}
	}
}

// CreateEmptySyntheticContacts writes a synthetic Address Book with no cards.
func CreateEmptySyntheticContacts(t *testing.T, path string) {
	t.Helper()
	os.Remove(path)
	ExecScript(t, path,
		`CREATE TABLE contacts (contact_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, display_name TEXT NOT NULL, given_name TEXT NOT NULL, middle_name TEXT NOT NULL, family_name TEXT NOT NULL, nickname TEXT NOT NULL, organization TEXT NOT NULL, department TEXT NOT NULL, job_title TEXT NOT NULL, note TEXT NOT NULL, created_at TEXT NOT NULL, modified_at TEXT NOT NULL)`,
		`CREATE TABLE phones (contact_id TEXT NOT NULL, value TEXT NOT NULL, label TEXT NOT NULL, is_primary INTEGER NOT NULL)`,
		`CREATE TABLE emails (contact_id TEXT NOT NULL, value TEXT NOT NULL, label TEXT NOT NULL, is_primary INTEGER NOT NULL)`,
	)
}
