package server

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"regexp"
	"sync"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/objectstore"
	"github.com/zachlatta/personal-data-warehouse/app/internal/timelapsefiles"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// screenTimelapseFileEndpoint receives one screen timelapse file per request:
// a periodic whole-screen image or the accessibility tree of the same moment,
// exactly as the capture loop wrote it. The app stores the bytes AND writes the
// file's row before it answers, so there is no inbox for a second process to
// promote: every row is determined by one file (its name says device-local
// time, display and kind), and the row and the bytes land together or the
// client sends the file again.
//
// Dedup is the row, not the object store: a resend of a moment whose row
// already carries the same sha is answered from Postgres without a Drive call,
// and a new file is uploaded with the store's per-upload Drive search skipped.
const screenTimelapseFileEndpoint = "/ingest/screen-timelapse/file"

// screenTimelapseSource is the ingest slug whose Drive store holds the bytes.
const screenTimelapseSource = "screen_timelapse"

var screenTimelapseDevicePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// screenTimelapseRow is one file's row in base_screen_timelapse.screenshots or
// .accessibility_trees (DisplayIndex is unused for a tree).
type screenTimelapseRow struct {
	Kind           timelapsefiles.Kind
	Device         string
	CapturedAt     time.Time
	DisplayIndex   int
	SourceFilename string
	SHA256         string
	MimeType       string
	SizeBytes      int64
	Stored         objectstore.StoredObject
	IngestedAt     time.Time
}

type screenTimelapseRowWriter interface {
	existing(ctx context.Context, kind timelapsefiles.Kind, device string, capturedAt time.Time, display int) (screenTimelapseRow, bool, error)
	upsert(ctx context.Context, row screenTimelapseRow) error
}

type screenTimelapseIngestService struct {
	store    objectstore.ObjectStore
	rows     screenTimelapseRowWriter
	signer   *pdwauth.Service
	maxBytes int64
	timeout  time.Duration
	now      func() time.Time
	logger   *slog.Logger
}

func (svc *screenTimelapseIngestService) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		endpoint := path.Clean(r.URL.Path)
		q := r.URL.Query()
		declaredSHA := q.Get("content_sha256")
		if err := svc.signer.VerifyObjectUpload(endpoint, declaredSHA, q.Get("exp"), q.Get("sig")); err != nil {
			svc.logger.WarnContext(r.Context(), "screen timelapse upload link rejected", "endpoint", endpoint, "error", err)
			http.Error(w, "invalid or expired upload link", http.StatusForbidden)
			return
		}
		body, err := readLimited(r.Body, svc.maxBytes)
		if err == errTooLarge {
			http.Error(w, "object too large", http.StatusRequestEntityTooLarge)
			return
		}
		if err != nil {
			http.Error(w, "could not read body", http.StatusBadRequest)
			return
		}
		sha := hex.EncodeToString(sha256Sum(body))
		if sha != declaredSHA {
			http.Error(w, "content_sha256 does not match body", http.StatusBadRequest)
			return
		}
		row, err := screenTimelapseRowFromRequest(q, sha, int64(len(body)))
		if err != nil {
			svc.logger.WarnContext(r.Context(), "screen timelapse upload refused", "error", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		timeout := svc.timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		existing, found, err := svc.rows.existing(ctx, row.Kind, row.Device, row.CapturedAt, row.DisplayIndex)
		if err != nil {
			svc.logger.ErrorContext(r.Context(), "screen timelapse row lookup failed", "error", err)
			http.Error(w, "warehouse error", http.StatusBadGateway)
			return
		}
		if found && existing.SHA256 == sha {
			writeScreenTimelapseResponse(w, existing.Stored, true)
			return
		}
		stored, err := svc.store.PutFile(r.Context(), objectstore.PutFileInput{
			ObjectKey:         screenTimelapseObjectKey(row),
			Content:           body,
			ContentSHA256:     sha,
			ContentType:       row.MimeType,
			SkipExistingCheck: true,
			Kind:              "screen_timelapse_" + string(row.Kind),
			AppProperties: map[string]string{
				"device":      row.Device,
				"captured_at": row.CapturedAt.Format(time.RFC3339),
			},
		})
		if err != nil {
			svc.logger.ErrorContext(r.Context(), "screen timelapse store write failed", "error", err)
			http.Error(w, "object store error", http.StatusBadGateway)
			return
		}
		row.Stored = stored
		row.IngestedAt = svc.now().UTC()
		if err := svc.rows.upsert(ctx, row); err != nil {
			// The bytes are stored but the row is not: answer an error so the
			// client sends the file again (an orphaned Drive object, never a
			// row without bytes).
			svc.logger.ErrorContext(r.Context(), "screen timelapse row write failed", "storage_key", stored.StorageKey, "error", err)
			http.Error(w, "warehouse error", http.StatusBadGateway)
			return
		}
		svc.logger.InfoContext(r.Context(), "screen timelapse file stored", "device", row.Device, "kind", row.Kind, "captured_at", row.CapturedAt, "bytes", len(body))
		writeScreenTimelapseResponse(w, stored, false)
	})
}

func writeScreenTimelapseResponse(w http.ResponseWriter, stored objectstore.StoredObject, skipped bool) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"storage_backend": stored.StorageBackend,
		"storage_key":     stored.StorageKey,
		"storage_file_id": stored.StorageFileID,
		"storage_url":     stored.StorageURL,
		"skipped":         skipped,
	})
}

// screenTimelapseRowFromRequest places a file from its name plus the instant
// the client resolved the name's local stamp to (only the client knows the
// capture host's zone). An instant more than a day from the stamp read as UTC
// cannot be a timezone, so it is refused rather than stored somewhere wrong.
func screenTimelapseRowFromRequest(q map[string][]string, sha string, size int64) (screenTimelapseRow, error) {
	get := func(key string) string {
		if values := q[key]; len(values) > 0 {
			return values[0]
		}
		return ""
	}
	device := get("device")
	if !screenTimelapseDevicePattern.MatchString(device) {
		return screenTimelapseRow{}, fmt.Errorf("device must match %s", screenTimelapseDevicePattern)
	}
	filename := get("source_filename")
	file, ok := timelapsefiles.Parse(filename)
	if !ok {
		return screenTimelapseRow{}, fmt.Errorf("source_filename %q is not a screen timelapse file", filename)
	}
	capturedAt, err := time.Parse(time.RFC3339Nano, get("captured_at"))
	if err != nil {
		return screenTimelapseRow{}, fmt.Errorf("invalid captured_at: %w", err)
	}
	if _, zoned := file.Offset(); zoned {
		if want := file.CapturedAt(time.UTC, capturedAt); !capturedAt.Equal(want) {
			return screenTimelapseRow{}, fmt.Errorf("captured_at %s is not the stamp of %s (%s)", capturedAt.Format(time.RFC3339), filename, want.Format(time.RFC3339))
		}
	}
	stampAsUTC := file.CapturedAt(time.UTC, capturedAt)
	if offset := capturedAt.Sub(stampAsUTC); offset > 26*time.Hour || offset < -26*time.Hour {
		return screenTimelapseRow{}, fmt.Errorf("captured_at %s is not the stamp %s in any timezone", capturedAt.Format(time.RFC3339), file.LocalStamp)
	}
	return screenTimelapseRow{
		Kind:           file.Kind,
		Device:         device,
		CapturedAt:     capturedAt.UTC(),
		DisplayIndex:   file.DisplayIndex,
		SourceFilename: filename,
		SHA256:         sha,
		MimeType:       file.MimeType,
		SizeBytes:      size,
	}, nil
}

// screenTimelapseObjectKey files the bytes by device and UTC day. The sha
// prefix keeps two files apart that share a name: the fall-back hour repeats
// every stamp in it.
func screenTimelapseObjectKey(row screenTimelapseRow) string {
	day := row.CapturedAt.UTC()
	return fmt.Sprintf("screen-timelapse/library/%s/%04d/%02d/%02d/%s-%s",
		row.Device, day.Year(), int(day.Month()), day.Day(), row.SHA256, row.SourceFilename)
}

// screenTimelapseRowStore writes the rows. The app is the only writer of these
// tables, so it provisions them too (once per process): a deploy that lands
// before any Python ensure path has run still ingests. The DDL is identical to
// the Python TableSpec/IndexSpec, pinned by tests/test_screen_timelapse_store.py.
type screenTimelapseRowStore struct {
	db          *sql.DB
	timeout     time.Duration
	schema      string
	screenshots string
	trees       string

	ensureMu sync.Mutex
	ensured  bool
}

func newScreenTimelapseRowStore(databaseURL string, timeout time.Duration) (*screenTimelapseRowStore, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &screenTimelapseRowStore{
		db:          db,
		timeout:     timeout,
		schema:      warehouse.SchemaOf("screen_timelapse_screenshots"),
		screenshots: warehouse.SQLRelation("screen_timelapse_screenshots"),
		trees:       warehouse.SQLRelation("screen_timelapse_accessibility_trees"),
	}, nil
}

func (s *screenTimelapseRowStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

const screenTimelapseFileColumnsDDL = `
	source_filename text NOT NULL DEFAULT '',
	content_sha256 text NOT NULL DEFAULT '',
	mime_type text NOT NULL DEFAULT '',
	size_bytes bigint NOT NULL DEFAULT 0,
	storage_backend text NOT NULL DEFAULT '',
	storage_key text NOT NULL DEFAULT '',
	storage_file_id text NOT NULL DEFAULT '',
	storage_url text NOT NULL DEFAULT '',
	ingested_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,`

func screenTimelapseDDL(schema, screenshots, trees string) []string {
	return []string{
		"CREATE SCHEMA IF NOT EXISTS " + warehouse.QuoteIdent(schema),
		`CREATE TABLE IF NOT EXISTS ` + screenshots + ` (
	device text NOT NULL DEFAULT '',
	captured_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
	display_index bigint NOT NULL DEFAULT 0,` + screenTimelapseFileColumnsDDL + `
	PRIMARY KEY (device, captured_at, display_index)
)`,
		`CREATE TABLE IF NOT EXISTS ` + trees + ` (
	device text NOT NULL DEFAULT '',
	captured_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,` + screenTimelapseFileColumnsDDL + `
	PRIMARY KEY (device, captured_at)
)`,
		`CREATE INDEX IF NOT EXISTS screen_timelapse_screenshots_ingested_idx ON ` + screenshots + ` (ingested_at)`,
		`CREATE INDEX IF NOT EXISTS screen_timelapse_screenshots_captured_idx ON ` + screenshots + ` (captured_at DESC)`,
		`CREATE INDEX IF NOT EXISTS screen_timelapse_accessibility_trees_ingested_idx ON ` + trees + ` (ingested_at)`,
	}
}

func (s *screenTimelapseRowStore) ensure(ctx context.Context) error {
	s.ensureMu.Lock()
	defer s.ensureMu.Unlock()
	if s.ensured {
		return nil
	}
	for _, statement := range screenTimelapseDDL(s.schema, s.screenshots, s.trees) {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	s.ensured = true
	return nil
}

func (s *screenTimelapseRowStore) relationFor(kind timelapsefiles.Kind) string {
	if kind == timelapsefiles.AccessibilityTree {
		return s.trees
	}
	return s.screenshots
}

func (s *screenTimelapseRowStore) existing(ctx context.Context, kind timelapsefiles.Kind, device string, capturedAt time.Time, display int) (screenTimelapseRow, bool, error) {
	if err := s.ensure(ctx); err != nil {
		return screenTimelapseRow{}, false, err
	}
	query := `SELECT content_sha256, storage_backend, storage_key, storage_file_id, storage_url FROM ` +
		s.relationFor(kind) + ` WHERE device = $1 AND captured_at = $2`
	args := []any{device, capturedAt}
	if kind == timelapsefiles.Screenshot {
		query += ` AND display_index = $3`
		args = append(args, display)
	}
	row := screenTimelapseRow{Kind: kind, Device: device, CapturedAt: capturedAt, DisplayIndex: display}
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&row.SHA256, &row.Stored.StorageBackend, &row.Stored.StorageKey, &row.Stored.StorageFileID, &row.Stored.StorageURL)
	if err == sql.ErrNoRows {
		return screenTimelapseRow{}, false, nil
	}
	if err != nil {
		return screenTimelapseRow{}, false, err
	}
	return row, true, nil
}

func (s *screenTimelapseRowStore) upsert(ctx context.Context, row screenTimelapseRow) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	fileColumns := []any{row.SourceFilename, row.SHA256, row.MimeType, row.SizeBytes,
		row.Stored.StorageBackend, row.Stored.StorageKey, row.Stored.StorageFileID, row.Stored.StorageURL, row.IngestedAt}
	const updates = `source_filename = EXCLUDED.source_filename, content_sha256 = EXCLUDED.content_sha256,
			mime_type = EXCLUDED.mime_type, size_bytes = EXCLUDED.size_bytes,
			storage_backend = EXCLUDED.storage_backend, storage_key = EXCLUDED.storage_key,
			storage_file_id = EXCLUDED.storage_file_id, storage_url = EXCLUDED.storage_url,
			ingested_at = EXCLUDED.ingested_at`
	const fileNames = `source_filename, content_sha256, mime_type, size_bytes,
			storage_backend, storage_key, storage_file_id, storage_url, ingested_at`
	var err error
	if row.Kind == timelapsefiles.AccessibilityTree {
		_, err = s.db.ExecContext(ctx, `INSERT INTO `+s.trees+` (device, captured_at, `+fileNames+`)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT (device, captured_at) DO UPDATE SET `+updates,
			append([]any{row.Device, row.CapturedAt}, fileColumns...)...)
	} else {
		_, err = s.db.ExecContext(ctx, `INSERT INTO `+s.screenshots+` (device, captured_at, display_index, `+fileNames+`)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT (device, captured_at, display_index) DO UPDATE SET `+updates,
			append([]any{row.Device, row.CapturedAt, row.DisplayIndex}, fileColumns...)...)
	}
	return err
}
