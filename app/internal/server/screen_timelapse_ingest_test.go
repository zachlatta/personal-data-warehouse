package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/objectstore"
	"github.com/zachlatta/personal-data-warehouse/app/internal/timelapsefiles"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

type fakeScreenTimelapseRows struct {
	rows      map[string]screenTimelapseRow
	upserts   int
	upsertErr error
}

func screenRowKey(kind timelapsefiles.Kind, device string, capturedAt time.Time, display int) string {
	return string(kind) + "|" + device + "|" + capturedAt.UTC().Format(time.RFC3339) + "|" + string(rune('0'+display))
}

func (f *fakeScreenTimelapseRows) existing(_ context.Context, kind timelapsefiles.Kind, device string, capturedAt time.Time, display int) (screenTimelapseRow, bool, error) {
	row, ok := f.rows[screenRowKey(kind, device, capturedAt, display)]
	return row, ok, nil
}

func (f *fakeScreenTimelapseRows) upsert(_ context.Context, row screenTimelapseRow) error {
	if f.upsertErr != nil {
		return f.upsertErr
	}
	f.upserts++
	if f.rows == nil {
		f.rows = map[string]screenTimelapseRow{}
	}
	f.rows[screenRowKey(row.Kind, row.Device, row.CapturedAt, row.DisplayIndex)] = row
	return nil
}

func screenTimelapseTestService(rows *fakeScreenTimelapseRows) (*screenTimelapseIngestService, *capturingStore) {
	store := &capturingStore{}
	return &screenTimelapseIngestService{
		store:    store,
		rows:     rows,
		signer:   objectsTestSigner(),
		maxBytes: 4096,
		timeout:  time.Second,
		now:      func() time.Time { return objectsTestNow },
		logger:   slog.Default(),
	}, store
}

func postScreenTimelapse(t *testing.T, svc *screenTimelapseIngestService, body []byte, params url.Values) *httptest.ResponseRecorder {
	t.Helper()
	target := signedIngestTarget(screenTimelapseFileEndpoint, body, params)
	rec := httptest.NewRecorder()
	svc.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, target, bytesReader(body)))
	return rec
}

func screenParams(device, filename, capturedAt string) url.Values {
	return url.Values{"device": {device}, "source_filename": {filename}, "captured_at": {capturedAt}}
}

func TestScreenTimelapseIngestStoresAScreenshotAndWritesItsRow(t *testing.T) {
	rows := &fakeScreenTimelapseRows{}
	svc, store := screenTimelapseTestService(rows)
	body := []byte("heic-bytes")
	rec := postScreenTimelapse(t, svc, body, screenParams("crobat", "periodic-screenshot-2026-10-22_14-00-54-display-2.heic", "2026-10-22T18:00:54Z"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	put := store.lastFile
	if put == nil {
		t.Fatal("expected the bytes to be stored")
	}
	wantKey := "screen-timelapse/library/crobat/2026/10/22/" + sha256Hex(body) + "-periodic-screenshot-2026-10-22_14-00-54-display-2.heic"
	if put.ObjectKey != wantKey || put.Kind != "screen_timelapse_screenshot" || put.ContentType != "image/heic" {
		t.Fatalf("put = %+v", put)
	}
	// Dedup is the warehouse row, not a Drive search before every upload.
	if !put.SkipExistingCheck {
		t.Fatal("the store's per-upload Drive dedup search must be skipped")
	}
	row, ok := rows.rows[screenRowKey(timelapsefiles.Screenshot, "crobat", time.Date(2026, 10, 22, 18, 0, 54, 0, time.UTC), 2)]
	if !ok {
		t.Fatalf("no row written: %+v", rows.rows)
	}
	if row.SourceFilename != "periodic-screenshot-2026-10-22_14-00-54-display-2.heic" || row.SHA256 != sha256Hex(body) ||
		row.SizeBytes != int64(len(body)) || row.MimeType != "image/heic" || row.Stored.StorageFileID != "fid-file" ||
		!row.IngestedAt.Equal(objectsTestNow) {
		t.Fatalf("row = %+v", row)
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp["storage_file_id"] != "fid-file" || resp["skipped"] != false {
		t.Fatalf("response = %s", rec.Body.String())
	}
}

func TestScreenTimelapseIngestStoresATreeAsItsOwnRow(t *testing.T) {
	rows := &fakeScreenTimelapseRows{}
	svc, store := screenTimelapseTestService(rows)
	rec := postScreenTimelapse(t, svc, []byte(`[{"caption":"x"}]`), screenParams("joltik", "phone-ax-2026-10-18_22-39-13.json", "2026-10-19T02:39:13Z"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if store.lastFile == nil || store.lastFile.Kind != "screen_timelapse_accessibility_tree" || store.lastFile.ContentType != "application/json" {
		t.Fatalf("put = %+v", store.lastFile)
	}
	row, ok := rows.rows[screenRowKey(timelapsefiles.AccessibilityTree, "joltik", time.Date(2026, 10, 19, 2, 39, 13, 0, time.UTC), 0)]
	if !ok || row.Kind != timelapsefiles.AccessibilityTree {
		t.Fatalf("rows = %+v", rows.rows)
	}
}

func TestScreenTimelapseIngestStoresAPixelFrameAndTree(t *testing.T) {
	rows := &fakeScreenTimelapseRows{}
	svc, store := screenTimelapseTestService(rows)
	at := time.Date(2026, 10, 24, 1, 9, 0, 0, time.UTC)
	rec := postScreenTimelapse(t, svc, []byte("avif-bytes"), screenParams("pichu", "phone-screenshot-2026-10-23_21-09-00-0400.avif", "2026-10-24T01:09:00Z"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if store.lastFile == nil || store.lastFile.ContentType != "image/avif" {
		t.Fatalf("put = %+v", store.lastFile)
	}
	if row, ok := rows.rows[screenRowKey(timelapsefiles.Screenshot, "pichu", at, 1)]; !ok || row.MimeType != "image/avif" {
		t.Fatalf("rows = %+v", rows.rows)
	}
	rec = postScreenTimelapse(t, svc, []byte("gzip-bytes"), screenParams("pichu", "phone-ax-2026-10-23_21-09-00-0400.xml.gz", "2026-10-24T01:09:00Z"))
	if rec.Code != http.StatusOK {
		t.Fatalf("tree status = %d, body %q", rec.Code, rec.Body.String())
	}
	if row, ok := rows.rows[screenRowKey(timelapsefiles.AccessibilityTree, "pichu", at, 0)]; !ok || row.MimeType != "application/gzip" {
		t.Fatalf("rows = %+v", rows.rows)
	}
}

func TestScreenTimelapseIngestAnswersAResendFromTheRowWithoutTouchingStorage(t *testing.T) {
	rows := &fakeScreenTimelapseRows{}
	svc, store := screenTimelapseTestService(rows)
	body := []byte("heic-bytes")
	params := screenParams("crobat", "periodic-screenshot-2026-10-22_14-00-54-display-1.heic", "2026-10-22T18:00:54Z")
	if rec := postScreenTimelapse(t, svc, body, params); rec.Code != http.StatusOK {
		t.Fatalf("first status = %d", rec.Code)
	}
	store.lastFile = nil
	rec := postScreenTimelapse(t, svc, body, params)
	if rec.Code != http.StatusOK {
		t.Fatalf("resend status = %d, body %q", rec.Code, rec.Body.String())
	}
	if store.lastFile != nil {
		t.Fatal("a resend of a stored file must not upload again")
	}
	if rows.upserts != 1 {
		t.Fatalf("upserts = %d, want 1", rows.upserts)
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp["skipped"] != true || resp["storage_file_id"] != "fid-file" {
		t.Fatalf("response = %s", rec.Body.String())
	}
}

func TestScreenTimelapseIngestReplacesAMomentWhoseBytesChanged(t *testing.T) {
	rows := &fakeScreenTimelapseRows{}
	svc, store := screenTimelapseTestService(rows)
	params := screenParams("crobat", "periodic-screenshot-2026-10-22_14-00-54-display-1.heic", "2026-10-22T18:00:54Z")
	postScreenTimelapse(t, svc, []byte("first"), params)
	store.lastFile = nil
	if rec := postScreenTimelapse(t, svc, []byte("second"), params); rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if store.lastFile == nil || rows.upserts != 2 {
		t.Fatalf("changed bytes must be stored and the row replaced (upserts %d)", rows.upserts)
	}
}

func TestScreenTimelapseIngestRefusesWhatItCannotPlace(t *testing.T) {
	cases := map[string]url.Values{
		"unrecognised filename": screenParams("crobat", "Screenshot 2026-10-22 at 14.00.54.png", "2026-10-22T18:00:54Z"),
		"missing device":        screenParams("", "phone-screenshot-2026-10-18_22-39-13.png", "2026-10-19T02:39:13Z"),
		"unsafe device":         screenParams("../crobat", "phone-screenshot-2026-10-18_22-39-13.png", "2026-10-19T02:39:13Z"),
		"bad captured_at":       screenParams("joltik", "phone-screenshot-2026-10-18_22-39-13.png", "yesterday"),
		// The client resolves the stamp in its own zone; an instant more than
		// a day away from the stamp is a bug, not a timezone.
		"captured_at far from the stamp": screenParams("joltik", "phone-screenshot-2026-10-18_22-39-13.png", "2026-10-25T02:39:13Z"),
		// A stamp that carries its offset names exactly one instant.
		"captured_at off an offset stamp": screenParams("pichu", "phone-screenshot-2026-10-23_21-09-00-0400.avif", "2026-10-23T21:09:00Z"),
	}
	for name, params := range cases {
		rows := &fakeScreenTimelapseRows{}
		svc, store := screenTimelapseTestService(rows)
		rec := postScreenTimelapse(t, svc, []byte("x"), params)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, body %q", name, rec.Code, rec.Body.String())
		}
		if store.lastFile != nil || rows.upserts != 0 {
			t.Fatalf("%s: nothing may be stored", name)
		}
	}
}

func TestScreenTimelapseIngestRejectsAnUnsignedPost(t *testing.T) {
	svc, store := screenTimelapseTestService(&fakeScreenTimelapseRows{})
	rec := httptest.NewRecorder()
	target := screenTimelapseFileEndpoint + "?" + screenParams("crobat", "phone-screenshot-2026-10-18_22-39-13.png", "2026-10-19T02:39:13Z").Encode()
	svc.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, target, strings.NewReader("x")))
	if rec.Code != http.StatusForbidden || store.lastFile != nil {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestScreenTimelapseIngestFailsWhenTheRowCannotBeWritten(t *testing.T) {
	// The client keeps the file unacknowledged and sends it again next run.
	svc, _ := screenTimelapseTestService(&fakeScreenTimelapseRows{upsertErr: errors.New("relation does not exist")})
	rec := postScreenTimelapse(t, svc, []byte("x"), screenParams("crobat", "periodic-screenshot-2026-10-22_14-00-54-display-1.heic", "2026-10-22T18:00:54Z"))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d", rec.Code)
	}
}

// Driven by tests/test_screen_timelapse_store.py against Python-provisioned
// pdw_test_* schemas; go test alone never guesses a database.
func TestScreenTimelapseRowStoreIntegration(t *testing.T) {
	dsn := os.Getenv("PDW_SCREEN_TIMELAPSE_TEST_DATABASE_URL")
	pythonSchema := os.Getenv("PDW_SCREEN_TIMELAPSE_TEST_SCHEMA")
	goSchema := os.Getenv("PDW_SCREEN_TIMELAPSE_TEST_GO_SCHEMA")
	if dsn == "" || pythonSchema == "" || goSchema == "" {
		t.Skip("exercised by tests/test_screen_timelapse_store.py in uv run pytest")
	}
	if !strings.HasPrefix(pythonSchema, "pdw_test_") || !strings.HasPrefix(goSchema, "pdw_test_") {
		t.Fatal("integration test requires isolated test schemas")
	}
	ctx := context.Background()
	at := time.Date(2026, 10, 22, 18, 0, 54, 0, time.UTC)
	for _, schema := range []string{pythonSchema, goSchema} {
		store, err := newScreenTimelapseRowStore(dsn, 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		store.screenshots = warehouse.QuoteIdent(schema) + `."screenshots"`
		store.trees = warehouse.QuoteIdent(schema) + `."accessibility_trees"`
		store.schema = schema
		for _, sha := range []string{"sha-a", "sha-b"} {
			for _, row := range []screenTimelapseRow{
				{Kind: timelapsefiles.Screenshot, Device: "crobat", CapturedAt: at, DisplayIndex: 1, SourceFilename: "s.heic", SHA256: sha, MimeType: "image/heic", SizeBytes: 10, Stored: objectstore.StoredObject{StorageBackend: "google_drive", StorageKey: "k", StorageFileID: "f-" + sha}, IngestedAt: at},
				{Kind: timelapsefiles.AccessibilityTree, Device: "crobat", CapturedAt: at, SourceFilename: "t.ax.json.gz", SHA256: sha, MimeType: "application/gzip", SizeBytes: 5, Stored: objectstore.StoredObject{StorageBackend: "google_drive", StorageKey: "k", StorageFileID: "f-" + sha}, IngestedAt: at},
			} {
				if err := store.upsert(ctx, row); err != nil {
					t.Fatalf("%s upsert %s: %v", schema, row.Kind, err)
				}
				got, ok, err := store.existing(ctx, row.Kind, row.Device, row.CapturedAt, row.DisplayIndex)
				if err != nil || !ok || got.SHA256 != sha || got.Stored.StorageFileID != "f-"+sha {
					t.Fatalf("%s existing %s = %+v %v %v", schema, row.Kind, got, ok, err)
				}
			}
		}
		var n int
		if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM `+store.screenshots).Scan(&n); err != nil || n != 1 {
			t.Fatalf("%s: a re-sent moment must replace its row, have %d (%v)", schema, n, err)
		}
		_ = store.Close()
	}
}
