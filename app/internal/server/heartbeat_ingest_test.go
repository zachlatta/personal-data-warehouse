package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/push"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

type fakeUploaderHeartbeatStore struct {
	calls  int
	last   uploaderHeartbeat
	err    error
	streak int64
}

func (f *fakeUploaderHeartbeatStore) upsert(_ context.Context, hb uploaderHeartbeat) (uploaderHeartbeatStreak, error) {
	f.calls++
	f.last = hb
	if hb.ExitCode != 0 {
		f.streak++
	} else {
		f.streak = 0
	}
	return uploaderHeartbeatStreak{ConsecutiveFailures: f.streak, FailingSince: time.Date(2026, 9, 30, 17, 24, 0, 0, time.UTC)}, f.err
}

func heartbeatTestService(store *fakeUploaderHeartbeatStore) *heartbeatIngestService {
	return &heartbeatIngestService{
		store:    store,
		signer:   objectsTestSigner(),
		maxBytes: 4096,
		timeout:  time.Second,
		logger:   slog.Default(),
	}
}

func postHeartbeat(t *testing.T, svc *heartbeatIngestService, target string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body))
	svc.handler().ServeHTTP(rec, req)
	return rec
}

func TestHeartbeatIngestStoresASignedRun(t *testing.T) {
	store := &fakeUploaderHeartbeatStore{}
	svc := heartbeatTestService(store)
	body, err := json.Marshal(uploaderHeartbeat{
		Pipeline:        " apple_notes ",
		Device:          " porygon ",
		RanAt:           "2026-08-27T03:00:00Z",
		ExitCode:        1,
		DurationSeconds: 12,
		Error:           " PermissionError: Operation not permitted ",
	})
	if err != nil {
		t.Fatal(err)
	}
	target := signedIngestTarget(uploaderHeartbeatEndpoint, body, url.Values{})

	rec := postHeartbeat(t, svc, target, body)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if store.calls != 1 {
		t.Fatalf("upsert calls = %d, want 1", store.calls)
	}
	if store.last.Pipeline != "apple_notes" || store.last.Device != "porygon" {
		t.Fatalf("keys = %q/%q", store.last.Pipeline, store.last.Device)
	}
	if store.last.ExitCode != 1 || store.last.DurationSeconds != 12 {
		t.Fatalf("run facts = %+v", store.last)
	}
	if store.last.Error != "PermissionError: Operation not permitted" {
		t.Fatalf("error = %q", store.last.Error)
	}
}

func TestHeartbeatIngestRequiresPipelineAndDevice(t *testing.T) {
	store := &fakeUploaderHeartbeatStore{}
	svc := heartbeatTestService(store)
	body := []byte(`{"pipeline":"","device":"porygon","exit_code":0}`)
	target := signedIngestTarget(uploaderHeartbeatEndpoint, body, url.Values{})

	rec := postHeartbeat(t, svc, target, body)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if store.calls != 0 {
		t.Fatalf("upsert calls = %d, want 0", store.calls)
	}
}

func TestHeartbeatIngestRejectsAnUnsignedPost(t *testing.T) {
	store := &fakeUploaderHeartbeatStore{}
	svc := heartbeatTestService(store)
	body := []byte(`{"pipeline":"apple_notes","device":"porygon","exit_code":0}`)

	rec := postHeartbeat(t, svc, uploaderHeartbeatEndpoint, body)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if store.calls != 0 {
		t.Fatalf("upsert calls = %d, want 0", store.calls)
	}
}

func TestHeartbeatIngestTruncatesLongErrors(t *testing.T) {
	store := &fakeUploaderHeartbeatStore{}
	svc := heartbeatTestService(store)
	body, _ := json.Marshal(uploaderHeartbeat{Pipeline: "photos", Device: "crobat", Error: strings.Repeat("x", 900)})
	target := signedIngestTarget(uploaderHeartbeatEndpoint, body, url.Values{})

	rec := postHeartbeat(t, svc, target, body)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if len(store.last.Error) != 500 {
		t.Fatalf("error length = %d, want 500", len(store.last.Error))
	}
}

// A failing uploader earns exactly one push, on the run that makes the streak
// long enough to not be a blip; a success resets it so the next streak alerts
// again.
func TestHeartbeatIngestAlertsOnceWhenAnUploaderKeepsFailing(t *testing.T) {
	store := &fakeUploaderHeartbeatStore{}
	svc := heartbeatTestService(store)
	var sent []push.Notification
	svc.notify = func(n push.Notification) { sent = append(sent, n) }
	post := func(exitCode int64) {
		body, _ := json.Marshal(uploaderHeartbeat{
			Pipeline: "apple_voice_memos", Device: "porygon", RanAt: "2026-09-30T18:00:00Z", ExitCode: exitCode,
			Error: "20260930 102329-2DAE4BC0.qta has failed 6 consecutive run(s) since 2026-09-30T17:24:00+00:00: HTTP 504",
		})
		if rec := postHeartbeat(t, svc, signedIngestTarget(uploaderHeartbeatEndpoint, body, url.Values{}), body); rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
	}
	for i := 0; i < uploaderFailureAlertRuns-1; i++ {
		post(1)
	}
	if len(sent) != 0 {
		t.Fatalf("alerted before the streak threshold: %+v", sent)
	}
	post(1)
	if len(sent) != 1 {
		t.Fatalf("alerts after %d failures = %d, want 1", uploaderFailureAlertRuns, len(sent))
	}
	n := sent[0]
	if err := n.Validate(); err != nil {
		t.Fatalf("notification invalid: %v", err)
	}
	if n.Title != "Uploader failing: apple_voice_memos" || !strings.Contains(n.Subtitle, "6 runs in a row on porygon") ||
		!strings.Contains(n.Body, "2DAE4BC0.qta") || n.CollapseID != "uploader-failing:apple_voice_memos:porygon" {
		t.Fatalf("notification = %+v", n)
	}
	post(1)
	post(1)
	if len(sent) != 1 {
		t.Fatalf("a continuing streak must not re-alert: %d", len(sent))
	}
	post(0)
	for i := 0; i < uploaderFailureAlertRuns; i++ {
		post(1)
	}
	if len(sent) != 2 {
		t.Fatalf("a new streak after a success must alert again: %d", len(sent))
	}
}

// Driven by tests/test_upload_heartbeat_store.py against a Python-provisioned
// pdw_test_* schema; go test alone never guesses a database.
func TestUploaderHeartbeatStoreStreakIntegration(t *testing.T) {
	url := os.Getenv("PDW_HEARTBEAT_TEST_DATABASE_URL")
	schema := os.Getenv("PDW_HEARTBEAT_TEST_SCHEMA")
	if url == "" || schema == "" {
		t.Skip("exercised by tests/test_upload_heartbeat_store.py in uv run pytest")
	}
	if !strings.HasPrefix(schema, "pdw_test_") {
		t.Fatal("integration test requires an isolated test schema")
	}
	store, err := newUploaderHeartbeatStore(url, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.schema = schema
	store.relation = warehouse.QuoteIdent(schema) + `."uploader_heartbeats"`
	ctx := context.Background()
	// A table from before the streak columns existed is brought up to date.
	if _, err := store.db.ExecContext(ctx, `ALTER TABLE `+store.relation+` DROP COLUMN consecutive_failures, DROP COLUMN failing_since`); err != nil {
		t.Fatal(err)
	}
	run := func(ranAt string, exitCode int64) uploaderHeartbeatStreak {
		t.Helper()
		streak, err := store.upsert(ctx, uploaderHeartbeat{Pipeline: "apple_voice_memos", Device: "porygon", RanAt: ranAt, ExitCode: exitCode, Error: "boom"})
		if err != nil {
			t.Fatal(err)
		}
		return streak
	}
	if got := run("2026-09-30T17:00:00Z", 0); got.ConsecutiveFailures != 0 {
		t.Fatalf("success streak = %+v", got)
	}
	first := run("2026-09-30T17:24:00Z", 1)
	second := run("2026-09-30T17:29:00Z", 1)
	since := time.Date(2026, 9, 30, 17, 24, 0, 0, time.UTC)
	if first.ConsecutiveFailures != 1 || second.ConsecutiveFailures != 2 || !second.FailingSince.Equal(since) {
		t.Fatalf("streak = %+v then %+v", first, second)
	}
	// A replayed older post changes nothing.
	if stale := run("2026-09-30T17:10:00Z", 1); stale.ConsecutiveFailures != 0 {
		t.Fatalf("stale replay = %+v", stale)
	}
	var stored int64
	if err := store.db.QueryRowContext(ctx, `SELECT consecutive_failures FROM `+store.relation).Scan(&stored); err != nil || stored != 2 {
		t.Fatalf("stored streak = %d (%v)", stored, err)
	}
	if reset := run("2026-09-30T17:34:00Z", 0); reset.ConsecutiveFailures != 0 || reset.FailingSince.Unix() != 0 {
		t.Fatalf("reset = %+v", reset)
	}
}
