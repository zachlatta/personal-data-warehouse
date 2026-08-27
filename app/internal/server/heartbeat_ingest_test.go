package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type fakeUploaderHeartbeatStore struct {
	calls int
	last  uploaderHeartbeat
	err   error
}

func (f *fakeUploaderHeartbeatStore) upsert(_ context.Context, hb uploaderHeartbeat) error {
	f.calls++
	f.last = hb
	return f.err
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
