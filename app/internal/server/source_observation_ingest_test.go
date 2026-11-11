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
)

type fakeSourceObservationStore struct {
	calls int
	last  sourceObservation
}

func (f *fakeSourceObservationStore) upsert(_ context.Context, obs sourceObservation) error {
	f.calls++
	f.last = obs
	return nil
}

func sourceObservationTestService(store *fakeSourceObservationStore) *sourceObservationIngestService {
	return &sourceObservationIngestService{
		store:    store,
		signer:   objectsTestSigner(),
		maxBytes: 4096,
		timeout:  time.Second,
		logger:   slog.Default(),
	}
}

func postSourceObservation(t *testing.T, svc *sourceObservationIngestService, target string, body []byte) int {
	t.Helper()
	rec := httptest.NewRecorder()
	svc.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body)))
	return rec.Code
}

func TestSourceObservationIngestStoresASignedObservation(t *testing.T) {
	store := &fakeSourceObservationStore{}
	svc := sourceObservationTestService(store)
	body, _ := json.Marshal(sourceObservation{
		Pipeline:         " apple_messages ",
		Scope:            " zach@example.com ",
		UpstreamNewestAt: "2026-11-08T20:04:04.6808Z",
		UpstreamNewestID: "p:0/ABC",
		Detail:           "chat.db newest message",
		ObservedAt:       "2026-11-11T12:00:00Z",
	})
	code := postSourceObservation(t, svc, signedIngestTarget(sourceObservationEndpoint, body, url.Values{}), body)
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if store.calls != 1 || store.last.Pipeline != "apple_messages" || store.last.Scope != "zach@example.com" {
		t.Fatalf("stored = %+v (calls %d)", store.last, store.calls)
	}
	if store.last.UpstreamNewestID != "p:0/ABC" {
		t.Fatalf("newest id = %q", store.last.UpstreamNewestID)
	}
}

func TestSourceObservationIngestRejectsBadInput(t *testing.T) {
	for name, body := range map[string]string{
		"no scope":      `{"pipeline":"apple_messages","scope":""}`,
		"no pipeline":   `{"pipeline":" ","scope":"a"}`,
		"bad timestamp": `{"pipeline":"apple_messages","scope":"a","upstream_newest_at":"yesterday"}`,
	} {
		store := &fakeSourceObservationStore{}
		svc := sourceObservationTestService(store)
		raw := []byte(body)
		code := postSourceObservation(t, svc, signedIngestTarget(sourceObservationEndpoint, raw, url.Values{}), raw)
		if code != http.StatusBadRequest || store.calls != 0 {
			t.Fatalf("%s: status = %d, calls = %d", name, code, store.calls)
		}
	}
}

func TestSourceObservationIngestRejectsAnUnsignedPost(t *testing.T) {
	store := &fakeSourceObservationStore{}
	svc := sourceObservationTestService(store)
	body := []byte(`{"pipeline":"apple_messages","scope":"a"}`)
	if code := postSourceObservation(t, svc, sourceObservationEndpoint, body); code != http.StatusForbidden || store.calls != 0 {
		t.Fatalf("status = %d, calls = %d", code, store.calls)
	}
}

// testdata/source_upstream_observations.ddl is the Python TableSpec, kept equal
// to it by tests/test_source_sla.py; the app's DDL must be that, line for line,
// because whichever side provisions first defines the table for both.
func TestSourceObservationDDLMatchesThePythonTableSpec(t *testing.T) {
	golden, err := os.ReadFile("testdata/source_upstream_observations.ddl")
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Split(strings.TrimSpace(string(golden)), "\n")
	ddl := sourceObservationDDL("ops", `"ops"."source_upstream_observations"`)
	create := ddl[len(ddl)-1]
	body := create[strings.Index(create, "(")+1 : strings.LastIndex(create, ")")]
	var got []string
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSuffix(strings.TrimSpace(line), ",")
		if line != "" {
			got = append(got, line)
		}
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("app DDL drifted from the Python TableSpec:\n got: %q\nwant: %q", got, want)
	}
}
