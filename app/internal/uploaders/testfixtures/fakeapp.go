package testfixtures

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// FakeApp is an httptest stand-in for the app's /ingest endpoints: it accepts
// every signed POST, records the endpoint and query, and answers with a
// stored-object reference. It verifies nothing -- the signing scheme has its
// own tests in ingestclient.
type FakeApp struct {
	Server *httptest.Server
	mu     sync.Mutex
	calls  []FakeCall
}

// FakeCall is one recorded request.
type FakeCall struct {
	Path  string
	Query map[string]string
	Body  []byte
}

// NewFakeApp starts the server and closes it with the test.
func NewFakeApp(t *testing.T) *FakeApp {
	t.Helper()
	app := &FakeApp{}
	app.Server = httptest.NewServer(http.HandlerFunc(app.handle))
	t.Cleanup(app.Server.Close)
	return app
}

func (a *FakeApp) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		return
	}
	body := make([]byte, 0)
	if r.Body != nil {
		buf := new(strings.Builder)
		chunk := make([]byte, 32*1024)
		for {
			n, err := r.Body.Read(chunk)
			buf.Write(chunk[:n])
			if err != nil {
				break
			}
		}
		body = []byte(buf.String())
	}
	query := map[string]string{}
	for key := range r.URL.Query() {
		query[key] = r.URL.Query().Get(key)
	}
	a.mu.Lock()
	a.calls = append(a.calls, FakeCall{Path: r.URL.Path, Query: query, Body: body})
	a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"storage_backend": "google_drive",
		"storage_key":     strings.TrimPrefix(r.URL.Path, "/ingest/") + "/object",
		"storage_file_id": "file-id",
		"storage_url":     "https://drive.example/file-id",
		"ok":              true,
	})
}

// URL is the fake app's base URL.
func (a *FakeApp) URL() string { return a.Server.URL }

// Calls returns every recorded request.
func (a *FakeApp) Calls() []FakeCall {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]FakeCall(nil), a.calls...)
}

// Paths returns the endpoint of every recorded request, in order.
func (a *FakeApp) Paths() []string {
	var out []string
	for _, call := range a.Calls() {
		out = append(out, call.Path)
	}
	return out
}
