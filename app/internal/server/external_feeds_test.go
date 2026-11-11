package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
)

type feedRunner struct{ calls int }

func (f *feedRunner) Query(context.Context, string, int) (query.RawResult, error) {
	f.calls++
	return query.RawResult{Rows: []map[string]any{{"recording_id": "memo", "transcript": "words"}}}, nil
}

func TestExternalFeedMuxAuthorizationAndUsage(t *testing.T) {
	authSvc := pdwauth.NewService([]byte(muxAPITestSecret), time.Now)
	mux := NewMux(config.Config{SecretToken: muxAPITestSecret}, authSvc, &feedRunner{})
	call := func(method, path, key, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer test:"+key)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}
	registry := call("GET", "/api/external-feeds", muxAPITestSecret, "")
	if registry.Code != 200 || !strings.Contains(registry.Body.String(), "Voice memos") || strings.Contains(registry.Body.String(), "SourceSQL") || strings.Contains(registry.Body.String(), "SELECT") {
		t.Fatalf("registry: %d %s", registry.Code, registry.Body.String())
	}
	created := call("POST", "/api/auth/tokens", muxAPITestSecret, `{"client_name":"consumer","scope":"feed:voice-memos","expires_at":"2099-01-01T00:00:00Z"}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("issue %d %s", created.Code, created.Body.String())
	}
	var issued struct {
		Token string
		Info  struct{ ID string }
	}
	if err := json.Unmarshal(created.Body.Bytes(), &issued); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/tools", "/api/auth/tokens", "/api/connections", "/api/external-feeds"} {
		if w := call("GET", path, issued.Token, ""); w.Code != 403 {
			t.Fatalf("%s: %d", path, w.Code)
		}
	}
	if w := call("GET", "/api/external-feeds/voice-memos", muxAPITestSecret, ""); w.Code != 403 {
		t.Fatal("master must issue a dedicated key")
	}
	for i := 0; i < 2; i++ {
		if w := call("GET", "/api/external-feeds/voice-memos", issued.Token, ""); w.Code != 200 {
			t.Fatalf("feed %d %s", w.Code, w.Body.String())
		}
	}
	listing := call("GET", "/api/auth/tokens", muxAPITestSecret, "")
	if !strings.Contains(listing.Body.String(), `"request_count":2`) || strings.Contains(listing.Body.String(), issued.Token) {
		t.Fatal("incorrect usage or secret leaked")
	}
	if w := call("DELETE", "/api/auth/tokens/"+issued.Info.ID, muxAPITestSecret, ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if w := call("GET", "/api/external-feeds/voice-memos", issued.Token, ""); w.Code != 401 {
		t.Fatal("revoked key accepted")
	}
}
