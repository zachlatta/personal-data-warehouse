package mcpproxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/oauth2"
)

// expiredConnection stores a connection whose access token has expired, so
// the next call must refresh it at tokenHandler's /token.
func expiredConnection(t *testing.T, tokenHandler http.HandlerFunc) (*Service, *memoryStore, *int) {
	t.Helper()
	refreshes := 0
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			refreshes++
			tokenHandler(w, r)
			return
		}
		w.WriteHeader(503)
	}))
	t.Cleanup(remote.Close)
	store := newMemoryStore()
	service := New(store, "https://pdw.example", remote.Client())
	store.Update(context.Background(), "warehouse", func(c *record) error {
		c.URL = remote.URL + "/mcp"
		c.Enabled = true
		c.AllClients = true
		c.Status = "connected"
		c.OAuth = oauth2.Config{ClientID: "client", Endpoint: oauth2.Endpoint{TokenURL: remote.URL + "/token", AuthStyle: oauth2.AuthStyleInParams}}
		c.Token = oauth2.Token{AccessToken: "expired-access-value", RefreshToken: "old-refresh-value", Expiry: time.Now().Add(-time.Hour)}
		c.Tools = []*mcp.Tool{{Name: "query", InputSchema: map[string]any{"type": "object"}}}
		return nil
	})
	return service, store, &refreshes
}

func invokeFirstTool(t *testing.T, service *Service) error {
	t.Helper()
	defs, err := service.Tools(context.Background())
	if err != nil || len(defs) != 1 {
		t.Fatalf("tools = %d, %v", len(defs), err)
	}
	_, _, err = defs[0].Invoke(context.Background(), json.RawMessage(`{}`))
	return err
}

func storedConnection(t *testing.T, store *memoryStore) record {
	t.Helper()
	records, _ := store.List(context.Background())
	if len(records) != 1 {
		t.Fatalf("records = %d", len(records))
	}
	return records[0]
}

// 2026-11-22..23: every refresh error, whatever it was, became "upstream
// authorization expired; reconnect in Connections" and flipped the status. A
// token endpoint that is down is not an expired grant.
func TestATokenEndpoint500IsTransientAndLeavesTheStatusAlone(t *testing.T) {
	service, store, refreshes := expiredConnection(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`upstream exploded`))
	})
	notified := 0
	service.OnAuthorizationFailed(func(string, string) { notified++ })

	err := invokeFirstTool(t, service)
	if err == nil || !strings.Contains(err.Error(), "transient") {
		t.Fatalf("error = %v, want a transient refresh failure", err)
	}
	if *refreshes != 2 {
		t.Fatalf("token endpoint called %d times, want one retry (2)", *refreshes)
	}
	got := storedConnection(t, store)
	if got.Status != "connected" {
		t.Fatalf("status = %q, want unchanged connected", got.Status)
	}
	if !strings.HasPrefix(got.LastError, "transient:") || !strings.Contains(got.LastError, "500") {
		t.Fatalf("last error = %q", got.LastError)
	}
	if notified != 0 {
		t.Fatalf("notified %d times for a transient failure", notified)
	}
}

func TestInvalidGrantIsAuthorizationFailedWithItsCauseAndNotifiesOnce(t *testing.T) {
	service, store, refreshes := expiredConnection(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"invalid_grant","error_description":"refresh token revoked"}`))
	})
	var mu sync.Mutex
	var alerts []string
	service.OnAuthorizationFailed(func(name, cause string) {
		mu.Lock()
		defer mu.Unlock()
		alerts = append(alerts, name+": "+cause)
	})

	err := invokeFirstTool(t, service)
	if err == nil || !strings.Contains(err.Error(), "invalid_grant") || !strings.Contains(err.Error(), "reconnect in Connections") {
		t.Fatalf("error = %v, want the invalid_grant cause and the reconnect hint", err)
	}
	if *refreshes != 1 {
		t.Fatalf("token endpoint called %d times; a refused grant is not retried", *refreshes)
	}
	got := storedConnection(t, store)
	if got.Status != "authorization_failed" || got.StatusChangedAt.IsZero() {
		t.Fatalf("status = %q changed at %v", got.Status, got.StatusChangedAt)
	}
	if !strings.Contains(got.LastError, "invalid_grant") || !strings.Contains(got.LastError, "refresh token revoked") {
		t.Fatalf("last error = %q, want the cause", got.LastError)
	}
	for _, secret := range []string{"expired-access-value", "old-refresh-value"} {
		if strings.Contains(got.LastError, secret) {
			t.Fatalf("last error leaks a credential: %q", got.LastError)
		}
	}

	// A second failing call is still authorization_failed but not a new alert.
	_ = invokeFirstTool(t, service)
	mu.Lock()
	defer mu.Unlock()
	if len(alerts) != 1 || !strings.HasPrefix(alerts[0], "warehouse: ") || !strings.Contains(alerts[0], "invalid_grant") {
		t.Fatalf("alerts = %q, want exactly one naming the connection and cause", alerts)
	}
}

func TestA401FromTheTokenEndpointIsAuthorizationFailed(t *testing.T) {
	service, store, _ := expiredConnection(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	if err := invokeFirstTool(t, service); err == nil {
		t.Fatal("expected a failure")
	}
	if got := storedConnection(t, store); got.Status != "authorization_failed" || !strings.Contains(got.LastError, "401") {
		t.Fatalf("status = %q, last error %q", got.Status, got.LastError)
	}
}
