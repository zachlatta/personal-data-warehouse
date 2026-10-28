package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/accesstokens"
	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
)

func mcpToolsListStatus(t *testing.T, srvURL string, client *http.Client, token string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srvURL+"/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", "2025-06-18")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// End to end through the app: a connector's login is a row `pdw token list`
// shows, and `pdw token revoke` (the same API) turns it off.
func TestRevokingAConnectorGrantThroughTheTokenAPIStopsItsMCPAccess(t *testing.T) {
	srv := newMCPTestServer(t)
	token := oauthAccessToken(t, srv)
	if code := mcpToolsListStatus(t, srv.URL, srv.Client(), token); code != http.StatusOK {
		t.Fatalf("/mcp before revoke = %d", code)
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+accesstokens.TokensPath, nil)
	req.Header.Set("Authorization", "Bearer admin:"+muxAPITestSecret)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var listed struct {
		Tokens []accesstokens.TokenView `json:"tokens"`
	}
	err = json.NewDecoder(resp.Body).Decode(&listed)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	var grantID string
	for _, tok := range listed.Tokens {
		if tok.Kind == accesstokens.KindOAuth && tok.ClientName == "claude-test" {
			grantID = tok.ID
		}
	}
	if grantID == "" {
		t.Fatalf("connector grant not listed: %+v", listed.Tokens)
	}

	req, _ = http.NewRequest(http.MethodDelete, srv.URL+accesstokens.TokensPath+"/"+grantID, nil)
	req.Header.Set("Authorization", "Bearer admin:"+muxAPITestSecret)
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("revoke status = %d", resp.StatusCode)
	}
	if code := mcpToolsListStatus(t, srv.URL, srv.Client(), token); code != http.StatusUnauthorized {
		t.Fatalf("/mcp after revoke = %d, want 401", code)
	}
}

// A connector's grant must never read as unknown or revoked just because the
// database is unusable: that answer logs it out for good.
func TestAnUnusableTokenDatabaseNeverAnswersAGrantUnknown(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newAccessTokenService(config.Config{PostgresDatabaseURL: "postgres://user@[::1/db"}, logger)
	err := svc.CheckOAuthGrant(context.Background(), "anygrant00000000")
	var rejected *pdwauth.TokenRejectedError
	if err == nil || errors.Is(err, pdwauth.ErrUnknownToken) || errors.As(err, &rejected) {
		t.Fatalf("grant check against an unusable database = %v, want a store error", err)
	}
	// With no database configured at all (a bare local run), memory is the
	// store and grants work in it.
	local := newAccessTokenService(config.Config{}, logger)
	id, err := local.CreateOAuthGrant(context.Background(), "claude")
	if err != nil {
		t.Fatal(err)
	}
	if err := local.CheckOAuthGrant(context.Background(), id); err != nil {
		t.Fatal(err)
	}
}
