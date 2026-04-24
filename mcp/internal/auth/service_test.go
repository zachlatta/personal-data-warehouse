package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestOAuthFlowAcceptsSecretAndIssuesBearerToken(t *testing.T) {
	svc := NewService([]byte("setup-secret"), func() time.Time { return time.Unix(1000, 0) })
	mux := http.NewServeMux()
	svc.RegisterHandlers(mux, "https://mcp.example.com")

	regBody := `{"redirect_uris":["https://claude.ai/api/mcp/auth_callback"],"token_endpoint_auth_method":"none"}`
	regReq := httptest.NewRequest(http.MethodPost, "/oauth/register", strings.NewReader(regBody))
	regReq.Header.Set("Content-Type", "application/json")
	regRec := httptest.NewRecorder()
	mux.ServeHTTP(regRec, regReq)
	if regRec.Code != http.StatusCreated {
		t.Fatalf("register status = %d body=%s", regRec.Code, regRec.Body.String())
	}
	var reg struct {
		ClientID string `json:"client_id"`
	}
	if err := json.Unmarshal(regRec.Body.Bytes(), &reg); err != nil {
		t.Fatal(err)
	}
	if reg.ClientID == "" {
		t.Fatal("client_id is empty")
	}

	form := url.Values{
		"client_id":             {reg.ClientID},
		"redirect_uri":          {"https://claude.ai/api/mcp/auth_callback"},
		"response_type":         {"code"},
		"state":                 {"abc"},
		"code_challenge":        {s256Challenge("challenge")},
		"code_challenge_method": {"S256"},
		"secret_token":          {"setup-secret"},
	}
	authReq := httptest.NewRequest(http.MethodPost, "/oauth/authorize", strings.NewReader(form.Encode()))
	authReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	authRec := httptest.NewRecorder()
	mux.ServeHTTP(authRec, authReq)
	if authRec.Code != http.StatusFound {
		t.Fatalf("authorize status = %d body=%s", authRec.Code, authRec.Body.String())
	}
	redirectTo := authRec.Header().Get("Location")
	parsed, err := url.Parse(redirectTo)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Query().Get("state") != "abc" {
		t.Fatalf("state was not preserved in redirect %q", redirectTo)
	}
	code := parsed.Query().Get("code")
	if code == "" {
		t.Fatalf("redirect missing code: %q", redirectTo)
	}

	tokenForm := url.Values{
		"grant_type":    {"authorization_code"},
		"client_id":     {reg.ClientID},
		"code":          {code},
		"redirect_uri":  {"https://claude.ai/api/mcp/auth_callback"},
		"code_verifier": {"challenge"},
	}
	tokenReq := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(tokenForm.Encode()))
	tokenReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokenRec := httptest.NewRecorder()
	mux.ServeHTTP(tokenRec, tokenReq)
	if tokenRec.Code != http.StatusOK {
		t.Fatalf("token status = %d body=%s", tokenRec.Code, tokenRec.Body.String())
	}
	var token struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		TokenType    string `json:"token_type"`
	}
	if err := json.Unmarshal(tokenRec.Body.Bytes(), &token); err != nil {
		t.Fatal(err)
	}
	if token.AccessToken == "" || token.RefreshToken == "" || token.TokenType != "Bearer" {
		t.Fatalf("bad token response: %#v", token)
	}
	if _, err := svc.VerifyBearer(token.AccessToken); err != nil {
		t.Fatalf("VerifyBearer failed: %v", err)
	}

	replayReq := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(tokenForm.Encode()))
	replayReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	replayRec := httptest.NewRecorder()
	mux.ServeHTTP(replayRec, replayReq)
	if replayRec.Code != http.StatusBadRequest {
		t.Fatalf("replayed authorization code status = %d body=%s", replayRec.Code, replayRec.Body.String())
	}
}

func TestAuthorizeRejectsPlainPKCE(t *testing.T) {
	svc := NewService([]byte("setup-secret"), func() time.Time { return time.Unix(1000, 0) })
	mux := http.NewServeMux()
	svc.RegisterHandlers(mux, "https://mcp.example.com")
	clientID := registerTestClient(t, mux)

	form := url.Values{
		"client_id":             {clientID},
		"redirect_uri":          {"https://claude.ai/api/mcp/auth_callback"},
		"response_type":         {"code"},
		"code_challenge":        {"plain-verifier"},
		"code_challenge_method": {"plain"},
		"secret_token":          {"setup-secret"},
	}
	authReq := httptest.NewRequest(http.MethodPost, "/oauth/authorize", strings.NewReader(form.Encode()))
	authReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	authRec := httptest.NewRecorder()
	mux.ServeHTTP(authRec, authReq)
	if authRec.Code != http.StatusBadRequest {
		t.Fatalf("authorize status = %d body=%s", authRec.Code, authRec.Body.String())
	}
}

func registerTestClient(t *testing.T, mux *http.ServeMux) string {
	t.Helper()
	regBody := `{"redirect_uris":["https://claude.ai/api/mcp/auth_callback"],"token_endpoint_auth_method":"none"}`
	regReq := httptest.NewRequest(http.MethodPost, "/oauth/register", strings.NewReader(regBody))
	regReq.Header.Set("Content-Type", "application/json")
	regRec := httptest.NewRecorder()
	mux.ServeHTTP(regRec, regReq)
	if regRec.Code != http.StatusCreated {
		t.Fatalf("register status = %d body=%s", regRec.Code, regRec.Body.String())
	}
	var reg struct {
		ClientID string `json:"client_id"`
	}
	if err := json.Unmarshal(regRec.Body.Bytes(), &reg); err != nil {
		t.Fatal(err)
	}
	return reg.ClientID
}

func s256Challenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func TestProtectedResourceMetadataAndChallenge(t *testing.T) {
	svc := NewService([]byte("setup-secret"), func() time.Time { return time.Unix(1000, 0) })
	mux := http.NewServeMux()
	svc.RegisterHandlers(mux, "https://mcp.example.com")
	protected := svc.RequireBearer("https://mcp.example.com/.well-known/oauth-protected-resource")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	mux.Handle("/protected", protected)

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Header().Get("WWW-Authenticate"), "resource_metadata") {
		t.Fatalf("missing metadata challenge: %q", rec.Header().Get("WWW-Authenticate"))
	}
}
