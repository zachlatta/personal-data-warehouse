package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeGrants is the OAuthGrants contract in memory. The real store is
// accesstokens.Service, tested in its own package.
type fakeGrants struct {
	mu      sync.Mutex
	rows    map[string]*fakeGrant
	next    int
	down    error
	created int
}

type fakeGrant struct {
	client  string
	revoked bool
}

func newFakeGrants() *fakeGrants { return &fakeGrants{rows: map[string]*fakeGrant{}} }

func (f *fakeGrants) CreateOAuthGrant(_ context.Context, client string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down != nil {
		return "", f.down
	}
	f.next++
	id := "grant" + strings.Repeat("x", f.next)
	f.rows[id] = &fakeGrant{client: client}
	f.created++
	return id, nil
}

func (f *fakeGrants) EnsureOAuthGrant(ctx context.Context, id, client string) error {
	f.mu.Lock()
	if f.down != nil {
		f.mu.Unlock()
		return f.down
	}
	if _, ok := f.rows[id]; !ok {
		f.rows[id] = &fakeGrant{client: client}
		f.created++
	}
	f.mu.Unlock()
	return f.CheckOAuthGrant(ctx, id)
}

func (f *fakeGrants) CheckOAuthGrant(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down != nil {
		return f.down
	}
	g, ok := f.rows[id]
	switch {
	case !ok:
		return ErrUnknownToken
	case g.revoked:
		return &TokenRejectedError{Reason: "connector grant " + id + " was revoked"}
	}
	return nil
}

func (f *fakeGrants) revoke(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows[id].revoked = true
}

func (f *fakeGrants) setDown(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = err
}

const testRedirect = "https://claude.ai/api/mcp/auth_callback"

type tokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

func newGrantTestService(t *testing.T, ring *Keyring) (*Service, *fakeGrants, *http.ServeMux) {
	t.Helper()
	return newGrantTestServiceAt(t, ring, func() time.Time { return time.Unix(1000, 0) })
}

// newGrantTestServiceAt is for tests that go through RequireBearer, which
// compares expiry with the wall clock: pass time.Now.
func newGrantTestServiceAt(t *testing.T, ring *Keyring, now func() time.Time) (*Service, *fakeGrants, *http.ServeMux) {
	t.Helper()
	svc := NewServiceWithKeyring(ring, now)
	grants := newFakeGrants()
	svc.SetOAuthGrants(grants)
	mux := http.NewServeMux()
	svc.RegisterHandlers(mux, "https://mcp.example.com")
	return svc, grants, mux
}

func postForm(mux *http.ServeMux, path string, form url.Values) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func registerClient(t *testing.T, mux *http.ServeMux) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/oauth/register", strings.NewReader(`{"redirect_uris":["`+testRedirect+`"]}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("register status = %d body=%s", rec.Code, rec.Body.String())
	}
	var reg struct {
		ClientID string `json:"client_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &reg); err != nil {
		t.Fatal(err)
	}
	return reg.ClientID
}

// connect runs authorize and the code exchange with secret typed at the
// consent page.
func connect(t *testing.T, mux *http.ServeMux, clientID, secret string) tokenPair {
	t.Helper()
	rec := postForm(mux, "/oauth/authorize", url.Values{
		"client_id": {clientID}, "redirect_uri": {testRedirect}, "response_type": {"code"},
		"code_challenge": {s256Challenge("verifier")}, "code_challenge_method": {"S256"},
		"secret_token": {secret}, "client_name": {"claude-test"},
	})
	if rec.Code != http.StatusFound {
		t.Fatalf("authorize status = %d body=%s", rec.Code, rec.Body.String())
	}
	location, err := url.Parse(rec.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	rec = postForm(mux, "/oauth/token", url.Values{
		"grant_type": {"authorization_code"}, "client_id": {clientID}, "code": {location.Query().Get("code")},
		"redirect_uri": {testRedirect}, "code_verifier": {"verifier"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("token status = %d body=%s", rec.Code, rec.Body.String())
	}
	var pair tokenPair
	if err := json.Unmarshal(rec.Body.Bytes(), &pair); err != nil {
		t.Fatal(err)
	}
	return pair
}

func refresh(mux *http.ServeMux, clientID, refreshToken string) *httptest.ResponseRecorder {
	return postForm(mux, "/oauth/token", url.Values{"grant_type": {"refresh_token"}, "client_id": {clientID}, "refresh_token": {refreshToken}})
}

func decodePair(t *testing.T, rec *httptest.ResponseRecorder) tokenPair {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("token status = %d body=%s", rec.Code, rec.Body.String())
	}
	var pair tokenPair
	if err := json.Unmarshal(rec.Body.Bytes(), &pair); err != nil {
		t.Fatal(err)
	}
	return pair
}

func oauthErrorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return body.Error
}

func payloadOf(t *testing.T, token string) signedPayload {
	t.Helper()
	parts := strings.Split(token, ".")
	data, err := base64.RawURLEncoding.DecodeString(parts[len(parts)-2])
	if err != nil {
		t.Fatal(err)
	}
	var p signedPayload
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

// legacySign is the "<body>.<sig>" format every token and client_id was
// issued in before key ids and grants: an HMAC of the body under the raw
// master secret. Production connectors hold values in exactly this shape.
func legacySign(t *testing.T, secret string, payload signedPayload) string {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	body := base64.RawURLEncoding.EncodeToString(data)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return body + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestOAuthTokensCarryARevocableGrant(t *testing.T) {
	svc, grants, mux := newGrantTestService(t, NewKeyring([]byte("setup-secret")))
	clientID := registerClient(t, mux)
	pair := connect(t, mux, clientID, "setup-secret")

	gid := payloadOf(t, pair.AccessToken).GrantID
	if gid == "" || payloadOf(t, pair.RefreshToken).GrantID != gid {
		t.Fatalf("access and refresh tokens must name one grant; got %q and %q", gid, payloadOf(t, pair.RefreshToken).GrantID)
	}
	if grants.rows[gid].client != "claude-test" {
		t.Fatalf("grant client = %q", grants.rows[gid].client)
	}
	if _, err := svc.VerifyBearer(context.Background(), pair.AccessToken); err != nil {
		t.Fatalf("fresh access token rejected: %v", err)
	}
	refreshed := decodePair(t, refresh(mux, clientID, pair.RefreshToken))
	if payloadOf(t, refreshed.AccessToken).GrantID != gid {
		t.Fatal("a refresh must stay on the same grant")
	}

	grants.revoke(gid)
	if _, err := svc.VerifyBearer(context.Background(), pair.AccessToken); err == nil {
		t.Fatal("access token of a revoked grant still verifies")
	}
	if _, err := svc.VerifyBearer(context.Background(), refreshed.AccessToken); err == nil {
		t.Fatal("refreshed access token of a revoked grant still verifies")
	}
	rec := refresh(mux, clientID, refreshed.RefreshToken)
	if rec.Code != http.StatusBadRequest || oauthErrorCode(t, rec) != "invalid_grant" {
		t.Fatalf("refresh on a revoked grant: status %d body %s; want 400 invalid_grant", rec.Code, rec.Body.String())
	}
}

func TestRevokedGrantIsUnauthorizedAtTheMCPEndpoint(t *testing.T) {
	svc, grants, mux := newGrantTestServiceAt(t, NewKeyring([]byte("setup-secret")), time.Now)
	pair := connect(t, mux, registerClient(t, mux), "setup-secret")
	if code := mcpStatus(svc, pair.AccessToken); code != http.StatusOK {
		t.Fatalf("/mcp with a live grant = %d, want 200", code)
	}
	grants.revoke(payloadOf(t, pair.AccessToken).GrantID)
	if code := mcpStatus(svc, pair.AccessToken); code != http.StatusUnauthorized {
		t.Fatalf("/mcp with a revoked grant = %d, want 401", code)
	}
}

func mcpStatus(svc *Service, token string) int {
	handler := svc.RequireBearer("https://mcp.example.com/.well-known/oauth-protected-resource")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec.Code
}

func TestGrantStoreOutageIsTemporaryNeverALogout(t *testing.T) {
	svc, grants, mux := newGrantTestService(t, NewKeyring([]byte("setup-secret")))
	clientID := registerClient(t, mux)
	pair := connect(t, mux, clientID, "setup-secret")
	live, liveGrants, liveMux := newGrantTestServiceAt(t, NewKeyring([]byte("setup-secret")), time.Now)
	liveToken := connect(t, liveMux, registerClient(t, liveMux), "setup-secret").AccessToken
	liveGrants.setDown(errors.New("connection refused"))
	grants.setDown(errors.New("connection refused"))

	rec := refresh(mux, clientID, pair.RefreshToken)
	if rec.Code != http.StatusServiceUnavailable || oauthErrorCode(t, rec) != "temporarily_unavailable" {
		t.Fatalf("refresh during an outage: status %d body %s; want 503 temporarily_unavailable", rec.Code, rec.Body.String())
	}
	_, err := svc.VerifyBearer(context.Background(), pair.AccessToken)
	var unavailable *GrantUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("VerifyBearer during an outage = %v, want GrantUnavailableError", err)
	}
	if code := mcpStatus(live, liveToken); code < 500 {
		t.Fatalf("/mcp during an outage = %d, want a 5xx (a 401 makes the connector drop its login)", code)
	}

	// The code exchange fails temporarily too, and the code stays usable.
	grants.setDown(nil)
	authRec := postForm(mux, "/oauth/authorize", url.Values{
		"client_id": {clientID}, "redirect_uri": {testRedirect}, "response_type": {"code"},
		"code_challenge": {s256Challenge("verifier")}, "code_challenge_method": {"S256"},
		"secret_token": {"setup-secret"}, "client_name": {"claude-test"},
	})
	location, _ := url.Parse(authRec.Header().Get("Location"))
	exchange := url.Values{
		"grant_type": {"authorization_code"}, "client_id": {clientID}, "code": {location.Query().Get("code")},
		"redirect_uri": {testRedirect}, "code_verifier": {"verifier"},
	}
	grants.setDown(errors.New("connection refused"))
	if rec := postForm(mux, "/oauth/token", exchange); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code exchange during an outage = %d, want 503", rec.Code)
	}
	grants.setDown(nil)
	decodePair(t, postForm(mux, "/oauth/token", exchange))

	// So does a refresh of a pre-grant token, which must not be moved onto
	// a grant nobody can find later.
	legacy := legacySign(t, "setup-secret", signedPayload{Type: "refresh", ClientID: clientID, ClientName: "claude-test", Scope: "query", Nonce: "n", Exp: 1000 + 3600, Iat: 900})
	before := grants.created
	grants.setDown(errors.New("connection refused"))
	if rec := refresh(mux, clientID, legacy); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("legacy refresh during an outage = %d, want 503", rec.Code)
	}
	if grants.created != before {
		t.Fatal("an outage must not create a grant")
	}
}

func TestNoGrantStoreIssuesNoOAuthTokens(t *testing.T) {
	svc := NewService([]byte("setup-secret"), func() time.Time { return time.Unix(1000, 0) })
	mux := http.NewServeMux()
	svc.RegisterHandlers(mux, "https://mcp.example.com")
	clientID := registerClient(t, mux)
	authRec := postForm(mux, "/oauth/authorize", url.Values{
		"client_id": {clientID}, "redirect_uri": {testRedirect}, "response_type": {"code"},
		"code_challenge": {s256Challenge("verifier")}, "code_challenge_method": {"S256"},
		"secret_token": {"setup-secret"}, "client_name": {"claude-test"},
	})
	location, _ := url.Parse(authRec.Header().Get("Location"))
	rec := postForm(mux, "/oauth/token", url.Values{
		"grant_type": {"authorization_code"}, "client_id": {clientID}, "code": {location.Query().Get("code")},
		"redirect_uri": {testRedirect}, "code_verifier": {"verifier"},
	})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code exchange without a grant store = %d, want 503", rec.Code)
	}
}

func TestPreGrantTokensKeepWorkingAndMoveOntoOneGrant(t *testing.T) {
	svc, grants, mux := newGrantTestService(t, NewKeyring([]byte("setup-secret")))
	// A connector registered, and holding tokens, from before this change.
	clientID := legacySign(t, "setup-secret", signedPayload{Type: "client", RedirectURIs: []string{testRedirect}, Scope: "query", Nonce: "c", Iat: 1})
	access := legacySign(t, "setup-secret", signedPayload{Type: "access", ClientID: clientID, ClientName: "claude-test", Scope: "query", Nonce: "a", Exp: 1000 + 3600, Iat: 900})
	refreshToken := legacySign(t, "setup-secret", signedPayload{Type: "refresh", ClientID: clientID, ClientName: "claude-test", Scope: "query", Nonce: "r", Exp: 1000 + 86400, Iat: 900})

	if _, err := svc.VerifyBearer(context.Background(), access); err != nil {
		t.Fatalf("pre-grant access token rejected before the cutoff: %v", err)
	}
	first := decodePair(t, refresh(mux, clientID, refreshToken))
	gid := payloadOf(t, first.AccessToken).GrantID
	if gid == "" {
		t.Fatal("refreshing a pre-grant token must move it onto a grant")
	}
	if strings.Contains(gid, tokenFingerprint(refreshToken)[:8]) {
		t.Fatal("grant id must not be a plain hash of the token")
	}
	retry := decodePair(t, refresh(mux, clientID, refreshToken))
	if payloadOf(t, retry.AccessToken).GrantID != gid || grants.created != 1 {
		t.Fatalf("a retried pre-grant refresh must land on the same grant; got %q, %d grants", payloadOf(t, retry.AccessToken).GrantID, grants.created)
	}
	// The connector's old client_id still authorizes a fresh login.
	connect(t, mux, clientID, "setup-secret")

	grants.revoke(gid)
	if rec := refresh(mux, clientID, refreshToken); rec.Code != http.StatusBadRequest {
		t.Fatalf("pre-grant refresh token after its grant was revoked = %d, want 400", rec.Code)
	}
}

func TestPreGrantTokensAreRefusedAfterTheCutoff(t *testing.T) {
	svc, grants, mux := newGrantTestService(t, NewKeyring([]byte("setup-secret")))
	svc.legacyCutoff = time.Unix(999, 0)
	access := legacySign(t, "setup-secret", signedPayload{Type: "access", ClientID: "c", ClientName: "claude-test", Scope: "query", Nonce: "a", Exp: 1000 + 3600, Iat: 900})
	refreshToken := legacySign(t, "setup-secret", signedPayload{Type: "refresh", ClientID: "c", ClientName: "claude-test", Scope: "query", Nonce: "r", Exp: 1000 + 86400, Iat: 900})
	if _, err := svc.VerifyBearer(context.Background(), access); err == nil {
		t.Fatal("pre-grant access token accepted after the cutoff")
	}
	rec := refresh(mux, "c", refreshToken)
	if rec.Code != http.StatusBadRequest || oauthErrorCode(t, rec) != "invalid_grant" {
		t.Fatalf("pre-grant refresh after the cutoff: status %d body %s", rec.Code, rec.Body.String())
	}
	if grants.created != 0 {
		t.Fatal("no grant may be created for a token past the cutoff")
	}
	if !LegacyOAuthTokenCutoff.Equal(time.Date(2026, 11, 28, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("cutoff = %s; it is 30 days after the deploy that introduced grants", LegacyOAuthTokenCutoff)
	}
}
