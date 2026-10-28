package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
)

type Service struct {
	keys          *Keyring
	now           func() time.Time
	consumedCodes map[string]int64
	mu            sync.Mutex
	logger        *slog.Logger
	tokens        TokenAuthenticator
	grants        OAuthGrants
	// legacyCutoff is when tokens issued before grants existed (no gid)
	// stop working; see LegacyOAuthTokenCutoff.
	legacyCutoff time.Time
}

// LegacyOAuthTokenCutoff is when OAuth tokens minted before grants existed
// stop being accepted. Until then such a refresh token still works, and its
// first use moves the connector onto a revocable grant; an access token of
// that age simply expires within a day. A connector unused the whole time
// re-authorizes once.
var LegacyOAuthTokenCutoff = time.Date(2026, 11, 28, 0, 0, 0, 0, time.UTC)

// OAuthGrants is the store of revocable OAuth MCP grants: one per
// authorization, carried by every access and refresh token minted from it as
// gid. Every method returns ErrUnknownToken for an id it does not have, a
// *TokenRejectedError for a grant that is revoked or idle past its limit, and
// any other error when the store cannot answer, which callers must treat as
// temporary rather than as a rejection.
type OAuthGrants interface {
	// CreateOAuthGrant records a new grant and returns its id.
	CreateOAuthGrant(ctx context.Context, clientName string) (string, error)
	// EnsureOAuthGrant records grant id if it does not exist yet, then
	// checks it as CheckOAuthGrant does. It moves a pre-grant refresh token
	// onto a grant; a retry with the same token lands on the same row.
	EnsureOAuthGrant(ctx context.Context, id, clientName string) error
	// CheckOAuthGrant reports whether grant id may still be used and records
	// the use.
	CheckOAuthGrant(ctx context.Context, id string) error
}

// GrantUnavailableError means the grant store could not answer. It is not a
// verdict on the token: the request should fail with a temporary error so
// the client retries instead of discarding its credentials.
type GrantUnavailableError struct{ Err error }

func (e *GrantUnavailableError) Error() string { return "grant store unavailable: " + e.Err.Error() }
func (e *GrantUnavailableError) Unwrap() error { return e.Err }

type Claims struct {
	ClientID   string
	ClientName string
	Scope      string
	Expires    time.Time
}

type signedPayload struct {
	Type                string   `json:"type"`
	ClientID            string   `json:"client_id,omitempty"`
	ClientName          string   `json:"client_name,omitempty"`
	RedirectURIs        []string `json:"redirect_uris,omitempty"`
	RedirectURI         string   `json:"redirect_uri,omitempty"`
	CodeChallenge       string   `json:"code_challenge,omitempty"`
	CodeChallengeMethod string   `json:"code_challenge_method,omitempty"`
	Scope               string   `json:"scope,omitempty"`
	Nonce               string   `json:"nonce,omitempty"`
	Exp                 int64    `json:"exp,omitempty"`
	Iat                 int64    `json:"iat,omitempty"`
	GrantID             string   `json:"gid,omitempty"`
}

// NewService is a Service keyed by one secret: the app with no rotation in
// progress, or a client that signs uploads with the secret it holds.
func NewService(secret []byte, now func() time.Time) *Service {
	return NewServiceWithKeyring(NewKeyring(secret), now)
}

// NewServiceWithKeyring is a Service that signs with the ring's current key
// and verifies with any key in it.
func NewServiceWithKeyring(keys *Keyring, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{keys: keys, now: now, consumedCodes: make(map[string]int64), logger: slog.Default().With("component", "auth"), legacyCutoff: LegacyOAuthTokenCutoff}
}

// SetOAuthGrants installs the grant store. Without one the service issues no
// OAuth tokens, since a token it could not revoke is what grants exist to
// prevent.
func (s *Service) SetOAuthGrants(g OAuthGrants) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grants = g
}

func (s *Service) oauthGrants() OAuthGrants {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.grants
}

func (s *Service) RegisterHandlers(mux *http.ServeMux, baseURL string) {
	baseURL = strings.TrimRight(baseURL, "/")
	s.logger.Info("registering OAuth handlers", "base_url", baseURL)
	// Clients derive discovery URLs from the /mcp endpoint in several shapes:
	// the plain root documents, the RFC 8414/9728 path-inserted forms
	// (/.well-known/<doc>/mcp), the path-appended forms
	// (/mcp/.well-known/<doc>), and the OpenID Connect discovery equivalents.
	// Claude's connector probes these when recovering from a 401 and gives up
	// on the server if they 404, so every variant serves the same document.
	resourceMeta := s.protectedResourceMetadata(baseURL)
	mux.HandleFunc("/.well-known/oauth-protected-resource", resourceMeta)
	mux.HandleFunc("/.well-known/oauth-protected-resource/", resourceMeta)
	mux.HandleFunc("/mcp/.well-known/oauth-protected-resource", resourceMeta)
	authMeta := s.authServerMetadata(baseURL)
	mux.HandleFunc("/.well-known/oauth-authorization-server", authMeta)
	mux.HandleFunc("/.well-known/oauth-authorization-server/", authMeta)
	mux.HandleFunc("/mcp/.well-known/oauth-authorization-server", authMeta)
	mux.HandleFunc("/.well-known/openid-configuration", authMeta)
	mux.HandleFunc("/.well-known/openid-configuration/", authMeta)
	mux.HandleFunc("/mcp/.well-known/openid-configuration", authMeta)
	mux.HandleFunc("/oauth/register", s.registerClient)
	mux.HandleFunc("/oauth/authorize", s.authorize)
	mux.HandleFunc("/oauth/token", s.token)
	mux.HandleFunc("/oauth/jwks", s.jwks)
}

func (s *Service) RequireBearer(metadataURL string) func(http.Handler) http.Handler {
	return mcpauth.RequireBearerToken(func(ctx context.Context, token string, req *http.Request) (*mcpauth.TokenInfo, error) {
		claims, err := s.VerifyBearer(ctx, token)
		var unavailable *GrantUnavailableError
		if errors.As(err, &unavailable) {
			// Not ErrInvalidToken: the SDK answers 500 rather than 401, so
			// the connector retries instead of dropping its login.
			s.logger.ErrorContext(ctx, "bearer token not checked", "path", req.URL.Path, "error", err)
			return nil, err
		}
		if err != nil {
			s.logger.WarnContext(ctx, "bearer token rejected", "path", req.URL.Path, "error", err)
			return nil, fmt.Errorf("%w: %v", mcpauth.ErrInvalidToken, err)
		}
		SetClientName(ctx, claims.ClientName)
		s.logger.DebugContext(ctx, "bearer token accepted", "path", req.URL.Path, "client", claims.ClientName)
		return &mcpauth.TokenInfo{
			Scopes:     strings.Fields(claims.Scope),
			Expiration: claims.Expires,
			UserID:     claims.ClientID,
		}, nil
	}, &mcpauth.RequireBearerTokenOptions{
		ResourceMetadataURL: metadataURL,
		Scopes:              []string{"query"},
	})
}

// VerifyBearer checks an OAuth access token: its signature, its expiry, and
// that its grant has not been revoked. A *GrantUnavailableError means the
// grant could not be checked, not that the token is bad.
func (s *Service) VerifyBearer(ctx context.Context, token string) (*Claims, error) {
	var payload signedPayload
	if _, err := s.verifySigned(token, "access", &payload); err != nil {
		return nil, err
	}
	if payload.Exp != 0 && s.now().Unix() > payload.Exp {
		return nil, errors.New("token expired")
	}
	if payload.ClientName == "" {
		return nil, errors.New("token missing client name; re-authorize the connector")
	}
	if payload.GrantID == "" {
		if !s.now().Before(s.legacyCutoff) {
			return nil, errors.New("token predates revocable grants; re-authorize the connector")
		}
	} else if err := s.checkGrant(ctx, payload.GrantID); err != nil {
		return nil, err
	}
	return &Claims{
		ClientID:   payload.ClientID,
		ClientName: payload.ClientName,
		Scope:      payload.Scope,
		Expires:    time.Unix(payload.Exp, 0),
	}, nil
}

func (s *Service) protectedResourceMetadata(baseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, oauthex.ProtectedResourceMetadata{
			Resource:               baseURL + "/mcp",
			AuthorizationServers:   []string{baseURL},
			ScopesSupported:        []string{"query"},
			BearerMethodsSupported: []string{"header"},
			ResourceName:           "Personal Data Warehouse MCP - Gmail, Slack, Apple Notes, Apple Messages/iMessage",
			ResourceDocumentation:  baseURL + "/",
			ResourcePolicyURI:      baseURL + "/",
			ResourceTOSURI:         baseURL + "/",
		})
	}
}

func (s *Service) authServerMetadata(baseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, oauthex.AuthServerMeta{
			Issuer:                            baseURL,
			AuthorizationEndpoint:             baseURL + "/oauth/authorize",
			TokenEndpoint:                     baseURL + "/oauth/token",
			JWKSURI:                           baseURL + "/oauth/jwks",
			RegistrationEndpoint:              baseURL + "/oauth/register",
			ScopesSupported:                   []string{"query"},
			ResponseTypesSupported:            []string{"code"},
			GrantTypesSupported:               []string{"authorization_code", "refresh_token"},
			TokenEndpointAuthMethodsSupported: []string{"none"},
			CodeChallengeMethodsSupported:     []string{"S256"},
		})
	}
}

func (s *Service) registerClient(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.logger.WarnContext(r.Context(), "client registration rejected", "reason", "method", "method", r.Method)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		RedirectURIs            []string `json:"redirect_uris"`
		TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method"`
		ClientName              string   `json:"client_name"`
		Scope                   string   `json:"scope"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.WarnContext(r.Context(), "client registration rejected", "reason", "invalid_json", "error", err)
		oauthError(w, http.StatusBadRequest, "invalid_client_metadata", "invalid registration JSON")
		return
	}
	if len(req.RedirectURIs) == 0 {
		s.logger.WarnContext(r.Context(), "client registration rejected", "reason", "missing_redirect_uris")
		oauthError(w, http.StatusBadRequest, "invalid_client_metadata", "redirect_uris is required")
		return
	}
	payload := signedPayload{
		Type:         "client",
		RedirectURIs: req.RedirectURIs,
		Scope:        "query",
		Nonce:        nonce(),
		Iat:          s.now().Unix(),
	}
	clientID, err := s.sign(payload)
	if err != nil {
		s.logger.ErrorContext(r.Context(), "client registration failed", "error", err)
		http.Error(w, "could not register client", http.StatusInternalServerError)
		return
	}
	s.logger.InfoContext(r.Context(), "client registered", "client", tokenFingerprint(clientID), "redirect_uris", len(req.RedirectURIs))
	writeJSON(w, http.StatusCreated, map[string]any{
		"client_id":                  clientID,
		"client_id_issued_at":        s.now().Unix(),
		"redirect_uris":              req.RedirectURIs,
		"token_endpoint_auth_method": "none",
		"grant_types":                []string{"authorization_code", "refresh_token"},
		"response_types":             []string{"code"},
		"scope":                      "query",
	})
}

func (s *Service) jwks(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"keys": []any{}})
}

func (s *Service) authorize(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.logger.DebugContext(r.Context(), "authorization page requested")
		if err := authorizePage.Execute(w, r.URL.Query()); err != nil {
			s.logger.ErrorContext(r.Context(), "authorization page render failed", "error", err)
		}
	case http.MethodPost:
		s.authorizePost(w, r)
	default:
		s.logger.WarnContext(r.Context(), "authorization rejected", "reason", "method", "method", r.Method)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Service) authorizePost(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.logger.WarnContext(r.Context(), "authorization rejected", "reason", "invalid_form", "error", err)
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	// Only the current secret authorizes a new connector, even during a
	// rotation window.
	if subtle.ConstantTimeCompare([]byte(r.Form.Get("secret_token")), s.keys.current().secret) != 1 {
		s.logger.WarnContext(r.Context(), "authorization rejected", "reason", "invalid_secret", "client", tokenFingerprint(r.Form.Get("client_id")))
		http.Error(w, "invalid secret token", http.StatusUnauthorized)
		return
	}
	clientName, ok := ValidateClientName(r.Form.Get("client_name"))
	if !ok {
		s.logger.WarnContext(r.Context(), "authorization rejected", "reason", "invalid_client_name")
		http.Error(w, "client name is required (letters, digits, '.', '_', '-', spaces; no ':' or control characters)", http.StatusBadRequest)
		return
	}
	clientID := r.Form.Get("client_id")
	redirectURI := r.Form.Get("redirect_uri")
	if err := s.validateClientRedirect(clientID, redirectURI); err != nil {
		s.logger.WarnContext(r.Context(), "authorization rejected", "reason", "invalid_redirect", "client", tokenFingerprint(clientID), "redirect_uri", redirectURI, "error", err)
		oauthError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if r.Form.Get("response_type") != "code" {
		s.logger.WarnContext(r.Context(), "authorization rejected", "reason", "unsupported_response_type", "client", tokenFingerprint(clientID))
		oauthError(w, http.StatusBadRequest, "unsupported_response_type", "response_type must be code")
		return
	}
	challenge := r.Form.Get("code_challenge")
	method := r.Form.Get("code_challenge_method")
	if challenge == "" {
		s.logger.WarnContext(r.Context(), "authorization rejected", "reason", "missing_code_challenge", "client", tokenFingerprint(clientID))
		oauthError(w, http.StatusBadRequest, "invalid_request", "code_challenge is required")
		return
	}
	if method != "S256" {
		s.logger.WarnContext(r.Context(), "authorization rejected", "reason", "unsupported_code_challenge_method", "client", tokenFingerprint(clientID), "method", method)
		oauthError(w, http.StatusBadRequest, "invalid_request", "code_challenge_method must be S256")
		return
	}
	code, err := s.sign(signedPayload{
		Type:                "code",
		ClientID:            clientID,
		ClientName:          clientName,
		RedirectURI:         redirectURI,
		CodeChallenge:       challenge,
		CodeChallengeMethod: method,
		Scope:               "query",
		Nonce:               nonce(),
		Exp:                 s.now().Add(10 * time.Minute).Unix(),
		Iat:                 s.now().Unix(),
	})
	if err != nil {
		s.logger.ErrorContext(r.Context(), "authorization code signing failed", "client", tokenFingerprint(clientID), "error", err)
		http.Error(w, "could not authorize", http.StatusInternalServerError)
		return
	}
	target, err := url.Parse(redirectURI)
	if err != nil {
		s.logger.WarnContext(r.Context(), "authorization rejected", "reason", "invalid_redirect_uri", "client", tokenFingerprint(clientID), "error", err)
		oauthError(w, http.StatusBadRequest, "invalid_request", "invalid redirect_uri")
		return
	}
	q := target.Query()
	q.Set("code", code)
	if state := r.Form.Get("state"); state != "" {
		q.Set("state", state)
	}
	target.RawQuery = q.Encode()
	s.logger.InfoContext(r.Context(), "authorization code issued", "client", clientName, "client_id", tokenFingerprint(clientID), "redirect_host", target.Host)
	http.Redirect(w, r, target.String(), http.StatusFound)
}

func (s *Service) token(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.logger.WarnContext(r.Context(), "token request rejected", "reason", "method", "method", r.Method)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		s.logger.WarnContext(r.Context(), "token request rejected", "reason", "invalid_form", "error", err)
		oauthError(w, http.StatusBadRequest, "invalid_request", "invalid form")
		return
	}
	s.logger.DebugContext(r.Context(), "token request received", "grant_type", r.Form.Get("grant_type"), "client", tokenFingerprint(r.Form.Get("client_id")))
	switch r.Form.Get("grant_type") {
	case "authorization_code":
		s.tokenFromCode(w, r)
	case "refresh_token":
		s.tokenFromRefresh(w, r)
	default:
		s.logger.WarnContext(r.Context(), "token request rejected", "reason", "unsupported_grant_type", "grant_type", r.Form.Get("grant_type"), "client", tokenFingerprint(r.Form.Get("client_id")))
		oauthError(w, http.StatusBadRequest, "unsupported_grant_type", "unsupported grant_type")
	}
}

func (s *Service) tokenFromCode(w http.ResponseWriter, r *http.Request) {
	rawCode := r.Form.Get("code")
	var code signedPayload
	if _, err := s.verifySigned(rawCode, "code", &code); err != nil {
		s.logger.WarnContext(r.Context(), "authorization code token exchange rejected", "reason", "invalid_code", "error", err)
		oauthError(w, http.StatusBadRequest, "invalid_grant", "invalid authorization code")
		return
	}
	if code.Exp != 0 && s.now().Unix() > code.Exp {
		s.logger.WarnContext(r.Context(), "authorization code token exchange rejected", "reason", "expired_code", "client", tokenFingerprint(code.ClientID))
		oauthError(w, http.StatusBadRequest, "invalid_grant", "authorization code expired")
		return
	}
	if code.ClientID != r.Form.Get("client_id") || code.RedirectURI != r.Form.Get("redirect_uri") {
		s.logger.WarnContext(r.Context(), "authorization code token exchange rejected", "reason", "client_or_redirect_mismatch", "client", tokenFingerprint(r.Form.Get("client_id")))
		oauthError(w, http.StatusBadRequest, "invalid_grant", "client_id or redirect_uri mismatch")
		return
	}
	if !verifyPKCE(r.Form.Get("code_verifier"), code.CodeChallenge, code.CodeChallengeMethod) {
		s.logger.WarnContext(r.Context(), "authorization code token exchange rejected", "reason", "pkce_failed", "client", tokenFingerprint(code.ClientID))
		oauthError(w, http.StatusBadRequest, "invalid_grant", "PKCE verification failed")
		return
	}
	if !s.consumeAuthorizationCode(rawCode, code.Exp) {
		s.logger.WarnContext(r.Context(), "authorization code token exchange rejected", "reason", "code_replay", "client", tokenFingerprint(code.ClientID))
		oauthError(w, http.StatusBadRequest, "invalid_grant", "authorization code already used")
		return
	}
	if code.ClientName == "" {
		s.logger.WarnContext(r.Context(), "authorization code token exchange rejected", "reason", "missing_client_name", "client_id", tokenFingerprint(code.ClientID))
		oauthError(w, http.StatusBadRequest, "invalid_grant", "authorization code missing client_name; re-authorize")
		return
	}
	grants := s.oauthGrants()
	if grants == nil {
		s.releaseAuthorizationCode(rawCode)
		s.logger.ErrorContext(r.Context(), "authorization code token exchange failed", "reason", "no_grant_store", "client", code.ClientName)
		oauthError(w, http.StatusServiceUnavailable, "temporarily_unavailable", "grant store is not configured")
		return
	}
	grantID, err := grants.CreateOAuthGrant(r.Context(), code.ClientName)
	if err != nil {
		// The code stays redeemable so the client's retry can succeed.
		s.releaseAuthorizationCode(rawCode)
		s.logger.ErrorContext(r.Context(), "authorization code token exchange failed", "reason", "grant_store", "client", code.ClientName, "error", err)
		oauthError(w, http.StatusServiceUnavailable, "temporarily_unavailable", "could not record the grant; retry")
		return
	}
	s.logger.InfoContext(r.Context(), "authorization code exchanged", "client", code.ClientName, "client_id", tokenFingerprint(code.ClientID), "grant_id", grantID)
	s.writeTokenResponse(w, code.ClientID, code.ClientName, grantID)
}

func (s *Service) tokenFromRefresh(w http.ResponseWriter, r *http.Request) {
	rawRefresh := r.Form.Get("refresh_token")
	var refresh signedPayload
	key, err := s.verifySigned(rawRefresh, "refresh", &refresh)
	if err != nil {
		s.logger.WarnContext(r.Context(), "refresh token exchange rejected", "reason", "invalid_refresh", "error", err)
		oauthError(w, http.StatusBadRequest, "invalid_grant", "invalid refresh token")
		return
	}
	if refresh.Exp != 0 && s.now().Unix() > refresh.Exp {
		s.logger.WarnContext(r.Context(), "refresh token exchange rejected", "reason", "expired_refresh", "client", tokenFingerprint(refresh.ClientID))
		oauthError(w, http.StatusBadRequest, "invalid_grant", "refresh token expired")
		return
	}
	if clientID := r.Form.Get("client_id"); clientID != "" && clientID != refresh.ClientID {
		s.logger.WarnContext(r.Context(), "refresh token exchange rejected", "reason", "client_mismatch", "client", tokenFingerprint(clientID))
		oauthError(w, http.StatusBadRequest, "invalid_grant", "client_id mismatch")
		return
	}
	if refresh.ClientName == "" {
		s.logger.WarnContext(r.Context(), "refresh token exchange rejected", "reason", "missing_client_name", "client_id", tokenFingerprint(refresh.ClientID))
		oauthError(w, http.StatusBadRequest, "invalid_grant", "refresh token missing client_name; re-authorize")
		return
	}
	grantID := refresh.GrantID
	if grantID == "" {
		if !s.now().Before(s.legacyCutoff) {
			s.logger.WarnContext(r.Context(), "refresh token exchange rejected", "reason", "pre_grant_token", "client", refresh.ClientName)
			oauthError(w, http.StatusBadRequest, "invalid_grant", "refresh token predates revocable grants; re-authorize")
			return
		}
		// A token from before grants: its grant id comes from the token
		// itself, so every use of it (and a revoke) meets the same row.
		grantID = legacyGrantID(key, rawRefresh)
		err = s.ensureGrant(r.Context(), grantID, refresh.ClientName)
	} else {
		err = s.checkGrant(r.Context(), grantID)
	}
	var unavailable *GrantUnavailableError
	if errors.As(err, &unavailable) {
		s.logger.ErrorContext(r.Context(), "refresh token exchange failed", "reason", "grant_store", "client", refresh.ClientName, "grant_id", grantID, "error", err)
		oauthError(w, http.StatusServiceUnavailable, "temporarily_unavailable", "could not check the grant; retry")
		return
	}
	if err != nil {
		s.logger.WarnContext(r.Context(), "refresh token exchange rejected", "reason", "grant_rejected", "client", refresh.ClientName, "grant_id", grantID, "error", err)
		oauthError(w, http.StatusBadRequest, "invalid_grant", err.Error())
		return
	}
	s.logger.InfoContext(r.Context(), "refresh token exchanged", "client", refresh.ClientName, "client_id", tokenFingerprint(refresh.ClientID), "grant_id", grantID, "migrated", refresh.GrantID == "")
	s.writeTokenResponse(w, refresh.ClientID, refresh.ClientName, grantID)
}

// checkGrant asks the grant store about id. Rejections come back as they
// are; anything the store could not answer is a *GrantUnavailableError.
func (s *Service) checkGrant(ctx context.Context, id string) error {
	grants := s.oauthGrants()
	if grants == nil {
		return &GrantUnavailableError{Err: errors.New("grant store is not configured")}
	}
	return classifyGrantError(grants.CheckOAuthGrant(ctx, id))
}

func (s *Service) ensureGrant(ctx context.Context, id, clientName string) error {
	grants := s.oauthGrants()
	if grants == nil {
		return &GrantUnavailableError{Err: errors.New("grant store is not configured")}
	}
	return classifyGrantError(grants.EnsureOAuthGrant(ctx, id, clientName))
}

func classifyGrantError(err error) error {
	var rejected *TokenRejectedError
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrUnknownToken):
		return errors.New("grant not found; re-authorize the connector")
	case errors.As(err, &rejected):
		return rejected
	default:
		return &GrantUnavailableError{Err: err}
	}
}

// legacyGrantID is the grant a pre-grant refresh token moves onto: a keyed
// hash of the token under the key that signed it, so it is stable for that
// token across rotations and reveals nothing about it.
func legacyGrantID(key rootKey, token string) string {
	mac := hmac.New(sha256.New, key.sub[purposeOAuthGrant])
	mac.Write([]byte(token))
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(mac.Sum(nil)[:10]))
}

func (s *Service) writeTokenResponse(w http.ResponseWriter, clientID, clientName, grantID string) {
	accessExp := s.now().Add(24 * time.Hour).Unix()
	refreshExp := s.now().Add(365 * 24 * time.Hour).Unix()
	access, err := s.sign(signedPayload{Type: "access", ClientID: clientID, ClientName: clientName, Scope: "query", Nonce: nonce(), Exp: accessExp, Iat: s.now().Unix(), GrantID: grantID})
	if err != nil {
		s.logger.Error("access token signing failed", "client", clientName, "error", err)
		http.Error(w, "could not issue access token", http.StatusInternalServerError)
		return
	}
	refresh, err := s.sign(signedPayload{Type: "refresh", ClientID: clientID, ClientName: clientName, Scope: "query", Nonce: nonce(), Exp: refreshExp, Iat: s.now().Unix(), GrantID: grantID})
	if err != nil {
		s.logger.Error("refresh token signing failed", "client", clientName, "error", err)
		http.Error(w, "could not issue refresh token", http.StatusInternalServerError)
		return
	}
	s.logger.Info("token response issued", "client", clientName, "access_expires_at", accessExp, "refresh_expires_at", refreshExp)
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":  access,
		"refresh_token": refresh,
		"token_type":    "Bearer",
		"expires_in":    86400,
		"scope":         "query",
	})
}

func (s *Service) validateClientRedirect(clientID, redirectURI string) error {
	var client signedPayload
	if _, err := s.verifySigned(clientID, "client", &client); err != nil {
		return errors.New("invalid client_id")
	}
	for _, registered := range client.RedirectURIs {
		if redirectURIsMatch(registered, redirectURI) {
			return nil
		}
	}
	return errors.New("redirect_uri was not registered")
}

// redirectURIsMatch reports whether a redirect_uri presented in an authorization
// request matches one registered by the client. Match is exact except for
// loopback URIs (RFC 8252 §7.3): when both URIs are http on a loopback host
// (127.0.0.1, ::1, or localhost), the port is allowed to differ so native
// clients can bind to an ephemeral port each flow.
func redirectURIsMatch(registered, presented string) bool {
	if registered == presented {
		return true
	}
	reg, err := url.Parse(registered)
	if err != nil {
		return false
	}
	pre, err := url.Parse(presented)
	if err != nil {
		return false
	}
	if !isLoopbackRedirect(reg) || !isLoopbackRedirect(pre) {
		return false
	}
	return reg.Scheme == pre.Scheme &&
		strings.EqualFold(reg.Hostname(), pre.Hostname()) &&
		reg.Path == pre.Path
}

func isLoopbackRedirect(u *url.URL) bool {
	if u.Scheme != "http" {
		return false
	}
	switch strings.ToLower(u.Hostname()) {
	case "127.0.0.1", "::1", "localhost":
		return true
	}
	return false
}

// sign returns "<kid>.<body>.<sig>": the current key's id, the base64url
// JSON payload, and an HMAC over both under that key's OAuth subkey.
func (s *Service) sign(payload signedPayload) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	key := s.keys.current()
	signed := key.id + "." + base64.RawURLEncoding.EncodeToString(data)
	mac := hmac.New(sha256.New, key.sub[purposeOAuthToken])
	mac.Write([]byte(signed))
	return signed + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// verifySigned checks a signed value and returns the key that signed it. It
// accepts the current "<kid>.<body>.<sig>" form and the original
// "<body>.<sig>" form, an HMAC under the raw master secret, which every
// registered client_id and pre-grant token still carries.
func (s *Service) verifySigned(token, expectedType string, payload *signedPayload) (rootKey, error) {
	parts := strings.Split(token, ".")
	var key rootKey
	var body string
	switch len(parts) {
	case 3:
		k, ok := s.keys.byID(parts[0])
		if !ok {
			return rootKey{}, errors.New("signed by an unknown key")
		}
		sig, err := base64.RawURLEncoding.DecodeString(parts[2])
		if err != nil {
			return rootKey{}, err
		}
		mac := hmac.New(sha256.New, k.sub[purposeOAuthToken])
		mac.Write([]byte(parts[0] + "." + parts[1]))
		if !hmac.Equal(sig, mac.Sum(nil)) {
			return rootKey{}, errors.New("bad signature")
		}
		key, body = k, parts[1]
	case 2:
		sig, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			return rootKey{}, err
		}
		found := false
		for _, k := range s.keys.keys {
			mac := hmac.New(sha256.New, k.secret)
			mac.Write([]byte(parts[0]))
			if hmac.Equal(sig, mac.Sum(nil)) {
				key, found = k, true
				break
			}
		}
		if !found {
			return rootKey{}, errors.New("bad signature")
		}
		body = parts[0]
	default:
		return rootKey{}, errors.New("malformed token")
	}
	data, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return rootKey{}, err
	}
	if err := json.Unmarshal(data, payload); err != nil {
		return rootKey{}, err
	}
	if payload.Type != expectedType {
		return rootKey{}, errors.New("wrong token type")
	}
	return key, nil
}

func verifyPKCE(verifier, challenge, method string) bool {
	if method != "S256" {
		return false
	}
	sum := sha256.Sum256([]byte(verifier))
	encoded := base64.RawURLEncoding.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(encoded), []byte(challenge)) == 1
}

func (s *Service) consumeAuthorizationCode(code string, expiresAt int64) bool {
	fingerprint := tokenFingerprint(code)
	now := s.now().Unix()
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, exp := range s.consumedCodes {
		if exp != 0 && now > exp {
			delete(s.consumedCodes, key)
		}
	}
	if _, exists := s.consumedCodes[fingerprint]; exists {
		return false
	}
	s.consumedCodes[fingerprint] = expiresAt
	return true
}

// releaseAuthorizationCode undoes consumeAuthorizationCode when the exchange
// failed for a reason that was not the client's.
func (s *Service) releaseAuthorizationCode(code string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.consumedCodes, tokenFingerprint(code))
}

func tokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func nonce() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func oauthError(w http.ResponseWriter, status int, code, description string) {
	writeJSON(w, status, map[string]string{"error": code, "error_description": description})
}

var authorizePage = template.Must(template.New("authorize").Parse(`<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Authorize Personal Data Warehouse</title>
<style>body{font-family:system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;max-width:36rem;margin:12vh auto;padding:0 1rem;color:#172026}label,input,button{font-size:1rem}input{display:block;width:100%;box-sizing:border-box;margin:.5rem 0 1rem;padding:.7rem;border:1px solid #b8c1cc;border-radius:6px}button{padding:.7rem 1rem;border:0;border-radius:6px;background:#172026;color:white}</style></head>
<body><h1>Authorize Personal Data Warehouse</h1><p>This grants access to PDW and any connected MCP tools shared with this client name. Connected tools may execute writes using the upstream account, without PDW mutation review. Manage sharing on the Connections page.</p><form method="post">
{{range $key, $values := .}}{{if and (ne $key "client_name") (ne $key "secret_token")}}{{range $values}}<input type="hidden" name="{{$key}}" value="{{.}}">{{end}}{{end}}{{end}}
<label for="client_name">Client name</label><input id="client_name" name="client_name" type="text" autocomplete="off" autofocus required placeholder="e.g. claude, codex, hermes" maxlength="64">
<label for="secret_token">Secret token</label><input id="secret_token" name="secret_token" type="password" required>
<p style="font-size:.9rem;color:#5a6b78;margin:-.5rem 0 1rem">The client name is logged on every request so you can tell connectors apart.</p>
<button type="submit">Authorize</button></form></body></html>`))
