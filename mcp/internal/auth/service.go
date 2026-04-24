package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
)

type Service struct {
	secret []byte
	now    func() time.Time
}

type Claims struct {
	ClientID string
	Scope    string
	Expires  time.Time
}

type signedPayload struct {
	Type                string   `json:"type"`
	ClientID            string   `json:"client_id,omitempty"`
	RedirectURIs        []string `json:"redirect_uris,omitempty"`
	RedirectURI         string   `json:"redirect_uri,omitempty"`
	CodeChallenge       string   `json:"code_challenge,omitempty"`
	CodeChallengeMethod string   `json:"code_challenge_method,omitempty"`
	Scope               string   `json:"scope,omitempty"`
	Nonce               string   `json:"nonce,omitempty"`
	Exp                 int64    `json:"exp,omitempty"`
	Iat                 int64    `json:"iat,omitempty"`
}

func NewService(secret []byte, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{secret: secret, now: now}
}

func (s *Service) RegisterHandlers(mux *http.ServeMux, baseURL string) {
	baseURL = strings.TrimRight(baseURL, "/")
	mux.HandleFunc("/.well-known/oauth-protected-resource", s.protectedResourceMetadata(baseURL))
	mux.HandleFunc("/.well-known/oauth-authorization-server", s.authServerMetadata(baseURL))
	mux.HandleFunc("/oauth/register", s.registerClient)
	mux.HandleFunc("/oauth/authorize", s.authorize)
	mux.HandleFunc("/oauth/token", s.token)
	mux.HandleFunc("/oauth/jwks", s.jwks)
}

func (s *Service) RequireBearer(metadataURL string) func(http.Handler) http.Handler {
	return mcpauth.RequireBearerToken(func(ctx context.Context, token string, req *http.Request) (*mcpauth.TokenInfo, error) {
		claims, err := s.VerifyBearer(token)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", mcpauth.ErrInvalidToken, err)
		}
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

func (s *Service) VerifyBearer(token string) (*Claims, error) {
	var payload signedPayload
	if err := s.verifySigned(token, "access", &payload); err != nil {
		return nil, err
	}
	if payload.Exp != 0 && s.now().Unix() > payload.Exp {
		return nil, errors.New("token expired")
	}
	return &Claims{
		ClientID: payload.ClientID,
		Scope:    payload.Scope,
		Expires:  time.Unix(payload.Exp, 0),
	}, nil
}

func (s *Service) protectedResourceMetadata(baseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, oauthex.ProtectedResourceMetadata{
			Resource:               baseURL + "/mcp",
			AuthorizationServers:   []string{baseURL},
			ScopesSupported:        []string{"query"},
			BearerMethodsSupported: []string{"header"},
			ResourceName:           "Personal Data Warehouse MCP",
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
			CodeChallengeMethodsSupported:     []string{"S256", "plain"},
		})
	}
}

func (s *Service) registerClient(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
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
		oauthError(w, http.StatusBadRequest, "invalid_client_metadata", "invalid registration JSON")
		return
	}
	if len(req.RedirectURIs) == 0 {
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
		http.Error(w, "could not register client", http.StatusInternalServerError)
		return
	}
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
		authorizePage.Execute(w, r.URL.Query())
	case http.MethodPost:
		s.authorizePost(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Service) authorizePost(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Form.Get("secret_token")), s.secret) != 1 {
		http.Error(w, "invalid secret token", http.StatusUnauthorized)
		return
	}
	clientID := r.Form.Get("client_id")
	redirectURI := r.Form.Get("redirect_uri")
	if err := s.validateClientRedirect(clientID, redirectURI); err != nil {
		oauthError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if r.Form.Get("response_type") != "code" {
		oauthError(w, http.StatusBadRequest, "unsupported_response_type", "response_type must be code")
		return
	}
	challenge := r.Form.Get("code_challenge")
	method := r.Form.Get("code_challenge_method")
	if challenge == "" {
		oauthError(w, http.StatusBadRequest, "invalid_request", "code_challenge is required")
		return
	}
	if method == "" {
		method = "plain"
	}
	if method != "plain" && method != "S256" {
		oauthError(w, http.StatusBadRequest, "invalid_request", "unsupported code_challenge_method")
		return
	}
	code, err := s.sign(signedPayload{
		Type:                "code",
		ClientID:            clientID,
		RedirectURI:         redirectURI,
		CodeChallenge:       challenge,
		CodeChallengeMethod: method,
		Scope:               "query",
		Nonce:               nonce(),
		Exp:                 s.now().Add(10 * time.Minute).Unix(),
		Iat:                 s.now().Unix(),
	})
	if err != nil {
		http.Error(w, "could not authorize", http.StatusInternalServerError)
		return
	}
	target, err := url.Parse(redirectURI)
	if err != nil {
		oauthError(w, http.StatusBadRequest, "invalid_request", "invalid redirect_uri")
		return
	}
	q := target.Query()
	q.Set("code", code)
	if state := r.Form.Get("state"); state != "" {
		q.Set("state", state)
	}
	target.RawQuery = q.Encode()
	http.Redirect(w, r, target.String(), http.StatusFound)
}

func (s *Service) token(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		oauthError(w, http.StatusBadRequest, "invalid_request", "invalid form")
		return
	}
	switch r.Form.Get("grant_type") {
	case "authorization_code":
		s.tokenFromCode(w, r)
	case "refresh_token":
		s.tokenFromRefresh(w, r)
	default:
		oauthError(w, http.StatusBadRequest, "unsupported_grant_type", "unsupported grant_type")
	}
}

func (s *Service) tokenFromCode(w http.ResponseWriter, r *http.Request) {
	var code signedPayload
	if err := s.verifySigned(r.Form.Get("code"), "code", &code); err != nil {
		oauthError(w, http.StatusBadRequest, "invalid_grant", "invalid authorization code")
		return
	}
	if code.Exp != 0 && s.now().Unix() > code.Exp {
		oauthError(w, http.StatusBadRequest, "invalid_grant", "authorization code expired")
		return
	}
	if code.ClientID != r.Form.Get("client_id") || code.RedirectURI != r.Form.Get("redirect_uri") {
		oauthError(w, http.StatusBadRequest, "invalid_grant", "client_id or redirect_uri mismatch")
		return
	}
	if !verifyPKCE(r.Form.Get("code_verifier"), code.CodeChallenge, code.CodeChallengeMethod) {
		oauthError(w, http.StatusBadRequest, "invalid_grant", "PKCE verification failed")
		return
	}
	s.writeTokenResponse(w, code.ClientID)
}

func (s *Service) tokenFromRefresh(w http.ResponseWriter, r *http.Request) {
	var refresh signedPayload
	if err := s.verifySigned(r.Form.Get("refresh_token"), "refresh", &refresh); err != nil {
		oauthError(w, http.StatusBadRequest, "invalid_grant", "invalid refresh token")
		return
	}
	if refresh.Exp != 0 && s.now().Unix() > refresh.Exp {
		oauthError(w, http.StatusBadRequest, "invalid_grant", "refresh token expired")
		return
	}
	if clientID := r.Form.Get("client_id"); clientID != "" && clientID != refresh.ClientID {
		oauthError(w, http.StatusBadRequest, "invalid_grant", "client_id mismatch")
		return
	}
	s.writeTokenResponse(w, refresh.ClientID)
}

func (s *Service) writeTokenResponse(w http.ResponseWriter, clientID string) {
	accessExp := s.now().Add(24 * time.Hour).Unix()
	refreshExp := s.now().Add(365 * 24 * time.Hour).Unix()
	access, err := s.sign(signedPayload{Type: "access", ClientID: clientID, Scope: "query", Nonce: nonce(), Exp: accessExp, Iat: s.now().Unix()})
	if err != nil {
		http.Error(w, "could not issue access token", http.StatusInternalServerError)
		return
	}
	refresh, err := s.sign(signedPayload{Type: "refresh", ClientID: clientID, Scope: "query", Nonce: nonce(), Exp: refreshExp, Iat: s.now().Unix()})
	if err != nil {
		http.Error(w, "could not issue refresh token", http.StatusInternalServerError)
		return
	}
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
	if err := s.verifySigned(clientID, "client", &client); err != nil {
		return errors.New("invalid client_id")
	}
	if !slices.Contains(client.RedirectURIs, redirectURI) {
		return errors.New("redirect_uri was not registered")
	}
	return nil
}

func (s *Service) sign(payload signedPayload) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	body := base64.RawURLEncoding.EncodeToString(data)
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(body))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return body + "." + sig, nil
}

func (s *Service) verifySigned(token, expectedType string, payload *signedPayload) error {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return errors.New("malformed token")
	}
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(parts[0]))
	expectedSig := mac.Sum(nil)
	actualSig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return err
	}
	if !hmac.Equal(actualSig, expectedSig) {
		return errors.New("bad signature")
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, payload); err != nil {
		return err
	}
	if payload.Type != expectedType {
		return errors.New("wrong token type")
	}
	return nil
}

func verifyPKCE(verifier, challenge, method string) bool {
	if method == "" || method == "plain" {
		return subtle.ConstantTimeCompare([]byte(verifier), []byte(challenge)) == 1
	}
	sum := sha256.Sum256([]byte(verifier))
	encoded := base64.RawURLEncoding.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(encoded), []byte(challenge)) == 1
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
<body><h1>Authorize Personal Data Warehouse</h1><form method="post">
{{range $key, $values := .}}{{range $values}}<input type="hidden" name="{{$key}}" value="{{.}}">{{end}}{{end}}
<label for="secret_token">Secret token</label><input id="secret_token" name="secret_token" type="password" autofocus required><button type="submit">Authorize</button></form></body></html>`))
