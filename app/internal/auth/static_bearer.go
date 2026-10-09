package auth

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

// RequireStaticBearer is the middleware for surfaces that authenticate with
// the raw shared secret (the same value used to bootstrap MCP OAuth). It is
// distinct from RequireBearer, which validates HMAC-signed MCP access tokens
// issued by the OAuth flow.
//
// Used by the HTTP API where there is no browser OAuth dance. CLI and script
// clients send:
//
//	Authorization: Bearer <client_name>:<PDW_SECRET_TOKEN>
//
// The token is the master secret (scope root) or an issued token from
// private.app_access_tokens (scope full or agent); see
// docs/agents/access-tokens.md.
//
// The client name identifies which tool is calling (e.g. "codex", "hermes",
// "claude-cli") and is logged on every authenticated request. It is required;
// a bare "Bearer <token>" is rejected.
func (s *Service) RequireStaticBearer() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if len(s.secret) == 0 {
				s.logger.ErrorContext(r.Context(), "static bearer middleware invoked with empty secret")
				http.Error(w, "server misconfigured: missing secret", http.StatusInternalServerError)
				return
			}
			authz := r.Header.Get("Authorization")
			if authz == "" {
				w.Header().Set("WWW-Authenticate", `Bearer realm="pdw-api"`)
				http.Error(w, "missing bearer token", http.StatusUnauthorized)
				return
			}
			const prefix = "Bearer "
			if !strings.HasPrefix(authz, prefix) {
				w.Header().Set("WWW-Authenticate", `Bearer realm="pdw-api"`)
				http.Error(w, "unsupported authorization scheme", http.StatusUnauthorized)
				return
			}
			credential := strings.TrimSpace(authz[len(prefix):])
			colon := strings.IndexByte(credential, ':')
			if colon <= 0 {
				s.logger.WarnContext(r.Context(), "static bearer rejected", "reason", "missing_client_name", "path", r.URL.Path)
				w.Header().Set("WWW-Authenticate", `Bearer realm="pdw-api", error="invalid_token"`)
				http.Error(w, `bearer credential must be formatted as "<client_name>:<token>"`, http.StatusUnauthorized)
				return
			}
			name, ok := ValidateClientName(credential[:colon])
			if !ok {
				s.logger.WarnContext(r.Context(), "static bearer rejected", "reason", "invalid_client_name", "path", r.URL.Path)
				w.Header().Set("WWW-Authenticate", `Bearer realm="pdw-api", error="invalid_token"`)
				http.Error(w, "invalid client name", http.StatusUnauthorized)
				return
			}
			token := credential[colon+1:]
			principal, status, reason := s.authenticateStatic(r, name, token)
			if status != 0 {
				if status == http.StatusUnauthorized {
					w.Header().Set("WWW-Authenticate", `Bearer realm="pdw-api", error="invalid_token"`)
				}
				http.Error(w, reason, status)
				return
			}
			if !principal.Allows(r.URL.Path) {
				s.logger.WarnContext(r.Context(), "static bearer scope denied", "client", name, "token_id", principal.TokenID, "scope", principal.Scope, "path", r.URL.Path)
				http.Error(w, "this token's scope ("+string(principal.Scope)+") reaches only "+agentToolsPath+", the warehouse tools API the pdw CLI uses", http.StatusForbidden)
				return
			}
			if !hasClientNameHolder(r.Context()) {
				r = r.WithContext(WithClientNameHolder(r.Context()))
			}
			setPrincipal(r.Context(), principal)
			s.logger.InfoContext(r.Context(), "static bearer accepted", "client", name, "scope", principal.Scope, "token_id", principal.TokenID, "path", r.URL.Path)
			next.ServeHTTP(w, r)
		})
	}
}

// authenticateStatic resolves the token half of a static bearer. The master
// secret is compared first and in constant time, so it keeps working through
// a token-store outage; anything else goes to the issued-token store. A
// non-zero status is the response to send, with a client-safe reason.
func (s *Service) authenticateStatic(r *http.Request, name, token string) (Principal, int, string) {
	if subtle.ConstantTimeCompare([]byte(token), s.secret) == 1 {
		return Principal{Scope: ScopeRoot, ClientName: name}, 0, ""
	}
	tokens := s.tokenAuthenticator()
	if tokens == nil {
		s.logger.WarnContext(r.Context(), "static bearer token rejected", "client", name, "path", r.URL.Path)
		return Principal{}, http.StatusUnauthorized, "invalid bearer token"
	}
	principal, err := tokens.AuthenticateToken(r.Context(), token)
	if err == nil {
		principal.ClientName = name
		return principal, 0, ""
	}
	var rejected *TokenRejectedError
	switch {
	case errors.Is(err, ErrUnknownToken):
		s.logger.WarnContext(r.Context(), "static bearer token rejected", "client", name, "path", r.URL.Path)
		return Principal{}, http.StatusUnauthorized, "invalid bearer token"
	case errors.As(err, &rejected):
		s.logger.WarnContext(r.Context(), "issued token rejected", "client", name, "path", r.URL.Path, "reason", rejected.Reason)
		return Principal{}, http.StatusUnauthorized, rejected.Reason
	default:
		s.logger.ErrorContext(r.Context(), "issued token lookup failed", "client", name, "path", r.URL.Path, "error", err)
		return Principal{}, http.StatusServiceUnavailable, "token store unavailable; retry, or use the master secret"
	}
}
