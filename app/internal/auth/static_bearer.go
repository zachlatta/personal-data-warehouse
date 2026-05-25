package auth

import (
	"crypto/subtle"
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
			if subtle.ConstantTimeCompare([]byte(token), s.secret) != 1 {
				s.logger.WarnContext(r.Context(), "static bearer token rejected", "client", name, "path", r.URL.Path)
				w.Header().Set("WWW-Authenticate", `Bearer realm="pdw-api", error="invalid_token"`)
				http.Error(w, "invalid bearer token", http.StatusUnauthorized)
				return
			}
			SetClientName(r.Context(), name)
			s.logger.InfoContext(r.Context(), "static bearer accepted", "client", name, "path", r.URL.Path)
			next.ServeHTTP(w, r)
		})
	}
}
