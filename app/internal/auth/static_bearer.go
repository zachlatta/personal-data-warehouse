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
// Used by the HTTP API where there is no browser OAuth dance — a CLI or
// script supplies "Authorization: Bearer <PDW_SECRET_TOKEN>".
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
			token := strings.TrimSpace(authz[len(prefix):])
			if subtle.ConstantTimeCompare([]byte(token), s.secret) != 1 {
				s.logger.WarnContext(r.Context(), "static bearer token rejected", "path", r.URL.Path)
				w.Header().Set("WWW-Authenticate", `Bearer realm="pdw-api", error="invalid_token"`)
				http.Error(w, "invalid bearer token", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
