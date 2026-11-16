package auth

import (
	"context"
	"errors"
	"github.com/zachlatta/personal-data-warehouse/app/internal/externalfeeds"
	"net/http"
	"strings"
)

// Scope is what a static-bearer credential may reach. See
// docs/agents/access-tokens.md for the model.
type Scope string

const (
	// ScopeRoot is the master secret (PDW_SECRET_TOKEN): every surface,
	// including token administration.
	ScopeRoot Scope = "root"
	// ScopeFull is an issued token: every bearer surface except token
	// administration, so a leaked token cannot mint its own successor. It
	// cannot decide a mutation request (see CanReview): host CLIs hold one,
	// and an agent there must not approve its own proposal.
	ScopeFull Scope = "full"
	// ScopeAgent is a token minted by the cli_authorize flow: the warehouse
	// tools API only, which is everything the pdw CLI calls.
	ScopeAgent Scope = "agent"
	// ScopeReviewer is an issued token for Zach's own review surfaces (the
	// phone app): every surface full reaches, plus the mutation review
	// decisions (approve, deny, edit, undo). A host CLI never holds one, so an
	// agent on a host cannot approve its own proposal.
	ScopeReviewer Scope = "reviewer"
	// ScopeOAuthQuery is an OAuth MCP connector grant (/mcp only). It is
	// never a static bearer: the token store refuses it there.
	ScopeOAuthQuery Scope = "query"
)

// Issuable reports whether s is one of the scopes an issued token may carry.
// Root is never issued: it is the master secret.
func (s Scope) Issuable() bool {
	return s == ScopeFull || s == ScopeAgent || s == ScopeReviewer || externalfeeds.IsScope(string(s))
}

// agentToolsPath is the whole surface an agent-scoped token reaches.
const agentToolsPath = "/api/tools"

// Principal is the authenticated caller of a static-bearer request.
type Principal struct {
	Scope      Scope
	TokenID    string // empty for the master secret
	ClientName string // the name the caller sent in "<client>:<token>"
}

// Allows reports whether this principal may reach path.
func (p Principal) Allows(path string) bool {
	if feed, ok := externalfeeds.Lookup(string(p.Scope)); ok {
		return path == feed.Path
	}
	switch p.Scope {
	case ScopeRoot, ScopeFull, ScopeReviewer:
		return true
	case ScopeAgent:
		return path == agentToolsPath || strings.HasPrefix(path, agentToolsPath+"/")
	}
	return false
}

// CanReview reports whether this principal may decide a mutation request:
// approve, deny, supersede, undo, or edit one before it runs. Only the master
// secret (the web app) and a reviewer token (the phone app) may; a full,
// agent or feed token may propose and withdraw, never decide.
func (p Principal) CanReview() bool {
	switch p.Scope {
	case ScopeRoot, ScopeReviewer:
		return true
	}
	return false
}

// TokenAuthenticator resolves an issued token to its principal. It returns
// ErrUnknownToken for a credential it does not recognise, a
// *TokenRejectedError for a recognised one that is no longer valid (the
// message is shown to the client), and any other error for an outage.
type TokenAuthenticator interface {
	AuthenticateToken(ctx context.Context, raw string) (Principal, error)
}

// ErrUnknownToken means the credential is neither the master secret nor an
// issued token.
var ErrUnknownToken = errors.New("invalid bearer token")

// TokenRejectedError is a recognised token that has been revoked, has gone
// unused past its idle window, or has passed its hard expiry.
type TokenRejectedError struct{ Reason string }

func (e *TokenRejectedError) Error() string { return e.Reason }

// SetTokenAuthenticator lets the static bearer accept issued tokens beside the
// master secret. Without one, only the master secret authenticates.
func (s *Service) SetTokenAuthenticator(a TokenAuthenticator) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens = a
}

func (s *Service) tokenAuthenticator() TokenAuthenticator {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokens
}

// PrincipalFromContext returns the static-bearer principal of the request,
// and false when the request was not authenticated by the static bearer.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	if h, ok := ctx.Value(clientNameKey{}).(*clientNameHolder); ok && h.principal != nil {
		return *h.principal, true
	}
	return Principal{}, false
}

// WithPrincipal returns ctx carrying p as the static-bearer principal, as
// RequireStaticBearer would set it. Handlers under test use it.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	if !hasClientNameHolder(ctx) {
		ctx = WithClientNameHolder(ctx)
	}
	setPrincipal(ctx, p)
	return ctx
}

func setPrincipal(ctx context.Context, p Principal) {
	if h, ok := ctx.Value(clientNameKey{}).(*clientNameHolder); ok {
		h.name = p.ClientName
		h.principal = &p
	}
}

// RequireRoot wraps a handler already behind RequireStaticBearer so that only
// the master secret reaches it.
func RequireRoot(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := PrincipalFromContext(r.Context())
		if !ok || p.Scope != ScopeRoot {
			http.Error(w, "this endpoint requires the master secret (PDW_SECRET_TOKEN); issued tokens cannot administer tokens", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
