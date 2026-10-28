package accesstokens

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

const (
	// KindOAuth is an OAuth MCP grant: one connector authorization. It has no
	// usable secret of its own; the access and refresh tokens minted from it
	// name it as gid, and revoking the row stops them all.
	KindOAuth = "oauth"

	// OAuthGrantIdleTimeout is how long a connector may go unused (no MCP
	// request and no refresh) before its grant lapses.
	OAuthGrantIdleTimeout = 90 * 24 * time.Hour

	// grantCacheFor is how long a successful grant check is reused, so an MCP
	// session does not cost a row read per request. Revoke clears it.
	grantCacheFor = time.Minute
)

// ErrGrantStoreUnavailable is the answer of a grant store that must not be
// trusted: the app has a database but its token store fell back to memory,
// where every grant would look unknown.
var ErrGrantStoreUnavailable = errors.New("token store is in memory because the database store failed to start; OAuth grants cannot be checked")

type grantCache struct {
	mu    sync.Mutex
	valid map[string]time.Time // grant id -> checked until
}

// DisableOAuthGrants makes every grant call fail as unavailable. It is for a
// service whose store is an in-memory stand-in for a database that failed to
// start: answering "unknown grant" from it would log every connector out.
func (s *Service) DisableOAuthGrants(reason error) { s.grantsDown = reason }

// CreateOAuthGrant implements pdwauth.OAuthGrants.
func (s *Service) CreateOAuthGrant(ctx context.Context, clientName string) (string, error) {
	if s.grantsDown != nil {
		return "", s.grantsDown
	}
	id, secret, err := newCredential()
	if err != nil {
		return "", err
	}
	if err := s.store.Insert(ctx, s.newGrant(id, clientName, "oauth connector"), hashSecret(secret)); err != nil {
		return "", err
	}
	s.logger.InfoContext(ctx, "oauth grant created", "token_id", id, "client", clientName)
	return id, nil
}

// EnsureOAuthGrant implements pdwauth.OAuthGrants.
func (s *Service) EnsureOAuthGrant(ctx context.Context, id, clientName string) error {
	if s.grantsDown != nil {
		return s.grantsDown
	}
	_, _, err := s.store.Get(ctx, id)
	if errors.Is(err, ErrNotFound) {
		// The secret column is unused for a grant; a random value keeps it
		// unmatchable.
		_, secret, credErr := newCredential()
		if credErr != nil {
			return credErr
		}
		insertErr := s.store.Insert(ctx, s.newGrant(id, clientName, "oauth connector (moved from a pre-grant token)"), hashSecret(secret))
		if insertErr == nil {
			s.logger.InfoContext(ctx, "oauth grant created for a pre-grant token", "token_id", id, "client", clientName)
		} else if _, _, getErr := s.store.Get(ctx, id); getErr != nil {
			// Not a concurrent retry that inserted it first.
			return insertErr
		}
	} else if err != nil {
		return err
	}
	return s.CheckOAuthGrant(ctx, id)
}

// CheckOAuthGrant implements pdwauth.OAuthGrants.
func (s *Service) CheckOAuthGrant(ctx context.Context, id string) error {
	if s.grantsDown != nil {
		return s.grantsDown
	}
	now := s.now().UTC()
	s.grantCache.mu.Lock()
	until, cached := s.grantCache.valid[id]
	s.grantCache.mu.Unlock()
	if cached && now.Before(until) {
		return nil
	}
	token, _, err := s.store.Get(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return pdwauth.ErrUnknownToken
	}
	if err != nil {
		return err
	}
	if token.Kind != KindOAuth {
		return pdwauth.ErrUnknownToken
	}
	if status := token.Status(now); status != StatusActive {
		return &pdwauth.TokenRejectedError{Reason: grantRejectionReason(token, status)}
	}
	if now.Sub(token.LastUsedAt) >= touchEvery {
		if err := s.store.Touch(ctx, id, now); err != nil {
			s.logger.WarnContext(ctx, "oauth grant last_used_at update failed", "token_id", id, "error", err)
		}
	}
	s.grantCache.mu.Lock()
	if s.grantCache.valid == nil {
		s.grantCache.valid = map[string]time.Time{}
	}
	s.grantCache.valid[id] = now.Add(grantCacheFor)
	s.grantCache.mu.Unlock()
	return nil
}

func (s *Service) forgetGrant(id string) {
	s.grantCache.mu.Lock()
	delete(s.grantCache.valid, id)
	s.grantCache.mu.Unlock()
}

func (s *Service) newGrant(id, clientName, label string) Token {
	now := s.now().UTC()
	return Token{
		ID:          id,
		Kind:        KindOAuth,
		Scope:       pdwauth.ScopeOAuthQuery,
		ClientName:  clientName,
		Label:       label,
		CreatedBy:   "oauth",
		CreatedAt:   now,
		LastUsedAt:  now,
		IdleTimeout: OAuthGrantIdleTimeout,
	}
}

func grantRejectionReason(token Token, status string) string {
	switch status {
	case StatusRevoked:
		return fmt.Sprintf("connector grant %s was revoked at %s; re-authorize the connector", token.ID, token.RevokedAt.UTC().Format(time.RFC3339))
	case StatusIdleExpired:
		return fmt.Sprintf("connector grant %s lapsed: unused since %s, past its %s idle limit; re-authorize the connector", token.ID, token.LastUsedAt.UTC().Format(time.RFC3339), formatDuration(token.IdleTimeout))
	default:
		return fmt.Sprintf("connector grant %s is %s; re-authorize the connector", token.ID, status)
	}
}
