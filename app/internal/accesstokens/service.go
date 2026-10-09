// Package accesstokens issues, authenticates, lists and revokes the static
// bearer tokens that sit beside PDW_SECRET_TOKEN, and runs the single-use
// bootstrap codes behind the cli_authorize MCP tool. The model and its
// reasons are in docs/agents/access-tokens.md.
package accesstokens

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

const (
	KindAccess    = "access"
	KindBootstrap = "bootstrap"

	accessPrefix    = "pdw_"
	bootstrapPrefix = "pdwb_"

	// DefaultIdleTimeout is how long an issued token may go unused before it
	// stops authenticating.
	DefaultIdleTimeout = 30 * 24 * time.Hour
	MinIdleTimeout     = time.Hour
	MaxIdleTimeout     = 365 * 24 * time.Hour

	// DefaultAgentTTL and MaxAgentTTL bound a cli_authorize token: a hard
	// expiry that use does not extend.
	DefaultAgentTTL = 24 * time.Hour
	MaxAgentTTL     = 7 * 24 * time.Hour

	// BootstrapCodeLifetime is how long a cli_authorize command can wait to be
	// run before its code is useless.
	BootstrapCodeLifetime = 15 * time.Minute

	// touchEvery throttles last_used_at writes: one per token per minute,
	// which is the precision of the idle clock.
	touchEvery = time.Minute
)

// Token statuses, as reported by Status and the listing.
const (
	StatusActive      = "active"
	StatusRevoked     = "revoked"
	StatusExpired     = "expired"
	StatusIdleExpired = "idle_expired"
	StatusRedeemed    = "redeemed"
)

// ErrNotFound is a token id that does not exist.
var ErrNotFound = errors.New("token not found")

// Token is one row of private.app_access_tokens, without its secret. Zero
// times mean "never" (the warehouse stores the epoch).
type Token struct {
	ID              string        `json:"id"`
	Kind            string        `json:"kind"`
	Scope           pdwauth.Scope `json:"scope"`
	ClientName      string        `json:"client_name"`
	Label           string        `json:"label,omitempty"`
	CreatedBy       string        `json:"created_by,omitempty"`
	CreatedAt       time.Time     `json:"created_at"`
	LastUsedAt      time.Time     `json:"last_used_at"`
	IdleTimeout     time.Duration `json:"-"`
	ExpiresAt       time.Time     `json:"expires_at,omitempty"`
	RevokedAt       time.Time     `json:"revoked_at,omitempty"`
	GrantTTL        time.Duration `json:"-"`
	RedeemedAt      time.Time     `json:"redeemed_at,omitempty"`
	RedeemedTokenID string        `json:"redeemed_token_id,omitempty"`
}

// Status is the token's standing at now.
func (t Token) Status(now time.Time) string {
	switch {
	case !t.RevokedAt.IsZero():
		return StatusRevoked
	case t.Kind == KindBootstrap && !t.RedeemedAt.IsZero():
		return StatusRedeemed
	case !t.ExpiresAt.IsZero() && !now.Before(t.ExpiresAt):
		return StatusExpired
	case t.IdleTimeout > 0 && !now.Before(t.LastUsedAt.Add(t.IdleTimeout)):
		return StatusIdleExpired
	}
	return StatusActive
}

// IdleExpiresAt is when the token dies if it is not used again.
func (t Token) IdleExpiresAt() time.Time {
	if t.IdleTimeout <= 0 {
		return time.Time{}
	}
	return t.LastUsedAt.Add(t.IdleTimeout)
}

// Issued is a token together with its plaintext, which exists only here.
type Issued struct {
	Token  Token
	Secret string
}

// Store persists tokens. Redeem must be atomic: of any number of concurrent
// calls for one code, exactly one may see check succeed and mint.
type Store interface {
	Insert(ctx context.Context, token Token, secretHash string) error
	Get(ctx context.Context, id string) (Token, string, error)
	Touch(ctx context.Context, id string, at time.Time) error
	List(ctx context.Context) ([]Token, error)
	Revoke(ctx context.Context, id string, at time.Time) error
	// Redeem locks the bootstrap row codeID, calls mint with it and its
	// secret hash, and when mint succeeds inserts the token it returned and
	// marks the code redeemed by it.
	Redeem(ctx context.Context, codeID string, mint func(code Token, hash string) (Token, error), mintedHash string, at time.Time) (Token, error)
}

// Service is the token authority over a Store.
type Service struct {
	store  Store
	now    func() time.Time
	logger *slog.Logger
}

func NewService(store Store, now func() time.Time, logger *slog.Logger) *Service {
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{store: store, now: now, logger: logger.With("component", "access_tokens")}
}

// IssueRequest asks for a long-lived token. Zero IdleTimeout means
// DefaultIdleTimeout; zero TTL means no hard expiry; zero Scope means full.
type IssueRequest struct {
	ClientName  string
	Label       string
	Scope       pdwauth.Scope
	IdleTimeout time.Duration
	TTL         time.Duration
	CreatedBy   string
}

func (s *Service) Issue(ctx context.Context, req IssueRequest) (Issued, error) {
	name, ok := pdwauth.ValidateClientName(req.ClientName)
	if !ok {
		return Issued{}, fmt.Errorf("client name is required (at most %d characters, no ':' or control characters)", pdwauth.MaxClientNameLen)
	}
	scope := req.Scope
	if scope == "" {
		scope = pdwauth.ScopeFull
	}
	if !scope.Issuable() {
		return Issued{}, fmt.Errorf("scope must be %q or %q", pdwauth.ScopeFull, pdwauth.ScopeAgent)
	}
	idle := req.IdleTimeout
	if idle == 0 {
		idle = DefaultIdleTimeout
	}
	if idle < MinIdleTimeout || idle > MaxIdleTimeout {
		return Issued{}, fmt.Errorf("idle timeout must be between %s and %s", MinIdleTimeout, MaxIdleTimeout)
	}
	if req.TTL < 0 {
		return Issued{}, errors.New("ttl must not be negative")
	}
	now := s.now().UTC()
	token := Token{
		Kind:        KindAccess,
		Scope:       scope,
		ClientName:  name,
		Label:       strings.TrimSpace(req.Label),
		CreatedBy:   req.CreatedBy,
		CreatedAt:   now,
		LastUsedAt:  now,
		IdleTimeout: idle,
	}
	if req.TTL > 0 {
		token.ExpiresAt = now.Add(req.TTL)
	}
	return s.insert(ctx, token, accessPrefix)
}

func (s *Service) insert(ctx context.Context, token Token, prefix string) (Issued, error) {
	id, secret, err := newCredential()
	if err != nil {
		return Issued{}, err
	}
	token.ID = id
	if err := s.store.Insert(ctx, token, hashSecret(secret)); err != nil {
		return Issued{}, err
	}
	s.logger.InfoContext(ctx, "access token issued", "token_id", id, "kind", token.Kind, "scope", token.Scope, "client", token.ClientName, "created_by", token.CreatedBy)
	return Issued{Token: token, Secret: prefix + id + "_" + secret}, nil
}

// AuthenticateToken implements pdwauth.TokenAuthenticator.
func (s *Service) AuthenticateToken(ctx context.Context, raw string) (pdwauth.Principal, error) {
	id, secret, ok := parseCredential(raw, accessPrefix)
	if !ok {
		return pdwauth.Principal{}, pdwauth.ErrUnknownToken
	}
	token, hash, err := s.store.Get(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return pdwauth.Principal{}, pdwauth.ErrUnknownToken
	}
	if err != nil {
		return pdwauth.Principal{}, err
	}
	if token.Kind != KindAccess || !hashMatches(hash, secret) {
		return pdwauth.Principal{}, pdwauth.ErrUnknownToken
	}
	now := s.now().UTC()
	if status := token.Status(now); status != StatusActive {
		return pdwauth.Principal{}, &pdwauth.TokenRejectedError{Reason: rejectionReason(token, status)}
	}
	if now.Sub(token.LastUsedAt) >= touchEvery {
		if err := s.store.Touch(ctx, id, now); err != nil {
			// A missed touch costs at most a minute of idle clock; it is not
			// worth failing an otherwise valid request over.
			s.logger.WarnContext(ctx, "access token last_used_at update failed", "token_id", id, "error", err)
		}
	}
	return pdwauth.Principal{Scope: token.Scope, TokenID: token.ID}, nil
}

func rejectionReason(token Token, status string) string {
	var reason string
	switch status {
	case StatusRevoked:
		reason = fmt.Sprintf("access token %s was revoked at %s", token.ID, token.RevokedAt.UTC().Format(time.RFC3339))
	case StatusExpired:
		reason = fmt.Sprintf("access token %s expired at %s", token.ID, token.ExpiresAt.UTC().Format(time.RFC3339))
	case StatusIdleExpired:
		reason = fmt.Sprintf("access token %s expired: unused since %s, past its %s idle limit", token.ID, token.LastUsedAt.UTC().Format(time.RFC3339), formatDuration(token.IdleTimeout))
	default:
		reason = fmt.Sprintf("access token %s is %s", token.ID, status)
	}
	if token.Scope == pdwauth.ScopeAgent {
		return reason + "; ask for a new one with the cli_authorize MCP tool"
	}
	return reason + "; issue a new one with `pdw token create`"
}

// List returns every token and bootstrap code, newest first.
func (s *Service) List(ctx context.Context) ([]Token, error) { return s.store.List(ctx) }

// Revoke stops a token (or an unredeemed bootstrap code) authenticating.
func (s *Service) Revoke(ctx context.Context, id string) error {
	if err := s.store.Revoke(ctx, strings.TrimSpace(id), s.now().UTC()); err != nil {
		return err
	}
	s.logger.InfoContext(ctx, "access token revoked", "token_id", id)
	return nil
}

// BootstrapRequest asks for a cli_authorize code.
type BootstrapRequest struct {
	ClientName string
	TokenTTL   time.Duration // zero means DefaultAgentTTL
	CreatedBy  string
}

// BootstrapCode is a single-use code and what redeeming it yields.
type BootstrapCode struct {
	Code      string
	ID        string
	ExpiresAt time.Time
	TokenTTL  time.Duration
}

func (s *Service) CreateBootstrap(ctx context.Context, req BootstrapRequest) (BootstrapCode, error) {
	name, ok := pdwauth.ValidateClientName(req.ClientName)
	if !ok {
		return BootstrapCode{}, fmt.Errorf("client name is required (at most %d characters, no ':' or control characters)", pdwauth.MaxClientNameLen)
	}
	ttl := req.TokenTTL
	if ttl == 0 {
		ttl = DefaultAgentTTL
	}
	if ttl < time.Hour || ttl > MaxAgentTTL {
		return BootstrapCode{}, fmt.Errorf("token lifetime must be between 1h and %s", formatDuration(MaxAgentTTL))
	}
	now := s.now().UTC()
	issued, err := s.insert(ctx, Token{
		Kind:       KindBootstrap,
		Scope:      pdwauth.ScopeAgent,
		ClientName: name,
		CreatedBy:  req.CreatedBy,
		CreatedAt:  now,
		LastUsedAt: now,
		ExpiresAt:  now.Add(BootstrapCodeLifetime),
		GrantTTL:   ttl,
	}, bootstrapPrefix)
	if err != nil {
		return BootstrapCode{}, err
	}
	return BootstrapCode{Code: issued.Secret, ID: issued.Token.ID, ExpiresAt: issued.Token.ExpiresAt, TokenTTL: ttl}, nil
}

// Redeem exchanges a bootstrap code for an agent token, exactly once.
func (s *Service) Redeem(ctx context.Context, code string) (Issued, error) {
	codeID, secret, ok := parseCredential(strings.TrimSpace(code), bootstrapPrefix)
	if !ok {
		return Issued{}, &pdwauth.TokenRejectedError{Reason: "not a cli_authorize code"}
	}
	id, mintedSecret, err := newCredential()
	if err != nil {
		return Issued{}, err
	}
	now := s.now().UTC()
	mint := func(row Token, hash string) (Token, error) {
		if row.Kind != KindBootstrap || !hashMatches(hash, secret) {
			return Token{}, &pdwauth.TokenRejectedError{Reason: "unknown cli_authorize code"}
		}
		switch row.Status(now) {
		case StatusActive:
		case StatusRedeemed:
			return Token{}, &pdwauth.TokenRejectedError{Reason: "this cli_authorize code was already used; ask for a new one"}
		case StatusExpired:
			return Token{}, &pdwauth.TokenRejectedError{Reason: fmt.Sprintf("this cli_authorize code expired at %s; ask for a new one", row.ExpiresAt.UTC().Format(time.RFC3339))}
		default:
			return Token{}, &pdwauth.TokenRejectedError{Reason: "this cli_authorize code was revoked"}
		}
		// The minted token inherits who asked for it and for how long. Its
		// idle window is its whole life: the hard expiry is the only clock.
		return Token{
			ID:          id,
			Kind:        KindAccess,
			Scope:       pdwauth.ScopeAgent,
			ClientName:  row.ClientName,
			Label:       "cli_authorize " + row.ID,
			CreatedBy:   row.CreatedBy,
			CreatedAt:   now,
			LastUsedAt:  now,
			IdleTimeout: row.GrantTTL,
			ExpiresAt:   now.Add(row.GrantTTL),
		}, nil
	}
	minted, err := s.store.Redeem(ctx, codeID, mint, hashSecret(mintedSecret), now)
	if errors.Is(err, ErrNotFound) {
		return Issued{}, &pdwauth.TokenRejectedError{Reason: "unknown cli_authorize code"}
	}
	if err != nil {
		return Issued{}, err
	}
	s.logger.InfoContext(ctx, "cli_authorize code redeemed", "code_id", codeID, "token_id", id, "client", minted.ClientName, "expires_at", minted.ExpiresAt)
	return Issued{Token: minted, Secret: accessPrefix + id + "_" + mintedSecret}, nil
}

// newCredential returns a public id (16 base32 characters, no '_') and a
// 256-bit secret.
func newCredential() (string, string, error) {
	var idBytes [10]byte
	var secretBytes [32]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return "", "", err
	}
	if _, err := rand.Read(secretBytes[:]); err != nil {
		return "", "", err
	}
	id := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(idBytes[:]))
	return id, base64.RawURLEncoding.EncodeToString(secretBytes[:]), nil
}

func parseCredential(raw, prefix string) (string, string, bool) {
	if !strings.HasPrefix(raw, prefix) {
		return "", "", false
	}
	id, secret, ok := strings.Cut(raw[len(prefix):], "_")
	if !ok || len(id) != 16 || secret == "" {
		return "", "", false
	}
	return id, secret, true
}

func hashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

func hashMatches(stored, secret string) bool {
	return subtle.ConstantTimeCompare([]byte(stored), []byte(hashSecret(secret))) == 1
}

func formatDuration(d time.Duration) string {
	if d >= 24*time.Hour && d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	}
	return d.String()
}
