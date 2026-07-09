package server

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// claudeDesktopCredentialEndpoint is the one non-object-store ingest endpoint:
// it persists a secret (the claude.ai session credential) into Postgres rather
// than writing a Drive object. The Claude Desktop app keeps no transcripts on
// disk, so a clientside pusher ships the session credential here and the
// serverside Dagster poller reads it back to fetch conversations.
const claudeDesktopCredentialEndpoint = "/ingest/claude-desktop/credential"

// claudeDesktopCredentialStore upserts the claude.ai session credential keyed by
// account. The table is also created (idempotently, identical DDL) by the Python
// warehouse's ensure_claude_desktop_tables; whichever runs first wins.
type claudeDesktopCredentialStore struct {
	db      *sql.DB
	timeout time.Duration
}

type claudeDesktopCredentialWriter interface {
	upsert(context.Context, claudeDesktopCredential) error
}

func newClaudeDesktopCredentialStore(databaseURL string, timeout time.Duration) (*claudeDesktopCredentialStore, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &claudeDesktopCredentialStore{db: db, timeout: timeout}, nil
}

func (s *claudeDesktopCredentialStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

const privateSchemaDDL = `CREATE SCHEMA IF NOT EXISTS "private"`

var claudeDesktopCredentialDDL = `CREATE TABLE IF NOT EXISTS ` + warehouse.SQLRelation("claude_desktop_credentials") + ` (
	account text PRIMARY KEY,
	session_key text NOT NULL,
	org_id text NOT NULL DEFAULT '',
	expires_at timestamptz NULL,
	captured_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now()
)`

func (s *claudeDesktopCredentialStore) ensure(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, privateSchemaDDL); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, claudeDesktopCredentialDDL)
	return err
}

type claudeDesktopCredential struct {
	Account    string `json:"account"`
	SessionKey string `json:"session_key"`
	OrgID      string `json:"org_id"`
	ExpiresAt  string `json:"expires_at"`
	CapturedAt string `json:"captured_at"`
}

func (s *claudeDesktopCredentialStore) upsert(ctx context.Context, cred claudeDesktopCredential) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	var expiresAt any
	if t, err := parseTimestampUTC(cred.ExpiresAt); err == nil {
		expiresAt = t
	}
	capturedAt, err := parseTimestampUTC(cred.CapturedAt)
	if err != nil {
		capturedAt = time.Now().UTC()
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO `+warehouse.SQLRelation("claude_desktop_credentials")+` (account, session_key, org_id, expires_at, captured_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (account) DO UPDATE SET
			session_key = EXCLUDED.session_key,
			org_id = EXCLUDED.org_id,
			expires_at = EXCLUDED.expires_at,
			captured_at = EXCLUDED.captured_at,
			updated_at = now()
	`, cred.Account, cred.SessionKey, cred.OrgID, expiresAt, capturedAt)
	return err
}

// credentialIngestService handles the credential endpoint. It reuses the same
// HMAC upload-signature scheme as the object-store ingest endpoints (the
// signature binds endpoint + body sha + expiry), so a client needs only the app
// URL and shared signing key.
type credentialIngestService struct {
	store    claudeDesktopCredentialWriter
	signer   *pdwauth.Service
	maxBytes int64
	timeout  time.Duration
	now      func() time.Time
	logger   *slog.Logger
}

func (svc *credentialIngestService) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		endpoint := path.Clean(r.URL.Path)
		q := r.URL.Query()
		declaredSHA := q.Get("content_sha256")
		if err := svc.signer.VerifyObjectUpload(endpoint, declaredSHA, q.Get("exp"), q.Get("sig")); err != nil {
			svc.logger.WarnContext(r.Context(), "credential upload link rejected", "endpoint", endpoint, "error", err)
			http.Error(w, "invalid or expired upload link", http.StatusForbidden)
			return
		}
		body, err := readLimited(r.Body, svc.maxBytes)
		if err == errTooLarge {
			http.Error(w, "object too large", http.StatusRequestEntityTooLarge)
			return
		}
		if err != nil {
			http.Error(w, "could not read body", http.StatusBadRequest)
			return
		}
		actualSHA := hex.EncodeToString(sha256Sum(body))
		if actualSHA != declaredSHA {
			http.Error(w, "content_sha256 does not match body", http.StatusBadRequest)
			return
		}
		var cred claudeDesktopCredential
		if err := json.Unmarshal(body, &cred); err != nil {
			http.Error(w, "invalid credential json", http.StatusBadRequest)
			return
		}
		cred.Account = strings.TrimSpace(cred.Account)
		cred.SessionKey = strings.TrimSpace(cred.SessionKey)
		cred.OrgID = strings.TrimSpace(cred.OrgID)
		if cred.Account == "" || cred.SessionKey == "" || cred.OrgID == "" {
			http.Error(w, "account, session_key, and org_id are required", http.StatusBadRequest)
			return
		}
		timeout := svc.timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err := svc.store.upsert(ctx, cred); err != nil {
			svc.logger.ErrorContext(r.Context(), "credential store write failed", "endpoint", endpoint, "error", err)
			http.Error(w, "credential store error", http.StatusBadGateway)
			return
		}
		svc.logger.InfoContext(r.Context(), "claude desktop credential stored", "account", cred.Account, "org_id", cred.OrgID)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"ok":true}`)
	})
}
