package slacksession

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

// Endpoint is the signed path the local publisher POSTs to. It reuses the
// generic object-upload signing scheme (HMAC over endpoint + body sha + exp).
const Endpoint = "/ingest/slack/session"

const maxBodyBytes = 1 << 20 // session credentials are small; cap defensively.

type publishRequest struct {
	Account         string `json:"account"`
	SessionKey      string `json:"session_key"`
	SessionToken    string `json:"session_token"`
	SessionCookie   string `json:"session_cookie"`
	TeamID          string `json:"team_id"`
	EnterpriseID    string `json:"enterprise_id"`
	UserID          string `json:"user_id"`
	TeamURL         string `json:"team_url"`
	SourceApp       string `json:"source_app"`
	CookieExpiresAt string `json:"cookie_expires_at"`
}

// Service verifies the upload signature and persists the credential.
type Service struct {
	store  Store
	signer *pdwauth.Service
	now    func() time.Time
	logger *slog.Logger
}

func NewService(store Store, signer *pdwauth.Service, now func() time.Time, logger *slog.Logger) *Service {
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{store: store, signer: signer, now: now, logger: logger}
}

// parseTime accepts RFC3339 and falls back to the zero time, which the store
// writes as the warehouse's epoch sentinel. A missing expiry must not reject an
// otherwise good credential: a rejected publish is a much worse failure than an
// unknown expiry.
func parseTime(raw string) time.Time {
	if raw == "" {
		return time.Unix(0, 0).UTC()
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Unix(0, 0).UTC()
	}
	return parsed.UTC()
}

func (s *Service) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		declaredSHA := q.Get("content_sha256")
		if err := s.signer.VerifyObjectUpload(Endpoint, declaredSHA, q.Get("exp"), q.Get("sig")); err != nil {
			s.logger.WarnContext(r.Context(), "slack session upload link rejected", "error", err)
			http.Error(w, "invalid or expired upload link", http.StatusForbidden)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
		if err != nil {
			http.Error(w, "could not read body", http.StatusBadRequest)
			return
		}
		if int64(len(body)) > maxBodyBytes {
			http.Error(w, "credential too large", http.StatusRequestEntityTooLarge)
			return
		}
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != declaredSHA {
			http.Error(w, "content_sha256 does not match body", http.StatusBadRequest)
			return
		}
		var req publishRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		// Both halves are required, and this is the place to enforce it: an
		// xoxc token without the `d` cookie authenticates as nobody, so
		// accepting one alone would store a credential that looks present in
		// every dashboard and 401s on first use.
		if req.Account == "" || req.SessionToken == "" || req.SessionCookie == "" {
			http.Error(w, "account, session_token and session_cookie are all required", http.StatusBadRequest)
			return
		}
		// Slack's Enterprise Grid orgs report an E-prefixed id from auth.test
		// where every warehouse row is keyed by the workspace T-id. Storing one
		// as the other would fork the dataset silently, so the client sends them
		// in separate fields and a swapped pair is rejected here rather than
		// written. See slack_session.py.
		if strings.HasPrefix(req.TeamID, "E") {
			http.Error(w, "team_id must be a workspace id, not an enterprise id", http.StatusBadRequest)
			return
		}
		sessionKey := req.SessionKey
		if sessionKey == "" {
			sessionKey = "default"
		}
		ack, err := s.store.Upsert(r.Context(), Credential{
			Account:         req.Account,
			SessionKey:      sessionKey,
			SessionToken:    req.SessionToken,
			SessionCookie:   req.SessionCookie,
			TeamID:          req.TeamID,
			EnterpriseID:    req.EnterpriseID,
			UserID:          req.UserID,
			TeamURL:         req.TeamURL,
			SourceApp:       req.SourceApp,
			CookieExpiresAt: parseTime(req.CookieExpiresAt),
		}, s.now())
		if err != nil {
			s.logger.ErrorContext(r.Context(), "slack session upsert failed", "error", err)
			http.Error(w, "could not store session", http.StatusBadGateway)
			return
		}
		s.logger.InfoContext(r.Context(), "slack client session published",
			"account", req.Account, "session_key", sessionKey,
			"token_sha256", ack.TokenSHA256, "team_id", ack.TeamID,
			"enterprise_id", ack.EnterpriseID, "cookie_expires_at", ack.CookieExpiresAt)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ack)
	})
}
