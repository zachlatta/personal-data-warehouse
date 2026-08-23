package whoopsession

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

// Endpoint is the signed path the local publisher POSTs to. It reuses the
// generic object-upload signing scheme (HMAC over endpoint + body sha + exp).
const Endpoint = "/ingest/whoop-private/session"

const maxBodyBytes = 1 << 20 // session credentials are small; cap defensively.

type publishRequest struct {
	Account          string `json:"account"`
	SessionKey       string `json:"session_key"`
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	AccessExpiresAt  string `json:"access_expires_at"`
	RefreshExpiresAt string `json:"refresh_expires_at"`
	SourceBrowser    string `json:"source_browser"`
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
// otherwise good credential: the poller refreshes on first use anyway, and a
// rejected publish is a much worse failure than a pessimistic expiry.
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
			s.logger.WarnContext(r.Context(), "whoop session upload link rejected", "error", err)
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
		// The refresh token is the load-bearing half: the access token expires
		// in 24h and the poller can always mint a new one, but without a
		// refresh token the credential is dead on arrival.
		if req.Account == "" || req.RefreshToken == "" {
			http.Error(w, "account and refresh_token are required", http.StatusBadRequest)
			return
		}
		sessionKey := req.SessionKey
		if sessionKey == "" {
			sessionKey = "default"
		}
		ack, err := s.store.Upsert(r.Context(), Credential{
			Account:          req.Account,
			SessionKey:       sessionKey,
			AccessToken:      req.AccessToken,
			RefreshToken:     req.RefreshToken,
			AccessExpiresAt:  parseTime(req.AccessExpiresAt),
			RefreshExpiresAt: parseTime(req.RefreshExpiresAt),
			SourceBrowser:    req.SourceBrowser,
		}, s.now())
		if err != nil {
			s.logger.ErrorContext(r.Context(), "whoop session upsert failed", "error", err)
			http.Error(w, "could not store session", http.StatusBadGateway)
			return
		}
		s.logger.InfoContext(r.Context(), "whoop private session published",
			"account", req.Account, "session_key", sessionKey,
			"refresh_token_sha256", ack.RefreshTokenSHA256,
			"refresh_expires_at", ack.RefreshExpiresAt)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ack)
	})
}
