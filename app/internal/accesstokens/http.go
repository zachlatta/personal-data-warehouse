package accesstokens

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

const (
	// TokensPath is the root-only token administration API.
	TokensPath = "/api/auth/tokens"
	// BootstrapRedeemPath exchanges a cli_authorize code for an agent token.
	// It takes no bearer: the single-use code is the credential.
	BootstrapRedeemPath = "/api/cli/bootstrap"
	// BootstrapScriptPath serves the script a cli_authorize command pipes to sh.
	BootstrapScriptPath = "/cli/bootstrap.sh"

	maxRequestBytes = 16 << 10
)

// TokenView is a token as the API shows it: never its secret.
type TokenView struct {
	ID              string     `json:"id"`
	Kind            string     `json:"kind"`
	Scope           string     `json:"scope"`
	Status          string     `json:"status"`
	ClientName      string     `json:"client_name"`
	Label           string     `json:"label,omitempty"`
	CreatedBy       string     `json:"created_by,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	LastUsedAt      time.Time  `json:"last_used_at"`
	IdleTimeout     string     `json:"idle_timeout,omitempty"`
	IdleExpiresAt   *time.Time `json:"idle_expires_at,omitempty"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
	RevokedAt       *time.Time `json:"revoked_at,omitempty"`
	RedeemedAt      *time.Time `json:"redeemed_at,omitempty"`
	RedeemedTokenID string     `json:"redeemed_token_id,omitempty"`
}

func viewOf(t Token, now time.Time) TokenView {
	opt := func(at time.Time) *time.Time {
		if at.IsZero() {
			return nil
		}
		at = at.UTC()
		return &at
	}
	v := TokenView{
		ID:              t.ID,
		Kind:            t.Kind,
		Scope:           string(t.Scope),
		Status:          t.Status(now),
		ClientName:      t.ClientName,
		Label:           t.Label,
		CreatedBy:       t.CreatedBy,
		CreatedAt:       t.CreatedAt.UTC(),
		LastUsedAt:      t.LastUsedAt.UTC(),
		ExpiresAt:       opt(t.ExpiresAt),
		RevokedAt:       opt(t.RevokedAt),
		RedeemedAt:      opt(t.RedeemedAt),
		RedeemedTokenID: t.RedeemedTokenID,
	}
	if t.Kind == KindAccess && t.IdleTimeout > 0 {
		v.IdleTimeout = formatDuration(t.IdleTimeout)
		v.IdleExpiresAt = opt(t.IdleExpiresAt())
	}
	return v
}

// RedeemResponse is what `pdw login --bootstrap` receives.
type RedeemResponse struct {
	BaseURL    string    `json:"base_url"`
	Token      string    `json:"token"`
	TokenID    string    `json:"token_id"`
	ClientName string    `json:"client_name"`
	Scope      string    `json:"scope"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// Register mounts the token API (behind bearer, then root-only), the code
// redemption endpoint and the bootstrap script.
func Register(mux *http.ServeMux, svc *Service, bearer func(http.Handler) http.Handler, baseURL string) {
	baseURL = strings.TrimRight(baseURL, "/")
	admin := bearer(pdwauth.RequireRoot(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		svc.serveAdmin(w, r)
	})))
	mux.Handle(TokensPath, admin)
	mux.Handle(TokensPath+"/", admin)
	mux.HandleFunc(BootstrapRedeemPath, func(w http.ResponseWriter, r *http.Request) {
		svc.serveRedeem(w, r, baseURL)
	})
	script := renderBootstrapScript(baseURL)
	mux.HandleFunc(BootstrapScriptPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/x-shellscript; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte(script))
	})
}

func (s *Service) serveAdmin(w http.ResponseWriter, r *http.Request) {
	id := strings.Trim(strings.TrimPrefix(r.URL.Path, TokensPath), "/")
	switch {
	case id == "" && r.Method == http.MethodGet:
		tokens, err := s.List(r.Context())
		if err != nil {
			s.logger.ErrorContext(r.Context(), "list access tokens failed", "error", err)
			http.Error(w, "could not list tokens", http.StatusInternalServerError)
			return
		}
		now := s.now()
		views := make([]TokenView, 0, len(tokens))
		for _, t := range tokens {
			views = append(views, viewOf(t, now))
		}
		writeJSON(w, http.StatusOK, map[string]any{"tokens": views})
	case id == "" && r.Method == http.MethodPost:
		var body struct {
			ClientName  string `json:"client_name"`
			Label       string `json:"label"`
			Scope       string `json:"scope"`
			IdleTimeout string `json:"idle_timeout"`
			TTL         string `json:"ttl"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes)).Decode(&body); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		idle, err := ParseDuration(body.IdleTimeout)
		if err != nil {
			http.Error(w, "idle_timeout: "+err.Error(), http.StatusBadRequest)
			return
		}
		ttl, err := ParseDuration(body.TTL)
		if err != nil {
			http.Error(w, "ttl: "+err.Error(), http.StatusBadRequest)
			return
		}
		issued, err := s.Issue(r.Context(), IssueRequest{
			ClientName:  body.ClientName,
			Label:       body.Label,
			Scope:       pdwauth.Scope(body.Scope),
			IdleTimeout: idle,
			TTL:         ttl,
			CreatedBy:   pdwauth.ClientNameFromContext(r.Context()),
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"token": issued.Secret, "info": viewOf(issued.Token, s.now())})
	case id != "" && r.Method == http.MethodDelete:
		err := s.Revoke(r.Context(), id)
		if errors.Is(err, ErrNotFound) {
			http.Error(w, "no token "+id, http.StatusNotFound)
			return
		}
		if err != nil {
			s.logger.ErrorContext(r.Context(), "revoke access token failed", "token_id", id, "error", err)
			http.Error(w, "could not revoke token", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"revoked": id})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Service) serveRedeem(w http.ResponseWriter, r *http.Request, baseURL string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes)).Decode(&body); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	issued, err := s.Redeem(r.Context(), body.Code)
	var rejected *pdwauth.TokenRejectedError
	if errors.As(err, &rejected) {
		s.logger.WarnContext(r.Context(), "cli_authorize redemption rejected", "reason", rejected.Reason)
		http.Error(w, rejected.Reason, http.StatusUnauthorized)
		return
	}
	if err != nil {
		s.logger.ErrorContext(r.Context(), "cli_authorize redemption failed", "error", err)
		http.Error(w, "could not redeem code", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, RedeemResponse{
		BaseURL:    baseURL,
		Token:      issued.Secret,
		TokenID:    issued.Token.ID,
		ClientName: issued.Token.ClientName,
		Scope:      string(issued.Token.Scope),
		ExpiresAt:  issued.Token.ExpiresAt.UTC(),
	})
}

// BootstrapCommand is the one line cli_authorize hands an agent.
func BootstrapCommand(baseURL, code string) string {
	return "curl -fsSL " + shellQuote(strings.TrimRight(baseURL, "/")+BootstrapScriptPath) + " | sh -s -- " + shellQuote(code)
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// The script carries no secret: the code arrives as $1. It installs pdw from
// the GitHub release when the environment has none, then redeems the code.
var bootstrapScript = template.Must(template.New("bootstrap").Parse(`#!/bin/sh
# Bootstrap an authorized pdw CLI from a cli_authorize code.
# Usage: curl -fsSL {{.BaseURL}}/cli/bootstrap.sh | sh -s -- <code>
set -eu

code="${1:-}"
if [ -z "$code" ]; then
  echo "usage: curl -fsSL {{.BaseURL}}/cli/bootstrap.sh | sh -s -- <code>" >&2
  echo "get a code from the cli_authorize tool of the Personal Data Warehouse MCP" >&2
  exit 2
fi
base_url={{.QuotedBaseURL}}

if command -v pdw >/dev/null 2>&1; then
  pdw_bin=$(command -v pdw)
else
  install_dir="${PDW_INSTALL_DIR:-$HOME/.local/bin}"
  mkdir -p "$install_dir"
  echo "==> installing pdw into $install_dir"
  curl -fsSL https://raw.githubusercontent.com/zachlatta/personal-data-warehouse/main/app/install.sh \
    | PDW_INSTALL_DIR="$install_dir" sh
  pdw_bin="$install_dir/pdw"
  case ":$PATH:" in
    *":$install_dir:"*) ;;
    *) echo "==> $install_dir is not on PATH; run: export PATH=\"$install_dir:\$PATH\"" ;;
  esac
fi

"$pdw_bin" login --bootstrap "$code" --base-url "$base_url"
`))

func renderBootstrapScript(baseURL string) string {
	var b strings.Builder
	_ = bootstrapScript.Execute(&b, map[string]string{"BaseURL": baseURL, "QuotedBaseURL": shellQuote(baseURL)})
	return b.String()
}

var durationPart = regexp.MustCompile(`^(\d+)d`)

// ParseDuration is time.ParseDuration plus a leading whole-days term ("30d",
// "1d12h"). Empty is zero; negative is an error.
func ParseDuration(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	var total time.Duration
	if m := durationPart.FindStringSubmatch(raw); m != nil {
		days, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, err
		}
		total = time.Duration(days) * 24 * time.Hour
		raw = raw[len(m[0]):]
	}
	if raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return 0, fmt.Errorf("want a duration like 30d, 12h or 1d12h: %w", err)
		}
		if d < 0 {
			return 0, errors.New("must not be negative")
		}
		total += d
	}
	return total, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
