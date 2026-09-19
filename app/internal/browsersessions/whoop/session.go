// Package whoop captures the app.whoop.com browser session and publishes it
// to the warehouse (`pdw whoop publish-session`), the port of
// personal_data_warehouse.whoop_private_session / whoop_private_setup.
//
// WHOOP requires MFA, so there is no unattended password login; the web app
// keeps its session in ordinary Chrome cookies on .whoop.com, so what the
// browser already holds is captured instead. The pair is a 24-hour access
// token (a Cognito JWT) and a 30-day refresh token that rotates on every
// refresh, so a sync that runs more often than monthly never needs this
// again. Nothing here logs or prints a token value.
package whoop

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
)

const (
	// CookieHostSuffix selects .whoop.com cookies.
	CookieHostSuffix = "whoop.com"
	// AccessTokenCookie carries the 24h Cognito JWT.
	AccessTokenCookie = "whoop-auth-token"
	// RefreshTokenCookie carries the opaque 30-day refresh token.
	RefreshTokenCookie = "whoop-auth-refresh-token"
	// AssumedRefreshTokenLifetime: the refresh token carries no readable
	// expiry, so the documented 30 days from capture is assumed; the server
	// corrects it on the first refresh.
	AssumedRefreshTokenLifetime = 30 * 24 * time.Hour
	// AssumedAccessTokenLifetime is used when the JWT cannot be read.
	AssumedAccessTokenLifetime = 24 * time.Hour
)

// CaptureError means no logged-in app.whoop.com session was found locally.
type CaptureError struct{ Msg string }

func (e *CaptureError) Error() string { return e.Msg }

func captureErrorf(format string, args ...any) error {
	return &CaptureError{Msg: fmt.Sprintf(format, args...)}
}

// Session is a captured WHOOP browser session.
type Session struct {
	Browser          string
	AccessToken      string
	RefreshToken     string
	AccessExpiresAt  time.Time
	RefreshExpiresAt time.Time
}

// Fingerprint is a stable, non-secret identity for the credential; it tracks
// the refresh token so the sync can tell a replacement from a rejection.
func (s Session) Fingerprint() string {
	sum := sha256.Sum256([]byte(s.RefreshToken))
	return hex.EncodeToString(sum[:])
}

// Redacted is the report-safe view.
func (s Session) Redacted() map[string]any {
	return map[string]any{
		"browser":              s.Browser,
		"access_expires_at":    Isoformat(s.AccessExpiresAt),
		"refresh_expires_at":   Isoformat(s.RefreshExpiresAt),
		"refresh_token_sha256": s.Fingerprint(),
	}
}

// Isoformat renders a UTC time the way Python's datetime.isoformat() does.
func Isoformat(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond() == 0 {
		return t.Format("2006-01-02T15:04:05+00:00")
	}
	return t.Format("2006-01-02T15:04:05.000000+00:00")
}

// AccessTokenExpiry reads `exp` out of the Cognito JWT without verifying it.
// An unverifiable or malformed token falls back to the documented 24 hours
// rather than failing the capture.
func AccessTokenExpiry(token string, now time.Time) time.Time {
	parts := strings.Split(token, ".")
	if len(parts) == 3 {
		if payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "=")); err == nil {
			var claims map[string]any
			if json.Unmarshal(payload, &claims) == nil {
				if exp, ok := claims["exp"].(float64); ok && exp == float64(int64(exp)) {
					return time.Unix(int64(exp), 0).UTC()
				}
			}
		}
	}
	return now.Add(AssumedAccessTokenLifetime)
}

// Capture finds a logged-in app.whoop.com session in a local Chrome-family
// browser; browser forces one, "" tries each in turn.
func Capture(host chromium.Host, browser string, now time.Time) (Session, error) {
	candidates := chromium.Browsers
	if browser != "" {
		profile, ok := chromium.BrowserByKey(browser)
		if !ok {
			return Session{}, captureErrorf("unknown browser %q; valid: %s", browser, chromium.BrowserKeys())
		}
		candidates = []chromium.Profile{profile}
	}
	var problems []string
	for _, profile := range candidates {
		dbs := host.CookieDBs(profile)
		if len(dbs) == 0 {
			continue
		}
		key, err := host.SafeStorageKey(profile)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", profile.DisplayName, err))
			continue
		}
		for _, db := range dbs {
			cookies, err := chromium.ReadCookiesForHost(db, key, CookieHostSuffix)
			if err != nil {
				problems = append(problems, fmt.Sprintf("%s: %v", profile.DisplayName, err))
				continue
			}
			access, refresh := cookies[AccessTokenCookie], cookies[RefreshTokenCookie]
			if access != "" && refresh != "" {
				return Session{
					Browser:          profile.DisplayName,
					AccessToken:      access,
					RefreshToken:     refresh,
					AccessExpiresAt:  AccessTokenExpiry(access, now),
					RefreshExpiresAt: now.Add(AssumedRefreshTokenLifetime),
				}, nil
			}
			if access != "" || refresh != "" {
				// A stale profile with one cookie must not shadow a good one.
				problems = append(problems, fmt.Sprintf("%s: partial whoop.com session (one cookie of two)", profile.DisplayName))
			}
		}
	}
	detail := "no Chrome-family browser held a whoop.com session"
	if len(problems) > 0 {
		detail = strings.Join(problems, "; ")
	}
	return Session{}, captureErrorf("could not find a logged-in app.whoop.com session in a local browser. Open Chrome, log in to app.whoop.com (MFA included), then retry. %s", detail)
}
