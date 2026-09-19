package chatgpt

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// DefaultBaseURL is chatgpt.com.
	DefaultBaseURL = "https://chatgpt.com"
	// UserAgent is the Chrome UA the Python client impersonated.
	UserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
	// TokenWarnWithin is how long before the hard expiry a human is told to
	// sign in again: the token lives exactly 10 days, two days is a weekend.
	TokenWarnWithin = 2 * 24 * time.Hour
)

// AuthError means ChatGPT rejected the session (expired, logged out).
type AuthError struct{ Msg string }

func (e *AuthError) Error() string { return e.Msg }

// Validation is what /api/auth/session said about a cookie header.
type Validation struct {
	// SignedInAs is the account's email (or name), "unknown account" if absent.
	SignedInAs string
	// AccessTokenExpiry is the bearer's `exp`; zero when it could not be read.
	AccessTokenExpiry time.Time
	// Blocked is set when Cloudflare's managed challenge answered instead of
	// ChatGPT: the session could not be validated locally, which is not the
	// same as it being rejected.
	Blocked bool
	// BlockedReason says what was seen when Blocked.
	BlockedReason string
}

// Validator checks a cookie header against chatgpt.com.
type Validator struct {
	BaseURL string
	HTTP    *http.Client
	Now     func() time.Time
}

// NewValidator targets the real chatgpt.com.
func NewValidator() *Validator {
	return &Validator{BaseURL: DefaultBaseURL, HTTP: &http.Client{Timeout: 30 * time.Second}, Now: time.Now}
}

// IsCloudflareChallenge recognises the managed challenge that answers plain
// HTTP clients: a 403 tagged `cf-mitigated: challenge`.
func IsCloudflareChallenge(resp *http.Response) bool {
	return resp != nil && resp.StatusCode == http.StatusForbidden &&
		strings.EqualFold(strings.TrimSpace(resp.Header.Get("cf-mitigated")), "challenge")
}

// Validate calls /api/auth/session with the full cookie header. A Cloudflare
// challenge is reported as Blocked rather than as an error, because the
// server-side poller (which runs with browser impersonation) validates the
// session for real and reports on /pipelines; an auth failure is *AuthError.
func (v *Validator) Validate(cookieHeader string) (Validation, error) {
	httpClient := v.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	now := time.Now
	if v.Now != nil {
		now = v.Now
	}
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(v.BaseURL, "/")+"/api/auth/session", nil)
	if err != nil {
		return Validation{}, err
	}
	req.Header.Set("Cookie", cookieHeader)
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("Accept", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return Validation{}, fmt.Errorf("/api/auth/session request failed: %w", err)
	}
	defer resp.Body.Close()
	if IsCloudflareChallenge(resp) {
		return Validation{Blocked: true, BlockedReason: "chatgpt.com answered with a Cloudflare managed challenge (403, cf-mitigated: challenge)"}, nil
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return Validation{}, &AuthError{Msg: fmt.Sprintf("/api/auth/session returned %d: session expired", resp.StatusCode)}
	}
	if resp.StatusCode >= 400 {
		return Validation{}, fmt.Errorf("/api/auth/session returned %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Validation{}, err
	}
	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		return Validation{}, fmt.Errorf("auth session response was not JSON")
	}
	token, _ := data["accessToken"].(string)
	if token == "" {
		// A logged-out session returns {}.
		return Validation{}, &AuthError{Msg: "auth session response had no accessToken: session expired"}
	}
	result := Validation{SignedInAs: "unknown account"}
	if user, ok := data["user"].(map[string]any); ok {
		if email, _ := user["email"].(string); email != "" {
			result.SignedInAs = email
		} else if name, _ := user["name"].(string); name != "" {
			result.SignedInAs = name
		}
	}
	if exp, ok := JWTExpiry(token); ok {
		if !exp.After(now()) {
			// /api/auth/session returns a cached, already-expired access token
			// when the browser session can no longer refresh it.
			return Validation{}, &AuthError{Msg: "auth session accessToken is expired: session expired"}
		}
		result.AccessTokenExpiry = exp
	} else if expires, _ := data["expires"].(string); expires != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, expires); err == nil {
			result.AccessTokenExpiry = parsed
		}
	}
	return result, nil
}

// JWTExpiry reads `exp` out of a JWT without verifying it. Opaque tokens and
// any decode failure yield false.
func JWTExpiry(token string) (time.Time, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return time.Time{}, false
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}, false
	}
	exp, ok := claims["exp"].(float64)
	if !ok {
		return time.Time{}, false
	}
	return time.Unix(int64(exp), 0).UTC(), true
}

// TokenExpiryWarning says the published session is about to lapse, or
// already has; "" when there is plenty of life left or the expiry is unknown
// (never invent an alarm from a missing fact).
func TokenExpiryWarning(expiry, now time.Time) string {
	if expiry.IsZero() {
		return ""
	}
	remaining := expiry.Sub(now)
	if remaining > TokenWarnWithin {
		return ""
	}
	action := "Sign out and back into chatgpt.com in the capture browser, then run `pdw chatgpt publish-session` - a token is only minted at sign-in and lives 10 days."
	if remaining <= 0 {
		return fmt.Sprintf("ChatGPT session expired %s ago. %s", humanDuration(-remaining), action)
	}
	return fmt.Sprintf("ChatGPT session expires in %s. %s", humanDuration(remaining), action)
}

func humanDuration(d time.Duration) string {
	days := d.Hours() / 24
	if days >= 1 {
		return fmt.Sprintf("%.1f days", days)
	}
	return fmt.Sprintf("%.1f hours", d.Hours())
}
