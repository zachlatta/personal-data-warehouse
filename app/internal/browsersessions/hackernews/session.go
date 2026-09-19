// Package hackernews captures the news.ycombinator.com login from a local
// Chrome-family browser and publishes it to the warehouse
// (`pdw hn publish-session`).
//
// HN keeps its login in one cookie, `user` (`<username>&<token>`), on
// news.ycombinator.com. The lists HN shows only to the logged-in user
// (upvoted, hidden) need it; everything else the source reads is public. As
// with ChatGPT and WHOOP the credential is read out of the browser rather
// than obtained by logging in from a server. Nothing here returns the token
// in a report: callers get the username and a fingerprint, so a run log can
// prove which credential was published.
package hackernews

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
)

const (
	// CookieHostSuffix selects news.ycombinator.com cookies.
	CookieHostSuffix = "news.ycombinator.com"
	// LoginCookie is HN's single login cookie, `<username>&<token>`.
	LoginCookie = "user"
)

// CaptureError means no logged-in news.ycombinator.com session was found in
// a local browser.
type CaptureError struct{ Msg string }

func (e *CaptureError) Error() string { return e.Msg }

func captureErrorf(format string, args ...any) error {
	return &CaptureError{Msg: fmt.Sprintf(format, args...)}
}

// Session is a captured Hacker News login.
type Session struct {
	Browser string
	// CookieHeader is the full `Cookie:` header value the poller sends
	// (today: `user=...`).
	CookieHeader string
	// UserID is the username embedded in the cookie, so a publish can refuse
	// the wrong account.
	UserID string
}

// Fingerprint is the non-secret identity of the credential.
func (s Session) Fingerprint() string {
	sum := sha256.Sum256([]byte(s.CookieHeader))
	return hex.EncodeToString(sum[:])
}

// Redacted is the report-safe view.
func (s Session) Redacted() map[string]any {
	return map[string]any{
		"browser":      s.Browser,
		"user_id":      s.UserID,
		"token_sha256": s.Fingerprint(),
	}
}

// UserIDFromCookie is the username half of HN's `user` cookie (`name&token`).
func UserIDFromCookie(value string) string {
	name, _, _ := strings.Cut(value, "&")
	return strings.TrimSpace(name)
}

// Capture finds a logged-in news.ycombinator.com session in a local
// Chrome-family browser; browser forces one, "" tries each in turn.
func Capture(host chromium.Host, browser string) (Session, error) {
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
			value := cookies[LoginCookie]
			userID := UserIDFromCookie(value)
			if value != "" && userID != "" {
				return Session{
					Browser:      profile.DisplayName,
					CookieHeader: LoginCookie + "=" + value,
					UserID:       userID,
				}, nil
			}
		}
	}
	detail := "no Chrome-family browser held a news.ycombinator.com login"
	if len(problems) > 0 {
		detail = strings.Join(problems, "; ")
	}
	return Session{}, captureErrorf("could not find a logged-in news.ycombinator.com session in a local browser. Open Chrome, log in to news.ycombinator.com, then retry. %s", detail)
}
