// Package x reads Zach's own X (Twitter) account into the warehouse
// (`pdw ingest x`).
//
// X has no API a person can use for their own likes, bookmarks and mentions
// without a paid developer plan, but the x.com web client reads all of them
// from its GraphQL API with the browser's login. So the uploader runs on the
// Mac whose Chrome is signed in to x.com: it reads that browser's x.com
// cookies (the same Chromium cookie machinery the ChatGPT and Hacker News
// session publishers use), calls the read-only GraphQL queries the web client
// calls, and posts what it read to the app's /ingest/x/batch endpoint. The
// cookie never leaves the Mac, and every request comes from the network and
// browser session X already sees Zach use. Nothing here writes to X: every
// request is a GET of a query (no like, follow, post or mark-as-read).
package x

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
)

const (
	// CookieHostSuffix selects x.com cookies (host_key ".x.com"); the leading
	// dot keeps another site ending in "x.com" out.
	CookieHostSuffix = ".x.com"
	// AuthCookie is the login; CSRFCookie is echoed back as x-csrf-token.
	AuthCookie = "auth_token"
	CSRFCookie = "ct0"
	// UserCookie holds the signed-in account's numeric id ("u=<id>", URL-encoded).
	UserCookie = "twid"
)

// CaptureError means no signed-in x.com session was found in a local browser.
type CaptureError struct{ Msg string }

func (e *CaptureError) Error() string { return e.Msg }

// Session is a captured x.com login. Cookies holds every x.com cookie the
// browser sends, so requests look like the browser's own.
type Session struct {
	Browser string
	Cookies map[string]string
	// UserID is the signed-in account's numeric id, from the twid cookie.
	UserID string
}

// CookieHeader is the `Cookie:` header value, names sorted for determinism.
func (s Session) CookieHeader() string {
	names := make([]string, 0, len(s.Cookies))
	for name := range s.Cookies {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+"="+s.Cookies[name])
	}
	return strings.Join(parts, "; ")
}

// CSRFToken is the ct0 cookie the API wants repeated as x-csrf-token.
func (s Session) CSRFToken() string { return s.Cookies[CSRFCookie] }

// Fingerprint is the non-secret identity of the login, for run logs.
func (s Session) Fingerprint() string {
	sum := sha256.Sum256([]byte(s.Cookies[AuthCookie]))
	return hex.EncodeToString(sum[:])[:16]
}

// UserIDFromTwid decodes the twid cookie ("u%3D12345" or "u=12345").
func UserIDFromTwid(value string) string {
	decoded, err := url.QueryUnescape(strings.Trim(value, `"`))
	if err != nil {
		decoded = value
	}
	id, ok := strings.CutPrefix(strings.TrimSpace(decoded), "u=")
	if !ok || id == "" || strings.Trim(id, "0123456789") != "" {
		return ""
	}
	return id
}

// Capture finds a signed-in x.com session in a local Chrome-family browser;
// browser forces one, "" tries each in turn.
func Capture(host chromium.Host, browser string) (Session, error) {
	candidates := chromium.Browsers
	if browser != "" {
		profile, ok := chromium.BrowserByKey(browser)
		if !ok {
			return Session{}, &CaptureError{Msg: fmt.Sprintf("unknown browser %q; valid: %s", browser, chromium.BrowserKeys())}
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
			if cookies[AuthCookie] == "" || cookies[CSRFCookie] == "" {
				continue
			}
			return Session{Browser: profile.DisplayName, Cookies: cookies, UserID: UserIDFromTwid(cookies[UserCookie])}, nil
		}
	}
	detail := "no Chrome-family browser held an x.com login"
	if len(problems) > 0 {
		detail = strings.Join(problems, "; ")
	}
	return Session{}, &CaptureError{Msg: "could not find a signed-in x.com session in a local browser. Open Chrome, sign in to x.com, then retry. " + detail}
}
