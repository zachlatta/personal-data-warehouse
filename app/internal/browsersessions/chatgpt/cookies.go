// Package chatgpt captures the chatgpt.com web session from a local
// Chrome-family browser and publishes it to the warehouse
// (`pdw chatgpt publish-session`), the port of
// personal_data_warehouse.chatgpt_cookies / chatgpt_setup and
// personal_data_warehouse_chatgpt.cli.
//
// The ChatGPT desktop app keeps its data behind an entitlement-locked
// keychain group, so the warehouse polls ChatGPT's backend server-side with a
// web session captured from a browser instead. Nothing here logs a cookie.
package chatgpt

import (
	"fmt"
	"sort"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
)

// SessionTokenPrefix names the NextAuth session cookie (and its chunks).
const SessionTokenPrefix = "__Secure-next-auth.session-token"

// CookieError means no readable, logged-in chatgpt.com session was found.
type CookieError struct{ Msg string }

func (e *CookieError) Error() string { return e.Msg }

func cookieErrorf(format string, args ...any) error {
	return &CookieError{Msg: fmt.Sprintf(format, args...)}
}

// CapturedSession is a browser's chatgpt.com cookie jar as one header.
type CapturedSession struct {
	Browser         string // display name
	CookieHeader    string
	CookieCount     int
	HasSessionToken bool
}

// CookieHeader de-dupes by name (later wins) and puts the session-token
// chunks first for readability.
func CookieHeader(cookies []chromium.Cookie) string {
	byName := map[string]string{}
	for _, c := range cookies {
		byName[c.Name] = c.Value
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		ti, tj := strings.HasPrefix(names[i], SessionTokenPrefix), strings.HasPrefix(names[j], SessionTokenPrefix)
		if ti != tj {
			return ti
		}
		return names[i] < names[j]
	})
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+"="+byName[name])
	}
	return strings.Join(parts, "; ")
}

// Discover finds a logged-in chatgpt.com session and returns its full Cookie
// header. With browser set only that browser is tried; otherwise every
// Chrome-family browser in turn.
func Discover(host chromium.Host, browser string) (CapturedSession, error) {
	candidates := chromium.Browsers
	if browser != "" {
		profile, ok := chromium.BrowserByKey(browser)
		if !ok {
			return CapturedSession{}, cookieErrorf("unknown browser %q; valid: %s", browser, chromium.BrowserKeys())
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
			problems = append(problems, err.Error())
			continue
		}
		for _, db := range dbs {
			cookies, err := chromium.ReadCookies(db, key, "chatgpt.com", "openai.com")
			if err != nil {
				problems = append(problems, fmt.Sprintf("%s: could not read %s: %v", profile.DisplayName, db, err))
				continue
			}
			names := map[string]bool{}
			hasToken := false
			for _, c := range cookies {
				names[c.Name] = true
				if strings.HasPrefix(c.Name, SessionTokenPrefix) {
					hasToken = true
				}
			}
			if hasToken {
				return CapturedSession{
					Browser:         profile.DisplayName,
					CookieHeader:    CookieHeader(cookies),
					CookieCount:     len(names),
					HasSessionToken: true,
				}, nil
			}
		}
	}
	detail := "no Chrome-family browser with a chatgpt.com login was found"
	if len(problems) > 0 {
		detail = strings.Join(problems, "; ")
	}
	return CapturedSession{}, cookieErrorf("could not find a logged-in chatgpt.com session in a local browser. Open Chrome/Brave/Edge/Arc, log into chatgpt.com, then retry. %s", detail)
}
