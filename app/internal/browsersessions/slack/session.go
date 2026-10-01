// Package slack publishes a Slack web session that Zach copies by hand from a
// browser (`pdw slack publish-session`).
//
// Reviewed Slack writes (sending as Zach, marking read) need a signed-in
// session: two pieces that are useless apart, an `xoxc-` token kept in the web
// client's localStorage and the HttpOnly `d` cookie. (The sync also spent it on
// `client.counts` until Slack refused that with team_is_restricted on
// 2026-10-01; it now polls with OAuth tokens.)
//
// Until 2026-09-29 this package read both out of the Slack DESKTOP app on
// crobat every hour and called auth.test + client.counts with them to pick the
// right workspace. Slack's anomaly detection flagged every one of those runs as
// `unexpected_scraping` ("Go-based tool": a Slack desktop User-Agent over Go's
// TLS fingerprint, from the desktop's own IP, on the desktop's own session) and
// reset every session Zach had -- four sign-outs on 09-29 alone, each to the
// second of an hourly run. So the session is now pasted from a browser once,
// and nothing in this package talks to Slack: identity comes from the same
// localStorage the token does. Nothing here logs or prints a secret; reports
// carry a fingerprint instead.
package slack

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strings"
)

const (
	// SessionCookie is the cookie that turns an xoxc token into a session.
	SessionCookie = "d"
	// SourceWeb marks a session pasted from the Slack web client.
	SourceWeb = "slack-web"
)

// ConsoleSnippet is pasted into the browser's DevTools console on
// app.slack.com. It copies one JSON line to the clipboard: every signed-in
// team's token and identity from localConfig_v2 (the key the Slack client
// keeps them under; verified against crobat's client on 2026-09-29), plus the
// browser's own User-Agent, which the warehouse then sends with the session so
// the requests at least name the browser that minted it.
const ConsoleSnippet = `copy(JSON.stringify({user_agent:navigator.userAgent,teams:Object.values(JSON.parse(localStorage.localConfig_v2).teams).map(t=>({id:t.id,name:t.name,url:t.url,user_id:t.user_id,enterprise_id:t.enterprise_id||"",token:t.token}))}))`

// Deliberately narrow: `xoxc-` only (a user client token); `xoxb-`/`xoxp-`
// do not work with the client endpoints, and the length floor keeps
// truncated fragments out.
var tokenRE = regexp.MustCompile(`xoxc-[0-9a-zA-Z-]{10,}`)

// The `d` cookie value. DevTools shows it URL-encoded by default and decoded
// ('/', '+', '=') when "Show URL-decoded" is ticked; both are accepted.
var cookieRE = regexp.MustCompile(`xoxd-[0-9A-Za-z%/+=_.-]+`)

// CaptureError means the paste did not hold a usable session.
type CaptureError struct{ Msg string }

func (e *CaptureError) Error() string { return e.Msg }

func captureErrorf(format string, args ...any) error {
	return &CaptureError{Msg: fmt.Sprintf(format, args...)}
}

// Session is one team's Slack client session from the paste.
type Session struct {
	Source       string
	Token        string
	CookieD      string
	TeamID       string
	EnterpriseID string
	UserID       string
	TeamURL      string
	TeamName     string
	UserAgent    string
}

// Fingerprint is a stable, non-secret identity for the credential.
func (s Session) Fingerprint() string {
	sum := sha256.Sum256([]byte(s.Token))
	return hex.EncodeToString(sum[:])
}

// Redacted is the report-safe view: identity plus a token sha, no secret.
func (s Session) Redacted() map[string]any {
	return map[string]any{
		"source":        s.Source,
		"team_id":       s.TeamID,
		"enterprise_id": s.EnterpriseID,
		"user_id":       s.UserID,
		"team_url":      s.TeamURL,
		"team_name":     s.TeamName,
		"token_sha256":  s.Fingerprint(),
	}
}

// Paste is everything recognised in what Zach pasted.
type Paste struct {
	Teams      []Session
	BareTokens []string // xoxc tokens found outside the snippet's JSON
	CookieD    string
	UserAgent  string
}

func (p Paste) hasToken() bool { return len(p.Teams) > 0 || len(p.BareTokens) > 0 }

// Complete reports whether both halves of a session are present.
func (p Paste) Complete() bool { return p.hasToken() && p.CookieD != "" }

type pastedTeam struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	URL          string `json:"url"`
	UserID       string `json:"user_id"`
	EnterpriseID string `json:"enterprise_id"`
	Token        string `json:"token"`
}

type pastedConfig struct {
	UserAgent string          `json:"user_agent"`
	Teams     json.RawMessage `json:"teams"`
}

// ParsePaste reads the snippet's JSON (or a whole localConfig_v2 value, whose
// teams are keyed by id), any bare xoxc token, and the `d` cookie, in any
// order.
func ParsePaste(text string) (Paste, error) {
	var paste Paste
	seenTokens := map[string]bool{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "{") {
			var cfg pastedConfig
			if err := json.Unmarshal([]byte(line), &cfg); err == nil && len(cfg.Teams) > 0 {
				teams, err := decodeTeams(cfg.Teams)
				if err != nil {
					return Paste{}, captureErrorf("could not read the teams in the pasted JSON: %v", err)
				}
				if cfg.UserAgent != "" {
					paste.UserAgent = cfg.UserAgent
				}
				for _, t := range teams {
					if t.Token == "" || seenTokens[t.Token] {
						continue
					}
					seenTokens[t.Token] = true
					paste.Teams = append(paste.Teams, sessionFromTeam(t))
				}
				continue
			}
		}
		for _, tok := range tokenRE.FindAllString(line, -1) {
			if !seenTokens[tok] {
				seenTokens[tok] = true
				paste.BareTokens = append(paste.BareTokens, tok)
			}
		}
		if m := cookieRE.FindString(line); m != "" {
			paste.CookieD = NormalizeCookie(m)
		}
	}
	for i := range paste.Teams {
		paste.Teams[i].CookieD = paste.CookieD
		paste.Teams[i].UserAgent = paste.UserAgent
	}
	return paste, nil
}

func decodeTeams(raw json.RawMessage) ([]pastedTeam, error) {
	var list []pastedTeam
	if err := json.Unmarshal(raw, &list); err == nil {
		return list, nil
	}
	var byID map[string]pastedTeam
	if err := json.Unmarshal(raw, &byID); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]pastedTeam, 0, len(ids))
	for _, id := range ids {
		t := byID[id]
		if t.ID == "" {
			t.ID = id
		}
		out = append(out, t)
	}
	return out, nil
}

// sessionFromTeam keeps an org entry's id out of team_id. On Enterprise Grid
// the client keeps one entry per workspace (a T id, with enterprise_id set) and
// one for the org itself (an E id); every warehouse row is keyed by the
// workspace, so storing an E id as team_id would fork the dataset silently.
func sessionFromTeam(t pastedTeam) Session {
	s := Session{
		Source:   SourceWeb,
		Token:    t.Token,
		UserID:   t.UserID,
		TeamURL:  t.URL,
		TeamName: t.Name,
	}
	if strings.HasPrefix(t.ID, "E") {
		s.EnterpriseID = t.ID
	} else {
		s.TeamID = t.ID
		s.EnterpriseID = t.EnterpriseID
	}
	return s
}

// NormalizeCookie returns the `d` value as the browser sends it: URL-encoded,
// without a `d=` prefix or a trailing separator.
func NormalizeCookie(raw string) string {
	v := strings.TrimSpace(raw)
	v = strings.TrimPrefix(v, SessionCookie+"=")
	v = strings.Trim(v, `"';`)
	if strings.ContainsAny(v, "/+=") {
		v = url.QueryEscape(v)
	}
	return v
}

// ReadPaste reads lines until both halves of a session have arrived, so a
// terminal user never has to know to press Ctrl-D, and returns an error naming
// whatever is still missing at EOF.
func ReadPaste(r io.Reader) (Paste, error) {
	reader := bufio.NewReaderSize(r, 1<<20)
	var text strings.Builder
	for {
		line, err := reader.ReadString('\n')
		text.WriteString(line)
		if line != "" {
			paste, perr := ParsePaste(text.String())
			if perr != nil {
				return Paste{}, perr
			}
			if paste.Complete() {
				return paste, nil
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return Paste{}, captureErrorf("could not read the paste: %v", err)
		}
	}
	paste, err := ParsePaste(text.String())
	if err != nil {
		return Paste{}, err
	}
	switch {
	case !paste.hasToken() && paste.CookieD == "":
		return Paste{}, captureErrorf("nothing was pasted: expected the console snippet's JSON and the \"d\" cookie (xoxd-...)")
	case !paste.hasToken():
		return Paste{}, captureErrorf("found the \"d\" cookie but no xoxc token: paste the JSON the console snippet copied")
	case paste.CookieD == "":
		return Paste{}, captureErrorf("found the token but no \"d\" cookie: copy the value of the cookie named d (it starts with xoxd-) from DevTools > Application > Cookies > https://app.slack.com")
	}
	return paste, nil
}

func stringOf(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		return fmt.Sprint(t)
	}
}
