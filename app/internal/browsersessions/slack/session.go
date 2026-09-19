// Package slack captures the Slack desktop app's client session and publishes
// it to the warehouse (`pdw slack publish-session`).
//
// Slack's public Web API cannot say which conversations changed; the client's
// own `client.counts` can, but only for a signed-in session. That session is
// two pieces that are useless apart: an `xoxc-` token kept in the app's
// localStorage (LevelDB) and the `d` cookie in its Chromium cookie store.
// This is the port of personal_data_warehouse.slack_session + slack_setup.
// Nothing here logs or prints a secret; reports carry a fingerprint instead.
package slack

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
)

const (
	// CookieHostSuffix selects slack.com cookies.
	CookieHostSuffix = "slack.com"
	// SessionCookie is the cookie that turns an xoxc token into a session.
	SessionCookie = "d"
	// DefaultAPIBaseURL is Slack's API origin.
	DefaultAPIBaseURL = "https://slack.com"
	userAgent         = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Slack_SSB/4.36.140"
)

// AppProfile is the Slack desktop app: an Electron Chromium profile, so the
// cookie store and the Safe Storage keychain entry have the browser shape.
var AppProfile = chromium.Profile{
	Key:                "slack-app",
	DisplayName:        "Slack",
	SupportSubdir:      "Slack",
	SafeStorageService: "Slack Safe Storage",
	SafeStorageAccount: "Slack",
	AppBundle:          "/Applications/Slack.app",
	HomebrewCask:       "slack",
}

// Deliberately narrow: `xoxc-` only (a user client token); `xoxb-`/`xoxp-`
// do not work with the client endpoints, and the length floor keeps
// truncated fragments out of the candidate list.
var tokenRE = regexp.MustCompile(`xoxc-[0-9a-zA-Z-]{10,}`)

// CaptureError means no usable logged-in Slack session was found.
type CaptureError struct{ Msg string }

func (e *CaptureError) Error() string { return e.Msg }

func captureErrorf(format string, args ...any) error {
	return &CaptureError{Msg: fmt.Sprintf(format, args...)}
}

// Session is a captured Slack client session.
type Session struct {
	Source          string
	Token           string
	CookieD         string
	TeamID          string
	EnterpriseID    string
	UserID          string
	TeamURL         string
	CookieExpiresAt time.Time // zero when unknown
}

// Fingerprint is a stable, non-secret identity for the credential.
func (s Session) Fingerprint() string {
	sum := sha256.Sum256([]byte(s.Token))
	return hex.EncodeToString(sum[:])
}

// CookieHeader is the Cookie header value the client endpoints need.
func (s Session) CookieHeader() string { return SessionCookie + "=" + s.CookieD }

// Redacted is the report-safe view: identity plus a token sha, no secret.
func (s Session) Redacted() map[string]any {
	var expires any
	if !s.CookieExpiresAt.IsZero() {
		expires = isoformat(s.CookieExpiresAt)
	}
	return map[string]any{
		"source":            s.Source,
		"team_id":           s.TeamID,
		"enterprise_id":     s.EnterpriseID,
		"user_id":           s.UserID,
		"team_url":          s.TeamURL,
		"cookie_expires_at": expires,
		"token_sha256":      s.Fingerprint(),
	}
}

// isoformat renders a UTC time the way Python's datetime.isoformat() does.
func isoformat(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond() == 0 {
		return t.Format("2006-01-02T15:04:05+00:00")
	}
	return t.Format("2006-01-02T15:04:05.000000+00:00")
}

func scanBytes(data []byte, found map[string]bool) {
	for _, m := range tokenRE.FindAll(data, -1) {
		found[string(m)] = true
	}
	// Chromium stores some localStorage values as UTF-16LE, where a byte-level
	// search for "xoxc-" finds nothing because every character is NUL-separated.
	// Both alignments are tried: LevelDB records carry a one-byte type prefix.
	for _, offset := range []int{0, 1} {
		if len(data) <= offset {
			continue
		}
		for _, m := range tokenRE.FindAllString(decodeUTF16LE(data[offset:]), -1) {
			found[m] = true
		}
	}
}

func decodeUTF16LE(data []byte) string {
	units := make([]uint16, 0, len(data)/2)
	for i := 0; i+1 < len(data); i += 2 {
		units = append(units, uint16(data[i])|uint16(data[i+1])<<8)
	}
	return string(utf16.Decode(units))
}

// ScanLocalStorageForTokens returns every xoxc- token in a Chromium
// localStorage tree, newest file first. Slack rewrites localConfig_v2 on each
// sign-in, so stale tokens from previous logins stay behind in older .ldb
// files; this only ORDERS candidates by mtime, and the caller decides by
// asking Slack.
func ScanLocalStorageForTokens(storeRoot string) []string {
	leveldb := filepath.Join(storeRoot, "Local Storage", "leveldb")
	entries, err := os.ReadDir(leveldb)
	if err != nil {
		return nil
	}
	type file struct {
		path  string
		mtime time.Time
	}
	var files []file
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, file{filepath.Join(leveldb, e.Name()), info.ModTime()})
	}
	sort.SliceStable(files, func(i, j int) bool { return files[i].mtime.After(files[j].mtime) })
	var ordered []string
	seen := map[string]bool{}
	for _, f := range files {
		data, err := os.ReadFile(f.path)
		if err != nil {
			continue
		}
		found := map[string]bool{}
		scanBytes(data, found)
		tokens := make([]string, 0, len(found))
		for tok := range found {
			tokens = append(tokens, tok)
		}
		sort.Strings(tokens)
		for _, tok := range tokens {
			if !seen[tok] {
				seen[tok] = true
				ordered = append(ordered, tok)
			}
		}
	}
	return ordered
}

// AuthTest is Slack's auth.test with a client session; it returns the raw
// payload (never raises for an API-level failure).
type AuthTest func(token, cookieHeader string) map[string]any

// Capture picks the token Slack accepts, given a store and its cookies.
func Capture(storeRoot string, cookies map[string]string, cookieExpiresAt time.Time, source string, authTest AuthTest) (Session, error) {
	cookieD := cookies[SessionCookie]
	if cookieD == "" {
		return Session{}, captureErrorf("found no `d` session cookie for slack.com; the xoxc token alone is not a session (sign in to Slack on this machine, then retry)")
	}
	candidates := ScanLocalStorageForTokens(storeRoot)
	if len(candidates) == 0 {
		return Session{}, captureErrorf("found no xoxc- token in %s/Local Storage", storeRoot)
	}
	header := SessionCookie + "=" + cookieD
	lastError := ""
	for _, token := range candidates {
		payload := authTest(token, header)
		if ok, _ := payload["ok"].(bool); ok {
			// On Enterprise Grid the client session authenticates against the
			// ORG, so auth.test returns an `E...` id where the app token returns
			// the workspace `T...` id every warehouse row is keyed by. Storing the
			// org id as team_id would fork the whole dataset, so the two are kept
			// apart and the caller resolves the workspace from base_slack.teams.
			reported := stringOf(payload["team_id"])
			isEnterprise := strings.HasPrefix(reported, "E")
			s := Session{
				Source:          source,
				Token:           token,
				CookieD:         cookieD,
				UserID:          stringOf(payload["user_id"]),
				TeamURL:         stringOf(payload["url"]),
				CookieExpiresAt: cookieExpiresAt,
			}
			if isEnterprise {
				s.EnterpriseID = reported
			} else {
				s.TeamID = reported
			}
			return s, nil
		}
		lastError = stringOf(payload["error"])
		if lastError == "" {
			lastError = "unknown_error"
		}
	}
	return Session{}, captureErrorf("found %d xoxc- token(s) but no working one (last error: %s); the stored session has probably been signed out", len(candidates), lastError)
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

// Client talks to Slack's client endpoints with a token + `d` cookie.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// NewClient returns a Slack client against the real API.
func NewClient() *Client {
	return &Client{BaseURL: DefaultAPIBaseURL, HTTP: &http.Client{Timeout: 30 * time.Second}}
}

// post POSTs to Slack with a client session. Both parts are required: the
// token alone returns `not_authed`, the cookie alone has nothing to
// authorise. The browser-shaped headers are not decoration.
func (c *Client) post(method, token, cookieHeader string, form map[string]string) map[string]any {
	values := url.Values{"token": {token}}
	for k, v := range form {
		values.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/api/"+method, strings.NewReader(values.Encode()))
	if err != nil {
		return map[string]any{"ok": false, "error": err.Error()}
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	req.Header.Set("Cookie", cookieHeader)
	req.Header.Set("User-Agent", userAgent)
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return map[string]any{"ok": false, "error": err.Error()}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return map[string]any{"ok": false, "error": fmt.Sprintf("http_%d", resp.StatusCode)}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return map[string]any{"ok": false, "error": err.Error()}
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return map[string]any{"ok": false, "error": err.Error()}
	}
	return payload
}

// AuthTest calls auth.test.
func (c *Client) AuthTest(token, cookieHeader string) map[string]any {
	return c.post("auth.test", token, cookieHeader, nil)
}

// ProbeClientCounts asks Slack, in ONE request, what has changed across every
// conversation, and returns a non-secret summary of what came back.
func (c *Client) ProbeClientCounts(s Session) map[string]any {
	payload := c.post("client.counts", s.Token, s.CookieHeader(), map[string]string{
		"thread_counts_by_channel": "true",
		"org_wide_aware":           "true",
	})
	if ok, _ := payload["ok"].(bool); !ok {
		errText := stringOf(payload["error"])
		if errText == "" {
			errText = "unknown_error"
		}
		return map[string]any{"ok": false, "error": errText}
	}
	summary := map[string]any{"ok": true}
	total, withLatest := 0, 0
	for _, bucket := range []string{"channels", "ims", "mpims"} {
		entries, _ := payload[bucket].([]any)
		summary[bucket] = len(entries)
		total += len(entries)
		for _, entry := range entries {
			if m, ok := entry.(map[string]any); ok && truthy(m["latest"]) {
				withLatest++
			}
		}
	}
	summary["total_conversations"] = total
	summary["with_latest_marker"] = withLatest
	return summary
}

func truthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case string:
		return t != ""
	case bool:
		return t
	case float64:
		return t != 0
	default:
		return true
	}
}

// Discover captures a Slack session from the Slack desktop app (the only
// source today). An explicit source that is not known is an error.
func Discover(host chromium.Host, source string, authTest AuthTest) (Session, error) {
	profiles := []chromium.Profile{AppProfile}
	if source != "" {
		var kept []chromium.Profile
		for _, p := range profiles {
			if p.Key == source {
				kept = append(kept, p)
			}
		}
		if len(kept) == 0 {
			return Session{}, captureErrorf("unknown Slack session source %q", source)
		}
		profiles = kept
	}
	var errs []string
	for _, profile := range profiles {
		key, err := host.SafeStorageKey(profile)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		for _, db := range host.CookieDBs(profile) {
			cookies, err := chromium.ReadCookies(db, key, CookieHostSuffix)
			if err != nil {
				errs = append(errs, fmt.Sprintf("%s: %v", profile.Key, err))
				continue
			}
			byName := map[string]string{}
			var expires time.Time
			for _, c := range cookies {
				byName[c.Name] = c.Value
				if c.Name == SessionCookie && expires.IsZero() {
					expires = c.Expires()
				}
			}
			if _, ok := byName[SessionCookie]; !ok {
				continue
			}
			return Capture(filepath.Dir(db), byName, expires, profile.Key, authTest)
		}
		errs = append(errs, fmt.Sprintf("%s: no slack.com `d` cookie in any cookie store", profile.Key))
	}
	if len(errs) == 0 {
		return Session{}, captureErrorf("no Slack session found")
	}
	return Session{}, captureErrorf("%s", strings.Join(errs, "; "))
}
