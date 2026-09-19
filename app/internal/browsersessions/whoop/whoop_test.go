package whoop_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium/chromiumtest"
	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/whoop"
	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
)

var now = time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func jwtExpiringAt(at time.Time) string {
	claims, _ := json.Marshal(map[string]any{"exp": at.Unix()})
	return "header." + base64.RawURLEncoding.EncodeToString(claims) + ".signature"
}

// multiHost builds an Application Support tree with one Cookies store per
// browser key, each holding the given whoop.com cookies.
func multiHost(t *testing.T, cookiesByBrowser map[string]map[string]string) chromium.Host {
	t.Helper()
	key := chromiumtest.Key("pw")
	support := t.TempDir()
	for browserKey, cookies := range cookiesByBrowser {
		profile, _ := chromium.BrowserByKey(browserKey)
		dir := filepath.Join(support, profile.SupportSubdir, "Default")
		os.MkdirAll(dir, 0o755)
		var rows []chromiumtest.Row
		for name, value := range cookies {
			rows = append(rows, chromiumtest.Row{HostKey: ".whoop.com", Name: name, Value: value})
		}
		chromiumtest.WriteCookieDB(t, filepath.Join(dir, "Cookies"), key, rows)
	}
	return chromium.Host{ApplicationSupport: support, KeychainPassword: func(string, string) (string, error) { return "pw", nil }}
}

func TestCapturesBothTokensAndReadsTheJWTExpiry(t *testing.T) {
	expires := now.Add(24 * time.Hour)
	host := multiHost(t, map[string]map[string]string{"chrome": {whoop.AccessTokenCookie: jwtExpiringAt(expires), whoop.RefreshTokenCookie: "refresh"}})
	got, err := whoop.Capture(host, "", now)
	if err != nil {
		t.Fatal(err)
	}
	if got.RefreshToken != "refresh" || !got.AccessExpiresAt.Equal(expires) || !got.RefreshExpiresAt.Equal(now.Add(30*24*time.Hour)) || got.Browser != "Google Chrome" {
		t.Fatalf("got %+v", got)
	}
}

func TestAMalformedAccessTokenFallsBackTo24h(t *testing.T) {
	if got := whoop.AccessTokenExpiry("not-a-jwt", now); !got.Equal(now.Add(24 * time.Hour)) {
		t.Fatalf("got %v", got)
	}
}

func TestCaptureSkipsABrowserHoldingOnlyHalfASession(t *testing.T) {
	host := multiHost(t, map[string]map[string]string{
		"chrome": {whoop.AccessTokenCookie: jwtExpiringAt(now)},
		"brave":  {whoop.AccessTokenCookie: jwtExpiringAt(now), whoop.RefreshTokenCookie: "refresh"},
	})
	got, err := whoop.Capture(host, "", now)
	if err != nil || got.Browser != "Brave" {
		t.Fatalf("got %+v err %v", got, err)
	}
	_, err = whoop.Capture(host, "chrome", now)
	if err == nil || !strings.Contains(err.Error(), "partial whoop.com session") {
		t.Fatalf("err = %v", err)
	}
}

func TestNoSessionAnywhereSaysWhatToDo(t *testing.T) {
	host := multiHost(t, map[string]map[string]string{"chrome": {}})
	_, err := whoop.Capture(host, "", now)
	var cerr *whoop.CaptureError
	if !errors.As(err, &cerr) || !strings.Contains(err.Error(), "app.whoop.com") {
		t.Fatalf("err = %v", err)
	}
	if _, err := whoop.Capture(host, "netscape", now); err == nil || !strings.Contains(err.Error(), "chrome") {
		t.Fatalf("err = %v", err)
	}
	host.KeychainPassword = func(string, string) (string, error) { return "", errors.New("nope") }
	if _, err := whoop.Capture(host, "chrome", now); err == nil || !strings.Contains(err.Error(), "Google Chrome: could not read") {
		t.Fatalf("err = %v", err)
	}
}

func TestTheRedactedReportNeverCarriesAToken(t *testing.T) {
	s := whoop.Session{Browser: "Google Chrome", AccessToken: jwtExpiringAt(now), RefreshToken: "super-secret-refresh", AccessExpiresAt: now, RefreshExpiresAt: now}
	blob, _ := json.Marshal(s.Redacted())
	if strings.Contains(string(blob), "super-secret-refresh") || !strings.Contains(string(blob), "refresh_token_sha256") {
		t.Fatalf("blob = %s", blob)
	}
	if s.Fingerprint() == (whoop.Session{RefreshToken: "b"}).Fingerprint() {
		t.Fatal("fingerprint must track the refresh token")
	}
	if s.Redacted()["access_expires_at"] != "2026-08-23T12:00:00+00:00" {
		t.Fatalf("isoformat = %v", s.Redacted()["access_expires_at"])
	}
}

func TestResolveAccount(t *testing.T) {
	if got := whoop.ResolveAccount("", envOf(map[string]string{"WHOOP_ACCOUNT": "w", "GMAIL_ACCOUNTS": "g"})); got != "w" {
		t.Fatalf("got %q", got)
	}
	if got := whoop.ResolveAccount("", envOf(map[string]string{"GMAIL_ACCOUNTS": " g@x.com , h"})); got != "g@x.com" {
		t.Fatalf("got %q", got)
	}
	if got := whoop.ResolveAccount("explicit", envOf(map[string]string{"WHOOP_ACCOUNT": "w"})); got != "explicit" {
		t.Fatalf("got %q", got)
	}
	if got := whoop.ResolveAccount("", envOf(nil)); got != "" {
		t.Fatalf("got %q", got)
	}
}

type harness struct {
	deps      whoop.Deps
	published *ingestclient.WhoopSession
}

func newHarness(session whoop.Session, captureErr error) *harness {
	h := &harness{}
	h.deps = whoop.Deps{
		Capture: func(browser string, at time.Time) (whoop.Session, error) { return session, captureErr },
		Publisher: func() (whoop.Publisher, error) {
			return func(s ingestclient.WhoopSession) (map[string]any, error) {
				h.published = &s
				return map[string]any{"refresh_token_sha256": "abc"}, nil
			}, nil
		},
		Now: func() time.Time { return now },
	}
	return h
}

func run(t *testing.T, h *harness, env map[string]string, args ...string) (int, map[string]any, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := whoop.RunWith(args, &stdout, &stderr, envOf(env), h.deps)
	if strings.Contains(stdout.String(), "secret") || strings.Contains(stderr.String(), "secret") {
		t.Fatalf("token leaked: %s %s", stdout.String(), stderr.String())
	}
	var report map[string]any
	if stdout.Len() > 0 {
		if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
			t.Fatalf("stdout is not JSON: %s", stdout.String())
		}
	}
	return code, report, stderr.String()
}

var live = whoop.Session{Browser: "Google Chrome", AccessToken: "access-secret", RefreshToken: "refresh-secret",
	AccessExpiresAt: now.Add(12 * time.Hour), RefreshExpiresAt: now.Add(30 * 24 * time.Hour)}

func TestRunPublishes(t *testing.T) {
	h := newHarness(live, nil)
	code, report, errOut := run(t, h, map[string]string{"WHOOP_ACCOUNT": "zach@example.com"}, "publish-session")
	if code != 0 {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	if report["published"] != true || report["account"] != "zach@example.com" || report["access_token_valid_for_hours"] != float64(12) || report["note"] != nil {
		t.Fatalf("report = %v", report)
	}
	if h.published == nil || h.published.AccessToken != "access-secret" || h.published.RefreshToken != "refresh-secret" ||
		h.published.SessionKey != "default" || h.published.SourceBrowser != "Google Chrome" ||
		h.published.AccessExpiresAt != "2026-08-24T00:00:00+00:00" || h.published.RefreshExpiresAt != "2026-09-22T12:00:00+00:00" {
		t.Fatalf("published = %+v", h.published)
	}
}

func TestRunDryRunReportsWithoutPublishing(t *testing.T) {
	expired := live
	expired.AccessExpiresAt = now.Add(-time.Hour)
	h := newHarness(expired, nil)
	h.deps.Publisher = func() (whoop.Publisher, error) { t.Fatal("must not publish"); return nil, nil }
	code, report, _ := run(t, h, nil, "publish-session", "--dry-run")
	if code != 0 || report["published"] != false || report["access_token_valid_for_hours"] != float64(-1) || !strings.Contains(report["note"].(string), "already expired") {
		t.Fatalf("code = %d report = %v", code, report)
	}
}

func TestRunExitCodes(t *testing.T) {
	h := newHarness(whoop.Session{}, &whoop.CaptureError{Msg: "could not find a logged-in app.whoop.com session"})
	if code, _, errOut := run(t, h, nil, "publish-session"); code != 2 || !strings.Contains(errOut, "whoop publish-session: could not find") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	h = newHarness(live, nil)
	if code, _, errOut := run(t, h, nil, "publish-session"); code != 1 || !strings.Contains(errOut, "pass --account") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	h = newHarness(live, nil)
	h.deps.Publisher = func() (whoop.Publisher, error) { return nil, errors.New("PDW_API_URL must be set") }
	if code, _, errOut := run(t, h, nil, "publish-session", "--account", "a"); code != 1 || !strings.Contains(errOut, "PDW_API_URL") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	h = newHarness(live, nil)
	code, _, _ := run(t, h, nil, "publish-session", "--account", "a", "--session-key", "alt", "--browser", "brave")
	if code != 0 || h.published.Account != "a" || h.published.SessionKey != "alt" {
		t.Fatalf("code = %d published = %+v", code, h.published)
	}
}

func TestRunVerbDispatch(t *testing.T) {
	h := newHarness(live, nil)
	if code, _, errOut := run(t, h, nil); code != 2 || !strings.Contains(errOut, "usage: pdw whoop publish-session") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	var helpOut, helpErr bytes.Buffer
	if code := whoop.RunWith([]string{"--help"}, &helpOut, &helpErr, envOf(nil), h.deps); code != 0 || !strings.Contains(helpOut.String(), "usage:") || helpErr.Len() != 0 {
		t.Fatalf("code = %d stdout = %q stderr = %q", code, helpOut.String(), helpErr.String())
	}
	if code, _, errOut := run(t, h, nil, "nope"); code != 2 || !strings.Contains(errOut, "unknown command") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	if code, _, _ := run(t, h, nil, "publish-session", "--bogus"); code != 2 {
		t.Fatalf("code = %d", code)
	}
	if code, _, errOut := run(t, h, nil, "publish-session", "extra"); code != 2 || !strings.Contains(errOut, "unexpected argument") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
}

func TestReportRendersHoursAsAPythonFloat(t *testing.T) {
	// The Python report printed round(hours, 1), so a whole number of hours
	// read "12.0"; encoding/json would print "12", which is a different byte
	// stream for anything diffing the two run logs.
	h := newHarness(live, nil)
	var stdout, stderr bytes.Buffer
	if code := whoop.RunWith([]string{"publish-session", "--dry-run"}, &stdout, &stderr, envOf(nil), h.deps); code != 0 {
		t.Fatalf("code = %d stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"access_token_valid_for_hours": 12.0`) {
		t.Fatalf("stdout = %s", stdout.String())
	}
}
