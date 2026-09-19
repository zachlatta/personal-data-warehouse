package hackernews_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium/chromiumtest"
	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/hackernews"
	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const secret = "s3cret-token"

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// multiHost builds an Application Support tree with one Cookies store per
// browser key, each holding the given news.ycombinator.com cookies.
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
			rows = append(rows, chromiumtest.Row{HostKey: "news.ycombinator.com", Name: name, Value: value})
		}
		chromiumtest.WriteCookieDB(t, filepath.Join(dir, "Cookies"), key, rows)
	}
	return chromium.Host{ApplicationSupport: support, KeychainPassword: func(string, string) (string, error) { return "pw", nil }}
}

func TestCaptureReadsTheUserCookieAndItsUsername(t *testing.T) {
	host := multiHost(t, map[string]map[string]string{"chrome": {"user": "zachlatta&" + secret, "other": "x"}})
	got, err := hackernews.Capture(host, "")
	if err != nil {
		t.Fatal(err)
	}
	if got.Browser != "Google Chrome" || got.UserID != "zachlatta" || got.CookieHeader != "user=zachlatta&"+secret {
		t.Fatalf("got %+v", got)
	}
}

func TestCaptureSkipsABrowserWithoutTheLogin(t *testing.T) {
	host := multiHost(t, map[string]map[string]string{
		"chrome": {"other": "x"},
		"brave":  {"user": "zachlatta&" + secret},
	})
	got, err := hackernews.Capture(host, "")
	if err != nil || got.Browser != "Brave" {
		t.Fatalf("got %+v err %v", got, err)
	}
	var cerr *hackernews.CaptureError
	if _, err := hackernews.Capture(host, "chrome"); !errors.As(err, &cerr) || !strings.Contains(err.Error(), "news.ycombinator.com") {
		t.Fatalf("err = %v", err)
	}
	if _, err := hackernews.Capture(host, "netscape"); err == nil || !strings.Contains(err.Error(), "chrome") {
		t.Fatalf("err = %v", err)
	}
	host.KeychainPassword = func(string, string) (string, error) { return "", errors.New("nope") }
	if _, err := hackernews.Capture(host, "brave"); err == nil || !strings.Contains(err.Error(), "Brave: could not read") {
		t.Fatalf("err = %v", err)
	}
}

func TestTheRedactedReportNeverCarriesTheCookie(t *testing.T) {
	s := hackernews.Session{Browser: "Google Chrome", CookieHeader: "user=zachlatta&" + secret, UserID: "zachlatta"}
	blob, _ := json.Marshal(s.Redacted())
	if strings.Contains(string(blob), secret) || !strings.Contains(string(blob), "token_sha256") || !strings.Contains(string(blob), `"user_id":"zachlatta"`) {
		t.Fatalf("blob = %s", blob)
	}
	if hackernews.UserIDFromCookie(" zach&tok") != "zach" || hackernews.UserIDFromCookie("") != "" {
		t.Fatal("user id parse")
	}
}

func TestResolveAccount(t *testing.T) {
	if got := hackernews.ResolveAccount("flag", envOf(map[string]string{"HACKER_NEWS_ACCOUNT": "env"}), "cookie"); got != "flag" {
		t.Fatalf("got %q", got)
	}
	if got := hackernews.ResolveAccount("", envOf(map[string]string{"HACKER_NEWS_ACCOUNT": " env "}), "cookie"); got != "env" {
		t.Fatalf("got %q", got)
	}
	if got := hackernews.ResolveAccount("", envOf(nil), "cookie"); got != "cookie" {
		t.Fatalf("got %q", got)
	}
	if got := hackernews.ResolveAccount("", envOf(nil), ""); got != "" {
		t.Fatalf("got %q", got)
	}
}

func hnServer(t *testing.T, handler http.HandlerFunc) *hackernews.Validator {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &hackernews.Validator{BaseURL: srv.URL, HTTP: srv.Client()}
}

const loginForm = `<html><body><b>Please log in.</b><form method="post" action="login"><input name="acct"><input name="pw" type="password"></form></body></html>`

func TestCheckLogin(t *testing.T) {
	t.Run("a list page means the cookie is live", func(t *testing.T) {
		v := hnServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/upvoted" || r.URL.Query().Get("id") != "zachlatta" || r.Header.Get("Cookie") != "user=zachlatta&"+secret || !strings.Contains(r.Header.Get("User-Agent"), "personal-data-warehouse") {
				t.Errorf("request = %s %v", r.URL, r.Header)
			}
			io.WriteString(w, `<html><table><tr class="athing" id="1"></tr></table></html>`)
		})
		ok, err := v.CheckLogin("user=zachlatta&"+secret, "zachlatta")
		if err != nil || !ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
	})
	t.Run("the login form means the cookie is dead", func(t *testing.T) {
		v := hnServer(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, loginForm) })
		ok, err := v.CheckLogin("user=zachlatta&"+secret, "zachlatta")
		if err != nil || ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
	})
	t.Run("throttling and other statuses are errors, not verdicts", func(t *testing.T) {
		for _, status := range []int{429, 503, 500, 404} {
			v := hnServer(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) })
			if _, err := v.CheckLogin("user=x&y", "x"); err == nil || !strings.Contains(err.Error(), "HTTP") {
				t.Fatalf("status %d: err = %v", status, err)
			}
		}
	})
}

func TestLooksLikeLoginPage(t *testing.T) {
	if !hackernews.LooksLikeLoginPage(loginForm) || !hackernews.LooksLikeLoginPage(`<form method="post"> acct pw`) {
		t.Fatal("login form not recognised")
	}
	if hackernews.LooksLikeLoginPage(strings.Repeat("x", 3000)+"Please log in") || hackernews.LooksLikeLoginPage("<html>upvoted</html>") {
		t.Fatal("false positive")
	}
}

type harness struct {
	deps      hackernews.Deps
	published *ingestclient.HackerNewsSession
	checked   []string
}

func newHarness(session hackernews.Session, captureErr error, loginOK bool, checkErr error) *harness {
	h := &harness{}
	h.deps = hackernews.Deps{
		Capture: func(string) (hackernews.Session, error) { return session, captureErr },
		CheckLogin: func(cookie, user string) (bool, error) {
			h.checked = append(h.checked, cookie+"|"+user)
			return loginOK, checkErr
		},
		Publisher: func() (hackernews.Publisher, error) {
			return func(s ingestclient.HackerNewsSession) (map[string]any, error) {
				h.published = &s
				return map[string]any{"token_sha256": "abc"}, nil
			}, nil
		},
	}
	return h
}

func run(t *testing.T, h *harness, env map[string]string, args ...string) (int, map[string]any, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := hackernews.RunWith(args, &stdout, &stderr, envOf(env), h.deps)
	if strings.Contains(stdout.String(), secret) || strings.Contains(stderr.String(), secret) {
		t.Fatalf("cookie leaked: %s %s", stdout.String(), stderr.String())
	}
	var report map[string]any
	if stdout.Len() > 0 {
		if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
			t.Fatalf("stdout is not JSON: %s", stdout.String())
		}
	}
	return code, report, stderr.String()
}

var live = hackernews.Session{Browser: "Google Chrome", CookieHeader: "user=zachlatta&" + secret, UserID: "zachlatta"}

func TestRunPublishesUnderTheCookiesOwnUsername(t *testing.T) {
	h := newHarness(live, nil, true, nil)
	code, report, errOut := run(t, h, nil, "publish-session")
	if code != 0 {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	if report["published"] != true || report["validated"] != true || report["account"] != "zachlatta" || report["user_id"] != "zachlatta" || report["token_sha256"] != live.Fingerprint() {
		t.Fatalf("report = %v", report)
	}
	if ack, _ := report["acknowledgement"].(map[string]any); ack["token_sha256"] != "abc" {
		t.Fatalf("ack = %v", report["acknowledgement"])
	}
	if h.published == nil || h.published.Account != "zachlatta" || h.published.SessionToken != live.CookieHeader || h.published.SessionKey != "default" || h.published.SourceBrowser != "Google Chrome" {
		t.Fatalf("published = %+v", h.published)
	}
	if len(h.checked) != 1 || h.checked[0] != live.CookieHeader+"|zachlatta" {
		t.Fatalf("checked = %v", h.checked)
	}
}

func TestRunDryRunValidatesAndNeverPublishes(t *testing.T) {
	h := newHarness(live, nil, true, nil)
	h.deps.Publisher = func() (hackernews.Publisher, error) { t.Fatal("must not publish"); return nil, nil }
	code, report, _ := run(t, h, nil, "publish-session", "--dry-run")
	if code != 0 || report["published"] != false || report["validated"] != true || report["account"] != "zachlatta" {
		t.Fatalf("code = %d report = %v", code, report)
	}
}

func TestRunADeadCookieIsRefusedBeforePublishing(t *testing.T) {
	h := newHarness(live, nil, false, nil)
	code, _, errOut := run(t, h, nil, "publish-session")
	if code != 1 || !strings.Contains(errOut, "login page") || h.published != nil {
		t.Fatalf("code = %d stderr = %s published = %v", code, errOut, h.published)
	}
	h = newHarness(live, nil, false, errors.New("HTTP 429"))
	if code, _, errOut := run(t, h, nil, "publish-session"); code != 1 || !strings.Contains(errOut, "could not check") || !strings.Contains(errOut, "429") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
}

func TestRunSkipCheckPublishesWithoutValidating(t *testing.T) {
	h := newHarness(live, nil, false, errors.New("must not be called"))
	code, report, _ := run(t, h, nil, "publish-session", "--skip-check", "--session-key", "alt")
	if code != 0 || report["validated"] != nil || len(h.checked) != 0 || h.published.SessionKey != "alt" {
		t.Fatalf("code = %d report = %v checked = %v", code, report, h.checked)
	}
}

func TestRunRefusesAnotherUsersCookieUnderThisAccount(t *testing.T) {
	other := live
	other.UserID = "someoneelse"
	other.CookieHeader = "user=someoneelse&" + secret
	h := newHarness(other, nil, true, nil)
	if code, _, errOut := run(t, h, nil, "publish-session", "--account", "zachlatta", "--skip-check"); code != 2 || !strings.Contains(errOut, "someoneelse") || h.published != nil {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	h = newHarness(other, nil, true, nil)
	if code, _, errOut := run(t, h, map[string]string{"HACKER_NEWS_ACCOUNT": "zachlatta"}, "publish-session"); code != 2 || !strings.Contains(errOut, "refusing") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
}

func TestRunMissingAccount(t *testing.T) {
	noUser := hackernews.Session{Browser: "Google Chrome", CookieHeader: "user=&" + secret}
	h := newHarness(noUser, nil, true, nil)
	if code, _, errOut := run(t, h, nil, "publish-session"); code != 1 || !strings.Contains(errOut, "pass --account") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
}

func TestRunExitCodes(t *testing.T) {
	h := newHarness(hackernews.Session{}, &hackernews.CaptureError{Msg: "could not find a logged-in news.ycombinator.com session"}, true, nil)
	if code, _, errOut := run(t, h, nil, "publish-session"); code != 2 || !strings.Contains(errOut, "hn publish-session: could not find") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	h = newHarness(live, nil, true, nil)
	h.deps.Publisher = func() (hackernews.Publisher, error) { return nil, errors.New("PDW_API_URL must be set") }
	if code, _, errOut := run(t, h, nil, "publish-session"); code != 1 || !strings.Contains(errOut, "PDW_API_URL") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	h = newHarness(live, nil, true, nil)
	h.deps.Publisher = func() (hackernews.Publisher, error) {
		return func(ingestclient.HackerNewsSession) (map[string]any, error) { return nil, errors.New("HTTP 403") }, nil
	}
	if code, _, errOut := run(t, h, nil, "publish-session"); code != 1 || !strings.Contains(errOut, "publish failed") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
}

func TestRunVerbDispatch(t *testing.T) {
	h := newHarness(live, nil, true, nil)
	if code, _, errOut := run(t, h, nil); code != 2 || !strings.Contains(errOut, "usage: pdw hn publish-session") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	var helpOut, helpErr bytes.Buffer
	if code := hackernews.RunWith([]string{"--help"}, &helpOut, &helpErr, envOf(nil), h.deps); code != 0 || !strings.Contains(helpOut.String(), "usage:") || helpErr.Len() != 0 {
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

// TestRunPublishesThroughTheSignedEndpoint drives the real ingest client at
// an httptest warehouse: the payload shape and endpoint are what
// app/internal/hackernewssession expects, and the cookie travels only in the
// signed body.
func TestRunPublishesThroughTheSignedEndpoint(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &gotBody)
		if r.URL.Query().Get("sig") == "" || r.URL.Query().Get("content_sha256") == "" {
			t.Errorf("unsigned upload: %s", r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"token_sha256":"abc","account":"zachlatta"}`)
	}))
	defer srv.Close()

	env := envOf(map[string]string{"PDW_INGEST_PROJECT_DIR": t.TempDir()})
	cfg := ingestclient.Config{BaseURL: srv.URL, Token: "warehouse-token"}
	var stderr bytes.Buffer
	ic, err := ingestclient.FromEnv(env, cfg, common.NewWriterLogger(&stderr))
	if err != nil {
		t.Fatal(err)
	}
	deps := hackernews.Deps{
		Capture:    func(string) (hackernews.Session, error) { return live, nil },
		CheckLogin: func(string, string) (bool, error) { return true, nil },
		Publisher:  func() (hackernews.Publisher, error) { return ic.PublishHackerNewsSession, nil },
	}
	var stdout bytes.Buffer
	if code := hackernews.RunWith([]string{"publish-session"}, &stdout, &stderr, env, deps); code != 0 {
		t.Fatalf("code = %d stderr = %s", code, stderr.String())
	}
	if gotPath != "/ingest/hacker-news/session" {
		t.Fatalf("path = %s", gotPath)
	}
	if gotBody["account"] != "zachlatta" || gotBody["session_token"] != live.CookieHeader || gotBody["session_key"] != "default" || gotBody["source_browser"] != "Google Chrome" {
		t.Fatalf("body = %v", gotBody)
	}
	if strings.Contains(stdout.String(), secret) || strings.Contains(stderr.String(), secret) {
		t.Fatalf("cookie leaked: %s %s", stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), `"published": true`) {
		t.Fatalf("stdout = %s", stdout.String())
	}
}
