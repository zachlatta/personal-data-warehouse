package chatgpt_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chatgpt"
	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium/chromiumtest"
)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func jwtExpiring(at time.Time) string {
	claims, _ := json.Marshal(map[string]any{"exp": at.Unix()})
	return "hdr." + base64.RawURLEncoding.EncodeToString(claims) + ".sig"
}

func TestCookieHeaderOrdersSessionTokenFirst(t *testing.T) {
	header := chatgpt.CookieHeader([]chromium.Cookie{
		{Name: "cf_clearance", Value: "cf"}, {Name: "__Secure-next-auth.session-token", Value: "tok"}, {Name: "oai-did", Value: "d"},
	})
	if !strings.HasPrefix(header, "__Secure-next-auth.session-token=tok") || !strings.Contains(header, "cf_clearance=cf") || !strings.Contains(header, "oai-did=d") {
		t.Fatalf("header = %q", header)
	}
}

func chromeHost(t *testing.T, key []byte, rows []chromiumtest.Row) chromium.Host {
	t.Helper()
	support := t.TempDir()
	profileDir := filepath.Join(support, "Google/Chrome/Default")
	os.MkdirAll(profileDir, 0o755)
	chromiumtest.WriteCookieDB(t, filepath.Join(profileDir, "Cookies"), key, rows)
	return chromium.Host{
		ApplicationSupport: support,
		KeychainPassword:   func(string, string) (string, error) { return "pw", nil },
		FileExists:         func(string) bool { return false },
	}
}

func TestDiscoverReadsSessionFromBrowser(t *testing.T) {
	key := chromiumtest.Key("pw")
	host := chromeHost(t, key, []chromiumtest.Row{
		{HostKey: "chatgpt.com", Name: "__Secure-next-auth.session-token", Value: "sess-tok"},
		{HostKey: "chatgpt.com", Name: "cf_clearance", Value: "cf-tok"},
		{HostKey: "example.com", Name: "irrelevant", Value: "nope"},
	})
	captured, err := chatgpt.Discover(host, "chrome")
	if err != nil {
		t.Fatal(err)
	}
	if captured.Browser != "Google Chrome" || !captured.HasSessionToken || captured.CookieCount != 2 {
		t.Fatalf("captured = %+v", captured)
	}
	if !strings.Contains(captured.CookieHeader, "__Secure-next-auth.session-token=sess-tok") || !strings.Contains(captured.CookieHeader, "cf_clearance=cf-tok") || strings.Contains(captured.CookieHeader, "irrelevant") {
		t.Fatalf("header = %q", captured.CookieHeader)
	}
	// Auto-detect walks every browser and finds the same one.
	if auto, err := chatgpt.Discover(host, ""); err != nil || auto.Browser != "Google Chrome" {
		t.Fatalf("auto = %+v, %v", auto, err)
	}
}

func TestDiscoverFailures(t *testing.T) {
	key := chromiumtest.Key("pw")
	host := chromeHost(t, key, []chromiumtest.Row{{HostKey: "chatgpt.com", Name: "cf_clearance", Value: "cf"}})
	_, err := chatgpt.Discover(host, "chrome")
	var cerr *chatgpt.CookieError
	if !errors.As(err, &cerr) || !strings.Contains(err.Error(), "log into chatgpt.com") {
		t.Fatalf("err = %v", err)
	}
	if _, err := chatgpt.Discover(host, "netscape"); err == nil || !strings.Contains(err.Error(), "unknown browser") || !strings.Contains(err.Error(), "chrome") {
		t.Fatalf("err = %v", err)
	}
	host.KeychainPassword = func(string, string) (string, error) { return "", errors.New("denied") }
	if _, err := chatgpt.Discover(host, "chrome"); err == nil || !strings.Contains(err.Error(), "Chrome Safe Storage") {
		t.Fatalf("keychain problem should be named: %v", err)
	}
}

func authServer(t *testing.T, handler http.HandlerFunc) *chatgpt.Validator {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &chatgpt.Validator{BaseURL: srv.URL, HTTP: srv.Client(), Now: func() time.Time { return time.Unix(1_800_000_000, 0) }}
}

func TestValidate(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	t.Run("signed in with a live jwt", func(t *testing.T) {
		v := authServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/auth/session" || r.Header.Get("Cookie") != "a=b" || !strings.Contains(r.Header.Get("User-Agent"), "Chrome/") {
				t.Errorf("request = %s %v", r.URL.Path, r.Header)
			}
			json.NewEncoder(w).Encode(map[string]any{"user": map[string]any{"email": "user@example.com"}, "accessToken": jwtExpiring(now.Add(5 * 24 * time.Hour))})
		})
		got, err := v.Validate("a=b")
		if err != nil || got.SignedInAs != "user@example.com" || got.Blocked || !got.AccessTokenExpiry.Equal(now.Add(5*24*time.Hour)) {
			t.Fatalf("got %+v, %v", got, err)
		}
	})
	t.Run("cloudflare challenge is blocked not rejected", func(t *testing.T) {
		v := authServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("cf-mitigated", "challenge")
			w.WriteHeader(403)
			fmt.Fprint(w, "<html>Just a moment...</html>")
		})
		got, err := v.Validate("a=b")
		if err != nil || !got.Blocked || !strings.Contains(got.BlockedReason, "Cloudflare") {
			t.Fatalf("got %+v, %v", got, err)
		}
	})
	rejected := func(name string, handler http.HandlerFunc, want string) {
		t.Run(name, func(t *testing.T) {
			_, err := authServer(t, handler).Validate("a=b")
			var authErr *chatgpt.AuthError
			if !errors.As(err, &authErr) || !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v", err)
			}
		})
	}
	rejected("plain 403 is an auth failure", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403) }, "returned 403")
	rejected("401 is an auth failure", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }, "returned 401")
	rejected("logged out returns {}", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "{}") }, "no accessToken")
	rejected("cached expired jwt", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"accessToken": jwtExpiring(now.Add(-time.Hour))})
	}, "accessToken is expired")
	t.Run("5xx is not an auth error", func(t *testing.T) {
		_, err := authServer(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(502) }).Validate("a=b")
		var authErr *chatgpt.AuthError
		if err == nil || errors.As(err, &authErr) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("opaque token uses the session expires", func(t *testing.T) {
		v := authServer(t, func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]any{"accessToken": "opaque", "expires": "2030-01-02T03:04:05.000Z", "user": map[string]any{"name": "Z"}})
		})
		got, err := v.Validate("a=b")
		if err != nil || got.SignedInAs != "Z" || got.AccessTokenExpiry.Year() != 2030 {
			t.Fatalf("got %+v, %v", got, err)
		}
	})
}

func TestTokenExpiryWarning(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	if w := chatgpt.TokenExpiryWarning(time.Time{}, now); w != "" {
		t.Fatalf("unknown expiry must not warn: %q", w)
	}
	if w := chatgpt.TokenExpiryWarning(now.Add(5*24*time.Hour), now); w != "" {
		t.Fatalf("plenty of life must not warn: %q", w)
	}
	if w := chatgpt.TokenExpiryWarning(now.Add(6*time.Hour), now); !strings.Contains(w, "expires in 6.0 hours") {
		t.Fatalf("w = %q", w)
	}
	if w := chatgpt.TokenExpiryWarning(now.Add(36*time.Hour), now); !strings.Contains(w, "expires in 1.5 days") {
		t.Fatalf("w = %q", w)
	}
	if w := chatgpt.TokenExpiryWarning(now.Add(-2*24*time.Hour), now); !strings.Contains(w, "expired 2.0 days ago") {
		t.Fatalf("w = %q", w)
	}
	if _, ok := chatgpt.JWTExpiry("not-a-jwt"); ok {
		t.Fatal("opaque token must not decode")
	}
}

// --- setup flow -------------------------------------------------------------

func fakeSetup(installed map[string]bool, discover func(string) (chatgpt.CapturedSession, error)) (*chatgpt.Setup, *[]string) {
	log := &[]string{}
	s := &chatgpt.Setup{
		Host: chromium.Host{FileExists: func(p string) bool { return installed[p] }},
		Run: func(argv []string) int {
			*log = append(*log, "run:"+strings.Join(argv, " "))
			installed["/Applications/Brave Browser.app"] = true
			return 0
		},
		Open:          func(target string) error { *log = append(*log, "open:"+target); return nil },
		Prompt:        func(string) error { *log = append(*log, "prompt"); return nil },
		Log:           func(m string) { *log = append(*log, "log:"+m) },
		Discover:      discover,
		BrewAvailable: func() bool { return true },
		Sleep:         func(time.Duration) {},
	}
	return s, log
}

func TestEnsureBrowser(t *testing.T) {
	s, log := fakeSetup(map[string]bool{"/Applications/Google Chrome.app": true}, nil)
	if p, err := s.EnsureBrowser("", false); err != nil || p.Key != "chrome" {
		t.Fatalf("p = %v err = %v", p, err)
	}
	if p, err := s.EnsureBrowser("chrome", false); err != nil || p.Key != "chrome" {
		t.Fatalf("p = %v err = %v", p, err)
	}
	if _, err := s.EnsureBrowser("brave", false); err == nil || !strings.Contains(err.Error(), "Brave is not installed") {
		t.Fatalf("err = %v", err)
	}
	if _, err := s.EnsureBrowser("netscape", true); err == nil || !strings.Contains(err.Error(), "unknown browser") {
		t.Fatalf("err = %v", err)
	}
	s, log = fakeSetup(map[string]bool{}, nil)
	if _, err := s.EnsureBrowser("", false); err == nil || !strings.Contains(err.Error(), "no Chrome-family browser is installed") {
		t.Fatalf("err = %v", err)
	}
	if len(*log) != 0 {
		t.Fatalf("no-install must not run anything: %v", *log)
	}
	p, err := s.EnsureBrowser("", true)
	if err != nil || p.Key != "brave" || !strings.Contains(strings.Join(*log, "\n"), "run:brew install --cask brave-browser") {
		t.Fatalf("p = %v err = %v log = %v", p, err, *log)
	}
	s.BrewAvailable = func() bool { return false }
	s.Host.FileExists = func(string) bool { return false }
	if _, err := s.EnsureBrowser("", true); err == nil || !strings.Contains(err.Error(), "Homebrew is required") {
		t.Fatalf("err = %v", err)
	}
}

func TestEnsureLoggedInOpensTheBrowserAndRetries(t *testing.T) {
	calls := 0
	s, log := fakeSetup(map[string]bool{}, func(key string) (chatgpt.CapturedSession, error) {
		calls++
		if calls < 2 {
			return chatgpt.CapturedSession{}, &chatgpt.CookieError{Msg: "no session"}
		}
		return chatgpt.CapturedSession{Browser: "Brave", CookieHeader: "h"}, nil
	})
	brave, _ := chromium.BrowserByKey("brave")
	got, err := s.EnsureLoggedIn(brave)
	if err != nil || got.Browser != "Brave" {
		t.Fatalf("got %+v err %v", got, err)
	}
	joined := strings.Join(*log, "\n")
	if !strings.Contains(joined, "open:/Applications/Brave Browser.app") || !strings.Contains(joined, "open:https://chatgpt.com/") || !strings.Contains(joined, "prompt") || !strings.Contains(joined, "attempt 1/2") {
		t.Fatalf("log = %v", *log)
	}
	always := func(string) (chatgpt.CapturedSession, error) {
		return chatgpt.CapturedSession{}, &chatgpt.CookieError{Msg: "still nothing"}
	}
	s, _ = fakeSetup(map[string]bool{}, always)
	if _, err := s.EnsureLoggedIn(brave); err == nil || err.Error() != "still nothing" {
		t.Fatalf("err = %v", err)
	}
}

// --- Run --------------------------------------------------------------------

type fakeValidator struct {
	result chatgpt.Validation
	err    error
}

func (f fakeValidator) Validate(string) (chatgpt.Validation, error) { return f.result, f.err }

type harness struct {
	deps      chatgpt.Deps
	published []string
	installs  []bool
	discovers []string
	prompted  bool
}

func newHarness(credential string, validation chatgpt.Validation, validateErr error) *harness {
	h := &harness{}
	brave, _ := chromium.BrowserByKey("brave")
	h.deps = chatgpt.Deps{
		Validator: fakeValidator{validation, validateErr},
		Now:       func() time.Time { return time.Unix(1_800_000_000, 0) },
		Publisher: func() (chatgpt.Publisher, error) {
			return func(account, token, key, browser string) (map[string]any, error) {
				h.published = []string{account, token, key, browser}
				return map[string]any{"token_sha256": "deadbeefcafe1234"}, nil
			}, nil
		},
	}
	h.deps.Setup = chatgpt.Setup{
		Host: chromium.Host{FileExists: func(p string) bool { return p == brave.AppBundle }},
		Discover: func(key string) (chatgpt.CapturedSession, error) {
			h.discovers = append(h.discovers, key)
			if credential == "" {
				return chatgpt.CapturedSession{}, &chatgpt.CookieError{Msg: "no session found"}
			}
			return chatgpt.CapturedSession{Browser: "Google Chrome", CookieHeader: credential, CookieCount: 3, HasSessionToken: true}, nil
		},
		Prompt: func(string) error { h.prompted = true; return nil },
		Open:   func(string) error { return nil },
		Log:    func(string) {},
		Sleep:  func(time.Duration) {},
	}
	return h
}

func run(t *testing.T, h *harness, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := chatgpt.RunWith(args, &stdout, &stderr, envOf(env), h.deps)
	if strings.Contains(stdout.String(), "header-secret") || strings.Contains(stderr.String(), "header-secret") {
		t.Fatalf("cookie leaked: %s %s", stdout.String(), stderr.String())
	}
	return code, stdout.String(), stderr.String()
}

var signedIn = chatgpt.Validation{SignedInAs: "user@example.com"}

func TestRunHappyPath(t *testing.T) {
	h := newHarness("header-secret", signedIn, nil)
	code, out, errOut := run(t, h, nil, "publish-session", "--account", "user@example.com")
	if code != 0 {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	if !strings.Contains(out, "Signed in as user@example.com") || !strings.Contains(out, "Published ChatGPT session for user@example.com (key=default, browser=Google Chrome, token_sha256=deadbeefcafe...)") {
		t.Fatalf("out = %s", out)
	}
	if strings.Join(h.published, "|") != "user@example.com|header-secret|default|Google Chrome" {
		t.Fatalf("published = %v", h.published)
	}
}

func TestRunAccountFallbackAndMissingAccount(t *testing.T) {
	h := newHarness("header-secret", signedIn, nil)
	code, _, _ := run(t, h, map[string]string{"GMAIL_ACCOUNTS": "primary@example.com, secondary@example.com"}, "publish-session")
	if code != 0 || h.published[0] != "primary@example.com" {
		t.Fatalf("code = %d published = %v", code, h.published)
	}
	h = newHarness("header-secret", signedIn, nil)
	code, _, errOut := run(t, h, nil, "publish-session")
	if code != 1 || !strings.Contains(errOut, "no ChatGPT account configured") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
}

func TestRunDryRunNeverPublishes(t *testing.T) {
	h := newHarness("header-secret", signedIn, nil)
	h.deps.Publisher = func() (chatgpt.Publisher, error) { t.Fatal("must not publish on dry-run"); return nil, nil }
	code, out, _ := run(t, h, nil, "publish-session", "--account", "a@b.com", "--dry-run")
	if code != 0 || !strings.Contains(out, "--dry-run: not publishing") {
		t.Fatalf("code = %d out = %s", code, out)
	}
}

func TestRunRejectedSession(t *testing.T) {
	h := newHarness("header-secret", chatgpt.Validation{}, &chatgpt.AuthError{Msg: "session expired"})
	code, _, errOut := run(t, h, nil, "publish-session", "--account", "a@b.com")
	if code != 1 || !strings.Contains(errOut, "rejected it") || h.published != nil {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	h = newHarness("header-secret", chatgpt.Validation{}, errors.New("/api/auth/session returned 502"))
	code, _, errOut = run(t, h, nil, "publish-session", "--account", "a@b.com")
	if code != 1 || !strings.Contains(errOut, "could not validate") || h.published != nil {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
}

func TestRunCloudflareChallengePublishesUnvalidated(t *testing.T) {
	h := newHarness("header-secret", chatgpt.Validation{Blocked: true, BlockedReason: "chatgpt.com answered with a Cloudflare managed challenge"}, nil)
	code, out, errOut := run(t, h, nil, "publish-session", "--account", "a@b.com")
	if code != 0 || h.published == nil {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	if !strings.Contains(errOut, "local validation was blocked") || !strings.Contains(errOut, "/pipelines") || strings.Contains(out, "Signed in as") {
		t.Fatalf("out = %s stderr = %s", out, errOut)
	}
	if !strings.Contains(out, "Published ChatGPT session") {
		t.Fatalf("out = %s", out)
	}
}

func TestRunCookieDiscoveryFailure(t *testing.T) {
	h := newHarness("", signedIn, nil)
	code, _, errOut := run(t, h, nil, "publish-session", "--account", "a@b.com")
	if code != 1 || !strings.Contains(errOut, "no session found") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
}

func TestRunReportsANearExpirySession(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	h := newHarness("header-secret", chatgpt.Validation{SignedInAs: "a", AccessTokenExpiry: now.Add(6 * time.Hour)}, nil)
	code, out, errOut := run(t, h, nil, "publish-session", "--account", "a@b.com")
	if code != 0 || !strings.Contains(errOut, "expires in") || !strings.Contains(out, "Published ChatGPT session") {
		t.Fatalf("code = %d out = %s stderr = %s", code, out, errOut)
	}
}

func TestRunNonInteractiveNeverInstallsOrPrompts(t *testing.T) {
	h := newHarness("", signedIn, nil)
	h.deps.Setup.Host.FileExists = func(string) bool { return false }
	h.deps.Setup.Run = func([]string) int { t.Fatal("must not install"); return 1 }
	h.deps.Setup.BrewAvailable = func() bool { return true }
	code, _, errOut := run(t, h, nil, "publish-session", "--account", "a@b.com", "--non-interactive", "--browser", "brave")
	// Nothing installed + no-install => the browser check fails before discovery.
	if code != 1 || !strings.Contains(errOut, "Brave is not installed") || h.prompted {
		t.Fatalf("code = %d stderr = %s prompted = %v", code, errOut, h.prompted)
	}
	h = newHarness("", signedIn, nil)
	code, _, errOut = run(t, h, nil, "publish-session", "--account", "a@b.com", "--non-interactive", "--browser", "brave")
	if code != 1 || !strings.Contains(errOut, "no session found") || h.prompted || strings.Join(h.discovers, ",") != "brave" {
		t.Fatalf("code = %d stderr = %s prompted = %v discovers = %v", code, errOut, h.prompted, h.discovers)
	}
}

func TestRunUnconfiguredWarehouse(t *testing.T) {
	h := newHarness("header-secret", signedIn, nil)
	h.deps.Publisher = func() (chatgpt.Publisher, error) { return nil, errors.New("PDW_API_URL must be set") }
	code, _, errOut := run(t, h, nil, "publish-session", "--account", "a@b.com")
	if code != 1 || !strings.Contains(errOut, "pdw login") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
}

func TestRunVerbDispatch(t *testing.T) {
	h := newHarness("header-secret", signedIn, nil)
	if code, _, errOut := run(t, h, nil); code != 2 || !strings.Contains(errOut, "usage: pdw chatgpt publish-session") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	if code, _, _ := run(t, h, nil, "--help"); code != 0 {
		t.Fatalf("code = %d", code)
	}
	if code, _, errOut := run(t, h, nil, "nope"); code != 2 || !strings.Contains(errOut, "unknown command") {
		t.Fatalf("code = %d stderr = %s", code, errOut)
	}
	if code, _, _ := run(t, h, nil, "publish-session", "--bogus"); code != 2 {
		t.Fatalf("code = %d", code)
	}
}
