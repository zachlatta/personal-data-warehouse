package slack_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium/chromiumtest"
	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/slack"
)

func writeLevelDB(t *testing.T, root string, files map[string][]byte) string {
	t.Helper()
	store := filepath.Join(root, "Local Storage", "leveldb")
	if err := os.MkdirAll(store, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, payload := range files {
		if err := os.WriteFile(filepath.Join(store, name), payload, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

func utf16le(s string) []byte {
	out := make([]byte, 0, 2*len(s))
	for _, r := range s {
		out = append(out, byte(r), byte(r>>8))
	}
	return out
}

func TestScanFindsTokensInAsciiAndUTF16(t *testing.T) {
	root := t.TempDir()
	writeLevelDB(t, root, map[string][]byte{"000005.ldb": []byte(`{"teams":{"T1":{"token":"xoxc-aaaa1111bbbb"}}}`)})
	if got := slack.ScanLocalStorageForTokens(root); len(got) != 1 || got[0] != "xoxc-aaaa1111bbbb" {
		t.Fatalf("got %v", got)
	}
	root2 := t.TempDir()
	writeLevelDB(t, root2, map[string][]byte{"026136.ldb": append([]byte{0}, utf16le(`{"token":"xoxc-utf16token99"}`)...)})
	if got := slack.ScanLocalStorageForTokens(root2); len(got) != 1 || got[0] != "xoxc-utf16token99" {
		t.Fatalf("got %v", got)
	}
	if got := slack.ScanLocalStorageForTokens(t.TempDir()); got != nil {
		t.Fatalf("missing store should be empty, got %v", got)
	}
}

func TestScanOrdersNewestFileFirstAndDedups(t *testing.T) {
	root := t.TempDir()
	store := writeLevelDB(t, root, map[string][]byte{
		"000005.ldb": []byte(`{"token":"xoxc-oldoldold11"}`),
		"026136.ldb": []byte(`{"token":"xoxc-newnewnew22"} xoxc-newnewnew22 xoxb-notauser99 xoxc-sh`),
	})
	os.Chtimes(filepath.Join(store, "000005.ldb"), time.Unix(1_000_000, 0), time.Unix(1_000_000, 0))
	os.Chtimes(filepath.Join(store, "026136.ldb"), time.Unix(2_000_000, 0), time.Unix(2_000_000, 0))
	got := slack.ScanLocalStorageForTokens(root)
	if strings.Join(got, ",") != "xoxc-newnewnew22,xoxc-oldoldold11" {
		t.Fatalf("got %v", got)
	}
}

func TestCapturePicksTheTokenSlackAccepts(t *testing.T) {
	root := t.TempDir()
	store := writeLevelDB(t, root, map[string][]byte{"old.ldb": []byte("xoxc-staletoken111"), "new.ldb": []byte("xoxc-livetoken2222")})
	// The stale token's file is NEWER so it is tried first; only Slack's answer saves us.
	os.Chtimes(filepath.Join(store, "old.ldb"), time.Unix(2_000_000, 0), time.Unix(2_000_000, 0))
	os.Chtimes(filepath.Join(store, "new.ldb"), time.Unix(1_000_000, 0), time.Unix(1_000_000, 0))
	var seen []string
	authTest := func(token, cookieHeader string) map[string]any {
		seen = append(seen, token)
		if cookieHeader != "d=xoxd-cookievalue" {
			t.Fatalf("cookie header %q", cookieHeader)
		}
		if token == "xoxc-livetoken2222" {
			return map[string]any{"ok": true, "team_id": "T0266FRGM", "user_id": "U09UE480JHH", "url": "https://hackclub.slack.com/"}
		}
		return map[string]any{"ok": false, "error": "invalid_auth"}
	}
	expires := time.Date(2027, 9, 28, 0, 0, 0, 0, time.UTC)
	s, err := slack.Capture(root, map[string]string{"d": "xoxd-cookievalue"}, expires, "slack-app", authTest)
	if err != nil {
		t.Fatal(err)
	}
	if s.Token != "xoxc-livetoken2222" || s.TeamID != "T0266FRGM" || s.UserID != "U09UE480JHH" || s.EnterpriseID != "" {
		t.Fatalf("session = %+v", s)
	}
	if strings.Join(seen, ",") != "xoxc-staletoken111,xoxc-livetoken2222" {
		t.Fatalf("seen = %v", seen)
	}
	if s.Redacted()["cookie_expires_at"] != "2027-09-28T00:00:00+00:00" {
		t.Fatalf("redacted = %v", s.Redacted())
	}
}

func TestCaptureFailures(t *testing.T) {
	root := t.TempDir()
	writeLevelDB(t, root, map[string][]byte{"a.ldb": []byte("xoxc-deadtoken1111")})
	_, err := slack.Capture(root, map[string]string{"d": "xoxd"}, time.Time{}, "slack-app",
		func(string, string) map[string]any { return map[string]any{"ok": false, "error": "invalid_auth"} })
	if err == nil || !strings.Contains(err.Error(), "no working") || !strings.Contains(err.Error(), "invalid_auth") {
		t.Fatalf("err = %v", err)
	}
	_, err = slack.Capture(root, map[string]string{"b": "not-the-session"}, time.Time{}, "slack-app",
		func(string, string) map[string]any { return map[string]any{"ok": true} })
	if err == nil || !strings.Contains(err.Error(), "`d` session cookie") {
		t.Fatalf("err = %v", err)
	}
	_, err = slack.Capture(t.TempDir(), map[string]string{"d": "x"}, time.Time{}, "slack-app",
		func(string, string) map[string]any { return map[string]any{"ok": true} })
	if err == nil || !strings.Contains(err.Error(), "no xoxc- token") {
		t.Fatalf("err = %v", err)
	}
}

func TestCaptureDoesNotPassOffAnEnterpriseIDAsATeamID(t *testing.T) {
	root := t.TempDir()
	writeLevelDB(t, root, map[string][]byte{"a.ldb": []byte("xoxc-enterprisetok1")})
	s, err := slack.Capture(root, map[string]string{"d": "xoxd"}, time.Time{}, "slack-app",
		func(string, string) map[string]any {
			return map[string]any{"ok": true, "team_id": "E09V59WQY1E", "user_id": "U1", "url": "https://hackclub.enterprise.slack.com/"}
		})
	if err != nil {
		t.Fatal(err)
	}
	if s.EnterpriseID != "E09V59WQY1E" || s.TeamID != "" {
		t.Fatalf("an enterprise id must never be stored as a team id: %+v", s)
	}
	if s.Redacted()["enterprise_id"] != "E09V59WQY1E" || s.Redacted()["cookie_expires_at"] != nil {
		t.Fatalf("redacted = %v", s.Redacted())
	}
}

func TestRedactedCarriesNoSecret(t *testing.T) {
	s := slack.Session{Source: "slack-app", Token: "xoxc-secrettoken1", CookieD: "xoxd-secretcookie", TeamID: "T1", UserID: "U1"}
	blob, _ := json.Marshal(s.Redacted())
	if strings.Contains(string(blob), "xoxc-secrettoken1") || strings.Contains(string(blob), "xoxd-secretcookie") {
		t.Fatalf("secret leaked: %s", blob)
	}
	if !strings.Contains(string(blob), s.Fingerprint()) || !strings.Contains(string(blob), `"team_id":"T1"`) {
		t.Fatalf("blob = %s", blob)
	}
}

func TestClientPostsTokenAndCookieAndSummarisesCounts(t *testing.T) {
	var gotCookie, gotToken, gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		gotCookie, gotToken, gotUA = r.Header.Get("Cookie"), r.PostForm.Get("token"), r.Header.Get("User-Agent")
		switch r.URL.Path {
		case "/api/auth.test":
			json.NewEncoder(w).Encode(map[string]any{"ok": true, "team_id": "T1", "user_id": "U1", "url": "https://x.slack.com/"})
		case "/api/client.counts":
			if r.PostForm.Get("org_wide_aware") != "true" || r.PostForm.Get("thread_counts_by_channel") != "true" {
				t.Errorf("form = %v", r.PostForm)
			}
			json.NewEncoder(w).Encode(map[string]any{"ok": true,
				"channels": []any{map[string]any{"latest": "1.2"}, map[string]any{}},
				"ims":      []any{map[string]any{"latest": "3.4"}},
				"mpims":    []any{}})
		case "/api/boom":
			w.WriteHeader(500)
		default:
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "unknown_method"})
		}
	}))
	defer srv.Close()
	c := &slack.Client{BaseURL: srv.URL, HTTP: srv.Client()}
	if p := c.AuthTest("xoxc-tok", "d=cookie"); p["team_id"] != "T1" {
		t.Fatalf("auth.test = %v", p)
	}
	if gotCookie != "d=cookie" || gotToken != "xoxc-tok" || !strings.Contains(gotUA, "Slack_SSB") {
		t.Fatalf("cookie=%q token=%q ua=%q", gotCookie, gotToken, gotUA)
	}
	summary := c.ProbeClientCounts(slack.Session{Token: "xoxc-tok", CookieD: "cookie"})
	if summary["ok"] != true || summary["channels"] != 2 || summary["ims"] != 1 || summary["mpims"] != 0 ||
		summary["total_conversations"] != 3 || summary["with_latest_marker"] != 2 {
		t.Fatalf("summary = %v", summary)
	}
	bad := &slack.Client{BaseURL: srv.URL + "/nope", HTTP: srv.Client()}
	if p := bad.ProbeClientCounts(slack.Session{}); p["ok"] != false || p["error"] != "unknown_method" {
		t.Fatalf("bad = %v", p)
	}
}

func TestDiscoverReadsTheSlackAppStore(t *testing.T) {
	key := chromiumtest.Key("slack-pw")
	support := t.TempDir()
	appRoot := filepath.Join(support, "Slack")
	os.MkdirAll(appRoot, 0o755)
	chromiumtest.WriteCookieDB(t, filepath.Join(appRoot, "Cookies"), key, []chromiumtest.Row{
		{HostKey: ".slack.com", Name: "d", Value: "xoxd-live", ExpiresUTC: 13400000000000000},
		{HostKey: ".slack.com", Name: "b", Value: "other"},
	})
	writeLevelDB(t, appRoot, map[string][]byte{"1.ldb": []byte("xoxc-apptoken1234")})
	host := chromium.Host{ApplicationSupport: support, KeychainPassword: func(service, account string) (string, error) {
		if service != "Slack Safe Storage" || account != "Slack" {
			t.Fatalf("keychain item %q/%q", service, account)
		}
		return "slack-pw", nil
	}}
	s, err := slack.Discover(host, "", func(token, cookie string) map[string]any {
		if token != "xoxc-apptoken1234" || cookie != "d=xoxd-live" {
			t.Fatalf("token=%q cookie=%q", token, cookie)
		}
		return map[string]any{"ok": true, "team_id": "T1"}
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.Source != "slack-app" || s.CookieExpiresAt.IsZero() || s.CookieD != "xoxd-live" {
		t.Fatalf("session = %+v", s)
	}
	if _, err := slack.Discover(host, "netscape", nil); err == nil || !strings.Contains(err.Error(), "unknown Slack session source") {
		t.Fatalf("err = %v", err)
	}
	host.ApplicationSupport = t.TempDir()
	if _, err := slack.Discover(host, "", nil); err == nil || !strings.Contains(err.Error(), "no slack.com `d` cookie") {
		t.Fatalf("err = %v", err)
	}
}
