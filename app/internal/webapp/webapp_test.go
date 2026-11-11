package webapp

import (
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	h, err := New()
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	Register(mux, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, url string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(body)
}

// Every SPA route, and every deep link beneath it, returns the same shell:
// the browser router decides what to show, and a review URL pushed to the
// phone or returned to an agent must open directly.
func TestEveryRouteServesTheShellWithoutAuth(t *testing.T) {
	srv := newServer(t)
	for _, route := range []string{"/connections", "/timeline", "/timeline/gmail_email/abc", "/mutation-review", "/mutation-review/requests/req-1", "/search", "/feed"} {
		resp, body := get(t, srv.URL+route)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: got %d, want 200", route, resp.StatusCode)
		}
		if !strings.Contains(resp.Header.Get("Content-Type"), "text/html") {
			t.Fatalf("%s: content type %q", route, resp.Header.Get("Content-Type"))
		}
		if resp.Header.Get("Cache-Control") != "no-store" {
			t.Fatalf("%s: the shell must not be cached, got %q", route, resp.Header.Get("Cache-Control"))
		}
		if !strings.Contains(body, `<div id="app"`) || strings.Contains(body, "__ASSET_VERSION__") {
			t.Fatalf("%s: shell not rendered: %q", route, body[:min(len(body), 200)])
		}
	}
}

// The shell must reference only assets that exist, with the version stamp,
// so a deploy never serves a page whose script 404s.
func TestShellReferencesVersionedEmbeddedAssets(t *testing.T) {
	srv := newServer(t)
	_, shell := get(t, srv.URL+"/timeline")
	sub, _ := fs.Sub(staticFS, "static")
	version, _ := assetVersion(sub)
	found := 0
	for _, line := range strings.Split(shell, "\n") {
		for _, attr := range []string{`src="`, `href="`} {
			idx := strings.Index(line, attr+AssetPrefix)
			if idx < 0 {
				continue
			}
			ref := line[idx+len(attr):]
			ref = ref[:strings.Index(ref, `"`)]
			if !strings.HasSuffix(ref, "?v="+version) {
				t.Fatalf("asset %q is not versioned with %q", ref, version)
			}
			resp, _ := get(t, srv.URL+ref)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("asset %q: got %d", ref, resp.StatusCode)
			}
			if resp.Header.Get("Cache-Control") != "public, max-age=31536000, immutable" {
				t.Fatalf("versioned asset %q must be cached hard, got %q", ref, resp.Header.Get("Cache-Control"))
			}
			found++
		}
	}
	if found < 2 {
		t.Fatalf("shell references %d assets; expected the script and the stylesheet", found)
	}
	// Unversioned requests must revalidate rather than pin a stale copy.
	resp, _ := get(t, srv.URL+AssetPrefix+"app.js")
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Cache-Control") != "no-cache" {
		t.Fatalf("unversioned asset: %d %q", resp.StatusCode, resp.Header.Get("Cache-Control"))
	}
	if resp, _ := get(t, srv.URL+AssetPrefix+"index.html"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("the raw shell must not be served as an asset, got %d", resp.StatusCode)
	}
}

// Every ES module the shell loads must import only modules that exist: a
// typo in an import path is a blank page in production, not a build error.
func TestModuleImportsResolve(t *testing.T) {
	sub, _ := fs.Sub(staticFS, "static")
	err := fs.WalkDir(sub, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(name, ".js") {
			return err
		}
		data, _ := fs.ReadFile(sub, name)
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "import ") && !strings.HasPrefix(line, "export ") {
				continue
			}
			from := strings.LastIndex(line, " from ")
			if from < 0 {
				continue
			}
			spec := strings.Trim(strings.TrimSuffix(strings.TrimSpace(line[from+6:]), ";"), `"'`)
			if !strings.HasPrefix(spec, "./") {
				t.Fatalf("%s imports %q: only relative embedded modules are allowed (no CDN)", name, spec)
			}
			if _, err := fs.Stat(sub, filepath.ToSlash(filepath.Join(filepath.Dir(name), spec))); err != nil {
				t.Fatalf("%s imports %q which does not exist", name, spec)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// The shell must not load anything from another origin: it has to work
// against a local build with the tightest CSP, and a CDN outage must not take
// the review UI with it.
func TestShellLoadsNoExternalAssets(t *testing.T) {
	srv := newServer(t)
	_, shell := get(t, srv.URL+"/timeline")
	for _, needle := range []string{"https://", "http://", "//cdn", "esm.sh"} {
		if strings.Contains(shell, needle) {
			t.Fatalf("shell references an external origin: %q", needle)
		}
	}
}

// Mutation decisions are routinely opened from push notifications. Keep the
// phone layout intentional instead of relying on a squeezed desktop table and
// 25px controls that happen to fit inside the viewport.
func TestMutationReviewCSSIncludesPhoneLayoutAndTouchTargets(t *testing.T) {
	css, err := fs.ReadFile(staticFS, "static/mutations.css")
	if err != nil {
		t.Fatal(err)
	}
	text := string(css)
	for _, contract := range []string{
		"@media (max-width: 640px)",
		"table.rtable th:nth-child(3)",
		".actions input.reason",
		".decision-dock",
		".review-metrics",
		".slack-review-row",
		".mutation-filter",
		// A thousand-mutation request is read a page at a time; the pager
		// is how a phone gets to the next page.
		".mutation-pager",
		// Answering a Slack message is often what settles a mark-read
		// review, so its link is a real touch target, not a 13px glyph.
		".slack-open",
		"min-height: 44px",
		"env(safe-area-inset-bottom)",
	} {
		if !strings.Contains(text, contract) {
			t.Fatalf("mutation review CSS has no phone contract %q", contract)
		}
	}
}

// Mobile deliberately opens with the attention tiers selected. The browser
// does not: broad discovery remains all-tier until the reader chooses chips,
// matching CLI and MCP. Keep that cross-surface difference explicit.
func TestSearchWebDefaultsToAllTiers(t *testing.T) {
	source, err := fs.ReadFile(staticFS, "static/search.js")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, contract := range []string{
		"const state = { priorities: {}, dead: false }",
		"if (tiers.length) input.priorities = tiers",
	} {
		if !strings.Contains(text, contract) {
			t.Fatalf("browser search no longer has its intentional all-tier default: missing %q", contract)
		}
	}
}

// The JS logic that used to be Go (contact summaries, calendar recurrence,
// time formatting) has its own node tests. They run here when node is
// available so `go test ./...` is still the one verification command.
func TestJavaScriptUnitTests(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed; JS unit tests skipped")
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	tests, _ := filepath.Glob(filepath.Join(dir, "static", "tests", "*.test.mjs"))
	if len(tests) == 0 {
		t.Fatal("no JS unit tests found under static/tests")
	}
	cmd := exec.Command(node, append([]string{"--test"}, tests...)...)
	cmd.Dir = filepath.Join(dir, "static")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node --test failed: %v\n%s", err, out)
	}
	// Syntax-check every script without executing browser/service-worker globals.
	entries, _ := filepath.Glob(filepath.Join(dir, "static", "*.js"))
	for _, entry := range entries {
		check := exec.Command(node, "--check", entry)
		check.Dir = filepath.Join(dir, "static")
		if out, err := check.CombinedOutput(); err != nil {
			t.Fatalf("%s does not parse: %v\n%s", filepath.Base(entry), err, out)
		}
	}
}

func jsString(value string) string {
	return `"` + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `"`, `\"`) + `"`
}

// Android opens PDW links in the phone app only if this file, fetched without
// auth, names the app's package and a signing key in the documented shape.
func TestAssetLinksNameTheAndroidApp(t *testing.T) {
	srv := newServer(t)
	resp, body := get(t, srv.URL+AssetLinksPath)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("content type %q", resp.Header.Get("Content-Type"))
	}
	var links []struct {
		Relation []string `json:"relation"`
		Target   struct {
			Namespace    string   `json:"namespace"`
			PackageName  string   `json:"package_name"`
			Fingerprints []string `json:"sha256_cert_fingerprints"`
		} `json:"target"`
	}
	if err := json.Unmarshal([]byte(body), &links); err != nil {
		t.Fatal(err)
	}
	if len(links) != 1 || links[0].Relation[0] != "delegate_permission/common.handle_all_urls" ||
		links[0].Target.Namespace != "android_app" || links[0].Target.PackageName != "com.zachlatta.pdw" ||
		len(links[0].Target.Fingerprints) == 0 {
		t.Fatalf("unexpected asset links: %s", body)
	}
	for _, fp := range links[0].Target.Fingerprints {
		if len(fp) != 95 || strings.ToUpper(fp) != fp {
			t.Fatalf("fingerprint %q is not upper-case colon-separated SHA-256", fp)
		}
	}
}

// The design lives in one set of tokens at the top of app.css: every page,
// including the server-rendered /pipelines and /oauth/authorize, links that
// file, so a colour or a radius is changed in one place. Nothing else may
// declare its own palette, and every token a stylesheet or script reads must
// be one app.css defines (or one a script sets on an element itself).
func TestDesignTokensAreDefinedOnceInAppCSS(t *testing.T) {
	appCSS, err := fs.ReadFile(staticFS, "static/app.css")
	if err != nil {
		t.Fatal(err)
	}
	app := string(appCSS)
	for _, contract := range []string{":root {", "@media (prefers-color-scheme: dark)", "--font:", "--mono:", "--accent:", "--ok:", "--warn:", "--danger:"} {
		if !strings.Contains(app, contract) {
			t.Fatalf("app.css is missing the design token contract %q", contract)
		}
	}
	defined := map[string]bool{}
	for _, m := range regexp.MustCompile(`(--[a-z0-9-]+)\s*:`).FindAllStringSubmatch(app, -1) {
		defined[m[1]] = true
	}
	sub, _ := fs.Sub(staticFS, "static")
	files := map[string]string{}
	err = fs.WalkDir(sub, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || strings.HasPrefix(name, "tests/") {
			return err
		}
		if strings.HasSuffix(name, ".css") || strings.HasSuffix(name, ".js") {
			data, _ := fs.ReadFile(sub, name)
			files[name] = string(data)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// A script may set a token on an element (a row's source colour); CSS reads it.
	for _, text := range files {
		for _, m := range regexp.MustCompile(`setProperty\("(--[a-z0-9-]+)"`).FindAllStringSubmatch(text, -1) {
			defined[m[1]] = true
		}
	}
	for name, text := range files {
		if name != "app.css" && strings.Contains(text, ":root") {
			t.Errorf("%s declares its own :root palette; tokens belong in app.css", name)
		}
		for _, legacy := range []string{"--amber", "--bg2", "--surface2", "#0e1116", "#e8b45a", "#10131a"} {
			if strings.Contains(text, legacy) {
				t.Errorf("%s still uses the retired dark/amber palette (%s)", name, legacy)
			}
		}
		for _, m := range regexp.MustCompile(`var\((--[a-z0-9-]+)`).FindAllStringSubmatch(text, -1) {
			if !defined[m[1]] {
				t.Errorf("%s reads %s, which app.css does not define", name, m[1])
			}
		}
	}
}

// Every page carries the same header: the brand and one tab per top-level page.
func TestShellHeaderLinksEveryPage(t *testing.T) {
	srv := newServer(t)
	_, shell := get(t, srv.URL+"/timeline")
	for _, route := range append(append([]string{}, Routes...), "/pipelines") {
		if !strings.Contains(shell, `href="`+route+`"`) {
			t.Fatalf("the header has no tab for %s", route)
		}
	}
	if !strings.Contains(shell, `class="brand-mark"`) {
		t.Fatal("the header has no brand mark")
	}
}
