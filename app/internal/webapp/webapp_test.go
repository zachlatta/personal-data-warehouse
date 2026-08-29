package webapp

import (
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
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
	for _, route := range []string{"/timeline", "/timeline/gmail_email/abc", "/mutation-review", "/mutation-review/requests/req-1", "/search"} {
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
		"min-height: 44px",
		"env(safe-area-inset-bottom)",
	} {
		if !strings.Contains(text, contract) {
			t.Fatalf("mutation review CSS has no phone contract %q", contract)
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
	// Syntax-check every shipped module too: node parses ES modules on import.
	entries, _ := filepath.Glob(filepath.Join(dir, "static", "*.js"))
	for _, entry := range entries {
		check := exec.Command(node, "--input-type=module", "-e", "import("+jsString(entry)+").then(()=>process.exit(0), e=>{console.error(e);process.exit(1)})")
		check.Dir = filepath.Join(dir, "static")
		if out, err := check.CombinedOutput(); err != nil && !strings.Contains(string(out), "document is not defined") && !strings.Contains(string(out), "window is not defined") {
			t.Fatalf("%s does not parse: %v\n%s", filepath.Base(entry), err, out)
		}
	}
}

func jsString(value string) string {
	return `"` + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `"`, `\"`) + `"`
}
