package feed

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

var articlePage = `<!doctype html><html lang="en"><head>
<title>How a lighthouse keeper mapped the coast</title>
<meta property="og:site_name" content="Example Journal">
<meta name="author" content="Ada Example">
<script>window.tracker = 1</script>
<style>body { color: red }</style>
</head><body>
<nav><a href="/">Home</a> <a href="/about">About</a></nav>
<article>
<h1>How a lighthouse keeper mapped the coast</h1>
<p onclick="steal()">` + loremWords + `</p>
<figure><img src="/img/coast.jpg" alt="The coast" style="width:9000px"><figcaption>The coast at dawn.</figcaption></figure>
<p>` + loremWords + ` <a href="/next">the next part</a> and <a href="javascript:alert(1)">a trap</a>.</p>
<iframe src="https://www.youtube.com/embed/abc123"></iframe>
<p>` + loremWords + `<script>alert(1)</script></p>
<form action="/subscribe"><input name="email"><button>Subscribe</button></form>
</article>
<footer>Copyright</footer>
</body></html>`

var loremWords = strings.Repeat("The keeper walked the shore each morning, counting rocks and writing down the tides. ", 6)

func articleServer(t *testing.T, page string, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits != nil {
			hits.Add(1)
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, page)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestArticleReaderKeepsTheArticleAndStripsAnythingThatRuns(t *testing.T) {
	srv := articleServer(t, articlePage, nil)
	a, err := newArticleReader(srv.Client()).Read(context.Background(), srv.URL+"/story")
	if err != nil {
		t.Fatal(err)
	}
	if a.Title != "How a lighthouse keeper mapped the coast" || a.Site != "Example Journal" || a.Byline != "Ada Example" {
		t.Errorf("metadata %q / %q / %q", a.Title, a.Site, a.Byline)
	}
	if a.Words < articleMinWords {
		t.Errorf("words = %d", a.Words)
	}
	for _, want := range []string{
		`src="` + srv.URL + `/img/coast.jpg"`, // images resolve against the page
		`href="` + srv.URL + `/next"`,         // so do links
		"The coast at dawn.",
		`href="https://www.youtube.com/embed/abc123"`, // an iframe becomes a link
	} {
		if !strings.Contains(a.HTML, want) {
			t.Errorf("article html lacks %s:\n%s", want, a.HTML)
		}
	}
	for _, banned := range []string{"<script", "<style", "onclick", "javascript:", "<iframe", "<form", "<input", "style=", "Copyright", "tracker"} {
		if strings.Contains(strings.ToLower(a.HTML), strings.ToLower(banned)) {
			t.Errorf("article html still has %s:\n%s", banned, a.HTML)
		}
	}
}

func TestArticleReaderRefusesAPageWithNoArticle(t *testing.T) {
	srv := articleServer(t, `<html><body><p>Sign in to continue.</p></body></html>`, nil)
	_, err := newArticleReader(srv.Client()).Read(context.Background(), srv.URL)
	if !errors.Is(err, ErrNotArticle) {
		t.Fatalf("err = %v, want ErrNotArticle", err)
	}
}

func TestArticleReaderCachesReadsAndRefusesBadURLs(t *testing.T) {
	var hits atomic.Int32
	srv := articleServer(t, articlePage, &hits)
	r := newArticleReader(srv.Client())
	for range 3 {
		if _, err := r.Read(context.Background(), srv.URL+"/story"); err != nil {
			t.Fatal(err)
		}
	}
	if hits.Load() != 1 {
		t.Errorf("fetched %d times, want 1", hits.Load())
	}
	for _, bad := range []string{"", "not a url", "file:///etc/passwd", "ftp://example.com/x"} {
		var invalidErr *ValidationError
		if _, err := r.Read(context.Background(), bad); !errors.As(err, &invalidErr) {
			t.Errorf("Read(%q) err = %v, want a ValidationError", bad, err)
		}
	}
}

func TestTheProductionReaderNeverFetchesAnInternalAddress(t *testing.T) {
	srv := articleServer(t, articlePage, nil)
	if _, err := NewArticleReader().Read(context.Background(), srv.URL); err == nil || !strings.Contains(err.Error(), "internal address") {
		t.Fatalf("loopback fetch err = %v, want a refusal", err)
	}
	for addr, refused := range map[string]bool{
		"127.0.0.1:80": true, "10.1.2.3:443": true, "192.168.0.10:80": true, "169.254.169.254:80": true,
		"100.100.1.1:443": true, "[::1]:80": true, "[fd00::1]:443": true, "0.0.0.0:80": true,
		"93.184.215.14:443": false, "[2606:4700::6810:84e5]:443": false,
	} {
		if err := publicOnly("tcp", addr, nil); (err != nil) != refused {
			t.Errorf("publicOnly(%s) = %v, want refused %v", addr, err, refused)
		}
	}
}

func TestArticleEndpointAnswersArticlesAndExplainsFailures(t *testing.T) {
	good := articleServer(t, articlePage, nil)
	empty := articleServer(t, `<html><body>nothing</body></html>`, nil)
	h := &handler{articles: newArticleReader(good.Client()), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	mux := http.NewServeMux()
	h.register(mux, func(next http.Handler) http.Handler { return next })

	get := func(target string) (int, map[string]any) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, APIPath+"/article?url="+target, nil))
		var body map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return rec.Code, body
	}
	if code, body := get(good.URL + "/story"); code != http.StatusOK || !strings.Contains(body["html"].(string), "keeper") {
		t.Errorf("article: %d %v", code, body)
	}
	if code, body := get(empty.URL); code != http.StatusUnprocessableEntity || body["error"] == "" {
		t.Errorf("no article: %d %v", code, body)
	}
	if code, _ := get(""); code != http.StatusBadRequest {
		t.Errorf("missing url: %d", code)
	}
}
