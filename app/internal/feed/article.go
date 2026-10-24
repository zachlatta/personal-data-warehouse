package feed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"

	readability "codeberg.org/readeck/go-readability/v2"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
	"golang.org/x/sync/singleflight"
)

// A tap on an article card shows the whole article in the detail view
// (Zach's voice memo, 2026-10-24: "embed the entire article content directly
// inline instead of me having to tap out ... with the images and all that
// stuff right there"). The phone asks GET /api/feed/article?url=...; the
// server fetches the page, keeps its readable part with Mozilla's
// Readability algorithm (go-readability), strips anything that could run,
// and returns the article as HTML for the phone to show in a WebView.

// Article is one page's readable content.
type Article struct {
	URL         string `json:"url"`
	Title       string `json:"title,omitempty"`
	Byline      string `json:"byline,omitempty"`
	Site        string `json:"site,omitempty"`
	ImageURL    string `json:"image_url,omitempty"`
	PublishedAt string `json:"published_at,omitempty"`
	Language    string `json:"language,omitempty"`
	// HTML is the article body: no scripts, styles, forms, frames or event
	// handlers; links and image sources are absolute.
	HTML string `json:"html"`
	// Words is the length of its text, for a reading time.
	Words int `json:"words"`
}

// ArticleReader fetches and extracts one page; tests swap the fetch.
type ArticleReader struct {
	client *http.Client
	now    func() time.Time

	mu    sync.Mutex
	cache map[string]cachedArticle
	group singleflight.Group
}

type cachedArticle struct {
	article *Article
	err     error
	at      time.Time
}

const (
	articleTimeout   = 12 * time.Second
	articleMaxBytes  = 8 << 20
	articleMinWords  = 80
	articleCacheSize = 300
	articleCacheTTL  = 6 * time.Hour
	articleErrorTTL  = 10 * time.Minute
	// A desktop browser's agent: several sites send a stripped page, or
	// refuse, to anything that looks like a bot.
	articleUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15"
)

// ErrNotArticle is a page that fetched but has no readable article (a video
// page, an app, a login wall).
var ErrNotArticle = errors.New("no readable article on this page")

// NewArticleReader fetches only public addresses: the url comes from a card
// an agent pushed, so it must not reach the app's own network.
func NewArticleReader() *ArticleReader {
	dialer := &net.Dialer{Timeout: 5 * time.Second, Control: publicOnly}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = dialer.DialContext
	transport.Proxy = nil
	return newArticleReader(&http.Client{Transport: transport, Timeout: articleTimeout})
}

func newArticleReader(client *http.Client) *ArticleReader {
	return &ArticleReader{client: client, now: time.Now, cache: map[string]cachedArticle{}}
}

// publicOnly refuses a connection to a loopback, private, link-local or
// otherwise internal address, after DNS (so a redirect or a name that
// resolves inward is refused too).
func publicOnly(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsInterfaceLocalMulticast() || isCGNAT(ip) {
		return fmt.Errorf("refusing to fetch an internal address (%s)", host)
	}
	return nil
}

func isCGNAT(ip net.IP) bool {
	v4 := ip.To4()
	return v4 != nil && v4[0] == 100 && v4[1]&0xc0 == 64
}

// Read returns the page's article, from a short cache when it was read
// recently. Concurrent reads of one url share a fetch.
func (r *ArticleReader) Read(ctx context.Context, raw string) (*Article, error) {
	pageURL, err := checkURL("url", raw, false)
	if err != nil {
		return nil, err
	}
	if pageURL == "" {
		return nil, invalid("url is required")
	}
	if a, err, ok := r.cached(pageURL); ok {
		return a, err
	}
	v, err, _ := r.group.Do(pageURL, func() (any, error) {
		a, err := r.fetch(context.WithoutCancel(ctx), pageURL)
		r.store(pageURL, a, err)
		return a, err
	})
	if err != nil {
		return nil, err
	}
	return v.(*Article), nil
}

func (r *ArticleReader) cached(key string) (*Article, error, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.cache[key]
	if !ok {
		return nil, nil, false
	}
	ttl := articleCacheTTL
	if c.err != nil {
		ttl = articleErrorTTL
	}
	if r.now().Sub(c.at) > ttl {
		delete(r.cache, key)
		return nil, nil, false
	}
	return c.article, c.err, true
}

func (r *ArticleReader) store(key string, a *Article, err error) {
	var invalidErr *ValidationError
	if errors.As(err, &invalidErr) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.cache) >= articleCacheSize {
		oldestKey, oldest := "", r.now()
		for k, c := range r.cache {
			if c.at.Before(oldest) {
				oldestKey, oldest = k, c.at
			}
		}
		delete(r.cache, oldestKey)
	}
	r.cache[key] = cachedArticle{article: a, err: err, at: r.now()}
}

func (r *ArticleReader) fetch(ctx context.Context, pageURL string) (*Article, error) {
	ctx, cancel := context.WithTimeout(ctx, articleTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", articleUserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.5")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching the page: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("the page answered HTTP %d", resp.StatusCode)
	}
	if mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); mediaType != "" &&
		mediaType != "text/html" && mediaType != "application/xhtml+xml" {
		return nil, fmt.Errorf("%w (it is %s)", ErrNotArticle, mediaType)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, articleMaxBytes))
	if err != nil {
		return nil, fmt.Errorf("reading the page: %w", err)
	}
	// Links and images resolve against where the page ended up.
	return extractArticle(body, resp.Request.URL)
}

// extractArticle keeps a page's readable part and makes it safe to show.
func extractArticle(page []byte, pageURL *url.URL) (*Article, error) {
	parser := readability.NewParser()
	parsed, err := parser.Parse(bytes.NewReader(page), pageURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotArticle, err)
	}
	if parsed.Node == nil {
		return nil, ErrNotArticle
	}
	sanitizeArticle(parsed.Node)
	var text strings.Builder
	if err := parsed.RenderText(&text); err != nil {
		return nil, err
	}
	words := len(strings.Fields(text.String()))
	if words < articleMinWords {
		return nil, fmt.Errorf("%w (only %d words)", ErrNotArticle, words)
	}
	var body strings.Builder
	if err := html.Render(&body, parsed.Node); err != nil {
		return nil, err
	}
	a := &Article{
		URL:      pageURL.String(),
		Title:    strings.TrimSpace(parsed.Title()),
		Byline:   strings.TrimSpace(parsed.Byline()),
		Site:     strings.TrimSpace(parsed.SiteName()),
		ImageURL: parsed.ImageURL(),
		Language: parsed.Language(),
		HTML:     body.String(),
		Words:    words,
	}
	if published, err := parsed.PublishedTime(); err == nil && !published.IsZero() {
		a.PublishedAt = published.UTC().Format(time.RFC3339)
	}
	return a, nil
}

// droppedElements never reach the phone: anything that runs code, takes
// input or loads another page. An iframe (an embedded video or post)
// becomes a link to what it showed.
var droppedElements = map[atom.Atom]bool{
	atom.Script: true, atom.Style: true, atom.Noscript: true, atom.Template: true,
	atom.Form: true, atom.Input: true, atom.Button: true, atom.Select: true, atom.Textarea: true,
	atom.Object: true, atom.Embed: true, atom.Applet: true, atom.Frame: true, atom.Frameset: true,
	atom.Link: true, atom.Meta: true, atom.Base: true, atom.Svg: true, atom.Math: true,
}

// keptAttributes are the only attributes left on any element.
var keptAttributes = map[string]bool{
	"href": true, "src": true, "srcset": true, "sizes": true, "alt": true, "title": true,
	"width": true, "height": true, "colspan": true, "rowspan": true, "start": true,
	"type": true, "datetime": true, "cite": true, "lang": true, "dir": true, "controls": true,
	"poster": true,
}

func sanitizeArticle(node *html.Node) {
	for child := node.FirstChild; child != nil; {
		next := child.NextSibling
		switch {
		case child.Type == html.CommentNode:
			node.RemoveChild(child)
		case child.Type == html.ElementNode && child.DataAtom == atom.Iframe:
			src := safeURL(attr(child, "src"))
			if src == "" {
				node.RemoveChild(child)
				break
			}
			link := &html.Node{Type: html.ElementNode, Data: "a", DataAtom: atom.A,
				Attr: []html.Attribute{{Key: "href", Val: src}}}
			link.AppendChild(&html.Node{Type: html.TextNode, Data: "Embedded content: " + hostOf(src)})
			p := &html.Node{Type: html.ElementNode, Data: "p", DataAtom: atom.P}
			p.AppendChild(link)
			node.InsertBefore(p, child)
			node.RemoveChild(child)
		case child.Type == html.ElementNode && droppedElements[child.DataAtom]:
			node.RemoveChild(child)
		case child.Type == html.ElementNode && child.DataAtom == 0:
			// An unknown element (a custom web component) keeps its
			// children as a plain block.
			child.Data, child.DataAtom, child.Namespace = "div", atom.Div, ""
			cleanAttributes(child)
			sanitizeArticle(child)
		case child.Type == html.ElementNode:
			cleanAttributes(child)
			sanitizeArticle(child)
		}
		child = next
	}
}

func cleanAttributes(n *html.Node) {
	kept := n.Attr[:0]
	for _, a := range n.Attr {
		key := strings.ToLower(a.Key)
		if a.Namespace != "" || !keptAttributes[key] {
			continue
		}
		switch key {
		case "href", "src", "poster", "cite":
			a.Val = safeURL(a.Val)
			if a.Val == "" {
				continue
			}
		case "srcset":
			if strings.Contains(strings.ToLower(a.Val), "javascript:") {
				continue
			}
		}
		a.Key = key
		kept = append(kept, a)
	}
	n.Attr = kept
}

// safeURL keeps http(s), mailto and in-page (#) links; anything else
// (javascript:, data:, file:) is dropped.
func safeURL(raw string) string {
	value := strings.TrimSpace(raw)
	if strings.HasPrefix(value, "#") {
		return value
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return ""
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https", "mailto":
		return value
	}
	return ""
}

func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if strings.EqualFold(a.Key, key) {
			return a.Val
		}
	}
	return ""
}
