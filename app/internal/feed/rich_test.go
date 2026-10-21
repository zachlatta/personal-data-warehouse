package feed

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

func TestXStatusID(t *testing.T) {
	for raw, want := range map[string]string{
		"https://x.com/karpathy/status/1885026028428681698":       "1885026028428681698",
		"https://twitter.com/jack/status/20?s=46":                 "20",
		"https://mobile.twitter.com/a_b/status/1234567/photo/1":   "1234567",
		"https://x.com/i/web/status/1885026028428681698":          "1885026028428681698",
		"https://www.x.com/someone/statuses/99999":                "99999",
		"https://x.com/karpathy":                                  "",
		"https://example.com/karpathy/status/1885026028428681698": "",
	} {
		got, ok := XStatusID(raw)
		if got != want || ok != (want != "") {
			t.Errorf("XStatusID(%q) = %q, %v; want %q", raw, got, ok, want)
		}
	}
}

func TestNormalizeMediaDefaultsToImagesAndNeedsHTTPS(t *testing.T) {
	media, err := normalizeMedia("media", []Media{{URL: "https://a.test/1.jpg"}, {Type: "VIDEO", URL: "https://a.test/v.mp4", ThumbnailURL: "https://a.test/v.jpg"}})
	if err != nil || len(media) != 2 || media[0].Type != "image" || media[1].Type != "video" {
		t.Fatalf("media %v, %v", media, err)
	}
	if _, err := normalizeMedia("media", []Media{{URL: "http://a.test/1.jpg"}}); err == nil {
		t.Fatal("plain-http media must be refused (iOS blocks it)")
	}
	if _, err := normalizeMedia("media", []Media{{Type: "audio", URL: "https://a.test/1.mp3"}}); err == nil {
		t.Fatal("unknown media type must be refused")
	}
	if _, err := normalizeMedia("media", make([]Media, maxMedia+1)); err == nil {
		t.Fatal("too many media must be refused")
	}
}

func TestNormalizeEmbedChecksThePostItsQuoteAndThread(t *testing.T) {
	quote := &Post{Type: EmbedTypeXPost, Text: "quoted"}
	e, err := normalizeEmbed(&Embed{Post: Post{Type: EmbedTypeXPost, Text: "top"}, Quote: quote,
		Thread: []Post{{Type: EmbedTypeXPost, Text: "two"}}})
	if err != nil || e.Quote.Text != "quoted" || len(e.Thread) != 1 {
		t.Fatalf("embed %+v, %v", e, err)
	}
	if _, err := normalizeEmbed(&Embed{Post: Post{Type: "tiktok"}}); err == nil {
		t.Fatal("unknown embed type must be refused")
	}
	if _, err := normalizeEmbed(&Embed{Post: Post{Type: EmbedTypeXPost}, Quote: &Post{Type: EmbedTypeXPost,
		AuthorAvatarURL: "http://a.test/x.jpg"}}); err == nil {
		t.Fatal("a quoted post's plain-http avatar must be refused")
	}
}

func TestEmbedXPostsFillsOnlyBareXLinksAndNeverFailsThePush(t *testing.T) {
	items := []PushItem{
		{URL: "https://x.com/ada/status/111111"},
		{URL: "https://x.com/bob/status/222222"},
		{URL: "https://example.com/post"},
		{URL: "https://x.com/cy/status/333333", Embed: &Embed{Post: Post{Type: EmbedTypeXPost, Text: "mine"}}},
	}
	calls := 0
	resolve := func(_ context.Context, u string) (*Embed, error) {
		calls++
		if strings.Contains(u, "bob") {
			return nil, errors.New("deleted")
		}
		return &Embed{Post: Post{Type: EmbedTypeXPost, AuthorName: "Ada", CreatedAt: "2026-10-20T12:00:00Z"}}, nil
	}
	embedXPosts(context.Background(), resolve, items)
	if calls != 2 {
		t.Fatalf("resolver calls = %d, want 2 (only bare x.com posts)", calls)
	}
	if items[0].Embed == nil || items[0].Author != "Ada" || items[0].Site != "X" || items[0].PublishedAt != "2026-10-20T12:00:00Z" {
		t.Fatalf("resolved: %+v", items[0])
	}
	if items[1].Embed != nil || items[2].Embed != nil || items[3].Embed.Text != "mine" {
		t.Fatalf("unresolved cards must stay as they were: %+v", items)
	}
}

func TestResolveXPostReadsFxTwitter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/status/1001":
			_, _ = w.Write([]byte(`{"code":200,"tweet":{"url":"https://x.com/ada/status/1001","id":"1001","text":"hello",
				"author":{"name":"Ada","screen_name":"ada","avatar_url":"https://pbs.twimg.com/a_normal.jpg","verification":{"verified":true}},
				"replies":1,"retweets":2,"likes":3,"views":4,"bookmarks":5,"created_timestamp":1792540800,
				"media":{"all":[{"type":"photo","url":"https://pbs.twimg.com/p.jpg","width":10,"height":20},
				  {"type":"video","url":"https://video.twimg.com/big.mp4","thumbnail_url":"https://pbs.twimg.com/t.jpg","duration":9.5,
				   "variants":[{"url":"https://video.twimg.com/hls.m3u8","content_type":"application/x-mpegURL"},
				     {"url":"https://video.twimg.com/small.mp4","bitrate":832000,"content_type":"video/mp4"},
				     {"url":"https://video.twimg.com/mid.mp4","bitrate":2176000,"content_type":"video/mp4"},
				     {"url":"https://video.twimg.com/big.mp4","bitrate":10368000,"content_type":"video/mp4"}]}]},
				"quote":{"id":"9","text":"quoted","author":{"name":"Bo","screen_name":"bo"}}}}`))
		case "/2/thread/1001":
			_, _ = w.Write([]byte(`{"thread":[{"id":"1001","text":"hello","author":{"screen_name":"ada"}},
				{"id":"1002","text":"and more","author":{"screen_name":"ada"}},
				{"id":"1003","text":"a reply","author":{"screen_name":"zed"}}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	old := FxTwitterBase
	FxTwitterBase = server.URL
	defer func() { FxTwitterBase = old }()

	e, err := ResolveXPost(context.Background(), "https://x.com/ada/status/1001?s=46")
	if err != nil {
		t.Fatal(err)
	}
	if e.Text != "hello" || e.AuthorHandle != "ada" || !e.AuthorVerified || e.Likes != 3 || e.Reposts != 2 ||
		e.AuthorAvatarURL != "https://pbs.twimg.com/a_200x200.jpg" {
		t.Fatalf("post: %+v", e)
	}
	if e.CreatedAt != time.Unix(1792540800, 0).UTC().Format(time.RFC3339) {
		t.Fatalf("created_at %q", e.CreatedAt)
	}
	if len(e.Media) != 2 || e.Media[0].Type != "image" || e.Media[1].URL != "https://video.twimg.com/mid.mp4" {
		t.Fatalf("media (video picks the phone-sized mp4): %+v", e.Media)
	}
	if e.Quote == nil || e.Quote.AuthorHandle != "bo" {
		t.Fatalf("quote: %+v", e.Quote)
	}
	if len(e.Thread) != 1 || e.Thread[0].Text != "and more" {
		t.Fatalf("thread keeps only the author's follow-ups: %+v", e.Thread)
	}
	if _, err := ResolveXPost(context.Background(), "https://x.com/ada/status/404404"); err == nil {
		t.Fatal("a missing post must be an error")
	}
}

// Every feed tool must register on a real MCP server: the SDK derives each
// input and output schema from the Go types and panics on a recursive type
// (a recursive Embed took production down on 2026-10-20).
func TestEveryFeedToolRegistersOnAnMCPServer(t *testing.T) {
	srv := mcp.NewServer(&mcp.Implementation{Name: "feed-test", Version: "0.1.0"}, nil)
	for _, def := range Tools(&Store{}) {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("%s: %v", def.Name(), r)
				}
			}()
			def.RegisterMCP(srv, tool.Hooks{})
		}()
	}
}
