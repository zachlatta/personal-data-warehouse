package feed

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func call(t *testing.T, tools []tool.Tool, name string, input any) map[string]any {
	t.Helper()
	raw, _ := json.Marshal(input)
	for _, tl := range tools {
		if tl.Name() != name {
			continue
		}
		out, isErr, err := tl.Invoke(context.Background(), raw)
		if err != nil || isErr {
			t.Fatalf("%s: %v %v", name, err, out)
		}
		data, _ := json.Marshal(out)
		var decoded map[string]any
		_ = json.Unmarshal(data, &decoded)
		return decoded
	}
	t.Fatalf("no tool %s", name)
	return nil
}

func callErr(t *testing.T, tools []tool.Tool, name string, input any) error {
	t.Helper()
	raw, _ := json.Marshal(input)
	for _, tl := range tools {
		if tl.Name() == name {
			_, _, err := tl.Invoke(context.Background(), raw)
			return err
		}
	}
	t.Fatalf("no tool %s", name)
	return nil
}

// Invoked by tests/test_feed.py against a schema the Python ensure path
// provisioned, so the Go module and feed.py cannot drift apart.
func TestPostgresFeedEndToEnd(t *testing.T) {
	databaseURL := os.Getenv("PDW_FEED_TEST_URL")
	if databaseURL == "" {
		t.Skip("run through tests/test_feed.py for managed Postgres")
	}
	var relations map[string]string
	if err := json.Unmarshal([]byte(os.Getenv("PDW_FEED_TEST_RELATIONS")), &relations); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(databaseURL, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	store.Expand = func(q string) string {
		// Longest names first: @feed_items must not eat @feed_items_x.
		for _, name := range []string{"marts_feed_item_engagement", "marts_feed_source_engagement", "feed_items", "feed_events"} {
			q = strings.ReplaceAll(q, "@"+name, relations[name])
		}
		return q
	}
	now := time.Now().UTC().Truncate(time.Second)
	store.Now = func() time.Time { return now }
	tools := Tools(store)

	// Push: a batch, idempotent on (source, key).
	pushed := call(t, tools, "feed_push", map[string]any{"source": "hn", "items": []map[string]any{
		{"title": "First", "url": "https://example.com/1", "score": 2, "tags": []string{"ai", "ai", "x\"y"}},
		{"title": "Second", "url": "https://example.com/2", "image_url": "https://example.com/2.jpg",
			"links": []map[string]any{{"url": "https://example.com/2-paper", "title": "Paper"}, {"url": "https://example.com/2"}}},
		{"source": "muse", "title": "Agent card", "body": "**why** it matters", "expires_at": "7d"},
	}})
	if pushed["created"] != float64(3) || pushed["updated"] != float64(0) {
		t.Fatalf("push: %v", pushed)
	}
	again := call(t, tools, "feed_push", map[string]any{"items": []map[string]any{
		{"source": "hn", "title": "First, edited", "url": "https://example.com/1", "tags": []string{"ai", "ai", "x\"y"}},
	}})
	if again["created"] != float64(0) || again["updated"] != float64(1) {
		t.Fatalf("re-push: %v", again)
	}
	listed := call(t, tools, "feed_list", map[string]any{"item_ids": []string{"hn:https://example.com/1"}})
	first := listed["items"].([]any)[0].(map[string]any)
	if first["title"] != "First, edited" || first["score"] != float64(2) || first["site"] != "example.com" {
		t.Fatalf("a re-push without a score must keep it: %v", first)
	}
	if tags := first["tags"].([]any); len(tags) != 2 || tags[1] != "x\"y" {
		t.Fatalf("tags: %v", tags)
	}
	if err := callErr(t, tools, "feed_push", map[string]any{"items": []map[string]any{{"title": "no source"}}}); err == nil ||
		!strings.Contains(err.Error(), "source is required") {
		t.Fatalf("missing source: %v", err)
	}

	links := call(t, tools, "feed_list", map[string]any{"item_ids": []string{"hn:https://example.com/2"}})["items"].([]any)[0].(map[string]any)["links"].([]any)
	if len(links) != 1 || links[0].(map[string]any)["title"] != "Paper" || links[0].(map[string]any)["site"] != "example.com" {
		t.Fatalf("links (the main url is not repeated): %v", links)
	}

	// Update, rank, remove.
	call(t, tools, "feed_update", map[string]any{"item_id": "hn:https://example.com/2", "title": "Second, better", "score": 1})
	if err := callErr(t, tools, "feed_update", map[string]any{"item_id": "hn:nope", "title": "x"}); err == nil {
		t.Fatal("updating an unknown item must fail")
	}
	call(t, tools, "feed_rank", map[string]any{"order": []string{"muse:" + mustKey(t, store, "muse"), "hn:https://example.com/2", "hn:https://example.com/1"}})
	feed := call(t, tools, "feed_list", map[string]any{"view": "feed"})
	if got := titles(feed); got != "Agent card,Second, better,First, edited" {
		t.Fatalf("ranked feed: %s", got)
	}

	// The phone: GET the feed and POST engagement, behind the app's auth.
	mux := http.NewServeMux()
	(&handler{store: store, logger: discardLogger()}).register(mux, func(h http.Handler) http.Handler { return h })
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, httptest.NewRequest("GET", APIPath+"?limit=2", nil))
	var page feedResponse
	if err := json.Unmarshal(res.Body.Bytes(), &page); err != nil || res.Code != 200 || len(page.Items) != 2 {
		t.Fatalf("GET feed: %d %s", res.Code, res.Body.String())
	}
	events := map[string]any{"device": "iPhone", "events": []map[string]any{
		{"event_id": "e1", "type": "session_start", "session_id": "s1", "occurred_at": now.Add(-time.Minute)},
		{"event_id": "e2", "type": "impression", "item_id": "hn:https://example.com/1", "session_id": "s1", "occurred_at": now, "dwell_ms": 300, "visible_fraction": 1, "position": 2},
		{"event_id": "e3", "type": "impression", "item_id": "hn:https://example.com/2", "session_id": "s1", "occurred_at": now, "dwell_ms": 4000, "visible_fraction": 0.9},
		{"event_id": "e4", "type": "open", "item_id": "hn:https://example.com/2", "session_id": "s1", "occurred_at": now, "metadata": map[string]any{"mode": "reader"}},
		{"event_id": "e5", "type": "close", "item_id": "hn:https://example.com/2", "session_id": "s1", "occurred_at": now, "dwell_ms": 60000},
		{"event_id": "e6", "type": "open", "item_id": "hn:https://example.com/2", "session_id": "s1", "occurred_at": now, "metadata": map[string]any{"mode": "browser", "url": "https://example.com/2-paper", "link_index": 1}},
		{"event_id": "e7", "type": "dismiss", "item_id": "hn:https://example.com/1", "session_id": "s1", "occurred_at": now, "metadata": map[string]any{"reason": "less_like_this"}},
	}}
	post := func(body any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("POST", APIPath+"/events", bytes.NewReader(raw)))
		return rec
	}
	if rec := post(events); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"accepted":7`) {
		t.Fatalf("POST events: %d %s", rec.Code, rec.Body.String())
	}
	if rec := post(events); !strings.Contains(rec.Body.String(), `"duplicates":7`) {
		t.Fatalf("a retried batch must be all duplicates: %s", rec.Body.String())
	}
	for _, kind := range []string{"stare", "like"} {
		if rec := post(map[string]any{"events": []map[string]any{{"event_id": "bad", "type": kind, "item_id": "x", "occurred_at": now}}}); rec.Code != 400 {
			t.Fatalf("type %s must be a 400: %d %s", kind, rec.Code, rec.Body.String())
		}
	}

	// Dismissed cards leave the feed; engagement reads back.
	feed = call(t, tools, "feed_list", map[string]any{"view": "feed"})
	if got := titles(feed); got != "Agent card,Second, better" {
		t.Fatalf("dismissed card still in the feed: %s", got)
	}
	second := call(t, tools, "feed_list", map[string]any{"item_ids": []string{"hn:https://example.com/2"}})["items"].([]any)[0].(map[string]any)
	engagement := second["engagement"].(map[string]any)
	// 2 opens x2 + 0.4 for 4 s on screen + 1 for a minute reading = 5.4
	if engagement["opens"] != float64(2) || engagement["links_opened"] != float64(1) || engagement["read_ms"] != float64(60000) ||
		engagement["engaged"] != true || engagement["dwell_ms"] != float64(4000) || engagement["engagement_score"] != 5.4 {
		t.Fatalf("engagement: %v", engagement)
	}
	stats := call(t, tools, "feed_stats", map[string]any{"since": "1d"})
	if byType := stats["events_by_type"].(map[string]any); byType["impression"] != float64(2) || byType["dismiss"] != float64(1) {
		t.Fatalf("events_by_type: %v", byType)
	}
	if stats["sessions"] != float64(1) || len(stats["sources"].([]any)) != 2 || len(stats["top_items"].([]any)) != 1 {
		t.Fatalf("stats: %v", stats)
	}
	got := call(t, tools, "feed_events", map[string]any{"types": []string{"dismiss"}})
	if got["count"] != float64(1) {
		t.Fatalf("events: %v", got)
	}
	ev := got["events"].([]any)[0].(map[string]any)
	if ev["device"] != "iPhone" || ev["metadata"].(map[string]any)["reason"] != "less_like_this" {
		t.Fatalf("event: %v", ev)
	}

	// Remove a whole source; re-pushing brings a card back.
	removed := call(t, tools, "feed_remove", map[string]any{"source": "muse", "reason": "test"})
	if removed["removed"] != float64(1) {
		t.Fatalf("remove: %v", removed)
	}
	if got := titles(call(t, tools, "feed_list", map[string]any{"view": "feed"})); got != "Second, better" {
		t.Fatalf("after remove: %s", got)
	}
	call(t, tools, "feed_push", map[string]any{"items": []map[string]any{{"source": "muse", "title": "Agent card", "body": "**why** it matters"}}})
	if got := titles(call(t, tools, "feed_list", map[string]any{"source": "muse"})); got != "Agent card" {
		t.Fatalf("re-push did not revive: %s", got)
	}
}

func mustKey(t *testing.T, store *Store, source string) string {
	t.Helper()
	items, err := store.List(context.Background(), ListOptions{Source: source, Limit: 1})
	if err != nil || len(items) != 1 {
		t.Fatalf("key for %s: %v %v", source, items, err)
	}
	return items[0].Key
}

func titles(out map[string]any) string {
	parts := []string{}
	for _, item := range out["items"].([]any) {
		parts = append(parts, item.(map[string]any)["title"].(string))
	}
	return strings.Join(parts, ",")
}
