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
	store, now := postgresStore(t)
	tools := Tools(store)
	feedEndToEnd(t, store, tools, now)
}

// Rich cards against the provisioned schema: a media carousel and an
// embedded X post round-trip whole, and an x.com url pushed bare is embedded
// through the resolver.
func TestPostgresFeedRichCards(t *testing.T) {
	store, _ := postgresStore(t)
	tools := Tools(store)
	// Rich cards: a media carousel and an embedded X post, read back whole;
	// an x.com url pushed bare is embedded through the resolver.
	store.ResolveX = func(_ context.Context, u string) (*Embed, error) {
		return &Embed{Type: EmbedTypeXPost, URL: u, AuthorName: "Ada", AuthorHandle: "ada", Text: "posted",
			CreatedAt: "2026-10-20T12:00:00Z", Media: []Media{{Type: "image", URL: "https://pbs.twimg.com/a.jpg"}}}, nil
	}
	call(t, tools, "feed_push", map[string]any{"source": "x", "items": []map[string]any{
		{"url": "https://x.com/ada/status/123456789"},
		{"title": "Gallery", "media": []map[string]any{{"url": "https://example.com/a.jpg"}, {"type": "video", "url": "https://example.com/v.mp4", "thumbnail_url": "https://example.com/v.jpg"}}},
	}})
	store.ResolveX = nil
	rich := call(t, tools, "feed_list", map[string]any{"source": "x"})["items"].([]any)
	byTitle := map[string]map[string]any{}
	for _, raw := range rich {
		item := raw.(map[string]any)
		byTitle[item["title"].(string)] = item
	}
	xCard := byTitle[""]
	if xCard == nil || xCard["embed"].(map[string]any)["author_handle"] != "ada" || xCard["author"] != "Ada" || xCard["site"] != "X" {
		t.Fatalf("embedded X post: %v", rich)
	}
	if media := byTitle["Gallery"]["media"].([]any); len(media) != 2 || media[0].(map[string]any)["type"] != "image" ||
		media[1].(map[string]any)["thumbnail_url"] != "https://example.com/v.jpg" {
		t.Fatalf("media: %v", media)
	}
	call(t, tools, "feed_update", map[string]any{"item_id": "x:https://x.com/ada/status/123456789", "clear": []string{"embed"}})
	cleared := call(t, tools, "feed_list", map[string]any{"item_ids": []string{"x:https://x.com/ada/status/123456789"}})["items"].([]any)[0].(map[string]any)
	if _, has := cleared["embed"]; has {
		t.Fatalf("clearing embed: %v", cleared)
	}

	if _, err := store.DB.Exec(store.q("DELETE FROM @feed_items WHERE source = 'x'")); err != nil {
		t.Fatal(err)
	}
}

func postgresStore(t *testing.T) (*Store, time.Time) {
	t.Helper()
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
		for _, name := range []string{"marts_feed_item_engagement", "marts_feed_source_engagement", "feed_item_versions", "feed_items", "feed_events"} {
			q = strings.ReplaceAll(q, "@"+name, relations[name])
		}
		return q
	}
	now := time.Now().UTC().Truncate(time.Second)
	store.Now = func() time.Time { return now }
	return store, now
}

func feedEndToEnd(t *testing.T, store *Store, tools []tool.Tool, now time.Time) {
	t.Helper()

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

// Runs after the end-to-end test on the same schema, under its own source.
func TestPostgresFeedVersioning(t *testing.T) {
	store, _ := postgresStore(t)
	tools := Tools(store)
	id := "ver:a"
	history := func() []any {
		t.Helper()
		return call(t, tools, "feed_history", map[string]any{"item_id": id})["versions"].([]any)
	}
	versionOf := func(v any) map[string]any { return v.(map[string]any) }

	call(t, tools, "feed_push", map[string]any{"source": "ver", "agent": "picker", "items": []map[string]any{
		{"key": "a", "title": "One", "body": "first body", "url": "https://example.com/a", "tags": []string{"x"}},
		{"key": "b", "title": "Two"},
	}})
	unchanged := call(t, tools, "feed_push", map[string]any{"source": "ver", "items": []map[string]any{
		{"key": "a", "title": "One", "body": "first body", "url": "https://example.com/a", "tags": []string{"x"}},
	}})
	if unchanged["unchanged"] != float64(1) || len(history()) != 1 {
		t.Fatalf("a re-push that changes nothing must write no version: %v %v", unchanged, history())
	}

	// Edit every field kind, clear some, and see each step in the history.
	call(t, tools, "feed_update", map[string]any{"item_id": id, "title": "One, edited", "reason": "typo", "agent": "editor"})
	call(t, tools, "feed_update", map[string]any{"item_id": id, "clear": []string{"body", "tags", "url"}})
	if err := callErr(t, tools, "feed_update", map[string]any{"item_id": id, "title": "x", "clear": []string{"title"}}); err == nil {
		t.Fatal("setting and clearing one field must be refused")
	}
	if err := callErr(t, tools, "feed_update", map[string]any{"item_id": "ver:nope", "title": "x"}); err == nil {
		t.Fatal("editing an unknown card must fail")
	}
	call(t, tools, "feed_rank", map[string]any{"scores": map[string]float64{id: 5}, "reason": "top pick"})
	call(t, tools, "feed_remove", map[string]any{"item_id": id, "reason": "stale"})
	versions := history()
	actions := []string{}
	for _, v := range versions {
		actions = append(actions, versionOf(v)["action"].(string))
	}
	if strings.Join(actions, ",") != "remove,rank,update,update,create" {
		t.Fatalf("history newest first: %v", actions)
	}
	edit := versionOf(versions[3])
	if edit["agent"] != "editor" || edit["reason"] != "typo" || edit["actor"] == "" {
		t.Fatalf("provenance: %v", edit)
	}
	if diff := edit["diff"].(map[string]any)["title"].(map[string]any); diff["from"] != "One" || diff["to"] != "One, edited" {
		t.Fatalf("diff: %v", edit["diff"])
	}
	cleared := versionOf(versions[2])["changed_fields"].([]any)
	if len(cleared) != 3 {
		t.Fatalf("clear changed: %v", cleared)
	}
	if versionOf(versions[4])["agent"] != "picker" {
		t.Fatalf("push agent: %v", versions[4])
	}

	// get: now (removed, still readable) and as it was at version 1.
	now := call(t, tools, "feed_get", map[string]any{"item_id": id})
	item := now["item"].(map[string]any)
	if item["removed_at"] == nil || item["body"] != nil || item["version"] != float64(5) || now["versions"] != float64(5) {
		t.Fatalf("get now: %v", now)
	}
	v1 := call(t, tools, "feed_get", map[string]any{"item_id": id, "version": 1})["version"].(map[string]any)["snapshot"].(map[string]any)
	if v1["title"] != "One" || v1["body"] != "first body" {
		t.Fatalf("get v1: %v", v1)
	}

	// restore: undo the removal (content stays), then put version 1 back.
	restored := call(t, tools, "feed_restore", map[string]any{"item_id": id})
	if restored["restored"] != float64(1) {
		t.Fatalf("restore: %v", restored)
	}
	got := call(t, tools, "feed_get", map[string]any{"item_id": id})["item"].(map[string]any)
	if got["removed_at"] != nil || got["title"] != "One, edited" {
		t.Fatalf("after restore: %v", got)
	}
	call(t, tools, "feed_restore", map[string]any{"item_id": id, "version": 1, "reason": "liked the original"})
	got = call(t, tools, "feed_get", map[string]any{"item_id": id})["item"].(map[string]any)
	if got["title"] != "One" || got["body"] != "first body" || got["score"] != float64(0) || got["version"] != float64(7) {
		t.Fatalf("after restore to v1: %v", got)
	}
	latest := versionOf(history()[0])
	if latest["action"] != "restore" || latest["restored_from"] != float64(1) || latest["reason"] != "liked the original" {
		t.Fatalf("restore version: %v", latest)
	}
	if len(history()) != 7 {
		t.Fatalf("every version persists: %d", len(history()))
	}

	// Bulk by source: edit, remove, restore.
	bulk := call(t, tools, "feed_update", map[string]any{"source": "ver", "expires_at": "2d"})
	if bulk["updated"] != float64(2) {
		t.Fatalf("bulk update: %v", bulk)
	}
	if n := call(t, tools, "feed_remove", map[string]any{"source": "ver"})["removed"]; n != float64(2) {
		t.Fatalf("bulk remove: %v", n)
	}
	if n := call(t, tools, "feed_restore", map[string]any{"source": "ver"})["restored"]; n != float64(2) {
		t.Fatalf("bulk restore: %v", n)
	}

	// A card deleted outright (plain SQL) is a tombstone that restores.
	if _, err := store.DB.Exec(store.q("DELETE FROM @feed_items WHERE item_id = 'ver:b'")); err != nil {
		t.Fatal(err)
	}
	gone := call(t, tools, "feed_get", map[string]any{"item_id": "ver:b"})
	if gone["deleted"] != true || gone["version"].(map[string]any)["action"] != "delete" {
		t.Fatalf("deleted card: %v", gone)
	}
	call(t, tools, "feed_restore", map[string]any{"item_id": "ver:b"})
	back := call(t, tools, "feed_get", map[string]any{"item_id": "ver:b"})["item"].(map[string]any)
	if back["title"] != "Two" || back["removed_at"] != nil {
		t.Fatalf("restored from tombstone: %v", back)
	}
	recent := call(t, tools, "feed_history", map[string]any{"source": "ver", "limit": 3})
	if recent["count"] != float64(3) {
		t.Fatalf("source history: %v", recent)
	}
}
