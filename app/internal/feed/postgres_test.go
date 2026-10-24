package feed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
		return &Embed{Post: Post{Type: EmbedTypeXPost, URL: u, AuthorName: "Ada", AuthorHandle: "ada", Text: "posted",
			CreatedAt: "2026-10-20T12:00:00Z", Media: []Media{{Type: "image", URL: "https://pbs.twimg.com/a.jpg"}}}}, nil
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
	for _, kind := range []string{"stare", "upvote"} {
		if rec := post(map[string]any{"events": []map[string]any{{"event_id": "bad", "type": kind, "item_id": "x", "occurred_at": now}}}); rec.Code != 400 {
			t.Fatalf("type %s must be a 400: %d %s", kind, rec.Code, rec.Body.String())
		}
	}

	// Seen and dismissed cards leave the feed; engagement reads back.
	feed = call(t, tools, "feed_list", map[string]any{"view": "feed"})
	if got := titles(feed); got != "Agent card" {
		t.Fatalf("a seen card is still in the feed: %s", got)
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
	if got := titles(call(t, tools, "feed_list", map[string]any{"view": "feed"})); got != "" {
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

// The read-later queue: a card goes in from the phone or from an agent he
// asked, stays after it leaves the feed, and comes out on unsave.
func TestPostgresFeedSavedQueue(t *testing.T) {
	store, now := postgresStore(t)
	tools := Tools(store)
	call(t, tools, "feed_push", map[string]any{"source": "saveq", "items": []map[string]any{
		{"title": "Watch later", "url": "https://example.com/watch"},
		{"title": "Changed my mind", "url": "https://example.com/meh"},
	}})
	watch, meh := "saveq:https://example.com/watch", "saveq:https://example.com/meh"
	saved := func() []string {
		ids := []string{}
		for _, item := range call(t, tools, "feed_list", map[string]any{"view": "saved"})["items"].([]any) {
			ids = append(ids, item.(map[string]any)["item_id"].(string))
		}
		return ids
	}
	if got := saved(); len(got) != 0 {
		t.Fatalf("nothing is saved yet: %v", got)
	}
	if out := call(t, tools, "feed_save", map[string]any{"item_id": watch, "agent": "voice-memo", "reason": "Zach asked"}); out["saved"] != float64(1) {
		t.Fatalf("feed_save: %v", out)
	}
	if err := callErr(t, tools, "feed_save", map[string]any{"item_ids": []string{"saveq:nope"}}); err == nil {
		t.Fatal("saving an unknown card must fail")
	}
	mux := http.NewServeMux()
	(&handler{store: store, logger: discardLogger()}).register(mux, func(h http.Handler) http.Handler { return h })
	raw, _ := json.Marshal(map[string]any{"events": []map[string]any{
		{"event_id": "sq1", "type": "save", "item_id": meh, "occurred_at": now.Add(time.Second)},
		{"event_id": "sq2", "type": "unsave", "item_id": meh, "occurred_at": now.Add(2 * time.Second)},
	}})
	if rec := httptest.NewRecorder(); func() bool {
		mux.ServeHTTP(rec, httptest.NewRequest("POST", APIPath+"/events", bytes.NewReader(raw)))
		return rec.Code != 200
	}() {
		t.Fatalf("POST save events: %d %s", rec.Code, rec.Body.String())
	}
	if got := strings.Join(saved(), ","); got != watch {
		t.Fatalf("saved view: %s", got)
	}
	item := call(t, tools, "feed_list", map[string]any{"item_ids": []string{watch}})["items"].([]any)[0].(map[string]any)
	if e := item["engagement"].(map[string]any); e["saved"] != true || e["saved_at"] == nil || e["engaged"] != true || e["engagement_score"] != float64(3) {
		t.Fatalf("a saved card's engagement: %v", e)
	}
	// The heart: the latest of like and unlike, and it scores 5.
	raw, _ = json.Marshal(map[string]any{"events": []map[string]any{
		{"event_id": "lk1", "type": "like", "item_id": meh, "occurred_at": now.Add(3 * time.Second)},
		{"event_id": "lk2", "type": "unlike", "item_id": meh, "occurred_at": now.Add(4 * time.Second)},
		{"event_id": "lk3", "type": "like", "item_id": meh, "occurred_at": now.Add(5 * time.Second)},
	}})
	if rec := httptest.NewRecorder(); func() bool {
		mux.ServeHTTP(rec, httptest.NewRequest("POST", APIPath+"/events", bytes.NewReader(raw)))
		return rec.Code != 200
	}() {
		t.Fatalf("POST like events: %d %s", rec.Code, rec.Body.String())
	}
	liked := call(t, tools, "feed_list", map[string]any{"item_ids": []string{meh}})["items"].([]any)[0].(map[string]any)
	if e := liked["engagement"].(map[string]any); e["liked"] != true || e["liked_at"] == nil || e["engaged"] != true || e["engagement_score"] != float64(5) {
		t.Fatalf("a liked card's engagement: %v", e)
	}
	events := call(t, tools, "feed_events", map[string]any{"item_id": watch, "types": []string{"save"}})["events"].([]any)
	if meta := events[0].(map[string]any)["metadata"].(map[string]any); meta["via"] != "agent" || meta["agent"] != "voice-memo" {
		t.Fatalf("an agent's save says so: %v", meta)
	}

	// The phone gets the queue with the feed, and it outlives a removal.
	call(t, tools, "feed_remove", map[string]any{"item_id": watch, "reason": "stale"})
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, httptest.NewRequest("GET", APIPath, nil))
	var page feedResponse
	if err := json.Unmarshal(res.Body.Bytes(), &page); err != nil || len(page.Saved) != 1 || page.Saved[0].ItemID != watch {
		t.Fatalf("GET feed saved: %d %s", res.Code, res.Body.String())
	}
	store.Now = func() time.Time { return now.Add(time.Minute) }
	call(t, tools, "feed_save", map[string]any{"item_id": watch, "unsave": true})
	if got := saved(); len(got) != 0 {
		t.Fatalf("unsave empties the queue: %v", got)
	}
}

// The feed never runs dry (Zach, 2026-10-21): after the unseen cards come
// seen ones he passed over, least seen first, marked resurfaced; cards he
// engaged with, saved or dismissed never come back.
func TestPostgresFeedResurfacesSeenCardsWhenUnseenRunOut(t *testing.T) {
	store, now := postgresStore(t)
	tools := Tools(store)
	ids := map[string]string{}
	items := []map[string]any{}
	for _, name := range []string{"fresh", "glanced", "twice", "read", "saved", "dismissed"} {
		url := "https://example.com/resurface/" + name
		ids[name] = "resurf:" + url
		items = append(items, map[string]any{"title": name, "url": url})
	}
	call(t, tools, "feed_push", map[string]any{"source": "resurf", "items": items})
	at := func(d time.Duration) time.Time { return now.Add(d) }
	events := []map[string]any{
		{"event_id": "rs1", "type": "impression", "item_id": ids["glanced"], "occurred_at": at(time.Second), "dwell_ms": 400},
		{"event_id": "rs2", "type": "impression", "item_id": ids["twice"], "occurred_at": at(-time.Hour), "dwell_ms": 400},
		{"event_id": "rs3", "type": "impression", "item_id": ids["twice"], "occurred_at": at(-time.Minute), "dwell_ms": 400},
		{"event_id": "rs4", "type": "impression", "item_id": ids["read"], "occurred_at": at(time.Second), "dwell_ms": 400},
		{"event_id": "rs5", "type": "open", "item_id": ids["read"], "occurred_at": at(time.Second)},
		{"event_id": "rs6", "type": "impression", "item_id": ids["saved"], "occurred_at": at(time.Second), "dwell_ms": 400},
		{"event_id": "rs7", "type": "save", "item_id": ids["saved"], "occurred_at": at(time.Second)},
		{"event_id": "rs8", "type": "impression", "item_id": ids["dismissed"], "occurred_at": at(time.Second), "dwell_ms": 400},
		{"event_id": "rs9", "type": "dismiss", "item_id": ids["dismissed"], "occurred_at": at(time.Second)},
	}
	mux := http.NewServeMux()
	(&handler{store: store, logger: discardLogger()}).register(mux, func(h http.Handler) http.Handler { return h })
	raw, _ := json.Marshal(map[string]any{"events": events})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("POST", APIPath+"/events", bytes.NewReader(raw)))
	if rec.Code != 200 {
		t.Fatalf("POST events: %d %s", rec.Code, rec.Body.String())
	}

	res := httptest.NewRecorder()
	mux.ServeHTTP(res, httptest.NewRequest("GET", APIPath, nil))
	var page feedResponse
	if err := json.Unmarshal(res.Body.Bytes(), &page); err != nil || res.Code != 200 {
		t.Fatalf("GET feed: %d %s", res.Code, res.Body.String())
	}
	got, resurfacing := []string{}, false
	for _, item := range page.Items {
		if item.Resurfaced {
			resurfacing = true
		} else if resurfacing {
			t.Fatalf("an unseen card came after a resurfaced one: %s", item.ItemID)
		}
		if item.Source == "resurf" {
			got = append(got, fmt.Sprintf("%s:%v", item.Title, item.Resurfaced))
		}
	}
	if strings.Join(got, ",") != "fresh:false,glanced:true,twice:true" {
		t.Fatalf("resurfaced feed: %v", got)
	}
	if !strings.Contains(res.Body.String(), `"resurfaced":true`) {
		t.Fatalf("the phone is told which cards are resurfaced: %s", res.Body.String())
	}

	// A full page of unseen cards resurfaces nothing.
	page1, err := store.Feed(context.Background(), 1)
	if err != nil || len(page1) != 1 || page1[0].Resurfaced {
		t.Fatalf("limit 1: %v %v", page1, err)
	}
	call(t, tools, "feed_remove", map[string]any{"source": "resurf", "reason": "test"})
}

// The search box (Zach, voice memo 2026-10-24): every card he was shown,
// opened, saved or liked, dismissed and removed ones too; every word must
// match somewhere on the card, title matches first, then the most recently
// seen. Cards he never saw are not history.
func TestPostgresFeedSearchesEverythingHeWasShown(t *testing.T) {
	store, now := postgresStore(t)
	tools := Tools(store)
	call(t, tools, "feed_push", map[string]any{"source": "hist", "items": []map[string]any{
		{"title": "Rust compilers explained", "url": "https://example.com/hist/rust"},
		{"title": "Gardening", "body": "Why rust kills tomato compilers", "url": "https://example.com/hist/garden"},
		{"title": "Rust, never seen", "url": "https://example.com/hist/unseen"},
		{"title": "Hidden rust compilers", "url": "https://example.com/hist/hidden"},
		{"title": "100% literal", "url": "https://example.com/hist/pct"},
	}})
	id := func(name string) string { return "hist:https://example.com/hist/" + name }
	mux := http.NewServeMux()
	(&handler{store: store, logger: discardLogger()}).register(mux, func(h http.Handler) http.Handler { return h })
	raw, _ := json.Marshal(map[string]any{"events": []map[string]any{
		{"event_id": "hs1", "type": "impression", "item_id": id("rust"), "occurred_at": now.Add(-time.Hour), "dwell_ms": 400},
		{"event_id": "hs2", "type": "impression", "item_id": id("garden"), "occurred_at": now.Add(-time.Minute), "dwell_ms": 400},
		{"event_id": "hs3", "type": "impression", "item_id": id("hidden"), "occurred_at": now.Add(-2 * time.Hour), "dwell_ms": 400},
		{"event_id": "hs4", "type": "dismiss", "item_id": id("hidden"), "occurred_at": now.Add(-2 * time.Hour)},
		{"event_id": "hs5", "type": "save", "item_id": id("pct"), "occurred_at": now.Add(-3 * time.Hour)},
	}})
	if rec := httptest.NewRecorder(); func() bool {
		mux.ServeHTTP(rec, httptest.NewRequest("POST", APIPath+"/events", bytes.NewReader(raw)))
		return rec.Code != 200
	}() {
		t.Fatalf("POST events: %d %s", rec.Code, rec.Body.String())
	}
	call(t, tools, "feed_remove", map[string]any{"item_id": id("rust"), "reason": "stale"})

	search := func(q string) []string {
		t.Helper()
		res := httptest.NewRecorder()
		mux.ServeHTTP(res, httptest.NewRequest("GET", APIPath+"/search?q="+url.QueryEscape(q), nil))
		var page searchResponse
		if err := json.Unmarshal(res.Body.Bytes(), &page); err != nil || res.Code != 200 {
			t.Fatalf("GET search %q: %d %s", q, res.Code, res.Body.String())
		}
		got := []string{}
		for _, item := range page.Items {
			if item.Source == "hist" {
				got = append(got, strings.TrimPrefix(item.ItemID, id("")))
			}
		}
		return got
	}
	if got := strings.Join(search("RUST compilers"), ","); got != "rust,hidden,garden" {
		t.Fatalf("title matches first, newest seen first, then body matches: %s", got)
	}
	if got := strings.Join(search(""), ","); got != "garden,rust,hidden,pct" {
		t.Fatalf("an empty query is the whole history, newest seen first: %s", got)
	}
	if got := strings.Join(search("100%"), ","); got != "pct" {
		t.Fatalf("%% is a literal: %s", got)
	}
	if got := search("tomato nowhere"); len(got) != 0 {
		t.Fatalf("every word must match: %v", got)
	}
	call(t, tools, "feed_remove", map[string]any{"source": "hist", "reason": "test"})
}
