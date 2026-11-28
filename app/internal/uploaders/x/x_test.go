package x_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium"
	"github.com/zachlatta/personal-data-warehouse/app/internal/browsersessions/chromium/chromiumtest"
	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	x "github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/x"
)

// Every value here is synthetic: no real account, post or token.
const (
	myID       = "100"
	myHandle   = "zachlatta"
	fakeAuth   = "fake-auth-token"
	fakeCSRF   = "fake-csrf-token"
	fakeBearer = "AAAAAAAAAAAAAAAAAAAAAFAKEBEARER%3DTESTONLY"
)

func cookieHost(t *testing.T, browserKey string, rows []chromiumtest.Row) chromium.Host {
	t.Helper()
	key := chromiumtest.Key("pw")
	support := t.TempDir()
	profile, _ := chromium.BrowserByKey(browserKey)
	dir := filepath.Join(support, profile.SupportSubdir, "Default")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	chromiumtest.WriteCookieDB(t, filepath.Join(dir, "Cookies"), key, rows)
	return chromium.Host{ApplicationSupport: support, KeychainPassword: func(string, string) (string, error) { return "pw", nil }}
}

func TestCaptureReadsTheXLoginAndTheSignedInUserID(t *testing.T) {
	host := cookieHost(t, "chrome", []chromiumtest.Row{
		{HostKey: ".x.com", Name: "auth_token", Value: fakeAuth},
		{HostKey: ".x.com", Name: "ct0", Value: fakeCSRF},
		{HostKey: ".x.com", Name: "twid", Value: "u%3D" + myID},
		// Another site whose name ends in "x.com" never contributes a cookie.
		{HostKey: ".notx.com", Name: "auth_token", Value: "other-site"},
	})
	session, err := x.Capture(host, "")
	if err != nil {
		t.Fatal(err)
	}
	if session.Browser != "Google Chrome" || session.UserID != myID || session.CSRFToken() != fakeCSRF {
		t.Fatalf("session = %+v", session)
	}
	if got := session.CookieHeader(); got != "auth_token="+fakeAuth+"; ct0="+fakeCSRF+"; twid=u%3D"+myID {
		t.Fatalf("cookie header = %q", got)
	}
	if strings.Contains(session.Fingerprint(), fakeAuth) || len(session.Fingerprint()) != 16 {
		t.Fatalf("fingerprint = %q", session.Fingerprint())
	}
}

func TestCaptureRefusesABrowserWithoutTheLogin(t *testing.T) {
	host := cookieHost(t, "chrome", []chromiumtest.Row{{HostKey: ".x.com", Name: "guest_id", Value: "v1"}})
	var capture *x.CaptureError
	if _, err := x.Capture(host, "chrome"); !errors.As(err, &capture) || !strings.Contains(err.Error(), "sign in to x.com") {
		t.Fatalf("err = %v", err)
	}
}

func TestUserIDFromTwid(t *testing.T) {
	for raw, want := range map[string]string{"u%3D12345": "12345", "u=12345": "12345", `"u=7"`: "7", "u%3Dabc": "", "": ""} {
		if got := x.UserIDFromTwid(raw); got != want {
			t.Fatalf("%q -> %q, want %q", raw, got, want)
		}
	}
}

func TestParseOperationsReadsQueryIDsAndSwitches(t *testing.T) {
	bundle := `e.exports={queryId:"abc123",operationName:"Likes",operationType:"query",metadata:{featureSwitches:["f_one","f_two"],fieldToggles:["t_one"]}}` +
		`;e.exports={queryId:"def456",operationName:"CreateBookmark",operationType:"mutation",metadata:{featureSwitches:[],fieldToggles:[]}}`
	ops := x.ParseOperations(bundle)
	likes := ops["Likes"]
	if likes.QueryID != "abc123" || strings.Join(likes.Features, ",") != "f_one,f_two" || strings.Join(likes.FieldToggles, ",") != "t_one" {
		t.Fatalf("Likes = %+v", likes)
	}
	if ops["CreateBookmark"].QueryID != "def456" {
		t.Fatalf("ops = %+v", ops)
	}
}

// --- synthetic GraphQL shapes ------------------------------------------------

func user(id, handle string) map[string]any {
	return map[string]any{"result": map[string]any{
		"__typename": "User", "rest_id": id,
		"core": map[string]any{"screen_name": handle, "name": strings.ToUpper(handle[:1]) + handle[1:]},
	}}
}

func tweet(id, authorID, handle, text string, extra map[string]any) map[string]any {
	legacy := map[string]any{
		"id_str": id, "full_text": text, "created_at": "Fri Nov 27 12:00:00 +0000 2026",
		"conversation_id_str": id, "favorite_count": 5.0, "retweet_count": 1.0, "reply_count": 2.0,
		"quote_count": 0.0, "bookmark_count": 1.0, "lang": "en", "user_id_str": authorID,
		"entities": map[string]any{"user_mentions": []any{}},
	}
	result := map[string]any{
		"__typename": "Tweet", "rest_id": id, "core": map[string]any{"user_results": user(authorID, handle)},
		"legacy": legacy, "views": map[string]any{"count": "321", "state": "EnabledWithCount"},
	}
	for k, v := range extra {
		if strings.HasPrefix(k, "legacy.") {
			legacy[strings.TrimPrefix(k, "legacy.")] = v
		} else {
			result[k] = v
		}
	}
	return result
}

func itemEntry(sortIndex string, result map[string]any) map[string]any {
	return map[string]any{
		"entryId": "tweet-" + fmt.Sprint(result["rest_id"]), "sortIndex": sortIndex,
		"content": map[string]any{
			"entryType":   "TimelineTimelineItem",
			"itemContent": map[string]any{"itemType": "TimelineTweet", "tweet_results": map[string]any{"result": result}},
		},
	}
}

func moduleEntry(sortIndex string, results ...map[string]any) map[string]any {
	var items []any
	for _, r := range results {
		items = append(items, map[string]any{"item": map[string]any{"itemContent": map[string]any{
			"itemType": "TimelineTweet", "tweet_results": map[string]any{"result": r},
		}}})
	}
	return map[string]any{
		"entryId": "profile-conversation-" + sortIndex, "sortIndex": sortIndex,
		"content": map[string]any{"entryType": "TimelineTimelineModule", "items": items},
	}
}

func cursorEntry(kind, value string) map[string]any {
	return map[string]any{
		"entryId": "cursor-" + strings.ToLower(kind), "sortIndex": "1",
		"content": map[string]any{"entryType": "TimelineTimelineCursor", "cursorType": kind, "value": value},
	}
}

// profileTimeline wraps entries the way UserTweetsAndReplies and Likes do.
func profileTimeline(entries ...map[string]any) map[string]any {
	var list []any
	for _, e := range entries {
		list = append(list, e)
	}
	return map[string]any{"user": map[string]any{"result": map[string]any{"timeline": map[string]any{"timeline": map[string]any{
		"instructions": []any{
			map[string]any{"type": "TimelineClearCache"},
			map[string]any{"type": "TimelineAddEntries", "entries": list},
		},
	}}}}}
}

// bookmarksTimeline is the bookmarks shape: a different path to the same instructions.
func bookmarksTimeline(entries ...map[string]any) map[string]any {
	var list []any
	for _, e := range entries {
		list = append(list, e)
	}
	return map[string]any{"bookmark_timeline_v2": map[string]any{"timeline": map[string]any{
		"instructions": []any{map[string]any{"type": "TimelineAddEntries", "entries": list}},
	}}}
}

func roundTrip(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestParsePageReadsItemsModulesContextAndTheBottomCursor(t *testing.T) {
	parent := tweet("200", "300", "someone", "the post he replied to", nil)
	reply := tweet("201", myID, myHandle, "@someone agreed", map[string]any{
		"legacy.in_reply_to_status_id_str": "200", "legacy.in_reply_to_user_id_str": "300",
		"legacy.in_reply_to_screen_name": "someone", "legacy.conversation_id_str": "200",
	})
	original := tweet("202", "301", "writer", "a thread about databases", nil)
	repost := tweet("203", myID, myHandle, "RT @writer: a thread about datab…", map[string]any{
		"legacy.retweeted_status_result": map[string]any{"result": original},
	})
	quoted := tweet("204", "302", "quoted", "the quoted post", nil)
	quote := tweet("205", myID, myHandle, "worth reading", map[string]any{
		"legacy.is_quote_status": true, "legacy.quoted_status_id_str": "204",
		"quoted_status_result": map[string]any{"result": quoted},
		"note_tweet":           map[string]any{"note_tweet_results": map[string]any{"result": map[string]any{"text": "worth reading, the long version"}}},
	})
	hidden := map[string]any{"__typename": "TweetWithVisibilityResults", "tweet": tweet("206", myID, myHandle, "limited", nil)}
	tombstone := map[string]any{"__typename": "TweetTombstone"}
	data := roundTrip(t, profileTimeline(
		moduleEntry("9000", parent, reply),
		itemEntry("8000", repost),
		itemEntry("7000", quote),
		itemEntry("6000", hidden),
		itemEntry("5000", tombstone),
		cursorEntry("Top", "top-cursor"),
		cursorEntry("Bottom", "next-cursor"),
	))
	page := x.ParsePage(data)
	if page.BottomCursor != "next-cursor" {
		t.Fatalf("cursor = %q", page.BottomCursor)
	}
	if len(page.Entries) != 4 || page.PostCount() != 5 {
		t.Fatalf("entries = %d posts = %d", len(page.Entries), page.PostCount())
	}
	module := page.Entries[0]
	if module.SortIndex != 9000 || module.Primary[0].PostID != "200" || module.Primary[1].PostID != "201" {
		t.Fatalf("module = %+v", module)
	}
	got := module.Primary[1]
	if got.InReplyToPostID != "200" || got.InReplyToScreenName != "someone" || got.ConversationID != "200" ||
		got.AuthorScreenName != myHandle || got.LikeCount != 5 || got.ViewCount != 321 ||
		!got.CreatedAt.Equal(time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("reply = %+v", got)
	}
	rt := page.Entries[1]
	if !rt.Primary[0].IsRetweet || rt.Primary[0].RetweetedPostID != "202" ||
		rt.Primary[0].Text != "RT @writer: a thread about databases" || rt.Context[0].PostID != "202" {
		t.Fatalf("repost = %+v context %+v", rt.Primary[0], rt.Context)
	}
	q := page.Entries[2]
	if !q.Primary[0].IsQuote || q.Primary[0].QuotedPostID != "204" || q.Primary[0].Text != "worth reading, the long version" ||
		len(q.Context) != 1 || q.Context[0].PostID != "204" {
		t.Fatalf("quote = %+v", q)
	}
	if page.Entries[3].Primary[0].PostID != "206" {
		t.Fatalf("visibility wrapper = %+v", page.Entries[3])
	}
	if got.URL() != "https://x.com/zachlatta/status/201" {
		t.Fatalf("url = %q", got.URL())
	}
}

func TestMentionsMatchesByIDOrHandle(t *testing.T) {
	p := x.Post{MentionedUserIDs: []string{"9"}, MentionedScreenNames: []string{"ZachLatta"}}
	if !p.Mentions(myID, myHandle) || !p.Mentions("9", "") || p.Mentions("8", "other") {
		t.Fatal("mention matching")
	}
}

// --- the client against a fake x.com -----------------------------------------

type fakeX struct {
	t        *testing.T
	server   *httptest.Server
	requests []*http.Request
	// pages maps "<operation>|<cursor>" to a response's data.
	pages  map[string]map[string]any
	status map[string]int
	// homeStatus / homeRedirect make /home refuse or bounce to a login page.
	homeStatus   int
	homeRedirect string
}

func newFakeX(t *testing.T) *fakeX {
	f := &fakeX{t: t, pages: map[string]map[string]any{}, status: map[string]int{}}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeX) serve(w http.ResponseWriter, r *http.Request) {
	f.requests = append(f.requests, r)
	switch {
	case r.URL.Path == "/home" && f.homeStatus != 0:
		w.WriteHeader(f.homeStatus)
	case r.URL.Path == "/home" && f.homeRedirect != "":
		http.Redirect(w, r, f.homeRedirect, http.StatusFound)
	case r.URL.Path == "/i/flow/login":
		fmt.Fprint(w, `<html>Sign in to X</html>`)
	case r.URL.Path == "/home":
		fmt.Fprintf(w, `<html><script src="%s/responsive-web/client-web/main.abc123.js"></script>`+
			`<script src="https://evil.example/responsive-web/client-web/main.evil.js"></script></html>`, f.server.URL)
	case r.URL.Path == "/responsive-web/client-web/main.abc123.js":
		fmt.Fprintf(w, `var a="Bearer %s";e.exports={queryId:"live-likes-id",operationName:"Likes",operationType:"query",metadata:{featureSwitches:["live_feature"],fieldToggles:["live_toggle"]}}`, fakeBearer)
	case strings.HasPrefix(r.URL.Path, "/i/api/graphql/"):
		parts := strings.Split(r.URL.Path, "/")
		operation := parts[len(parts)-1]
		var vars map[string]any
		_ = json.Unmarshal([]byte(r.URL.Query().Get("variables")), &vars)
		cursor, _ := vars["cursor"].(string)
		key := operation + "|" + cursor
		if code := f.status[key]; code != 0 {
			w.Header().Set("x-rate-limit-reset", "1900000000")
			w.WriteHeader(code)
			fmt.Fprint(w, `{"errors":[{"message":"synthetic failure"}]}`)
			return
		}
		data, ok := f.pages[key]
		if !ok {
			data = profileTimeline()
		}
		w.Header().Set("x-rate-limit-remaining", "50")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	default:
		http.NotFound(w, r)
	}
}

func testSession() x.Session {
	return x.Session{Browser: "Google Chrome", UserID: myID, Cookies: map[string]string{"auth_token": fakeAuth, "ct0": fakeCSRF}}
}

func TestClientDiscoversTheLiveClientAndSendsTheSessionOnlyToXCom(t *testing.T) {
	f := newFakeX(t)
	f.pages["Likes|"] = profileTimeline(itemEntry("10", tweet("1", "2", "a", "liked", nil)))
	c := x.NewClient(testSession(), f.server.URL, "")
	if err := c.Discover(); err != nil {
		t.Fatal(err)
	}
	data, err := c.Query("Likes", map[string]any{"userId": myID})
	if err != nil {
		t.Fatal(err)
	}
	if x.ParsePage(data).PostCount() != 1 || c.RemainingRequests() != 50 {
		t.Fatalf("data = %v", data)
	}
	last := f.requests[len(f.requests)-1]
	if !strings.HasPrefix(last.URL.Path, "/i/api/graphql/live-likes-id/Likes") {
		t.Fatalf("the live query id must win over the fallback: %s", last.URL.Path)
	}
	bearer, _ := url.QueryUnescape(fakeBearer)
	if last.Header.Get("Authorization") != "Bearer "+bearer || last.Header.Get("x-csrf-token") != fakeCSRF ||
		!strings.Contains(last.Header.Get("Cookie"), "auth_token="+fakeAuth) {
		t.Fatalf("headers = %v", last.Header)
	}
	if !strings.Contains(last.URL.Query().Get("features"), `"live_feature":true`) {
		t.Fatalf("features = %s", last.URL.Query().Get("features"))
	}
	for _, r := range f.requests {
		if strings.Contains(r.Host, "evil") {
			t.Fatal("fetched a bundle from a host other than X's")
		}
	}
	// An operation the live bundle did not name falls back to the built-in id.
	if _, err := c.Query("Bookmarks", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.requests[len(f.requests)-1].URL.Path, "/Bookmarks") {
		t.Fatal("bookmarks fallback")
	}
}

func TestClientMapsRateLimitsAndRejectedSessions(t *testing.T) {
	f := newFakeX(t)
	f.status["Likes|"] = http.StatusTooManyRequests
	f.status["Bookmarks|"] = http.StatusUnauthorized
	c := x.NewClient(testSession(), f.server.URL, "")
	if err := c.Discover(); err != nil {
		t.Fatal(err)
	}
	var limit *x.RateLimitError
	if _, err := c.Query("Likes", map[string]any{}); !errors.As(err, &limit) || limit.Reset.Unix() != 1900000000 {
		t.Fatalf("err = %v", err)
	}
	var auth *x.AuthError
	if _, err := c.Query("Bookmarks", map[string]any{}); !errors.As(err, &auth) || auth.Status != 401 {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(auth.Error(), fakeAuth) || strings.Contains(auth.Error(), fakeCSRF) {
		t.Fatal("an error must never carry the session")
	}
}

// --- the runner -------------------------------------------------------------

type recordingUploader struct{ batches [][]map[string]any }

func (u *recordingUploader) UploadXBatch(body []byte, _ string) (ingestclient.StoredObject, error) {
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return ingestclient.StoredObject{}, err
	}
	raw, _ := io.ReadAll(reader)
	var batch []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return ingestclient.StoredObject{}, err
		}
		batch = append(batch, record)
	}
	u.batches = append(u.batches, batch)
	return ingestclient.StoredObject{StorageKey: fmt.Sprintf("batch-%d", len(u.batches))}, nil
}

func (u *recordingUploader) relations() map[string]string {
	out := map[string]string{}
	for _, batch := range u.batches {
		for _, record := range batch {
			if record["record_type"] == "relation" {
				rec := record["record"].(map[string]any)
				out[rec["relation"].(string)+"|"+rec["post_id"].(string)] = rec["discovered_at"].(string)
			}
		}
	}
	return out
}

func (u *recordingUploader) posts() map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, batch := range u.batches {
		for _, record := range batch {
			if record["record_type"] == "post" {
				rec := record["record"].(map[string]any)
				out[rec["post_id"].(string)] = rec
			}
		}
	}
	return out
}

type fakeAPI struct {
	pages     map[string]map[string]any
	errs      map[string]error
	calls     []string
	remaining int
}

func (f *fakeAPI) Discover() error { return nil }
func (f *fakeAPI) RemainingRequests() int {
	return f.remaining
}
func (f *fakeAPI) Query(operation string, vars map[string]any) (map[string]any, error) {
	cursor, _ := vars["cursor"].(string)
	key := operation + "|" + cursor
	f.calls = append(f.calls, key)
	if err := f.errs[key]; err != nil {
		return nil, err
	}
	if page, ok := f.pages[key]; ok {
		return page, nil
	}
	return profileTimeline(), nil
}

func openState(t *testing.T) *x.State {
	t.Helper()
	state, err := x.OpenState(filepath.Join(t.TempDir(), "state.sqlite"), myHandle)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	return state
}

func TestRunnerRecordsEachStreamsRelationOnlyForThePostsItOwns(t *testing.T) {
	parent := tweet("200", "300", "someone", "the post he replied to", nil)
	reply := tweet("201", myID, myHandle, "@someone agreed", map[string]any{
		"legacy.created_at": "Fri Nov 27 13:00:00 +0000 2026", "legacy.in_reply_to_status_id_str": "200",
	})
	older := tweet("199", myID, myHandle, "an older post", map[string]any{"legacy.created_at": "Thu Nov 26 09:00:00 +0000 2026"})
	selfMention := tweet("210", myID, myHandle, "replying in my mentions", nil)
	mention := tweet("211", "400", "fan", "@zachlatta nice", map[string]any{
		"legacy.entities": map[string]any{"user_mentions": []any{map[string]any{"id_str": myID, "screen_name": myHandle}}},
	})
	api := &fakeAPI{remaining: -1, pages: map[string]map[string]any{
		"UserTweetsAndReplies|": roundTrip(t, profileTimeline(moduleEntry("90", parent, reply), itemEntry("80", older))),
		"Likes|":                roundTrip(t, profileTimeline(itemEntry("70", tweet("220", "500", "writer", "liked post", nil)))),
		"Bookmarks|":            roundTrip(t, bookmarksTimeline(itemEntry("60", tweet("230", "501", "author", "bookmarked post", nil)))),
		"NotificationsTimeline|": roundTrip(t, map[string]any{"viewer_v2": map[string]any{"user_results": map[string]any{"result": map[string]any{
			"notification_timeline": map[string]any{"timeline": map[string]any{"instructions": []any{map[string]any{
				"type": "TimelineAddEntries", "entries": []any{itemEntry("50", mention), itemEntry("49", selfMention)},
			}}}},
		}}}}),
	}}
	uploader := &recordingUploader{}
	var observed []ingestclient.SourceObservation
	now := time.Date(2026, 11, 27, 14, 0, 0, 0, time.UTC)
	summary, err := (&x.Runner{
		Account: myHandle, UserID: myID, API: api, Client: uploader, State: openState(t),
		Logger: common.NewWriterLogger(io.Discard), Now: func() time.Time { return now },
		Observe: func(o ingestclient.SourceObservation) (map[string]any, error) {
			observed = append(observed, o)
			return nil, nil
		},
	}).Sync()
	if err != nil {
		t.Fatal(err)
	}
	rels := uploader.relations()
	want := []string{"authored|201", "authored|199", "liked|220", "bookmarked|230", "mentioned|211"}
	if len(rels) != len(want) {
		t.Fatalf("relations = %v", rels)
	}
	for _, key := range want {
		if _, ok := rels[key]; !ok {
			t.Fatalf("missing %s in %v", key, rels)
		}
	}
	posts := uploader.posts()
	// The post he replied to rides along without a relation; the mention says so.
	if _, ok := posts["200"]; !ok || posts["211"]["mentions_account"] != true || posts["220"]["mentions_account"] != false {
		t.Fatalf("posts = %v", posts)
	}
	if summary.Batches != 4 || summary.Pages != 4 {
		t.Fatalf("summary = %+v", summary)
	}
	if len(observed) != 1 || observed[0].Pipeline != "x" || observed[0].Scope != myHandle ||
		observed[0].NewestID != "201" || observed[0].NewestAt != "2026-11-27T13:00:00+00:00" {
		t.Fatalf("observed = %+v", observed)
	}
	for _, batch := range uploader.batches {
		for _, record := range batch {
			if record["account"] != myHandle || record["source"] != "x" {
				t.Fatalf("envelope = %v", record)
			}
		}
	}
}

func TestRunnerCatchesUpThenBackfillsFromWhereItStoppedAndKeepsFirstSightings(t *testing.T) {
	page := func(cursor string, ids ...string) map[string]any {
		var entries []map[string]any
		for _, id := range ids {
			entries = append(entries, itemEntry(id, tweet(id, "500", "writer", "post "+id, nil)))
		}
		if cursor != "" {
			entries = append(entries, cursorEntry("Bottom", cursor))
		}
		return roundTrip(t, profileTimeline(entries...))
	}
	api := &fakeAPI{remaining: -1, pages: map[string]map[string]any{
		"Likes|":   page("c1", "10", "9"),
		"Likes|c1": page("c2", "8", "7"),
		"Likes|c2": page("c3", "6"),
		"Likes|c3": page("c3"),
	}}
	likes, _ := x.StreamByName("likes")
	state := openState(t)
	uploader := &recordingUploader{}
	first := time.Date(2026, 11, 27, 14, 0, 0, 0, time.UTC)
	runner := func(now time.Time) *x.Runner {
		return &x.Runner{
			Account: myHandle, UserID: myID, API: api, Client: uploader, State: state, Streams: []x.Stream{likes},
			Logger: common.NewWriterLogger(io.Discard), Now: func() time.Time { return now }, Pages: 1, BackfillPages: 1,
		}
	}
	if _, err := runner(first).Sync(); err != nil {
		t.Fatal(err)
	}
	// One catch-up page, then one backfill page starting at the catch-up's cursor.
	if strings.Join(api.calls, ",") != "Likes|,Likes|c1" {
		t.Fatalf("calls = %v", api.calls)
	}
	api.calls = nil
	second := first.Add(30 * time.Minute)
	if _, err := runner(second).Sync(); err != nil {
		t.Fatal(err)
	}
	// Nothing new at the top, so the catch-up stops; the backfill resumes at c2.
	if strings.Join(api.calls, ",") != "Likes|,Likes|c2" {
		t.Fatalf("calls = %v", api.calls)
	}
	rels := uploader.relations()
	if rels["liked|10"] != "2026-11-27T14:00:00+00:00" || rels["liked|6"] != "2026-11-27T14:30:00+00:00" {
		t.Fatalf("a re-sent relation must keep its first sighting: %v", rels)
	}
	api.calls = nil
	if _, err := runner(second.Add(time.Hour)).Sync(); err != nil {
		t.Fatal(err)
	}
	// c3 answers no posts: the walk is done and never repeats.
	api.calls = nil
	if _, err := runner(second.Add(2 * time.Hour)).Sync(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(api.calls, ",") != "Likes|" {
		t.Fatalf("a finished backfill must not walk again: %v", api.calls)
	}
}

func TestARateLimitStopsOneStreamAndARejectedSessionStopsTheRun(t *testing.T) {
	api := &fakeAPI{remaining: -1, errs: map[string]error{"Likes|": &x.RateLimitError{}}}
	uploader := &recordingUploader{}
	summary, err := (&x.Runner{
		Account: myHandle, UserID: myID, API: api, Client: uploader, Logger: common.NewWriterLogger(io.Discard),
	}).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(summary.RateLimited, ",") != "likes" || len(api.calls) != 4 {
		t.Fatalf("summary = %+v calls %v", summary, api.calls)
	}
	api = &fakeAPI{remaining: -1, errs: map[string]error{"UserTweetsAndReplies|": &x.AuthError{Status: 401}}}
	_, err = (&x.Runner{Account: myHandle, UserID: myID, API: api, Client: uploader, Logger: common.NewWriterLogger(io.Discard)}).Sync()
	var auth *x.AuthError
	if !errors.As(err, &auth) || len(api.calls) != 1 {
		t.Fatalf("err = %v calls %v", err, api.calls)
	}
	api = &fakeAPI{remaining: -1, errs: map[string]error{"Bookmarks|": errors.New("HTTP 404")}}
	summary, err = (&x.Runner{Account: myHandle, UserID: myID, API: api, Client: uploader, Logger: common.NewWriterLogger(io.Discard)}).Sync()
	if !errors.Is(err, x.ErrStreamsFailed) || len(summary.Failed) != 1 || len(api.calls) != 4 {
		t.Fatalf("a failed stream fails the run after the others ran: %v %+v", err, summary)
	}
}

func TestRunWithRefusesAnotherAccountsSession(t *testing.T) {
	f := newFakeX(t)
	f.pages["UserByScreenName|"] = map[string]any{"user": user("999", myHandle)}
	env := map[string]string{"X_ACCOUNT": "@ZachLatta", "PDW_INGEST_PROJECT_DIR": t.TempDir()}
	var stdout, stderr bytes.Buffer
	deps := x.Deps{
		Capture: func(string) (x.Session, error) { return testSession(), nil },
		NewAPI:  func(s x.Session, _ common.Getenv) x.API { return x.NewClient(s, f.server.URL, "") },
	}
	stateDir := t.TempDir()
	code := x.RunWith([]string{"--dry-run", "--state-file", filepath.Join(stateDir, "s.sqlite"), "--lock-file", filepath.Join(stateDir, "lock")},
		&stdout, &stderr, func(k string) string { return env[k] }, ingestclient.Config{}, deps)
	if code != 1 || !strings.Contains(stderr.String(), "different X account") {
		t.Fatalf("code %d stderr %q", code, stderr.String())
	}
	if strings.Contains(stdout.String()+stderr.String(), fakeAuth) {
		t.Fatal("the cookie reached the output")
	}

	// The right account dry-runs every stream and uploads nothing.
	f.pages["UserByScreenName|"] = map[string]any{"user": user(myID, myHandle)}
	env["X_REQUEST_DELAY_MS"] = "0"
	stdout.Reset()
	stderr.Reset()
	code = x.RunWith([]string{"--dry-run", "--state-file", filepath.Join(stateDir, "s.sqlite"), "--lock-file", filepath.Join(stateDir, "lock")},
		&stdout, &stderr, func(k string) string { return env[k] }, ingestclient.Config{}, deps)
	if code != 0 || !strings.Contains(stdout.String(), "dry_run=true") {
		t.Fatalf("code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(filepath.Join(stateDir, "s.sqlite")); !os.IsNotExist(err) {
		t.Fatal("a dry run must not create state")
	}
}

func TestRunWithNeedsTheAccount(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := map[string]string{"PDW_INGEST_PROJECT_DIR": t.TempDir()}
	code := x.RunWith(nil, &stdout, &stderr, func(k string) string { return env[k] }, ingestclient.Config{}, x.Deps{})
	if code != 1 || !strings.Contains(stderr.String(), "X_ACCOUNT") {
		t.Fatalf("code %d stderr %q", code, stderr.String())
	}
}

// likesPages builds a Likes timeline of `pages` pages, newest first, ids
// counting down from top, `per` posts a page; the last page has no cursor.
func likesPages(t *testing.T, top, pages, per int) map[string]map[string]any {
	out := map[string]map[string]any{}
	id := top
	for p := 0; p < pages; p++ {
		var entries []map[string]any
		for i := 0; i < per; i++ {
			entries = append(entries, itemEntry(fmt.Sprint(id), tweet(fmt.Sprint(id), "500", "writer", "post", nil)))
			id--
		}
		if p < pages-1 {
			entries = append(entries, cursorEntry("Bottom", fmt.Sprintf("c%d", p+1)))
		}
		key := "Likes|"
		if p > 0 {
			key = fmt.Sprintf("Likes|c%d", p)
		}
		out[key] = roundTrip(t, profileTimeline(entries...))
	}
	return out
}

func TestAnOutageLongerThanThePageBudgetIsCaughtUpOverLaterRuns(t *testing.T) {
	likes, _ := x.StreamByName("likes")
	state := openState(t)
	now := time.Date(2026, 11, 27, 14, 0, 0, 0, time.UTC)
	// The history walk is finished and posts 1..2 were shipped long ago.
	if err := state.SetBackfill("likes", "", true, now); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"1", "2"} {
		if err := state.MarkDiscovered("likes", id, now); err != nil {
			t.Fatal(err)
		}
	}
	// Then an outage: 10 new likes land on top, 2 a page; the old ones follow.
	pages := likesPages(t, 12, 6, 2)
	api := &fakeAPI{remaining: -1, pages: pages}
	uploader := &recordingUploader{}
	run := func() {
		t.Helper()
		if _, err := (&x.Runner{
			Account: myHandle, UserID: myID, API: api, Client: uploader, State: state, Streams: []x.Stream{likes},
			Logger: common.NewWriterLogger(io.Discard), Now: func() time.Time { return now }, Pages: 2, BackfillPages: 5,
		}).Sync(); err != nil {
			t.Fatal(err)
		}
	}
	run()
	if got := len(uploader.relations()); got != 4 {
		t.Fatalf("first run ships the page budget: %d", got)
	}
	run()
	run()
	rels := uploader.relations()
	for id := 3; id <= 12; id++ {
		if _, ok := rels[fmt.Sprintf("liked|%d", id)]; !ok {
			t.Fatalf("liked|%d was never shipped: %v", id, rels)
		}
	}
	// Caught up: the next run reads the top page only.
	api.calls = nil
	run()
	if strings.Join(api.calls, ",") != "Likes|" {
		t.Fatalf("calls after catching up = %v", api.calls)
	}
}

func TestARateLimitMidCatchUpResumesWhereItStopped(t *testing.T) {
	likes, _ := x.StreamByName("likes")
	state := openState(t)
	now := time.Date(2026, 11, 27, 14, 0, 0, 0, time.UTC)
	if err := state.SetBackfill("likes", "", true, now); err != nil {
		t.Fatal(err)
	}
	if err := state.MarkDiscovered("likes", "1", now); err != nil {
		t.Fatal(err)
	}
	api := &fakeAPI{remaining: -1, pages: likesPages(t, 7, 4, 2), errs: map[string]error{"Likes|c1": &x.RateLimitError{}}}
	uploader := &recordingUploader{}
	runner := func() *x.Runner {
		return &x.Runner{
			Account: myHandle, UserID: myID, API: api, Client: uploader, State: state, Streams: []x.Stream{likes},
			Logger: common.NewWriterLogger(io.Discard), Now: func() time.Time { return now }, Pages: 3,
		}
	}
	if _, err := runner().Sync(); err != nil {
		t.Fatal(err)
	}
	delete(api.errs, "Likes|c1")
	if _, err := runner().Sync(); err != nil {
		t.Fatal(err)
	}
	rels := uploader.relations()
	for id := 2; id <= 7; id++ {
		if _, ok := rels[fmt.Sprintf("liked|%d", id)]; !ok {
			t.Fatalf("liked|%d lost to the rate limit: %v", id, rels)
		}
	}
}

func TestAHistoryPageOfOnlyDeletedPostsDoesNotEndTheWalk(t *testing.T) {
	likes, _ := x.StreamByName("likes")
	tombstones := roundTrip(t, profileTimeline(
		itemEntry("5", map[string]any{"__typename": "TweetTombstone", "rest_id": "5"}),
		cursorEntry("Bottom", "c2"),
	))
	api := &fakeAPI{remaining: -1, pages: map[string]map[string]any{
		"Likes|":   roundTrip(t, profileTimeline(itemEntry("9", tweet("9", "500", "w", "p", nil)), cursorEntry("Bottom", "c1"))),
		"Likes|c1": tombstones,
		"Likes|c2": roundTrip(t, profileTimeline(itemEntry("3", tweet("3", "500", "w", "older", nil)))),
	}}
	uploader := &recordingUploader{}
	if _, err := (&x.Runner{
		Account: myHandle, UserID: myID, API: api, Client: uploader, State: openState(t), Streams: []x.Stream{likes},
		Logger: common.NewWriterLogger(io.Discard), Pages: 1, BackfillPages: 3,
	}).Sync(); err != nil {
		t.Fatal(err)
	}
	if _, ok := uploader.relations()["liked|3"]; !ok {
		t.Fatalf("the walk stopped at the deleted posts: calls %v", api.calls)
	}
}

// --- pacing -----------------------------------------------------------------

func seededPacer(seed uint64, base time.Duration) *x.Pacer {
	return &x.Pacer{Rand: rand.New(rand.NewPCG(seed, seed)), Base: base, SkipChance: 0.1, PauseChance: 0.1}
}

func TestPacerGapsAreJitteredWithOccasionalReadingPauses(t *testing.T) {
	p := seededPacer(1, 3*time.Second)
	seen := map[time.Duration]bool{}
	pauses := 0
	for i := 0; i < 500; i++ {
		gap := p.Gap()
		if gap < 1500*time.Millisecond || gap > 45*time.Second {
			t.Fatalf("gap %s out of range", gap)
		}
		if gap > 6*time.Second {
			pauses++
		}
		seen[gap] = true
	}
	if len(seen) < 400 || pauses < 20 || pauses > 90 {
		t.Fatalf("distinct gaps %d, reading pauses %d", len(seen), pauses)
	}
	if (&x.Pacer{Rand: p.Rand}).Gap() != 0 {
		t.Fatal("a zero base means no wait")
	}
}

func TestPacerOrdersSkipsAndBudgetsVaryButStayBounded(t *testing.T) {
	p := seededPacer(2, time.Second)
	orders := map[string]bool{}
	skips := 0
	for i := 0; i < 200; i++ {
		var names []string
		for _, s := range p.Order(x.Streams) {
			names = append(names, s.Name)
		}
		if len(names) != 4 {
			t.Fatalf("order lost a stream: %v", names)
		}
		orders[strings.Join(names, ",")] = true
		skipped := p.Skip(x.Streams)
		if len(skipped) == 4 {
			t.Fatal("never skip every stream")
		}
		skips += len(skipped)
		if n := p.Pages(5); n < 3 || n > 5 {
			t.Fatalf("pages %d", n)
		}
		if d := p.StartDelay(10 * time.Minute); d < 0 || d >= 10*time.Minute {
			t.Fatalf("start delay %s", d)
		}
	}
	if len(orders) < 15 || skips < 40 || skips > 130 {
		t.Fatalf("orders %d skips %d", len(orders), skips)
	}
	if p.Pages(1) != 1 {
		t.Fatal("a one-page budget stays one page")
	}
}

func TestPacerCooldownsOutlastTheResetAndVary(t *testing.T) {
	p := seededPacer(3, time.Second)
	now := time.Date(2026, 11, 27, 14, 0, 0, 0, time.UTC)
	reset := now.Add(2 * time.Hour)
	for i := 0; i < 50; i++ {
		if at := p.RateLimitCooldown(reset, now); at.Before(reset.Add(5*time.Minute)) || at.After(reset.Add(45*time.Minute)) {
			t.Fatalf("after a named reset: %s", at)
		}
		if at := p.RateLimitCooldown(time.Time{}, now); at.Before(now.Add(35*time.Minute)) || at.After(now.Add(75*time.Minute)) {
			t.Fatalf("without a reset: %s", at)
		}
		if at := p.ChallengeCooldown(now); at.Before(now.Add(2*time.Hour)) || at.After(now.Add(6*time.Hour)) {
			t.Fatalf("challenge: %s", at)
		}
	}
}

func TestARunWithAPacerShufflesSleepsAndCoolsARateLimitedStreamDown(t *testing.T) {
	api := &fakeAPI{remaining: -1, errs: map[string]error{"Likes|": &x.RateLimitError{}}}
	state := openState(t)
	now := time.Date(2026, 11, 27, 14, 0, 0, 0, time.UTC)
	var slept []time.Duration
	pacer := seededPacer(4, 3*time.Second)
	pacer.SkipChance = 0
	runner := func() *x.Runner {
		return &x.Runner{
			Account: myHandle, UserID: myID, API: api, Client: &recordingUploader{}, State: state,
			Logger: common.NewWriterLogger(io.Discard), Now: func() time.Time { return now },
			Pace: pacer, Sleep: func(d time.Duration) { slept = append(slept, d) },
		}
	}
	summary, err := runner().Sync()
	if err != nil {
		t.Fatal(err)
	}
	if len(slept) != len(api.calls)-1 {
		t.Fatalf("one gap between requests: slept %v calls %v", slept, api.calls)
	}
	for _, d := range slept {
		if d == 3*time.Second {
			t.Fatalf("a fixed gap slipped through: %v", slept)
		}
	}
	until, reason, err := state.Cooldown("likes")
	if err != nil || reason != "rate_limit" || until.Before(now.Add(35*time.Minute)) {
		t.Fatalf("cooldown %s %q %v", until, reason, err)
	}
	if strings.Join(summary.RateLimited, ",") != "likes" {
		t.Fatalf("summary %+v", summary)
	}
	// The next run, within the cooldown, never asks for likes.
	api.calls = nil
	delete(api.errs, "Likes|")
	summary, err = runner().Sync()
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range api.calls {
		if strings.HasPrefix(call, "Likes") {
			t.Fatalf("called likes while cooling down: %v", api.calls)
		}
	}
	if strings.Join(summary.CoolingDown, ",") != "likes" {
		t.Fatalf("summary %+v", summary)
	}
	// A different seed reads the streams in a different order.
	var orders []string
	for seed := uint64(10); seed < 14; seed++ {
		api := &fakeAPI{remaining: -1}
		p := seededPacer(seed, 0)
		p.SkipChance = 0
		(&x.Runner{Account: myHandle, UserID: myID, API: api, Client: &recordingUploader{},
			Logger: common.NewWriterLogger(io.Discard), Pace: p}).Sync()
		orders = append(orders, strings.Join(api.calls, ","))
	}
	if orders[0] == orders[1] && orders[1] == orders[2] && orders[2] == orders[3] {
		t.Fatalf("stream order never varies: %v", orders)
	}
}

func TestARejectedSessionKeepsEveryLaterRunOffXComForHours(t *testing.T) {
	api := &fakeAPI{remaining: -1, errs: map[string]error{"UserTweetsAndReplies|": &x.AuthError{Status: 403}}}
	state := openState(t)
	now := time.Date(2026, 11, 27, 14, 0, 0, 0, time.UTC)
	pacer := seededPacer(5, 0)
	pacer.SkipChance = 0
	_, err := (&x.Runner{Account: myHandle, UserID: myID, API: api, Client: &recordingUploader{}, State: state,
		Streams: []x.Stream{x.Streams[0]}, Logger: common.NewWriterLogger(io.Discard), Now: func() time.Time { return now }, Pace: pacer}).Sync()
	var auth *x.AuthError
	if !errors.As(err, &auth) {
		t.Fatalf("err = %v", err)
	}
	until, reason, _ := state.Cooldown(x.RunScope)
	if reason != "session" || until.Before(now.Add(2*time.Hour)) {
		t.Fatalf("run cooldown %s %q", until, reason)
	}
}

func TestTheClientSendsTheInstalledChromesHeadersAndTheStreamsPage(t *testing.T) {
	if got := x.ChromeUserAgent("142.0.7444.60"); !strings.Contains(got, "Chrome/142.0.0.0 Safari") {
		t.Fatalf("ua %q", got)
	}
	if x.ChromeUserAgent("") != x.DefaultUserAgent || x.ChromeUserAgent("garbage") != x.DefaultUserAgent {
		t.Fatal("an unreadable version falls back")
	}
	// Arc ("1.79.1") and Vivaldi ("7.1.3570.39") number their bundles with
	// their own version, not Chrome's: reading it would send "Chrome/1.0.0.0".
	bundles := map[string]string{
		"/Applications/Google Chrome.app": "142.0.7444.60",
		"/Applications/Arc.app":           "1.79.1",
		"/Applications/Vivaldi.app":       "7.1.3570.39",
	}
	read := func(bundle string) (string, error) { return bundles[bundle], nil }
	if v := x.BundleChromeVersion("Google Chrome", read); v != "142.0.7444.60" {
		t.Fatalf("chrome version %q", v)
	}
	for _, browser := range []string{"Arc", "Vivaldi"} {
		if v := x.BundleChromeVersion(browser, read); v != "" {
			t.Fatalf("%s bundle version %q used as a Chrome version", browser, v)
		}
	}
	f := newFakeX(t)
	c := x.NewClient(testSession(), f.server.URL, x.ChromeUserAgent("143.0.1.2"))
	if err := c.Discover(); err != nil {
		t.Fatal(err)
	}
	home := f.requests[0]
	if home.Header.Get("sec-fetch-mode") != "navigate" || home.Header.Get("sec-ch-ua-platform") != `"macOS"` {
		t.Fatalf("home headers %v", home.Header)
	}
	c.SetReferer("/i/bookmarks")
	if _, err := c.Query("Bookmarks", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	last := f.requests[len(f.requests)-1]
	if last.Header.Get("Referer") != f.server.URL+"/i/bookmarks" ||
		!strings.Contains(last.Header.Get("sec-ch-ua"), `"Google Chrome";v="143"`) ||
		!strings.Contains(last.Header.Get("User-Agent"), "Chrome/143.0.0.0") ||
		last.Header.Get("sec-fetch-site") != "same-origin" || last.Header.Get("Accept-Language") == "" {
		t.Fatalf("api headers %v", last.Header)
	}
	// Each stream names the page its query comes from.
	for _, s := range x.Streams {
		if s.Page == nil || !strings.HasPrefix(s.Page(myHandle), "/") {
			t.Fatalf("%s has no page", s.Name)
		}
	}
}

func TestRunWithWaitsARandomStartAndHonorsARunCooldown(t *testing.T) {
	f := newFakeX(t)
	f.pages["UserByScreenName|"] = map[string]any{"user": user(myID, myHandle)}
	stateDir := t.TempDir()
	statePath := filepath.Join(stateDir, "s.sqlite")
	env := map[string]string{"X_ACCOUNT": myHandle, "PDW_INGEST_PROJECT_DIR": t.TempDir(), "X_REQUEST_DELAY_MS": "0", "X_START_JITTER_SECONDS": "600"}
	now := time.Date(2026, 11, 27, 14, 0, 0, 0, time.UTC)
	var slept []time.Duration
	captured := 0
	deps := x.Deps{
		Capture: func(string) (x.Session, error) { captured++; return testSession(), nil },
		NewAPI:  func(s x.Session, _ common.Getenv) x.API { return x.NewClient(s, f.server.URL, "") },
		Pacer:   func(base time.Duration) *x.Pacer { return seededPacer(6, base) },
		Sleep:   func(d time.Duration) { slept = append(slept, d) },
		Now:     func() time.Time { return now },
	}
	args := []string{"--dry-run", "--start-jitter", "10m", "--state-file", statePath, "--lock-file", filepath.Join(stateDir, "lock")}
	var stdout, stderr bytes.Buffer
	if code := x.RunWith(args, &stdout, &stderr, func(k string) string { return env[k] }, ingestclient.Config{}, deps); code != 0 {
		t.Fatalf("code %d %s", code, stderr.String())
	}
	if len(slept) == 0 || slept[0] <= 0 || slept[0] >= 10*time.Minute {
		t.Fatalf("start wait %v", slept)
	}
	// The dry run reads every stream: none sits out.
	for _, s := range []string{"posts", "likes", "bookmarks", "mentions"} {
		if !strings.Contains(stdout.String(), "X "+s+": read a page") {
			t.Fatalf("dry run skipped %s: %s", s, stdout.String())
		}
	}
	// A session cooldown in state stops a real run before it touches x.com.
	state, err := x.OpenState(statePath, myHandle)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SetCooldown(x.RunScope, now.Add(3*time.Hour), "session"); err != nil {
		t.Fatal(err)
	}
	state.Close()
	captured = 0
	requests := len(f.requests)
	stderr.Reset()
	code := x.RunWith([]string{"--state-file", statePath, "--lock-file", filepath.Join(stateDir, "lock")},
		&stdout, &stderr, func(k string) string { return env[k] }, ingestclient.Config{}, deps)
	if code != 1 || captured != 0 || len(f.requests) != requests || !strings.Contains(stderr.String(), "not calling x.com until") {
		t.Fatalf("code %d captured %d stderr %q", code, captured, stderr.String())
	}
}

func TestTheRunnerSendsEachStreamsOwnPageAsReferer(t *testing.T) {
	f := newFakeX(t)
	c := x.NewClient(testSession(), f.server.URL, "")
	if err := c.Discover(); err != nil {
		t.Fatal(err)
	}
	if _, err := (&x.Runner{Account: myHandle, UserID: myID, API: c, Client: &recordingUploader{},
		Logger: common.NewWriterLogger(io.Discard)}).Sync(); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"UserTweetsAndReplies":  "/zachlatta/with_replies",
		"Likes":                 "/zachlatta/likes",
		"Bookmarks":             "/i/bookmarks",
		"NotificationsTimeline": "/notifications/mentions",
	}
	for _, r := range f.requests {
		for op, page := range want {
			if strings.HasSuffix(r.URL.Path, "/"+op) {
				if got := r.Header.Get("Referer"); got != f.server.URL+page {
					t.Fatalf("%s referer %q, want %q", op, got, page)
				}
				delete(want, op)
			}
		}
	}
	if len(want) != 0 {
		t.Fatalf("not called: %v", want)
	}
}

func TestClearCooldownLiftsTheSessionCooldown(t *testing.T) {
	f := newFakeX(t)
	f.pages["UserByScreenName|"] = map[string]any{"user": user(myID, myHandle)}
	stateDir := t.TempDir()
	statePath := filepath.Join(stateDir, "s.sqlite")
	now := time.Date(2026, 11, 27, 14, 0, 0, 0, time.UTC)
	state, err := x.OpenState(statePath, myHandle)
	if err != nil {
		t.Fatal(err)
	}
	state.SetCooldown(x.RunScope, now.Add(3*time.Hour), "session")
	state.Close()
	env := map[string]string{"X_ACCOUNT": myHandle, "PDW_INGEST_PROJECT_DIR": t.TempDir(), "X_REQUEST_DELAY_MS": "0"}
	captured := false
	deps := x.Deps{
		Capture: func(string) (x.Session, error) {
			captured = true
			return x.Session{}, errors.New("stop after the gate")
		},
		Now:   func() time.Time { return now },
		Sleep: func(time.Duration) {},
	}
	var stdout, stderr bytes.Buffer
	x.RunWith([]string{"--clear-cooldown", "--start-jitter", "0", "--state-file", statePath, "--lock-file", filepath.Join(stateDir, "lock")},
		&stdout, &stderr, func(k string) string { return env[k] }, ingestclient.Config{}, deps)
	if !captured {
		t.Fatalf("the cleared cooldown still blocked the run: %s", stderr.String())
	}
}

// refusedRun runs once against a fake x.com, then again, and reports whether
// the second run reached x.com at all.
func refusedRun(t *testing.T, setup func(f *fakeX)) (first, second string, secondRequests int, code2 int, state *x.State) {
	t.Helper()
	f := newFakeX(t)
	f.pages["UserByScreenName|"] = map[string]any{"user": user(myID, myHandle)}
	setup(f)
	stateDir := t.TempDir()
	statePath := filepath.Join(stateDir, "s.sqlite")
	env := map[string]string{"X_ACCOUNT": myHandle, "PDW_INGEST_PROJECT_DIR": t.TempDir(), "X_REQUEST_DELAY_MS": "0"}
	now := time.Date(2026, 11, 27, 14, 0, 0, 0, time.UTC)
	deps := x.Deps{
		Capture: func(string) (x.Session, error) { return testSession(), nil },
		NewAPI:  func(s x.Session, _ common.Getenv) x.API { return x.NewClient(s, f.server.URL, "") },
		Pacer:   func(base time.Duration) *x.Pacer { return seededPacer(7, base) },
		Sleep:   func(time.Duration) {},
		Now:     func() time.Time { return now },
	}
	args := []string{"--start-jitter", "0", "--state-file", statePath, "--lock-file", filepath.Join(stateDir, "lock")}
	var out1, err1, out2, err2 bytes.Buffer
	x.RunWith(args, &out1, &err1, func(k string) string { return env[k] }, ingestclient.Config{}, deps)
	before := len(f.requests)
	code2 = x.RunWith(args, &out2, &err2, func(k string) string { return env[k] }, ingestclient.Config{}, deps)
	st, err := x.OpenState(statePath, myHandle)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return err1.String(), err2.String(), len(f.requests) - before, code2, st
}

func TestARefusedAccountLookupCoolsTheSessionDown(t *testing.T) {
	first, second, requests, code, state := refusedRun(t, func(f *fakeX) { f.status["UserByScreenName|"] = http.StatusForbidden })
	if !strings.Contains(first, "HTTP 403") {
		t.Fatalf("first run: %q", first)
	}
	until, reason, _ := state.Cooldown(x.RunScope)
	if reason != "session" || until.Before(time.Date(2026, 11, 27, 16, 0, 0, 0, time.UTC)) {
		t.Fatalf("cooldown %s %q", until, reason)
	}
	if requests != 0 || code != 1 || !strings.Contains(second, "not calling x.com until") {
		t.Fatalf("second run reached x.com (%d requests, code %d): %q", requests, code, second)
	}
}

func TestARateLimitedAccountLookupCoolsTheRunDownWithoutFailing(t *testing.T) {
	_, second, requests, code, state := refusedRun(t, func(f *fakeX) { f.status["UserByScreenName|"] = http.StatusTooManyRequests })
	_, reason, _ := state.Cooldown(x.RunScope)
	if reason != "rate_limit" || requests != 0 || code != 0 {
		t.Fatalf("reason %q requests %d code %d: %q", reason, requests, code, second)
	}
}

func TestARefusedOrRedirectedHomePageCoolsTheSessionDown(t *testing.T) {
	for name, setup := range map[string]func(f *fakeX){
		"403":      func(f *fakeX) { f.homeStatus = http.StatusForbidden },
		"to login": func(f *fakeX) { f.homeRedirect = "/i/flow/login" },
	} {
		t.Run(name, func(t *testing.T) {
			_, _, requests, code, state := refusedRun(t, setup)
			_, reason, _ := state.Cooldown(x.RunScope)
			if reason != "session" || requests != 0 || code != 1 {
				t.Fatalf("reason %q requests %d code %d", reason, requests, code)
			}
		})
	}
}
