package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
)

// fakeTimelineRunner satisfies both query.Runner and timelineQuerier. QueryArgs
// dispatches on a substring of the SQL so handler tests can canned-respond per
// endpoint without depending on exact whitespace.
type fakeTimelineRunner struct {
	fakeRunner
	argResults map[string]query.RawResult
	argErrs    map[string]error
	calls      []fakeArgsCall
}

type fakeArgsCall struct {
	SQL  string
	Args []any
}

func (f *fakeTimelineRunner) QueryArgs(_ context.Context, sql string, args []any, maxRows int) (query.RawResult, error) {
	f.calls = append(f.calls, fakeArgsCall{SQL: sql, Args: args})
	for fragment, err := range f.argErrs {
		if strings.Contains(sql, fragment) {
			return query.RawResult{}, err
		}
	}
	for fragment, result := range f.argResults {
		if strings.Contains(sql, fragment) {
			if maxRows > 0 && len(result.Rows) > maxRows {
				result.Rows = result.Rows[:maxRows]
			}
			return result, nil
		}
	}
	return query.RawResult{}, nil
}

func timelineEventRow(eventID string, seq int64, ts string) map[string]any {
	return map[string]any{
		"adapter": "gmail_email", "event_id": eventID, "source": "gmail", "kind": "email",
		"event_ts": ts, "end_ts": "1970-01-01T00:00:00Z", "actor": "alice@example.test",
		"title": "Hello", "snippet": "hi there", "context": "z@x.test",
		"source_table": "gmail_messages",
		"source_pk":    `{"account": "z@x.test", "message_id": "m1"}`,
		"metadata":     `{"thread_id": "th1"}`,
		"seq":          seq,
	}
}

const timelineTestMediaBase = "http://media.example.test"

func newTimelineTestServer(t *testing.T, runner *fakeTimelineRunner) *httptest.Server {
	t.Helper()
	authSvc := pdwauth.NewService([]byte(muxAPITestSecret), func() time.Time { return time.Unix(0, 0) })
	cfg := config.Config{
		Addr:                    ":0",
		BaseURL:                 "http://example.test",
		SecretToken:             muxAPITestSecret,
		TimelineMediaBaseURL:    timelineTestMediaBase,
		TimelineMediaSigningKey: "media-signing-key-at-least-32-chars-long",
		ObjectStoreURLTTL:       time.Hour,
	}
	mux := NewMux(cfg, authSvc, runner)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func timelineGET(t *testing.T, srv *httptest.Server, path string, authed bool) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if authed {
		req.Header.Set("Authorization", "Bearer test-client:"+muxAPITestSecret)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, body
}

func TestTimelineAPIRequiresBearer(t *testing.T) {
	srv := newTimelineTestServer(t, &fakeTimelineRunner{})
	for _, path := range []string{"/api/timeline", "/api/timeline/sources", "/api/timeline/item?adapter=a&event_id=b"} {
		resp, _ := timelineGET(t, srv, path, false)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s without bearer: got %d, want 401", path, resp.StatusCode)
		}
	}
}

func TestTimelinePageIsServedWithoutAuth(t *testing.T) {
	srv := newTimelineTestServer(t, &fakeTimelineRunner{})
	resp, body := timelineGET(t, srv, "/timeline", false)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(string(body), "PDW/TIMELINE") {
		t.Fatalf("timeline page missing marker")
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("timeline page content type = %q", resp.Header.Get("Content-Type"))
	}
}

func TestTimelineEndpointsAbsentWithoutArgsRunner(t *testing.T) {
	authSvc := pdwauth.NewService([]byte(muxAPITestSecret), func() time.Time { return time.Unix(0, 0) })
	cfg := config.Config{Addr: ":0", BaseURL: "http://example.test", SecretToken: muxAPITestSecret}
	mux := NewMux(cfg, authSvc, fakeRunner{})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	resp, _ := timelineGET(t, srv, "/timeline", false)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("timeline should be unregistered for a runner without QueryArgs; got %d", resp.StatusCode)
	}
}

func TestTimelineListReturnsItemsAndCursor(t *testing.T) {
	rows := make([]map[string]any, 0, 2)
	rows = append(rows, timelineEventRow("e1", 20, "2026-06-01T12:00:00Z"))
	rows = append(rows, timelineEventRow("e2", 10, "2026-06-01T11:00:00Z"))
	runner := &fakeTimelineRunner{argResults: map[string]query.RawResult{
		"FROM timeline_events": {Columns: []string{"adapter"}, Rows: rows},
	}}
	srv := newTimelineTestServer(t, runner)

	resp, body := timelineGET(t, srv, "/api/timeline?limit=2&sources=gmail,slack", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d: %s", resp.StatusCode, body)
	}
	var payload struct {
		Items      []map[string]any `json:"items"`
		NextCursor string           `json:"next_cursor"`
		HasMore    bool             `json:"has_more"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Items) != 2 || !payload.HasMore {
		t.Fatalf("items=%d has_more=%v", len(payload.Items), payload.HasMore)
	}
	if payload.NextCursor != "2026-06-01T11:00:00Z|10" {
		t.Fatalf("next_cursor = %q", payload.NextCursor)
	}
	pk, ok := payload.Items[0]["source_pk"].(map[string]any)
	if !ok || pk["message_id"] != "m1" {
		t.Fatalf("source_pk not decoded as JSON: %#v", payload.Items[0]["source_pk"])
	}
	// The filters must ride as bind args, not spliced SQL.
	call := runner.calls[0]
	if call.Args[0] != "gmail,slack" {
		t.Fatalf("sources arg = %#v", call.Args[0])
	}
	if fmt.Sprint(call.Args[2]) != "infinity" {
		t.Fatalf("first page cursor ts = %#v", call.Args[2])
	}
}

func TestTimelineListSecondPageUsesCursor(t *testing.T) {
	runner := &fakeTimelineRunner{}
	srv := newTimelineTestServer(t, runner)
	resp, _ := timelineGET(t, srv, "/api/timeline?before="+
		"2026-06-01T11%3A00%3A00Z%7C10", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d", resp.StatusCode)
	}
	call := runner.calls[0]
	if fmt.Sprint(call.Args[2]) != "2026-06-01T11:00:00Z" || fmt.Sprint(call.Args[3]) != "10" {
		t.Fatalf("cursor args = %#v", call.Args)
	}
}

func TestTimelineListRejectsBadInput(t *testing.T) {
	srv := newTimelineTestServer(t, &fakeTimelineRunner{})
	for _, path := range []string{
		"/api/timeline?before=garbage",
		"/api/timeline?before=2026-06-01T11:00:00Z%7Cnope",
		"/api/timeline?limit=zero",
		"/api/timeline?sources=gmail%27%3BDROP",
		"/api/timeline?jump=not-a-date",
	} {
		resp, _ := timelineGET(t, srv, path, true)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s: got %d, want 400", path, resp.StatusCode)
		}
	}
}

func TestTimelineItemReturnsDetailWithChildrenAndSourceRow(t *testing.T) {
	runner := &fakeTimelineRunner{argResults: map[string]query.RawResult{
		"FROM timeline_events": {Rows: []map[string]any{timelineEventRow("e1", 20, "2026-06-01T12:00:00Z")}},
		"FROM gmail_attachments WHERE": {Rows: []map[string]any{
			{"filename": "doc.pdf", "mime_type": "application/pdf", "size": int64(1234)},
		}},
		"row_to_json": {Rows: []map[string]any{
			{"row": `{"account":"z@x.test","message_id":"m1","body_text":"full body"}`},
		}},
	}}
	srv := newTimelineTestServer(t, runner)
	resp, body := timelineGET(t, srv, "/api/timeline/item?adapter=gmail_email&event_id=z%40x.test%7Cm1", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d: %s", resp.StatusCode, body)
	}
	var payload struct {
		Item      map[string]any            `json:"item"`
		SourceRow map[string]any            `json:"source_row"`
		Children  map[string]json.RawMessage `json:"children"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Item["adapter"] != "gmail_email" {
		t.Fatalf("item = %#v", payload.Item)
	}
	if payload.SourceRow["body_text"] != "full body" {
		t.Fatalf("source_row = %#v", payload.SourceRow)
	}
	var attachments []map[string]any
	if err := json.Unmarshal(payload.Children["attachments"], &attachments); err != nil {
		t.Fatal(err)
	}
	if len(attachments) != 1 || attachments[0]["filename"] != "doc.pdf" {
		t.Fatalf("attachments = %#v", attachments)
	}
	// The source-row query must be built from the jsonb primary key with
	// parameter placeholders only.
	found := false
	for _, call := range runner.calls {
		if strings.Contains(call.SQL, "row_to_json") {
			found = true
			if !strings.Contains(call.SQL, "FROM gmail_messages t WHERE account = $1 AND message_id = $2") {
				t.Fatalf("source row SQL = %q", call.SQL)
			}
			if call.Args[0] != "z@x.test" || call.Args[1] != "m1" {
				t.Fatalf("source row args = %#v", call.Args)
			}
		}
	}
	if !found {
		t.Fatal("source row query never ran")
	}
}

func TestTimelineItemNotFound(t *testing.T) {
	srv := newTimelineTestServer(t, &fakeTimelineRunner{})
	resp, _ := timelineGET(t, srv, "/api/timeline/item?adapter=gmail_email&event_id=missing", true)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("got %d, want 404", resp.StatusCode)
	}
}

func TestTimelineSourcesAggregatesAndCaches(t *testing.T) {
	runner := &fakeTimelineRunner{argResults: map[string]query.RawResult{
		"GROUP BY source, kind": {Rows: []map[string]any{
			{"source": "gmail", "kind": "email", "count": int64(10), "oldest": "2020-01-01T00:00:00Z", "newest": "2026-06-01T00:00:00Z"},
		}},
		"FROM timeline_sync_state": {Rows: []map[string]any{
			{"adapter": "gmail_email", "backfill_done": int64(1), "backfill_rows": int64(10)},
		}},
	}}
	srv := newTimelineTestServer(t, runner)
	resp, body := timelineGET(t, srv, "/api/timeline/sources", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d", resp.StatusCode)
	}
	var payload struct {
		Sources []map[string]any `json:"sources"`
		Sync    []map[string]any `json:"sync"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Sources) != 1 || len(payload.Sync) != 1 {
		t.Fatalf("payload = %s", body)
	}
	callsAfterFirst := len(runner.calls)
	resp2, _ := timelineGET(t, srv, "/api/timeline/sources", true)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("got %d", resp2.StatusCode)
	}
	if len(runner.calls) != callsAfterFirst {
		t.Fatalf("second read should be served from cache; calls %d -> %d", callsAfterFirst, len(runner.calls))
	}
}

func TestTimelineChildQueriesCoverEveryEventTable(t *testing.T) {
	// Keep the Go-side detail map aligned with the Python adapter registry:
	// every adapter source_table must have an entry (possibly empty).
	expected := []string{
		"gmail_messages", "slack_messages", "slack_files", "apple_messages",
		"whatsapp_messages", "agent_session_events", "apple_note_revisions",
		"apple_voice_memos_files", "calendar_events", "google_drive_files",
		"contact_cards", "upstream_mutations", "upstream_mutation_requests", "agent_runs",
	}
	for _, table := range expected {
		if _, ok := timelineChildQueries[table]; !ok {
			t.Fatalf("timelineChildQueries missing %s", table)
		}
	}
	if len(timelineChildQueries) != len(expected) {
		t.Fatalf("timelineChildQueries has %d entries, want %d", len(timelineChildQueries), len(expected))
	}
}

func TestParseTimelineCursor(t *testing.T) {
	ts, seq, err := parseTimelineCursor("")
	if err != nil || ts != "infinity" || seq <= 0 {
		t.Fatalf("empty cursor: %v %v %v", ts, seq, err)
	}
	ts, seq, err = parseTimelineCursor("2026-06-01T11:00:00.123456Z|42")
	if err != nil || ts != "2026-06-01T11:00:00.123456Z" || seq != 42 {
		t.Fatalf("cursor parse: %v %v %v", ts, seq, err)
	}
	for _, bad := range []string{"x", "|1", "2026-06-01T11:00:00Z|", "nope|1"} {
		if _, _, err := parseTimelineCursor(bad); err == nil {
			t.Fatalf("cursor %q should be rejected", bad)
		}
	}
}

func TestTimelineChildRowsGetSignedMediaURLs(t *testing.T) {
	item := timelineEventRow("e1", 20, "2026-06-01T12:00:00Z")
	item["adapter"] = "whatsapp_message"
	item["source_table"] = "whatsapp_messages"
	item["source_pk"] = `{"account": "z@x.test", "chat_id": "c1", "message_id": "wm1"}`
	runner := &fakeTimelineRunner{argResults: map[string]query.RawResult{
		"FROM timeline_events": {Rows: []map[string]any{item}},
		"FROM whatsapp_media_items": {Rows: []map[string]any{
			{"filename": "photo.jpg", "mime_type": "image/jpeg", "is_missing": int64(0), "storage_file_id": "blob123"},
			{"filename": "gone.jpg", "mime_type": "image/jpeg", "is_missing": int64(1), "storage_file_id": "blob999"},
		}},
		"row_to_json": {Rows: []map[string]any{{"row": `{"account":"z@x.test"}`}}},
	}}
	srv := newTimelineTestServer(t, runner)
	resp, body := timelineGET(t, srv, "/api/timeline/item?adapter=whatsapp_message&event_id=x", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d: %s", resp.StatusCode, body)
	}
	var payload struct {
		Children map[string][]map[string]any `json:"children"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	media := payload.Children["media"]
	if len(media) != 2 {
		t.Fatalf("media rows = %#v", media)
	}
	stored := media[0]
	url, _ := stored["media_url"].(string)
	if !strings.HasPrefix(url, timelineTestMediaBase+"/objects/blob123?exp=") || !strings.Contains(url, "&sig=") {
		t.Fatalf("media_url = %q", url)
	}
	if stored["media_kind"] != "image" {
		t.Fatalf("media_kind = %v", stored["media_kind"])
	}
	if _, ok := media[1]["media_url"]; ok {
		t.Fatalf("missing blob must not get a media_url: %#v", media[1])
	}
}

func TestTimelineVoiceMemoGetsItemMedia(t *testing.T) {
	item := timelineEventRow("e1", 20, "2026-06-01T12:00:00Z")
	item["adapter"] = "voice_memo"
	item["source_table"] = "apple_voice_memos_files"
	item["source_pk"] = `{"account": "z@x.test", "recording_id": "rec1"}`
	runner := &fakeTimelineRunner{argResults: map[string]query.RawResult{
		"FROM timeline_events": {Rows: []map[string]any{item}},
		"row_to_json": {Rows: []map[string]any{
			{"row": `{"account":"z@x.test","recording_id":"rec1","storage_file_id":"audio42","content_type":"audio/mp4","filename":"standup.m4a"}`},
		}},
	}}
	srv := newTimelineTestServer(t, runner)
	resp, body := timelineGET(t, srv, "/api/timeline/item?adapter=voice_memo&event_id=x", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d: %s", resp.StatusCode, body)
	}
	var payload struct {
		ItemMedia map[string]any `json:"item_media"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.ItemMedia == nil {
		t.Fatalf("item_media missing: %s", body)
	}
	url, _ := payload.ItemMedia["media_url"].(string)
	if !strings.HasPrefix(url, timelineTestMediaBase+"/objects/audio42?exp=") {
		t.Fatalf("item media url = %q", url)
	}
	if payload.ItemMedia["media_kind"] != "audio" || payload.ItemMedia["filename"] != "standup.m4a" {
		t.Fatalf("item_media = %#v", payload.ItemMedia)
	}
}

func TestTimelineMediaKindClassification(t *testing.T) {
	cases := map[string]string{
		"image/jpeg": "image", "image/heic": "image", "audio/mp4": "audio",
		"video/quicktime": "video", "application/pdf": "pdf", "text/plain": "file", "": "file",
	}
	for contentType, want := range cases {
		if got := timelineMediaKind(contentType); got != want {
			t.Fatalf("timelineMediaKind(%q) = %q, want %q", contentType, got, want)
		}
	}
}

func TestCapJSONStringsTruncates(t *testing.T) {
	long := strings.Repeat("a", 100)
	raw, err := truncateJSONStrings([]byte(`{"short":"ok","long":"`+long+`","nested":{"v":"`+long+`"}}`), 10)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out["short"] != "ok" {
		t.Fatalf("short = %v", out["short"])
	}
	if !strings.HasSuffix(out["long"].(string), "… (truncated)") {
		t.Fatalf("long = %v", out["long"])
	}
}
