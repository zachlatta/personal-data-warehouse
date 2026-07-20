package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// fakeTimelineRunner satisfies both query.Runner and timelineQuerier. QueryArgs
// dispatches on a substring of the SQL so handler tests can canned-respond per
// endpoint without depending on exact whitespace.
type fakeTimelineRunner struct {
	fakeRunner
	mu         sync.Mutex
	argResults map[string]query.RawResult
	argErrs    map[string]error
	calls      []fakeArgsCall
}

type fakeArgsCall struct {
	SQL  string
	Args []any
}

func (f *fakeTimelineRunner) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeTimelineRunner) call(i int) fakeArgsCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[i]
}

func (f *fakeTimelineRunner) QueryArgs(_ context.Context, sql string, args []any, maxRows int) (query.RawResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, fakeArgsCall{SQL: sql, Args: args})
	f.mu.Unlock()
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
		"priority": int64(2),
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
	for _, path := range []string{
		"/api/timeline",
		"/api/timeline/sources",
		"/api/timeline/item?adapter=a&event_id=b",
		"/api/timeline/item/children?adapter=a&event_id=b&child=c",
	} {
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
	if !strings.Contains(string(body), `whoop: "#`) {
		t.Fatal("timeline page is missing a distinct WHOOP source hue")
	}
	if !strings.Contains(string(body), `api("/api/timeline/item/children"`) || !strings.Contains(string(body), "load more") {
		t.Fatal("timeline page is missing pageable child controls")
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
		"FROM " + warehouse.SQLRelation("timeline_events"): {Columns: []string{"adapter"}, Rows: rows},
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
	call := runner.call(0)
	if call.Args[0] != "gmail,slack" {
		t.Fatalf("sources arg = %#v", call.Args[0])
	}
	if fmt.Sprint(call.Args[3]) != "infinity" {
		t.Fatalf("first page cursor ts = %#v", call.Args[3])
	}
}

func TestTimelineListPassesPriorityFilter(t *testing.T) {
	runner := &fakeTimelineRunner{}
	srv := newTimelineTestServer(t, runner)
	resp, _ := timelineGET(t, srv, "/api/timeline?priorities=1,2", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d", resp.StatusCode)
	}
	if runner.call(0).Args[2] != "1,2" {
		t.Fatalf("priorities arg = %#v", runner.call(0).Args[2])
	}
	resp, _ = timelineGET(t, srv, "/api/timeline?priorities=1%3BDROP", true)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad priorities filter: got %d, want 400", resp.StatusCode)
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
	call := runner.call(0)
	if fmt.Sprint(call.Args[3]) != "2026-06-01T11:00:00Z" || fmt.Sprint(call.Args[4]) != "10" {
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
		"FROM " + warehouse.SQLRelation("timeline_events"): {Rows: []map[string]any{timelineEventRow("e1", 20, "2026-06-01T12:00:00Z")}},
		"FROM " + warehouse.SQLRelation("gmail_attachments") + " WHERE": {Rows: []map[string]any{
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
		Item      map[string]any             `json:"item"`
		SourceRow map[string]any             `json:"source_row"`
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
	for i := 0; i < runner.callCount(); i++ {
		call := runner.call(i)
		if strings.Contains(call.SQL, "row_to_json") {
			found = true
			if !strings.Contains(call.SQL, "FROM "+warehouse.SQLRelation("gmail_messages")+" t WHERE \"account\" = $1 AND \"message_id\" = $2") {
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

func TestTimelineWhoopItemReturnsAuthoritativeSourceRow(t *testing.T) {
	item := timelineEventRow("whoop-event", 21, "2026-01-02T03:04:05Z")
	item["adapter"] = "whoop_recovery"
	item["source"] = "whoop"
	item["kind"] = "recovery"
	item["source_table"] = "whoop_recoveries"
	item["source_pk"] = `{"account":"z@x.test","cycle_id":"cycle-1"}`

	runner := &fakeTimelineRunner{argResults: map[string]query.RawResult{
		"FROM " + warehouse.SQLRelation("timeline_events"): {Rows: []map[string]any{item}},
		"row_to_json": {Rows: []map[string]any{
			{"row": `{"cycle_id":"cycle-1","recovery_score":70,"resting_heart_rate":60}`},
		}},
	}}
	srv := newTimelineTestServer(t, runner)
	resp, body := timelineGET(t, srv, "/api/timeline/item?adapter=whoop_recovery&event_id=whoop-event", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d: %s", resp.StatusCode, body)
	}
	var payload struct {
		SourceRow map[string]any             `json:"source_row"`
		Children  map[string]json.RawMessage `json:"children"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.SourceRow["recovery_score"] != float64(70) {
		t.Fatalf("source_row = %#v", payload.SourceRow)
	}
	if len(payload.Children) != 0 {
		t.Fatalf("WHOOP recovery should not have child tables: %#v", payload.Children)
	}

	found := false
	for i := 0; i < runner.callCount(); i++ {
		call := runner.call(i)
		if !strings.Contains(call.SQL, "row_to_json") {
			continue
		}
		found = true
		if !strings.Contains(call.SQL, "FROM "+warehouse.SQLRelation("whoop_recoveries")+" t WHERE \"account\" = $1 AND \"cycle_id\" = $2") {
			t.Fatalf("WHOOP source row SQL = %q", call.SQL)
		}
		if call.Args[0] != "z@x.test" || call.Args[1] != "cycle-1" {
			t.Fatalf("WHOOP source row args = %#v", call.Args)
		}
	}
	if !found {
		t.Fatal("WHOOP source row query never ran")
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
		"FROM " + warehouse.SQLRelation("timeline_sync_state"): {Rows: []map[string]any{
			{"adapter": "gmail_email", "backfill_done": int64(1), "backfill_rows": int64(10)},
		}},
	}}
	srv := newTimelineTestServer(t, runner)
	type sourcesPayload struct {
		Sources    []map[string]any `json:"sources"`
		Sync       []map[string]any `json:"sync"`
		Priorities []map[string]any `json:"priorities"`
		Warming    bool             `json:"warming"`
	}

	// The aggregates never block a request: the first call returns a warming
	// payload carrying only the fast sync rows while the aggregate builds in
	// the background.
	resp, body := timelineGET(t, srv, "/api/timeline/sources", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d", resp.StatusCode)
	}
	var first sourcesPayload
	if err := json.Unmarshal(body, &first); err != nil {
		t.Fatal(err)
	}
	if !first.Warming || len(first.Sync) != 1 {
		t.Fatalf("first payload should be warming with sync rows: %s", body)
	}
	if !timelinePayloadHasSourceKind(first.Sources, "gmail", "email") || !timelinePayloadHasSourceKind(first.Sources, "slack", "message") {
		t.Fatalf("warming payload should include filter catalog so the sidebar is usable immediately: %s", body)
	}
	for _, kind := range []string{"health_cycle", "recovery", "sleep", "workout"} {
		if !timelinePayloadHasSourceKind(first.Sources, "whoop", kind) {
			t.Fatalf("warming payload should expose WHOOP %s filters immediately: %s", kind, body)
		}
	}
	for _, pair := range [][2]string{
		{"alice_voice_recordings", "voice_recording"},
		{"finance", "transaction"},
		{"finance", "balance_observation"},
		{"finance", "document"},
		{"chatgpt", "agent_session"},
		{"claude_code", "agent_session"},
		{"claude_desktop", "agent_session"},
		{"codex", "agent_session"},
		{"openclaw", "agent_session"},
		{"pi", "agent_session"},
	} {
		if !timelinePayloadHasSourceKind(first.Sources, pair[0], pair[1]) {
			t.Fatalf("warming payload missing %s/%s: %s", pair[0], pair[1], body)
		}
	}
	if timelinePayloadHasSourceKind(first.Sources, "agent_sessions", "agent_session") {
		t.Fatalf("warming payload must not expose synthetic agent_sessions source: %s", body)
	}
	if !timelinePayloadHasPriority(first.Priorities, 1) || !timelinePayloadHasPriority(first.Priorities, 5) {
		t.Fatalf("warming payload should include priority catalog: %s", body)
	}

	var warmed sourcesPayload
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, body = timelineGET(t, srv, "/api/timeline/sources", true)
		if err := json.Unmarshal(body, &warmed); err != nil {
			t.Fatal(err)
		}
		if !warmed.Warming {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sources cache never warmed: %s", body)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(warmed.Sources) != 1 || len(warmed.Sync) != 1 {
		t.Fatalf("warmed payload = %s", body)
	}

	callsAfterWarm := runner.callCount()
	resp2, _ := timelineGET(t, srv, "/api/timeline/sources", true)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("got %d", resp2.StatusCode)
	}
	if runner.callCount() != callsAfterWarm {
		t.Fatalf("cached read must not re-query; calls %d -> %d", callsAfterWarm, runner.callCount())
	}
}

func timelinePayloadHasSourceKind(rows []map[string]any, source, kind string) bool {
	for _, row := range rows {
		if row["source"] == source && row["kind"] == kind {
			return true
		}
	}
	return false
}

func timelinePayloadHasPriority(rows []map[string]any, priority int64) bool {
	for _, row := range rows {
		if fmt.Sprint(row["priority"]) == fmt.Sprint(priority) {
			return true
		}
	}
	return false
}

func TestTimelineChildQueriesCoverEveryEventTable(t *testing.T) {
	// Keep the Go-side detail map aligned with the Python adapter registry:
	// every adapter source_table must have an entry (possibly empty).
	expected := []string{
		"gmail_messages", "slack_messages", "slack_files", "apple_messages",
		"whatsapp_messages", "agent_session_events", "apple_note_revisions",
		"apple_voice_memos_files", "calendar_events", "google_drive_files",
		"photo_assets", "contact_cards", "whoop_cycles", "whoop_recoveries",
		"whoop_sleeps", "whoop_workouts", "upstream_mutations",
		"upstream_mutation_requests", "agent_runs", "alice_voice_recordings",
		"finance_transactions", "finance_observations", "manual_finance_documents",
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

func TestTimelineDetailQueriesExposeEveryKnownRelationship(t *testing.T) {
	apple := timelineChildQueries["apple_messages"]
	whatsapp := timelineChildQueries["whatsapp_messages"]
	joined := func(queries []timelineChildQuery) string {
		parts := make([]string, 0, len(queries))
		for _, query := range queries {
			parts = append(parts, query.name+" "+query.sql)
		}
		return strings.Join(parts, "\n")
	}
	appleSQL := joined(apple)
	for _, required := range []string{"content_type", "chats", "attachment_enrichments"} {
		if !strings.Contains(appleSQL, required) {
			t.Fatalf("Apple Messages detail queries missing %q: %s", required, appleSQL)
		}
	}
	if !strings.Contains(joined(whatsapp), "attachment_enrichments") {
		t.Fatalf("WhatsApp detail queries must expose attachment enrichment text")
	}
	if queries := timelineChildQueries["agent_session_events"]; len(queries) != 1 || queries[0].name != "events" {
		t.Fatalf("agent session detail must use one pageable event stream: %#v", queries)
	}
	for table, queries := range timelineChildQueries {
		for _, query := range queries {
			if strings.Contains(strings.ToUpper(query.sql), " LIMIT ") {
				t.Fatalf("%s/%s still has a fixed detail cap: %s", table, query.name, query.sql)
			}
		}
	}
}

func TestTimelineItemChildrenArePageable(t *testing.T) {
	item := timelineEventRow("e1", 20, "2026-06-01T12:00:00Z")
	rows := make([]map[string]any, 0, 51)
	for i := 0; i < 51; i++ {
		rows = append(rows, map[string]any{"part_id": fmt.Sprintf("p-%02d", i)})
	}
	runner := &fakeTimelineRunner{argResults: map[string]query.RawResult{
		"FROM " + warehouse.SQLRelation("timeline_events"):   {Rows: []map[string]any{item}},
		"FROM " + warehouse.SQLRelation("gmail_attachments"): {Rows: rows},
		"row_to_json": {Rows: []map[string]any{{"row": `{"account":"z@x.test"}`}}},
	}}
	srv := newTimelineTestServer(t, runner)

	resp, body := timelineGET(t, srv, "/api/timeline/item?adapter=gmail_email&event_id=e1", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("item got %d: %s", resp.StatusCode, body)
	}
	var detail struct {
		Children     map[string][]map[string]any `json:"children"`
		ChildrenMeta map[string]struct {
			HasMore    bool `json:"has_more"`
			NextOffset int  `json:"next_offset"`
		} `json:"children_meta"`
	}
	if err := json.Unmarshal(body, &detail); err != nil {
		t.Fatal(err)
	}
	if len(detail.Children["attachments"]) != 50 || !detail.ChildrenMeta["attachments"].HasMore || detail.ChildrenMeta["attachments"].NextOffset != 50 {
		t.Fatalf("first attachment page = %#v, meta = %#v", detail.Children["attachments"], detail.ChildrenMeta["attachments"])
	}

	resp, body = timelineGET(t, srv, "/api/timeline/item/children?adapter=gmail_email&event_id=e1&child=attachments&offset=50", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("children got %d: %s", resp.StatusCode, body)
	}
	var page struct {
		Rows       []map[string]any `json:"rows"`
		HasMore    bool             `json:"has_more"`
		NextOffset int              `json:"next_offset"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Rows) != 50 || !page.HasMore || page.NextOffset != 100 {
		t.Fatalf("child page = %#v", page)
	}
	last := runner.call(runner.callCount() - 1)
	if !strings.Contains(last.SQL, "LIMIT $3 OFFSET $4") || fmt.Sprint(last.Args) != "[z@x.test m1 51 50]" {
		t.Fatalf("page query = %q args=%#v", last.SQL, last.Args)
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
		"FROM " + warehouse.SQLRelation("timeline_events"): {Rows: []map[string]any{item}},
		"FROM " + warehouse.SQLRelation("whatsapp_media_items"): {Rows: []map[string]any{
			{"filename": "photo.jpg", "mime_type": "image/jpeg", "is_missing": int64(0), "storage_file_id": "blob123"},
			{"filename": "gone.jpg", "mime_type": "image/jpeg", "is_missing": int64(1), "storage_file_id": "blob999"},
			{"filename": "voice.caf", "mime_type": "application/octet-stream", "content_type": "audio/x-caf", "is_missing": int64(0), "storage_file_id": "audio123"},
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
	if len(media) != 3 {
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
	if media[2]["media_kind"] != "audio" {
		t.Fatalf("normalized content_type must win over generic mime_type: %#v", media[2])
	}
}

func TestTimelineVoiceMemoGetsItemMedia(t *testing.T) {
	item := timelineEventRow("e1", 20, "2026-06-01T12:00:00Z")
	item["adapter"] = "voice_memo"
	item["source_table"] = "apple_voice_memos_files"
	item["source_pk"] = `{"account": "z@x.test", "recording_id": "rec1"}`
	runner := &fakeTimelineRunner{argResults: map[string]query.RawResult{
		"FROM " + warehouse.SQLRelation("timeline_events"): {Rows: []map[string]any{item}},
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

func TestTimelinePhotoGetsItemMedia(t *testing.T) {
	item := timelineEventRow("e1", 20, "2026-06-01T12:00:00Z")
	item["adapter"] = "photo"
	item["source_table"] = "photo_assets"
	item["source_pk"] = `{"photo_id": "ph1"}`
	runner := &fakeTimelineRunner{argResults: map[string]query.RawResult{
		"FROM " + warehouse.SQLRelation("timeline_events"): {Rows: []map[string]any{item}},
		"row_to_json": {Rows: []map[string]any{
			{"row": `{"photo_id":"ph1","thumbnail_storage_file_id":"thumb42","thumbnail_content_type":"image/jpeg","best_file_filename":"IMG_0001.HEIC"}`},
		}},
	}}
	srv := newTimelineTestServer(t, runner)
	resp, body := timelineGET(t, srv, "/api/timeline/item?adapter=photo&event_id=ph1", true)
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
	if !strings.HasPrefix(url, timelineTestMediaBase+"/objects/thumb42?exp=") {
		t.Fatalf("item media url = %q", url)
	}
	if payload.ItemMedia["media_kind"] != "image" {
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
