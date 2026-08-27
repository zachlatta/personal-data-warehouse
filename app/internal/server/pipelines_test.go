package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

var errFakeQuery = errors.New("snapshot query exploded")

func pipelineHealthRow(pipeline, status string) map[string]any {
	return map[string]any{
		"pipeline": pipeline, "label": "Gmail", "kind": "source",
		"cadence": "every 15 min", "transport": "Dagster gmail_sync",
		"status": status, "data_status": status, "run_status": "ok",
		"last_write_at": "2026-07-27T11:15:47Z", "newest_event_at": "2026-07-27T11:02:00Z",
		"last_run_at":      "2026-07-27T11:20:00Z",
		"data_age_seconds": int64(320), "run_age_seconds": int64(60),
		"expected_data_interval_seconds": int64(21600), "expected_run_interval_seconds": int64(2700),
		"row_estimate": int64(760875), "byte_size": int64(25769803776),
		"table_count": int64(3), "tables_probed": int64(2), "tables_skipped": int64(1),
		"state_table": "gmail_sync_state", "state_rows": int64(2),
		"state_error_rows": int64(0), "state_attention_rows": int64(0),
		"last_error": nil, "last_error_at": nil,
		"collected_at": "2026-07-27T11:21:00Z", "snapshot_age_seconds": int64(120),
		"note": "",
	}
}

func pipelineTableRow(tableID, pipeline, role string) map[string]any {
	return map[string]any{
		"table_id": tableID, "pipeline": pipeline, "role": role,
		"layer": "base_gmail", "table_schema": "base_gmail", "table_name": "messages",
		"written_at_column": "synced_at", "event_at_column": "internal_date",
		"last_write_at": "2026-07-27T11:15:47Z", "newest_event_at": "2026-07-27T11:02:00Z",
		"data_age_seconds": int64(320), "row_estimate": int64(760875),
		"byte_size": int64(25769803776), "probe_status": "ok", "probe_detail": nil,
		"probe_ms": int64(3), "collected_at": "2026-07-27T11:21:00Z", "note": "",
	}
}

func newPipelinesTestRunner() *fakeTimelineRunner {
	return &fakeTimelineRunner{
		argResults: map[string]query.RawResult{
			warehouse.SQLRelation("marts_pipeline_health"): {
				Columns: []string{"pipeline"},
				Rows: []map[string]any{
					pipelineHealthRow("gmail", "ok"),
					pipelineHealthRow("apple_notes", "stale"),
				},
			},
			warehouse.SQLRelation("marts_pipeline_table_freshness"): {
				Columns: []string{"table_id"},
				Rows: []map[string]any{
					pipelineTableRow("gmail_messages", "gmail", "data"),
					pipelineTableRow("gmail_sync_state", "gmail", "state"),
				},
			},
		},
	}
}

func TestPipelinesAPIRequiresBearer(t *testing.T) {
	srv := newTimelineTestServer(t, newPipelinesTestRunner())
	resp, _ := timelineGET(t, srv, "/api/pipelines", false)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without a bearer token, got %d", resp.StatusCode)
	}
}

func TestPipelinesAPIReturnsPipelinesAndTables(t *testing.T) {
	srv := newTimelineTestServer(t, newPipelinesTestRunner())
	resp, body := timelineGET(t, srv, "/api/pipelines", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	var payload struct {
		Pipelines []map[string]any `json:"pipelines"`
		Tables    []map[string]any `json:"tables"`
		ServerNow string           `json:"server_now"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	if len(payload.Pipelines) != 2 || len(payload.Tables) != 2 {
		t.Fatalf("expected 2 pipelines and 2 tables, got %d and %d", len(payload.Pipelines), len(payload.Tables))
	}
	if payload.Pipelines[0]["pipeline"] != "gmail" {
		t.Fatalf("unexpected first pipeline: %v", payload.Pipelines[0])
	}
	// The page renders ages against the server's clock, so the payload has to
	// carry it; without it a skewed browser invents staleness.
	if payload.ServerNow == "" {
		t.Fatal("payload must carry server_now")
	}
}

// The dashboard reads the marts_ops views, never the ops snapshot tables
// directly: the app's query runner assumes the read-only query role, and the
// views are what the catalog publishes.
func TestPipelinesAPIReadsTheMartsViews(t *testing.T) {
	runner := newPipelinesTestRunner()
	srv := newTimelineTestServer(t, runner)
	if resp, body := timelineGET(t, srv, "/api/pipelines", true); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	seen := make([]string, 0, runner.callCount())
	for i := 0; i < runner.callCount(); i++ {
		seen = append(seen, runner.call(i).SQL)
	}
	joined := strings.Join(seen, "\n")
	for _, relation := range []string{
		warehouse.SQLRelation("marts_pipeline_health"),
		warehouse.SQLRelation("marts_pipeline_table_freshness"),
		warehouse.SQLRelation("marts_timeline_priority_mix"),
		warehouse.SQLRelation("marts_agent_usage"),
		warehouse.SQLRelation("marts_search_benchmark"),
	} {
		if !strings.Contains(joined, relation) {
			t.Fatalf("expected a query against %s, got:\n%s", relation, joined)
		}
	}
	for _, forbidden := range []string{
		warehouse.SQLRelation("pipeline_health"),
		warehouse.SQLRelation("pipeline_table_freshness"),
	} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("dashboard must not read the ops snapshot table %s directly:\n%s", forbidden, joined)
		}
	}
}

// Between the app deploying and the collector's first run the marts_ops views do
// not exist. That has to read as "warming", not as a broken dashboard.
func TestPipelinesAPITreatsMissingViewsAsPending(t *testing.T) {
	runner := newPipelinesTestRunner()
	runner.argErrs = map[string]error{
		warehouse.SQLRelation("marts_pipeline_health"): errors.New(
			`ERROR: relation "marts_ops.pipeline_health" does not exist (SQLSTATE 42P01)`),
	}
	srv := newTimelineTestServer(t, runner)
	resp, body := timelineGET(t, srv, "/api/pipelines", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 while the snapshot is unprovisioned, got %d: %s", resp.StatusCode, body)
	}
	var payload struct {
		Pipelines []map[string]any `json:"pipelines"`
		Pending   bool             `json:"pending"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	if !payload.Pending || len(payload.Pipelines) != 0 {
		t.Fatalf("expected an empty pending payload, got %s", body)
	}
}

func TestPipelinesAPISurfacesQueryFailures(t *testing.T) {
	runner := newPipelinesTestRunner()
	runner.argErrs = map[string]error{
		warehouse.SQLRelation("marts_pipeline_health"): errFakeQuery,
	}
	srv := newTimelineTestServer(t, runner)
	resp, _ := timelineGET(t, srv, "/api/pipelines", true)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 when the snapshot query fails, got %d", resp.StatusCode)
	}
}

func TestPipelinesPageIsServedUnauthenticated(t *testing.T) {
	srv := newTimelineTestServer(t, newPipelinesTestRunner())
	resp, body := timelineGET(t, srv, pipelinesPagePath, false)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected the page shell to render without a bearer token, got %d", resp.StatusCode)
	}
	page := string(body)
	for _, needle := range []string{"PDW/PIPELINES", "/api/pipelines", "marts_ops.pipeline_health"} {
		if !strings.Contains(page, needle) {
			t.Fatalf("page is missing %q", needle)
		}
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("page must not be cached, got Cache-Control %q", got)
	}
}

// The page has to be reachable: an unlinked dashboard is one nobody opens.
func TestTimelinePageLinksToPipelines(t *testing.T) {
	if !strings.Contains(timelinePageHTML, `href="`+pipelinesPagePath+`"`) {
		t.Fatalf("the timeline page must link to %s", pipelinesPagePath)
	}
	if !strings.Contains(pipelinesPageHTML, `href="/timeline"`) {
		t.Fatal("the pipelines page must link back to the timeline")
	}
}

// Both pages read the same localStorage key and accept the same #token= handoff,
// so unlocking one unlocks the other.
func TestPipelinesPageSharesTheTimelineToken(t *testing.T) {
	for _, needle := range []string{"pdw_timeline_token", "#token="} {
		if !strings.Contains(pipelinesPageHTML, needle) {
			t.Fatalf("pipelines page is missing the shared token handling %q", needle)
		}
	}
}

// Go raw string literals cannot contain a backtick, so the embedded JS must not
// either (the timeline page carries the same constraint).
func TestPipelinesPageAvoidsBackticks(t *testing.T) {
	if strings.Contains(pipelinesPageHTML, "`") {
		t.Fatal("the pipelines page must not contain a backtick")
	}
}

// Every status the marts_ops view can emit needs a color and a tooltip, or a new
// state renders as an unexplained gray row.
func TestPipelinesPageExplainsEveryStatus(t *testing.T) {
	for _, status := range []string{
		"ok", "late", "stale", "failing", "attention", "manual", "no_data", "unknown",
	} {
		if !strings.Contains(pipelinesPageHTML, status+": \"") && !strings.Contains(pipelinesPageHTML, status+": ") {
			t.Fatalf("page does not handle status %q", status)
		}
		if !strings.Contains(pipelinesPageHTML, "\""+status+"\"") {
			t.Fatalf("page does not name status %q", status)
		}
	}
}

// --- levels 2-4: marts, timeline adapters, collation integrity ---------------

func martHealthRow(viewID, status, inputStatus string) map[string]any {
	return map[string]any{
		"view_id": viewID, "domain": "ai_conversations",
		"view_schema": "marts_ai_conversations", "view_name": "events",
		"status": status, "input_status": inputStatus, "probe_status": "ok",
		"probe_detail": nil, "probe_ms": int64(4), "has_rows": int64(1),
		"input_tables":                      []any{"pi_events", "codex_events"},
		"input_count":                       int64(2),
		"inputs_unmeasured":                 int64(0),
		"stalest_pipeline":                  "pi",
		"input_pipelines":                   []any{"pi", "codex"},
		"stalest_pipeline_at":               "2026-07-20T11:15:47Z",
		"stalest_pipeline_age_seconds":      int64(604800),
		"stalest_pipeline_expected_seconds": int64(259200),
		"definition_sha256":                 "abc123def456",
		"first_seen_at":                     "2026-07-01T00:00:00Z",
		"definition_age_seconds":            int64(2246400),
		"collected_at":                      "2026-07-27T11:21:00Z",
		"note":                              "",
	}
}

func adapterHealthRow(adapter, status string) map[string]any {
	return map[string]any{
		"adapter": adapter, "status": status,
		"backfill_done": int64(1), "backfill_rows": int64(1200), "incremental_rows": int64(34),
		"watermark_ingest_ts": "2026-07-27T11:00:00Z", "last_run_at": "2026-07-27T11:20:00Z",
		"watermark_age_seconds": int64(1260), "run_age_seconds": int64(60),
		"last_error": nil, "updated_at": "2026-07-27T11:20:00Z",
	}
}

func collationHealthRow(objectID, scope, status, finding string) map[string]any {
	return map[string]any{
		"object_id": objectID, "scope": scope, "object_name": objectID,
		"status": status, "finding": finding,
		"detail":   "this database cannot detect collation drift; text index ordering is unverified",
		"provider": "database default", "recorded_version": nil, "actual_version": "2.36",
		"dependent_indexes": int64(188), "table_name": nil,
		"is_unique": int64(0), "is_partial": int64(0), "predicate": nil,
		"key_columns": []any{}, "heap_rows": int64(0), "distinct_keys": int64(0),
		"excess_rows": int64(0), "probe_ms": int64(0),
		"collected_at": "2026-07-27T11:21:00Z",
	}
}

func newFullPipelinesTestRunner() *fakeTimelineRunner {
	runner := newPipelinesTestRunner()
	runner.argResults[warehouse.SQLRelation("marts_mart_view_health")] = query.RawResult{
		Columns: []string{"view_id"},
		Rows: []map[string]any{
			martHealthRow("ai_conversation_events", "stale", "stale"),
			martHealthRow("marts_finance_net_worth", "ok", "ok"),
		},
	}
	runner.argResults[warehouse.SQLRelation("marts_timeline_adapter_health")] = query.RawResult{
		Columns: []string{"adapter"},
		Rows: []map[string]any{
			adapterHealthRow("gmail", "ok"),
			adapterHealthRow("pi", "failing"),
		},
	}
	runner.argResults[warehouse.SQLRelation("marts_collation_health")] = query.RawResult{
		Columns: []string{"object_id"},
		Rows: []map[string]any{
			collationHealthRow("database", "database", "attention", "no_baseline"),
			collationHealthRow("index:base_slack.messages_pkey", "index", "ok", "ok"),
		},
	}
	runner.argResults[warehouse.SQLRelation("marts_search_health")] = query.RawResult{
		Columns: []string{"component"},
		Rows:    []map[string]any{{"component": "chunks", "status": "ok"}, {"component": "embeddings", "status": "backfilling"}},
	}
	return runner
}

// All four levels have to reach the browser, or the SQL surface and the web
// surface disagree about what the warehouse's health is.
func TestPipelinesAPIReturnsEveryHealthLevel(t *testing.T) {
	srv := newTimelineTestServer(t, newFullPipelinesTestRunner())
	resp, body := timelineGET(t, srv, "/api/pipelines", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	var payload struct {
		Pipelines []map[string]any `json:"pipelines"`
		Tables    []map[string]any `json:"tables"`
		Marts     []map[string]any `json:"marts"`
		Adapters  []map[string]any `json:"adapters"`
		Collation []map[string]any `json:"collation"`
		Search    []map[string]any `json:"search"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	if len(payload.Marts) != 2 {
		t.Fatalf("expected the marts layer in the payload, got %d rows", len(payload.Marts))
	}
	if len(payload.Adapters) != 2 {
		t.Fatalf("expected timeline adapter health in the payload, got %d rows", len(payload.Adapters))
	}
	if len(payload.Collation) != 2 {
		t.Fatalf("expected collation health in the payload, got %d rows", len(payload.Collation))
	}
	if len(payload.Search) != 2 {
		t.Fatalf("expected search convergence health in the payload, got %d rows", len(payload.Search))
	}
	if payload.Marts[0]["stalest_pipeline"] != "pi" {
		t.Fatalf("mart rows must carry the stalest pipeline: %v", payload.Marts[0])
	}
}

// Each supplementary level is provisioned by a different ensure path and a
// different collector, so one of them missing must not take the whole dashboard
// down: the four levels that DO work are exactly what a health page is for.
func TestPipelinesAPIDegradesPerLevel(t *testing.T) {
	for _, relation := range []string{
		warehouse.SQLRelation("marts_mart_view_health"),
		warehouse.SQLRelation("marts_timeline_adapter_health"),
		warehouse.SQLRelation("marts_collation_health"),
		warehouse.SQLRelation("marts_search_health"),
	} {
		runner := newFullPipelinesTestRunner()
		runner.argErrs = map[string]error{
			relation: errors.New(`ERROR: relation does not exist (SQLSTATE 42P01)`),
		}
		srv := newTimelineTestServer(t, runner)
		resp, body := timelineGET(t, srv, "/api/pipelines", true)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s missing must not fail the request, got %d: %s", relation, resp.StatusCode, body)
		}
		var payload struct {
			Pipelines []map[string]any `json:"pipelines"`
			Marts     []map[string]any `json:"marts"`
			Adapters  []map[string]any `json:"adapters"`
			Collation []map[string]any `json:"collation"`
			Search    []map[string]any `json:"search"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("decode: %v (%s)", err, body)
		}
		if len(payload.Pipelines) != 2 {
			t.Fatalf("%s missing must not hide the pipeline rows: %s", relation, body)
		}
		// The absent level answers with an empty slice, never null: the page
		// iterates these without a nil check, like every other row list here.
		if payload.Marts == nil || payload.Adapters == nil || payload.Collation == nil || payload.Search == nil {
			t.Fatalf("%s missing produced a null level: %s", relation, body)
		}
	}
}

// Same rule as the pipeline rows: read the published marts_ops views, never the
// ops snapshot tables, because the app's runner assumes the read-only role.
func TestPipelinesAPIReadsEveryHealthLevelThroughMarts(t *testing.T) {
	runner := newFullPipelinesTestRunner()
	srv := newTimelineTestServer(t, runner)
	if resp, body := timelineGET(t, srv, "/api/pipelines", true); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	seen := make([]string, 0, runner.callCount())
	for i := 0; i < runner.callCount(); i++ {
		seen = append(seen, runner.call(i).SQL)
	}
	joined := strings.Join(seen, "\n")
	for _, relation := range []string{
		warehouse.SQLRelation("marts_mart_view_health"),
		warehouse.SQLRelation("marts_timeline_adapter_health"),
		warehouse.SQLRelation("marts_collation_health"),
	} {
		if !strings.Contains(joined, relation) {
			t.Fatalf("expected a query against %s, got:\n%s", relation, joined)
		}
	}
	for _, forbidden := range []string{
		warehouse.SQLRelation("mart_view_health"),
		warehouse.SQLRelation("collation_health"),
	} {
		if strings.Contains(joined, forbidden+"\n") {
			t.Fatalf("dashboard must not read the ops snapshot table %s directly:\n%s", forbidden, joined)
		}
	}
}

// newest_event_at was collected, stored, shipped and rendered for months and
// never judged. The API has to carry the verdict, or the page cannot show it.
func TestPipelinesAPICarriesTheEventVerdict(t *testing.T) {
	runner := newFullPipelinesTestRunner()
	srv := newTimelineTestServer(t, runner)
	if resp, body := timelineGET(t, srv, "/api/pipelines", true); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	joined := ""
	for i := 0; i < runner.callCount(); i++ {
		joined += runner.call(i).SQL + "\n"
	}
	for _, column := range []string{
		"event_status", "event_age_seconds", "expected_event_interval_seconds",
		"event_tables_probed", "data_basis",
	} {
		if !strings.Contains(joined, column) {
			t.Fatalf("the pipeline query must select %s:\n%s", column, joined)
		}
	}
}

// A section nobody can read is a section nobody acts on.
func TestPipelinesPageRendersEveryHealthLevel(t *testing.T) {
	for _, needle := range []string{
		"marts_ops.mart_view_health",
		"marts_ops.timeline_adapter_health",
		"marts_ops.collation_health",
		"stalest pipeline",
		"definition",
		"amcheck",
		"pg_database.datcollversion",
	} {
		if !strings.Contains(pipelinesPageHTML, needle) {
			t.Fatalf("pipelines page is missing %q", needle)
		}
	}
}

// The statuses only these levels can emit need a color and a tooltip too, or a
// mart with an unmeasured input renders as an unexplained gray row.
func TestPipelinesPageExplainsTheNewStatuses(t *testing.T) {
	for _, status := range []string{"backfilling", "unmeasured", "unmonitored"} {
		if !strings.Contains(pipelinesPageHTML, status+": ") {
			t.Fatalf("page does not colour status %q", status)
		}
		if !strings.Contains(pipelinesPageHTML, status+":") {
			t.Fatalf("page does not explain status %q", status)
		}
	}
}

// "we did not look" must not read as "nothing ever arrived", and the page has
// to say which one it means.
func TestPipelinesPageDistinguishesUnmeasuredFromNoData(t *testing.T) {
	if !strings.Contains(pipelinesPageHTML, "we did not look") {
		t.Fatal("page must explain that unmeasured is not no_data")
	}
	if !strings.Contains(pipelinesPageHTML, "nothing has ever arrived") {
		t.Fatal("page must still explain no_data")
	}
}

// A Postgres text[] reaches the browser as the raw array literal "{a,b,c}",
// not as a JSON array — so a string whose .length is truthy and whose .join is
// undefined. Calling .join on one threw inside the marts section and, because
// load()'s catch swallows the error into the status line, silently blanked
// every section rendered after it: marts, adapters, integrity, the snapshot
// counts and the legend all disappeared while the pipeline rows above them
// looked perfectly fine.
func TestPipelinesPageNormalizesPostgresArrayLiterals(t *testing.T) {
	if !strings.Contains(pipelinesPageHTML, "function list(value)") {
		t.Fatal("page must carry the text[] literal normalizer")
	}
	for _, field := range []string{"input_pipelines", "input_tables", "key_columns"} {
		if strings.Contains(pipelinesPageHTML, "."+field+".join") {
			t.Fatalf("%s is a Postgres text[] literal; run it through list() before .join", field)
		}
		if strings.Contains(pipelinesPageHTML, "."+field+".length") {
			t.Fatalf("%s is a Postgres text[] literal; .length on the raw string is meaningless", field)
		}
	}
	if !strings.Contains(pipelinesPageHTML, "list(m.input_pipelines)") {
		t.Fatal("the marts row must normalize input_pipelines")
	}
	if !strings.Contains(pipelinesPageHTML, "list(m.input_tables)") {
		t.Fatal("the marts row must normalize input_tables")
	}
}

// TestPipelinesPageCoversEveryOpsHealthView pins web/SQL parity.
//
// C7 says pipeline health is inspectable "via SQL and web". It drifted:
// marts_ops.slack_conversation_health and marts_ops.plaid_item_health existed
// in SQL and appeared nowhere on /pipelines. Those two are not incidental —
// they are the detectors built BECAUSE the level-1 roll-up hid an outage.
// Slack aggregates as one pipeline and ~19k public-channel messages a day kept
// it green through a total group-DM outage; a Plaid re-link minted a second
// live Item and double-counted net worth while the pipeline stayed green.
// A health page that omits exactly the detectors written for the failures the
// page could not see is worse than no page.
func TestPipelinesPageCoversEveryOpsHealthView(t *testing.T) {
	source, err := os.ReadFile("pipelines.go")
	if err != nil {
		t.Fatalf("read pipelines.go: %v", err)
	}
	page, err := os.ReadFile("pipelines_page.go")
	if err != nil {
		t.Fatalf("read pipelines_page.go: %v", err)
	}
	// Every marts_ops health view the warehouse publishes must be queried by
	// the API and rendered by the page.
	for _, view := range []struct{ catalogID, pageKey string }{
		{"marts_pipeline_health", "pipelines"},
		{"marts_pipeline_table_freshness", "tables"},
		{"marts_mart_view_health", "marts"},
		{"marts_timeline_adapter_health", "adapters"},
		{"marts_collation_health", "collation"},
		{"marts_search_health", "search"},
		{"marts_ops_slack_conversation_health", "slack"},
		{"marts_ops_plaid_item_health", "plaid"},
		{"marts_pgbackrest_health", "backups"},
	} {
		if !strings.Contains(string(source), view.catalogID) {
			t.Errorf("/api/pipelines does not query %s; it is inspectable in SQL and invisible on the web", view.catalogID)
		}
		// The page reaches a level either as a rendered section keyed by name
		// or as state.<key>; pipelines and tables are the primary view and are
		// not in the LEVELS list.
		if !strings.Contains(string(page), `"`+view.pageKey+`"`) &&
			!strings.Contains(string(page), "state."+view.pageKey) {
			t.Errorf("the /pipelines page does not render the %q level", view.pageKey)
		}
	}
}
