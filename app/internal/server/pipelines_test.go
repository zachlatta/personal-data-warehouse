package server

import (
	"encoding/json"
	"errors"
	"net/http"
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
