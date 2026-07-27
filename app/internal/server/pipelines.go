package server

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// The freshness dashboard reads the marts_ops views over the snapshot the
// Dagster pipeline_health asset refreshes every ten minutes. Both views are one
// row per pipeline (~30) and one row per warehouse table (~95), so unlike the
// timeline's sidebar aggregates there is nothing here worth caching: the whole
// payload is two indexless scans of two tiny tables.
const (
	pipelinesMaxRows      = 500
	pipelinesTableMaxRows = 1000
)

var pipelineHealthSQL = `
SELECT pipeline, label, kind, cadence, transport, status, data_status, run_status,
       last_write_at, newest_event_at, last_run_at,
       data_age_seconds, run_age_seconds,
       expected_data_interval_seconds, expected_run_interval_seconds,
       row_estimate, byte_size, table_count, tables_probed, tables_skipped,
       state_table, state_rows, state_error_rows, state_attention_rows,
       last_error, last_error_at, collected_at, snapshot_age_seconds, note
FROM ` + warehouse.SQLRelation("marts_pipeline_health") + `
ORDER BY pipeline`

var pipelineTableFreshnessSQL = `
SELECT table_id, pipeline, role, layer, table_schema, table_name,
       written_at_column, event_at_column, last_write_at, newest_event_at,
       data_age_seconds, row_estimate, byte_size,
       probe_status, probe_detail, probe_ms, collected_at, note
FROM ` + warehouse.SQLRelation("marts_pipeline_table_freshness") + `
ORDER BY pipeline, role, table_id`

type pipelineService struct {
	warehouse timelineQuerier
	logger    *slog.Logger
	now       func() time.Time
}

func newPipelineService(querier timelineQuerier, logger *slog.Logger) *pipelineService {
	return &pipelineService{warehouse: querier, logger: logger, now: time.Now}
}

// handlePipelines serves the whole dashboard payload: every pipeline, every
// table's freshness, and the server's clock. The clock matters — the page
// renders ages relative to it, so a browser with a skewed clock still reports
// the same "3 minutes ago" the warehouse computed.
func (s *pipelineService) handlePipelines(w http.ResponseWriter, r *http.Request) {
	pipelines, err := s.warehouse.QueryArgs(r.Context(), pipelineHealthSQL, nil, pipelinesMaxRows)
	if err != nil {
		// The app and Dagster deploy from the same push, so between the app
		// coming up and the collector's first run the marts_ops views do not
		// exist yet. That is a warming state, not a failure: answer with an
		// empty snapshot so the page renders its "no snapshot yet" message.
		if isMissingRelation(err) {
			s.logger.InfoContext(r.Context(), "pipeline health views not provisioned yet", "error", err)
			writeJSON(w, map[string]any{
				"pipelines":  []map[string]any{},
				"tables":     []map[string]any{},
				"server_now": s.now().UTC().Format(time.RFC3339Nano),
				"pending":    true,
			})
			return
		}
		s.logger.ErrorContext(r.Context(), "pipeline health query failed", "error", err)
		httpError(w, http.StatusInternalServerError, "pipeline health query failed")
		return
	}
	tables, err := s.warehouse.QueryArgs(r.Context(), pipelineTableFreshnessSQL, nil, pipelinesTableMaxRows)
	if err != nil {
		s.logger.ErrorContext(r.Context(), "pipeline table freshness query failed", "error", err)
		httpError(w, http.StatusInternalServerError, "pipeline table freshness query failed")
		return
	}
	writeJSON(w, map[string]any{
		"pipelines":  nonNilRows(pipelines.Rows),
		"tables":     nonNilRows(tables.Rows),
		"server_now": s.now().UTC().Format(time.RFC3339Nano),
	})
}

// isMissingRelation recognizes Postgres's undefined_table/undefined_view report.
// The query runner returns the driver's error text rather than a typed error, so
// the SQLSTATE is matched in the message it carries.
func isMissingRelation(err error) bool {
	if err == nil {
		return false
	}
	text := err.Error()
	return strings.Contains(text, "SQLSTATE 42P01") ||
		strings.Contains(text, "42P01") ||
		(strings.Contains(text, "does not exist") && strings.Contains(text, "relation"))
}

func (s *pipelineService) registerRoutes(mux *http.ServeMux, requireAuth func(http.Handler) http.Handler) {
	mux.Handle("/api/pipelines", requireAuth(http.HandlerFunc(s.handlePipelines)))
	mux.HandleFunc(pipelinesPagePath, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// Same reasoning as the timeline shell: tiny, frequently iterated, and
		// useless if a browser serves a stale copy.
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte(pipelinesPageHTML))
	})
}
