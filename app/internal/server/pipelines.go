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
	// The marts layer is ~35 views, the timeline ~25 adapters, and the
	// collation snapshot one row per checked collation plus one per probed
	// unique index (~110 in production). All tiny.
	pipelinesMartMaxRows      = 500
	pipelinesAdapterMaxRows   = 500
	pipelinesCollationMaxRows = 2000
	pipelinesSearchMaxRows    = 10
	pipelinesSlackMaxRows     = 100
	pipelinesPlaidMaxRows     = 200
	pipelinesBackupMaxRows    = 20
	pipelinesPriorityMaxRows  = 500
)

var pipelineHealthSQL = `
SELECT pipeline, label, kind, cadence, transport, status, data_status, run_status,
       event_status, last_write_at, newest_event_at, last_run_at,
       data_age_seconds, run_age_seconds, event_age_seconds,
       expected_data_interval_seconds, expected_run_interval_seconds,
       expected_event_interval_seconds, event_tables_probed, data_basis,
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

// Level 2: the marts_* read interface. A view has no stamped column to measure,
// so these rows carry input freshness, a bounded non-empty probe, and the
// definition hash instead.
var pipelineMartHealthSQL = `
SELECT view_id, domain, view_schema, view_name, status, input_status, probe_status,
       probe_detail, probe_ms, has_rows, input_tables, input_pipelines, input_count, inputs_unmeasured,
       stalest_pipeline, stalest_pipeline_at, stalest_pipeline_age_seconds,
       stalest_pipeline_expected_seconds, definition_sha256, first_seen_at,
       definition_age_seconds, collected_at, note
FROM ` + warehouse.SQLRelation("marts_mart_view_health") + `
ORDER BY view_schema, view_name`

// Level 3: per-adapter timeline currency. The single `timeline` pipeline row is
// a max() over every adapter, so one frozen adapter hides behind the rest.
var pipelineAdapterHealthSQL = `
SELECT adapter, status, backfill_done, backfill_rows, incremental_rows,
       watermark_ingest_ts, last_run_at, watermark_age_seconds, run_age_seconds,
       last_error, updated_at
FROM ` + warehouse.SQLRelation("marts_timeline_adapter_health") + `
ORDER BY adapter`

// Level 4: collation drift and unique-index divergence. Postgres cannot warn
// about the former on this database (no recorded baseline, and REFRESH
// COLLATION VERSION refuses to create one from NULL), so this is the only cover.
var pipelineCollationHealthSQL = `
SELECT object_id, scope, object_name, status, finding, detail, provider,
       recorded_version, actual_version, dependent_indexes, table_name,
       is_unique, is_partial, predicate, key_columns, heap_rows, distinct_keys,
       excess_rows, probe_ms, amcheck_status, amcheck_detail, amcheck_ms, amcheck_at, collected_at
FROM ` + warehouse.SQLRelation("marts_collation_health") + `
ORDER BY scope, object_name`

// Level 5: per-source detectors. These exist because level 1 AGGREGATES a
// source into one row and that is exactly how two outages hid. Slack rolls up
// as a single pipeline, and ~19k public-channel messages a day kept it `ok`
// through a total group-DM outage; Plaid rolls up the same way, and a re-link
// that minted a second live Item double-counted net worth while the pipeline
// stayed green. Both had a detector in SQL and neither appeared on this page,
// so the page disagreed with the warehouse about what "healthy" means.
var pipelineSlackConversationHealthSQL = `
SELECT account, team_id, conversation_type, conversation_count, archived_count,
       live_count, refreshed_count, refreshed_fraction,
       oldest_conversation_synced_at, newest_conversation_synced_at,
       discovery_age_seconds, expected_cycle_seconds, discovery_status,
       last_discovery_at, newest_message_at, message_age_seconds, status
FROM ` + warehouse.SQLRelation("marts_ops_slack_conversation_health") + `
ORDER BY status, conversation_type`

var pipelinePlaidItemHealthSQL = `
SELECT account, item_id, institution_name, linked_at, synced_at,
       error_code, error_type, error_message, status, account_count,
       account_names, newest_transaction_at, transaction_age_seconds
FROM ` + warehouse.SQLRelation("marts_ops_plaid_item_health") + `
ORDER BY status, institution_name`

// Backups. The level with no Dagster collector: only the pgBackRest loop
// inside the Postgres container can ask pgbackrest anything, so it writes its
// own row. Absent from this page until 2026-08-26, which is how production ran
// a day with no valid backup while every other level here read green.
var pipelineBackupHealthSQL = `
SELECT stanza, status, repo_status, repo_message,
       last_full_at, last_diff_at, last_incr_at, last_backup_label, last_backup_type,
       backup_count, repo_bytes, wal_min, wal_max, archived_count, failed_count,
       last_archived_at, last_attempt_at, last_attempt_type, last_attempt_ok,
       last_error, collected_at, full_age_seconds, snapshot_age_seconds
FROM ` + warehouse.SQLRelation("marts_pgbackrest_health") + `
ORDER BY stanza`

// Contract C2, per source: the five tiers are only a contract if the mix is
// visible. An `unclassified` row is a classification outage; a source whose
// share collapses into one tier after an adapter edit shows here first.
var pipelinePriorityMixSQL = `
SELECT source, priority, status, events_7d, events_1d, share_7d, source_events_7d,
       newest_event_at, collected_at, snapshot_age_seconds
FROM ` + warehouse.SQLRelation("marts_timeline_priority_mix") + `
ORDER BY source, priority`

var pipelineSearchHealthSQL = `
SELECT component, status, model, configured, pgvector_available,
       timeline_max_seq, chunk_cursor_seq, seq_lag, caught_up,
       processed_rows, pending_count, oldest_pending_at,
       last_success_at, last_run_at, last_error, updated_at, snapshot_age_seconds
FROM ` + warehouse.SQLRelation("marts_search_health") + `
ORDER BY component`

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
				"marts":      []map[string]any{},
				"adapters":   []map[string]any{},
				"collation":  []map[string]any{},
				"search":     []map[string]any{},
				"priority":   []map[string]any{},
				"slack":      []map[string]any{},
				"plaid":      []map[string]any{},
				"backups":    []map[string]any{},
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
	// The levels below the pipeline roll-up degrade independently. Each
	// one is provisioned by a different ensure path and a different collector,
	// so a deploy can legitimately have the pipeline views before the mart or
	// collation snapshot exists. Answering 500 for the whole dashboard because
	// one of them is still warming would hide the four levels that DO work,
	// which is the opposite of what a health page is for.
	marts := s.optionalRows(r, "mart health", pipelineMartHealthSQL, pipelinesMartMaxRows)
	adapters := s.optionalRows(r, "timeline adapter health", pipelineAdapterHealthSQL, pipelinesAdapterMaxRows)
	collation := s.optionalRows(r, "collation health", pipelineCollationHealthSQL, pipelinesCollationMaxRows)
	search := s.optionalRows(r, "search health", pipelineSearchHealthSQL, pipelinesSearchMaxRows)
	priorityMix := s.optionalRows(r, "timeline priority mix", pipelinePriorityMixSQL, pipelinesPriorityMaxRows)
	slack := s.optionalRows(r, "slack conversation health", pipelineSlackConversationHealthSQL, pipelinesSlackMaxRows)
	plaid := s.optionalRows(r, "plaid item health", pipelinePlaidItemHealthSQL, pipelinesPlaidMaxRows)
	backups := s.optionalRows(r, "backup health", pipelineBackupHealthSQL, pipelinesBackupMaxRows)
	writeJSON(w, map[string]any{
		"pipelines":  nonNilRows(pipelines.Rows),
		"tables":     nonNilRows(tables.Rows),
		"marts":      marts,
		"adapters":   adapters,
		"collation":  collation,
		"search":     search,
		"priority":   priorityMix,
		"slack":      slack,
		"plaid":      plaid,
		"backups":    backups,
		"server_now": s.now().UTC().Format(time.RFC3339Nano),
	})
}

// optionalRows runs a supplementary health query, answering with an empty slice
// rather than failing the request when its relation has not been provisioned or
// its query errors. A missing relation is logged at info (a warming deploy); a
// real error is logged at error but still does not take the page down.
func (s *pipelineService) optionalRows(
	r *http.Request, label, sql string, maxRows int,
) []map[string]any {
	result, err := s.warehouse.QueryArgs(r.Context(), sql, nil, maxRows)
	if err != nil {
		if isMissingRelation(err) {
			s.logger.InfoContext(r.Context(), label+" view not provisioned yet", "error", err)
		} else {
			s.logger.ErrorContext(r.Context(), label+" query failed", "error", err)
		}
		return []map[string]any{}
	}
	return nonNilRows(result.Rows)
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
