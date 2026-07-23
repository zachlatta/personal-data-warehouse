package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// timelineSlowQuerier is the optional capability for app-internal background
// aggregates whose legitimate runtime exceeds the agent-facing statement
// budget (the sidebar's full-table counts take minutes cold). Fakes without it
// fall back to the plain QueryArgs budget.
type timelineSlowQuerier interface {
	QueryArgsWithTimeout(ctx context.Context, statement string, args []any, maxRows int, timeout time.Duration) (query.RawResult, error)
}

// timelineQuerier is the parameterized-query capability the timeline endpoints
// need. *query.PostgresRunner implements it; the plain fake runner used by
// unrelated tests does not, in which case the timeline endpoints simply are
// not registered.
type timelineQuerier interface {
	QueryArgs(ctx context.Context, statement string, args []any, maxRows int) (query.RawResult, error)
}

const (
	timelineDefaultPageSize = 60
	timelineMaxPageSize     = 200
	// The sidebar count aggregates scan all of timeline.events (~85s of
	// database time per refresh in production); counts of a 42M-row table do
	// not need minute-level freshness, and the handler already serves the
	// stale payload while a background refresh runs. `?refresh=` still forces
	// an immediate rebuild.
	timelineSourcesCacheTTL   = 24 * time.Hour
	timelineDetailFieldChars  = 50000
	timelineChildRowFieldChar = 4000
	// Statement budget for the background sidebar-count aggregates; they scan
	// all of timeline.events and legitimately outlive the agent-facing query
	// timeout.
	timelineSourcesRefreshBudget = 10 * time.Minute
)

// timelineMediaSigner mints signed /objects/ download links for inline media
// in the timeline UI. The links are verified by whichever app baseURL points
// at, so the signer's key must be that app's secret: in production this app
// signs for itself; a local development app can sign for the production app
// (which holds the object-store credential) instead.
type timelineMediaSigner struct {
	baseURL string
	signer  *pdwauth.Service
	ttl     time.Duration
	now     func() time.Time
}

func (m *timelineMediaSigner) signURL(fileID string) string {
	if m == nil || m.signer == nil || m.baseURL == "" || fileID == "" {
		return ""
	}
	exp := m.now().Add(m.ttl)
	sig := m.signer.SignObjectDownload(fileID, "", exp)
	return fmt.Sprintf("%s/objects/%s?exp=%d&sig=%s", m.baseURL, fileID, exp.Unix(), sig)
}

type timelineService struct {
	// timeline holds timeline_events/timeline_sync_state; source holds the
	// authoritative per-source tables for detail views. In production they are
	// the same database.
	timeline timelineQuerier
	source   timelineQuerier
	media    *timelineMediaSigner
	logger   *slog.Logger

	mu                sync.Mutex
	sourcesPayload    []byte
	sourcesFetched    time.Time
	sourcesRefreshing bool
}

func newTimelineService(timeline, source timelineQuerier, media *timelineMediaSigner, logger *slog.Logger) *timelineService {
	return &timelineService{timeline: timeline, source: source, media: media, logger: logger}
}

// --- list -------------------------------------------------------------------

var timelineListSQL = `
SELECT adapter, event_id, source, kind, priority, event_ts, end_ts, actor, title, snippet, context,
       source_table, source_pk::text AS source_pk, metadata::text AS metadata, seq
FROM ` + warehouse.SQLRelation("timeline_events") + `
WHERE ($1 = '' OR source = ANY(string_to_array($1, ',')))
  AND ($2 = '' OR kind = ANY(string_to_array($2, ',')))
  AND ($3 = '' OR priority = ANY(string_to_array($3, ',')::bigint[]))
  AND (event_ts, seq) < ($4::timestamptz, $5::bigint)
ORDER BY event_ts DESC, seq DESC
LIMIT $6`

func parseTimelineCursor(raw string) (string, int64, error) {
	if raw == "" {
		return "infinity", math.MaxInt64, nil
	}
	idx := strings.LastIndexByte(raw, '|')
	if idx <= 0 || idx == len(raw)-1 {
		return "", 0, fmt.Errorf("malformed cursor")
	}
	ts := raw[:idx]
	seq, err := strconv.ParseInt(raw[idx+1:], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("malformed cursor seq")
	}
	if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
		return "", 0, fmt.Errorf("malformed cursor timestamp")
	}
	return ts, seq, nil
}

var (
	timelineTokenListPattern    = regexp.MustCompile(`^[a-z0-9_,-]*$`)
	timelinePriorityListPattern = regexp.MustCompile(`^[0-9,]*$`)
)

func (s *timelineService) handleList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := timelineDefaultPageSize
	if raw := q.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			httpError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = min(parsed, timelineMaxPageSize)
	}
	sources := strings.TrimSpace(q.Get("sources"))
	kinds := strings.TrimSpace(q.Get("kinds"))
	if !timelineTokenListPattern.MatchString(sources) || !timelineTokenListPattern.MatchString(kinds) {
		httpError(w, http.StatusBadRequest, "invalid sources/kinds filter")
		return
	}
	priorities := strings.TrimSpace(q.Get("priorities"))
	if !timelinePriorityListPattern.MatchString(priorities) {
		httpError(w, http.StatusBadRequest, "invalid priorities filter")
		return
	}
	cursorTS, cursorSeq, err := parseTimelineCursor(q.Get("before"))
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	// jump=<RFC3339 or date> starts the page just below a point in time.
	if jump := strings.TrimSpace(q.Get("jump")); jump != "" && q.Get("before") == "" {
		parsed, jerr := parseTimelineJump(jump)
		if jerr != nil {
			httpError(w, http.StatusBadRequest, jerr.Error())
			return
		}
		cursorTS = parsed
	}

	result, err := s.timeline.QueryArgs(r.Context(), timelineListSQL,
		[]any{sources, kinds, priorities, cursorTS, cursorSeq, limit}, limit)
	if err != nil {
		s.logger.ErrorContext(r.Context(), "timeline list query failed", "error", err)
		httpError(w, http.StatusInternalServerError, "timeline query failed")
		return
	}
	items := make([]map[string]any, 0, len(result.Rows))
	for _, row := range result.Rows {
		items = append(items, timelineItemJSON(row))
	}
	response := map[string]any{"items": items, "has_more": len(items) == limit}
	if len(items) == limit {
		last := result.Rows[len(result.Rows)-1]
		response["next_cursor"] = fmt.Sprintf("%v|%v", last["event_ts"], last["seq"])
	}
	writeJSON(w, response)
}

func parseTimelineJump(raw string) (string, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02"} {
		if ts, err := time.Parse(layout, raw); err == nil {
			// End of the given instant's day for date-only jumps so the page
			// starts with that day's newest events.
			if layout == "2006-01-02" {
				ts = ts.Add(24 * time.Hour)
			}
			return ts.UTC().Format(time.RFC3339Nano), nil
		}
	}
	return "", fmt.Errorf("invalid jump timestamp")
}

func timelineItemJSON(row map[string]any) map[string]any {
	item := make(map[string]any, len(row))
	for key, value := range row {
		switch key {
		case "source_pk", "metadata":
			if raw, ok := value.(string); ok && raw != "" {
				item[key] = json.RawMessage(raw)
			} else {
				item[key] = json.RawMessage("{}")
			}
		default:
			item[key] = value
		}
	}
	return item
}

// --- sources / sync status ----------------------------------------------------

var timelineSourcesSQL = `
SELECT source, kind, count(*)::bigint AS count, min(event_ts) AS oldest, max(event_ts) AS newest
FROM ` + warehouse.SQLRelation("timeline_events") + `
GROUP BY source, kind
ORDER BY source, kind`

var timelinePrioritiesSQL = `
SELECT priority, count(*)::bigint AS count
FROM ` + warehouse.SQLRelation("timeline_events") + `
GROUP BY priority
ORDER BY priority`

var timelineSyncStateSQL = `
SELECT adapter, backfill_done, backfill_cursor_event_ts, backfill_rows, incremental_rows,
       watermark_ingest_ts, last_run_at, last_error
FROM ` + warehouse.SQLRelation("timeline_sync_state") + `
ORDER BY adapter`

type timelineFilterCatalogEntry struct {
	source string
	kind   string
}

// A cheap static catalog keeps the sidebar usable while the expensive exact
// count aggregates warm in the background. Keep this list in step with the
// Python timeline adapter registry in src/personal_data_warehouse/timeline.py.
var timelineFilterCatalog = []timelineFilterCatalogEntry{
	{source: "gmail", kind: "email"},
	{source: "slack", kind: "message"},
	{source: "slack", kind: "file_share"},
	{source: "apple_messages", kind: "message"},
	{source: "whatsapp", kind: "message"},
	{source: "apple_notes", kind: "note_edit"},
	{source: "voice_memos", kind: "voice_memo"},
	{source: "calendar", kind: "event"},
	{source: "google_drive", kind: "file_change"},
	{source: "photos", kind: "photo"},
	{source: "contacts", kind: "contact_update"},
	{source: "whoop", kind: "health_cycle"},
	{source: "whoop", kind: "recovery"},
	{source: "whoop", kind: "sleep"},
	{source: "whoop", kind: "workout"},
	{source: "mutations", kind: "mutation"},
	{source: "mutations", kind: "mutation_request"},
	{source: "warehouse", kind: "enrichment_run"},
	{source: "alice_voice_recordings", kind: "voice_recording"},
	{source: "finance", kind: "transaction"},
	{source: "finance", kind: "balance_observation"},
	{source: "finance", kind: "document"},
	{source: "chatgpt", kind: "agent_session"},
	{source: "claude_code", kind: "agent_session"},
	{source: "claude_desktop", kind: "agent_session"},
	{source: "codex", kind: "agent_session"},
	{source: "openclaw", kind: "agent_session"},
	{source: "pi", kind: "agent_session"},
}

var timelinePriorityCatalog = []int64{1, 2, 3, 4, 5}

// handleSources serves the sidebar's counts. The aggregates behind them scan
// the whole timeline (minutes on a cold multi-GB table), so the handler never
// makes a request wait for them: it serves the cached payload (refreshing in
// the background once stale), and before the first aggregate completes it
// serves a "warming" payload carrying just the fast sync-state rows so the
// page renders immediately and retries for counts.
func (s *timelineService) handleSources(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	cached := s.sourcesPayload
	stale := time.Since(s.sourcesFetched) >= timelineSourcesCacheTTL
	s.mu.Unlock()

	if cached == nil || stale || r.URL.Query().Get("refresh") != "" {
		s.refreshSourcesAsync()
	}
	if cached != nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		return
	}
	syncRows, err := s.timeline.QueryArgs(r.Context(), timelineSyncStateSQL, nil, 1000)
	if err != nil {
		s.logger.ErrorContext(r.Context(), "timeline sync state query failed", "error", err)
		httpError(w, http.StatusInternalServerError, "timeline sync state query failed")
		return
	}
	writeJSON(w, map[string]any{
		"sources":    timelineFilterCatalogRows(),
		"sync":       nonNilRows(syncRows.Rows),
		"priorities": timelinePriorityCatalogRows(),
		"warming":    true,
	})
}

func timelineFilterCatalogRows() []map[string]any {
	rows := make([]map[string]any, 0, len(timelineFilterCatalog))
	for _, entry := range timelineFilterCatalog {
		rows = append(rows, map[string]any{
			"source": entry.source,
			"kind":   entry.kind,
			"count":  nil,
			"oldest": nil,
			"newest": nil,
		})
	}
	return rows
}

func timelinePriorityCatalogRows() []map[string]any {
	rows := make([]map[string]any, 0, len(timelinePriorityCatalog))
	for _, priority := range timelinePriorityCatalog {
		rows = append(rows, map[string]any{"priority": priority, "count": nil})
	}
	return rows
}

func (s *timelineService) refreshSourcesAsync() {
	s.mu.Lock()
	if s.sourcesRefreshing {
		s.mu.Unlock()
		return
	}
	s.sourcesRefreshing = true
	s.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()
		payload, err := s.buildSourcesPayload(ctx)
		s.mu.Lock()
		s.sourcesRefreshing = false
		if err == nil {
			s.sourcesPayload = payload
			s.sourcesFetched = time.Now()
		}
		s.mu.Unlock()
		if err != nil {
			s.logger.Error("timeline sources refresh failed", "error", err)
		}
	}()
}

func (s *timelineService) slowQueryArgs(ctx context.Context, statement string, maxRows int) (query.RawResult, error) {
	if slow, ok := s.timeline.(timelineSlowQuerier); ok {
		return slow.QueryArgsWithTimeout(ctx, statement, nil, maxRows, timelineSourcesRefreshBudget)
	}
	return s.timeline.QueryArgs(ctx, statement, nil, maxRows)
}

func (s *timelineService) buildSourcesPayload(ctx context.Context) ([]byte, error) {
	sources, err := s.slowQueryArgs(ctx, timelineSourcesSQL, 10000)
	if err != nil {
		return nil, fmt.Errorf("sources aggregate: %w", err)
	}
	syncRows, err := s.timeline.QueryArgs(ctx, timelineSyncStateSQL, nil, 1000)
	if err != nil {
		return nil, fmt.Errorf("sync state: %w", err)
	}
	priorities, err := s.slowQueryArgs(ctx, timelinePrioritiesSQL, 100)
	if err != nil {
		return nil, fmt.Errorf("priorities aggregate: %w", err)
	}
	return json.Marshal(map[string]any{
		"sources":    nonNilRows(sources.Rows),
		"sync":       nonNilRows(syncRows.Rows),
		"priorities": nonNilRows(priorities.Rows),
	})
}

func nonNilRows(rows []map[string]any) []map[string]any {
	if rows == nil {
		return []map[string]any{}
	}
	return rows
}

// --- item detail ---------------------------------------------------------------

var timelineIdentifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// timelineChildQuery surfaces a "detail"-classified table's rows inside its
// parent event's inspector. Keep in sync with TIMELINE_TABLE_COVERAGE in
// src/personal_data_warehouse/timeline.py.
type timelineChildQuery struct {
	name     string
	params   []string
	pageSize int
	sql      string
}

var timelineChildQueries = map[string][]timelineChildQuery{
	"gmail_messages": {
		{
			name:   "attachments",
			params: []string{"account", "message_id"},
			sql: `SELECT part_id, filename, mime_type, size, storage_status, storage_file_id, content_sha256
			      FROM ` + warehouse.SQLRelation("gmail_attachments") + ` WHERE account = $1 AND message_id = $2
			      ORDER BY part_id`,
		},
		{
			name:   "attachment_enrichments",
			params: []string{"account", "message_id"},
			sql: `SELECT e.content_sha256, e.ai_model, left(e.text, 4000) AS text
			      FROM ` + warehouse.SQLRelation("file_attachment_enrichments") + ` e
			      JOIN ` + warehouse.SQLRelation("gmail_attachments") + ` a ON a.content_sha256 = e.content_sha256
			      WHERE a.account = $1 AND a.message_id = $2 ORDER BY e.updated_at DESC`,
		},
	},
	"slack_messages": {
		{
			name:   "reactions",
			params: []string{"account", "team_id", "conversation_id", "message_ts"},
			sql: `SELECT reaction_name, user_id, reaction_count
			      FROM ` + warehouse.SQLRelation("slack_message_reactions") + `
			      WHERE account = $1 AND team_id = $2 AND conversation_id = $3 AND message_ts = $4
			        AND is_deleted = 0
			      ORDER BY reaction_name`,
		},
		{
			name:   "thread_replies",
			params: []string{"account", "team_id", "conversation_id", "message_ts"},
			sql: `SELECT m.message_ts, m.message_datetime, m.user_id,
			             COALESCE(NULLIF(u.display_name, ''), NULLIF(u.real_name, ''), m.user_id) AS actor,
			             left(m.text, 1000) AS text
			      FROM ` + warehouse.SQLRelation("slack_messages") + ` m
			      LEFT JOIN ` + warehouse.SQLRelation("slack_users") + ` u ON u.account = m.account AND u.team_id = m.team_id AND u.user_id = m.user_id
			      WHERE m.account = $1 AND m.team_id = $2 AND m.conversation_id = $3 AND m.thread_ts = $4
			        AND m.is_deleted = 0
			      ORDER BY m.message_datetime`,
		},
		{
			// slack_files.file_id is a valid /objects/ file id: the app's Slack
			// store fetches it live from the Slack API.
			name:   "files",
			params: []string{"account", "team_id", "conversation_id", "message_ts"},
			sql: `SELECT file_id AS storage_file_id, name AS filename, title, mimetype AS mime_type, size
			      FROM ` + warehouse.SQLRelation("slack_files") + `
			      WHERE account = $1 AND team_id = $2 AND conversation_id = $3 AND message_ts = $4
			        AND is_deleted = 0
			      ORDER BY file_id`,
		},
	},
	"apple_messages": {
		{
			name:   "attachments",
			params: []string{"account", "message_id"},
			sql: `SELECT attachment_id, transfer_name AS filename,
			             COALESCE(NULLIF(content_type, ''), mime_type) AS content_type,
			             mime_type, total_bytes, is_missing,
			             storage_file_id, content_sha256
			      FROM ` + warehouse.SQLRelation("apple_message_attachments") + ` WHERE account = $1 AND message_id = $2
			      ORDER BY attachment_id`,
		},
		{
			name:   "chats",
			params: []string{"account", "message_id"},
			sql: `SELECT c.chat_id, c.display_name, c.chat_identifier, c.style
			      FROM ` + warehouse.SQLRelation("apple_message_chat_messages") + ` cm
			      JOIN ` + warehouse.SQLRelation("apple_message_chats") + ` c
			        ON c.account = cm.account AND c.chat_id = cm.chat_id
			      WHERE cm.account = $1 AND cm.message_id = $2
			      ORDER BY c.display_name, c.chat_id`,
		},
		{
			name:   "attachment_enrichments",
			params: []string{"account", "message_id"},
			sql: `SELECT e.content_sha256, e.ai_model, left(e.text, 4000) AS text
			      FROM ` + warehouse.SQLRelation("file_attachment_enrichments") + ` e
			      JOIN ` + warehouse.SQLRelation("apple_message_attachments") + ` a
			        ON a.content_sha256 = e.content_sha256
			      WHERE a.account = $1 AND a.message_id = $2
			      ORDER BY e.updated_at DESC`,
		},
	},
	"whatsapp_messages": {
		{
			name:   "media",
			params: []string{"account", "chat_id", "message_id"},
			sql: `SELECT media_type, filename, mime_type, size_bytes, is_missing,
			             storage_file_id, content_sha256
			      FROM ` + warehouse.SQLRelation("whatsapp_media_items") + `
			      WHERE account = $1 AND chat_id = $2 AND message_id = $3 ORDER BY media_type, filename`,
		},
		{
			name:   "attachment_enrichments",
			params: []string{"account", "chat_id", "message_id"},
			sql: `SELECT e.content_sha256, e.ai_model, left(e.text, 4000) AS text
			      FROM ` + warehouse.SQLRelation("file_attachment_enrichments") + ` e
			      JOIN ` + warehouse.SQLRelation("whatsapp_media_items") + ` m
			        ON m.content_sha256 = e.content_sha256
			      WHERE m.account = $1 AND m.chat_id = $2 AND m.message_id = $3
			      ORDER BY e.updated_at DESC`,
		},
	},
	"agent_session_events": {
		{
			name:     "events",
			params:   []string{"source", "session_id"},
			pageSize: 100,
			sql: `SELECT seq, occurred_at, role, event_type, tool_name, left(text, 700) AS text
			      FROM ` + warehouse.SQLRelation("agent_session_events") + ` WHERE source = $1 AND session_id = $2
			      ORDER BY seq`,
		},
	},
	"apple_note_revisions": {
		{
			name:   "attachments",
			params: []string{"account", "note_id", "revision_id"},
			sql: `SELECT attachment_id, filename, content_type, size_bytes, is_missing, storage_file_id
			      FROM ` + warehouse.SQLRelation("apple_note_attachments") + `
			      WHERE account = $1 AND note_id = $2 AND revision_id = $3 ORDER BY attachment_id`,
		},
	},
	"apple_voice_memos_files": {
		{
			name:   "enrichments",
			params: []string{"account", "recording_id"},
			sql: `SELECT provider, model, status, title, left(summary, 4000) AS summary,
			             action_items_json, calendar_event_id
			      FROM ` + warehouse.SQLRelation("apple_voice_memos_enrichments") + `
			      WHERE account = $1 AND recording_id = $2 ORDER BY created_at DESC`,
		},
		{
			name:   "transcription_runs",
			params: []string{"account", "recording_id"},
			sql: `SELECT provider, model, status, requested_at, completed_at, left(error, 500) AS error
			      FROM ` + warehouse.SQLRelation("apple_voice_memos_transcription_runs") + `
			      WHERE account = $1 AND recording_id = $2 ORDER BY requested_at DESC`,
		},
		{
			name:     "transcript_segments",
			params:   []string{"account", "recording_id"},
			pageSize: 100,
			sql: `SELECT segment_index, speaker_label, start_ms, end_ms, left(text, 700) AS text
			      FROM ` + warehouse.SQLRelation("apple_voice_memos_transcript_segments") + `
			      WHERE account = $1 AND recording_id = $2 ORDER BY segment_index`,
		},
	},
	"google_drive_files": {
		{
			name:   "extracted_texts",
			params: []string{"account", "file_id"},
			sql: `SELECT extractor, char_count, truncated, left(text, 4000) AS text
			      FROM ` + warehouse.SQLRelation("google_drive_file_texts") + `
			      WHERE account = $1 AND file_id = $2 ORDER BY extractor`,
		},
	},
	"photo_assets": {
		{
			// Every rendition of the logical photo across sources; rows carry
			// storage_file_id + mime_type so attachMedia signs inline URLs.
			name:   "renditions",
			params: []string{"photo_id"},
			sql: `SELECT source, source_native_id, role, filename, mime_type, size_bytes,
			             width, height, content_sha256, storage_file_id, match_method, match_score
			      FROM ` + warehouse.SQLRelation("photo_files") + `
			      WHERE photo_id = $1 ORDER BY source, role`,
		},
		{
			name:   "enrichments",
			params: []string{"photo_id"},
			sql: `SELECT e.content_sha256, e.ai_model, left(e.text, 4000) AS text
			      FROM ` + warehouse.SQLRelation("file_attachment_enrichments") + ` e
			      JOIN ` + warehouse.SQLRelation("photo_assets") + ` a
			        ON e.content_sha256 IN (a.thumbnail_content_sha256, a.best_file_sha256)
			      WHERE a.photo_id = $1 AND e.text != '' ORDER BY e.updated_at DESC`,
		},
	},
	"upstream_mutations": {
		{
			name:   "events",
			params: []string{"id"},
			sql: `SELECT event_index, event_type, actor_type, actor_id, created_at,
			             left(event_json::text, 2000) AS event_json
			      FROM ` + warehouse.SQLRelation("upstream_mutation_events") + ` WHERE mutation_id = $1
			      ORDER BY event_index`,
		},
	},
	"upstream_mutation_requests": {
		{
			name:   "events",
			params: []string{"id"},
			sql: `SELECT event_index, event_type, actor_type, actor_id, created_at,
			             left(event_json::text, 2000) AS event_json
			      FROM ` + warehouse.SQLRelation("upstream_mutation_request_events") + ` WHERE request_id = $1
			      ORDER BY event_index`,
		},
	},
	"agent_runs": {
		{
			name:   "tool_calls",
			params: []string{"run_id"},
			sql: `SELECT event_index, tool_name, started_at, completed_at, left(error, 500) AS error
			      FROM ` + warehouse.SQLRelation("agent_run_tool_calls") + ` WHERE run_id = $1 ORDER BY event_index`,
		},
		{
			name:   "events",
			params: []string{"run_id"},
			sql: `SELECT event_index, stream, event_type, created_at, left(text, 700) AS text
			      FROM ` + warehouse.SQLRelation("agent_run_events") + ` WHERE run_id = $1 ORDER BY event_index`,
		},
	},
	"alice_voice_recordings": {
		{
			name:   "artifacts",
			params: []string{"account", "recording_id"},
			sql: `SELECT artifact_id, kind, filename, content_type, size_bytes,
			             content_sha256, storage_file_id
			      FROM ` + warehouse.SQLRelation("alice_voice_recording_artifacts") + `
			      WHERE account = $1 AND recording_id = $2 ORDER BY kind, artifact_id`,
		},
	},
	"finance_transactions": {
		{
			name:   "source_links",
			params: []string{"transaction_id"},
			sql: `SELECT source, source_row_key, match_method, match_score, created_at
			      FROM ` + warehouse.SQLRelation("finance_transaction_links") + `
			      WHERE transaction_id = $1 ORDER BY source, source_row_key`,
		},
	},
	"finance_observations": {},
	"manual_finance_documents": {
		{
			name:   "extractions",
			params: []string{"source_native_id"},
			sql: `SELECT ai_provider, ai_model, ai_prompt_version, status, error,
			             document_type, institution, account_name_hint, account_mask,
			             period_start, period_end, currency, closing_balance,
			             left(summary, 4000) AS summary, uncertainties_json,
			             ai_processed_at, created_at
			      FROM ` + warehouse.SQLRelation("manual_finance_extractions") + `
			      WHERE content_sha256 = $1 ORDER BY created_at DESC`,
		},
	},
	"slack_files":      {},
	"calendar_events":  {},
	"contact_cards":    {},
	"whoop_cycles":     {},
	"whoop_recoveries": {},
	"whoop_sleeps":     {},
	"whoop_workouts":   {},
}

var timelineItemSQL = `
SELECT adapter, event_id, source, kind, priority, event_ts, end_ts, actor, title, snippet, context,
       source_table, source_pk::text AS source_pk, metadata::text AS metadata, seq,
       ingest_ts, first_seen_at, updated_at
FROM ` + warehouse.SQLRelation("timeline_events") + `
WHERE adapter = $1 AND event_id = $2`

type timelineChildPage struct {
	Rows       []map[string]any `json:"rows"`
	HasMore    bool             `json:"has_more"`
	NextOffset int              `json:"next_offset"`
}

func timelineSourcePointer(row map[string]any) (string, map[string]any) {
	sourceTable, _ := row["source_table"].(string)
	pkRaw, _ := row["source_pk"].(string)
	pk := map[string]any{}
	if err := json.Unmarshal([]byte(pkRaw), &pk); err != nil {
		pk = map[string]any{}
	}
	return sourceTable, pk
}

func timelineChildByName(children []timelineChildQuery, name string) (timelineChildQuery, bool) {
	for _, child := range children {
		if child.name == name {
			return child, true
		}
	}
	return timelineChildQuery{}, false
}

func timelineChildArgs(child timelineChildQuery, pk map[string]any) ([]any, bool) {
	args := make([]any, 0, len(child.params))
	for _, param := range child.params {
		value, ok := pk[param]
		if !ok {
			return nil, false
		}
		args = append(args, value)
	}
	return args, true
}

func (s *timelineService) fetchChildPage(
	ctx context.Context,
	child timelineChildQuery,
	pk map[string]any,
	offset int,
) (timelineChildPage, error) {
	args, ok := timelineChildArgs(child, pk)
	if !ok {
		return timelineChildPage{}, fmt.Errorf("source primary key is missing fields for %s", child.name)
	}
	pageSize := child.pageSize
	if pageSize <= 0 {
		pageSize = 50
	}
	limitParam := len(args) + 1
	offsetParam := len(args) + 2
	statement := fmt.Sprintf("%s LIMIT $%d OFFSET $%d", child.sql, limitParam, offsetParam)
	args = append(args, pageSize+1, offset)
	result, err := s.source.QueryArgs(ctx, statement, args, pageSize+1)
	if err != nil {
		return timelineChildPage{}, err
	}
	hasMore := len(result.Rows) > pageSize
	rows := result.Rows
	if hasMore {
		rows = rows[:pageSize]
	}
	for _, row := range rows {
		s.attachMedia(row)
	}
	return timelineChildPage{
		Rows:       nonNilRows(rows),
		HasMore:    hasMore,
		NextOffset: offset + len(rows),
	}, nil
}

func (s *timelineService) handleItem(w http.ResponseWriter, r *http.Request) {
	adapter := r.URL.Query().Get("adapter")
	eventID := r.URL.Query().Get("event_id")
	if adapter == "" || eventID == "" {
		httpError(w, http.StatusBadRequest, "adapter and event_id are required")
		return
	}
	result, err := s.timeline.QueryArgs(r.Context(), timelineItemSQL, []any{adapter, eventID}, 1)
	if err != nil {
		s.logger.ErrorContext(r.Context(), "timeline item query failed", "error", err)
		httpError(w, http.StatusInternalServerError, "timeline item query failed")
		return
	}
	if len(result.Rows) == 0 {
		httpError(w, http.StatusNotFound, "timeline item not found")
		return
	}
	row := result.Rows[0]
	item := timelineItemJSON(row)
	sourceTable, pk := timelineSourcePointer(row)

	response := map[string]any{"item": item}
	children, known := timelineChildQueries[sourceTable]
	if !known {
		// A source table the UI does not know children for yet; still return
		// the timeline row and the raw source row below when possible.
		s.logger.WarnContext(r.Context(), "timeline item for unmapped source table", "source_table", sourceTable)
	}

	if timelineIdentifierPattern.MatchString(sourceTable) && sourceTable != "agent_session_events" {
		if sourceRow, srcErr := s.fetchSourceRow(r.Context(), sourceTable, pk); srcErr != nil {
			response["source_row_error"] = srcErr.Error()
		} else {
			response["source_row"] = sourceRow
		}
	}

	childResults := map[string]any{}
	childMetadata := map[string]any{}
	for _, child := range children {
		if _, ok := timelineChildArgs(child, pk); !ok {
			continue
		}
		page, childErr := s.fetchChildPage(r.Context(), child, pk, 0)
		if childErr != nil {
			childResults[child.name] = map[string]any{"error": childErr.Error()}
			continue
		}
		childResults[child.name] = page.Rows
		childMetadata[child.name] = map[string]any{
			"has_more":    page.HasMore,
			"next_offset": page.NextOffset,
		}
	}
	response["children"] = childResults
	response["children_meta"] = childMetadata
	if media := s.itemMedia(sourceTable, response["source_row"]); media != nil {
		response["item_media"] = media
	}
	writeJSON(w, response)
}

func (s *timelineService) handleItemChildren(w http.ResponseWriter, r *http.Request) {
	adapter := r.URL.Query().Get("adapter")
	eventID := r.URL.Query().Get("event_id")
	childName := r.URL.Query().Get("child")
	if adapter == "" || eventID == "" || childName == "" {
		httpError(w, http.StatusBadRequest, "adapter, event_id, and child are required")
		return
	}
	offset := 0
	if raw := r.URL.Query().Get("offset"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			httpError(w, http.StatusBadRequest, "offset must be a non-negative integer")
			return
		}
		offset = parsed
	}
	result, err := s.timeline.QueryArgs(r.Context(), timelineItemSQL, []any{adapter, eventID}, 1)
	if err != nil {
		s.logger.ErrorContext(r.Context(), "timeline item query failed", "error", err)
		httpError(w, http.StatusInternalServerError, "timeline item query failed")
		return
	}
	if len(result.Rows) == 0 {
		httpError(w, http.StatusNotFound, "timeline item not found")
		return
	}
	sourceTable, pk := timelineSourcePointer(result.Rows[0])
	child, ok := timelineChildByName(timelineChildQueries[sourceTable], childName)
	if !ok {
		httpError(w, http.StatusNotFound, "timeline child collection not found")
		return
	}
	page, err := s.fetchChildPage(r.Context(), child, pk, offset)
	if err != nil {
		s.logger.ErrorContext(r.Context(), "timeline child query failed", "error", err)
		httpError(w, http.StatusInternalServerError, "timeline child query failed")
		return
	}
	writeJSON(w, page)
}

// attachMedia decorates a row that references a stored blob with a signed
// download URL and a coarse render kind for the UI.
func (s *timelineService) attachMedia(row map[string]any) {
	fileID, _ := row["storage_file_id"].(string)
	if fileID == "" {
		return
	}
	if missing, ok := row["is_missing"].(int64); ok && missing != 0 {
		return
	}
	url := s.media.signURL(fileID)
	if url == "" {
		return
	}
	contentType := ""
	// content_type is the normalized value for sources such as iMessage;
	// prefer it over a generic provider mime_type fallback.
	for _, key := range []string{"content_type", "mime_type", "mimetype"} {
		if value, ok := row[key].(string); ok && value != "" {
			contentType = value
			break
		}
	}
	row["media_url"] = url
	row["media_kind"] = timelineMediaKind(contentType)
}

func timelineMediaKind(contentType string) string {
	switch {
	case strings.HasPrefix(contentType, "image/"):
		return "image"
	case strings.HasPrefix(contentType, "audio/"):
		return "audio"
	case strings.HasPrefix(contentType, "video/"):
		return "video"
	case contentType == "application/pdf":
		return "pdf"
	default:
		return "file"
	}
}

// itemMedia surfaces the event's own blob (a voice memo's audio, a Slack
// file share's file, a Drive file's stored copy) from the fetched source row.
var timelineItemMediaColumns = map[string][2]string{
	"apple_voice_memos_files":  {"storage_file_id", "content_type"},
	"alice_voice_recordings":   {"storage_file_id", "content_type"},
	"manual_finance_documents": {"storage_file_id", "mime_type"},
	"slack_files":              {"file_id", "mimetype"},
	"google_drive_files":       {"storage_file_id", "mime_type"},
	"photo_assets":             {"thumbnail_storage_file_id", "thumbnail_content_type"},
}

func (s *timelineService) itemMedia(sourceTable string, sourceRow any) map[string]any {
	columns, ok := timelineItemMediaColumns[sourceTable]
	if !ok {
		return nil
	}
	raw, ok := sourceRow.(json.RawMessage)
	if !ok {
		return nil
	}
	var row map[string]any
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil
	}
	fileID, _ := row[columns[0]].(string)
	if fileID == "" {
		return nil
	}
	url := s.media.signURL(fileID)
	if url == "" {
		return nil
	}
	contentType, _ := row[columns[1]].(string)
	filename, _ := row["filename"].(string)
	if filename == "" {
		filename, _ = row["name"].(string)
	}
	return map[string]any{
		"media_url":  url,
		"media_kind": timelineMediaKind(contentType),
		"mime_type":  contentType,
		"filename":   filename,
	}
}

func (s *timelineService) fetchSourceRow(ctx context.Context, table string, pk map[string]any) (json.RawMessage, error) {
	if len(pk) == 0 {
		return nil, fmt.Errorf("no source primary key recorded")
	}
	keys := make([]string, 0, len(pk))
	for key := range pk {
		if !timelineIdentifierPattern.MatchString(key) {
			return nil, fmt.Errorf("invalid source key %q", key)
		}
		keys = append(keys, key)
	}
	// jsonb object keys come back sorted already, but do not rely on it.
	sortStrings(keys)
	clauses := make([]string, 0, len(keys))
	args := make([]any, 0, len(keys))
	if _, ok := warehouse.Relations[table]; !ok {
		return nil, fmt.Errorf("unknown source table %q", table)
	}
	for i, key := range keys {
		clauses = append(clauses, fmt.Sprintf("%s = $%d", warehouse.QuoteIdent(key), i+1))
		args = append(args, pk[key])
	}
	sql := fmt.Sprintf("SELECT row_to_json(t)::text AS row FROM %s t WHERE %s LIMIT 1",
		warehouse.SQLRelation(table), strings.Join(clauses, " AND "))
	result, err := s.source.QueryArgs(ctx, sql, args, 1)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("source row not found")
	}
	raw, _ := result.Rows[0]["row"].(string)
	truncated, err := truncateJSONStrings([]byte(raw), timelineDetailFieldChars)
	if err != nil {
		return nil, err
	}
	return truncated, nil
}

// truncateJSONStrings caps every string value in a JSON document so a
// multi-megabyte source row (gmail body_html, drive raw metadata) stays
// inspectable without shipping the whole blob to the browser.
func truncateJSONStrings(raw []byte, maxChars int) (json.RawMessage, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	capped := capJSONStrings(value, maxChars)
	out, err := json.Marshal(capped)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func capJSONStrings(value any, maxChars int) any {
	switch v := value.(type) {
	case string:
		if len(v) > maxChars {
			return v[:maxChars] + "… (truncated)"
		}
		return v
	case map[string]any:
		for key, item := range v {
			v[key] = capJSONStrings(item, maxChars)
		}
		return v
	case []any:
		for i, item := range v {
			v[i] = capJSONStrings(item, maxChars)
		}
		return v
	default:
		return v
	}
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

// --- plumbing -------------------------------------------------------------------

func (s *timelineService) registerRoutes(mux *http.ServeMux, requireAuth func(http.Handler) http.Handler) {
	mux.Handle("/api/timeline", requireAuth(http.HandlerFunc(s.handleList)))
	mux.Handle("/api/timeline/sources", requireAuth(http.HandlerFunc(s.handleSources)))
	mux.Handle("/api/timeline/item", requireAuth(http.HandlerFunc(s.handleItem)))
	mux.Handle("/api/timeline/item/children", requireAuth(http.HandlerFunc(s.handleItemChildren)))
	mux.HandleFunc("/timeline", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// The shell is tiny and iterated on; never let a browser cache a stale copy.
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte(timelinePageHTML))
	})
}

func httpError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
