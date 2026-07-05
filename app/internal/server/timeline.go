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
)

// timelineQuerier is the parameterized-query capability the timeline endpoints
// need. *query.PostgresRunner implements it; the plain fake runner used by
// unrelated tests does not, in which case the timeline endpoints simply are
// not registered.
type timelineQuerier interface {
	QueryArgs(ctx context.Context, statement string, args []any, maxRows int) (query.RawResult, error)
}

const (
	timelineDefaultPageSize   = 60
	timelineMaxPageSize       = 200
	timelineSourcesCacheTTL   = 5 * time.Minute
	timelineDetailFieldChars  = 50000
	timelineChildRowFieldChar = 4000
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

const timelineListSQL = `
SELECT adapter, event_id, source, kind, priority, event_ts, end_ts, actor, title, snippet, context,
       source_table, source_pk::text AS source_pk, metadata::text AS metadata, seq
FROM timeline_events
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

const timelineSourcesSQL = `
SELECT source, kind, count(*)::bigint AS count, min(event_ts) AS oldest, max(event_ts) AS newest
FROM timeline_events
GROUP BY source, kind
ORDER BY source, kind`

const timelinePrioritiesSQL = `
SELECT priority, count(*)::bigint AS count
FROM timeline_events
GROUP BY priority
ORDER BY priority`

const timelineSyncStateSQL = `
SELECT adapter, backfill_done, backfill_cursor_event_ts, backfill_rows, incremental_rows,
       watermark_ingest_ts, last_run_at, last_error
FROM timeline_sync_state
ORDER BY adapter`

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
		"sources":    []any{},
		"sync":       nonNilRows(syncRows.Rows),
		"priorities": []any{},
		"warming":    true,
	})
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

func (s *timelineService) buildSourcesPayload(ctx context.Context) ([]byte, error) {
	sources, err := s.timeline.QueryArgs(ctx, timelineSourcesSQL, nil, 10000)
	if err != nil {
		return nil, fmt.Errorf("sources aggregate: %w", err)
	}
	syncRows, err := s.timeline.QueryArgs(ctx, timelineSyncStateSQL, nil, 1000)
	if err != nil {
		return nil, fmt.Errorf("sync state: %w", err)
	}
	priorities, err := s.timeline.QueryArgs(ctx, timelinePrioritiesSQL, nil, 100)
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
	name   string
	params []string
	sql    string
}

var timelineChildQueries = map[string][]timelineChildQuery{
	"gmail_messages": {
		{
			name:   "attachments",
			params: []string{"account", "message_id"},
			sql: `SELECT part_id, filename, mime_type, size, storage_status, storage_file_id, content_sha256
			      FROM gmail_attachments WHERE account = $1 AND message_id = $2
			      ORDER BY part_id LIMIT 50`,
		},
		{
			name:   "attachment_enrichments",
			params: []string{"account", "message_id"},
			sql: `SELECT e.content_sha256, e.ai_model, left(e.text, 4000) AS text
			      FROM file_attachment_enrichments e
			      JOIN gmail_attachments a ON a.content_sha256 = e.content_sha256
			      WHERE a.account = $1 AND a.message_id = $2 LIMIT 20`,
		},
	},
	"slack_messages": {
		{
			name:   "reactions",
			params: []string{"account", "team_id", "conversation_id", "message_ts"},
			sql: `SELECT reaction_name, user_id, reaction_count
			      FROM slack_message_reactions
			      WHERE account = $1 AND team_id = $2 AND conversation_id = $3 AND message_ts = $4
			        AND is_deleted = 0
			      ORDER BY reaction_name LIMIT 100`,
		},
		{
			name:   "thread_replies",
			params: []string{"account", "team_id", "conversation_id", "message_ts"},
			sql: `SELECT m.message_ts, m.message_datetime, m.user_id,
			             COALESCE(NULLIF(u.display_name, ''), NULLIF(u.real_name, ''), m.user_id) AS actor,
			             left(m.text, 1000) AS text
			      FROM slack_messages m
			      LEFT JOIN slack_users u ON u.account = m.account AND u.team_id = m.team_id AND u.user_id = m.user_id
			      WHERE m.account = $1 AND m.team_id = $2 AND m.conversation_id = $3 AND m.thread_ts = $4
			        AND m.is_deleted = 0
			      ORDER BY m.message_datetime LIMIT 50`,
		},
		{
			// slack_files.file_id is a valid /objects/ file id: the app's Slack
			// store fetches it live from the Slack API.
			name:   "files",
			params: []string{"account", "team_id", "conversation_id", "message_ts"},
			sql: `SELECT file_id AS storage_file_id, name AS filename, title, mimetype AS mime_type, size
			      FROM slack_files
			      WHERE account = $1 AND team_id = $2 AND conversation_id = $3 AND message_ts = $4
			        AND is_deleted = 0
			      ORDER BY file_id LIMIT 20`,
		},
	},
	"apple_messages": {
		{
			name:   "attachments",
			params: []string{"account", "message_id"},
			sql: `SELECT attachment_id, transfer_name AS filename, mime_type, total_bytes, is_missing,
			             storage_file_id, content_sha256
			      FROM apple_message_attachments WHERE account = $1 AND message_id = $2
			      ORDER BY attachment_id LIMIT 50`,
		},
	},
	"whatsapp_messages": {
		{
			name:   "media",
			params: []string{"account", "chat_id", "message_id"},
			sql: `SELECT media_type, filename, mime_type, size_bytes, is_missing,
			             storage_file_id, content_sha256
			      FROM whatsapp_media_items
			      WHERE account = $1 AND chat_id = $2 AND message_id = $3 LIMIT 20`,
		},
	},
	"agent_session_events": {
		{
			name:   "events_head",
			params: []string{"source", "session_id"},
			sql: `SELECT seq, occurred_at, role, event_type, tool_name, left(text, 700) AS text
			      FROM agent_session_events WHERE source = $1 AND session_id = $2
			      ORDER BY seq LIMIT 100`,
		},
		{
			name:   "events_tail",
			params: []string{"source", "session_id"},
			sql: `SELECT seq, occurred_at, role, event_type, tool_name, left(text, 700) AS text
			      FROM agent_session_events WHERE source = $1 AND session_id = $2
			      ORDER BY seq DESC LIMIT 50`,
		},
	},
	"apple_note_revisions": {
		{
			name:   "attachments",
			params: []string{"account", "note_id", "revision_id"},
			sql: `SELECT attachment_id, filename, content_type, size_bytes, is_missing, storage_file_id
			      FROM apple_note_attachments
			      WHERE account = $1 AND note_id = $2 AND revision_id = $3 LIMIT 50`,
		},
	},
	"apple_voice_memos_files": {
		{
			name:   "enrichments",
			params: []string{"account", "recording_id"},
			sql: `SELECT provider, model, status, title, left(summary, 4000) AS summary,
			             action_items_json, calendar_event_id
			      FROM apple_voice_memos_enrichments
			      WHERE account = $1 AND recording_id = $2 ORDER BY created_at DESC LIMIT 5`,
		},
		{
			name:   "transcription_runs",
			params: []string{"account", "recording_id"},
			sql: `SELECT provider, model, status, requested_at, completed_at, left(error, 500) AS error
			      FROM apple_voice_memos_transcription_runs
			      WHERE account = $1 AND recording_id = $2 LIMIT 10`,
		},
		{
			name:   "transcript_segments",
			params: []string{"account", "recording_id"},
			sql: `SELECT segment_index, speaker_label, start_ms, end_ms, left(text, 700) AS text
			      FROM apple_voice_memos_transcript_segments
			      WHERE account = $1 AND recording_id = $2 ORDER BY segment_index LIMIT 100`,
		},
	},
	"google_drive_files": {
		{
			name:   "extracted_texts",
			params: []string{"account", "file_id"},
			sql: `SELECT extractor, char_count, truncated, left(text, 4000) AS text
			      FROM google_drive_file_texts
			      WHERE account = $1 AND file_id = $2 LIMIT 5`,
		},
	},
	"upstream_mutations": {
		{
			name:   "events",
			params: []string{"id"},
			sql: `SELECT event_index, event_type, actor_type, actor_id, created_at,
			             left(event_json::text, 2000) AS event_json
			      FROM upstream_mutation_events WHERE mutation_id = $1
			      ORDER BY event_index LIMIT 100`,
		},
	},
	"upstream_mutation_requests": {
		{
			name:   "events",
			params: []string{"id"},
			sql: `SELECT event_index, event_type, actor_type, actor_id, created_at,
			             left(event_json::text, 2000) AS event_json
			      FROM upstream_mutation_request_events WHERE request_id = $1
			      ORDER BY event_index LIMIT 100`,
		},
	},
	"agent_runs": {
		{
			name:   "tool_calls",
			params: []string{"run_id"},
			sql: `SELECT event_index, tool_name, started_at, completed_at, left(error, 500) AS error
			      FROM agent_run_tool_calls WHERE run_id = $1 ORDER BY event_index LIMIT 100`,
		},
		{
			name:   "events_head",
			params: []string{"run_id"},
			sql: `SELECT event_index, stream, event_type, created_at, left(text, 700) AS text
			      FROM agent_run_events WHERE run_id = $1 ORDER BY event_index LIMIT 100`,
		},
	},
	"slack_files":                {},
	"calendar_events":            {},
	"contact_cards":              {},
}

const timelineItemSQL = `
SELECT adapter, event_id, source, kind, priority, event_ts, end_ts, actor, title, snippet, context,
       source_table, source_pk::text AS source_pk, metadata::text AS metadata, seq,
       ingest_ts, first_seen_at, updated_at
FROM timeline_events
WHERE adapter = $1 AND event_id = $2`

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
	sourceTable, _ := row["source_table"].(string)
	pkRaw, _ := row["source_pk"].(string)
	var pk map[string]any
	if err := json.Unmarshal([]byte(pkRaw), &pk); err != nil {
		pk = map[string]any{}
	}

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
	for _, child := range children {
		args := make([]any, 0, len(child.params))
		missing := false
		for _, param := range child.params {
			value, ok := pk[param]
			if !ok {
				missing = true
				break
			}
			args = append(args, value)
		}
		if missing {
			continue
		}
		childRows, childErr := s.source.QueryArgs(r.Context(), child.sql, args, 500)
		if childErr != nil {
			childResults[child.name] = map[string]any{"error": childErr.Error()}
			continue
		}
		for _, childRow := range childRows.Rows {
			s.attachMedia(childRow)
		}
		childResults[child.name] = nonNilRows(childRows.Rows)
	}
	response["children"] = childResults
	if media := s.itemMedia(sourceTable, response["source_row"]); media != nil {
		response["item_media"] = media
	}
	writeJSON(w, response)
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
	for _, key := range []string{"mime_type", "content_type", "mimetype"} {
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
	"apple_voice_memos_files": {"storage_file_id", "content_type"},
	"slack_files":             {"file_id", "mimetype"},
	"google_drive_files":      {"storage_file_id", "mime_type"},
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
	for i, key := range keys {
		clauses = append(clauses, fmt.Sprintf("%s = $%d", key, i+1))
		args = append(args, pk[key])
	}
	sql := fmt.Sprintf("SELECT row_to_json(t)::text AS row FROM %s t WHERE %s LIMIT 1",
		table, strings.Join(clauses, " AND "))
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
