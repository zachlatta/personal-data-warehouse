package query

import (
	"context"
	"fmt"
	"strings"
)

// ContextMaxWindow is timeline.context()'s own cap on each side of a hit.
const ContextMaxWindow = 50

// ContextDefaultWindow is how many events either side a caller gets unasked.
const ContextDefaultWindow = 5

// contextSQL names the columns a reader uses. `SELECT *` from
// timeline.context() returned metadata and the full search_text of every row:
// 3.8 KB against 0.5 KB, measured over twelve sessions on 2026-09-22.
const contextSQL = "SELECT adapter, event_id, priority::text AS priority, event_ts, actor, title, snippet " +
	"FROM timeline.context($1, $2::integer, $3::integer)"

// contextFullSQL is the --full read: each event's whole indexed text (the
// same document search matches against), capped per event, plus the source
// row's coordinates so the response can name a drill statement for anything
// longer than the cap.
const contextFullSQL = "SELECT adapter, event_id, priority::text AS priority, event_ts, actor, title, " +
	"left(search_text, $4::integer) AS text, length(search_text) AS text_chars, source_table, source_pk " +
	"FROM timeline.context($1, $2::integer, $3::integer)"

// ContextFullTextMaxChars caps one event's text in a --full read. A Gmail
// event averages 8.7 KB of text and a voice memo runs to 77 KB; past the cap
// the row's drill_sql reads the rest.
const ContextFullTextMaxChars = 12000

// ContextRequest asks for the conversation around one search hit. Full
// returns each event's whole text (up to ContextFullTextMaxChars) instead of
// its stored preview.
type ContextRequest struct {
	Ref    string
	Before int
	After  int
	Full   bool
}

// ContextResponse is the conversation around a hit, oldest first.
type ContextResponse struct {
	Ref       string           `json:"ref"`
	Before    int              `json:"before"`
	After     int              `json:"after"`
	TotalRows int              `json:"total_rows"`
	Rows      []map[string]any `json:"rows"`
	Error     string           `json:"error,omitempty"`
}

// Context is the one way to read the conversation around a hit, on both
// surfaces: the `context` tool over MCP and `pdw context` on the CLI. A Gmail
// hit returns its thread, a Slack hit its thread or channel, a chat hit the
// rest of that chat, an agent turn its neighbouring turns.
func (s *Service) Context(ctx context.Context, req ContextRequest) ContextResponse {
	resp := ContextResponse{Ref: strings.TrimSpace(req.Ref), Before: req.Before, After: req.After, Rows: []map[string]any{}}
	if resp.Ref == "" {
		resp.Error = "ref must be a search hit's ref, e.g. gmail_email:<account>|<message_id>"
		return resp
	}
	if req.Before < 0 || req.After < 0 || req.Before > ContextMaxWindow || req.After > ContextMaxWindow {
		resp.Error = fmt.Sprintf("before and after must be between 0 and %d", ContextMaxWindow)
		return resp
	}
	runner, ok := s.runner.(ArgsRunner)
	if !ok {
		resp.Error = "context requires a parameterized-query runner"
		return resp
	}
	statement, args := contextSQL, []any{resp.Ref, req.Before, req.After}
	if req.Full {
		statement, args = contextFullSQL, append(args, ContextFullTextMaxChars)
	}
	raw, err := runner.QueryArgs(ctx, statement, args, 0)
	if err != nil {
		resp.Error = s.queryErrorMessage(ctx, err.Error(), statement)
		return resp
	}
	if req.Full {
		for _, row := range raw.Rows {
			table, _ := row["source_table"].(string)
			if drill := DrillSQL(table, row["source_pk"]); drill != "" {
				row["drill_sql"] = drill
			}
			delete(row, "source_table")
			delete(row, "source_pk")
		}
	}
	resp.Rows = raw.Rows
	resp.TotalRows = len(raw.Rows)
	return resp
}
