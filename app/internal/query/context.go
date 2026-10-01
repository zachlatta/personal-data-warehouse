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

// ContextRequest asks for the conversation around one search hit.
type ContextRequest struct {
	Ref    string
	Before int
	After  int
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
	raw, err := runner.QueryArgs(ctx, contextSQL, []any{resp.Ref, req.Before, req.After}, 0)
	if err != nil {
		resp.Error = s.queryErrorMessage(ctx, err.Error(), contextSQL)
		return resp
	}
	resp.Rows = raw.Rows
	resp.TotalRows = len(raw.Rows)
	return resp
}
