package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
)

// contextUsage is the command's own help.
var contextUsage = `pdw context - the conversation around a search hit.

USAGE
  pdw context [flags] REF

REF is a hit's ref from pdw search (gmail_email:..., slack_message:...,
apple_message:..., agent_session_turn:...). A Gmail hit returns its thread, a
Slack hit its thread or channel, a chat hit the rest of that chat, an agent
turn its neighbouring turns; everything else returns the neighbouring events
of the same stream. It runs timeline.context(ref, before, after) and prints
one line per event, oldest first.

FLAGS
  -b, --before N   Events before the hit (default 5, max 50).
  -a, --after N    Events after the hit (default 5, max 50).
  --refs           Append each event's own ref, for a further hop.
  --output FMT     text (default) or json.
`

const (
	contextDefaultWindow = 5
	contextMaxWindow     = 50
	contextSnippetRunes  = 400
)

type contextRow struct {
	Adapter  string `json:"adapter"`
	EventID  string `json:"event_id"`
	Priority string `json:"priority"`
	EventTS  string `json:"event_ts"`
	Actor    string `json:"actor"`
	Title    string `json:"title"`
	Snippet  string `json:"snippet"`
}

// runContext is the first-class spelling of `SELECT ... FROM
// timeline.context(ref, b, a)`. Measured over twelve real sessions the SQL
// form was written with `SELECT *` every time (the guide's example did), which
// returns metadata and the full search_text of every row -- 3.8 KB against
// 0.5 KB for the three columns anyone reads -- and was used a quarter as
// often as search because the ref has to be quoted inside a quoted statement.
func runContext(client *cliclient.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("context", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var before, after int
	var refs bool
	var output string
	fs.IntVar(&before, "before", contextDefaultWindow, "events before the hit")
	fs.IntVar(&before, "b", contextDefaultWindow, "alias for --before")
	fs.IntVar(&after, "after", contextDefaultWindow, "events after the hit")
	fs.IntVar(&after, "a", contextDefaultWindow, "alias for --after")
	fs.BoolVar(&refs, "refs", false, "append each event's ref")
	fs.StringVar(&output, "output", "text", "text or json")
	ordered, err := searchFlagsFirst(fs, args)
	if err != nil {
		fmt.Fprintln(stderr, "pdw context:", err)
		return 2
	}
	if err := fs.Parse(ordered); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stdout, contextUsage)
			return 0
		}
		fmt.Fprintln(stderr, "pdw context:", err)
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "pdw context: exactly one ref is required (the ref: line of a search hit). Example: pdw context 'slack_message:<account>|<team>|<conversation>|<ts>'")
		return 2
	}
	ref := strings.TrimSpace(fs.Arg(0))
	if ref == "" || strings.ContainsAny(ref, "'\\\n") {
		fmt.Fprintln(stderr, "pdw context: ref must be a search hit's ref, e.g. gmail_email:<account>|<message_id>")
		return 2
	}
	if before < 0 || after < 0 || before > contextMaxWindow || after > contextMaxWindow {
		fmt.Fprintf(stderr, "pdw context: --before and --after must be between 0 and %d\n", contextMaxWindow)
		return 2
	}
	if output != "text" && output != "json" {
		fmt.Fprintln(stderr, "pdw context: --output must be text or json")
		return 2
	}
	sql := fmt.Sprintf("SELECT adapter, event_id, priority, event_ts, actor, title, snippet FROM timeline.context('%s', %d, %d)", ref, before, after)
	input, err := json.Marshal(sqlCommandInput{Question: "conversation around " + ref, SQL: sql, Format: "json"})
	if err != nil {
		fmt.Fprintln(stderr, "pdw context:", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultSQLTimeout)
	defer cancel()
	out, err := client.CallTool(ctx, "sql", input)
	if err != nil {
		var apiErr *cliclient.APIError
		if errors.As(err, &apiErr) {
			fmt.Fprintf(stderr, "pdw context: %s (http %d): %s\n", apiErr.Code, apiErr.Status, apiErr.Message)
			return 1
		}
		fmt.Fprintln(stderr, "pdw context:", err)
		return 1
	}
	var payload sqlCommandResponse
	if err := json.Unmarshal(out, &payload); err != nil {
		fmt.Fprintln(stdout, string(out))
		return 0
	}
	if payload.Error != "" {
		fmt.Fprintln(stderr, "pdw context:", payload.Error)
		return 1
	}
	if output == "json" {
		return printSQLRows(payload.Rows, "json", stdout)
	}
	var rows []contextRow
	if err := json.Unmarshal(payload.Rows, &rows); err != nil {
		fmt.Fprintln(stdout, string(payload.Rows))
		return 0
	}
	fmt.Fprintf(stdout, "Context: %s (%d before, %d after) — %d events\n", ref, before, after, len(rows))
	for _, row := range rows {
		line := strings.Join(nonemptySearchParts(shortSearchTime(row.EventTS), row.Priority, row.Actor), " · ")
		title := strings.TrimSpace(row.Title)
		body := strings.TrimSpace(row.Snippet)
		if title != "" && strings.HasPrefix(body, title) {
			body = strings.TrimSpace(strings.TrimPrefix(body, title))
		}
		text := []string{}
		if title != "" {
			text = append(text, compactSearchLine(title, searchBriefTitleRunes))
		}
		if body != "" && body != title {
			text = append(text, compactSearchLine(body, contextSnippetRunes))
		}
		if len(text) > 0 {
			line += " — " + strings.Join(text, " | ")
		}
		if refs && row.Adapter != "" {
			line += "  [" + row.Adapter + ":" + row.EventID + "]"
		}
		fmt.Fprintln(stdout, line)
	}
	return 0
}
