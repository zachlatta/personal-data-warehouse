package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
)

const cliSearchDefaultMaxResults = 20

type searchSourcesFlag []string

func (s *searchSourcesFlag) String() string { return strings.Join(*s, ",") }

func (s *searchSourcesFlag) Set(value string) error {
	for _, source := range strings.Split(value, ",") {
		if source = strings.TrimSpace(source); source != "" {
			*s = append(*s, source)
		}
	}
	return nil
}

type cliSearchResponse struct {
	Query          string         `json:"query"`
	Mode           string         `json:"mode"`
	FallbackReason string         `json:"fallback_reason,omitempty"`
	Hint           string         `json:"hint,omitempty"`
	Guidance       string         `json:"guidance,omitempty"`
	TotalRows      int            `json:"total_rows"`
	Rows           []cliSearchHit `json:"rows,omitempty"`
	Error          string         `json:"error,omitempty"`
}

type cliSearchHit struct {
	Source     string `json:"source"`
	OccurredAt string `json:"occurred_at"`
	Who        string `json:"who"`
	Title      string `json:"title"`
	Text       string `json:"text"`
	Ref        string `json:"ref"`
}

func runSearch(client *cliclient.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	mode := fs.String("mode", "hybrid", "hybrid, keyword, or exact")
	maxResults := fs.Int("max-results", cliSearchDefaultMaxResults, "maximum hits")
	fs.IntVar(maxResults, "n", cliSearchDefaultMaxResults, "alias for --max-results")
	since := fs.String("since", "", "event-time lower bound")
	output := fs.String("output", "text", "text or json")
	var sources searchSourcesFlag
	fs.Var(&sources, "source", "source aliases, comma-separated; repeatable")
	fs.Var(&sources, "sources", "alias for --source")
	if err := fs.Parse(searchFlagsFirst(args)); err != nil {
		fmt.Fprintln(stderr, "pdw search:", err)
		return 2
	}
	queryText := strings.TrimSpace(strings.Join(fs.Args(), " "))
	if queryText == "" {
		fmt.Fprintln(stderr, "pdw search: query is required (example: pdw search 'budget approval')")
		return 2
	}
	if *maxResults <= 0 {
		fmt.Fprintln(stderr, "pdw search: --max-results must be greater than zero")
		return 2
	}
	if *output != "text" && *output != "json" {
		fmt.Fprintln(stderr, "pdw search: --output must be text or json")
		return 2
	}

	input, err := json.Marshal(map[string]any{
		"query": queryText, "mode": *mode, "max_results": *maxResults,
		"sources": []string(sources), "since": *since,
	})
	if err != nil {
		fmt.Fprintln(stderr, "pdw search:", err)
		return 1
	}
	raw, err := client.CallTool(context.Background(), "search", input)
	if err != nil {
		fmt.Fprintln(stderr, "pdw search:", err)
		return 1
	}
	var resp cliSearchResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		fmt.Fprintln(stderr, "pdw search: decode response:", err)
		return 1
	}
	if resp.Error != "" {
		fmt.Fprintln(stderr, "pdw search:", resp.Error)
		return 1
	}
	if *output == "json" {
		pretty, err := prettyJSON(raw)
		if err != nil {
			fmt.Fprintln(stderr, "pdw search: encode response:", err)
			return 1
		}
		fmt.Fprintln(stdout, pretty)
		return 0
	}
	printSearchText(stdout, resp)
	return 0
}

// The standard flag package stops parsing at the first positional argument,
// but agents naturally write both `pdw search --source gmail terms` and
// `pdw search terms --source gmail`. Move this command's known value flags to
// the front so both forms have identical semantics.
func searchFlagsFirst(args []string) []string {
	known := map[string]bool{
		"--mode": true, "--max-results": true, "-n": true,
		"--since": true, "--output": true, "--source": true, "--sources": true,
	}
	flags := make([]string, 0, len(args))
	positionals := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		name := arg
		if before, _, ok := strings.Cut(arg, "="); ok {
			name = before
		}
		if known[name] {
			flags = append(flags, arg)
			if !strings.Contains(arg, "=") && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
			continue
		}
		positionals = append(positionals, arg)
	}
	return append(flags, positionals...)
}

func printSearchText(w io.Writer, resp cliSearchResponse) {
	noun := "results"
	if resp.TotalRows == 1 {
		noun = "result"
	}
	fmt.Fprintf(w, "Search: %q — %d %s (%s)\n", resp.Query, resp.TotalRows, noun, resp.Mode)
	if resp.FallbackReason != "" {
		fmt.Fprintf(w, "Fallback: %s\n", resp.FallbackReason)
	}
	for i, hit := range resp.Rows {
		fmt.Fprintf(w, "\n%d. %s\n", i+1, strings.Join(nonemptySearchParts(hit.Source, hit.OccurredAt, hit.Who), " · "))
		if title := strings.TrimSpace(hit.Title); title != "" && title != strings.TrimSpace(hit.Text) {
			fmt.Fprintf(w, "   %s\n", compactSearchLine(title))
		}
		if body := strings.TrimSpace(hit.Text); body != "" {
			fmt.Fprintf(w, "   %s\n", compactSearchLine(body))
		}
		if hit.Ref != "" {
			fmt.Fprintf(w, "   ref: %s\n", hit.Ref)
		}
	}
	if resp.Hint != "" {
		fmt.Fprintf(w, "\nHint: %s\n", resp.Hint)
	}
	if resp.Guidance != "" {
		fmt.Fprintf(w, "\nNext: %s\n", resp.Guidance)
	}
}

func nonemptySearchParts(parts ...string) []string {
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func compactSearchLine(value string) string {
	const maxRunes = 800
	line := strings.Join(strings.Fields(value), " ")
	runes := []rune(line)
	if len(runes) <= maxRunes {
		return line
	}
	return string(runes[:maxRunes]) + "…"
}
