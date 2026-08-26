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
	Priority   string `json:"priority"`
	OccurredAt string `json:"occurred_at"`
	Who        string `json:"who"`
	Title      string `json:"title"`
	Text       string `json:"text"`
	Ref        string `json:"ref"`
}

// searchUsage is this command's OWN help. `pdw search --help` used to
// short-circuit to the global usage, so the flags the command accepts were
// undiscoverable from the command itself -- which is a large part of why
// --priority was used six times in a month of real agent sessions while it
// silently worked the whole time.
const searchUsage = `pdw search - hybrid search across every synced source.

USAGE
  pdw search [flags] QUERY...

Flags may appear before or after the query. This is the normal CLI search
path; no JSON or SQL required.

FLAGS
  --mode MODE          hybrid (default), keyword, or exact. Use exact for a
                       literal phrase, email address, phone, amount, id or path.
  -n, --max-results N  Maximum hits (default 20).
  --source NAMES       Source aliases, comma-separated; repeatable.
                       (alias: --sources)
  --priority TIERS     Attention tiers, comma-separated; repeatable.
                       (alias: --priorities)
                         self        Zach initiated it
                         direct      a real person reaching him directly
                         cc          real-people activity he is peripheral to
                         noise       bulk or automated traffic
                         background  the warehouse's own machinery
                       Omitting it searches every tier. "What needs my
                       attention" means self,direct,cc -- noise is most of the
                       corpus, so leaving it in is usually why a search returns
                       junk. (unclassified is also accepted, but it is a
                       fail-loud sentinel for rows an adapter has not
                       classified, not a sixth tier.)
  --since TIME         Lower event-time bound, e.g. 2026-08-01.
  --output FMT         text (default) or json.

EXAMPLES
  pdw search 'runway burn rate months cash remaining'
  pdw search --priority self,direct 'budget approval'
  pdw search --source gmail,slack --since 2026-08-01 'budget approval'
  pdw search --mode exact --output json 'admin/api-keys'
`

// searchOptions holds every value the search FlagSet binds. It exists so the
// FlagSet can be built once and inspected by the usage-drift test, which walks
// it with VisitAll and requires each flag to appear in the help text.
type searchOptions struct {
	mode       string
	maxResults int
	since      string
	output     string
	sources    searchSourcesFlag
	priorities searchSourcesFlag
}

func newSearchFlagSet(opts *searchOptions) *flag.FlagSet {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.mode, "mode", "hybrid", "hybrid, keyword, or exact")
	fs.IntVar(&opts.maxResults, "max-results", cliSearchDefaultMaxResults, "maximum hits")
	fs.IntVar(&opts.maxResults, "n", cliSearchDefaultMaxResults, "alias for --max-results")
	fs.StringVar(&opts.since, "since", "", "event-time lower bound")
	fs.StringVar(&opts.output, "output", "text", "text or json")
	fs.Var(&opts.sources, "source", "source aliases, comma-separated; repeatable")
	fs.Var(&opts.sources, "sources", "alias for --source")
	fs.Var(&opts.priorities, "priority", "attention tiers (self, direct, cc, noise, background), comma-separated; repeatable")
	fs.Var(&opts.priorities, "priorities", "alias for --priority")
	return fs
}

func runSearch(client *cliclient.Client, args []string, stdout, stderr io.Writer) int {
	var opts searchOptions
	fs := newSearchFlagSet(&opts)
	if err := fs.Parse(searchFlagsFirst(args)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stdout, searchUsage)
			return 0
		}
		fmt.Fprintln(stderr, "pdw search:", err)
		return 2
	}
	queryText := strings.TrimSpace(strings.Join(fs.Args(), " "))
	if queryText == "" {
		fmt.Fprintln(stderr, "pdw search: query is required (example: pdw search 'budget approval')")
		return 2
	}
	if opts.maxResults <= 0 {
		fmt.Fprintln(stderr, "pdw search: --max-results must be greater than zero")
		return 2
	}
	if opts.output != "text" && opts.output != "json" {
		fmt.Fprintln(stderr, "pdw search: --output must be text or json")
		return 2
	}

	input, err := json.Marshal(map[string]any{
		"query": queryText, "mode": opts.mode, "max_results": opts.maxResults,
		"sources": []string(opts.sources), "since": opts.since,
		"priorities": []string(opts.priorities),
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
	if opts.output == "json" {
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
		"--priority": true, "--priorities": true,
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
		fmt.Fprintf(w, "\n%d. %s\n", i+1, strings.Join(nonemptySearchParts(hit.Source, hit.Priority, hit.OccurredAt, hit.Who), " · "))
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
