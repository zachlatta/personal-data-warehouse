package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// cliSearchDefaultMaxResults is 10: measured over real sessions the default
// 20-hit page ran 7-20 KB and agents cut it with `head -60` (which truncates
// mid-hit) far more often than they read the second half.
const cliSearchDefaultMaxResults = 10

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
	Query                  string         `json:"query"`
	Mode                   string         `json:"mode"`
	PriorityScope          string         `json:"priority_scope"`
	SelectedPriorities     []string       `json:"selected_priorities"`
	ReturnedPriorityCounts map[string]int `json:"returned_priority_counts"`
	HintCodes              []string       `json:"hint_codes"`
	SuggestedPriorities    []string       `json:"suggested_priorities"`
	FallbackReason         string         `json:"fallback_reason,omitempty"`
	Hint                   string         `json:"hint,omitempty"`
	Guidance               string         `json:"guidance,omitempty"`
	TotalRows              int            `json:"total_rows"`
	Rows                   []cliSearchHit `json:"rows,omitempty"`
	Error                  string         `json:"error,omitempty"`
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
var searchUsage = `pdw search - hybrid search across every synced source.

USAGE
  pdw search [flags] QUERY...

Flags may appear before or after the query. This is the normal CLI search
path; no JSON or SQL required.

FLAGS
  --mode MODE          hybrid (default), keyword, or exact. Use exact for a
                       literal phrase, email address, phone, amount, id or path.
  -n, --max-results N  Maximum hits (default 10; alias --limit).
  --full               Long previews (up to 800 characters per hit) instead of the
                       default one line per hit.
  --source NAMES       Source aliases, comma-separated; repeatable.
                       (alias: --sources)
  --priority TIERS     Attention tiers, comma-separated; repeatable.
                       (alias: --priorities)
` + warehouse.TimelinePriorityHelpLines("                         ") + `
                       Omitting it searches every tier. ` + warehouse.TimelinePriorities.Sentinel.Name + ` is also
                       accepted for diagnosis, but is not a sixth tier: ` + warehouse.TimelinePriorities.Sentinel.Meaning + `.
  --since TIME         Lower event-time bound, e.g. 2026-08-01.
  --output FMT         text (default) or json.

An unknown flag anywhere on the line is an error, never part of the query.

SCOPE GUIDE
` + warehouse.TimelinePrioritySelectionHelpLines("  ") + `

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
	full       bool
	sources    searchSourcesFlag
	priorities searchSourcesFlag
}

func newSearchFlagSet(opts *searchOptions) *flag.FlagSet {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.mode, "mode", "hybrid", "hybrid, keyword, or exact")
	fs.IntVar(&opts.maxResults, "max-results", cliSearchDefaultMaxResults, "maximum hits")
	fs.IntVar(&opts.maxResults, "n", cliSearchDefaultMaxResults, "alias for --max-results")
	fs.IntVar(&opts.maxResults, "limit", cliSearchDefaultMaxResults, "alias for --max-results")
	fs.BoolVar(&opts.full, "full", false, "long previews instead of one line per hit")
	fs.StringVar(&opts.since, "since", "", "event-time lower bound")
	fs.StringVar(&opts.output, "output", "text", "text or json")
	fs.Var(&opts.sources, "source", "source aliases, comma-separated; repeatable")
	fs.Var(&opts.sources, "sources", "alias for --source")
	fs.Var(&opts.priorities, "priority", "attention tiers ("+strings.Join(warehouse.TimelinePriorityTierNames(), ", ")+"), comma-separated; repeatable")
	fs.Var(&opts.priorities, "priorities", "alias for --priority")
	return fs
}

func runSearch(client *cliclient.Client, args []string, stdout, stderr io.Writer) int {
	var opts searchOptions
	fs := newSearchFlagSet(&opts)
	ordered, err := searchFlagsFirst(fs, args)
	if err != nil {
		fmt.Fprintln(stderr, "pdw search:", err)
		return 2
	}
	if err := fs.Parse(ordered); err != nil {
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
	// A new CLI may briefly talk to an older server during a rolling deploy.
	// The request still tells us the effective scope, and hit priorities can
	// reconstruct the returned mix, so text output never regresses to silence.
	if resp.PriorityScope == "" {
		if len(opts.priorities) > 0 {
			resp.PriorityScope = "selected"
			resp.SelectedPriorities = append([]string(nil), opts.priorities...)
		} else {
			resp.PriorityScope = "all"
		}
	}
	if resp.ReturnedPriorityCounts == nil {
		resp.ReturnedPriorityCounts = map[string]int{}
		for _, hit := range resp.Rows {
			if hit.Priority != "" {
				resp.ReturnedPriorityCounts[hit.Priority]++
			}
		}
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
	printSearchText(stdout, resp, opts.full)
	return 0
}

// The standard flag package stops parsing at the first positional argument,
// but agents naturally write both `pdw search --source gmail terms` and
// `pdw search terms --source gmail`. Move this command's flags to the front so
// both forms have identical semantics.
//
// A flag the command does not define is an ERROR wherever it appears. It used
// to be folded into the query when it came after the terms: `pdw search
// "Howden" --limit 15` searched for the string "Howden --limit 15", the header
// said so, and two sessions ran three polluted searches each without noticing.
func searchFlagsFirst(fs *flag.FlagSet, args []string) ([]string, error) {
	known := map[string]bool{}
	boolean := map[string]bool{}
	fs.VisitAll(func(f *flag.Flag) {
		known["-"+f.Name] = true
		known["--"+f.Name] = true
		if _, isBool := f.Value.(interface{ IsBoolFlag() bool }); isBool {
			boolean["-"+f.Name] = true
			boolean["--"+f.Name] = true
		}
	})
	flags := make([]string, 0, len(args))
	positionals := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positionals = append(positionals, args[i+1:]...)
			break
		}
		name := arg
		if before, _, ok := strings.Cut(arg, "="); ok {
			name = before
		}
		if known[name] {
			flags = append(flags, arg)
			if !strings.Contains(arg, "=") && !boolean[name] && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
			continue
		}
		if strings.HasPrefix(arg, "-") && len(arg) > 1 && !isNumeric(arg[1:]) {
			return nil, fmt.Errorf("unknown flag %s (run `pdw search --help`; put the query in quotes if it really starts with a dash)", name)
		}
		positionals = append(positionals, arg)
	}
	return append(flags, positionals...), nil
}

func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && r != '.' {
			return false
		}
	}
	return true
}

func printSearchText(w io.Writer, resp cliSearchResponse, full bool) {
	noun := "results"
	if resp.TotalRows == 1 {
		noun = "result"
	}
	fmt.Fprintf(w, "Search: %q — %d %s (%s)\n", resp.Query, resp.TotalRows, noun, resp.Mode)
	if resp.PriorityScope == "selected" || resp.PriorityScope == "invalid" {
		fmt.Fprintf(w, "Scope: %s\n", strings.Join(resp.SelectedPriorities, ", "))
	} else {
		fmt.Fprintln(w, "Scope: all tiers")
	}
	if counts := orderedPriorityCounts(resp.ReturnedPriorityCounts); len(counts) > 0 {
		fmt.Fprintf(w, "Returned priorities: %s\n", strings.Join(counts, ", "))
	}
	if resp.FallbackReason != "" {
		fmt.Fprintf(w, "Fallback: %s\n", resp.FallbackReason)
	}
	for i, hit := range resp.Rows {
		if full {
			fmt.Fprintf(w, "\n%d. %s\n", i+1, strings.Join(nonemptySearchParts(hit.Source, hit.Priority, hit.OccurredAt, hit.Who), " · "))
			if title := strings.TrimSpace(hit.Title); title != "" && title != strings.TrimSpace(hit.Text) {
				fmt.Fprintf(w, "   %s\n", compactSearchLine(title, searchFullPreviewRunes))
			}
			if body := strings.TrimSpace(hit.Text); body != "" {
				fmt.Fprintf(w, "   %s\n", compactSearchLine(body, searchFullPreviewRunes))
			}
			if hit.Ref != "" {
				fmt.Fprintf(w, "   ref: %s\n", hit.Ref)
			}
			continue
		}
		fmt.Fprintf(w, "%d. %s\n", i+1, searchBriefLine(hit))
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

func orderedPriorityCounts(counts map[string]int) []string {
	if len(counts) == 0 {
		return nil
	}
	parts := make([]string, 0, len(counts))
	seen := make(map[string]bool, len(counts))
	for _, priority := range warehouse.TimelinePriorityAcceptedNames() {
		if count, ok := counts[priority]; ok {
			parts = append(parts, fmt.Sprintf("%s=%d", priority, count))
			seen[priority] = true
		}
	}
	remaining := make([]string, 0)
	for priority := range counts {
		if !seen[priority] {
			remaining = append(remaining, priority)
		}
	}
	sort.Strings(remaining)
	for _, priority := range remaining {
		parts = append(parts, fmt.Sprintf("%s=%d", priority, counts[priority]))
	}
	return parts
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

// searchFullPreviewRunes is the --full preview length; searchBriefTextRunes
// and searchBriefTitleRunes bound the default one-line form. Measured on
// twelve real sessions the agents that were shown the long form wrote their
// own `python3 -c` to print `date | who | title | ref` and read that instead,
// so the one-line form is what they were already asking for.
const (
	searchFullPreviewRunes = 800
	searchBriefTitleRunes  = 100
	searchBriefTextRunes   = 160
)

// searchBriefLine renders one hit on one line: source, tier, date, who, then
// the title and the first words of the preview. The date keeps only the day
// and minute; the ref follows on its own line so it can be copied whole.
func searchBriefLine(hit cliSearchHit) string {
	head := strings.Join(nonemptySearchParts(hit.Source, hit.Priority, shortSearchTime(hit.OccurredAt), hit.Who), " · ")
	title := strings.TrimSpace(hit.Title)
	body := strings.TrimSpace(hit.Text)
	if title != "" && strings.HasPrefix(body, title) {
		body = strings.TrimSpace(strings.TrimPrefix(body, title))
	}
	parts := []string{}
	if title != "" {
		parts = append(parts, compactSearchLine(title, searchBriefTitleRunes))
	}
	if body != "" && body != title {
		parts = append(parts, compactSearchLine(body, searchBriefTextRunes))
	}
	if len(parts) == 0 {
		return head
	}
	return head + " — " + strings.Join(parts, " | ")
}

// shortSearchTime trims an RFC 3339 stamp to `2026-09-10 10:29`; sub-minute
// precision and the zone suffix are noise on a list line.
func shortSearchTime(ts string) string {
	ts = strings.TrimSpace(ts)
	if len(ts) >= 16 && ts[10] == 'T' {
		return ts[:10] + " " + ts[11:16]
	}
	return ts
}

func compactSearchLine(value string, maxRunes int) string {
	line := strings.Join(strings.Fields(value), " ")
	runes := []rune(line)
	if len(runes) <= maxRunes {
		return line
	}
	return string(runes[:maxRunes]) + "…"
}
