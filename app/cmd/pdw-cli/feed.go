package main

// `pdw feed`: the agent surface of the Feed tab (docs/feed.md). Every
// subcommand is a thin spelling of one feed_* server tool, the same tools MCP
// agents call, so the two surfaces drive the feed one way.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
)

var feedUsage = `pdw feed - push cards into the PDW app's Feed tab and read how they land.

USAGE
  pdw feed push [flags]                 One card from flags, or many as JSON on
                                        stdin / --file (an array, {"source":..,
                                        "items":[..]}, one object, or JSON Lines).
  pdw feed list [flags]                 Cards with their engagement.
  pdw feed update ITEM_ID [flags]       Change fields of one card.
  pdw feed remove ITEM_ID... | --source S [--reason R]
  pdw feed rank ITEM_ID...              Order cards best first (or --score ID=N).
  pdw feed stats [--source S] [--since 7d]
  pdw feed events [--item ID] [--type like,open] [--since 7d] [-n 100]

A card is idempotent on (source, key); the key defaults to the url, so
re-pushing a link updates its card instead of duplicating it.

PUSH / UPDATE FLAGS
  --source NAME     The agent or feed name (lowercase; required for push).
  --title TEXT      Headline.
  --body TEXT       Markdown; the card shows the first lines, the reader all.
  --url URL         The link a tap opens.
  --image URL       https image shown full width.
  --author TEXT, --site TEXT, --key KEY
  --tag TAG         Repeatable or comma-separated.
  --score N         Rank weight, higher first.
  --published TIME  RFC3339 or YYYY-MM-DD.
  --expires TIME    RFC3339, YYYY-MM-DD or a duration (48h, 7d).
  --file PATH       Read cards as JSON from a file (- for stdin).

LIST / EVENTS FLAGS
  --source NAME     Only this source.
  --view VIEW       list: recent (default) or feed (exactly what the phone shows).
  --all             list: include removed and dismissed cards.
  -n, --limit N     Rows (list default 50, events default 100).
  --type TYPES      events: session_start, session_end, impression, open, close,
                    expand, like, unlike, dismiss, undismiss.
  --item ID, --since TIME (2026-10-01, or 7d / 12h)
  --output FMT      text (default) or json.

EXAMPLES
  pdw feed push --source hn --title "Show HN: ..." --url https://news.ycombinator.com/item?id=1
  jq -c '{source:"muse-picks", items:.}' picks.json | pdw feed push
  pdw feed stats --since 7d
  pdw sql "SELECT title, impressions, dwell_ms, opens, read_ms, liked FROM marts_feed.item_engagement ORDER BY last_event_at DESC NULLS LAST LIMIT 20"
`

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*s = append(*s, part)
		}
	}
	return nil
}

type feedCardFlags struct {
	source, title, body, url, image, author, site, key, published, expires, score, file string
	tags                                                                                stringList
}

func (c *feedCardFlags) bind(fs *flag.FlagSet) {
	fs.StringVar(&c.source, "source", "", "source")
	fs.StringVar(&c.title, "title", "", "title")
	fs.StringVar(&c.body, "body", "", "body")
	fs.StringVar(&c.url, "url", "", "url")
	fs.StringVar(&c.image, "image", "", "image url")
	fs.StringVar(&c.image, "image-url", "", "alias for --image")
	fs.StringVar(&c.author, "author", "", "author")
	fs.StringVar(&c.site, "site", "", "site")
	fs.StringVar(&c.key, "key", "", "key")
	fs.StringVar(&c.published, "published", "", "published time")
	fs.StringVar(&c.expires, "expires", "", "expiry")
	fs.StringVar(&c.score, "score", "", "score")
	fs.Var(&c.tags, "tag", "tag")
	fs.Var(&c.tags, "tags", "alias for --tag")
}

// set returns the card fields given on the command line, by JSON name.
func (c *feedCardFlags) set(fs *flag.FlagSet) (map[string]any, error) {
	given := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { given[f.Name] = true })
	out := map[string]any{}
	for name, field := range map[string]struct {
		json  string
		value string
	}{
		"title": {"title", c.title}, "body": {"body", c.body}, "url": {"url", c.url}, "image": {"image_url", c.image},
		"image-url": {"image_url", c.image}, "author": {"author", c.author}, "site": {"site", c.site}, "key": {"key", c.key},
		"published": {"published_at", c.published}, "expires": {"expires_at", c.expires},
	} {
		if given[name] {
			out[field.json] = field.value
		}
	}
	if given["tag"] || given["tags"] {
		out["tags"] = []string(c.tags)
	}
	if given["score"] {
		score, err := strconv.ParseFloat(c.score, 64)
		if err != nil {
			return nil, fmt.Errorf("--score %q is not a number", c.score)
		}
		out["score"] = score
	}
	return out, nil
}

func runFeed(client *cliclient.Client, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, feedUsage)
		return 2
	}
	sub, rest := args[0], args[1:]
	fail := func(err error) int {
		fmt.Fprintf(stderr, "pdw feed %s: %v\n", sub, err)
		return 2
	}
	fs := flag.NewFlagSet("feed "+sub, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var output string
	fs.StringVar(&output, "output", "text", "text or json")
	parse := func() error {
		ordered, err := flagsFirst(fs, rest, "feed "+sub)
		if err != nil {
			return err
		}
		if err := fs.Parse(ordered); err != nil {
			return fmt.Errorf("%v (run pdw feed --help)", err)
		}
		if output != "text" && output != "json" {
			return errors.New("--output must be text or json")
		}
		return nil
	}
	call := func(tool string, input any) int {
		raw, err := json.Marshal(input)
		if err != nil {
			return fail(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		out, err := client.CallTool(ctx, tool, raw)
		if err != nil {
			var apiErr *cliclient.APIError
			if errors.As(err, &apiErr) {
				fmt.Fprintf(stderr, "pdw feed %s: %s\n", sub, apiErr.Message)
			} else {
				fmt.Fprintf(stderr, "pdw feed %s: %v\n", sub, err)
			}
			return 1
		}
		if output == "json" {
			var pretty bytes.Buffer
			if json.Indent(&pretty, out, "", "  ") != nil {
				pretty.Reset()
				pretty.Write(out)
			}
			fmt.Fprintln(stdout, pretty.String())
			return 0
		}
		printFeedText(stdout, sub, out)
		return 0
	}

	switch sub {
	case "push":
		var card feedCardFlags
		card.bind(fs)
		fs.StringVar(&card.file, "file", "", "JSON file")
		if err := parse(); err != nil {
			return fail(err)
		}
		fields, err := card.set(fs)
		if err != nil {
			return fail(err)
		}
		if fs.NArg() > 0 {
			return fail(fmt.Errorf("unexpected argument %q; give cards as flags or JSON on stdin", fs.Arg(0)))
		}
		input := map[string]any{"source": card.source}
		if len(fields) > 0 {
			if card.file != "" {
				return fail(errors.New("give one card as flags or cards as --file, not both"))
			}
			input["items"] = []map[string]any{fields}
		} else {
			reader := stdin
			if card.file != "" && card.file != "-" {
				f, err := os.Open(card.file)
				if err != nil {
					return fail(err)
				}
				defer f.Close()
				reader = f
			}
			source, items, err := readFeedCards(reader)
			if err != nil {
				return fail(err)
			}
			if card.source == "" {
				input["source"] = source
			}
			input["items"] = items
		}
		return call("feed_push", input)
	case "update":
		var card feedCardFlags
		card.bind(fs)
		if err := parse(); err != nil {
			return fail(err)
		}
		if fs.NArg() != 1 {
			return fail(errors.New("exactly one ITEM_ID is required (pdw feed list shows ids)"))
		}
		fields, err := card.set(fs)
		if err != nil {
			return fail(err)
		}
		delete(fields, "key")
		fields["item_id"] = fs.Arg(0)
		return call("feed_update", fields)
	case "remove":
		var source, reason string
		fs.StringVar(&source, "source", "", "every card of this source")
		fs.StringVar(&reason, "reason", "", "why")
		if err := parse(); err != nil {
			return fail(err)
		}
		if fs.NArg() == 0 && source == "" {
			return fail(errors.New("give ITEM_IDs or --source"))
		}
		return call("feed_remove", map[string]any{"item_ids": fs.Args(), "source": source, "reason": reason})
	case "rank":
		var scores stringList
		fs.Var(&scores, "score", "ID=N")
		if err := parse(); err != nil {
			return fail(err)
		}
		input := map[string]any{}
		if fs.NArg() > 0 {
			input["order"] = fs.Args()
		}
		if len(scores) > 0 {
			byID := map[string]float64{}
			for _, pair := range scores {
				id, raw, ok := strings.Cut(pair, "=")
				n, err := strconv.ParseFloat(raw, 64)
				if !ok || err != nil {
					return fail(fmt.Errorf("--score %q must be ITEM_ID=NUMBER", pair))
				}
				byID[id] = n
			}
			input["scores"] = byID
		}
		if len(input) == 0 {
			return fail(errors.New("give ITEM_IDs best first, or --score ITEM_ID=N"))
		}
		return call("feed_rank", input)
	case "list":
		var source, view string
		var all bool
		var limit int
		fs.StringVar(&source, "source", "", "source")
		fs.StringVar(&view, "view", "", "recent or feed")
		fs.BoolVar(&all, "all", false, "include removed and dismissed")
		fs.IntVar(&limit, "limit", 0, "rows")
		fs.IntVar(&limit, "n", 0, "rows")
		if err := parse(); err != nil {
			return fail(err)
		}
		return call("feed_list", map[string]any{"source": source, "view": view, "include_removed": all,
			"include_dismissed": all, "limit": limit, "item_ids": fs.Args()})
	case "stats":
		var source, since string
		fs.StringVar(&source, "source", "", "source")
		fs.StringVar(&since, "since", "", "window")
		if err := parse(); err != nil {
			return fail(err)
		}
		return call("feed_stats", map[string]any{"source": source, "since": since})
	case "events":
		var item, source, since string
		var types stringList
		var limit int
		fs.StringVar(&item, "item", "", "item id")
		fs.StringVar(&source, "source", "", "source")
		fs.StringVar(&since, "since", "", "window")
		fs.Var(&types, "type", "types")
		fs.IntVar(&limit, "limit", 0, "rows")
		fs.IntVar(&limit, "n", 0, "rows")
		if err := parse(); err != nil {
			return fail(err)
		}
		return call("feed_events", map[string]any{"item_id": item, "source": source, "types": []string(types),
			"since": since, "limit": limit})
	default:
		fmt.Fprintf(stderr, "pdw feed: unknown subcommand %q (push, list, update, remove, rank, stats, events)\n", sub)
		return 2
	}
}

// readFeedCards accepts a JSON array of cards, {"source":..,"items":[..]},
// one card object, or JSON Lines of cards.
func readFeedCards(r io.Reader) (string, []json.RawMessage, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", nil, err
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return "", nil, errors.New("no cards: pass --title/--url/--body, or JSON on stdin or --file")
	}
	if data[0] == '[' {
		var items []json.RawMessage
		if err := json.Unmarshal(data, &items); err != nil {
			return "", nil, fmt.Errorf("stdin is not a JSON array of cards: %w", err)
		}
		return "", items, nil
	}
	var envelope struct {
		Source string            `json:"source"`
		Items  []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(data, &envelope); err == nil && envelope.Items != nil {
		return envelope.Source, envelope.Items, nil
	}
	var items []json.RawMessage
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 1<<20), 16<<20)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if !json.Valid(line) {
			if json.Valid(data) {
				return "", []json.RawMessage{json.RawMessage(data)}, nil
			}
			return "", nil, errors.New("stdin is not JSON: give an array, {\"items\": [...]}, one card, or JSON Lines")
		}
		items = append(items, append(json.RawMessage(nil), line...))
	}
	return "", items, scanner.Err()
}

func printFeedText(w io.Writer, sub string, raw json.RawMessage) {
	var out map[string]any
	if json.Unmarshal(raw, &out) != nil {
		fmt.Fprintln(w, string(raw))
		return
	}
	num := func(m map[string]any, k string) float64 { v, _ := m[k].(float64); return v }
	str := func(m map[string]any, k string) string { v, _ := m[k].(string); return v }
	secs := func(ms float64) string { return strconv.FormatFloat(ms/1000, 'f', 1, 64) + "s" }
	switch sub {
	case "push":
		fmt.Fprintf(w, "created %d, updated %d\n", int(num(out, "created")), int(num(out, "updated")))
		for _, it := range asList(out["items"]) {
			fmt.Fprintf(w, "  %-8s %s\n", str(it, "status"), str(it, "item_id"))
		}
	case "list":
		items := asList(out["items"])
		if len(items) == 0 {
			fmt.Fprintln(w, "no cards")
		}
		for _, it := range items {
			e, _ := it["engagement"].(map[string]any)
			flags := ""
			if b, _ := e["liked"].(bool); b {
				flags += " liked"
			}
			if b, _ := e["dismissed"].(bool); b {
				flags += " dismissed"
			}
			if str(it, "removed_at") != "" {
				flags += " removed"
			}
			fmt.Fprintf(w, "%s  score=%g  seen=%d dwell=%s opens=%d read=%s%s\n    %s\n", str(it, "item_id"), num(it, "score"),
				int(num(e, "impressions")), secs(num(e, "dwell_ms")), int(num(e, "opens")), secs(num(e, "read_ms")), flags, str(it, "title"))
		}
	case "stats":
		fmt.Fprintf(w, "since %s: %d sessions\n", str(out, "since"), int(num(out, "sessions")))
		if byType, ok := out["events_by_type"].(map[string]any); ok && len(byType) > 0 {
			parts := []string{}
			for _, kind := range []string{"impression", "open", "close", "expand", "like", "unlike", "dismiss", "undismiss", "session_start", "session_end"} {
				if n, ok := byType[kind].(float64); ok {
					parts = append(parts, fmt.Sprintf("%s=%d", kind, int(n)))
				}
			}
			fmt.Fprintln(w, "events: "+strings.Join(parts, " "))
		}
		fmt.Fprintln(w, "per source (all time):")
		for _, s := range asList(out["sources"]) {
			fmt.Fprintf(w, "  %-20s items=%d seen=%d opened=%d liked=%d dismissed=%d dwell=%s read=%s\n", str(s, "source"),
				int(num(s, "items")), int(num(s, "items_seen")), int(num(s, "items_opened")), int(num(s, "items_liked")),
				int(num(s, "items_dismissed")), secs(num(s, "dwell_ms")), secs(num(s, "read_ms")))
		}
		for _, section := range []struct{ key, label string }{{"top_items", "most engaged"}, {"ignored", "seen twice+, no engagement"}} {
			items := asList(out[section.key])
			if len(items) == 0 {
				continue
			}
			fmt.Fprintln(w, section.label+":")
			for _, it := range items {
				e, _ := it["engagement"].(map[string]any)
				fmt.Fprintf(w, "  %s  seen=%d read=%s  %s\n", str(it, "item_id"), int(num(e, "impressions")), secs(num(e, "read_ms")), str(it, "title"))
			}
		}
	case "events":
		events := asList(out["events"])
		if len(events) == 0 {
			fmt.Fprintln(w, "no events")
		}
		for _, e := range events {
			line := fmt.Sprintf("%s  %-13s %s", str(e, "occurred_at"), str(e, "type"), str(e, "item_id"))
			if ms := num(e, "dwell_ms"); ms > 0 {
				line += " dwell=" + secs(ms)
			}
			if f := num(e, "visible_fraction"); f > 0 {
				line += fmt.Sprintf(" visible=%.2f", f)
			}
			if m, ok := e["metadata"].(map[string]any); ok && len(m) > 0 {
				meta, _ := json.Marshal(m)
				line += " " + string(meta)
			}
			fmt.Fprintln(w, line)
		}
	default:
		pretty, _ := json.Marshal(out)
		fmt.Fprintln(w, string(pretty))
	}
}

func asList(v any) []map[string]any {
	raw, _ := v.([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}
