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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
)

var feedUsage = `pdw feed - full control of the PDW app's Feed tab, with every past version kept.

USAGE
  pdw feed push [flags]                 Add or update cards (alias add): one from flags,
                                        or many as JSON on stdin / --file (an array,
                                        {"source":..,"items":[..]}, one object, or JSON Lines).
  pdw feed get ITEM_ID [--version N]    One card in full, now or as it was at version N.
  pdw feed update ITEM_ID... [flags]    Edit cards (alias edit); --source S edits every
                                        live card of a source. --clear FIELDS empties fields.
  pdw feed remove ITEM_ID... | --source S
                                        Soft-remove cards (alias delete); restorable.
  pdw feed restore ITEM_ID [--version N] | --source S
                                        Bring cards back; --version also restores that
                                        version's content.
  pdw feed history [ITEM_ID] [--source S] [--since 7d] [--full]
                                        Every version, newest first, with what changed.
  pdw feed rank ITEM_ID...              Order cards best first (or --score ID=N).
  pdw feed save ITEM_ID...              Put cards in Zach's read-later queue (the phone's
                                        Saved view), only when he asks; unsave takes them out.
  pdw feed list [flags]                 Cards with their engagement and version.
  pdw feed stats [--source S] [--since 7d]
  pdw feed events [--item ID] [--type open,dismiss] [--since 7d] [-n 100]

Every create, edit, rank, removal, restore and deletion writes an immutable version
(who, which agent, when, why, the diff and a full snapshot) to base_feed.item_versions.
Nothing is ever lost: removal is soft, and restore can return to any version.
A card is idempotent on (source, key); the key defaults to the url, so re-pushing a
link updates its card, and a re-push that changes nothing writes no version.

CARD FLAGS (push, update)
  --source NAME     The agent or feed name (lowercase; required for push).
  --title TEXT      Headline.
  --body TEXT       Markdown; the card shows the first lines, the reader all.
  --url URL         The link a tap opens (defaults to the first --link).
  --link LINK       More links on the card (update: replaces them): URL or "Title|URL". Repeatable
                    (at most 10): the paper, the repo, the discussion.
  --image URL       https image shown full width.
  --media URL       Photos or videos (.mp4) shown as a swipeable carousel (update: replaces them).
                    Repeatable, at most 10. A --url that is an x.com post is embedded on its
                    own: the post's text, author, photos, video and quoted post show in the card.
  --author TEXT, --site TEXT, --key KEY (push only)
  --tag TAG         Repeatable or comma-separated; replaces the tags.
  --score N         Rank weight, higher first.
  --published TIME  RFC3339 or YYYY-MM-DD.
  --expires TIME    RFC3339, YYYY-MM-DD or a duration (48h, 7d).
  --metadata JSON   update: replace the card's metadata object.
  --clear FIELDS    update: empty title, body, url, links, image_url, media, embed, author, site, tags,
                    published_at, expires_at or metadata.
  --file PATH       push: read cards as JSON from a file (- for stdin).

PROVENANCE FLAGS (push, update, remove, restore, rank)
  --agent NAME      Which agent acted (default $PDW_AGENT_NAME, else $PASEO_AGENT_ID).
  --reason TEXT     Why; kept on the version.

READ FLAGS
  --view VIEW       list: recent (default), feed (exactly what the phone shows) or
                    saved (his read-later queue, newest saved first).
  --all             list: include removed and dismissed cards.
  --url URL         list: only cards with this url, key or link (repeatable). Check a
                    link with --all --url before pushing it, instead of paging the list.
  --full            history: include each version's full snapshot.
  -n, --limit N     Rows.
  --type TYPES      events: session_start, session_end, impression, open, close,
                    expand, dismiss, undismiss, share, media, save, unsave.
  --item ID, --since TIME (2026-10-01, or 7d / 12h)
  --output FMT      text (default) or json.

EXAMPLES
  pdw feed add --source hn --title "Show HN: ..." --url https://news.ycombinator.com/item?id=1
  pdw feed add --source x-picks --url https://x.com/karpathy/status/1885026028428681698
  jq -c '{source:"muse-picks", items:.}' picks.json | pdw feed push --agent muse-picks
  pdw feed edit hn:1 --title "Better title" --clear image_url --reason "broken image"
  pdw feed history hn:1
  pdw feed restore hn:1 --version 2
  pdw feed remove --source hn --reason "stale batch"
  pdw feed save picks:https://www.youtube.com/watch?v=LKrwhjOI8Nc --reason "Zach asked"
  pdw feed list --view saved
  pdw feed list --all --url https://news.ycombinator.com/item?id=1
  pdw feed stats --since 7d
  pdw sql "SELECT title, impressions, dwell_ms, opens, read_ms, engagement_score FROM marts_feed.item_engagement ORDER BY last_event_at DESC NULLS LAST LIMIT 20"
`

// urlList is a repeatable flag that, unlike stringList, never splits on a
// comma: a URL can contain one.
type urlList []string

func (u *urlList) String() string { return strings.Join(*u, " ") }
func (u *urlList) Set(v string) error {
	if v = strings.TrimSpace(v); v != "" {
		*u = append(*u, v)
	}
	return nil
}

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
	links                                                                               linkList
	media                                                                               mediaList
}

// mediaList is a repeatable --media URL; an .mp4 is a video.
type mediaList []map[string]any

func (l *mediaList) String() string { return fmt.Sprint(*l) }
func (l *mediaList) Set(v string) error {
	link := strings.TrimSpace(v)
	if link == "" {
		return fmt.Errorf("--media needs a url")
	}
	kind := "image"
	if path := strings.ToLower(strings.SplitN(link, "?", 2)[0]); strings.HasSuffix(path, ".mp4") || strings.HasSuffix(path, ".mov") {
		kind = "video"
	}
	*l = append(*l, map[string]any{"type": kind, "url": link})
	return nil
}

// linkList is a repeatable --link: "URL" or "Title|URL".
type linkList []map[string]any

func (l *linkList) String() string { return fmt.Sprint(*l) }
func (l *linkList) Set(v string) error {
	title, link := "", strings.TrimSpace(v)
	if i := strings.Index(link, "|"); i >= 0 {
		title, link = strings.TrimSpace(link[:i]), strings.TrimSpace(link[i+1:])
	}
	if link == "" {
		return fmt.Errorf("--link %q has no url", v)
	}
	entry := map[string]any{"url": link}
	if title != "" {
		entry["title"] = title
	}
	*l = append(*l, entry)
	return nil
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
	fs.Var(&c.links, "link", "more links: URL or Title|URL (repeatable)")
	fs.Var(&c.media, "media", "photo or video url for the carousel (repeatable)")
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
	if given["link"] {
		out["links"] = []map[string]any(c.links)
	}
	if given["media"] {
		out["media"] = []map[string]any(c.media)
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

// feedAliases are the other words agents reach for.
var feedAliases = map[string]string{"add": "push", "edit": "update", "delete": "remove", "rm": "remove", "show": "get", "log": "history"}

func runFeed(client *cliclient.Client, args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, feedUsage)
		return 2
	}
	sub, rest := args[0], args[1:]
	if canonical, ok := feedAliases[sub]; ok {
		sub = canonical
	}
	fail := func(err error) int {
		fmt.Fprintf(stderr, "pdw feed %s: %v\n", sub, err)
		return 2
	}
	fs := flag.NewFlagSet("feed "+sub, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var output string
	fs.StringVar(&output, "output", "text", "text or json")
	// Provenance for every version a mutation writes: which agent, and why.
	// The agent defaults to $PDW_AGENT_NAME, else the Paseo agent id.
	var agent, reason string
	mutating := map[string]bool{"push": true, "update": true, "remove": true, "restore": true, "rank": true, "save": true, "unsave": true}[sub]
	if mutating {
		fs.StringVar(&agent, "agent", "", "acting agent")
		fs.StringVar(&reason, "reason", "", "why")
	}
	withProvenance := func(input map[string]any) map[string]any {
		if agent == "" && getenv != nil {
			agent = getenv("PDW_AGENT_NAME")
			if agent == "" {
				agent = getenv("PASEO_AGENT_ID")
			}
		}
		if agent != "" {
			input["agent"] = agent
		}
		if reason != "" {
			input["reason"] = reason
		}
		return input
	}
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
		return call("feed_push", withProvenance(input))
	case "get":
		var version int64
		fs.Int64Var(&version, "version", 0, "version")
		if err := parse(); err != nil {
			return fail(err)
		}
		if fs.NArg() != 1 {
			return fail(errors.New("exactly one ITEM_ID is required (pdw feed list --all shows ids)"))
		}
		return call("feed_get", map[string]any{"item_id": fs.Arg(0), "version": version})
	case "update":
		var card feedCardFlags
		card.bind(fs)
		var clear stringList
		var metadata string
		fs.Var(&clear, "clear", "fields to empty")
		fs.StringVar(&metadata, "metadata", "", "JSON object")
		if err := parse(); err != nil {
			return fail(err)
		}
		fields, err := card.set(fs)
		if err != nil {
			return fail(err)
		}
		delete(fields, "key")
		if card.source != "" {
			fields["source"] = card.source
		}
		if fs.NArg() == 0 && card.source == "" {
			return fail(errors.New("give ITEM_IDs, or --source S to edit every live card of a source"))
		}
		if fs.NArg() > 0 {
			fields["item_ids"] = fs.Args()
		}
		if len(clear) > 0 {
			fields["clear"] = []string(clear)
		}
		if metadata != "" {
			var m map[string]any
			if err := json.Unmarshal([]byte(metadata), &m); err != nil || m == nil {
				return fail(errors.New("--metadata must be a JSON object"))
			}
			fields["metadata"] = m
		}
		return call("feed_update", withProvenance(fields))
	case "remove":
		var source string
		fs.StringVar(&source, "source", "", "every card of this source")
		if err := parse(); err != nil {
			return fail(err)
		}
		if fs.NArg() == 0 && source == "" {
			return fail(errors.New("give ITEM_IDs or --source"))
		}
		return call("feed_remove", withProvenance(map[string]any{"item_ids": fs.Args(), "source": source}))
	case "restore":
		var source string
		var version int64
		fs.StringVar(&source, "source", "", "every removed card of this source")
		fs.Int64Var(&version, "version", 0, "version")
		if err := parse(); err != nil {
			return fail(err)
		}
		if fs.NArg() == 0 && source == "" {
			return fail(errors.New("give ITEM_IDs or --source"))
		}
		return call("feed_restore", withProvenance(map[string]any{"item_ids": fs.Args(), "source": source, "version": version}))
	case "history":
		var source, since string
		var limit int
		var full bool
		fs.StringVar(&source, "source", "", "source")
		fs.StringVar(&since, "since", "", "window")
		fs.IntVar(&limit, "limit", 0, "rows")
		fs.IntVar(&limit, "n", 0, "rows")
		fs.BoolVar(&full, "full", false, "snapshots")
		if err := parse(); err != nil {
			return fail(err)
		}
		if fs.NArg() > 1 {
			return fail(errors.New("at most one ITEM_ID"))
		}
		return call("feed_history", map[string]any{"item_id": fs.Arg(0), "source": source, "since": since, "limit": limit, "snapshots": full})
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
		return call("feed_rank", withProvenance(input))
	case "save", "unsave":
		if err := parse(); err != nil {
			return fail(err)
		}
		if fs.NArg() == 0 {
			return fail(errors.New("give ITEM_IDs"))
		}
		return call("feed_save", withProvenance(map[string]any{"item_ids": fs.Args(), "unsave": sub == "unsave"}))
	case "list":
		var source, view string
		var all bool
		var limit int
		var urls urlList
		fs.StringVar(&source, "source", "", "source")
		fs.StringVar(&view, "view", "", "recent, feed or saved")
		fs.BoolVar(&all, "all", false, "include removed and dismissed")
		fs.Var(&urls, "url", "only cards with this url, key or link (repeatable)")
		fs.IntVar(&limit, "limit", 0, "rows")
		fs.IntVar(&limit, "n", 0, "rows")
		if err := parse(); err != nil {
			return fail(err)
		}
		return call("feed_list", map[string]any{"source": source, "view": view, "include_removed": all,
			"include_dismissed": all, "limit": limit, "item_ids": fs.Args(), "urls": []string(urls)})
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
		fmt.Fprintf(stderr, "pdw feed: unknown subcommand %q (push, get, update, remove, restore, history, rank, save, unsave, list, stats, events)\n", sub)
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
			if b, _ := e["engaged"].(bool); b {
				flags += " engaged"
			}
			if b, _ := e["dismissed"].(bool); b {
				flags += " dismissed"
			}
			if b, _ := e["saved"].(bool); b {
				flags += " saved"
			}
			if str(it, "removed_at") != "" {
				flags += " removed"
			}
			fmt.Fprintf(w, "%s  score=%g  seen=%d dwell=%s opens=%d read=%s engagement=%g%s\n    %s\n", str(it, "item_id"), num(it, "score"),
				int(num(e, "impressions")), secs(num(e, "dwell_ms")), int(num(e, "opens")), secs(num(e, "read_ms")),
				num(e, "engagement_score"), flags, str(it, "title"))
			if u := str(it, "url"); u != "" {
				fmt.Fprintf(w, "    %s\n", u)
			}
			for _, link := range asList(it["links"]) {
				fmt.Fprintf(w, "    + %s %s\n", str(link, "title"), str(link, "url"))
			}
		}
	case "stats":
		fmt.Fprintf(w, "since %s: %d sessions\n", str(out, "since"), int(num(out, "sessions")))
		if byType, ok := out["events_by_type"].(map[string]any); ok && len(byType) > 0 {
			parts := []string{}
			for _, kind := range []string{"impression", "open", "close", "expand", "dismiss", "undismiss", "share", "save", "unsave", "like", "unlike", "web", "session_start", "session_end"} {
				if n, ok := byType[kind].(float64); ok {
					parts = append(parts, fmt.Sprintf("%s=%d", kind, int(n)))
				}
			}
			fmt.Fprintln(w, "events: "+strings.Join(parts, " "))
		}
		fmt.Fprintln(w, "per source (all time):")
		for _, s := range asList(out["sources"]) {
			fmt.Fprintf(w, "  %-20s items=%d seen=%d engaged=%d opened=%d dismissed=%d avg_engagement=%g dwell=%s read=%s\n",
				str(s, "source"), int(num(s, "items")), int(num(s, "items_seen")), int(num(s, "items_engaged")),
				int(num(s, "items_opened")), int(num(s, "items_dismissed")), num(s, "avg_engagement_score"),
				secs(num(s, "dwell_ms")), secs(num(s, "read_ms")))
		}
		for _, section := range []struct{ key, label string }{{"top_items", "most engaged"}, {"ignored", "seen twice+, no engagement"}} {
			items := asList(out[section.key])
			if len(items) == 0 {
				continue
			}
			fmt.Fprintln(w, section.label+":")
			for _, it := range items {
				e, _ := it["engagement"].(map[string]any)
				fmt.Fprintf(w, "  %s  seen=%d dwell=%s read=%s engagement=%g  %s\n", str(it, "item_id"), int(num(e, "impressions")),
					secs(num(e, "dwell_ms")), secs(num(e, "read_ms")), num(e, "engagement_score"), str(it, "title"))
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
	case "update":
		fmt.Fprintf(w, "updated %d\n", int(num(out, "updated")))
	case "remove":
		fmt.Fprintf(w, "removed %d (restorable: pdw feed restore)\n", int(num(out, "removed")))
	case "rank":
		fmt.Fprintf(w, "ranked %d\n", int(num(out, "ranked")))
	case "save", "unsave":
		fmt.Fprintf(w, "saved %d, unsaved %d (pdw feed list --view saved)\n", int(num(out, "saved")), int(num(out, "unsaved")))
	case "restore":
		fmt.Fprintf(w, "restored %d\n", int(num(out, "restored")))
		for _, it := range asList(out["items"]) {
			line := fmt.Sprintf("  %-12s %s", str(it, "status"), str(it, "item_id"))
			if v := num(it, "version"); v > 0 {
				line += fmt.Sprintf(" -> v%d", int(v))
			}
			if v := num(it, "from_version"); v > 0 {
				line += fmt.Sprintf(" (content of v%d)", int(v))
			}
			fmt.Fprintln(w, line)
		}
	case "get":
		if it, ok := out["item"].(map[string]any); ok {
			fmt.Fprintf(w, "%s  v%d of %d", str(it, "item_id"), int(num(it, "version")), int(num(out, "versions")))
			if str(it, "removed_at") != "" {
				fmt.Fprintf(w, "  REMOVED %s %s", str(it, "removed_at"), str(it, "removed_reason"))
			}
			fmt.Fprintln(w)
			printCardFields(w, it)
			if e, ok := it["engagement"].(map[string]any); ok {
				fmt.Fprintf(w, "engagement: seen=%d dwell=%s opens=%d read=%s liked=%v dismissed=%v\n", int(num(e, "impressions")),
					secs(num(e, "dwell_ms")), int(num(e, "opens")), secs(num(e, "read_ms")), e["liked"], e["dismissed"])
			}
			return
		}
		if v, ok := out["version"].(map[string]any); ok {
			label := ""
			if b, _ := out["deleted"].(bool); b {
				label = "  DELETED (pdw feed restore brings it back)"
			}
			fmt.Fprintf(w, "%s  as of v%d (%s by %s, %s)%s\n", str(v, "item_id"), int(num(v, "version")), str(v, "action"),
				str(v, "actor"), str(v, "changed_at"), label)
			if snap, ok := v["snapshot"].(map[string]any); ok {
				printCardFields(w, snap)
			}
		}
	case "history":
		versions := asList(out["versions"])
		if len(versions) == 0 {
			fmt.Fprintln(w, "no versions")
		}
		for _, v := range versions {
			who := str(v, "actor")
			if a := str(v, "agent"); a != "" {
				who += " / " + a
			}
			line := fmt.Sprintf("%s v%d  %-8s %s  by %s", str(v, "item_id"), int(num(v, "version")), str(v, "action"), str(v, "changed_at"), who)
			if r := num(v, "restored_from"); r > 0 {
				line += fmt.Sprintf("  (from v%d)", int(r))
			}
			if r := str(v, "reason"); r != "" {
				line += "  reason: " + r
			}
			fmt.Fprintln(w, line)
			if diff, ok := v["diff"].(map[string]any); ok {
				keys := make([]string, 0, len(diff))
				for k := range diff {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, k := range keys {
					change, _ := diff[k].(map[string]any)
					fmt.Fprintf(w, "    %s: %s -> %s\n", k, short(change["from"]), short(change["to"]))
				}
			}
		}
	default:
		pretty, _ := json.Marshal(out)
		fmt.Fprintln(w, string(pretty))
	}
}

func printCardFields(w io.Writer, card map[string]any) {
	for _, k := range []string{"source", "key", "item_key", "title", "url", "image_url", "author", "site", "tags", "score", "published_at", "expires_at", "metadata", "body"} {
		v, ok := card[k]
		if !ok || v == nil || v == "" || v == "1970-01-01T00:00:00+00:00" {
			continue
		}
		fmt.Fprintf(w, "%s: %s\n", k, short(v))
	}
}

// short renders a diff or field value on one line.
func short(v any) string {
	var text string
	switch x := v.(type) {
	case nil:
		return "(none)"
	case string:
		text = strconv.Quote(x)
	default:
		raw, _ := json.Marshal(x)
		text = string(raw)
	}
	if r := []rune(text); len(r) > 160 {
		text = string(r[:157]) + "..."
	}
	return text
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
