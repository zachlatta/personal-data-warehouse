// Package feed is the PDW app's Feed tab: cards agents push (`pdw feed push`,
// the feed_* tools) and the engagement the iOS app records on them.
//
// It is a self-contained module. Everything it needs is in this directory;
// it hooks into the app at exactly one line (server.go calls Register), and
// PDW_FEED_ENABLED=0 turns it off. The tables and marts views are declared
// once, in src/personal_data_warehouse/feed.py. docs/feed.md lists every file
// and registration line to delete to remove the feature.
package feed

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// EventTypes are the engagement events the phone records. The Python module
// holds the same list (feed.py EVENT_TYPES); tests/test_feed.py pins the two.
var EventTypes = []string{
	"session_start", "session_end", "impression", "open", "close", "expand",
	"dismiss", "undismiss", "share", "media", "save", "unsave",
	"ui", "scroll", "load",
}

// cardlessEventTypes may come without an item_id: a sitting, a feed load,
// a scroll of the feed, or a tap on the feed itself (the source rail, pull
// to refresh, Undo).
var cardlessEventTypes = map[string]bool{
	"session_start": true, "session_end": true, "ui": true, "scroll": true, "load": true,
}

const (
	MaxPushItems   = 500
	MaxEventBatch  = 500
	maxTitleChars  = 500
	maxBodyChars   = 20000
	maxKeyChars    = 512
	maxURLChars    = 4096
	maxTags        = 20
	maxTagChars    = 64
	maxMetadataLen = 16384
	maxLinks       = 10
	maxLinkTitle   = 200
)

var sourceRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)

// Item is one card. Absent timestamps are nil here and the epoch in storage.
type Item struct {
	ItemID        string         `json:"item_id"`
	Source        string         `json:"source"`
	Key           string         `json:"key"`
	Title         string         `json:"title"`
	Body          string         `json:"body,omitempty"`
	URL           string         `json:"url,omitempty"`
	Links         []Link         `json:"links"`
	ImageURL      string         `json:"image_url,omitempty"`
	Media         []Media        `json:"media"`
	Embed         *Embed         `json:"embed,omitempty"`
	Author        string         `json:"author,omitempty"`
	Site          string         `json:"site,omitempty"`
	Tags          []string       `json:"tags"`
	Score         float64        `json:"score"`
	PublishedAt   *time.Time     `json:"published_at,omitempty"`
	ExpiresAt     *time.Time     `json:"expires_at,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	PushedBy      string         `json:"pushed_by,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
	RemovedAt     *time.Time     `json:"removed_at,omitempty"`
	RemovedReason string         `json:"removed_reason,omitempty"`
	// Version is the card's latest row in base_feed.item_versions.
	Version    int64      `json:"version"`
	Engagement Engagement `json:"engagement"`
}

// Link is one more place a card points to (the paper, the repo, the
// discussion), shown under the body; URL stays the card's main tap target.
type Link struct {
	URL   string `json:"url" jsonschema:"http(s) link"`
	Title string `json:"title,omitempty" jsonschema:"label shown for the link, e.g. Paper, Repo, HN discussion"`
	Site  string `json:"site,omitempty" jsonschema:"defaults to the url's host"`
}

// Engagement is an item's row in marts_feed.item_engagement. There is no
// like: interest is measured (Engaged, Score), not asked for.
type Engagement struct {
	Impressions   int64      `json:"impressions"`
	DwellMS       int64      `json:"dwell_ms"`
	ScrollPasts   int64      `json:"scroll_pasts"`
	Opens         int64      `json:"opens"`
	LinksOpened   int64      `json:"links_opened"`
	ReadMS        int64      `json:"read_ms"`
	Expands       int64      `json:"expands"`
	Dismissed     bool       `json:"dismissed"`
	DismissReason string     `json:"dismiss_reason,omitempty"`
	Engaged       bool       `json:"engaged"`
	Score         float64    `json:"engagement_score"`
	FirstSeenAt   *time.Time `json:"first_seen_at,omitempty"`
	// Saved is his read-later queue: the latest of save and unsave.
	Saved   bool       `json:"saved"`
	SavedAt *time.Time `json:"saved_at,omitempty"`
}

// PushItem is one card as an agent sends it. Only a source (here or on the
// batch) and one of title/body/url are required.
type PushItem struct {
	Source      string         `json:"source,omitempty" jsonschema:"feed or agent name, lowercase [a-z0-9_.-]; defaults to the batch source"`
	Key         string         `json:"key,omitempty" jsonschema:"idempotency key, unique within the source; re-pushing a key updates that card. Defaults to the url, else a hash of title and body"`
	Title       string         `json:"title,omitempty" jsonschema:"headline shown on the card"`
	Body        string         `json:"body,omitempty" jsonschema:"markdown text: the card shows the first lines, the reader the whole"`
	URL         string         `json:"url,omitempty" jsonschema:"http(s) link a tap opens; defaults to the first of links"`
	Links       []Link         `json:"links,omitempty" jsonschema:"more links on the card (at most 10), each {url, title}: the paper, the repo, the discussion"`
	ImageURL    string         `json:"image_url,omitempty" jsonschema:"https image shown full width on the card"`
	Media       []Media        `json:"media,omitempty" jsonschema:"photos and videos shown as a swipeable carousel (at most 10), each {type: image|video|gif, url, thumbnail_url}"`
	Embed       *Embed         `json:"embed,omitempty" jsonschema:"a post shown inside the card; leave it out for an X post url, which is read and embedded automatically"`
	Author      string         `json:"author,omitempty"`
	Site        string         `json:"site,omitempty" jsonschema:"publication name; defaults to the url's host"`
	Tags        []string       `json:"tags,omitempty"`
	Score       *float64       `json:"score,omitempty" jsonschema:"rank weight, higher shows first; omit to keep the current score on a re-push (new cards get 0)"`
	PublishedAt string         `json:"published_at,omitempty" jsonschema:"RFC3339 time or YYYY-MM-DD the content was published"`
	ExpiresAt   string         `json:"expires_at,omitempty" jsonschema:"RFC3339 time or a duration from now (48h, 7d) after which the card is hidden"`
	Metadata    map[string]any `json:"metadata,omitempty" jsonschema:"anything else an agent wants to keep with the card (why it was picked, ids)"`
}

// normalized is a validated PushItem ready to store.
type normalized struct {
	ItemID, Source, Key, Title, Body, URL, ImageURL, Author, Site string
	Links                                                         []Link
	Media                                                         []Media
	Embed                                                         *Embed
	Tags                                                          []string
	Score                                                         *float64
	PublishedAt, ExpiresAt                                        time.Time
	Metadata                                                      map[string]any
}

// ValidationError is a caller mistake; the HTTP and tool surfaces answer 400.
type ValidationError struct{ Message string }

func (e *ValidationError) Error() string { return e.Message }

func invalid(format string, args ...any) error {
	return &ValidationError{Message: fmt.Sprintf(format, args...)}
}

// ItemID is the stable id of a (source, key) pair.
func ItemID(source, key string) string { return source + ":" + key }

func normalizeSource(raw string) (string, error) {
	source := strings.ToLower(strings.TrimSpace(raw))
	if source == "" {
		return "", invalid("source is required: name the agent or feed, e.g. \"hn\" or \"muse-picks\"")
	}
	if !sourceRe.MatchString(source) {
		return "", invalid("source %q must be lowercase letters, digits, '_', '.' or '-' (at most 64)", raw)
	}
	return source, nil
}

func checkURL(field, raw string, httpsOnly bool) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", nil
	}
	if len(value) > maxURLChars {
		return "", invalid("%s is longer than %d characters", field, maxURLChars)
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return "", invalid("%s %q is not an absolute URL", field, raw)
	}
	switch {
	case parsed.Scheme == "https":
	case parsed.Scheme == "http" && !httpsOnly:
	case httpsOnly:
		return "", invalid("%s must be https: iOS blocks plain-http images", field)
	default:
		return "", invalid("%s must be http or https", field)
	}
	return value, nil
}

// ParseTime accepts RFC3339, a bare date, or (when relative is set) a
// duration from now such as 48h or 7d. An empty string is the zero time.
func ParseTime(field, raw string, now time.Time, relative bool) (time.Time, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse("2006-01-02", value); err == nil {
		return t.UTC(), nil
	}
	if relative {
		if d, ok := parseDuration(value); ok {
			return now.Add(d).UTC(), nil
		}
		return time.Time{}, invalid("%s %q must be RFC3339, YYYY-MM-DD, or a duration like 48h or 7d", field, raw)
	}
	return time.Time{}, invalid("%s %q must be RFC3339 or YYYY-MM-DD", field, raw)
}

func parseDuration(value string) (time.Duration, bool) {
	if strings.HasSuffix(value, "d") || strings.HasSuffix(value, "w") {
		n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimSuffix(value, "d"), "w"))
		if err != nil || n <= 0 {
			return 0, false
		}
		day := 24 * time.Hour
		if strings.HasSuffix(value, "w") {
			day *= 7
		}
		return time.Duration(n) * day, true
	}
	d, err := time.ParseDuration(value)
	return d, err == nil && d > 0
}

func hostOf(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.TrimPrefix(strings.ToLower(parsed.Hostname()), "www.")
}

// normalizeLinks validates a card's extra links and drops repeats of the
// main url and of each other.
func normalizeLinks(links []Link, mainURL string) ([]Link, error) {
	if len(links) > maxLinks {
		return nil, invalid("at most %d links", maxLinks)
	}
	out := []Link{}
	seen := map[string]bool{mainURL: mainURL != ""}
	for i, link := range links {
		value, err := checkURL(fmt.Sprintf("links[%d].url", i), link.URL, false)
		if err != nil {
			return nil, err
		}
		if value == "" {
			return nil, invalid("links[%d] needs a url", i)
		}
		if seen[value] {
			continue
		}
		seen[value] = true
		title := strings.TrimSpace(link.Title)
		if len([]rune(title)) > maxLinkTitle {
			return nil, invalid("links[%d].title is longer than %d characters", i, maxLinkTitle)
		}
		site := strings.TrimSpace(link.Site)
		if site == "" {
			site = hostOf(value)
		}
		out = append(out, Link{URL: value, Title: title, Site: site})
	}
	return out, nil
}

func normalizeTags(tags []string) ([]string, error) {
	if len(tags) > maxTags {
		return nil, invalid("at most %d tags", maxTags)
	}
	out := []string{}
	seen := map[string]bool{}
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" || seen[tag] {
			continue
		}
		if len([]rune(tag)) > maxTagChars {
			return nil, invalid("tag %q is longer than %d characters", tag, maxTagChars)
		}
		seen[tag] = true
		out = append(out, tag)
	}
	return out, nil
}

// normalizePush validates one pushed card against the batch's default source.
func normalizePush(in PushItem, defaultSource string, now time.Time) (normalized, error) {
	rawSource := in.Source
	if strings.TrimSpace(rawSource) == "" {
		rawSource = defaultSource
	}
	source, err := normalizeSource(rawSource)
	if err != nil {
		return normalized{}, err
	}
	out := normalized{Source: source, Title: strings.TrimSpace(in.Title), Body: strings.TrimSpace(in.Body),
		Author: strings.TrimSpace(in.Author), Site: strings.TrimSpace(in.Site), Score: in.Score, Metadata: in.Metadata}
	if out.URL, err = checkURL("url", in.URL, false); err != nil {
		return normalized{}, err
	}
	if out.ImageURL, err = checkURL("image_url", in.ImageURL, true); err != nil {
		return normalized{}, err
	}
	if out.Links, err = normalizeLinks(in.Links, out.URL); err != nil {
		return normalized{}, err
	}
	if out.Media, err = normalizeMedia("media", in.Media); err != nil {
		return normalized{}, err
	}
	if out.Embed, err = normalizeEmbed(in.Embed); err != nil {
		return normalized{}, err
	}
	if out.URL == "" && len(out.Links) > 0 {
		out.URL, out.Links = out.Links[0].URL, out.Links[1:]
	}
	if out.Title == "" && out.Body == "" && out.URL == "" {
		return normalized{}, invalid("a card needs a title, a body or a url")
	}
	if len([]rune(out.Title)) > maxTitleChars {
		return normalized{}, invalid("title is longer than %d characters", maxTitleChars)
	}
	if len([]rune(out.Body)) > maxBodyChars {
		return normalized{}, invalid("body is longer than %d characters", maxBodyChars)
	}
	if out.Site == "" && out.URL != "" {
		out.Site = hostOf(out.URL)
	}
	if out.Tags, err = normalizeTags(in.Tags); err != nil {
		return normalized{}, err
	}
	if out.PublishedAt, err = ParseTime("published_at", in.PublishedAt, now, false); err != nil {
		return normalized{}, err
	}
	if out.ExpiresAt, err = ParseTime("expires_at", in.ExpiresAt, now, true); err != nil {
		return normalized{}, err
	}
	out.Key = strings.TrimSpace(in.Key)
	if out.Key == "" {
		if out.URL != "" {
			out.Key = out.URL
		} else {
			sum := sha256.Sum256([]byte(out.Title + "\n" + out.Body))
			out.Key = hex.EncodeToString(sum[:])[:20]
		}
	}
	if len(out.Key) > maxKeyChars {
		return normalized{}, invalid("key is longer than %d characters", maxKeyChars)
	}
	if out.Metadata == nil {
		out.Metadata = map[string]any{}
	}
	out.ItemID = ItemID(out.Source, out.Key)
	return out, nil
}

// Event is one engagement record from the phone. EventID is generated on
// the device, so a retried batch is idempotent.
type Event struct {
	EventID         string         `json:"event_id"`
	Type            string         `json:"type"`
	ItemID          string         `json:"item_id,omitempty"`
	SessionID       string         `json:"session_id,omitempty"`
	OccurredAt      time.Time      `json:"occurred_at"`
	ReceivedAt      time.Time      `json:"received_at,omitempty"`
	DwellMS         int64          `json:"dwell_ms,omitempty"`
	VisibleFraction float64        `json:"visible_fraction,omitempty"`
	Position        *int64         `json:"position,omitempty"`
	ClientName      string         `json:"client_name,omitempty"`
	Device          string         `json:"device,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
}

func validEventType(kind string) bool {
	for _, known := range EventTypes {
		if kind == known {
			return true
		}
	}
	return false
}

// ValidateEvent checks one event before it is stored.
func ValidateEvent(e *Event, now time.Time) error {
	e.EventID = strings.TrimSpace(e.EventID)
	if e.EventID == "" || len(e.EventID) > 128 {
		return invalid("event_id is required (at most 128 characters)")
	}
	if !validEventType(e.Type) {
		return invalid("event %s: type %q is not one of %s", e.EventID, e.Type, strings.Join(EventTypes, ", "))
	}
	if e.ItemID == "" && !cardlessEventTypes[e.Type] {
		return invalid("event %s: %s needs an item_id", e.EventID, e.Type)
	}
	if e.OccurredAt.IsZero() {
		return invalid("event %s: occurred_at is required", e.EventID)
	}
	if e.OccurredAt.After(now.Add(24 * time.Hour)) {
		return invalid("event %s: occurred_at is more than a day in the future", e.EventID)
	}
	if e.DwellMS < 0 || e.DwellMS > int64(24*time.Hour/time.Millisecond) {
		return invalid("event %s: dwell_ms must be between 0 and one day", e.EventID)
	}
	if e.VisibleFraction < 0 || e.VisibleFraction > 1 {
		return invalid("event %s: visible_fraction must be between 0 and 1", e.EventID)
	}
	return nil
}

// Rank orders live cards the way the phone shows them, after the Feeeed
// model: no unread count, so nothing is a backlog to clear. Higher score
// first, then newer; and no source runs three in a row while another is
// waiting. Seen cards never reach it: Feed leaves them out.
func Rank(items []Item) []Item {
	ordered := append([]Item(nil), items...)
	sort.SliceStable(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		ta, tb := freshness(a), freshness(b)
		if !ta.Equal(tb) {
			return ta.After(tb)
		}
		return a.ItemID < b.ItemID
	})
	out := make([]Item, 0, len(ordered))
	for len(ordered) > 0 {
		pick := 0
		if n := len(out); n >= 2 && out[n-1].Source == out[n-2].Source {
			for i, candidate := range ordered {
				if candidate.Source != out[n-1].Source {
					pick = i
					break
				}
			}
		}
		out = append(out, ordered[pick])
		ordered = append(ordered[:pick], ordered[pick+1:]...)
	}
	return out
}

func freshness(item Item) time.Time {
	if item.PublishedAt != nil && item.PublishedAt.After(item.CreatedAt) {
		return *item.PublishedAt
	}
	return item.CreatedAt
}
