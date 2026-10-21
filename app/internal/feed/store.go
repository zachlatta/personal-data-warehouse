package feed

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// epoch is how the warehouse stores "absent" timestamps (AGENTS.md).
var epoch = time.Unix(0, 0).UTC()

// Store reads and writes base_feed and reads marts_feed. The tables and
// views are created by the Python ensure path (feed.py), never here.
type Store struct {
	DB      *sql.DB
	Timeout time.Duration
	Now     func() time.Time
	// Expand is overridden only by integration tests with isolated schemas.
	Expand func(string) string
	// ResolveX reads an X post for a pushed x.com url (xpost.go); nil
	// leaves X links as plain cards. Register sets ResolveXPost.
	ResolveX XResolver
}

func NewStore(databaseURL string, timeout time.Duration) (*Store, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Store{DB: db, Timeout: timeout, Now: time.Now}, nil
}

func (s *Store) q(query string) string {
	if s.Expand != nil {
		return s.Expand(query)
	}
	return warehouse.ExpandRelations(query)
}

func (s *Store) now() time.Time {
	if s.Now == nil {
		return time.Now().UTC()
	}
	return s.Now().UTC()
}

func (s *Store) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.Timeout)
}

func orEpoch(t time.Time) time.Time {
	if t.IsZero() {
		return epoch
	}
	return t
}

func fromEpoch(t time.Time) *time.Time {
	if t.IsZero() || !t.After(epoch) {
		return nil
	}
	utc := t.UTC()
	return &utc
}

func tagsLiteral(tags []string) string {
	// A text[] literal: every element double-quoted with \ and " escaped.
	parts := make([]string, len(tags))
	for i, tag := range tags {
		parts[i] = `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(tag) + `"`
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func linksJSON(links []Link) (string, error) {
	if links == nil {
		links = []Link{}
	}
	raw, err := json.Marshal(links)
	return string(raw), err
}

// ListOptions selects cards for an agent; the phone's feed is Feed().
type ListOptions struct {
	Source           string
	ItemIDs          []string
	IncludeRemoved   bool
	IncludeDismissed bool
	Limit            int
}

const itemColumns = `i.item_id, i.source, i.item_key, i.title, i.body, i.url, i.links, i.image_url, i.media, i.embed, i.author, i.site,
	to_jsonb(i.tags), i.score, i.published_at, i.expires_at, i.metadata, i.pushed_by, i.created_at, i.updated_at,
	i.removed_at, i.removed_reason,
	COALESCE(e.impressions, 0), COALESCE(e.dwell_ms, 0), COALESCE(e.scroll_pasts, 0), COALESCE(e.opens, 0),
	COALESCE(e.links_opened, 0), COALESCE(e.read_ms, 0), COALESCE(e.expands, 0), COALESCE(e.dismissed, 0),
	COALESCE(e.dismiss_reason, ''), COALESCE(e.engaged, 0), COALESCE(e.engagement_score, 0), e.first_seen_at,
	COALESCE(e.saved, 0), e.saved_at,
	COALESCE((SELECT max(v.version) FROM @feed_item_versions v WHERE v.item_id = i.item_id), 0)`

func (s *Store) List(ctx context.Context, opts ListOptions) ([]Item, error) {
	where := []string{"TRUE"}
	args := []any{}
	if opts.Source != "" {
		args = append(args, opts.Source)
		where = append(where, fmt.Sprintf("i.source = $%d", len(args)))
	}
	if len(opts.ItemIDs) > 0 {
		args = append(args, tagsLiteral(opts.ItemIDs))
		where = append(where, fmt.Sprintf("i.item_id = ANY($%d::text[])", len(args)))
	}
	if !opts.IncludeRemoved {
		where = append(where, "i.removed_at = 'epoch'")
	}
	if !opts.IncludeDismissed {
		where = append(where, "COALESCE(e.dismissed, 0) = 0")
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 50
	}
	args = append(args, limit)
	return s.query(ctx, `SELECT `+itemColumns+`
		FROM @feed_items i LEFT JOIN @marts_feed_item_engagement e ON e.item_id = i.item_id
		WHERE `+strings.Join(where, " AND ")+fmt.Sprintf(`
		ORDER BY i.created_at DESC, i.item_id LIMIT $%d`, len(args)), args...)
}

// Feed is what the phone shows: live, unexpired, undismissed cards he has not
// seen yet, ranked. A card leaves the feed at its first impression (Zach,
// 2026-10-20: "after I've seen a post, don't show it to me again"); the
// ones he saved stay in Saved.
func (s *Store) Feed(ctx context.Context, limit int) ([]Item, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	// Read a wider pool than is shown so ranking has room to work.
	items, err := s.query(ctx, `SELECT `+itemColumns+`
		FROM @feed_items i LEFT JOIN @marts_feed_item_engagement e ON e.item_id = i.item_id
		WHERE i.removed_at = 'epoch' AND (i.expires_at = 'epoch' OR i.expires_at > $1)
		  AND COALESCE(e.dismissed, 0) = 0 AND COALESCE(e.impressions, 0) = 0
		ORDER BY i.score DESC, i.created_at DESC LIMIT $2`, s.now(), limit*3)
	if err != nil {
		return nil, err
	}
	ranked := Rank(items)
	if len(ranked) > limit {
		ranked = ranked[:limit]
	}
	return ranked, nil
}

// Saved is his read-later queue, most recently saved first. A saved card
// stays in it after it expires or an agent removes it from the feed: only he
// takes it out (unsave), or a dismissal.
func (s *Store) Saved(ctx context.Context, limit int) ([]Item, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	return s.query(ctx, `SELECT `+itemColumns+`
		FROM @feed_items i JOIN @marts_feed_item_engagement e ON e.item_id = i.item_id
		WHERE e.saved = 1 AND e.dismissed = 0
		ORDER BY e.saved_at DESC, i.item_id LIMIT $1`, limit)
}

// SetSaved puts cards in or takes them out of the read-later queue by
// recording save or unsave events, the same events the phone writes, so an
// agent saving on his behalf ("save that for me") and a tap on the phone
// are one history. Every id must name a card.
func (s *Store) SetSaved(ctx context.Context, ids []string, save bool, m Mutation) (int, error) {
	if len(ids) == 0 {
		return 0, invalid("pass item_id(s)")
	}
	known, err := s.List(ctx, ListOptions{ItemIDs: ids, IncludeRemoved: true, IncludeDismissed: true, Limit: len(ids)})
	if err != nil {
		return 0, err
	}
	found := map[string]bool{}
	for _, item := range known {
		found[item.ItemID] = true
	}
	kind := "unsave"
	if save {
		kind = "save"
	}
	now := s.now()
	events := make([]Event, 0, len(ids))
	for _, id := range ids {
		if !found[id] {
			return 0, fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		metadata := map[string]any{"via": "agent"}
		if m.Agent != "" {
			metadata["agent"] = m.Agent
		}
		if m.Reason != "" {
			metadata["reason"] = m.Reason
		}
		events = append(events, Event{EventID: newEventID(now), Type: kind, ItemID: id, OccurredAt: now, Metadata: metadata})
	}
	accepted, _, err := s.RecordEvents(ctx, events, m.Actor, "agent")
	return accepted, err
}

func newEventID(now time.Time) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("agent-%d-%s", now.UnixMilli(), hex.EncodeToString(b[:]))
}

func (s *Store) query(ctx context.Context, query string, args ...any) ([]Item, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	rows, err := s.DB.QueryContext(ctx, s.q(query), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Item{}
	for rows.Next() {
		var (
			item                        Item
			tags, links, media, embed   []byte
			metadata                    []byte
			published, expires, removed time.Time
			dismissed, engaged, saved   int64
			firstSeen, savedAt          sql.NullTime
		)
		if err := rows.Scan(&item.ItemID, &item.Source, &item.Key, &item.Title, &item.Body, &item.URL, &links,
			&item.ImageURL, &media, &embed, &item.Author, &item.Site, &tags, &item.Score, &published, &expires, &metadata, &item.PushedBy,
			&item.CreatedAt, &item.UpdatedAt, &removed, &item.RemovedReason,
			&item.Engagement.Impressions, &item.Engagement.DwellMS, &item.Engagement.ScrollPasts,
			&item.Engagement.Opens, &item.Engagement.LinksOpened, &item.Engagement.ReadMS, &item.Engagement.Expands,
			&dismissed, &item.Engagement.DismissReason, &engaged, &item.Engagement.Score, &firstSeen,
			&saved, &savedAt, &item.Version); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(media, &item.Media)
		if item.Media == nil {
			item.Media = []Media{}
		}
		item.Embed = decodeEmbed(embed)
		_ = json.Unmarshal(links, &item.Links)
		if item.Links == nil {
			item.Links = []Link{}
		}
		_ = json.Unmarshal(tags, &item.Tags)
		if item.Tags == nil {
			item.Tags = []string{}
		}
		if len(metadata) > 0 {
			_ = json.Unmarshal(metadata, &item.Metadata)
		}
		item.PublishedAt, item.ExpiresAt, item.RemovedAt = fromEpoch(published), fromEpoch(expires), fromEpoch(removed)
		item.CreatedAt, item.UpdatedAt = item.CreatedAt.UTC(), item.UpdatedAt.UTC()
		item.Engagement.Dismissed, item.Engagement.Engaged = dismissed == 1, engaged == 1
		if firstSeen.Valid {
			item.Engagement.FirstSeenAt = fromEpoch(firstSeen.Time)
		}
		item.Engagement.Saved = saved == 1
		if savedAt.Valid {
			item.Engagement.SavedAt = fromEpoch(savedAt.Time)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// RecordEvents stores validated events; a repeated event_id is a duplicate,
// not an error, so the phone can retry a batch blindly.
func (s *Store) RecordEvents(ctx context.Context, events []Event, clientName, device string) (accepted, duplicates int, err error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = tx.Rollback() }()
	statement := s.q(`
		INSERT INTO @feed_events (event_id, event_type, item_id, session_id, occurred_at, received_at,
			dwell_ms, visible_fraction, position, client_name, device, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
		ON CONFLICT (event_id) DO NOTHING`)
	now := s.now()
	for _, e := range events {
		metadata := []byte("{}")
		if e.Metadata != nil {
			if metadata, err = json.Marshal(e.Metadata); err != nil || len(metadata) > maxMetadataLen {
				return 0, 0, invalid("event %s: metadata must be JSON under %d bytes", e.EventID, maxMetadataLen)
			}
		}
		position := int64(-1)
		if e.Position != nil {
			position = *e.Position
		}
		res, err := tx.ExecContext(ctx, statement, e.EventID, e.Type, e.ItemID, e.SessionID, e.OccurredAt.UTC(), now,
			e.DwellMS, e.VisibleFraction, position, clientName, device, string(metadata))
		if err != nil {
			return 0, 0, fmt.Errorf("record event %s: %w", e.EventID, err)
		}
		if n, _ := res.RowsAffected(); n == 1 {
			accepted++
		} else {
			duplicates++
		}
	}
	return accepted, duplicates, tx.Commit()
}

// EventQuery selects raw engagement events.
type EventQuery struct {
	ItemID string
	Source string
	Types  []string
	Since  time.Time
	Limit  int
}

func (s *Store) Events(ctx context.Context, q EventQuery) ([]Event, error) {
	where := []string{"TRUE"}
	args := []any{}
	if q.ItemID != "" {
		args = append(args, q.ItemID)
		where = append(where, fmt.Sprintf("e.item_id = $%d", len(args)))
	}
	if q.Source != "" {
		args = append(args, q.Source)
		where = append(where, fmt.Sprintf("i.source = $%d", len(args)))
	}
	if len(q.Types) > 0 {
		args = append(args, tagsLiteral(q.Types))
		where = append(where, fmt.Sprintf("e.event_type = ANY($%d::text[])", len(args)))
	}
	if !q.Since.IsZero() {
		args = append(args, q.Since)
		where = append(where, fmt.Sprintf("e.occurred_at >= $%d", len(args)))
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit)
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	rows, err := s.DB.QueryContext(ctx, s.q(`
		SELECT e.event_id, e.event_type, e.item_id, e.session_id, e.occurred_at, e.received_at, e.dwell_ms,
		       e.visible_fraction, e.position, e.client_name, e.device, e.metadata
		FROM @feed_events e LEFT JOIN @feed_items i ON i.item_id = e.item_id
		WHERE `+strings.Join(where, " AND ")+fmt.Sprintf(`
		ORDER BY e.occurred_at DESC, e.event_id LIMIT $%d`, len(args))), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []Event{}
	for rows.Next() {
		var (
			e        Event
			position int64
			metadata []byte
		)
		if err := rows.Scan(&e.EventID, &e.Type, &e.ItemID, &e.SessionID, &e.OccurredAt, &e.ReceivedAt, &e.DwellMS,
			&e.VisibleFraction, &position, &e.ClientName, &e.Device, &metadata); err != nil {
			return nil, err
		}
		e.OccurredAt, e.ReceivedAt = e.OccurredAt.UTC(), e.ReceivedAt.UTC()
		if position >= 0 {
			e.Position = &position
		}
		if len(metadata) > 0 && string(metadata) != "{}" {
			_ = json.Unmarshal(metadata, &e.Metadata)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// SourceStats is a row of marts_feed.source_engagement.
type SourceStats struct {
	Source         string     `json:"source"`
	Items          int64      `json:"items"`
	LiveItems      int64      `json:"live_items"`
	ItemsSeen      int64      `json:"items_seen"`
	Impressions    int64      `json:"impressions"`
	DwellMS        int64      `json:"dwell_ms"`
	ItemsOpened    int64      `json:"items_opened"`
	ReadMS         int64      `json:"read_ms"`
	ItemsEngaged   int64      `json:"items_engaged"`
	ItemsDismissed int64      `json:"items_dismissed"`
	ItemsSaved     int64      `json:"items_saved"`
	OpenRate       *float64   `json:"open_rate"`
	EngagedRate    *float64   `json:"engaged_rate"`
	DismissRate    *float64   `json:"dismiss_rate"`
	AvgScore       *float64   `json:"avg_engagement_score"`
	LastPushedAt   *time.Time `json:"last_pushed_at,omitempty"`
	LastEventAt    *time.Time `json:"last_event_at,omitempty"`
}

// Stats is the agent's read of how the feed is landing.
type Stats struct {
	Sources      []SourceStats    `json:"sources"`
	EventsByType map[string]int64 `json:"events_by_type"`
	Since        time.Time        `json:"since"`
	Sessions     int64            `json:"sessions"`
	TopItems     []Item           `json:"top_items"`
	Ignored      []Item           `json:"ignored"`
}

func (s *Store) Stats(ctx context.Context, source string, since time.Time) (Stats, error) {
	out := Stats{Sources: []SourceStats{}, EventsByType: map[string]int64{}, Since: since}
	qctx, cancel := s.ctx(ctx)
	defer cancel()
	rows, err := s.DB.QueryContext(qctx, s.q(`
		SELECT source, items, live_items, items_seen, impressions, dwell_ms, items_opened, read_ms, items_engaged,
		       items_dismissed, open_rate::float8, engaged_rate::float8, dismiss_rate::float8,
		       avg_engagement_score::float8, last_pushed_at, last_event_at, items_saved
		FROM @marts_feed_source_engagement WHERE ($1 = '' OR source = $1) ORDER BY items DESC, source`), source)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var (
			row              SourceStats
			pushed, lastSeen sql.NullTime
		)
		if err := rows.Scan(&row.Source, &row.Items, &row.LiveItems, &row.ItemsSeen, &row.Impressions, &row.DwellMS,
			&row.ItemsOpened, &row.ReadMS, &row.ItemsEngaged, &row.ItemsDismissed, &row.OpenRate, &row.EngagedRate,
			&row.DismissRate, &row.AvgScore, &pushed, &lastSeen, &row.ItemsSaved); err != nil {
			rows.Close()
			return out, err
		}
		if pushed.Valid {
			row.LastPushedAt = fromEpoch(pushed.Time)
		}
		if lastSeen.Valid {
			row.LastEventAt = fromEpoch(lastSeen.Time)
		}
		out.Sources = append(out.Sources, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}
	typeRows, err := s.DB.QueryContext(qctx, s.q(`
		SELECT e.event_type, count(*), count(DISTINCT e.session_id) FILTER (WHERE e.event_type = 'session_start')
		FROM @feed_events e LEFT JOIN @feed_items i ON i.item_id = e.item_id
		WHERE e.occurred_at >= $1 AND ($2 = '' OR i.source = $2)
		GROUP BY e.event_type`), since, source)
	if err != nil {
		return out, err
	}
	for typeRows.Next() {
		var (
			kind    string
			n, sess int64
		)
		if err := typeRows.Scan(&kind, &n, &sess); err != nil {
			typeRows.Close()
			return out, err
		}
		out.EventsByType[kind] = n
		out.Sessions += sess
	}
	typeRows.Close()
	if err := typeRows.Err(); err != nil {
		return out, err
	}
	sourceFilter := ""
	args := []any{}
	if source != "" {
		args = append(args, source)
		sourceFilter = " AND i.source = $1"
	}
	if out.TopItems, err = s.query(ctx, `SELECT `+itemColumns+`
		FROM @feed_items i JOIN @marts_feed_item_engagement e ON e.item_id = i.item_id
		WHERE e.engaged = 1`+sourceFilter+`
		ORDER BY e.engagement_score DESC, e.read_ms + e.dwell_ms DESC LIMIT 10`, args...); err != nil {
		return out, err
	}
	// Seen at least twice and never opened, expanded, read or dismissed:
	// what the feed keeps showing that is not landing.
	out.Ignored, err = s.query(ctx, `SELECT `+itemColumns+`
		FROM @feed_items i JOIN @marts_feed_item_engagement e ON e.item_id = i.item_id
		WHERE e.impressions >= 2 AND e.engaged = 0 AND e.dismissed = 0`+sourceFilter+`
		ORDER BY e.impressions DESC LIMIT 10`, args...)
	return out, err
}
