package feed

// Every write to a card goes through mutate(), which names who made it, for
// which agent, why, and as what action. The trigger on base_feed.items
// (feed.py) turns each changed row into an immutable base_feed.item_versions
// row carrying those settings, a diff and a full snapshot; nothing here
// writes versions itself, so no path can change a card without history.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Mutation is the provenance stamped onto every version a write produces.
type Mutation struct {
	Actor        string // the pdw client name the bearer authenticated as
	Agent        string // optional: which agent or schedule acted
	Reason       string
	Action       string // empty lets the trigger classify the change
	RestoredFrom int64
}

var ErrNotFound = errors.New("no such feed item")

func (s *Store) mutate(ctx context.Context, m Mutation, fn func(ctx context.Context, tx *sql.Tx) error) error {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := setProvenance(ctx, tx, m); err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func setProvenance(ctx context.Context, tx *sql.Tx, m Mutation) error {
	_, err := tx.ExecContext(ctx, `SELECT set_config('pdw.feed_actor', $1, true), set_config('pdw.feed_agent', $2, true),
		set_config('pdw.feed_reason', $3, true), set_config('pdw.feed_action', $4, true),
		set_config('pdw.feed_restored_from', $5, true)`,
		m.Actor, m.Agent, m.Reason, m.Action, strconv.FormatInt(m.RestoredFrom, 10))
	return err
}

// PushResult is one card's outcome.
type PushResult struct {
	ItemID string `json:"item_id"`
	Status string `json:"status"` // created | updated | unchanged
}

// Push upserts cards by (source, key). Re-pushing a removed card restores
// it; a score left out keeps the stored one; a re-push that changes nothing
// writes nothing (and so no version).
func (s *Store) Push(ctx context.Context, items []normalized, m Mutation) ([]PushResult, error) {
	results := make([]PushResult, 0, len(items))
	err := s.mutate(ctx, m, func(ctx context.Context, tx *sql.Tx) error {
		statement := s.q(`
			INSERT INTO @feed_items AS f (item_id, source, item_key, title, body, url, image_url, author, site,
				tags, score, published_at, expires_at, metadata, pushed_by, created_at, updated_at, links, media, embed)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::text[], COALESCE($11::float8, 0), $12, $13, $14::jsonb, $15, $16, $16,
				$17::jsonb, $18::jsonb, $19::jsonb)
			ON CONFLICT (item_id) DO UPDATE SET
				title = EXCLUDED.title, body = EXCLUDED.body, url = EXCLUDED.url, links = EXCLUDED.links,
				image_url = EXCLUDED.image_url, media = EXCLUDED.media, embed = EXCLUDED.embed,
				author = EXCLUDED.author, site = EXCLUDED.site, tags = EXCLUDED.tags,
				score = COALESCE($11::float8, f.score), published_at = EXCLUDED.published_at,
				expires_at = EXCLUDED.expires_at, metadata = EXCLUDED.metadata, pushed_by = EXCLUDED.pushed_by,
				updated_at = EXCLUDED.updated_at, removed_at = 'epoch', removed_reason = ''
			WHERE (f.title, f.body, f.url, f.links, f.image_url, f.media, f.embed, f.author, f.site, f.tags, f.published_at,
			       f.expires_at, f.metadata, f.removed_at)
			      IS DISTINCT FROM (EXCLUDED.title, EXCLUDED.body, EXCLUDED.url, EXCLUDED.links, EXCLUDED.image_url, EXCLUDED.media,
			       EXCLUDED.embed, EXCLUDED.author, EXCLUDED.site, EXCLUDED.tags, EXCLUDED.published_at, EXCLUDED.expires_at,
			       EXCLUDED.metadata, 'epoch'::timestamptz)
			   OR ($11::float8 IS NOT NULL AND $11::float8 IS DISTINCT FROM f.score)
			RETURNING (xmax = 0)`)
		now := s.now()
		for _, item := range items {
			metadata, err := json.Marshal(item.Metadata)
			if err != nil {
				return invalid("item %s: metadata is not JSON-encodable", item.ItemID)
			}
			if len(metadata) > maxMetadataLen {
				return invalid("item %s: metadata is larger than %d bytes", item.ItemID, maxMetadataLen)
			}
			links, err := linksJSON(item.Links)
			if err != nil {
				return err
			}
			media, err := mediaJSON(item.Media)
			if err != nil {
				return err
			}
			embed, err := embedJSON(item.Embed)
			if err != nil {
				return invalid("item %s: %s", item.ItemID, err.Error())
			}
			var inserted bool
			err = tx.QueryRowContext(ctx, statement, item.ItemID, item.Source, item.Key, item.Title, item.Body,
				item.URL, item.ImageURL, item.Author, item.Site, tagsLiteral(item.Tags), item.Score,
				orEpoch(item.PublishedAt), orEpoch(item.ExpiresAt), string(metadata), m.Actor, now, links, media, embed).Scan(&inserted)
			status := "updated"
			switch {
			case errors.Is(err, sql.ErrNoRows):
				status = "unchanged"
			case err != nil:
				return fmt.Errorf("push %s: %w", item.ItemID, err)
			case inserted:
				status = "created"
			}
			results = append(results, PushResult{ItemID: item.ItemID, Status: status})
		}
		return nil
	})
	return results, err
}

// Update edits cards: one (item_id), several (item_ids) or every live card
// of a source. Pointer fields left nil are kept; Clear empties fields.
type Update struct {
	ItemID      string         `json:"item_id,omitempty" jsonschema:"the card to change (source:key, from push or list)"`
	ItemIDs     []string       `json:"item_ids,omitempty" jsonschema:"several cards to change the same way"`
	Source      string         `json:"source,omitempty" jsonschema:"change every live card of this source the same way"`
	Title       *string        `json:"title,omitempty"`
	Body        *string        `json:"body,omitempty"`
	URL         *string        `json:"url,omitempty"`
	Links       *[]Link        `json:"links,omitempty" jsonschema:"replaces the card's extra links ([] clears them)"`
	ImageURL    *string        `json:"image_url,omitempty"`
	Media       *[]Media       `json:"media,omitempty" jsonschema:"replaces the card's photos and videos ([] clears them)"`
	Embed       *Embed         `json:"embed,omitempty" jsonschema:"replaces the embedded post"`
	Author      *string        `json:"author,omitempty"`
	Site        *string        `json:"site,omitempty"`
	Tags        []string       `json:"tags,omitempty" jsonschema:"replaces the tags"`
	Score       *float64       `json:"score,omitempty"`
	PublishedAt *string        `json:"published_at,omitempty"`
	ExpiresAt   *string        `json:"expires_at,omitempty" jsonschema:"RFC3339, YYYY-MM-DD or a duration from now (7d)"`
	Metadata    map[string]any `json:"metadata,omitempty" jsonschema:"replaces the card's metadata"`
	Clear       []string       `json:"clear,omitempty" jsonschema:"fields to empty: title, body, url, links, image_url, media, embed, author, site, tags, published_at, expires_at, metadata"`
	Agent       string         `json:"agent,omitempty" jsonschema:"which agent is acting, recorded on the version"`
	Reason      string         `json:"reason,omitempty" jsonschema:"why, recorded on the version"`
}

var clearable = map[string]string{
	"title": "title = ''", "body": "body = ''", "url": "url = ''", "links": "links = '[]'", "image_url": "image_url = ''",
	"media": "media = '[]'", "embed": "embed = '{}'",
	"author": "author = ''", "site": "site = ''", "tags": "tags = '{}'",
	"published_at": "published_at = 'epoch'", "expires_at": "expires_at = 'epoch'", "metadata": "metadata = '{}'",
}

func (u Update) targets() ([]string, string, error) {
	ids := append([]string(nil), u.ItemIDs...)
	if strings.TrimSpace(u.ItemID) != "" {
		ids = append(ids, strings.TrimSpace(u.ItemID))
	}
	source := strings.TrimSpace(u.Source)
	if len(ids) == 0 && source == "" {
		return nil, "", invalid("name the cards: item_id, item_ids or source")
	}
	return ids, source, nil
}

// Update applies one edit to every target; it returns how many cards changed.
func (s *Store) Update(ctx context.Context, u Update, m Mutation) (int64, error) {
	ids, source, err := u.targets()
	if err != nil {
		return 0, err
	}
	now := s.now()
	sets := []string{}
	args := []any{}
	given := map[string]bool{}
	add := func(column, expr string, value any) {
		given[column] = true
		args = append(args, value)
		sets = append(sets, fmt.Sprintf("%s = $%d%s", column, len(args), expr))
	}
	for _, field := range []struct {
		column string
		value  *string
		max    int
	}{{"title", u.Title, maxTitleChars}, {"body", u.Body, maxBodyChars}, {"author", u.Author, 0}, {"site", u.Site, 0}} {
		if field.value == nil {
			continue
		}
		if field.max > 0 && len([]rune(*field.value)) > field.max {
			return 0, invalid("%s is longer than %d characters", field.column, field.max)
		}
		add(field.column, "", strings.TrimSpace(*field.value))
	}
	for column, value := range map[string]*string{"url": u.URL, "image_url": u.ImageURL} {
		if value == nil {
			continue
		}
		checked, err := checkURL(column, *value, column == "image_url")
		if err != nil {
			return 0, err
		}
		add(column, "", checked)
	}
	if u.Links != nil {
		links, err := normalizeLinks(*u.Links, "")
		if err != nil {
			return 0, err
		}
		raw, err := linksJSON(links)
		if err != nil {
			return 0, err
		}
		add("links", "::jsonb", raw)
	}
	if u.Media != nil {
		media, err := normalizeMedia("media", *u.Media)
		if err != nil {
			return 0, err
		}
		raw, err := mediaJSON(media)
		if err != nil {
			return 0, err
		}
		add("media", "::jsonb", raw)
	}
	if u.Embed != nil {
		embed, err := normalizeEmbed(u.Embed, 0)
		if err != nil {
			return 0, err
		}
		raw, err := embedJSON(embed)
		if err != nil {
			return 0, err
		}
		add("embed", "::jsonb", raw)
	}
	if u.Tags != nil {
		tags, err := normalizeTags(u.Tags)
		if err != nil {
			return 0, err
		}
		add("tags", "::text[]", tagsLiteral(tags))
	}
	if u.Score != nil {
		add("score", "::float8", *u.Score)
	}
	for column, value := range map[string]*string{"published_at": u.PublishedAt, "expires_at": u.ExpiresAt} {
		if value == nil {
			continue
		}
		t, err := ParseTime(column, *value, now, column == "expires_at")
		if err != nil {
			return 0, err
		}
		add(column, "", orEpoch(t))
	}
	if u.Metadata != nil {
		raw, err := json.Marshal(u.Metadata)
		if err != nil || len(raw) > maxMetadataLen {
			return 0, invalid("metadata must be JSON under %d bytes", maxMetadataLen)
		}
		add("metadata", "::jsonb", string(raw))
	}
	for _, field := range u.Clear {
		expr, ok := clearable[field]
		if !ok {
			return 0, invalid("cannot clear %q; clearable: title, body, url, links, image_url, media, embed, author, site, tags, published_at, expires_at, metadata", field)
		}
		if given[field] {
			return 0, invalid("%s is both set and cleared", field)
		}
		sets = append(sets, expr)
	}
	if len(sets) == 0 {
		return 0, invalid("nothing to change: pass a field to set or clear")
	}
	args = append(args, now)
	sets = append(sets, fmt.Sprintf("updated_at = $%d", len(args)))
	args = append(args, tagsLiteral(ids), source)
	where := fmt.Sprintf("item_id = ANY($%d::text[]) OR ($%d <> '' AND source = $%d AND removed_at = 'epoch')", len(args)-1, len(args), len(args))
	var changed int64
	err = s.mutate(ctx, m, func(ctx context.Context, tx *sql.Tx) error {
		if len(ids) > 0 {
			var found int
			if err := tx.QueryRowContext(ctx, s.q("SELECT count(*) FROM @feed_items WHERE item_id = ANY($1::text[])"),
				tagsLiteral(ids)).Scan(&found); err != nil {
				return err
			}
			if found < len(uniq(ids)) {
				return ErrNotFound
			}
		}
		res, err := tx.ExecContext(ctx, s.q("UPDATE @feed_items SET "+strings.Join(sets, ", ")+" WHERE "+where), args...)
		if err != nil {
			return err
		}
		changed, _ = res.RowsAffected()
		return nil
	})
	return changed, err
}

func uniq(ids []string) map[string]bool {
	out := map[string]bool{}
	for _, id := range ids {
		out[id] = true
	}
	return out
}

// Remove hides cards (by id, or every live card of a source). It is a soft
// delete: a `remove` version records it and Restore takes it back.
func (s *Store) Remove(ctx context.Context, ids []string, source string, m Mutation) (int64, error) {
	var n int64
	err := s.mutate(ctx, m, func(ctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, s.q(`
			UPDATE @feed_items SET removed_at = $1, removed_reason = $2, updated_at = $1
			WHERE removed_at = 'epoch' AND (item_id = ANY($3::text[]) OR ($4 <> '' AND source = $4))`),
			s.now(), m.Reason, tagsLiteral(ids), source)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		return nil
	})
	return n, err
}

// SetScores writes rank weights in one transaction; it fails if any id is unknown.
func (s *Store) SetScores(ctx context.Context, scores map[string]float64, m Mutation) error {
	m.Action = "rank"
	return s.mutate(ctx, m, func(ctx context.Context, tx *sql.Tx) error {
		statement := s.q("UPDATE @feed_items SET score = $1, updated_at = $2 WHERE item_id = $3")
		now := s.now()
		for id, score := range scores {
			res, err := tx.ExecContext(ctx, statement, score, now, id)
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n == 0 {
				return invalid("unknown item_id %q", id)
			}
		}
		return nil
	})
}

// RestoreResult is one card's outcome.
type RestoreResult struct {
	ItemID      string `json:"item_id"`
	Status      string `json:"status"` // restored | already_live
	FromVersion int64  `json:"from_version,omitempty"`
	Version     int64  `json:"version,omitempty"`
}

// The card fields a restore writes back from a snapshot. Identity
// (item_id, source, item_key) never changes, and created_at is history.
const contentColumns = "title, body, url, links, image_url, media, embed, author, site, tags, score, published_at, expires_at, metadata"

// Columns added after versioning began are absent from older snapshots.
var snapshotDefaults = map[string]string{"media": "'[]'::jsonb", "embed": "'{}'::jsonb"}

// Restore brings cards back. With version 0 it undoes a removal (or a hard
// delete, from the tombstone's snapshot) and keeps the content; with a
// version it also puts that version's content back. Either way the restore
// is itself a new version: nothing in between is lost.
func (s *Store) Restore(ctx context.Context, ids []string, source string, version int64, m Mutation) ([]RestoreResult, error) {
	m.Action = "restore"
	results := []RestoreResult{}
	err := s.mutate(ctx, m, func(ctx context.Context, tx *sql.Tx) error {
		if len(ids) == 0 && source != "" {
			if version > 0 {
				return invalid("restore a whole source without a version, or name one card with --version")
			}
			rows, err := tx.QueryContext(ctx, s.q(`SELECT item_id FROM @feed_items WHERE source = $1 AND removed_at <> 'epoch' ORDER BY item_id`), source)
			if err != nil {
				return err
			}
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					rows.Close()
					return err
				}
				ids = append(ids, id)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
		}
		if len(ids) == 0 {
			return invalid("name the cards to restore: item ids or a source with removed cards")
		}
		if version > 0 && len(ids) > 1 {
			return invalid("--version restores one card at a time")
		}
		for _, id := range ids {
			result, err := s.restoreOne(ctx, tx, id, version, m)
			if err != nil {
				return err
			}
			results = append(results, result)
		}
		return nil
	})
	return results, err
}

func (s *Store) restoreOne(ctx context.Context, tx *sql.Tx, id string, version int64, m Mutation) (RestoreResult, error) {
	var removed time.Time
	err := tx.QueryRowContext(ctx, s.q("SELECT removed_at FROM @feed_items WHERE item_id = $1 FOR UPDATE"), id).Scan(&removed)
	exists := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return RestoreResult{}, err
	}
	from := version
	if !exists && from == 0 {
		// Hard-deleted: the tombstone's snapshot is the card as it was.
		if err := tx.QueryRowContext(ctx, s.q("SELECT COALESCE(max(version), 0) FROM @feed_item_versions WHERE item_id = $1"), id).Scan(&from); err != nil {
			return RestoreResult{}, err
		}
	}
	if from == 0 {
		if !exists {
			return RestoreResult{}, ErrNotFound
		}
		if !removed.After(epoch) {
			return RestoreResult{ItemID: id, Status: "already_live"}, nil
		}
		if _, err := tx.ExecContext(ctx, s.q("UPDATE @feed_items SET removed_at = 'epoch', removed_reason = '', updated_at = $2 WHERE item_id = $1"), id, s.now()); err != nil {
			return RestoreResult{}, err
		}
		return s.restored(ctx, tx, id, 0)
	}
	var snapshot []byte
	err = tx.QueryRowContext(ctx, s.q("SELECT snapshot FROM @feed_item_versions WHERE item_id = $1 AND version = $2"), id, from).Scan(&snapshot)
	if errors.Is(err, sql.ErrNoRows) {
		return RestoreResult{}, invalid("card %s has no version %d (pdw feed history %s)", id, from, id)
	}
	if err != nil {
		return RestoreResult{}, err
	}
	m.RestoredFrom = from
	if err := setProvenance(ctx, tx, m); err != nil {
		return RestoreResult{}, err
	}
	if exists {
		_, err = tx.ExecContext(ctx, s.q(`UPDATE @feed_items SET (`+contentColumns+`) =
			(SELECT `+prefixed("r.", contentColumns)+` FROM jsonb_populate_record(NULL::@feed_items, $2::jsonb) r),
			removed_at = 'epoch', removed_reason = '', updated_at = $3 WHERE item_id = $1`), id, string(snapshot), s.now())
	} else {
		_, err = tx.ExecContext(ctx, s.q(`INSERT INTO @feed_items (item_id, source, item_key, pushed_by, created_at, updated_at, `+contentColumns+`)
			SELECT r.item_id, r.source, r.item_key, r.pushed_by, r.created_at, $2, `+prefixed("r.", contentColumns)+`
			FROM jsonb_populate_record(NULL::@feed_items, $1::jsonb) r`), string(snapshot), s.now())
	}
	if err != nil {
		return RestoreResult{}, fmt.Errorf("restore %s from version %d: %w", id, from, err)
	}
	return s.restored(ctx, tx, id, from)
}

func (s *Store) restored(ctx context.Context, tx *sql.Tx, id string, from int64) (RestoreResult, error) {
	result := RestoreResult{ItemID: id, Status: "restored", FromVersion: from}
	err := tx.QueryRowContext(ctx, s.q("SELECT COALESCE(max(version), 0) FROM @feed_item_versions WHERE item_id = $1"), id).Scan(&result.Version)
	return result, err
}

func prefixed(prefix, columns string) string {
	parts := strings.Split(columns, ", ")
	for i := range parts {
		if fallback, ok := snapshotDefaults[parts[i]]; ok {
			parts[i] = "COALESCE(" + prefix + parts[i] + ", " + fallback + ")"
			continue
		}
		parts[i] = prefix + parts[i]
	}
	return strings.Join(parts, ", ")
}

// Version is one row of base_feed.item_versions.
type Version struct {
	ItemID        string         `json:"item_id"`
	Version       int64          `json:"version"`
	Action        string         `json:"action"`
	Actor         string         `json:"actor"`
	Agent         string         `json:"agent,omitempty"`
	Reason        string         `json:"reason,omitempty"`
	RestoredFrom  int64          `json:"restored_from,omitempty"`
	ChangedFields []string       `json:"changed_fields"`
	Diff          map[string]any `json:"diff,omitempty"`
	Snapshot      map[string]any `json:"snapshot,omitempty"`
	ChangedAt     time.Time      `json:"changed_at"`
}

// HistoryQuery selects versions, newest first.
type HistoryQuery struct {
	ItemID    string
	Source    string
	Since     time.Time
	Limit     int
	Snapshots bool
}

func (s *Store) History(ctx context.Context, q HistoryQuery) ([]Version, error) {
	where := []string{"TRUE"}
	args := []any{}
	if q.ItemID != "" {
		args = append(args, q.ItemID)
		where = append(where, fmt.Sprintf("item_id = $%d", len(args)))
	}
	if q.Source != "" {
		args = append(args, q.Source)
		where = append(where, fmt.Sprintf("snapshot->>'source' = $%d", len(args)))
	}
	if !q.Since.IsZero() {
		args = append(args, q.Since)
		where = append(where, fmt.Sprintf("changed_at >= $%d", len(args)))
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	args = append(args, limit)
	snapshot := "'{}'::jsonb"
	if q.Snapshots {
		snapshot = "snapshot"
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	rows, err := s.DB.QueryContext(ctx, s.q(`
		SELECT item_id, version, action, actor, agent, reason, restored_from, to_jsonb(changed_fields), diff, `+snapshot+`, changed_at
		FROM @feed_item_versions WHERE `+strings.Join(where, " AND ")+fmt.Sprintf(`
		ORDER BY changed_at DESC, item_id, version DESC LIMIT $%d`, len(args))), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Version{}
	for rows.Next() {
		var (
			v                      Version
			fields, diff, snapJSON []byte
		)
		if err := rows.Scan(&v.ItemID, &v.Version, &v.Action, &v.Actor, &v.Agent, &v.Reason, &v.RestoredFrom,
			&fields, &diff, &snapJSON, &v.ChangedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(fields, &v.ChangedFields)
		if v.ChangedFields == nil {
			v.ChangedFields = []string{}
		}
		_ = json.Unmarshal(diff, &v.Diff)
		if len(v.Diff) == 0 {
			v.Diff = nil
		}
		_ = json.Unmarshal(snapJSON, &v.Snapshot)
		if len(v.Snapshot) == 0 {
			v.Snapshot = nil
		}
		v.ChangedAt = v.ChangedAt.UTC()
		out = append(out, v)
	}
	return out, rows.Err()
}

// GetResult is one card: its current state, or one version of it.
type GetResult struct {
	Item     *Item    `json:"item,omitempty"`
	Deleted  bool     `json:"deleted,omitempty"`
	Version  *Version `json:"version,omitempty"`
	Versions int64    `json:"versions"`
}

// Get reads a card as it is now (removed or dismissed included), or as it
// was at a version. A hard-deleted card answers with its tombstone.
func (s *Store) Get(ctx context.Context, id string, version int64) (GetResult, error) {
	var out GetResult
	qctx, cancel := s.ctx(ctx)
	err := s.DB.QueryRowContext(qctx, s.q("SELECT count(*) FROM @feed_item_versions WHERE item_id = $1"), id).Scan(&out.Versions)
	cancel()
	if err != nil {
		return out, err
	}
	if version == 0 {
		items, err := s.query(ctx, `SELECT `+itemColumns+`
			FROM @feed_items i LEFT JOIN @marts_feed_item_engagement e ON e.item_id = i.item_id
			WHERE i.item_id = $1`, id)
		if err != nil {
			return out, err
		}
		if len(items) == 1 {
			out.Item = &items[0]
			return out, nil
		}
		if out.Versions == 0 {
			return out, ErrNotFound
		}
		out.Deleted = true
		version = out.Versions
	}
	versions, err := s.History(ctx, HistoryQuery{ItemID: id, Limit: 100000, Snapshots: true})
	if err != nil {
		return out, err
	}
	for i := range versions {
		if versions[i].Version == version {
			out.Version = &versions[i]
			return out, nil
		}
	}
	if out.Versions == 0 {
		return out, ErrNotFound
	}
	return out, invalid("card %s has no version %d; it has %d", id, version, out.Versions)
}
