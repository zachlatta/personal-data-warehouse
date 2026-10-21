package feed

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

// toolError turns a caller mistake into the 400 the transports expect.
func toolError(err error) error {
	var invalidErr *ValidationError
	if errors.As(err, &invalidErr) {
		return &tool.InvalidInputError{Message: invalidErr.Message}
	}
	if errors.Is(err, ErrNotFound) {
		return &tool.InvalidInputError{Message: err.Error()}
	}
	return err
}

// provenance is what every mutating tool records on the versions it writes.
func provenance(ctx context.Context, agent, reason string) Mutation {
	return Mutation{Actor: pdwauth.ClientNameFromContext(ctx), Agent: strings.TrimSpace(agent), Reason: strings.TrimSpace(reason)}
}

type pushInput struct {
	Source string     `json:"source,omitempty" jsonschema:"default source for every item that does not name its own"`
	Items  []PushItem `json:"items" jsonschema:"cards to add or update (at most 500); each is idempotent on (source, key)"`
	Agent  string     `json:"agent,omitempty" jsonschema:"which agent is acting, recorded on each version"`
	Reason string     `json:"reason,omitempty"`
}

type pushOutput struct {
	Created   int          `json:"created"`
	Updated   int          `json:"updated"`
	Unchanged int          `json:"unchanged"`
	Items     []PushResult `json:"items"`
}

type updateOutput struct {
	Updated int64 `json:"updated"`
}

type getInput struct {
	ItemID  string `json:"item_id" jsonschema:"the card (source:key)"`
	Version int64  `json:"version,omitempty" jsonschema:"read the card as it was at this version instead of now"`
}

type restoreInput struct {
	ItemID  string   `json:"item_id,omitempty"`
	ItemIDs []string `json:"item_ids,omitempty"`
	Source  string   `json:"source,omitempty" jsonschema:"restore every removed card of this source"`
	Version int64    `json:"version,omitempty" jsonschema:"also put back the content of this version (one card only); omit to just undo the removal"`
	Agent   string   `json:"agent,omitempty"`
	Reason  string   `json:"reason,omitempty"`
}

type restoreOutput struct {
	Restored int             `json:"restored"`
	Items    []RestoreResult `json:"items"`
}

type historyInput struct {
	ItemID    string `json:"item_id,omitempty" jsonschema:"one card's versions; omit for recent changes across the feed"`
	Source    string `json:"source,omitempty"`
	Since     string `json:"since,omitempty" jsonschema:"RFC3339, YYYY-MM-DD, or a duration back from now (7d)"`
	Limit     int    `json:"limit,omitempty" jsonschema:"default 50, at most 1000"`
	Snapshots bool   `json:"snapshots,omitempty" jsonschema:"include each version's full card snapshot"`
}

type historyOutput struct {
	Count    int       `json:"count"`
	Versions []Version `json:"versions"`
}

type removeInput struct {
	ItemIDs []string `json:"item_ids,omitempty" jsonschema:"cards to hide"`
	ItemID  string   `json:"item_id,omitempty"`
	Source  string   `json:"source,omitempty" jsonschema:"hide every live card of this source"`
	Reason  string   `json:"reason,omitempty"`
	Agent   string   `json:"agent,omitempty"`
}

type removeOutput struct {
	Removed int64 `json:"removed"`
}

type rankInput struct {
	Order  []string           `json:"order,omitempty" jsonschema:"item ids best first; they get scores len(order) down to 1"`
	Scores map[string]float64 `json:"scores,omitempty" jsonschema:"item id to score; higher shows first"`
	Agent  string             `json:"agent,omitempty"`
	Reason string             `json:"reason,omitempty"`
}

type rankOutput struct {
	Ranked int `json:"ranked"`
}

type listInput struct {
	Source           string   `json:"source,omitempty"`
	ItemIDs          []string `json:"item_ids,omitempty"`
	View             string   `json:"view,omitempty" jsonschema:"recent (default, newest pushed first), feed (exactly what the phone shows, ranked) or saved (his read-later queue, newest saved first)"`
	IncludeRemoved   bool     `json:"include_removed,omitempty"`
	IncludeDismissed bool     `json:"include_dismissed,omitempty"`
	Limit            int      `json:"limit,omitempty" jsonschema:"default 50, at most 500"`
}

type listOutput struct {
	Count int    `json:"count"`
	Items []Item `json:"items"`
}

type saveInput struct {
	ItemID  string   `json:"item_id,omitempty"`
	ItemIDs []string `json:"item_ids,omitempty"`
	Unsave  bool     `json:"unsave,omitempty" jsonschema:"take the cards out of the queue instead"`
	Agent   string   `json:"agent,omitempty"`
	Reason  string   `json:"reason,omitempty" jsonschema:"why, e.g. Zach asked in a voice memo"`
}

type saveOutput struct {
	Saved   int `json:"saved"`
	Unsaved int `json:"unsaved"`
}

type statsInput struct {
	Source string `json:"source,omitempty"`
	Since  string `json:"since,omitempty" jsonschema:"window for events_by_type and sessions: RFC3339, YYYY-MM-DD, or a duration back from now (7d, 24h); default 7d"`
}

type eventsInput struct {
	ItemID string   `json:"item_id,omitempty"`
	Source string   `json:"source,omitempty"`
	Types  []string `json:"types,omitempty" jsonschema:"event types: session_start, session_end, impression, open, close, expand, dismiss, undismiss, share, media, save, unsave"`
	Since  string   `json:"since,omitempty" jsonschema:"RFC3339, YYYY-MM-DD, or a duration back from now (7d)"`
	Limit  int      `json:"limit,omitempty" jsonschema:"default 100, at most 1000"`
}

type eventsOutput struct {
	Count  int     `json:"count"`
	Events []Event `json:"events"`
}

func sinceTime(raw string, now time.Time, fallback time.Duration) (time.Time, error) {
	if strings.TrimSpace(raw) == "" {
		if fallback == 0 {
			return time.Time{}, nil
		}
		return now.Add(-fallback), nil
	}
	if d, ok := parseDuration(strings.TrimSpace(raw)); ok {
		return now.Add(-d), nil
	}
	return ParseTime("since", raw, now, false)
}

// Tools are the agent surface, on MCP and as `pdw call` / `pdw feed`.
func Tools(store *Store) []tool.Tool {
	return []tool.Tool{
		&tool.Typed[pushInput, pushOutput]{
			NameStr:  "feed_push",
			TitleStr: "Push Cards to the Feed Tab",
			DescriptionStr: "Add or update cards in the Feed tab of Zach's PDW iOS app (full-width cards he scrolls like a social feed). " +
				"Each item needs a source (the agent or feed name, here or per item) and a title, body or url; re-pushing the same " +
				"(source, key) updates the card instead of duplicating it, and the key defaults to the url. Optional: links (more links " +
				"on one card, [{url, title}], e.g. the paper, the repo and the discussion), body (markdown), media (photos and " +
				"videos shown as a swipeable carousel), " +
				"https image_url, author, site, tags, score (higher shows first), published_at, expires_at (RFC3339 or 48h/7d). " +
				"An x.com post url is embedded: the post's text, author, photos, video and quoted post show inside the card, not as a link. " +
				"Every impression, dwell, open (per link), read time and dismissal is recorded; there is no like button, interest is " +
				"measured (engagement_score): read it back with feed_stats. CLI: pdw feed push (alias add). Every create and change is kept as a version (feed_history).",
			Handle: func(ctx context.Context, in pushInput) (pushOutput, error) {
				if len(in.Items) == 0 {
					return pushOutput{}, &tool.InvalidInputError{Message: "items is empty: pass at least one card"}
				}
				if len(in.Items) > MaxPushItems {
					return pushOutput{}, &tool.InvalidInputError{Message: fmt.Sprintf("at most %d items per push", MaxPushItems)}
				}
				embedXPosts(ctx, store.ResolveX, in.Items)
				now := store.now()
				batch := make([]normalized, 0, len(in.Items))
				seen := map[string]int{}
				for i, raw := range in.Items {
					item, err := normalizePush(raw, in.Source, now)
					if err != nil {
						return pushOutput{}, toolError(fmt.Errorf("items[%d]: %w", i, err))
					}
					if first, dup := seen[item.ItemID]; dup {
						return pushOutput{}, &tool.InvalidInputError{Message: fmt.Sprintf("items[%d] and items[%d] are the same card (%s)", first, i, item.ItemID)}
					}
					seen[item.ItemID] = i
					batch = append(batch, item)
				}
				results, err := store.Push(ctx, batch, provenance(ctx, in.Agent, in.Reason))
				if err != nil {
					return pushOutput{}, toolError(err)
				}
				out := pushOutput{Items: results}
				for _, r := range results {
					switch r.Status {
					case "created":
						out.Created++
					case "unchanged":
						out.Unchanged++
					default:
						out.Updated++
					}
				}
				return out, nil
			},
		},
		&tool.Typed[getInput, GetResult]{
			NameStr:        "feed_get",
			TitleStr:       "Get a Feed Card",
			DescriptionStr: "One Feed tab card in full: every field, its engagement, and its current version number, whether live, removed or dismissed. With version, the card as it was at that version (a full snapshot). A card deleted outright answers with its last snapshot and deleted=true. CLI: pdw feed get.",
			Handle: func(ctx context.Context, in getInput) (GetResult, error) {
				if strings.TrimSpace(in.ItemID) == "" {
					return GetResult{}, &tool.InvalidInputError{Message: "item_id is required"}
				}
				out, err := store.Get(ctx, strings.TrimSpace(in.ItemID), in.Version)
				if errors.Is(err, ErrNotFound) {
					return out, &tool.InvalidInputError{Message: fmt.Sprintf("no feed card %q, now or in its history (pdw feed list --all shows ids)", in.ItemID)}
				}
				return out, toolError(err)
			},
		},
		&tool.Typed[Update, updateOutput]{
			NameStr:  "feed_update",
			TitleStr: "Edit Feed Cards",
			DescriptionStr: "Edit Feed tab cards: one (item_id), several (item_ids) or every live card of a source. Any field can be set " +
				"(title, body, url, image_url, author, site, tags, score, published_at, expires_at, metadata); clear lists fields to empty. " +
				"Fields left out are kept. Every change writes a new version; the old one stays in feed_history. CLI: pdw feed update (alias edit).",
			Handle: func(ctx context.Context, in Update) (updateOutput, error) {
				n, err := store.Update(ctx, in, provenance(ctx, in.Agent, in.Reason))
				if errors.Is(err, ErrNotFound) {
					return updateOutput{}, &tool.InvalidInputError{Message: "a named card does not exist (pdw feed list --all shows ids; a deleted card comes back with feed_restore)"}
				}
				return updateOutput{Updated: n}, toolError(err)
			},
		},
		&tool.Typed[removeInput, removeOutput]{
			NameStr:        "feed_remove",
			TitleStr:       "Remove Feed Cards",
			DescriptionStr: "Remove Feed tab cards by item_id(s), or every live card of a source. Removal is soft: the card leaves the phone, its engagement and every version are kept, and feed_restore brings it back. CLI: pdw feed remove (alias delete).",
			Handle: func(ctx context.Context, in removeInput) (removeOutput, error) {
				ids := in.ItemIDs
				if strings.TrimSpace(in.ItemID) != "" {
					ids = append(ids, strings.TrimSpace(in.ItemID))
				}
				if len(ids) == 0 && strings.TrimSpace(in.Source) == "" {
					return removeOutput{}, &tool.InvalidInputError{Message: "pass item_id(s) or a source"}
				}
				n, err := store.Remove(ctx, ids, strings.TrimSpace(in.Source), provenance(ctx, in.Agent, in.Reason))
				return removeOutput{Removed: n}, toolError(err)
			},
		},
		&tool.Typed[restoreInput, restoreOutput]{
			NameStr:        "feed_restore",
			TitleStr:       "Restore Feed Cards",
			DescriptionStr: "Bring back removed (or deleted) Feed tab cards: by item_id(s) or every removed card of a source. With version (one card), also put back that version's content. The restore is a new version; nothing in between is lost. CLI: pdw feed restore.",
			Handle: func(ctx context.Context, in restoreInput) (restoreOutput, error) {
				ids := in.ItemIDs
				if strings.TrimSpace(in.ItemID) != "" {
					ids = append(ids, strings.TrimSpace(in.ItemID))
				}
				results, err := store.Restore(ctx, ids, strings.TrimSpace(in.Source), in.Version, provenance(ctx, in.Agent, in.Reason))
				if errors.Is(err, ErrNotFound) {
					return restoreOutput{}, &tool.InvalidInputError{Message: "a named card has no history to restore from"}
				}
				out := restoreOutput{Items: results}
				for _, r := range results {
					if r.Status == "restored" {
						out.Restored++
					}
				}
				return out, toolError(err)
			},
		},
		&tool.Typed[historyInput, historyOutput]{
			NameStr:        "feed_history",
			TitleStr:       "Feed Card History",
			DescriptionStr: "Every version of a Feed tab card, newest first: action (create, update, rank, remove, restore, delete), who (client and agent), when, why, the fields that changed with from/to values, and optionally the full snapshot. Without item_id: recent changes across the feed or one source. Versions are immutable. CLI: pdw feed history.",
			Handle: func(ctx context.Context, in historyInput) (historyOutput, error) {
				if in.Limit > 1000 {
					return historyOutput{}, &tool.InvalidInputError{Message: "limit is at most 1000"}
				}
				since, err := sinceTime(in.Since, store.now(), 0)
				if err != nil {
					return historyOutput{}, toolError(err)
				}
				versions, err := store.History(ctx, HistoryQuery{ItemID: strings.TrimSpace(in.ItemID), Source: strings.TrimSpace(in.Source),
					Since: since, Limit: in.Limit, Snapshots: in.Snapshots})
				if err != nil {
					return historyOutput{}, err
				}
				return historyOutput{Count: len(versions), Versions: versions}, nil
			},
		},
		&tool.Typed[rankInput, rankOutput]{
			NameStr:        "feed_rank",
			TitleStr:       "Rank Feed Cards",
			DescriptionStr: "Set Feed tab ranking: order (ids best first) or scores (id to score, higher first). Cards Zach has seen still sink below unseen ones (those he scrolled past without engaging furthest), and sources are interleaved. Each change is a rank version. CLI: pdw feed rank.",
			Handle: func(ctx context.Context, in rankInput) (rankOutput, error) {
				scores := map[string]float64{}
				for i, id := range in.Order {
					scores[id] = float64(len(in.Order) - i)
				}
				for id, score := range in.Scores {
					scores[id] = score
				}
				if len(scores) == 0 {
					return rankOutput{}, &tool.InvalidInputError{Message: "pass order or scores"}
				}
				if err := store.SetScores(ctx, scores, provenance(ctx, in.Agent, in.Reason)); err != nil {
					return rankOutput{}, toolError(err)
				}
				return rankOutput{Ranked: len(scores)}, nil
			},
		},
		&tool.Typed[listInput, listOutput]{
			NameStr:        "feed_list",
			TitleStr:       "List Feed Cards",
			DescriptionStr: "List Feed tab cards with each card's engagement (impressions, dwell_ms, opens, read_ms, dismissed, engaged, engagement_score) and its links. view=feed returns exactly what the phone shows, ranked; view=saved is his read-later queue. CLI: pdw feed list.",
			Handle: func(ctx context.Context, in listInput) (listOutput, error) {
				if in.Limit > 500 {
					return listOutput{}, &tool.InvalidInputError{Message: "limit is at most 500"}
				}
				var (
					items []Item
					err   error
				)
				switch in.View {
				case "", "recent":
					items, err = store.List(ctx, ListOptions{Source: in.Source, ItemIDs: in.ItemIDs,
						IncludeRemoved: in.IncludeRemoved, IncludeDismissed: in.IncludeDismissed, Limit: in.Limit})
				case "feed":
					items, err = store.Feed(ctx, in.Limit)
				case "saved":
					items, err = store.Saved(ctx, in.Limit)
				default:
					return listOutput{}, &tool.InvalidInputError{Message: "view must be recent, feed or saved"}
				}
				if err != nil {
					return listOutput{}, err
				}
				return listOutput{Count: len(items), Items: items}, nil
			},
		},
		&tool.Typed[saveInput, saveOutput]{
			NameStr:  "feed_save",
			TitleStr: "Save Feed Cards for Later",
			DescriptionStr: "Put Feed tab cards in Zach's read-later queue (the Saved view on the phone, where he bookmarks what he must read or " +
				"watch later), or take them out with unsave. Only when he asks (\"save that for me\"): the queue is his. It records the same " +
				"save/unsave events his taps do. A saved card stays in the queue after it expires or is removed. List it with feed_list " +
				"view=saved. CLI: pdw feed save (pdw feed unsave).",
			Handle: func(ctx context.Context, in saveInput) (saveOutput, error) {
				ids := in.ItemIDs
				if strings.TrimSpace(in.ItemID) != "" {
					ids = append(ids, strings.TrimSpace(in.ItemID))
				}
				n, err := store.SetSaved(ctx, ids, !in.Unsave, provenance(ctx, in.Agent, in.Reason))
				if errors.Is(err, ErrNotFound) {
					return saveOutput{}, &tool.InvalidInputError{Message: err.Error() + " (pdw feed list --all shows ids)"}
				}
				if in.Unsave {
					return saveOutput{Unsaved: n}, toolError(err)
				}
				return saveOutput{Saved: n}, toolError(err)
			},
		},
		&tool.Typed[statsInput, Stats]{
			NameStr:  "feed_stats",
			TitleStr: "Feed Engagement Stats",
			DescriptionStr: "How the Feed tab is landing: per source (items, seen, engaged/open/dismiss rates, average engagement_score, dwell and read time, from " +
				"marts_feed.source_engagement), event counts by type and sessions since a window (default 7d), the most engaged cards, " +
				"and cards seen twice or more without any engagement. Use it before writing the next batch. CLI: pdw feed stats.",
			Handle: func(ctx context.Context, in statsInput) (Stats, error) {
				since, err := sinceTime(in.Since, store.now(), 7*24*time.Hour)
				if err != nil {
					return Stats{}, toolError(err)
				}
				return store.Stats(ctx, strings.TrimSpace(in.Source), since)
			},
		},
		&tool.Typed[eventsInput, eventsOutput]{
			NameStr:        "feed_events",
			TitleStr:       "Feed Engagement Events",
			DescriptionStr: "Raw Feed tab engagement events, newest first: impressions with dwell_ms and visible_fraction, opens, close (read time), expand, dismiss/undismiss with reason, share (swiped to email it), save/unsave (read later), sessions. Filter by item_id, source, types, since. CLI: pdw feed events.",
			Handle: func(ctx context.Context, in eventsInput) (eventsOutput, error) {
				if in.Limit > 1000 {
					return eventsOutput{}, &tool.InvalidInputError{Message: "limit is at most 1000"}
				}
				for _, kind := range in.Types {
					if !validEventType(kind) {
						return eventsOutput{}, &tool.InvalidInputError{Message: fmt.Sprintf("unknown event type %q; valid: %s", kind, strings.Join(EventTypes, ", "))}
					}
				}
				since, err := sinceTime(in.Since, store.now(), 0)
				if err != nil {
					return eventsOutput{}, toolError(err)
				}
				events, err := store.Events(ctx, EventQuery{ItemID: in.ItemID, Source: in.Source, Types: in.Types, Since: since, Limit: in.Limit})
				if err != nil {
					return eventsOutput{}, err
				}
				return eventsOutput{Count: len(events), Events: events}, nil
			},
		},
	}
}
