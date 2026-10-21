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

type pushInput struct {
	Source string     `json:"source,omitempty" jsonschema:"default source for every item that does not name its own"`
	Items  []PushItem `json:"items" jsonschema:"cards to add or update (at most 500); each is idempotent on (source, key)"`
}

type pushOutput struct {
	Created int          `json:"created"`
	Updated int          `json:"updated"`
	Items   []PushResult `json:"items"`
}

type updateOutput struct {
	ItemID string `json:"item_id"`
	Status string `json:"status"`
}

type removeInput struct {
	ItemIDs []string `json:"item_ids,omitempty" jsonschema:"cards to hide"`
	Source  string   `json:"source,omitempty" jsonschema:"hide every live card of this source"`
	Reason  string   `json:"reason,omitempty"`
}

type removeOutput struct {
	Removed int64 `json:"removed"`
}

type rankInput struct {
	Order  []string           `json:"order,omitempty" jsonschema:"item ids best first; they get scores len(order) down to 1"`
	Scores map[string]float64 `json:"scores,omitempty" jsonschema:"item id to score; higher shows first"`
}

type rankOutput struct {
	Ranked int `json:"ranked"`
}

type listInput struct {
	Source           string   `json:"source,omitempty"`
	ItemIDs          []string `json:"item_ids,omitempty"`
	View             string   `json:"view,omitempty" jsonschema:"recent (default, newest pushed first) or feed (exactly what the phone shows, ranked)"`
	IncludeRemoved   bool     `json:"include_removed,omitempty"`
	IncludeDismissed bool     `json:"include_dismissed,omitempty"`
	Limit            int      `json:"limit,omitempty" jsonschema:"default 50, at most 500"`
}

type listOutput struct {
	Count int    `json:"count"`
	Items []Item `json:"items"`
}

type statsInput struct {
	Source string `json:"source,omitempty"`
	Since  string `json:"since,omitempty" jsonschema:"window for events_by_type and sessions: RFC3339, YYYY-MM-DD, or a duration back from now (7d, 24h); default 7d"`
}

type eventsInput struct {
	ItemID string   `json:"item_id,omitempty"`
	Source string   `json:"source,omitempty"`
	Types  []string `json:"types,omitempty" jsonschema:"event types: session_start, session_end, impression, open, close, expand, dismiss, undismiss"`
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
				"on one card, [{url, title}], e.g. the paper, the repo and the discussion), body (markdown), " +
				"https image_url, author, site, tags, score (higher shows first), published_at, expires_at (RFC3339 or 48h/7d). " +
				"Every impression, dwell, open (per link), read time and dismissal is recorded; there is no like button, interest is " +
				"measured (engagement_score): read it back with feed_stats. CLI: pdw feed push.",
			Handle: func(ctx context.Context, in pushInput) (pushOutput, error) {
				if len(in.Items) == 0 {
					return pushOutput{}, &tool.InvalidInputError{Message: "items is empty: pass at least one card"}
				}
				if len(in.Items) > MaxPushItems {
					return pushOutput{}, &tool.InvalidInputError{Message: fmt.Sprintf("at most %d items per push", MaxPushItems)}
				}
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
				results, err := store.Push(ctx, batch, pdwauth.ClientNameFromContext(ctx))
				if err != nil {
					return pushOutput{}, toolError(err)
				}
				out := pushOutput{Items: results}
				for _, r := range results {
					if r.Status == "created" {
						out.Created++
					} else {
						out.Updated++
					}
				}
				return out, nil
			},
		},
		&tool.Typed[Update, updateOutput]{
			NameStr:        "feed_update",
			TitleStr:       "Edit a Feed Card",
			DescriptionStr: "Change fields of one Feed tab card by item_id; fields left out are kept. CLI: pdw feed update.",
			Handle: func(ctx context.Context, in Update) (updateOutput, error) {
				if strings.TrimSpace(in.ItemID) == "" {
					return updateOutput{}, &tool.InvalidInputError{Message: "item_id is required"}
				}
				if err := store.Update(ctx, in); err != nil {
					if errors.Is(err, ErrNotFound) {
						return updateOutput{}, &tool.InvalidInputError{Message: fmt.Sprintf("no feed item %q (pdw feed list shows ids)", in.ItemID)}
					}
					return updateOutput{}, toolError(err)
				}
				return updateOutput{ItemID: in.ItemID, Status: "updated"}, nil
			},
		},
		&tool.Typed[removeInput, removeOutput]{
			NameStr:        "feed_remove",
			TitleStr:       "Remove Feed Cards",
			DescriptionStr: "Hide Feed tab cards by item_ids, or every card of a source. Engagement already recorded is kept; re-pushing a card brings it back. CLI: pdw feed remove.",
			Handle: func(ctx context.Context, in removeInput) (removeOutput, error) {
				if len(in.ItemIDs) == 0 && strings.TrimSpace(in.Source) == "" {
					return removeOutput{}, &tool.InvalidInputError{Message: "pass item_ids or a source"}
				}
				n, err := store.Remove(ctx, in.ItemIDs, strings.TrimSpace(in.Source), in.Reason)
				return removeOutput{Removed: n}, toolError(err)
			},
		},
		&tool.Typed[rankInput, rankOutput]{
			NameStr:        "feed_rank",
			TitleStr:       "Rank Feed Cards",
			DescriptionStr: "Set Feed tab ranking: order (ids best first) or scores (id to score, higher first). Cards Zach has seen still sink below unseen ones (those he scrolled past without engaging furthest), and sources are interleaved. CLI: pdw feed rank.",
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
				if err := store.SetScores(ctx, scores); err != nil {
					return rankOutput{}, toolError(err)
				}
				return rankOutput{Ranked: len(scores)}, nil
			},
		},
		&tool.Typed[listInput, listOutput]{
			NameStr:        "feed_list",
			TitleStr:       "List Feed Cards",
			DescriptionStr: "List Feed tab cards with each card's engagement (impressions, dwell_ms, opens, read_ms, dismissed, engaged, engagement_score) and its links. view=feed returns exactly what the phone shows, ranked. CLI: pdw feed list.",
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
				default:
					return listOutput{}, &tool.InvalidInputError{Message: "view must be recent or feed"}
				}
				if err != nil {
					return listOutput{}, err
				}
				return listOutput{Count: len(items), Items: items}, nil
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
			DescriptionStr: "Raw Feed tab engagement events, newest first: impressions with dwell_ms and visible_fraction, opens, close (read time), expand, dismiss/undismiss with reason, sessions. Filter by item_id, source, types, since. CLI: pdw feed events.",
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
