package x

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Stream is one list the uploader reads: which query, which relation it
// records, and the query's variables for a page.
type Stream struct {
	Name      string
	Relation  string
	Operation string
	Variables func(userID, cursor string) map[string]any
	// Page is the x.com page whose client makes this call, sent as Referer.
	Page func(handle string) string
	// Fallback is the query used for the rest of a run when Operation
	// answers 404 (x.com gating it), with its own variables and page.
	Fallback *Stream
}

// pageSize is the posts asked for per request, what the web client asks.
const pageSize = 40

func withCursor(vars map[string]any, cursor string) map[string]any {
	if cursor != "" {
		vars["cursor"] = cursor
	}
	return vars
}

// Streams are every list read, in order. His own posts first: they carry the
// C11 observation, and a run cut short by a rate limit should have read them.
var Streams = []Stream{
	{
		Name: "posts", Relation: "authored", Operation: "UserTweetsAndReplies",
		Page: func(h string) string { return "/" + h + "/with_replies" },
		Variables: func(userID, cursor string) map[string]any {
			return withCursor(map[string]any{
				"userId": userID, "count": pageSize, "includePromotedContent": false,
				"withCommunity": true, "withVoice": true,
			}, cursor)
		},
		// UserTweetsAndReplies requires x-client-transaction-id (generated
		// from /home's verification key, SVG frames and ondemand.s bundle); if
		// X changes that scheme and answers 404, fall back to UserTweets (his
		// posts, reposts and quotes) for the rest of the run so non-reply posts
		// keep landing.
		Fallback: &Stream{
			Name: "posts", Relation: "authored", Operation: "UserTweets",
			Page: func(h string) string { return "/" + h },
			Variables: func(userID, cursor string) map[string]any {
				return withCursor(map[string]any{
					"userId": userID, "count": pageSize, "includePromotedContent": false,
					"withQuickPromoteEligibilityTweetFields": true, "withVoice": true,
				}, cursor)
			},
		},
	},
	{
		Name: "likes", Relation: "liked", Operation: "Likes",
		Page: func(h string) string { return "/" + h + "/likes" },
		Variables: func(userID, cursor string) map[string]any {
			return withCursor(map[string]any{
				"userId": userID, "count": pageSize, "includePromotedContent": false,
				"withClientEventToken": false, "withBirdwatchNotes": false, "withVoice": true,
			}, cursor)
		},
	},
	{
		Name: "bookmarks", Relation: "bookmarked", Operation: "Bookmarks",
		Page: func(string) string { return "/i/bookmarks" },
		Variables: func(_ string, cursor string) map[string]any {
			return withCursor(map[string]any{"count": pageSize, "includePromotedContent": false}, cursor)
		},
	},
	{
		Name: "mentions", Relation: "mentioned", Operation: "NotificationsTimeline",
		Page: func(string) string { return "/notifications/mentions" },
		Variables: func(_ string, cursor string) map[string]any {
			return withCursor(map[string]any{"timeline_type": "Mentions", "count": pageSize}, cursor)
		},
	},
}

// StreamByName finds a stream.
func StreamByName(name string) (Stream, bool) {
	for _, s := range Streams {
		if s.Name == name {
			return s, true
		}
	}
	return Stream{}, false
}

// API is what the runner needs from the x.com client.
type API interface {
	Discover() error
	Query(operation string, variables map[string]any) (map[string]any, error)
	RemainingRequests() int
}

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadXBatch(gzipBytes []byte, exportedAt string) (ingestclient.StoredObject, error)
}

// Defaults for one run's page budget per stream. The newest pages catch up
// every run; the backfill walks older history a few pages a run so a first
// sync of years of likes never bursts into X's rate limits.
const (
	DefaultIncrementalPages = 3
	DefaultBackfillPages    = 5
	DefaultRequestDelay     = 3 * time.Second
)

// Summary is the run's counts.
type Summary struct {
	Posts       int
	Relations   int
	Batches     int
	Pages       int
	RateLimited []string
	Failed      []string
	Deferred    bool
	// Skipped sat this run out by chance; CoolingDown are still waiting out
	// an earlier rate limit.
	Skipped     []string
	CoolingDown []string
}

// ErrStreamsFailed is returned (wrapped) when a stream failed; the others ran.
var ErrStreamsFailed = errors.New("some X streams failed")

// Runner reads every stream and uploads what it read.
type Runner struct {
	// Account is the handle, lowercased; UserID is its numeric id.
	Account string
	UserID  string
	API     API
	Client  Uploader
	State   *State
	Logger  common.Logger
	Streams []Stream
	Mode    string
	// Pages bounds the newest-first catch-up per stream; BackfillPages the
	// walk into older history per stream.
	Pages         int
	BackfillPages int
	Delay         time.Duration
	// Pace randomizes gaps, stream order, skips, page budgets and cooldowns;
	// nil keeps every run identical (a fixed Delay, the listed order).
	Pace              *Pacer
	Sleep             func(time.Duration)
	Now               func() time.Time
	BeforeUploadCheck func() string
	// Observe posts the newest post of his own x.com shows (contract C11).
	Observe ingestclient.ObservationPoster

	requests int
	// fellBack names the streams on their Fallback query for this run.
	fellBack  map[string]bool
	guarded   bool
	summary   Summary
	exhausted bool
}

// Sync runs one pass over every stream.
func (r *Runner) Sync() (Summary, error) {
	if r.API == nil || r.Client == nil {
		return Summary{}, errors.New("x.com client and ingest client are required")
	}
	if r.Mode == "" {
		r.Mode = "incremental"
	}
	if r.Mode != "incremental" && r.Mode != "full" {
		return Summary{}, errors.New("mode must be 'full' or 'incremental'")
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
	if r.Sleep == nil {
		r.Sleep = time.Sleep
	}
	if r.Pages <= 0 {
		r.Pages = DefaultIncrementalPages
	}
	if r.BackfillPages < 0 {
		r.BackfillPages = 0
	}
	streams := r.Streams
	if streams == nil {
		streams = Streams
	}
	if r.Mode == "full" && r.State != nil {
		for _, stream := range streams {
			if err := r.State.ResetBackfill(stream.Name); err != nil {
				return r.summary, err
			}
		}
	}
	skip := map[string]bool{}
	if r.Pace != nil {
		streams = r.Pace.Order(streams)
		skip = r.Pace.Skip(streams)
	}
	for _, stream := range streams {
		if r.summary.Deferred || r.exhausted {
			break
		}
		if skip[stream.Name] {
			r.Logger.Infof("X %s: sitting this run out", stream.Name)
			r.summary.Skipped = append(r.summary.Skipped, stream.Name)
			continue
		}
		if r.State != nil {
			until, _, err := r.State.Cooldown(stream.Name)
			if err != nil {
				return r.summary, err
			}
			if until.After(r.Now()) {
				r.Logger.Infof("X %s: cooling down after a rate limit until %s", stream.Name, until.Format(time.RFC3339))
				r.summary.CoolingDown = append(r.summary.CoolingDown, stream.Name)
				continue
			}
		}
		err := r.syncStream(stream)
		var auth *AuthError
		if errors.As(err, &auth) {
			if r.State != nil && r.Pace != nil {
				until := r.Pace.ChallengeCooldown(r.Now())
				if cerr := r.State.SetCooldown(RunScope, until, "session"); cerr != nil {
					return r.summary, cerr
				}
				r.Logger.Warningf("X: no request to x.com until %s", until.Format(time.RFC3339))
			}
			return r.summary, err
		}
		var limit *RateLimitError
		if errors.As(err, &limit) {
			r.Logger.Warningf("X %s: %v; resuming next run", stream.Name, limit)
			r.summary.RateLimited = append(r.summary.RateLimited, stream.Name)
			if err := r.coolDown(stream.Name, limit.Reset); err != nil {
				return r.summary, err
			}
			continue
		}
		if err != nil {
			r.Logger.Warningf("X %s failed: %v", stream.Name, err)
			r.summary.Failed = append(r.summary.Failed, fmt.Sprintf("%s: %v", stream.Name, err))
		}
	}
	if len(r.summary.Failed) > 0 {
		return r.summary, fmt.Errorf("%w: %s", ErrStreamsFailed, strings.Join(r.summary.Failed, "; "))
	}
	return r.summary, nil
}

// coolDown holds a scope off x.com for a randomized while after a rate limit.
func (r *Runner) coolDown(scope string, reset time.Time) error {
	if r.State == nil || r.Pace == nil {
		return nil
	}
	until := r.Pace.RateLimitCooldown(reset, r.Now())
	r.Logger.Warningf("X %s: next request no sooner than %s", scope, until.Format(time.RFC3339))
	return r.State.SetCooldown(scope, until, "rate_limit")
}

// pages is this run's page budget for one walk.
func (r *Runner) pages(n int) int {
	if r.Pace == nil {
		return n
	}
	return r.Pace.Pages(n)
}

func (r *Runner) syncStream(stream Stream) error {
	// Gaps an earlier run left open: it stopped (page budget, rate limit,
	// error) while its pages still held items it had never shipped.
	var pending []string
	if r.State != nil {
		var err error
		if pending, err = r.State.Catchups(stream.Name); err != nil {
			return err
		}
	}
	// Newest first, until a page holds nothing this stream has not seen.
	top, err := r.catchUp(stream, "")
	if err != nil || r.summary.Deferred || r.exhausted {
		return err
	}
	if r.State == nil {
		return nil
	}
	backfill, err := r.State.Backfill(stream.Name)
	if err != nil {
		return err
	}
	if !backfill.Started && top.open != "" {
		// A first run's open gap is simply the history: the backfill below
		// starts there, so the catch-up does not walk it twice.
		if err := r.State.RemoveCatchup(stream.Name, top.open); err != nil {
			return err
		}
	}
	// Then each earlier gap, from where it stopped, until it meets seen items.
	for _, cursor := range pending {
		if _, err := r.catchUp(stream, cursor); err != nil || r.summary.Deferred || r.exhausted {
			return err
		}
	}
	if r.BackfillPages == 0 || backfill.Done {
		return nil
	}
	// Then a few pages further into history, from where the last run stopped.
	if !backfill.Started {
		// The first backfill starts where this run's catch-up stopped, so it
		// never re-reads the newest pages.
		if top.reachedEnd {
			return r.State.SetBackfill(stream.Name, "", true, r.Now())
		}
		backfill.Cursor = top.next
	}
	cursor := backfill.Cursor
	for page, budget := 0, r.pages(r.BackfillPages); page < budget; page++ {
		read, err := r.readPage(stream, cursor, false)
		if err != nil || r.summary.Deferred {
			return err
		}
		next := read.next
		done := read.items == 0 || next == "" || next == cursor
		if err := r.State.SetBackfill(stream.Name, next, done, r.Now()); err != nil {
			return err
		}
		if done {
			r.Logger.Infof("X %s: history backfill complete", stream.Name)
			return nil
		}
		if r.exhausted {
			return nil
		}
		cursor = next
	}
	return nil
}

func (r *Runner) query(stream Stream, cursor string) (map[string]any, error) {
	if setter, ok := r.API.(interface{ SetReferer(string) }); ok && stream.Page != nil {
		// The page a person would be on when the web client makes this call.
		setter.SetReferer(stream.Page(r.Account))
	}
	return r.API.Query(stream.Operation, stream.Variables(r.UserID, cursor))
}

// pageRead is what one page held: its entries (readable or not), how many
// of its relations this stream had not seen before, and the next cursor.
type pageRead struct {
	items int
	fresh int
	next  string
}

// walk is where a catch-up stopped: the next cursor, whether the list ended,
// and the gap it left open (a cursor saved in state), if any.
type walk struct {
	next       string
	reachedEnd bool
	open       string
}

// catchUp reads newest-first from start ("" is the top) for up to r.Pages
// pages, until a page holds nothing new. While pages keep holding new items
// the position is saved as an open gap after every shipped page, so a run
// that stops early (the page budget, a rate limit, an error, the warehouse
// refusing the upload) is resumed by the next run instead of leaving the
// items between here and what was already shipped unread for good.
func (r *Runner) catchUp(stream Stream, start string) (walk, error) {
	w := walk{next: start, open: start}
	for page, budget := 0, r.pages(r.Pages); page < budget; page++ {
		read, err := r.readPage(stream, w.next, start == "" && page == 0)
		if err != nil || r.summary.Deferred {
			return w, err
		}
		w.next = read.next
		if read.items == 0 || read.next == "" || read.fresh == 0 {
			w.reachedEnd = read.items == 0 || read.next == ""
			return w, r.closeGap(stream, &w)
		}
		if err := r.moveGap(stream, &w, read.next); err != nil {
			return w, err
		}
		if r.exhausted {
			return w, nil
		}
	}
	return w, nil
}

func (r *Runner) moveGap(stream Stream, w *walk, next string) error {
	if r.State == nil {
		return nil
	}
	if err := r.State.ReplaceCatchup(stream.Name, w.open, next, r.Now()); err != nil {
		return err
	}
	w.open = next
	return nil
}

func (r *Runner) closeGap(stream Stream, w *walk) error {
	if r.State == nil || w.open == "" {
		return nil
	}
	err := r.State.RemoveCatchup(stream.Name, w.open)
	w.open = ""
	return err
}

// readPage fetches, uploads and records one page.
func (r *Runner) readPage(stream Stream, cursor string, first bool) (pageRead, error) {
	if r.requests > 0 {
		gap := r.Delay
		if r.Pace != nil {
			gap = r.Pace.Gap()
		}
		if gap > 0 {
			r.Sleep(gap)
		}
	}
	r.requests++
	query := stream
	if r.fellBack[stream.Name] && stream.Fallback != nil {
		query = *stream.Fallback
	}
	data, err := r.query(query, cursor)
	if err != nil && stream.Fallback != nil && !r.fellBack[stream.Name] && strings.Contains(err.Error(), "HTTP 404") {
		r.Logger.Warningf("X %s: %s answered 404; reading %s instead for this run", stream.Name, stream.Operation, stream.Fallback.Operation)
		if r.fellBack == nil {
			r.fellBack = map[string]bool{}
		}
		r.fellBack[stream.Name] = true
		query = *stream.Fallback
		if r.Pace != nil {
			r.Sleep(r.Pace.Gap())
		} else if r.Delay > 0 {
			r.Sleep(r.Delay)
		}
		data, err = r.query(query, cursor)
	}
	if err != nil {
		return pageRead{}, err
	}
	stream.Operation = query.Operation
	if remaining := r.API.RemainingRequests(); remaining == 0 {
		r.Logger.Warningf("X %s: rate limit exhausted after this page; resuming next run", stream.Name)
		r.summary.RateLimited = append(r.summary.RateLimited, stream.Name)
		r.exhausted = true
		if err := r.coolDown(stream.Name, time.Time{}); err != nil {
			return pageRead{}, err
		}
	}
	r.summary.Pages++
	page := ParsePage(data)
	now := r.Now()
	posts := map[string]Post{}
	var order []string
	keep := func(p Post) {
		if _, ok := posts[p.PostID]; !ok {
			order = append(order, p.PostID)
		}
		posts[p.PostID] = p
	}
	type relation struct {
		postID    string
		sortIndex int64
	}
	var relations []relation
	for _, entry := range page.Entries {
		for _, post := range entry.Primary {
			keep(post)
			if r.relates(stream, post) {
				relations = append(relations, relation{post.PostID, entry.SortIndex})
			}
		}
		for _, post := range entry.Context {
			keep(post)
		}
	}
	if stream.Name == "posts" && first {
		r.observeNewest(page)
	}
	read := pageRead{items: page.Items, next: page.BottomCursor}
	r.Logger.Infof("X %s: read a page via %s: %d entries, %d posts, more=%t",
		stream.Name, stream.Operation, page.Items, len(posts), page.BottomCursor != "")
	if len(posts) == 0 {
		return read, nil
	}
	if !r.guarded && r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			r.Logger.Warningf("Skipping X upload: %s", reason)
			r.summary.Deferred = true
			return pageRead{}, nil
		}
	}
	r.guarded = true
	fresh := 0
	records := make([]map[string]any, 0, len(posts)+len(relations))
	for _, id := range order {
		records = append(records, Envelope(r.Account, now, "post", PostPayload(posts[id], r.UserID, r.Account)))
	}
	type seenRow struct {
		postID       string
		discoveredAt time.Time
	}
	var newlySeen []seenRow
	for _, rel := range relations {
		discovered := now
		if r.State != nil {
			at, ok, err := r.State.Discovered(stream.Name, rel.postID)
			if err != nil {
				return pageRead{}, err
			}
			if ok {
				discovered = at
			} else {
				fresh++
				newlySeen = append(newlySeen, seenRow{rel.postID, now})
			}
		} else {
			fresh++
		}
		records = append(records, Envelope(r.Account, now, "relation", map[string]any{
			"post_id":       rel.postID,
			"relation":      stream.Relation,
			"discovered_at": common.ISOFormat(discovered),
			"sort_index":    rel.sortIndex,
		}))
	}
	body, err := common.GzipJSONL(records)
	if err != nil {
		return pageRead{}, err
	}
	stored, err := r.Client.UploadXBatch(body, common.ISOFormat(now))
	if err != nil {
		return pageRead{}, err
	}
	r.summary.Batches++
	r.summary.Posts += len(posts)
	r.summary.Relations += len(relations)
	r.Logger.Infof("Uploaded X batch %s: %s page with %d posts, %d %s (%d new)",
		stored.StorageKey, stream.Name, len(posts), len(relations), stream.Relation, fresh)
	if r.State != nil {
		for _, row := range newlySeen {
			if err := r.State.MarkDiscovered(stream.Name, row.postID, row.discoveredAt); err != nil {
				return pageRead{}, err
			}
		}
	}
	read.fresh = fresh
	return read, nil
}

// relates says whether a primary post on this stream's page carries its
// relation: his own posts on his timeline (the page also shows the posts he
// replied to), everything on his likes and bookmarks, and other people's
// posts on his mentions.
func (r *Runner) relates(stream Stream, post Post) bool {
	mine := post.AuthorUserID == r.UserID || strings.EqualFold(post.AuthorScreenName, r.Account)
	switch stream.Relation {
	case "authored":
		return mine
	case "mentioned":
		return !mine
	default:
		return true
	}
}

// observeNewest posts the newest post of his own on the first page of his
// timeline (contract C11). A pinned post is older than his newest, so taking
// the maximum is right whether or not one is pinned.
func (r *Runner) observeNewest(page Page) {
	var newest Post
	for _, entry := range page.Entries {
		for _, post := range entry.Primary {
			if r.relates(Stream{Relation: "authored"}, post) && post.CreatedAt.After(newest.CreatedAt) {
				newest = post
			}
		}
	}
	ingestclient.Observe(r.Observe, r.Logger, "x", r.Account, newest.CreatedAt, newest.PostID,
		"x.com UserTweetsAndReplies: the newest post of his own on the first page", r.Now())
}

// PostPayload is the record shipped for one post.
func PostPayload(post Post, accountUserID, account string) map[string]any {
	raw := post.Raw
	if raw == nil {
		raw = map[string]any{}
	}
	return map[string]any{
		"post_id":                 post.PostID,
		"author_user_id":          post.AuthorUserID,
		"author_screen_name":      post.AuthorScreenName,
		"author_name":             post.AuthorName,
		"created_at":              common.ISOFormat(post.CreatedAt),
		"text":                    post.Text,
		"lang":                    post.Lang,
		"conversation_id":         post.ConversationID,
		"in_reply_to_post_id":     post.InReplyToPostID,
		"in_reply_to_user_id":     post.InReplyToUserID,
		"in_reply_to_screen_name": post.InReplyToScreenName,
		"quoted_post_id":          post.QuotedPostID,
		"retweeted_post_id":       post.RetweetedPostID,
		"is_retweet":              post.IsRetweet,
		"is_quote":                post.IsQuote,
		"mentions_account":        post.Mentions(accountUserID, account),
		"reply_count":             post.ReplyCount,
		"retweet_count":           post.RetweetCount,
		"like_count":              post.LikeCount,
		"quote_count":             post.QuoteCount,
		"bookmark_count":          post.BookmarkCount,
		"view_count":              post.ViewCount,
		"url":                     post.URL(),
		"raw":                     raw,
	}
}

// Envelope wraps one record for the batch.
func Envelope(account string, exportedAt time.Time, recordType string, record map[string]any) map[string]any {
	return map[string]any{
		"schema_version": int64(1),
		"source":         "x",
		"account":        account,
		"exported_at":    common.ISOFormat(exportedAt),
		"record_type":    recordType,
		"record":         record,
	}
}
