package x

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Post is one tweet, normalized for base_x.posts. Raw is the GraphQL result
// verbatim (the faithful copy); every other field is read from it.
type Post struct {
	PostID               string
	AuthorUserID         string
	AuthorScreenName     string
	AuthorName           string
	CreatedAt            time.Time
	Text                 string
	Lang                 string
	ConversationID       string
	InReplyToPostID      string
	InReplyToUserID      string
	InReplyToScreenName  string
	QuotedPostID         string
	RetweetedPostID      string
	IsRetweet            bool
	IsQuote              bool
	MentionedUserIDs     []string
	MentionedScreenNames []string
	ReplyCount           int64
	RetweetCount         int64
	LikeCount            int64
	QuoteCount           int64
	BookmarkCount        int64
	ViewCount            int64
	Raw                  map[string]any
}

// URL is the post's canonical address.
func (p Post) URL() string {
	if p.AuthorScreenName == "" {
		return "https://x.com/i/status/" + p.PostID
	}
	return "https://x.com/" + p.AuthorScreenName + "/status/" + p.PostID
}

// Mentions reports whether the post @-mentions the account (by id or handle).
func (p Post) Mentions(userID, screenName string) bool {
	for _, id := range p.MentionedUserIDs {
		if userID != "" && id == userID {
			return true
		}
	}
	for _, name := range p.MentionedScreenNames {
		if screenName != "" && strings.EqualFold(name, screenName) {
			return true
		}
	}
	return false
}

// Entry is one timeline entry: the posts it shows as itself (Primary: an
// item's tweet, or each tweet of a conversation module) and every post that
// rides along (Context: what a primary post quotes or reposts).
type Entry struct {
	EntryID   string
	SortIndex int64
	Primary   []Post
	Context   []Post
}

// Page is one timeline response.
type Page struct {
	Entries      []Entry
	BottomCursor string
}

// PostCount is how many primary posts the page holds.
func (p Page) PostCount() int {
	n := 0
	for _, e := range p.Entries {
		n += len(e.Primary)
	}
	return n
}

// ParsePage reads a timeline response's `data`. The instructions sit at a
// different depth per query (data.user.result.timeline.timeline for a
// profile, data.bookmark_timeline_v2.timeline for bookmarks,
// data.viewer_v2.user_results.result.notification_timeline.timeline for
// mentions), so they are found by name rather than by path.
func ParsePage(data map[string]any) Page {
	var page Page
	for _, instruction := range findInstructions(data) {
		inst, _ := instruction.(map[string]any)
		switch str(inst["type"]) {
		case "TimelineAddEntries":
			for _, raw := range list(inst["entries"]) {
				page.add(asMap(raw))
			}
		case "TimelinePinEntry":
			page.add(asMap(inst["entry"]))
		case "TimelineReplaceEntry":
			// Later pages of some timelines deliver their cursors this way.
			page.add(asMap(inst["entry"]))
		}
	}
	return page
}

func (p *Page) add(entry map[string]any) {
	if entry == nil {
		return
	}
	content := asMap(entry["content"])
	if cursorType := str(content["cursorType"]); cursorType != "" ||
		str(content["entryType"]) == "TimelineTimelineCursor" {
		if cursorType == "Bottom" {
			p.BottomCursor = str(content["value"])
		}
		return
	}
	e := Entry{EntryID: str(entry["entryId"]), SortIndex: int64Of(entry["sortIndex"])}
	var results []map[string]any
	if item := asMap(content["itemContent"]); item != nil {
		results = append(results, tweetResult(item))
	}
	for _, raw := range list(content["items"]) {
		item := asMap(asMap(asMap(raw)["item"])["itemContent"])
		results = append(results, tweetResult(item))
	}
	for _, result := range results {
		post, ok := NormalizeTweet(result)
		if !ok {
			continue
		}
		e.Primary = append(e.Primary, post)
		e.Context = append(e.Context, contextPosts(result)...)
	}
	if len(e.Primary) > 0 {
		p.Entries = append(p.Entries, e)
	}
}

func tweetResult(itemContent map[string]any) map[string]any {
	if itemContent == nil {
		return nil
	}
	return asMap(asMap(itemContent["tweet_results"])["result"])
}

// unwrap returns the Tweet inside a visibility wrapper, or nil for a
// tombstone / unavailable post.
func unwrap(result map[string]any) map[string]any {
	switch str(result["__typename"]) {
	case "TweetWithVisibilityResults":
		return asMap(result["tweet"])
	case "Tweet", "":
		if asMap(result["legacy"]) == nil {
			return nil
		}
		return result
	default:
		return nil
	}
}

func contextPosts(result map[string]any) []Post {
	tweet := unwrap(result)
	if tweet == nil {
		return nil
	}
	legacy := asMap(tweet["legacy"])
	var out []Post
	for _, nested := range []map[string]any{
		asMap(asMap(legacy["retweeted_status_result"])["result"]),
		asMap(asMap(tweet["quoted_status_result"])["result"]),
	} {
		if post, ok := NormalizeTweet(nested); ok {
			out = append(out, post)
			out = append(out, contextPosts(nested)...)
		}
	}
	return out
}

// xTimeLayout is the API's created_at, e.g. "Wed May 03 18:52:37 +0000 2017".
const xTimeLayout = "Mon Jan 02 15:04:05 -0700 2006"

// NormalizeTweet reads one GraphQL tweet result; ok is false for anything
// that is not a readable post (a tombstone, a withheld post, an ad).
func NormalizeTweet(result map[string]any) (Post, bool) {
	tweet := unwrap(result)
	if tweet == nil {
		return Post{}, false
	}
	legacy := asMap(tweet["legacy"])
	id := firstNonEmpty(str(tweet["rest_id"]), str(legacy["id_str"]))
	if id == "" {
		return Post{}, false
	}
	user := asMap(asMap(asMap(tweet["core"])["user_results"])["result"])
	userCore := asMap(user["core"])
	userLegacy := asMap(user["legacy"])
	post := Post{
		PostID:              id,
		AuthorUserID:        firstNonEmpty(str(user["rest_id"]), str(legacy["user_id_str"])),
		AuthorScreenName:    firstNonEmpty(str(userCore["screen_name"]), str(userLegacy["screen_name"])),
		AuthorName:          firstNonEmpty(str(userCore["name"]), str(userLegacy["name"])),
		Lang:                str(legacy["lang"]),
		ConversationID:      firstNonEmpty(str(legacy["conversation_id_str"]), id),
		InReplyToPostID:     str(legacy["in_reply_to_status_id_str"]),
		InReplyToUserID:     str(legacy["in_reply_to_user_id_str"]),
		InReplyToScreenName: str(legacy["in_reply_to_screen_name"]),
		QuotedPostID:        str(legacy["quoted_status_id_str"]),
		IsQuote:             legacy["is_quote_status"] == true,
		ReplyCount:          int64Of(legacy["reply_count"]),
		RetweetCount:        int64Of(legacy["retweet_count"]),
		LikeCount:           int64Of(legacy["favorite_count"]),
		QuoteCount:          int64Of(legacy["quote_count"]),
		BookmarkCount:       int64Of(legacy["bookmark_count"]),
		ViewCount:           int64Of(asMap(tweet["views"])["count"]),
		Raw:                 tweet,
	}
	if created, err := time.Parse(xTimeLayout, str(legacy["created_at"])); err == nil {
		post.CreatedAt = created.UTC()
	}
	post.Text = fullText(tweet)
	if retweeted, ok := NormalizeTweet(asMap(asMap(legacy["retweeted_status_result"])["result"])); ok {
		post.IsRetweet = true
		post.RetweetedPostID = retweeted.PostID
		post.Text = fmt.Sprintf("RT @%s: %s", retweeted.AuthorScreenName, retweeted.Text)
	}
	for _, raw := range list(asMap(legacy["entities"])["user_mentions"]) {
		mention := asMap(raw)
		if id := str(mention["id_str"]); id != "" {
			post.MentionedUserIDs = append(post.MentionedUserIDs, id)
		}
		if name := str(mention["screen_name"]); name != "" {
			post.MentionedScreenNames = append(post.MentionedScreenNames, name)
		}
	}
	return post, true
}

// fullText is a long post's note text when it has one, else full_text.
func fullText(tweet map[string]any) string {
	note := asMap(asMap(asMap(tweet["note_tweet"])["note_tweet_results"])["result"])
	if text := str(note["text"]); text != "" {
		return text
	}
	return str(asMap(tweet["legacy"])["full_text"])
}

// UserID reads a UserByScreenName response's rest_id.
func UserID(data map[string]any) string {
	return str(asMap(asMap(data["user"])["result"])["rest_id"])
}

func findInstructions(value any) []any {
	switch v := value.(type) {
	case map[string]any:
		if instructions, ok := v["instructions"].([]any); ok {
			return instructions
		}
		for _, child := range v {
			if found := findInstructions(child); found != nil {
				return found
			}
		}
	case []any:
		for _, child := range v {
			if found := findInstructions(child); found != nil {
				return found
			}
		}
	}
	return nil
}

func asMap(value any) map[string]any {
	m, _ := value.(map[string]any)
	return m
}

func list(value any) []any {
	l, _ := value.([]any)
	return l
}

func str(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

func int64Of(value any) int64 {
	switch v := value.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	default:
		return 0
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
