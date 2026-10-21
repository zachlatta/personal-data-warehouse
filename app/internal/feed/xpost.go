package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

// An X post pushed as a plain url becomes an embedded card: the push path
// reads the post from fxtwitter's public API (x.com itself answers a login
// wall) and stores its text, author, media and quoted post in embed, so the
// phone shows the post itself rather than a link that opens X.

// XResolver reads one X post; ResolveXPost is the production one.
type XResolver func(ctx context.Context, statusURL string) (*Embed, error)

var xStatusRe = regexp.MustCompile(`^/(?:([A-Za-z0-9_]{1,15})/|i/(?:web/)?)status(?:es)?/(\d{1,25})`)

// XStatusID returns the post id of an x.com or twitter.com status url.
func XStatusID(raw string) (string, bool) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", false
	}
	switch strings.TrimPrefix(strings.ToLower(parsed.Hostname()), "www.") {
	case "x.com", "twitter.com", "mobile.twitter.com", "mobile.x.com":
	default:
		return "", false
	}
	m := xStatusRe.FindStringSubmatch(parsed.Path)
	if m == nil {
		return "", false
	}
	return m[2], true
}

// FxTwitterBase is overridden by tests.
var FxTwitterBase = "https://api.fxtwitter.com"

var xClient = &http.Client{Timeout: 8 * time.Second}

type fxMedia struct {
	Type         string  `json:"type"`
	URL          string  `json:"url"`
	ThumbnailURL string  `json:"thumbnail_url"`
	Width        int     `json:"width"`
	Height       int     `json:"height"`
	AltText      string  `json:"altText"`
	Duration     float64 `json:"duration"`
	Variants     []struct {
		URL         string `json:"url"`
		Bitrate     int    `json:"bitrate"`
		ContentType string `json:"content_type"`
	} `json:"variants"`
}

type fxTweet struct {
	URL    string `json:"url"`
	ID     string `json:"id"`
	Text   string `json:"text"`
	Author struct {
		Name         string `json:"name"`
		ScreenName   string `json:"screen_name"`
		AvatarURL    string `json:"avatar_url"`
		Verification *struct {
			Verified bool `json:"verified"`
		} `json:"verification"`
	} `json:"author"`
	Replies          int64  `json:"replies"`
	Retweets         int64  `json:"retweets"`
	Likes            int64  `json:"likes"`
	Views            int64  `json:"views"`
	Bookmarks        int64  `json:"bookmarks"`
	CreatedTimestamp int64  `json:"created_timestamp"`
	ReplyingTo       string `json:"replying_to"`
	Media            *struct {
		All []fxMedia `json:"all"`
	} `json:"media"`
	Quote *fxTweet `json:"quote"`
}

// phoneVideo picks the mp4 a phone streams well: the best one at or under
// about 2.5 Mbps, else the smallest.
func phoneVideo(m fxMedia) string {
	best, bestRate, smallest, smallestRate := "", -1, "", 1<<31-1
	for _, v := range m.Variants {
		if v.ContentType != "video/mp4" || !strings.HasPrefix(v.URL, "https://") {
			continue
		}
		if v.Bitrate <= 2_500_000 && v.Bitrate > bestRate {
			best, bestRate = v.URL, v.Bitrate
		}
		if v.Bitrate < smallestRate {
			smallest, smallestRate = v.URL, v.Bitrate
		}
	}
	switch {
	case best != "":
		return best
	case smallest != "":
		return smallest
	}
	return m.URL
}

func (t *fxTweet) post() Post {
	e := Post{Type: EmbedTypeXPost, URL: t.URL, ID: t.ID, AuthorName: t.Author.Name,
		AuthorHandle: t.Author.ScreenName, AuthorAvatarURL: t.Author.AvatarURL, Text: t.Text,
		Replies: t.Replies, Reposts: t.Retweets, Likes: t.Likes, Views: t.Views, Bookmarks: t.Bookmarks,
		ReplyingTo: t.ReplyingTo}
	if t.Author.Verification != nil {
		e.AuthorVerified = t.Author.Verification.Verified
	}
	if t.CreatedTimestamp > 0 {
		e.CreatedAt = time.Unix(t.CreatedTimestamp, 0).UTC().Format(time.RFC3339)
	}
	// Larger avatars than the 48px default.
	e.AuthorAvatarURL = strings.Replace(e.AuthorAvatarURL, "_normal.", "_200x200.", 1)
	if t.Media != nil {
		for _, m := range t.Media.All {
			switch m.Type {
			case "photo":
				e.Media = append(e.Media, Media{Type: "image", URL: m.URL, Width: m.Width, Height: m.Height, Alt: m.AltText})
			case "video", "gif":
				e.Media = append(e.Media, Media{Type: m.Type, URL: phoneVideo(m), ThumbnailURL: m.ThumbnailURL,
					Width: m.Width, Height: m.Height, DurationSec: m.Duration})
			}
			if len(e.Media) == maxMedia {
				break
			}
		}
	}
	return e
}

func (t *fxTweet) embed() *Embed {
	e := &Embed{Post: t.post()}
	if t.Quote != nil {
		quote := t.Quote.post()
		e.Quote = &quote
	}
	return e
}

func fetchFx(ctx context.Context, path string, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, FxTwitterBase+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "pdw-feed/1 (+https://personal-data-warehouse.zachlatta.com)")
	resp, err := xClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fxtwitter %s: HTTP %d", path, resp.StatusCode)
	}
	return json.Unmarshal(body, into)
}

// ResolveXPost reads a post, and the author's own thread under it when
// there is one, from fxtwitter.
func ResolveXPost(ctx context.Context, statusURL string) (*Embed, error) {
	id, ok := XStatusID(statusURL)
	if !ok {
		return nil, fmt.Errorf("%q is not an X post url", statusURL)
	}
	var one struct {
		Code  int      `json:"code"`
		Tweet *fxTweet `json:"tweet"`
	}
	if err := fetchFx(ctx, "/status/"+id, &one); err != nil {
		return nil, err
	}
	if one.Tweet == nil {
		return nil, fmt.Errorf("X post %s: not found (code %d)", id, one.Code)
	}
	e := one.Tweet.embed()
	var thread struct {
		Thread []fxTweet `json:"thread"`
	}
	if fetchFx(ctx, "/2/thread/"+id, &thread) == nil {
		for _, t := range thread.Thread {
			if t.ID == id || t.Author.ScreenName != e.AuthorHandle {
				continue
			}
			e.Thread = append(e.Thread, t.post())
			if len(e.Thread) == 25 {
				break
			}
		}
	}
	return e, nil
}

// embedXPosts fills embed on every pushed card whose url is an X post and
// that brought no embed of its own, a few at a time. A post that cannot be
// read stays a plain link card: the push never fails on it.
func embedXPosts(ctx context.Context, resolve XResolver, items []PushItem) {
	if resolve == nil {
		return
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for i := range items {
		item := &items[i]
		if item.Embed != nil {
			continue
		}
		if _, ok := XStatusID(item.URL); !ok {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			e, err := resolve(ctx, item.URL)
			if err != nil || e == nil {
				return
			}
			item.Embed = e
			if strings.TrimSpace(item.Author) == "" && e.AuthorName != "" {
				item.Author = e.AuthorName
			}
			if strings.TrimSpace(item.Site) == "" {
				item.Site = "X"
			}
			if strings.TrimSpace(item.PublishedAt) == "" {
				item.PublishedAt = e.CreatedAt
			}
		}()
	}
	wg.Wait()
}
