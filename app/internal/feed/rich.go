package feed

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Rich cards: a swipeable media carousel (Instagram) and an embedded post
// shown natively, text and media inline (an X post), instead of a link out.
// Both are optional columns on base_feed.items (media, embed).

const (
	maxMedia       = 10
	maxEmbedText   = 8000
	maxEmbedChars  = 32768
	EmbedTypeXPost = "x_post"
)

var mediaTypes = map[string]bool{"image": true, "video": true, "gif": true}

// Media is one photo or video on a card. For a video or gif, URL is the
// mp4 and ThumbnailURL the still the card shows until it is played.
type Media struct {
	Type         string  `json:"type" jsonschema:"image, video or gif"`
	URL          string  `json:"url" jsonschema:"https image, or the https mp4 for a video or gif"`
	ThumbnailURL string  `json:"thumbnail_url,omitempty" jsonschema:"https still for a video or gif"`
	Width        int     `json:"width,omitempty"`
	Height       int     `json:"height,omitempty"`
	Alt          string  `json:"alt,omitempty"`
	DurationSec  float64 `json:"duration_sec,omitempty"`
}

// Post is one X post: who wrote it, what it says, its numbers and media.
type Post struct {
	Type            string  `json:"type" jsonschema:"x_post"`
	URL             string  `json:"url,omitempty"`
	ID              string  `json:"id,omitempty"`
	AuthorName      string  `json:"author_name,omitempty"`
	AuthorHandle    string  `json:"author_handle,omitempty"`
	AuthorAvatarURL string  `json:"author_avatar_url,omitempty"`
	AuthorVerified  bool    `json:"author_verified,omitempty"`
	Text            string  `json:"text,omitempty"`
	CreatedAt       string  `json:"created_at,omitempty" jsonschema:"RFC3339"`
	Replies         int64   `json:"replies,omitempty"`
	Reposts         int64   `json:"reposts,omitempty"`
	Likes           int64   `json:"likes,omitempty"`
	Views           int64   `json:"views,omitempty"`
	Bookmarks       int64   `json:"bookmarks,omitempty"`
	ReplyingTo      string  `json:"replying_to,omitempty" jsonschema:"handle this post answers"`
	Media           []Media `json:"media,omitempty"`
}

// Embed is a post rendered inside the card, with the post it quotes and the
// author's own thread under it (shown in the detail view). Type is x_post
// today; an agent usually never builds one: pushing an x.com status url
// fills it in (ResolveXPost). Quote and Thread are Posts, not Embeds, so the
// type is not recursive: the MCP SDK derives tool schemas from these types
// and panics on a cycle.
type Embed struct {
	Post
	Quote  *Post  `json:"quote,omitempty" jsonschema:"the post this one quotes"`
	Thread []Post `json:"thread,omitempty" jsonschema:"the author's follow-up posts"`
}

func normalizeMedia(field string, media []Media) ([]Media, error) {
	if len(media) > maxMedia {
		return nil, invalid("%s: at most %d media", field, maxMedia)
	}
	out := []Media{}
	for i, m := range media {
		kind := strings.ToLower(strings.TrimSpace(m.Type))
		if kind == "" {
			kind = "image"
		}
		if !mediaTypes[kind] {
			return nil, invalid("%s[%d].type %q must be image, video or gif", field, i, m.Type)
		}
		value, err := checkURL(fmt.Sprintf("%s[%d].url", field, i), m.URL, true)
		if err != nil {
			return nil, err
		}
		if value == "" {
			return nil, invalid("%s[%d] needs a url", field, i)
		}
		thumb, err := checkURL(fmt.Sprintf("%s[%d].thumbnail_url", field, i), m.ThumbnailURL, true)
		if err != nil {
			return nil, err
		}
		if m.Width < 0 || m.Height < 0 {
			return nil, invalid("%s[%d]: width and height must not be negative", field, i)
		}
		out = append(out, Media{Type: kind, URL: value, ThumbnailURL: thumb, Width: m.Width, Height: m.Height,
			Alt: strings.TrimSpace(m.Alt), DurationSec: m.DurationSec})
	}
	return out, nil
}

func normalizePost(field string, p Post) (Post, error) {
	p.Type = strings.TrimSpace(p.Type)
	if p.Type != EmbedTypeXPost {
		return p, invalid("%s.type %q must be %s", field, p.Type, EmbedTypeXPost)
	}
	if len([]rune(p.Text)) > maxEmbedText {
		return p, invalid("%s.text is longer than %d characters", field, maxEmbedText)
	}
	var err error
	if p.URL, err = checkURL(field+".url", p.URL, false); err != nil {
		return p, err
	}
	if p.AuthorAvatarURL, err = checkURL(field+".author_avatar_url", p.AuthorAvatarURL, true); err != nil {
		return p, err
	}
	if p.Media, err = normalizeMedia(field+".media", p.Media); err != nil {
		return p, err
	}
	if len(p.Media) == 0 {
		p.Media = nil
	}
	return p, nil
}

func normalizeEmbed(e *Embed) (*Embed, error) {
	if e == nil {
		return nil, nil
	}
	out := Embed{}
	var err error
	if out.Post, err = normalizePost("embed", e.Post); err != nil {
		return nil, err
	}
	if e.Quote != nil {
		quote, err := normalizePost("embed.quote", *e.Quote)
		if err != nil {
			return nil, err
		}
		out.Quote = &quote
	}
	for i, p := range e.Thread {
		if i == 25 {
			break
		}
		post, err := normalizePost(fmt.Sprintf("embed.thread[%d]", i), p)
		if err != nil {
			return nil, err
		}
		out.Thread = append(out.Thread, post)
	}
	return &out, nil
}

func mediaJSON(media []Media) (string, error) {
	if media == nil {
		media = []Media{}
	}
	raw, err := json.Marshal(media)
	return string(raw), err
}

func embedJSON(e *Embed) (string, error) {
	if e == nil {
		return "{}", nil
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	if len(raw) > maxEmbedChars {
		return "", invalid("embed is larger than %d bytes", maxEmbedChars)
	}
	return string(raw), nil
}

// decodeEmbed reads the stored column; '{}' is no embed.
func decodeEmbed(raw []byte) *Embed {
	var e Embed
	if len(raw) == 0 || json.Unmarshal(raw, &e) != nil || e.Type == "" {
		return nil
	}
	return &e
}
