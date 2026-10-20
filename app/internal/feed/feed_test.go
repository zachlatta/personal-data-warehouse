package feed

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
)

var testNow = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

func TestNormalizePushDefaultsKeySiteAndSource(t *testing.T) {
	item, err := normalizePush(PushItem{Title: " Hello ", URL: "https://www.example.com/a?b=1"}, "HN", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if item.Source != "hn" || item.Key != "https://www.example.com/a?b=1" || item.Site != "example.com" {
		t.Fatalf("got %+v", item)
	}
	if item.ItemID != "hn:https://www.example.com/a?b=1" || item.Title != "Hello" {
		t.Fatalf("got %+v", item)
	}
	noURL, err := normalizePush(PushItem{Source: "notes", Title: "T", Body: "B"}, "", testNow)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := normalizePush(PushItem{Source: "notes", Title: "T", Body: "B"}, "", testNow)
	if noURL.Key == "" || noURL.Key != again.Key {
		t.Fatalf("a card without a url must hash to a stable key: %q %q", noURL.Key, again.Key)
	}
}

func TestNormalizePushRefusesWhatThePhoneWouldDropSilently(t *testing.T) {
	cases := map[string]PushItem{
		"source is required":        {Title: "x"},
		"must be lowercase":         {Source: "Has Space", Title: "x"},
		"title, a body or a url":    {Source: "a"},
		"image_url must be https":   {Source: "a", Title: "x", ImageURL: "http://example.com/i.png"},
		"url must be http or https": {Source: "a", Title: "x", URL: "ftp://example.com/f"},
		"not an absolute URL":       {Source: "a", Title: "x", URL: "/relative"},
		"expires_at":                {Source: "a", Title: "x", ExpiresAt: "next tuesday"},
		"at most 20 tags":           {Source: "a", Title: "x", Tags: strings.Split(strings.Repeat("t,", 21), ",")[:21]},
	}
	for want, in := range cases {
		if in.Source == "" {
			in.Source = ""
		}
		_, err := normalizePush(in, "", testNow)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: got %v", want, err)
		}
	}
}

func TestParseTimeAcceptsDatesAndRelativeExpiry(t *testing.T) {
	for raw, want := range map[string]time.Time{
		"2026-10-21T09:00:00-04:00": time.Date(2026, 10, 21, 13, 0, 0, 0, time.UTC),
		"2026-10-21":                time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC),
		"48h":                       testNow.Add(48 * time.Hour),
		"7d":                        testNow.Add(7 * 24 * time.Hour),
		"2w":                        testNow.Add(14 * 24 * time.Hour),
	} {
		got, err := ParseTime("expires_at", raw, testNow, true)
		if err != nil || !got.Equal(want) {
			t.Errorf("%s: got %v %v want %v", raw, got, err, want)
		}
	}
	if _, err := ParseTime("published_at", "7d", testNow, false); err == nil {
		t.Fatal("published_at must not accept a duration")
	}
}

func TestValidateEvent(t *testing.T) {
	ok := Event{EventID: "e1", Type: "impression", ItemID: "hn:1", OccurredAt: testNow, DwellMS: 1200, VisibleFraction: 0.8}
	if err := ValidateEvent(&ok, testNow); err != nil {
		t.Fatal(err)
	}
	session := Event{EventID: "s1", Type: "session_start", OccurredAt: testNow}
	if err := ValidateEvent(&session, testNow); err != nil {
		t.Fatal(err)
	}
	for want, e := range map[string]Event{
		"event_id is required": {Type: "like", ItemID: "x", OccurredAt: testNow},
		"is not one of":        {EventID: "a", Type: "view", ItemID: "x", OccurredAt: testNow},
		"needs an item_id":     {EventID: "a", Type: "like", OccurredAt: testNow},
		"occurred_at":          {EventID: "a", Type: "like", ItemID: "x"},
		"in the future":        {EventID: "a", Type: "like", ItemID: "x", OccurredAt: testNow.Add(48 * time.Hour)},
		"dwell_ms":             {EventID: "a", Type: "impression", ItemID: "x", OccurredAt: testNow, DwellMS: -1},
		"visible_fraction":     {EventID: "a", Type: "impression", ItemID: "x", OccurredAt: testNow, VisibleFraction: 1.5},
	} {
		if err := ValidateEvent(&e, testNow); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: got %v", want, err)
		}
	}
}

func card(id, source string, score float64, ageHours int, impressions int64, liked bool) Item {
	return Item{ItemID: id, Source: source, Score: score, CreatedAt: testNow.Add(-time.Duration(ageHours) * time.Hour),
		Engagement: Engagement{Impressions: impressions, Liked: liked}}
}

func ids(items []Item) string {
	out := make([]string, len(items))
	for i, item := range items {
		out[i] = item.ItemID
	}
	return strings.Join(out, ",")
}

func TestRankPutsScoreThenFreshnessFirstAndSinksSeenCards(t *testing.T) {
	got := Rank([]Item{
		card("old", "a", 0, 10, 0, false),
		card("new", "b", 0, 1, 0, false),
		card("top", "c", 5, 20, 0, false),
		card("seen", "d", 9, 1, 3, false),
		card("liked", "e", 1, 30, 3, true),
	})
	if ids(got) != "top,liked,new,old,seen" {
		t.Fatalf("got %s", ids(got))
	}
}

func TestRankNeverRunsOneSourceThreeInARowWhileAnotherWaits(t *testing.T) {
	got := Rank([]Item{
		card("a1", "a", 9, 1, 0, false), card("a2", "a", 8, 1, 0, false), card("a3", "a", 7, 1, 0, false),
		card("a4", "a", 6, 1, 0, false), card("b1", "b", 1, 1, 0, false),
	})
	if ids(got) != "a1,a2,b1,a3,a4" {
		t.Fatalf("got %s", ids(got))
	}
}

func TestEnabledFlag(t *testing.T) {
	for value, want := range map[string]bool{"": true, "1": true, "true": true, "0": false, "false": false, "OFF": false, "no": false} {
		if got := Enabled(func(string) string { return value }); got != want {
			t.Errorf("%q: got %v", value, got)
		}
	}
}

func TestRegisterMountsNothingWhenDisabled(t *testing.T) {
	mux := http.NewServeMux()
	tools := Register(mux, Options{DatabaseURL: "postgres://unused", Getenv: func(string) string { return "0" },
		Auth: func(h http.Handler) http.Handler { return h }})
	if tools != nil {
		t.Fatal("disabled module returned tools")
	}
	if _, pattern := mux.Handler(&http.Request{Method: "GET", URL: mustURL(t, APIPath)}); pattern != "" {
		t.Fatalf("disabled module mounted %q", pattern)
	}
	enabled := Register(http.NewServeMux(), Options{DatabaseURL: "postgres://unused", Getenv: func(string) string { return "" },
		Auth: func(h http.Handler) http.Handler { return h }})
	names := []string{}
	for _, tl := range enabled {
		names = append(names, tl.Name())
	}
	if strings.Join(names, ",") != "feed_push,feed_update,feed_remove,feed_rank,feed_list,feed_stats,feed_events" {
		t.Fatalf("tools: %v", names)
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
