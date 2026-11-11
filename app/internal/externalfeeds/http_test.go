package externalfeeds

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestCursorPollingAndETag(t *testing.T) {
	store := NewMemoryStore()
	feed := All()[0]
	handler := feed.Handler(feed, store)
	store.Replace(feed.Scope, []Item{{Key: "one", Payload: map[string]any{"id": "one"}}})
	call := func(url, etag string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", url, nil)
		r.Header.Set("If-None-Match", etag)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	w := call(feed.Path, "")
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	var body struct {
		Next       string           `json:"next_cursor"`
		Recordings []map[string]any `json:"recordings"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	url := feed.Path + "?since=" + body.Next
	empty := call(url, "")
	if empty.Code != 200 {
		t.Fatal(empty.Code)
	}
	if poll := call(url, empty.Header().Get("ETag")); poll.Code != 304 {
		t.Fatal(poll.Code)
	}
	store.Replace(feed.Scope, []Item{{Key: "one", Payload: map[string]any{"id": "one"}}, {Key: "two", Payload: map[string]any{"id": "two"}}})
	changed := call(url, empty.Header().Get("ETag"))
	if changed.Code != 200 {
		t.Fatal("missed new memo")
	}
	if err := json.Unmarshal(changed.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Recordings) != 1 || body.Recordings[0]["id"] != "two" {
		t.Fatal(body.Recordings)
	}
	for _, q := range []string{"?since=broken", "?offset=1"} {
		if bad := call(feed.Path+q, ""); bad.Code != 400 {
			t.Fatal(bad.Code)
		}
	}
}
