package externalfeeds

import (
	"context"
	"testing"
)

func TestProjectionCursorDoesNotSkipConcurrentArrivals(t *testing.T) {
	store := NewMemoryStore()
	feed := All()[0]
	// The source ordering is deliberately unrelated to arrival order.
	store.Replace(feed.Scope, []Item{{Key: "b", Payload: map[string]any{"id": "b"}}, {Key: "c", Payload: map[string]any{"id": "c"}}})
	rows, cursor, more, err := store.Page(context.Background(), feed, 0, 1)
	if err != nil || len(rows) != 1 || !more {
		t.Fatalf("first page %v %v", rows, err)
	}
	store.Replace(feed.Scope, []Item{{Key: "a", Payload: map[string]any{"id": "a"}}, {Key: "b", Payload: map[string]any{"id": "b"}}, {Key: "c", Payload: map[string]any{"id": "c"}}})
	seen := map[string]bool{rows[0]["id"].(string): true}
	for {
		rows, cursor, more, err = store.Page(context.Background(), feed, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			seen[row["id"].(string)] = true
		}
		if !more {
			break
		}
	}
	if len(seen) != 3 {
		t.Fatalf("missed a memo: %v", seen)
	}
	// A revision of an already delivered item receives a new arrival number.
	store.Replace(feed.Scope, []Item{{Key: "b", Payload: map[string]any{"id": "b", "transcript": "new"}}})
	rows, _, _, err = store.Page(context.Background(), feed, cursor, 10)
	if err != nil || len(rows) != 1 || rows[0]["transcript"] != "new" {
		t.Fatalf("revision %v %v", rows, err)
	}
}

func TestRegistryDefinesAuthorizationAndHandlers(t *testing.T) {
	for _, feed := range All() {
		if !IsScope(feed.Scope) || feed.Path == "" || feed.DisplayName == "" || feed.Handler == nil {
			t.Fatalf("incomplete definition: %+v", feed)
		}
	}
	if IsScope("feed:unknown") {
		t.Fatal("unknown feed accepted")
	}
}
