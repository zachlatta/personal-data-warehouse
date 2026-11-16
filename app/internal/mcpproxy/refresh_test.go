package mcpproxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestScheduledDiscoveryReplacesSnapshotAndIsolatesFailures(t *testing.T) {
	upstream := mcp.NewServer(&mcp.Implementation{Name: "fake", Version: "1"}, nil)
	add := func(name, description string) {
		upstream.AddTool(&mcp.Tool{Name: name, Description: description, InputSchema: json.RawMessage(`{"type":"object"}`)}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			t.Error("discovery invoked a tool")
			return nil, nil
		})
	}
	add("old", "old")
	add("changed", "before")
	remote := httptest.NewTLSServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return upstream }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
	defer remote.Close()
	store := newMemoryStore()
	for _, name := range []string{"good", "broken", "disabled"} {
		store.Update(context.Background(), name, func(c *record) error {
			c.Enabled = name != "disabled"
			c.URL = remote.URL
			c.AllClients = true
			if name == "broken" {
				c.URL = remote.URL + "/missing"
			}
			return nil
		})
	}
	// The broken endpoint gets a deterministic transport failure.
	client := remote.Client()
	base := client.Transport
	client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/missing" {
			return nil, context.DeadlineExceeded
		}
		return base.RoundTrip(r)
	})
	svc := New(store, "https://pdw.example", client)
	if err := svc.RefreshAll(context.Background()); err == nil {
		t.Fatal("failure not reported")
	}
	upstream.RemoveTools("old")
	add("new", "updated schema description")
	add("changed", "after")
	if err := svc.RefreshAll(context.Background()); err == nil {
		t.Fatal("failure not reported")
	}
	records, _ := store.List(context.Background())
	for _, c := range records {
		switch c.Name {
		case "good":
			if len(c.Tools) != 2 || c.Tools[0].Name != "changed" || c.Tools[0].Description != "after" || c.Tools[1].Name != "new" || c.DiscoveryStatus != "ok" || c.LastDiscoveredAt.IsZero() {
				t.Fatalf("snapshot: %+v", c)
			}
		case "broken":
			if c.DiscoveryStatus != "error" {
				t.Fatalf("failure: %+v", c)
			}
		case "disabled":
			if !c.LastDiscoveryAttemptAt.IsZero() {
				t.Fatal("disabled connection refreshed")
			}
		}
	}
}

func TestDiscoveryCancellationRetainsSnapshot(t *testing.T) {
	store := newMemoryStore()
	store.Update(context.Background(), "slow", func(c *record) error {
		c.Enabled = true
		c.URL = "https://upstream.example/mcp"
		c.Tools = []*mcp.Tool{{Name: "cached"}}
		return nil
	})
	started := make(chan struct{})
	var once sync.Once
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodDelete {
			return nil, context.Canceled
		}
		first := false
		once.Do(func() { first = true; close(started) })
		if !first {
			return nil, context.Canceled
		}
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	svc := New(store, "https://pdw.example", client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- svc.RefreshAll(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("discovery did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancellation not reported")
		}
	case <-time.After(time.Second):
		t.Fatal("discovery ignored cancellation")
	}
	records, _ := store.List(context.Background())
	if len(records[0].Tools) != 1 || records[0].Tools[0].Name != "cached" || records[0].DiscoveryStatus != "error" {
		t.Fatal("cancellation erased snapshot or failure")
	}
}

func TestRefreshLoopRunsImmediatelyRepeatsAndStops(t *testing.T) {
	svc := New(newMemoryStore(), "https://pdw.example", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reports := make(chan error, 10)
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.runRefreshLoop(ctx, 10*time.Millisecond, func(err error) { reports <- err })
	}()
	for range 2 {
		select {
		case err := <-reports:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("missing scheduled pass")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not stop")
	}
}
