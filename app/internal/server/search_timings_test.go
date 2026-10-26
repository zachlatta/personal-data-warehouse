package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// keywordRunner answers every keyword search with one hit.
type keywordRunner struct{}

func (keywordRunner) Query(context.Context, string, int) (query.RawResult, error) {
	return query.RawResult{}, nil
}

func (keywordRunner) QueryArgs(context.Context, string, []any, int) (query.RawResult, error) {
	return query.RawResult{
		Columns: []string{"ref", "priority", "title"},
		Rows:    []map[string]any{{"ref": "gmail_email:a|m1", "priority": "direct", "title": "Offer"}},
	}, nil
}

type fakeTimingSink struct {
	mu         sync.Mutex
	insertErr  error
	block      chan struct{} // non-nil: insert waits on it
	inserted   []query.SearchTiming
	pressures  []query.HostPressure
	heartbeats []searchTimingTick
}

func (s *fakeTimingSink) insert(ctx context.Context, t query.SearchTiming, p query.HostPressure) error {
	if s.block != nil {
		<-s.block
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.insertErr != nil {
		return s.insertErr
	}
	s.inserted = append(s.inserted, t)
	s.pressures = append(s.pressures, p)
	return nil
}

func (s *fakeTimingSink) heartbeat(_ context.Context, tick searchTimingTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeats = append(s.heartbeats, tick)
	return nil
}

func (s *fakeTimingSink) snapshot() ([]query.SearchTiming, []searchTimingTick) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]query.SearchTiming(nil), s.inserted...), append([]searchTimingTick(nil), s.heartbeats...)
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func searchOnce(t *testing.T, svc *query.Service) query.SearchResponse {
	t.Helper()
	started := time.Now()
	resp := svc.Search(context.Background(), query.SearchRequest{Query: "offer", Mode: query.SearchModeKeyword})
	if resp.Error != "" || resp.TotalRows != 1 {
		t.Fatalf("search = error %q, rows %d", resp.Error, resp.TotalRows)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("search took %s with the timing writer attached", elapsed)
	}
	return resp
}

func TestAFailingTimingWriteNeverFailsTheSearchAndTheHeartbeatSaysWhy(t *testing.T) {
	sink := &fakeTimingSink{insertErr: errors.New("permission denied for schema ops")}
	writer := newSearchTimingWriter(sink, quietLogger())
	writer.readHostFile = func(string) ([]byte, error) { return nil, errors.New("no /proc") }
	writer.start(time.Hour)
	defer writer.close()
	svc := query.NewService(keywordRunner{}, query.Options{TimingRecorder: writer, Logger: quietLogger()})

	searchOnce(t, svc)
	waitFor(t, "the startup heartbeat", func() bool { _, beats := sink.snapshot(); return len(beats) == 1 })
	waitFor(t, "the queued write to be attempted", func() bool { return len(writer.queue) == 0 })
	writer.close() // waits for the worker to finish the attempt it dequeued
	writer.beat()

	_, beats := sink.snapshot()
	last := beats[len(beats)-1]
	if last.Status != searchTimingStatusError || !strings.Contains(last.Error, "permission denied") {
		t.Fatalf("heartbeat after a failed write = %+v", last)
	}
	// A beat that reported the error clears it: the next one is judged afresh.
	writer.beat()
	_, beats = sink.snapshot()
	if got := beats[len(beats)-1]; got.Status != searchTimingStatusOK {
		t.Fatalf("heartbeat after the error was reported = %+v", got)
	}
}

func TestABlockedTimingWriterNeverBlocksTheSearchAndCountsWhatItDropped(t *testing.T) {
	sink := &fakeTimingSink{block: make(chan struct{})}
	writer := newSearchTimingWriter(sink, quietLogger())
	writer.readHostFile = func(string) ([]byte, error) { return nil, errors.New("no /proc") }
	writer.start(time.Hour)
	svc := query.NewService(keywordRunner{}, query.Options{TimingRecorder: writer, Logger: quietLogger()})

	// The worker holds one timing in a blocked insert; the queue takes
	// searchTimingQueueSize more; everything past that is dropped, and no
	// search waits for any of it.
	for i := 0; i < searchTimingQueueSize+25; i++ {
		searchOnce(t, svc)
	}
	waitFor(t, "drops to be counted", func() bool { return writer.dropped.Load() >= 24 })
	close(sink.block)
	writer.close()

	writer.beat()
	_, beats := sink.snapshot()
	last := beats[len(beats)-1]
	if last.Status != searchTimingStatusDropped || last.Dropped < 24 || !strings.Contains(last.Error, "dropped") {
		t.Fatalf("heartbeat after an overflow = %+v", last)
	}
}

func TestTheTimingWriterStoresHostPressureAndAnIDWithEachSearch(t *testing.T) {
	sink := &fakeTimingSink{}
	writer := newSearchTimingWriter(sink, quietLogger())
	writer.readHostFile = func(path string) ([]byte, error) {
		if path == "/proc/pressure/io" {
			return []byte("some avg10=30.00 avg60=0 avg300=0 total=1\nfull avg10=18.50 avg60=0 avg300=0 total=1\n"), nil
		}
		return nil, errors.New("unreadable")
	}
	writer.start(time.Hour)
	defer writer.close()
	svc := query.NewService(keywordRunner{}, query.Options{TimingRecorder: writer, Logger: quietLogger()})

	searchOnce(t, svc)
	waitFor(t, "the write", func() bool { got, _ := sink.snapshot(); return len(got) == 1 })
	got, _ := sink.snapshot()
	if got[0].RequestID == "" || got[0].Mode != query.SearchModeKeyword || got[0].Hybrid {
		t.Fatalf("stored timing = %+v", got[0])
	}
	sink.mu.Lock()
	pressure := sink.pressures[0]
	sink.mu.Unlock()
	if pressure.IOFullAvg10 != 18.5 || pressure.CPUSomeAvg10 != -1 || pressure.Load1m != -1 {
		t.Fatalf("stored pressure = %+v", pressure)
	}
}

func TestSearchSurfaceMiddlewareTagsTheRecordedSearch(t *testing.T) {
	for _, surface := range []string{"mcp", "api"} {
		sink := &fakeTimingSink{}
		writer := newSearchTimingWriter(sink, quietLogger())
		writer.readHostFile = func(string) ([]byte, error) { return nil, errors.New("no /proc") }
		writer.start(time.Hour)
		svc := query.NewService(keywordRunner{}, query.Options{TimingRecorder: writer, Logger: quietLogger()})
		handler := withSearchSurface(surface, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			svc.Search(r.Context(), query.SearchRequest{Query: "offer", Mode: query.SearchModeKeyword})
		}))
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
		waitFor(t, "the write", func() bool { got, _ := sink.snapshot(); return len(got) == 1 })
		writer.close()
		if got, _ := sink.snapshot(); got[0].Surface != surface {
			t.Fatalf("surface = %q, want %q", got[0].Surface, surface)
		}
	}
}

// Driven by tests/test_search_request_timing_store.py against a
// Python-provisioned pdw_test_* schema; go test alone never guesses a database.
// It writes through the real store into the Python tables, and creates the Go
// DDL in a sibling schema the Python test then compares column by column.
func TestSearchTimingStoreIntegration(t *testing.T) {
	url := os.Getenv("PDW_SEARCH_TIMING_TEST_DATABASE_URL")
	schema := os.Getenv("PDW_SEARCH_TIMING_TEST_SCHEMA")
	goSchema := os.Getenv("PDW_SEARCH_TIMING_TEST_GO_SCHEMA")
	if url == "" || schema == "" || goSchema == "" {
		t.Skip("exercised by tests/test_search_request_timing_store.py in uv run pytest")
	}
	if !strings.HasPrefix(schema, "pdw_test_") || !strings.HasPrefix(goSchema, "pdw_test_") {
		t.Fatal("integration test requires isolated test schemas")
	}
	ctx := context.Background()
	open := func(target string) *searchTimingStore {
		store, err := newSearchTimingStore(url)
		if err != nil {
			t.Fatal(err)
		}
		store.schema = target
		store.timings = warehouse.QuoteIdent(target) + `."search_request_timings"`
		store.state = warehouse.QuoteIdent(target) + `."search_request_timing_state"`
		return store
	}

	goStore := open(goSchema)
	defer goStore.Close()
	if err := goStore.ensure(ctx); err != nil {
		t.Fatalf("Go DDL: %v", err)
	}

	store := open(schema)
	defer store.Close()
	occurred := time.Date(2026, 10, 25, 14, 0, 0, 0, time.UTC)
	hybrid := query.SearchTiming{
		OccurredAt: occurred, RequestID: "r1", Surface: "mcp", Client: "claude", Mode: "hybrid", Status: "ok",
		HasPriorities: true, Rows: 10, Duration: 3400 * time.Millisecond, Hybrid: true,
		Embed: 40 * time.Millisecond, Lexical: 800 * time.Millisecond, Exact: 300 * time.Millisecond,
		Semantic: 3100 * time.Millisecond, Fuse: 20 * time.Millisecond, SlowestLeg: "semantic",
	}
	keyword := query.SearchTiming{OccurredAt: occurred, RequestID: "r2", Surface: "api", Client: "pdw", Mode: "keyword", Status: "error", Duration: time.Minute}
	if err := store.insert(ctx, hybrid, query.HostPressure{IOFullAvg10: 18.5, CPUSomeAvg10: 0.4, Load1m: 3.1, CPUCount: 28}); err != nil {
		t.Fatal(err)
	}
	if err := store.insert(ctx, keyword, query.HostPressure{IOFullAvg10: -1, CPUSomeAvg10: -1, Load1m: -1, CPUCount: 28}); err != nil {
		t.Fatal(err)
	}
	var semantic, keywordSemantic, flags int64
	var ioFull float64
	if err := store.db.QueryRowContext(ctx, `SELECT semantic_ms, io_pressure_full_avg10, has_priorities FROM `+store.timings+` WHERE request_id = 'r1'`).Scan(&semantic, &ioFull, &flags); err != nil {
		t.Fatal(err)
	}
	if semantic != 3100 || ioFull != 18.5 || flags != 1 {
		t.Fatalf("hybrid row = semantic %d, io %v, has_priorities %d", semantic, ioFull, flags)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT semantic_ms FROM `+store.timings+` WHERE request_id = 'r2'`).Scan(&keywordSemantic); err != nil || keywordSemantic != -1 {
		t.Fatalf("keyword row semantic_ms = %d (%v), want -1", keywordSemantic, err)
	}

	// Two containers during a deploy beat into the ONE row; the newest wins.
	other := open(schema)
	defer other.Close()
	if err := other.heartbeat(ctx, searchTimingTick{TickedAt: occurred, Status: "error", Error: "connection reset"}); err != nil {
		t.Fatal(err)
	}
	if err := store.heartbeat(ctx, searchTimingTick{TickedAt: occurred.Add(time.Minute), LastWriteAt: occurred, Status: "ok", Written: 2}); err != nil {
		t.Fatal(err)
	}
	var rows int
	var status string
	var written int64
	if err := store.db.QueryRowContext(ctx, `SELECT count(*), max(status), max(written_since_tick) FROM `+store.state).Scan(&rows, &status, &written); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || status != "ok" || written != 2 {
		t.Fatalf("heartbeat rows = %d, status %q, written %d", rows, status, written)
	}
}
