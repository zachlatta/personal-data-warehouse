package query

import (
	"context"
	"errors"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

type capturingRecorder struct{ timings []SearchTiming }

func (r *capturingRecorder) Record(t SearchTiming) { r.timings = append(r.timings, t) }

type panickingRecorder struct{}

func (panickingRecorder) Record(SearchTiming) { panic("timing store exploded") }

func searchContext(surface, client string) context.Context {
	ctx := pdwauth.WithClientNameHolder(context.Background())
	pdwauth.SetClientName(ctx, client)
	return WithSearchSurface(ctx, surface)
}

func TestEverySearchHandsItsTimingsToTheRecorderWithoutTheQueryText(t *testing.T) {
	runner := &fakeSearchRunner{
		fakeRunner: fakeRunner{results: hybridProbeResult(true)},
		argsResults: map[string]RawResult{
			searchHybridLexicalSQL:  searchRefRows("gmail_email:a|m1"),
			searchHybridSemanticSQL: semanticHits(),
			searchHybridExactSQL:    searchRefRows(),
			searchHybridFuseSQL:     searchHit(),
		},
		delays: map[string]time.Duration{searchHybridSemanticSQL: 20 * time.Millisecond},
	}
	recorder := &capturingRecorder{}
	svc := NewService(runner, Options{
		SearchEmbedder: &fakeEmbedder{model: "m", vector: []float64{1}},
		TimingRecorder: recorder,
	})

	resp := svc.Search(searchContext("mcp", "claude"), SearchRequest{
		Query: "offer letter", Priorities: []string{"self"},
	})
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	if len(recorder.timings) != 1 {
		t.Fatalf("recorded %d timings, want 1", len(recorder.timings))
	}
	got := recorder.timings[0]
	if got.Surface != "mcp" || got.Client != "claude" || got.Mode != SearchModeHybrid || got.Status != "ok" {
		t.Fatalf("timing identity = %+v", got)
	}
	if !got.HasPriorities || got.HasSources || got.Rows != resp.TotalRows || !got.Hybrid {
		t.Fatalf("timing scope = %+v", got)
	}
	if got.SlowestLeg != "semantic" || got.Semantic < 20*time.Millisecond || got.Duration < got.Semantic {
		t.Fatalf("timing legs = %+v", got)
	}
	if got.OccurredAt.IsZero() {
		t.Fatal("timing has no occurred_at")
	}
}

func TestAFailedSearchIsRecordedAsAnErrorWithItsDuration(t *testing.T) {
	runner := &fakeSearchRunner{
		argsErrs: map[string]error{searchTextSQL: errors.New("canceling statement due to statement timeout")},
		delays:   map[string]time.Duration{searchTextSQL: 15 * time.Millisecond},
	}
	recorder := &capturingRecorder{}
	svc := NewService(runner, Options{TimingRecorder: recorder})

	resp := svc.Search(searchContext("api", "pdw"), SearchRequest{Query: "invoice", Mode: SearchModeKeyword, Sources: []string{"gmail"}})
	if resp.Error == "" {
		t.Fatal("expected the search to fail")
	}
	if len(recorder.timings) != 1 {
		t.Fatalf("recorded %d timings, want 1", len(recorder.timings))
	}
	got := recorder.timings[0]
	if got.Status != "error" || got.Mode != SearchModeKeyword || got.Hybrid || !got.HasSources || got.Duration < 15*time.Millisecond {
		t.Fatalf("failed-search timing = %+v", got)
	}
}

func TestARejectedRequestRanNoSearchAndRecordsNothing(t *testing.T) {
	recorder := &capturingRecorder{}
	svc := NewService(&fakeSearchRunner{}, Options{TimingRecorder: recorder})
	svc.Search(context.Background(), SearchRequest{Query: "  "})
	svc.Search(context.Background(), SearchRequest{Query: "x", Mode: "fuzzy"})
	if len(recorder.timings) != 0 {
		t.Fatalf("recorded %d timings for requests that never searched", len(recorder.timings))
	}
}

func TestAPanickingRecorderNeverFailsTheSearch(t *testing.T) {
	runner := &fakeSearchRunner{argsResults: map[string]RawResult{searchTextSQL: searchHit()}}
	svc := NewService(runner, Options{TimingRecorder: panickingRecorder{}})

	resp := svc.Search(context.Background(), SearchRequest{Query: "offer letter", Mode: SearchModeKeyword})
	if resp.Error != "" || resp.TotalRows != 1 {
		t.Fatalf("search with a panicking recorder = error %q, rows %d", resp.Error, resp.TotalRows)
	}
}

func TestSampleHostPressureMarksUnreadableGaugesUnmeasured(t *testing.T) {
	read := func(path string) ([]byte, error) {
		switch path {
		case "/proc/pressure/io":
			return []byte("some avg10=12.25 avg60=0 avg300=0 total=1\nfull avg10=9.75 avg60=0 avg300=0 total=1\n"), nil
		}
		return nil, errors.New("unreadable")
	}
	got := SampleHostPressure(read)
	if got.IOFullAvg10 != 9.75 || got.CPUSomeAvg10 != -1 || got.Load1m != -1 || got.CPUCount <= 0 {
		t.Fatalf("pressure = %+v", got)
	}
}
