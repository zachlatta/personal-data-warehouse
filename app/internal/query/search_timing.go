package query

import (
	"context"
	"log/slog"
	"runtime"
	"strconv"
	"strings"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

// slowSearchThreshold is C6's line. A search slower than this is a question
// about the host as much as the query: was the long pole CPU-bound on an idle
// box, or waiting on pages? The answer is only knowable at the time of the
// request, so the request records it.
const slowSearchThreshold = 2 * time.Second

// exactLegGrace is how long the hybrid literal leg may run on after the
// lexical and semantic legs have both answered before it is cut. C6's budget
// is two seconds a search; on 2026-10-27 the median of the slower of those two
// legs was about 1.3s, so 750ms keeps a cut search inside it.
const exactLegGrace = 750 * time.Millisecond

// hybridTimings is where a hybrid search spent its wall clock. The three
// retrieval legs run concurrently, so wall time is roughly embed + the
// slowest leg + fuse; naming the slowest leg says which one to fix.
type hybridTimings struct {
	Embed    time.Duration
	Lexical  time.Duration
	Exact    time.Duration
	Semantic time.Duration
	Fuse     time.Duration
	// DegradedLegs names the legs the fused answer went without: "exact"
	// when the literal leg was cut after exactLegGrace, or any leg that hit
	// the statement timeout.
	DegradedLegs []string
}

func (t hybridTimings) slowestLeg() string {
	leg, longest := "lexical", t.Lexical
	if t.Exact > longest {
		leg, longest = "exact", t.Exact
	}
	if t.Semantic > longest {
		leg = "semantic"
	}
	return leg
}

func (t hybridTimings) logAttrs() []any {
	return []any{
		"embed_ms", t.Embed.Milliseconds(),
		"lexical_ms", t.Lexical.Milliseconds(),
		"exact_ms", t.Exact.Milliseconds(),
		"semantic_ms", t.Semantic.Milliseconds(),
		"fuse_ms", t.Fuse.Milliseconds(),
		"slowest_leg", t.slowestLeg(),
		"degraded_legs", strings.Join(t.DegradedLegs, ","),
	}
}

// hostPressure reads the PSI averages and load the kernel already keeps, so a
// slow search can say whether the host was saturated. Every field is optional:
// an unreadable file is left out rather than reported as zero, because zero
// pressure reads as an idle host, which is the verdict that matters most.
func (s *Service) hostPressure() []any {
	attrs := []any{"cpu_count", runtime.NumCPU()}
	if s.readHostFile == nil {
		return attrs
	}
	gauges, load, loadOK := readHostGauges(s.readHostFile)
	for _, gauge := range gauges {
		attrs = append(attrs, gauge.name, gauge.value)
	}
	if loadOK {
		attrs = append(attrs, "load_1m", load)
	}
	return attrs
}

type hostGauge struct {
	name  string
	value float64
}

// readHostGauges parses /proc/pressure/{cpu,io} avg10 lines (cpu_some_avg10,
// io_full_avg10, ...) and the 1-minute load. Unreadable values are absent.
func readHostGauges(read func(string) ([]byte, error)) ([]hostGauge, float64, bool) {
	var gauges []hostGauge
	for _, source := range []struct{ path, prefix string }{
		{"/proc/pressure/cpu", "cpu"},
		{"/proc/pressure/io", "io"},
	} {
		raw, err := read(source.path)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(raw), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 || (fields[0] != "some" && fields[0] != "full") {
				continue
			}
			value, ok := strings.CutPrefix(fields[1], "avg10=")
			if !ok {
				continue
			}
			if parsed, err := strconv.ParseFloat(value, 64); err == nil {
				gauges = append(gauges, hostGauge{source.prefix + "_" + fields[0] + "_avg10", parsed})
			}
		}
	}
	if raw, err := read("/proc/loadavg"); err == nil {
		if fields := strings.Fields(string(raw)); len(fields) > 0 {
			if parsed, err := strconv.ParseFloat(fields[0], 64); err == nil {
				return gauges, parsed, true
			}
		}
	}
	return gauges, 0, false
}

// HostPressure is the host's saturation at one moment, as stored beside a
// search's timings. -1 is unmeasured: 0 would read as an idle host.
type HostPressure struct {
	IOFullAvg10  float64
	CPUSomeAvg10 float64
	Load1m       float64
	CPUCount     int
}

// SampleHostPressure reads the same PSI and load gauges the slow-search log
// carries. read is os.ReadFile in production.
func SampleHostPressure(read func(string) ([]byte, error)) HostPressure {
	p := HostPressure{IOFullAvg10: -1, CPUSomeAvg10: -1, Load1m: -1, CPUCount: runtime.NumCPU()}
	if read == nil {
		return p
	}
	gauges, load, loadOK := readHostGauges(read)
	for _, gauge := range gauges {
		switch gauge.name {
		case "io_full_avg10":
			p.IOFullAvg10 = gauge.value
		case "cpu_some_avg10":
			p.CPUSomeAvg10 = gauge.value
		}
	}
	if loadOK {
		p.Load1m = load
	}
	return p
}

// SearchTiming is one answered search as ops.search_request_timings stores
// it: how long, which leg, from which surface and client, and whether a scope
// was given. Never the query text or the scope's values -- a query can be
// private, and the timing is the evidence C6 needs.
type SearchTiming struct {
	OccurredAt    time.Time
	RequestID     string
	Surface       string
	Client        string
	Mode          string
	Status        string
	HasPriorities bool
	HasSources    bool
	Rows          int
	Duration      time.Duration
	// Hybrid is false for keyword and exact searches; their leg fields are
	// then meaningless and stored as -1.
	Hybrid     bool
	Embed      time.Duration
	Lexical    time.Duration
	Exact      time.Duration
	Semantic   time.Duration
	Fuse       time.Duration
	SlowestLeg string
	// DegradedLegs is the comma-separated legs the answer went without
	// ("exact" when the literal leg was cut); empty when all three answered.
	DegradedLegs string
}

// TimingRecorder receives every search the service ran, after the response
// is built. Record must return at once and never fail the search: the
// production recorder queues the timing for a background writer and drops it
// when the queue is full.
type TimingRecorder interface {
	Record(SearchTiming)
}

type searchSurfaceKey struct{}

// WithSearchSurface marks the entry point a request came through ("mcp" or
// "api"), so a stored timing can tell agents on MCP from the CLI and web app.
func WithSearchSurface(ctx context.Context, surface string) context.Context {
	return context.WithValue(ctx, searchSurfaceKey{}, surface)
}

func searchSurfaceFrom(ctx context.Context) string {
	surface, _ := ctx.Value(searchSurfaceKey{}).(string)
	return surface
}

// recordSearch hands the finished search to the recorder. A recorder that
// panics is logged and swallowed: a timing is never worth a search.
func (s *Service) recordSearch(ctx context.Context, req SearchRequest, resp SearchResponse, status string, duration time.Duration, timings *hybridTimings) {
	if s.timingRecorder == nil {
		return
	}
	timing := SearchTiming{
		OccurredAt:    time.Now().UTC(),
		Surface:       searchSurfaceFrom(ctx),
		Client:        pdwauth.ClientNameFromContext(ctx),
		Mode:          resp.Mode,
		Status:        status,
		HasPriorities: len(req.Priorities) > 0,
		HasSources:    len(req.Sources) > 0,
		Rows:          resp.TotalRows,
		Duration:      duration,
	}
	if timings != nil {
		timing.Hybrid = true
		timing.Embed, timing.Lexical, timing.Exact = timings.Embed, timings.Lexical, timings.Exact
		timing.Semantic, timing.Fuse = timings.Semantic, timings.Fuse
		timing.SlowestLeg = timings.slowestLeg()
		timing.DegradedLegs = strings.Join(timings.DegradedLegs, ",")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.WarnContext(ctx, "search timing recorder panicked", slog.Any("panic", recovered))
		}
	}()
	s.timingRecorder.Record(timing)
}

func (s *Service) logSearchCompleted(ctx context.Context, resp SearchResponse, duration time.Duration, timings *hybridTimings) {
	attrs := []any{
		"query", resp.Query, "mode", resp.Mode, "fallback_reason", resp.FallbackReason,
		"rows", resp.TotalRows, "duration", duration,
	}
	if timings != nil {
		attrs = append(attrs, timings.logAttrs()...)
	}
	s.logger.InfoContext(ctx, "search completed", attrs...)
	if s.slowSearchThreshold <= 0 || duration < s.slowSearchThreshold {
		return
	}
	slow := append([]any{"duration_ms", duration.Milliseconds()}, attrs...)
	slow = append(slow, s.hostPressure()...)
	s.logger.WarnContext(ctx, "slow search", slow...)
}
