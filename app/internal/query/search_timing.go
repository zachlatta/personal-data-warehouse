package query

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// slowSearchThreshold is C6's line. A search slower than this is a question
// about the host as much as the query: was the long pole CPU-bound on an idle
// box, or waiting on pages? The answer is only knowable at the time of the
// request, so the request records it.
const slowSearchThreshold = 2 * time.Second

// hybridTimings is where a hybrid search spent its wall clock. The three
// retrieval legs run concurrently, so wall time is roughly embed + the
// slowest leg + fuse; naming the slowest leg says which one to fix.
type hybridTimings struct {
	Embed    time.Duration
	Lexical  time.Duration
	Exact    time.Duration
	Semantic time.Duration
	Fuse     time.Duration
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
	}
}

// hostPressure reads the PSI averages and load the kernel already keeps, so a
// slow search can say whether the host was saturated. Every field is optional:
// an unreadable file is left out rather than reported as zero, because zero
// pressure reads as an idle host, which is the verdict that matters most.
func (s *Service) hostPressure() []any {
	attrs := []any{"cpu_count", runtime.NumCPU()}
	read := s.readHostFile
	if read == nil {
		return attrs
	}
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
				attrs = append(attrs, source.prefix+"_"+fields[0]+"_avg10", parsed)
			}
		}
	}
	if raw, err := read("/proc/loadavg"); err == nil {
		if fields := strings.Fields(string(raw)); len(fields) > 0 {
			if parsed, err := strconv.ParseFloat(fields[0], 64); err == nil {
				attrs = append(attrs, "load_1m", parsed)
			}
		}
	}
	return attrs
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
