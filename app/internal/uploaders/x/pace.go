package x

import (
	"math/rand/v2"
	"time"
)

// Pacer is every random choice a run makes so its traffic reads like a
// person's browser rather than a clock: launchd fires on a fixed interval,
// and a fixed gap, a fixed stream order and a fixed page count would make
// every run the same fingerprint. The random source is injected, so tests
// are deterministic. A nil *Pacer means no randomness at all.
type Pacer struct {
	Rand *rand.Rand
	// Base is the median gap between requests.
	Base time.Duration
	// SkipChance is the chance a stream sits out one run (its gaps wait).
	SkipChance float64
	// PauseChance is the chance a gap is a longer "reading" pause.
	PauseChance float64
}

// NewPacer is a pacer seeded from the clock.
func NewPacer(base time.Duration) *Pacer {
	seed := uint64(time.Now().UnixNano())
	return &Pacer{Rand: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), Base: base, SkipChance: 0.1, PauseChance: 0.1}
}

func (p *Pacer) between(lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(p.Rand.Int64N(int64(hi-lo)))
}

// Gap is the pause before a request: half to twice Base, and now and then
// four to fifteen times it, as when someone stops to read.
func (p *Pacer) Gap() time.Duration {
	if p.Base <= 0 {
		return 0
	}
	if p.Rand.Float64() < p.PauseChance {
		return p.between(4*p.Base, 15*p.Base)
	}
	return p.between(p.Base/2, 2*p.Base)
}

// StartDelay is how long a run waits before its first request, up to max.
func (p *Pacer) StartDelay(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return p.between(0, max)
}

// Order shuffles the streams for this run.
func (p *Pacer) Order(streams []Stream) []Stream {
	out := append([]Stream(nil), streams...)
	p.Rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// Skip says which streams sit out this run; it never skips them all.
func (p *Pacer) Skip(streams []Stream) map[string]bool {
	skip := map[string]bool{}
	for _, s := range streams {
		if p.Rand.Float64() < p.SkipChance {
			skip[s.Name] = true
		}
	}
	if len(skip) == len(streams) && len(streams) > 0 {
		delete(skip, streams[p.Rand.IntN(len(streams))].Name)
	}
	return skip
}

// Pages varies a page budget between about half of n and n (at least 1).
func (p *Pacer) Pages(n int) int {
	if n <= 1 {
		return n
	}
	lo := (n + 1) / 2
	return lo + p.Rand.IntN(n-lo+1)
}

// RateLimitCooldown is when a rate-limited stream may try again: after X's
// own reset if it named one, and at least half an hour out, plus 5-45
// minutes so the retry never lands on the reset second.
func (p *Pacer) RateLimitCooldown(reset, now time.Time) time.Time {
	at := now.Add(30 * time.Minute)
	if reset.After(at) {
		at = reset
	}
	return at.Add(p.between(5*time.Minute, 45*time.Minute))
}

// ChallengeCooldown is how long every request waits after x.com refused the
// session or challenged it: two to six hours, so a signed-out or challenged
// session is never retried every thirty minutes.
func (p *Pacer) ChallengeCooldown(now time.Time) time.Time {
	return now.Add(p.between(2*time.Hour, 6*time.Hour))
}
