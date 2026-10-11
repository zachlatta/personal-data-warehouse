package query

import (
	"testing"
	"time"
)

// A timestamp without time zone has no zone, so it must not be rendered with a
// "Z". On 2026-10-11 an agent ran `start_at at time zone 'America/Chicago'`,
// got 13:30 back as "2026-10-12T13:30:00Z", converted it to Central a second
// time and told Zach a 1:30pm meeting was at 8:30am.
func TestNormalizeValueKeepsWallClockTimestampsZoneless(t *testing.T) {
	chicago := time.Date(2026, 10, 12, 13, 30, 0, 0, time.UTC)
	cases := []struct {
		dbType string
		value  time.Time
		want   string
	}{
		{"TIMESTAMP", chicago, "2026-10-12T13:30:00"},
		{"TIMESTAMP", time.Date(2026, 10, 12, 13, 30, 0, 500000000, time.UTC), "2026-10-12T13:30:00.5"},
		{"DATE", time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC), "2026-10-12"},
		{"TIMESTAMPTZ", time.Date(2026, 10, 12, 13, 30, 0, 0, time.FixedZone("CDT", -5*3600)), "2026-10-12T18:30:00Z"},
		{"", chicago, "2026-10-12T13:30:00Z"},
	}
	for _, tc := range cases {
		if got := normalizeValue(tc.value, tc.dbType); got != tc.want {
			t.Errorf("normalizeValue(%v, %q) = %v, want %v", tc.value, tc.dbType, got, tc.want)
		}
	}
}
