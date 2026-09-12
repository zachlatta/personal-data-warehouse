package notifications

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestLedgerPageParams(t *testing.T) {
	cases := []struct {
		query  string
		limit  int
		before string
		ok     bool
	}{
		{"", defaultLedgerLimit, "", true},
		{"?limit=10", 10, "", true},
		{"?limit=0", 0, "", false},
		{"?limit=-3", 0, "", false},
		{"?limit=abc", 0, "", false},
		{"?limit=100000", maxLedgerLimit, "", true},
		{"?before=2026-09-10T12:00:00Z|abc", defaultLedgerLimit, "2026-09-10T12:00:00Z", true},
		{"?before=2026-09-10T12:00:00Z", 0, "", false},
		{"?before=yesterday|abc", 0, "", false},
	}
	for _, c := range cases {
		page, ok := ledgerPageParams(httptest.NewRequest("GET", APIPath+c.query, nil))
		if ok != c.ok {
			t.Fatalf("%q: ok=%v want %v", c.query, ok, c.ok)
		}
		if !ok {
			continue
		}
		if page.limit != c.limit {
			t.Fatalf("%q: limit=%d want %d", c.query, page.limit, c.limit)
		}
		var want time.Time
		if c.before != "" {
			want, _ = time.Parse(time.RFC3339, c.before)
		}
		if !page.before.Equal(want) {
			t.Fatalf("%q: before=%v want %v", c.query, page.before, want)
		}
		if c.before != "" && page.beforeID != "abc" {
			t.Fatalf("%q: beforeID=%q", c.query, page.beforeID)
		}
	}
}

func TestCursorRoundTrip(t *testing.T) {
	at := time.Date(2026, 9, 10, 12, 0, 0, 123456789, time.UTC)
	got, id, ok := decodeCursor(encodeCursor(at, "n|1"))
	if !ok || !got.Equal(at) || id != "n|1" {
		t.Fatalf("round trip: %v %q %v", got, id, ok)
	}
}
