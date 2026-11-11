package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// Read the unified mart so every voice source and its content deduplication
// share the same eligibility rules. Never return source storage credentials.
const voiceFeedSQL = `SELECT source, account, recording_id, content_sha256,
 recorded_at, duration_seconds, title, summary, transcript,
 participants_json, action_items_json, evidence_json, calendar_event_id,
 calendar_confidence, meeting_start_at, meeting_end_at, transcribed_at, enriched_at
 FROM @marts_voice_memos_recordings
 WHERE recorded_at >= now() - interval '48 hours' AND recorded_at <= now()
 AND transcribed_at IS NOT NULL AND enrichment_provider IS NOT NULL
 AND enriched_at IS NOT NULL AND transcript IS NOT NULL AND btrim(transcript) <> '' AND is_deleted = 0
 ORDER BY enriched_at DESC, source, account, recording_id
 LIMIT 201 OFFSET %d`

type feedPage struct {
	body  []byte
	etag  string
	until time.Time
}
type voiceFeed struct {
	runner query.Runner
	mu     sync.Mutex
	pages  map[int]feedPage
}

func newVoiceFeed(runner query.Runner) *voiceFeed {
	return &voiceFeed{runner: runner, pages: map[int]feedPage{}}
}

func (f *voiceFeed) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-cache")
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}
	offset := 0
	if raw := r.URL.Query().Get("offset"); raw != "" {
		var err error
		offset, err = strconv.Atoi(raw)
		if err != nil || offset < 0 || offset > 100000 {
			http.Error(w, "invalid offset", 400)
			return
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	page := f.pages[offset]
	if !time.Now().Before(page.until) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		result, err := f.runner.Query(ctx, warehouse.ExpandRelations(fmt.Sprintf(voiceFeedSQL, offset)), 201)
		if err != nil {
			http.Error(w, "voice feed temporarily unavailable", 503)
			return
		}
		rows := result.Rows
		if rows == nil {
			rows = []map[string]any{}
		}
		var next *int
		if len(rows) > 200 {
			n := offset + 200
			next = &n
			rows = rows[:200]
		}
		body, err := json.Marshal(map[string]any{"feed": "voice-memos", "recordings": rows, "next_offset": next})
		if err != nil {
			http.Error(w, "could not encode feed", 500)
			return
		}
		page = feedPage{body: body, etag: fmt.Sprintf(`"%x"`, sha256.Sum256(body)), until: time.Now().Add(time.Second)}
		// A cached response must never retain a memo beyond its 48-hour cutoff.
		for _, row := range rows {
			var recorded time.Time
			switch v := row["recorded_at"].(type) {
			case time.Time:
				recorded = v
			case string:
				recorded, _ = time.Parse(time.RFC3339Nano, v)
			}
			if !recorded.IsZero() && recorded.Add(48*time.Hour).Before(page.until) {
				page.until = recorded.Add(48 * time.Hour)
			}
		}
		// Bounded memory even when clients probe arbitrary pages.
		for k, p := range f.pages {
			if !time.Now().Before(p.until) {
				delete(f.pages, k)
			}
		}
		if len(f.pages) < 32 {
			f.pages[offset] = page
		}
	}
	w.Header().Set("ETag", page.etag)
	if r.Header.Get("If-None-Match") == page.etag {
		w.WriteHeader(304)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(page.body)
}
