// Package externalfeeds defines outgoing read interfaces, distinct from upstream MCP connections.
package externalfeeds

import (
	"context"
	"errors"
	"net/http"
)

type Definition struct {
	Scope       string                               `json:"scope"`
	Path        string                               `json:"path"`
	DisplayName string                               `json:"display_name"`
	Description string                               `json:"description"`
	SourceSQL   string                               `json:"-"`
	Handler     func(Definition, Store) http.Handler `json:"-"`
}

// Adding a feed means one entry here plus its source query. Authentication,
// cursor delivery, usage accounting and the management UI consume this registry.
var definitions = []Definition{{
	Scope: "feed:voice-memos", Path: "/api/external-feeds/voice-memos", DisplayName: "Voice memos",
	Description: "Recordings from the last 48 hours after transcription and current recording enrichment. Stopped-and-restarted recordings may arrive as separate parts; session joining does not hold them back.",
	SourceSQL:   voiceFeedSQL, Handler: NewHandler,
}}

func All() []Definition { return append([]Definition(nil), definitions...) }
func Lookup(scope string) (Definition, bool) {
	for _, d := range definitions {
		if d.Scope == scope {
			return d, true
		}
	}
	return Definition{}, false
}
func IsScope(scope string) bool { _, ok := Lookup(scope); return ok }

type Store interface {
	Page(context.Context, Definition, uint64, int) ([]map[string]any, uint64, bool, error)
}

type UnavailableStore struct{}

func (UnavailableStore) Page(context.Context, Definition, uint64, int) ([]map[string]any, uint64, bool, error) {
	return nil, 0, false, errors.New("feed store unavailable")
}

// One unified source query; never return storage URLs or audio credentials.
// The version must be at least as new as the transcription it consumed.
const voiceFeedSQL = `SELECT jsonb_build_array(m.source,m.account,m.recording_id)::text AS item_key,
 m.recorded_at, to_jsonb(m)::text AS payload_json FROM (
 SELECT source, account, recording_id, content_sha256,
 recorded_at, duration_seconds, title, summary, transcript,
 participants_json, action_items_json, evidence_json, calendar_event_id,
 calendar_confidence, meeting_start_at, meeting_end_at, transcribed_at, enriched_at
 FROM @marts_voice_memos_recordings
 WHERE recorded_at >= now() - interval '48 hours' AND recorded_at <= now()
 AND transcribed_at IS NOT NULL AND enrichment_provider IS NOT NULL
 AND enriched_at >= transcribed_at
 AND transcript IS NOT NULL AND btrim(transcript) <> '' AND is_deleted = 0
 ) m`
