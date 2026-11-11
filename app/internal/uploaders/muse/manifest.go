package muse

import (
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/agentsessions"
)

// The session manifest makes hosted Muse checkable from the warehouse alone.
// The VM accepts no inbound connection, so nothing outside it can list its
// transcripts; without a list, "every session landed" had no evidence. Each
// run therefore publishes, as generated base_muse.files records, the
// metadata of every transcript under ~/agents/*/sessions/: its session id,
// the number of events the warehouse will hold for it (counted by the
// uploader's own line rule and keyed as the warehouse keys Muse events), its
// newest line's timestamp and the file's size and mtime. No transcript text.
//
// It is sharded by the session id's first character so a run re-sends only
// the shards whose sessions moved, and each shard stays far below the inline
// text cap. A shard leaves out any time of its own, so an idle Muse produces
// identical bytes and nothing is re-sent; base_muse.files.modified_at is the
// time a shard's content was last uploaded.

// ManifestDir is where the shards live in base_muse.files.path.
const ManifestDir = GeneratedDir + "/session-manifest"

// ManifestFormat names the shard layout for readers.
const ManifestFormat = "pdw.muse.session_manifest.v1"

// DefaultManifestInterval is how often a changing shard is re-sent. The hook
// runs every five minutes; while Muse works, its active shards change every
// run, and once an hour is enough for a completeness check.
const DefaultManifestInterval = time.Hour

type manifestShard struct {
	Format   string                            `json:"format"`
	Shard    string                            `json:"shard"`
	Sessions []agentsessions.MuseManifestEntry `json:"sessions"`
}

// SessionManifestFiles builds the manifest shards for agentsRoot.
func SessionManifestFiles(agentsRoot string) ([]GeneratedFile, error) {
	shards := map[string][]agentsessions.MuseManifestEntry{}
	for _, entry := range agentsessions.MuseManifest(agentsRoot) {
		key := manifestShardKey(entry.SessionID)
		shards[key] = append(shards[key], entry)
	}
	keys := make([]string, 0, len(shards))
	for key := range shards {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	files := make([]GeneratedFile, 0, len(keys))
	for _, key := range keys {
		content, err := json.Marshal(manifestShard{Format: ManifestFormat, Shard: key, Sessions: shards[key]})
		if err != nil {
			return nil, err
		}
		files = append(files, GeneratedFile{Path: ManifestDir + "/" + key + ".json", MimeType: "application/json", Content: content})
	}
	return files, nil
}

// manifestShardKey is the lowercase first hex digit of a session id, or "_".
func manifestShardKey(sessionID string) string {
	if sessionID == "" {
		return "_"
	}
	first := strings.ToLower(sessionID[:1])
	if strings.ContainsAny(first, "0123456789abcdef") {
		return first
	}
	return "_"
}
