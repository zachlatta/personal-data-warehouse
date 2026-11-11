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

// SessionManifestFiles builds the manifest shards for agentsRoot. Each shard
// holds the sessions whose ids share a prefix, starting at one character; a
// shard whose JSON would exceed maxBytes (the inline text cap) is split by
// the next character, so no shard is ever too large to ride inline. cache
// may be nil.
func SessionManifestFiles(agentsRoot string, cache agentsessions.MuseManifestCache, maxBytes int64) ([]GeneratedFile, error) {
	entries, err := agentsessions.MuseManifest(agentsRoot, cache)
	if err != nil {
		return nil, err
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxInlineBytes
	}
	var files []GeneratedFile
	if err := appendManifestShards(&files, entries, 1, maxBytes); err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func appendManifestShards(files *[]GeneratedFile, entries []agentsessions.MuseManifestEntry, prefixLen int, maxBytes int64) error {
	groups := map[string][]agentsessions.MuseManifestEntry{}
	for _, entry := range entries {
		key := manifestShardKey(entry.SessionID, prefixLen)
		groups[key] = append(groups[key], entry)
	}
	for key, group := range groups {
		content, err := json.Marshal(manifestShard{Format: ManifestFormat, Shard: key, Sessions: group})
		if err != nil {
			return err
		}
		// Split while a longer prefix can still separate the group; a lone
		// session too large to fit is shipped and refused by the uploader.
		if int64(len(content)) > maxBytes && len(group) > 1 && prefixLen < manifestMaxPrefix {
			if err := appendManifestShards(files, group, prefixLen+1, maxBytes); err != nil {
				return err
			}
			continue
		}
		*files = append(*files, GeneratedFile{Path: ManifestDir + "/" + key + ".json", MimeType: "application/json", Content: content})
	}
	return nil
}

// manifestMaxPrefix bounds the split; a UUID's first eight characters are hex.
const manifestMaxPrefix = 8

// manifestShardKey is the session id's first n characters, lowercased, each
// kept when it is a hex digit and "_" otherwise.
func manifestShardKey(sessionID string, n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		c := byte('_')
		if i < len(sessionID) {
			c = strings.ToLower(sessionID[i : i+1])[0]
			if !strings.ContainsRune("0123456789abcdef", rune(c)) {
				c = '_'
			}
		}
		b.WriteByte(c)
	}
	return b.String()
}
