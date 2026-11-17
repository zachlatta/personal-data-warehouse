package server

import (
	"sort"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
)

// The service enables an ingest endpoint only for a source that config gives
// a Drive folder, and the other ingest tests build their stores directly, so a
// source config did not know about shipped with every endpoint answering 404
// (android_call_recordings, 2026-11-16). Every source must resolve the shared
// object-store folder when it has no override of its own.
func TestEveryIngestSourceResolvesTheSharedFolder(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL":                   "postgres://d:s@example.com/d",
		"MCP_SECRET_TOKEN":                        "0123456789abcdef0123456789abcdef",
		"PDW_OBJECT_STORE_GOOGLE_TOKEN_JSON":      `{"type":"authorized_user"}`,
		"PDW_OBJECT_STORE_GOOGLE_DRIVE_FOLDER_ID": "shared-folder",
	}
	cfg, err := config.LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv returned error: %v", err)
	}
	var missing []string
	for slug := range ingestSourceDefs {
		if cfg.IngestFolderIDs[slug] != "shared-folder" {
			missing = append(missing, slug)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("ingest sources with no folder (their endpoints would 404): %v", missing)
	}
}
