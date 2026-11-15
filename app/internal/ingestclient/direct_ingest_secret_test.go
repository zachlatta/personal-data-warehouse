package ingestclient

import "testing"

// A host signs uploads with PDW_INGEST_SECRET, the upload-only key, ahead of
// the master secret and the login file's token.
func TestResolveConfigPrefersTheIngestSecret(t *testing.T) {
	env := map[string]string{"PDW_API_URL": "https://pdw.example", "PDW_INGEST_SECRET": "ingest-only", "PDW_SECRET_TOKEN": "master", "PDW_CONFIG": "/nonexistent"}
	getenv := func(name string) string { return env[name] }
	if cfg := ResolveConfig(getenv, "", ""); cfg.Token != "ingest-only" {
		t.Fatalf("token = %q", cfg.Token)
	}
	if cfg := (Config{BaseURL: "https://pdw.example"}).WithEnvFallback(getenv); cfg.Token != "ingest-only" {
		t.Fatalf("fallback token = %q", cfg.Token)
	}
}
