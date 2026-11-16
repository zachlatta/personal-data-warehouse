package config

import (
	"strings"
	"testing"
)

func TestTheIngestSecretIsLoadedAndMustDifferFromTheMasterSecret(t *testing.T) {
	base := map[string]string{
		"POSTGRES_DATABASE_URL": "postgres://example/pdw",
		"PDW_SECRET_TOKEN":      "master-secret-at-least-32-characters-long",
		"PDW_INGEST_SECRET":     "ingest-secret-at-least-32-characters-long",
	}
	get := func(env map[string]string) func(string) string { return func(k string) string { return env[k] } }
	cfg, err := LoadFromEnv(get(base))
	if err != nil || cfg.IngestSecret != base["PDW_INGEST_SECRET"] {
		t.Fatalf("cfg %q err %v", cfg.IngestSecret, err)
	}
	same := map[string]string{}
	for k, v := range base {
		same[k] = v
	}
	same["PDW_INGEST_SECRET"] = same["PDW_SECRET_TOKEN"]
	if _, err := LoadFromEnv(get(same)); err == nil || !strings.Contains(err.Error(), "differ") {
		t.Fatalf("an ingest secret equal to the master secret: %v", err)
	}
	same["PDW_INGEST_SECRET"] = "short"
	if _, err := LoadFromEnv(get(same)); err == nil {
		t.Fatal("a short ingest secret was accepted")
	}
}
