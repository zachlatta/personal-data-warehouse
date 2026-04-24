package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadFromEnvRequiresClickHouseURLAndSecret(t *testing.T) {
	_, err := LoadFromEnv(func(string) string { return "" })
	if err == nil {
		t.Fatal("expected missing env error")
	}
	if !strings.Contains(err.Error(), "CLICKHOUSE_URL") || !strings.Contains(err.Error(), "MCP_SECRET_TOKEN") {
		t.Fatalf("error should mention both missing env vars, got %q", err.Error())
	}
}

func TestLoadFromEnvDefaultsAndOverrides(t *testing.T) {
	env := map[string]string{
		"CLICKHOUSE_URL":      "https://default:secret@example.com/default?secure=true",
		"MCP_SECRET_TOKEN":    "0123456789abcdef0123456789abcdef",
		"MCP_ADDR":            ":9090",
		"MCP_BASE_URL":        "https://mcp.example.com/",
		"MCP_MAX_ROWS":        "7",
		"MCP_MAX_FIELD_CHARS": "12",
		"MCP_QUERY_TIMEOUT":   "42s",
	}

	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv returned error: %v", err)
	}

	if cfg.Addr != ":9090" {
		t.Fatalf("Addr = %q", cfg.Addr)
	}
	if cfg.BaseURL != "https://mcp.example.com" {
		t.Fatalf("BaseURL = %q", cfg.BaseURL)
	}
	if cfg.MaxRows != 7 || cfg.MaxFieldChars != 12 {
		t.Fatalf("limits = rows %d chars %d", cfg.MaxRows, cfg.MaxFieldChars)
	}
	if cfg.QueryTimeout != 42*time.Second {
		t.Fatalf("QueryTimeout = %s", cfg.QueryTimeout)
	}
}

func TestLoadFromEnvRejectsWeakSecretToken(t *testing.T) {
	env := map[string]string{
		"CLICKHOUSE_URL":   "https://default:secret@example.com/default?secure=true",
		"MCP_SECRET_TOKEN": "short",
	}
	_, err := LoadFromEnv(func(key string) string { return env[key] })
	if err == nil {
		t.Fatal("expected weak secret error")
	}
	if !strings.Contains(err.Error(), "at least 32 characters") {
		t.Fatalf("unexpected error: %v", err)
	}
}
