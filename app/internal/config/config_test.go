package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadFromEnvRequiresPostgresURLAndSecret(t *testing.T) {
	_, err := LoadFromEnv(func(string) string { return "" })
	if err == nil {
		t.Fatal("expected missing env error")
	}
	if !strings.Contains(err.Error(), "POSTGRES_DATABASE_URL") || !strings.Contains(err.Error(), "MCP_SECRET_TOKEN") {
		t.Fatalf("error should mention both missing env vars, got %q", err.Error())
	}
}

func TestLoadFromEnvDefaultsAndOverrides(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL":               "postgres://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":                    "0123456789abcdef0123456789abcdef",
		"MCP_ADDR":                            ":9090",
		"MCP_BASE_URL":                        "https://mcp.example.com/",
		"MCP_MAX_ROWS":                        "7",
		"MCP_MAX_FIELD_CHARS":                 "12",
		"MCP_QUERY_CACHE_MAX_BYTES":           "1048576",
		"MCP_GET_FIELD_MAX_CHARS":             "345",
		"MCP_QUERY_CACHE_TTL":                 "15m",
		"MCP_DEBUG_CACHE_TOOL":                "true",
		"MCP_QUERY_TIMEOUT":                   "42s",
		"PDW_MUTATION_UI_PASSWORD":            "review-password",
		"PDW_MUTATION_UI_SESSION_SECRET":      "0123456789abcdef0123456789abcdef",
		"PDW_MUTATION_UI_SESSION_TTL_SECONDS": "7200",
		"GMAIL_ACCOUNTS":                      "zach@example.test, work@example.test",
		"CONTACT_GOOGLE_ACCOUNTS":             "contacts@example.test",
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
	if cfg.QueryCacheMaxBytes != 1048576 || cfg.GetFieldMaxChars != 345 || cfg.QueryCacheTTL != 15*time.Minute || !cfg.DebugCacheTool {
		t.Fatalf("cache limits = bytes %d field chars %d ttl %s debug %v", cfg.QueryCacheMaxBytes, cfg.GetFieldMaxChars, cfg.QueryCacheTTL, cfg.DebugCacheTool)
	}
	if cfg.QueryTimeout != 42*time.Second {
		t.Fatalf("QueryTimeout = %s", cfg.QueryTimeout)
	}
	if cfg.MutationUIPassword != "review-password" || cfg.MutationUISessionSecret != "0123456789abcdef0123456789abcdef" || cfg.MutationUISessionTTL != 2*time.Hour {
		t.Fatalf("mutation UI config = password %q ttl %s secret_len %d", cfg.MutationUIPassword, cfg.MutationUISessionTTL, len(cfg.MutationUISessionSecret))
	}
	if strings.Join(cfg.GmailAccounts, ",") != "zach@example.test,work@example.test" {
		t.Fatalf("GmailAccounts = %#v", cfg.GmailAccounts)
	}
	if strings.Join(cfg.ContactGoogleAccounts, ",") != "contacts@example.test" {
		t.Fatalf("ContactGoogleAccounts = %#v", cfg.ContactGoogleAccounts)
	}
	if cfg.PostgresDatabaseURL != "postgresql://default:secret@example.com/default" {
		t.Fatalf("PostgresDatabaseURL = %q", cfg.PostgresDatabaseURL)
	}
}

func TestLoadFromEnvPrefersPDWSecretTokenOverMCPSecretToken(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL": "postgresql://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":      "legacy0123456789abcdef0123456789abcd",
		"PDW_SECRET_TOKEN":      "preferred0123456789abcdef0123456789abcd",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.SecretToken != "preferred0123456789abcdef0123456789abcd" {
		t.Fatalf("SecretToken = %q; PDW_SECRET_TOKEN must win over legacy MCP_SECRET_TOKEN", cfg.SecretToken)
	}
}

func TestLoadFromEnvFallsBackToMCPSecretToken(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL": "postgresql://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":      "legacy0123456789abcdef0123456789abcd",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.SecretToken != "legacy0123456789abcdef0123456789abcd" {
		t.Fatalf("SecretToken = %q; expected MCP_SECRET_TOKEN fallback", cfg.SecretToken)
	}
}

func TestLoadFromEnvErrorMentionsPDWSecretToken(t *testing.T) {
	_, err := LoadFromEnv(func(string) string { return "" })
	if err == nil {
		t.Fatal("expected missing env error")
	}
	if !strings.Contains(err.Error(), "PDW_SECRET_TOKEN") {
		t.Fatalf("missing-env error should mention PDW_SECRET_TOKEN, got %q", err.Error())
	}
}

func TestLoadFromEnvRejectsWeakSecretToken(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL": "postgresql://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":      "short",
	}
	_, err := LoadFromEnv(func(key string) string { return env[key] })
	if err == nil {
		t.Fatal("expected weak secret error")
	}
	if !strings.Contains(err.Error(), "at least 32 characters") {
		t.Fatalf("unexpected error: %v", err)
	}
}
