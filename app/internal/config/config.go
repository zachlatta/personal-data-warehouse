package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const MinSecretTokenLength = 32

type Config struct {
	Addr                    string
	BaseURL                 string
	PostgresDatabaseURL     string
	SecretToken             string
	MaxRows                 int
	MaxFieldChars           int
	QueryCacheMaxBytes      int64
	GetFieldMaxChars        int
	QueryCacheTTL           time.Duration
	DebugCacheTool          bool
	QueryTimeout            time.Duration
	MutationUIPassword      string
	MutationUISessionSecret string
	MutationUISessionTTL    time.Duration
	GmailAccounts           []string
	ContactGoogleAccounts   []string
}

func LoadFromEnv(getenv func(string) string) (Config, error) {
	cfg := Config{
		Addr:                    valueOrDefault(getenv("MCP_ADDR"), ":8080"),
		BaseURL:                 strings.TrimRight(strings.TrimSpace(getenv("MCP_BASE_URL")), "/"),
		PostgresDatabaseURL:     normalizePostgresURL(getenv("POSTGRES_DATABASE_URL")),
		SecretToken:             firstNonEmpty(getenv("PDW_SECRET_TOKEN"), getenv("MCP_SECRET_TOKEN")),
		MaxRows:                 100000,
		MaxFieldChars:           4000,
		QueryCacheMaxBytes:      256 * 1024 * 1024,
		GetFieldMaxChars:        200000,
		QueryCacheTTL:           30 * time.Minute,
		DebugCacheTool:          false,
		QueryTimeout:            300 * time.Second,
		MutationUIPassword:      getenv("PDW_MUTATION_UI_PASSWORD"),
		MutationUISessionSecret: getenv("PDW_MUTATION_UI_SESSION_SECRET"),
		MutationUISessionTTL:    12 * time.Hour,
		GmailAccounts:           parseCSV(getenv("GMAIL_ACCOUNTS")),
		ContactGoogleAccounts:   parseCSV(getenv("CONTACT_GOOGLE_ACCOUNTS")),
	}

	var missing []string
	if cfg.PostgresDatabaseURL == "" {
		missing = append(missing, "POSTGRES_DATABASE_URL")
	}
	if cfg.SecretToken == "" {
		missing = append(missing, "PDW_SECRET_TOKEN (or legacy MCP_SECRET_TOKEN)")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}
	if len(cfg.SecretToken) < MinSecretTokenLength {
		return Config{}, fmt.Errorf("PDW_SECRET_TOKEN (or legacy MCP_SECRET_TOKEN) must be at least %d characters", MinSecretTokenLength)
	}

	var err error
	if cfg.MaxRows, err = parsePositiveInt(getenv("MCP_MAX_ROWS"), cfg.MaxRows, "MCP_MAX_ROWS"); err != nil {
		return Config{}, err
	}
	if cfg.MaxFieldChars, err = parsePositiveInt(getenv("MCP_MAX_FIELD_CHARS"), cfg.MaxFieldChars, "MCP_MAX_FIELD_CHARS"); err != nil {
		return Config{}, err
	}
	if cfg.QueryCacheMaxBytes, err = parsePositiveInt64(getenv("MCP_QUERY_CACHE_MAX_BYTES"), cfg.QueryCacheMaxBytes, "MCP_QUERY_CACHE_MAX_BYTES"); err != nil {
		return Config{}, err
	}
	if cfg.GetFieldMaxChars, err = parsePositiveInt(getenv("MCP_GET_FIELD_MAX_CHARS"), cfg.GetFieldMaxChars, "MCP_GET_FIELD_MAX_CHARS"); err != nil {
		return Config{}, err
	}
	if raw := strings.TrimSpace(getenv("MCP_QUERY_CACHE_TTL")); raw != "" {
		cfg.QueryCacheTTL, err = time.ParseDuration(raw)
		if err != nil || cfg.QueryCacheTTL <= 0 {
			return Config{}, fmt.Errorf("MCP_QUERY_CACHE_TTL must be a positive Go duration")
		}
	}
	cfg.DebugCacheTool = parseBool(getenv("MCP_DEBUG_CACHE_TOOL"))
	if raw := strings.TrimSpace(getenv("MCP_QUERY_TIMEOUT")); raw != "" {
		cfg.QueryTimeout, err = time.ParseDuration(raw)
		if err != nil || cfg.QueryTimeout <= 0 {
			return Config{}, fmt.Errorf("MCP_QUERY_TIMEOUT must be a positive Go duration")
		}
	}
	if raw := strings.TrimSpace(getenv("PDW_MUTATION_UI_SESSION_TTL_SECONDS")); raw != "" {
		seconds, err := parsePositiveInt64(raw, int64(cfg.MutationUISessionTTL/time.Second), "PDW_MUTATION_UI_SESSION_TTL_SECONDS")
		if err != nil {
			return Config{}, err
		}
		cfg.MutationUISessionTTL = time.Duration(seconds) * time.Second
	}
	if cfg.MutationUIPassword != "" && cfg.MutationUISessionSecret != "" && len(cfg.MutationUISessionSecret) < MinSecretTokenLength {
		return Config{}, fmt.Errorf("PDW_MUTATION_UI_SESSION_SECRET must be at least %d characters", MinSecretTokenLength)
	}

	return cfg, nil
}

func normalizePostgresURL(raw string) string {
	value := strings.TrimSpace(raw)
	if strings.HasPrefix(value, "postgres://") {
		return "postgresql://" + strings.TrimPrefix(value, "postgres://")
	}
	if strings.HasPrefix(value, "postgresql+psycopg2://") {
		return "postgresql://" + strings.TrimPrefix(value, "postgresql+psycopg2://")
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func valueOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func parsePositiveInt(raw string, fallback int, name string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, errors.New(name + " must be a positive integer")
	}
	return value, nil
}

func parsePositiveInt64(raw string, fallback int64, name string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, errors.New(name + " must be a positive integer")
	}
	return value, nil
}

func parseBool(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func parseCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, strings.ToLower(value))
		}
	}
	return values
}
