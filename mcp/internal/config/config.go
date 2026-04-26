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
	Addr          string
	BaseURL       string
	ClickHouseURL string
	SecretToken   string
	MaxRows       int
	MaxFieldChars int
	QueryTimeout  time.Duration
}

func LoadFromEnv(getenv func(string) string) (Config, error) {
	cfg := Config{
		Addr:          valueOrDefault(getenv("MCP_ADDR"), ":8080"),
		BaseURL:       strings.TrimRight(strings.TrimSpace(getenv("MCP_BASE_URL")), "/"),
		ClickHouseURL: strings.TrimSpace(getenv("CLICKHOUSE_URL")),
		SecretToken:   getenv("MCP_SECRET_TOKEN"),
		MaxRows:       100,
		MaxFieldChars: 4000,
		QueryTimeout:  300 * time.Second,
	}

	var missing []string
	if cfg.ClickHouseURL == "" {
		missing = append(missing, "CLICKHOUSE_URL")
	}
	if cfg.SecretToken == "" {
		missing = append(missing, "MCP_SECRET_TOKEN")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}
	if len(cfg.SecretToken) < MinSecretTokenLength {
		return Config{}, fmt.Errorf("MCP_SECRET_TOKEN must be at least %d characters", MinSecretTokenLength)
	}

	var err error
	if cfg.MaxRows, err = parsePositiveInt(getenv("MCP_MAX_ROWS"), cfg.MaxRows, "MCP_MAX_ROWS"); err != nil {
		return Config{}, err
	}
	if cfg.MaxFieldChars, err = parsePositiveInt(getenv("MCP_MAX_FIELD_CHARS"), cfg.MaxFieldChars, "MCP_MAX_FIELD_CHARS"); err != nil {
		return Config{}, err
	}
	if raw := strings.TrimSpace(getenv("MCP_QUERY_TIMEOUT")); raw != "" {
		cfg.QueryTimeout, err = time.ParseDuration(raw)
		if err != nil || cfg.QueryTimeout <= 0 {
			return Config{}, fmt.Errorf("MCP_QUERY_TIMEOUT must be a positive Go duration")
		}
	}

	return cfg, nil
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
