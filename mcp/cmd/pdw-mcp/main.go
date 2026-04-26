package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/mcp/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/mcp/internal/config"
	"github.com/zachlatta/personal-data-warehouse/mcp/internal/query"
	"github.com/zachlatta/personal-data-warehouse/mcp/internal/server"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	cfg, err := config.LoadFromEnv(os.Getenv)
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	logger.Info("configuration loaded", "addr", cfg.Addr, "base_url", cfg.BaseURL, "max_rows", cfg.MaxRows, "max_field_chars", cfg.MaxFieldChars, "query_timeout", cfg.QueryTimeout)
	runner, err := query.NewClickHouseRunner(cfg.ClickHouseURL, cfg.QueryTimeout)
	if err != nil {
		logger.Error("connect to ClickHouse failed", "error", err)
		os.Exit(1)
	}
	defer runner.Close()

	authSvc := pdwauth.NewService([]byte(cfg.SecretToken), time.Now)
	logger.Info("personal data warehouse MCP server listening", "addr", cfg.Addr)
	if err := http.ListenAndServe(cfg.Addr, server.NewMux(cfg, authSvc, runner)); err != nil {
		logger.Error("HTTP server stopped", "error", err)
		os.Exit(1)
	}
}
