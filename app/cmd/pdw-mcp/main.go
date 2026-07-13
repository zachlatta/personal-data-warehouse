package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/buildinfo"
	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutations"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/server"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	cfg, err := config.LoadFromEnv(os.Getenv)
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	logger.Info("configuration loaded", "addr", cfg.Addr, "base_url", cfg.BaseURL, "max_rows", cfg.MaxRows, "max_field_chars", cfg.MaxFieldChars, "query_timeout", cfg.QueryTimeout, "git_sha", buildinfo.GitSHA())
	runner, err := query.NewPostgresRunnerWithRole(cfg.PostgresDatabaseURL, cfg.QueryTimeout, cfg.QueryPostgresRole)
	if err != nil {
		logger.Error("connect to Postgres failed", "error", err)
		os.Exit(1)
	}
	defer runner.Close()

	authSvc := pdwauth.NewService([]byte(cfg.SecretToken), time.Now)
	var mutationSvc *mutations.Service
	if cfg.MutationUIPassword != "" {
		mutationStore, err := mutations.NewPostgresStore(cfg.PostgresDatabaseURL, cfg.QueryTimeout)
		if err != nil {
			logger.Error("connect to mutation Postgres store failed", "error", err)
			os.Exit(1)
		}
		defer mutationStore.Close()
		if err := mutationStore.EnsureTables(context.Background()); err != nil {
			logger.Error("ensure mutation tables failed", "error", err)
			os.Exit(1)
		}
		mutationSvc = mutations.NewService(mutationStore, mutations.Config{
			BaseURL:               cfg.BaseURL,
			UIPassword:            cfg.MutationUIPassword,
			SessionSecret:         cfg.MutationUISessionSecret,
			SessionTTL:            cfg.MutationUISessionTTL,
			GmailAccounts:         cfg.GmailAccounts,
			ContactGoogleAccounts: cfg.ContactGoogleAccounts,
			CalendarAccounts:      cfg.CalendarAccounts,
		})
		logger.Info("mutation review UI enabled", "path", mutations.ReviewPath)
	}
	logger.Info("personal data warehouse MCP server listening", "addr", cfg.Addr)
	if err := http.ListenAndServe(cfg.Addr, server.NewMux(cfg, authSvc, runner, mutationSvc)); err != nil {
		logger.Error("HTTP server stopped", "error", err)
		os.Exit(1)
	}
}
