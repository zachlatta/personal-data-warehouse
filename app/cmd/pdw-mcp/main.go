package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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
	// Reviewed mutations live in the same Postgres as the warehouse; the
	// review surfaces are the JSON API (the web SPA and the iOS app) behind
	// the app's static bearer, so nothing else gates them.
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
	mutationSvc := mutations.NewService(mutationStore, mutations.Config{
		BaseURL:               cfg.BaseURL,
		GmailAccounts:         cfg.GmailAccounts,
		ContactGoogleAccounts: cfg.ContactGoogleAccounts,
		CalendarAccounts:      cfg.CalendarAccounts,
		AppleNotesAccounts:    cfg.AppleNotesAccounts,
		SlackAccounts:         cfg.SlackMutationAccounts,
	})
	logger.Info("mutation review enabled", "path", mutations.ReviewPath, "api", mutations.APIPath)
	notificationSvc, err := server.NewTimelineNotifications(cfg, os.Getenv("PDW_WEB_PUSH_PUBLIC_KEY"), os.Getenv("PDW_WEB_PUSH_PRIVATE_KEY"), cfg.BaseURL)
	if err != nil {
		logger.Error("notification worker initialization failed", "error", err)
		os.Exit(1)
	}
	defer notificationSvc.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); notificationSvc.Run(ctx) }()
	logger.Info("personal data warehouse MCP server listening", "addr", cfg.Addr)
	httpServer := &http.Server{Addr: cfg.Addr, Handler: server.NewMuxWithNotifications(cfg, authSvc, runner, notificationSvc, mutationSvc), ReadHeaderTimeout: 10 * time.Second}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdown, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		_ = httpServer.Shutdown(shutdown)
	}()
	failed := false
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("HTTP server stopped", "error", err)
		failed = true
		cancel()
	}
	cancel()
	<-workerDone
	<-shutdownDone
	if failed {
		os.Exit(1)
	}
}
