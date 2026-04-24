package main

import (
	"log"
	"net/http"
	"os"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/mcp/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/mcp/internal/config"
	"github.com/zachlatta/personal-data-warehouse/mcp/internal/query"
	"github.com/zachlatta/personal-data-warehouse/mcp/internal/server"
)

func main() {
	cfg, err := config.LoadFromEnv(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	runner, err := query.NewClickHouseRunner(cfg.ClickHouseURL, cfg.QueryTimeout)
	if err != nil {
		log.Fatalf("connect to ClickHouse: %v", err)
	}
	defer runner.Close()

	authSvc := pdwauth.NewService([]byte(cfg.SecretToken), time.Now)
	log.Printf("personal data warehouse MCP server listening on %s", cfg.Addr)
	if err := http.ListenAndServe(cfg.Addr, server.NewMux(cfg, authSvc, runner)); err != nil {
		log.Fatal(err)
	}
}
