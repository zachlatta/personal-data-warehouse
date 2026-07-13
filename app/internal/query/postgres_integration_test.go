package query

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func postgresURLForIntegrationTest(t *testing.T) string {
	t.Helper()
	postgresURL := os.Getenv("POSTGRES_DATABASE_URL")
	if postgresURL == "" {
		t.Skip("POSTGRES_DATABASE_URL is not set")
	}
	return postgresURL
}

func TestQueryRoleSQLQuotesValidatedRole(t *testing.T) {
	got, err := queryRoleSQL("pdw_query")
	if err != nil {
		t.Fatalf("queryRoleSQL: %v", err)
	}
	if got != `SET LOCAL ROLE "pdw_query"` {
		t.Fatalf("queryRoleSQL = %q", got)
	}
}

func TestQueryRoleSQLRejectsInjection(t *testing.T) {
	if _, err := queryRoleSQL(`pdw_query"; RESET ROLE; --`); err == nil {
		t.Fatal("expected unsafe role to be rejected")
	}
}

func TestPostgresRunnerUsesRealPostgresDatabaseURL(t *testing.T) {
	runner, err := NewPostgresRunner(postgresURLForIntegrationTest(t), 5*time.Second)
	if err != nil {
		t.Fatalf("NewPostgresRunner returned error: %v", err)
	}
	defer runner.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := runner.Query(ctx, "SELECT 1 AS ok", 2)
	if err != nil {
		t.Fatalf("real Postgres query failed: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["ok"] == nil {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestPostgresRunnerAssumesRestrictedRoleAndCannotReadPrivatePlaidTokens(t *testing.T) {
	role := os.Getenv("PDW_QUERY_POSTGRES_ROLE")
	if role == "" {
		t.Skip("PDW_QUERY_POSTGRES_ROLE is not set")
	}
	runner, err := NewPostgresRunnerWithRole(postgresURLForIntegrationTest(t), 5*time.Second, role)
	if err != nil {
		t.Fatalf("NewPostgresRunnerWithRole returned error: %v", err)
	}
	defer runner.Close()

	result, err := runner.Query(context.Background(), "SELECT current_user AS role", 1)
	if err != nil {
		t.Fatalf("query through restricted role failed: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["role"] != role {
		t.Fatalf("query ran as %#v, want %q", result.Rows, role)
	}
	if _, err := runner.Query(context.Background(), "SELECT count(*) FROM plaid.accounts", 1); err != nil {
		t.Fatalf("restricted query role could not read a queryable Plaid table: %v", err)
	}
	if _, err := runner.Query(context.Background(), "SELECT access_token FROM private.plaid_item_tokens LIMIT 1", 1); err == nil {
		t.Fatal("restricted query role could select private Plaid tokens")
	}
}

func TestPostgresRunnerRejectsWrites(t *testing.T) {
	runner, err := NewPostgresRunner(postgresURLForIntegrationTest(t), 5*time.Second)
	if err != nil {
		t.Fatalf("NewPostgresRunner returned error: %v", err)
	}
	defer runner.Close()

	_, err = runner.Query(context.Background(), "CREATE TEMP TABLE pdw_ro_probe (x int)", 1)
	if err == nil {
		t.Fatal("expected read-only transaction to reject CREATE TEMP TABLE")
	}

	if _, err := runner.Query(context.Background(), "SELECT 1", 1); err != nil {
		t.Fatalf("read query failed after rejected write: %v", err)
	}
}

func TestPostgresRunnerAppliesStatementTimeout(t *testing.T) {
	runner, err := NewPostgresRunner(postgresURLForIntegrationTest(t), 5*time.Second)
	if err != nil {
		t.Fatalf("NewPostgresRunner returned error: %v", err)
	}
	defer runner.Close()
	runner.queryTimeout = 100 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = runner.Query(ctx, "SELECT pg_sleep(1)", 1)
	if err == nil {
		t.Fatal("expected pg_sleep query to be canceled by statement_timeout")
	}
	if !strings.Contains(err.Error(), "statement timeout") && !strings.Contains(err.Error(), "SQLSTATE 57014") {
		t.Fatalf("expected statement_timeout error, got %v", err)
	}
}
