package query

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// Postgres already computes the answer to most undefined-column errors and
// ships it as HINT. pgx's PgError.Error() renders only severity/message/code,
// so that suggestion was being discarded before any caller saw it.
func TestWithPostgresDiagnosticsSurfacesServerHint(t *testing.T) {
	err := withPostgresDiagnostics(&pgconn.PgError{
		Severity: "ERROR",
		Code:     "42703",
		Message:  `column "message_ate" does not exist`,
		Hint:     `Perhaps you meant to reference the column "messages.message_at".`,
	})

	text := err.Error()
	if !strings.Contains(text, "SQLSTATE 42703") {
		t.Fatalf("original message lost: %q", text)
	}
	if !strings.Contains(text, `HINT: Perhaps you meant to reference the column "messages.message_at".`) {
		t.Fatalf("server hint not surfaced: %q", text)
	}
}

func TestWithPostgresDiagnosticsSurfacesDetail(t *testing.T) {
	err := withPostgresDiagnostics(&pgconn.PgError{
		Severity: "ERROR",
		Code:     "22P02",
		Message:  "invalid input syntax for type numeric",
		Detail:   `Value "" is not a valid number.`,
	})

	if !strings.Contains(err.Error(), `DETAIL: Value "" is not a valid number.`) {
		t.Fatalf("server detail not surfaced: %q", err.Error())
	}
}

// The decorated error must still unwrap to the pgconn error so callers can keep
// matching on SQLSTATE.
func TestWithPostgresDiagnosticsPreservesUnwrap(t *testing.T) {
	original := &pgconn.PgError{Severity: "ERROR", Code: "42703", Message: "boom", Hint: "try again"}

	var pgErr *pgconn.PgError
	if !errors.As(withPostgresDiagnostics(original), &pgErr) || pgErr.Code != "42703" {
		t.Fatalf("decorated error no longer unwraps to *pgconn.PgError")
	}
}

func TestWithPostgresDiagnosticsPassesThroughOtherErrors(t *testing.T) {
	original := errors.New("connection refused")
	if got := withPostgresDiagnostics(original); got != original {
		t.Fatalf("non-Postgres error was rewritten: %v", got)
	}
	bare := &pgconn.PgError{Severity: "ERROR", Code: "42601", Message: "syntax error"}
	if got := withPostgresDiagnostics(bare); got != error(bare) {
		t.Fatalf("PgError without detail/hint should pass through unchanged: %v", got)
	}
}
