package query

import (
	"context"
	"strings"
	"testing"
)

type capturingScriptRunner struct{ statements []string }

func (r *capturingScriptRunner) Query(_ context.Context, statement string, _ int) (RawResult, error) {
	r.statements = append(r.statements, statement)
	return RawResult{Rows: []map[string]any{{"script_rows": `[]`, "script_oversize": false, "script_truncated": false}}}, nil
}

// A script's SQL is ordinary agent SQL: a leading comment or a trailing line
// comment must neither be refused nor comment out the bounding wrapper.
func TestScriptRunnerAcceptsCommentedSQL(t *testing.T) {
	for _, statement := range []string{
		"-- recent events\nSELECT 1 AS n",
		"/* recent */ WITH x AS (SELECT 1 AS n) SELECT n FROM x",
		"SELECT 1 AS n -- trailing note",
	} {
		inner := &capturingScriptRunner{}
		if _, err := (ScriptRunner{Runner: inner}).Query(context.Background(), statement, 10); err != nil {
			t.Fatalf("%q: %v", statement, err)
		}
		if len(inner.statements) != 1 || !strings.Contains(inner.statements[0], statement+"\n) t") {
			t.Fatalf("%q was not wrapped on its own lines: %v", statement, inner.statements)
		}
	}
	if _, err := (ScriptRunner{Runner: &capturingScriptRunner{}}).Query(context.Background(), "-- note\nSHOW search_path", 10); err == nil {
		t.Fatal("scripts accepted a non-SELECT statement behind a comment")
	}
}
