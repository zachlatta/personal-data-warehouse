package scripts

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// Python's full suite supplies a disposable database and namespace. No Go
// integration test guesses or mutates a production warehouse.
func TestScriptDatabaseIntegration(t *testing.T) {
	url, schema, role := os.Getenv("PDW_SCRIPT_TEST_URL"), os.Getenv("PDW_SCRIPT_TEST_SCHEMA"), os.Getenv("PDW_SCRIPT_TEST_ROLE")
	if url == "" || schema == "" || role == "" {
		t.Skip("run by tests/test_script_runs_integration.py")
	}
	if !strings.HasPrefix(schema, "pdw_test_") {
		t.Fatal("test namespace required")
	}
	store, e := NewStore(url)
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	store.schema = schema
	store.table = warehouse.QuoteIdent(schema) + `."script_runs"`
	record := Record{Response: Response{RunID: "integration-run", DurationMS: 3, Operations: []Operation{{Method: "search"}}}, Outcome: "ok", Client: "test", BytesReturned: 100}
	if e = store.Record(context.Background(), record); e != nil {
		t.Fatal(e)
	}
	// Also build the Go DDL next to the Python table for exact column comparison.
	if _, e = store.db.ExecContext(context.Background(), StoreDDL(warehouse.QuoteIdent(schema)+`."script_runs_go"`)); e != nil {
		t.Fatal(e)
	}
	runner, e := query.NewPostgresRunnerWithRole(url, 5*time.Second, role)
	if e != nil {
		t.Fatal(e)
	}
	defer runner.Close()
	svc := query.NewService(query.ScriptRunner{Runner: runner}, query.Options{MaxRows: 1000})
	scripts := newTestService(func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		var in struct {
			SQL string `json:"sql"`
		}
		if e := json.Unmarshal(raw, &in); e != nil {
			return nil, e
		}
		r := svc.ExecuteBatchFull(ctx, []query.Statement{{Question: "sandbox integration", SQL: in.SQL}}, "json").Results[0]
		if r.Error != "" {
			return nil, SafeError(r.Error, r.Hint)
		}
		return r, nil
	}, true, nil)
	r := scripts.Run(context.Background(), Input{Code: `const r=await pdw.sql({sql:"SELECT 9007199254740993::bigint AS n, 1.234567890123456789::numeric AS d"});return r.rows[0];`})
	if r.Error != nil || !strings.Contains(string(r.Value), `"n":"9007199254740993"`) || !strings.Contains(string(r.Value), `"d":"1.234567890123456789"`) {
		t.Fatalf("%+v", r)
	}
	for _, sql := range []string{"SELECT repeat('x',1048577) AS body", "SELECT * FROM generate_series(1,1002)", "SELECT * FROM private.app_access_tokens"} {
		code, _ := json.Marshal(sql)
		r = scripts.Run(context.Background(), Input{Code: "return await pdw.sql({sql:" + string(code) + "});"})
		if r.Error == nil {
			t.Fatalf("unbounded/private read succeeded: %s", sql)
		}
	}
	// The parameterized path is needed by search/context and must share bounds.
	raw, e := (query.ScriptRunner{Runner: runner}).QueryArgs(context.Background(), "SELECT $1::text AS value", []any{"literal"}, 1)
	if e != nil || len(raw.Rows) != 1 || raw.Rows[0]["value"] != "literal" {
		t.Fatalf("%+v %v", raw, e)
	}
}
