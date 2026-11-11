package server

import (
	"context"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/scripts"
)

type boundedScriptFake struct{}

func (boundedScriptFake) QueryRole() string { return "pdw_query" }
func (r boundedScriptFake) Query(ctx context.Context, sql string, n int) (query.RawResult, error) {
	return r.QueryArgs(ctx, sql, nil, n)
}
func (boundedScriptFake) QueryArgs(_ context.Context, sql string, _ []any, _ int) (query.RawResult, error) {
	rows := `[{"ref":"test:1","adapter":"test","event_id":"1","priority":"self"}]`
	if strings.Contains(sql, "information_schema.tables") {
		rows = `[{"schema":"timeline","name":"events"}]`
	}
	if strings.Contains(sql, "pg_attribute") {
		rows = `[{"name":"occurred_at","type":"timestamp with time zone"}]`
	}
	return query.RawResult{Rows: []map[string]any{{"script_rows": rows, "script_oversize": false, "script_truncated": false}}}, nil
}
func TestScriptFacadeUsesParameterizedSearchContextAndStructuredColumns(t *testing.T) {
	tool := scriptTool(boundedScriptFake{}, query.Options{})
	out, bad, err := tool.Invoke(context.Background(), []byte(`{"code":"const c=await pdw.columns({table:'timeline.events'}); const s=await pdw.search({query:'anchor',mode:'keyword',priorities:['self']}); await pdw.context({ref:s.rows[0].ref}); return c.columns[0].name;"}`))
	if err != nil || bad {
		t.Fatalf("%+v %v", out, err)
	}
	r := out.(scripts.Response)
	if string(r.Value) != `"occurred_at"` || len(r.Operations) != 3 || len(r.Sources) != 1 {
		t.Fatalf("%+v", r)
	}
	if policy, ok := tool.(interface{ LogOutput() bool }); !ok || policy.LogOutput() {
		t.Fatal("script results must not be logged")
	}
}
