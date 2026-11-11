package query

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

func (r *PostgresRunner) QueryRole() string { return r.queryRole }

// ScriptRunner bounds the wire response on the PostgreSQL side, before pgx
// allocates fields. It still delegates to the same read-only role/transaction
// runner. No privileged database pool is exposed to the script broker.
type ScriptRunner struct{ Runner Runner }

func (r ScriptRunner) Query(ctx context.Context, statement string, maxRows int) (RawResult, error) {
	return r.QueryArgs(ctx, statement, nil, maxRows)
}

func (r ScriptRunner) QueryArgs(ctx context.Context, statement string, args []any, maxRows int) (RawResult, error) {
	if err := ValidateReadOnlySQL(statement); err != nil {
		return RawResult{}, err
	}
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	// Judge the first keyword after comments, as ValidateReadOnlySQL does.
	if words := sqlWords(trimmed); len(words) == 0 || (words[0] != "SELECT" && words[0] != "WITH") {
		return RawResult{}, fmt.Errorf("scripts support SELECT and WITH only")
	}
	if maxRows <= 0 || maxRows > 1000 {
		maxRows = 1000
	}
	// Filter before aggregation: never serialize an oversized aggregate to the
	// driver, even when a query returns many long text fields. The statement
	// sits on its own lines so a trailing -- comment cannot swallow ") t".
	sql := fmt.Sprintf(`WITH __pdw_rows AS MATERIALIZED (SELECT to_jsonb(t) AS j FROM (
%s
) t LIMIT %d),
 __pdw_sized AS (SELECT j, sum(octet_length(j::text)) OVER () AS bytes FROM __pdw_rows)
 SELECT coalesce(jsonb_agg(j) FILTER (WHERE bytes <= 1048576), '[]'::jsonb)::text AS script_rows,
 coalesce(max(bytes),0)>1048576 AS script_oversize, count(*)>%d AS script_truncated FROM __pdw_sized`, trimmed, maxRows+1, maxRows)
	var raw RawResult
	var err error
	if len(args) > 0 {
		delegate, ok := r.Runner.(interface {
			QueryArgs(context.Context, string, []any, int) (RawResult, error)
		})
		if !ok {
			return RawResult{}, fmt.Errorf("parameterized query runner required")
		}
		raw, err = delegate.QueryArgs(ctx, sql, args, 1)
	} else {
		raw, err = r.Runner.Query(ctx, sql, 1)
	}
	if err != nil {
		return RawResult{}, err
	}
	if len(raw.Rows) != 1 {
		return RawResult{}, fmt.Errorf("missing bounded result")
	}
	if raw.Rows[0]["script_oversize"] == true {
		return RawResult{}, fmt.Errorf("script operation exceeds 1 MiB; narrow the query")
	}
	if raw.Rows[0]["script_truncated"] == true {
		return RawResult{}, fmt.Errorf("script operation exceeds %d rows; use LIMIT", maxRows)
	}
	encoded, _ := raw.Rows[0]["script_rows"].(string)
	var rows []map[string]any
	decoder := json.NewDecoder(strings.NewReader(encoded))
	decoder.UseNumber()
	if err = decoder.Decode(&rows); err != nil {
		return RawResult{}, err
	}
	cols := []string{}
	if len(rows) > 0 {
		for k := range rows[0] {
			cols = append(cols, k)
		}
		sort.Strings(cols)
	}
	return RawResult{Columns: cols, Rows: rows}, nil
}

// StructuredColumns shares relation resolution and precise type SQL with the
// existing describe_table surface. Scripts do not have to parse rendered CSV.
func (s *Service) StructuredColumns(ctx context.Context, relation string) (RawResult, error) {
	ref, failure := s.resolveRelation(ctx, relation)
	if failure != "" {
		return RawResult{}, fmt.Errorf("%s", failure)
	}
	return s.runner.Query(ctx, describeColumnsSQLFor(ref), 1000)
}
func (s *Service) StructuredSchema(ctx context.Context, filter string) (RawResult, error) {
	filter = strings.ToLower(strings.TrimSpace(filter))
	if filter != "" && !schemaFilterRe.MatchString(filter) {
		return RawResult{}, fmt.Errorf("invalid schema prefix")
	}
	return s.runner.Query(ctx, schemaRelationsSQL()+" AND table_schema LIKE "+warehouse.SQLString(filter+"%")+" ORDER BY table_schema, table_name", 1000)
}
func schemaRelationsSQL() string {
	return "SELECT table_schema AS schema, table_name AS name FROM information_schema.tables WHERE table_schema = ANY(" + queryableSchemaArraySQL() + ") AND table_type IN ('BASE TABLE', 'VIEW')"
}
