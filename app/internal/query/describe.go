package query

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// DescribeTable returns one relation's full catalog: row estimate, index list,
// and every column with its exact Postgres type.
//
// It exists because schema_overview deliberately stopped carrying per-table
// column lists. At 108 relations that dump was ~61KB — too large for some
// clients to render at all, and far too large to keep in context — so agents
// called it once, lost it, and then guessed column names. In 30 days of real
// transcripts 70% of failed warehouse queries were SQLSTATE 42703
// (undefined column) against a long tail of invented names, while per-table
// discovery was used once per ~42 queries because it only existed as a CLI
// subcommand. Discovery has to be cheap, per-table, and available on every
// surface before "don't guess" is advice a caller can actually follow.
func (s *Service) DescribeTable(ctx context.Context, relation string) Response {
	started := time.Now()
	result := Result{SQL: "SELECT pg_attribute + pg_index + pg_class FOR " + relation}
	s.logger.InfoContext(ctx, "describe table started", "relation", relation)

	ref, failure := s.resolveRelation(ctx, relation)
	if failure != "" {
		result.Error = failure
		result.CSV = errorCSV(failure)
		s.logger.InfoContext(ctx, "describe table unresolved", "relation", relation, "error", failure, "duration", time.Since(started))
		return Response{Results: []Result{result}}
	}

	described := s.describeTable(ctx, ref)
	if described.Error != "" {
		s.logger.ErrorContext(ctx, "describe table failed", "relation", ref.DisplayName(), "error", described.Error, "duration", time.Since(started))
		return Response{Results: []Result{described}}
	}

	var out strings.Builder
	out.WriteString("# ")
	out.WriteString(ref.DisplayName())
	if estimate, ok := s.tableRowEstimate(ctx, ref); ok {
		out.WriteString(" (~")
		out.WriteString(formatRowCount(estimate))
		out.WriteString(" rows, estimated)")
	}
	out.WriteString("\n")
	if lines := s.tableIndexList(ctx, ref); len(lines) > 0 {
		out.WriteString("# indexes:\n")
		for _, line := range lines {
			out.WriteString("#   ")
			out.WriteString(line)
			out.WriteString("\n")
		}
	}
	out.WriteString("\n")
	out.WriteString(described.CSV)
	out.WriteString("\n")

	result.CSV = out.String()
	s.logger.InfoContext(ctx, "describe table completed", "relation", ref.DisplayName(), "duration", time.Since(started))
	return Response{Results: []Result{result}}
}

// resolveRelation turns caller input into a queryable relation. It accepts a
// schema-qualified name, a bare table name (resolved when exactly one schema
// has it), and the known-wrong names agents reach for. Every failure path names
// concrete candidates rather than sending the caller back to schema_overview
// empty-handed, because a bare "no such relation" is what produced the retry
// loops in the first place.
func (s *Service) resolveRelation(ctx context.Context, relation string) (tableRef, string) {
	trimmed := strings.TrimSpace(relation)
	trimmed = strings.TrimSuffix(trimmed, ";")
	if trimmed == "" {
		return tableRef{}, "relation is required, e.g. describe_table('gmail.messages'). Run schema_overview for the relation list."
	}

	parts := strings.Split(trimmed, ".")
	for i, part := range parts {
		parts[i] = strings.Trim(strings.TrimSpace(part), `"`)
	}
	if len(parts) > 2 {
		// A database-qualified name ("postgres.gmail.messages"): drop the leading
		// database segment rather than failing, since that prefix is the exact
		// mistake the overview's first line warns about.
		parts = parts[len(parts)-2:]
	}

	name := parts[len(parts)-1]
	schema := ""
	if len(parts) == 2 {
		schema = parts[0]
	}
	if name == "" {
		return tableRef{}, "relation is required, e.g. describe_table('gmail.messages'). Run schema_overview for the relation list."
	}
	if !validIdentifier(name) || (schema != "" && !validIdentifier(schema)) {
		return tableRef{}, fmt.Sprintf("%q is not a relation name — pass a schema-qualified relation like gmail.messages.", relation)
	}

	matches := s.relationsNamed(ctx, name)
	if schema != "" {
		for _, match := range matches {
			if match.Schema == schema {
				return match, ""
			}
		}
	} else if len(matches) == 1 {
		return matches[0], ""
	} else if len(matches) > 1 {
		return tableRef{}, fmt.Sprintf("%q is ambiguous — it exists as %s. Re-run with the schema-qualified name.", name, displayNames(matches))
	}

	// Nothing matched the name as given. A known wrong name is answered with the
	// right one; otherwise offer relations whose name contains the request, which
	// covers the singular/plural and prefix guesses that dominate 42P01s.
	lowered := strings.ToLower(trimmed)
	if remap, ok := tableRemaps[lowered]; ok {
		return tableRef{}, fmt.Sprintf("there is no %s relation — use %s.", trimmed, remap)
	}
	if remap, ok := tableRemaps[strings.ToLower(name)]; ok {
		return tableRef{}, fmt.Sprintf("there is no %s relation — use %s.", trimmed, remap)
	}
	if near := s.relationsLike(ctx, name); len(near) > 0 {
		return tableRef{}, fmt.Sprintf("no relation named %s. Closest matches: %s.", trimmed, displayNames(near))
	}
	if len(matches) > 0 {
		return tableRef{}, fmt.Sprintf("no relation named %s — %s exists as %s.", trimmed, name, displayNames(matches))
	}
	return tableRef{}, fmt.Sprintf("no relation named %s. Run schema_overview for the full relation list.", trimmed)
}

// relationsNamed returns every queryable relation with the given table name.
func (s *Service) relationsNamed(ctx context.Context, name string) []tableRef {
	sql := "SELECT table_schema AS schema, table_name AS name FROM information_schema.tables " +
		"WHERE table_schema = ANY(" + queryableSchemaArraySQL() + ") AND table_name = " + warehouse.SQLString(name) + " " +
		"ORDER BY table_schema"
	return s.relationLookup(ctx, sql)
}

// relationsLike returns queryable relations whose name contains the request, so
// a near miss (messages vs message, event vs events) resolves in one round trip.
func (s *Service) relationsLike(ctx context.Context, name string) []tableRef {
	pattern := "%" + strings.ReplaceAll(strings.ReplaceAll(name, "%", `\%`), "_", `\_`) + "%"
	sql := "SELECT table_schema AS schema, table_name AS name FROM information_schema.tables " +
		"WHERE table_schema = ANY(" + queryableSchemaArraySQL() + ") AND table_name LIKE " + warehouse.SQLString(pattern) + " " +
		"ORDER BY table_schema, table_name"
	return s.relationLookup(ctx, sql)
}

func (s *Service) relationLookup(ctx context.Context, sql string) []tableRef {
	result, err := s.runner.Query(ctx, sql, 0)
	if err != nil {
		s.logger.WarnContext(ctx, "relation lookup failed", "sql", sql, "error", err)
		return nil
	}
	return schemaTableRefs(result)
}

// tableRowEstimate reads one relation's planner row estimate. Views have none,
// which is reported as absent rather than zero.
func (s *Service) tableRowEstimate(ctx context.Context, ref tableRef) (int64, bool) {
	sql := "SELECT c.reltuples::bigint AS row_estimate FROM pg_class c " +
		"JOIN pg_namespace n ON n.oid = c.relnamespace " +
		"WHERE n.nspname = " + warehouse.SQLString(ref.Schema) + " AND c.relname = " + warehouse.SQLString(ref.Name) + " " +
		"AND c.relkind IN ('r', 'p', 'm') AND c.reltuples >= 0"
	result, err := s.runner.Query(ctx, sql, 1)
	if err != nil || len(result.Rows) == 0 {
		return 0, false
	}
	estimate, ok := int64Value(result.Rows[0]["row_estimate"])
	return estimate, ok
}

// tableIndexList reads one relation's indexes in the same rendering the schema
// overview used to emit, so callers that learned that format keep reading it.
func (s *Service) tableIndexList(ctx context.Context, ref tableRef) []string {
	sql := "SELECT regexp_replace(pg_get_indexdef(ix.indexrelid), '^.* USING ', '') AS def, " +
		"CASE WHEN ix.indisprimary THEN ' [primary key]' WHEN ix.indisunique THEN ' [unique]' ELSE '' END AS flag " +
		"FROM pg_index ix " +
		"JOIN pg_class i ON i.oid = ix.indexrelid " +
		"JOIN pg_class t ON t.oid = ix.indrelid " +
		"JOIN pg_namespace n ON n.oid = t.relnamespace " +
		"WHERE n.nspname = " + warehouse.SQLString(ref.Schema) + " AND t.relname = " + warehouse.SQLString(ref.Name) + " " +
		"AND t.relkind IN ('r', 'p', 'm') " +
		"ORDER BY ix.indisprimary DESC, def"
	result, err := s.runner.Query(ctx, sql, 0)
	if err != nil {
		s.logger.WarnContext(ctx, "describe table index lookup failed", "sql", sql, "error", err)
		return nil
	}
	lines := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		def, _ := row["def"].(string)
		flag, _ := row["flag"].(string)
		if def == "" {
			continue
		}
		lines = append(lines, def+flag)
	}
	return lines
}

func displayNames(refs []tableRef) string {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.DisplayName())
	}
	sort.Strings(names)
	if len(names) > 8 {
		return strings.Join(names[:8], ", ") + ", ..."
	}
	return strings.Join(names, ", ")
}

// sqlRelationRef matches a schema-qualified relation in a FROM/JOIN/UPDATE/INTO
// position. It is deliberately shallow: the only question asked of it is "does
// this statement reference exactly one relation", where a false negative just
// means the caller gets the plain hint.
var sqlRelationRef = regexp.MustCompile(`(?i)\b(?:from|join|update|into)\s+"?([a-z_][a-z0-9_]*)"?\s*\.\s*"?([a-z_][a-z0-9_]*)"?`)

// soleRelationInSQL returns the single relation a statement reads from, or
// false when it references none or several. With several, naming one table's
// columns would be actively misleading about which side of a join is wrong.
func soleRelationInSQL(sql string) (tableRef, bool) {
	var found tableRef
	for _, match := range sqlRelationRef.FindAllStringSubmatch(sql, -1) {
		ref := tableRef{Schema: strings.ToLower(match[1]), Name: strings.ToLower(match[2])}
		if found.Name != "" && found != ref {
			return tableRef{}, false
		}
		found = ref
	}
	return found, found.Name != ""
}

// describeColumnsSQLFor pulls each column's precise type via format_type (e.g.
// text[], bigint, timestamp with time zone) rather than
// information_schema.data_type, which collapses every array to the unhelpful
// "ARRAY". Callers use these types to avoid writing predicates the planner
// rejects, such as `is_deleted = false` against a bigint.
func describeColumnsSQLFor(ref tableRef) string {
	return "SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type " +
		"FROM pg_attribute a " +
		"JOIN pg_class c ON c.oid = a.attrelid " +
		"JOIN pg_namespace n ON n.oid = c.relnamespace " +
		"WHERE n.nspname = " + warehouse.SQLString(ref.Schema) + " AND c.relname = " + warehouse.SQLString(ref.Name) + " " +
		"AND a.attnum > 0 AND NOT a.attisdropped " +
		"ORDER BY a.attnum"
}

// relationColumnNames returns a relation's column names, or nil if it cannot be
// read. Used to answer an undefined-column error with the columns that do
// exist, which is the shortest path from a wrong guess to a working query.
func (s *Service) relationColumnNames(ctx context.Context, ref tableRef) []string {
	result, err := s.runner.Query(ctx, describeColumnsSQLFor(ref), 0)
	if err != nil {
		return nil
	}
	return describedColumnNames(result)
}

// int64Value normalizes the driver's numeric representations of a bigint
// column, which vary by driver and by whether the value round-tripped through
// JSON.
func int64Value(value any) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case float64:
		return int64(v), true
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		return n, err == nil
	}
	return 0, false
}

func validIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch == '_':
		case ch >= '0' && ch <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}
