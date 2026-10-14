package query

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// drillRelations maps a timeline source_table (a catalog object id such as
// gmail_messages) to the relation a reader can SELECT from (base_gmail.messages).
//
// Why a hit carries a ready statement: source_table is a catalog id, not a
// relation name, and the CLI never printed source_pk at all, so the "one-hop
// drill-down" the guidance promised was a guess. Measured over the 30 days to
// 2026-10-13, reading a search hit's whole record produced a large share of
// all SQL errors: base_gmail.messages.id (18 sessions), timeline.events.ref
// (21), agent_sessions.turns (14) and the gmail.* / *_emails relations (17).
// A statement the agent can run verbatim leaves nothing to guess.
var drillRelations = func() map[string]string {
	out := make(map[string]string, len(warehouse.Objects))
	for _, obj := range warehouse.Objects {
		if obj.ID == "" || obj.Schema == "" || obj.Name == "" || !obj.IsRelation() {
			continue
		}
		out[obj.ID] = obj.Schema + "." + obj.Name
	}
	return out
}()

// DrillSQL returns a statement that reads the authoritative row behind a
// timeline event from its source_table and source_pk, or "" when either does
// not resolve. sourcePK may be the decoded jsonb (map) or its JSON text.
func DrillSQL(sourceTable string, sourcePK any) string {
	relation, ok := drillRelations[strings.TrimSpace(sourceTable)]
	if !ok {
		return ""
	}
	pk, ok := drillKey(sourcePK)
	if !ok || len(pk) == 0 {
		return ""
	}
	keys := make([]string, 0, len(pk))
	for key := range pk {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	predicates := make([]string, 0, len(keys))
	for _, key := range keys {
		if !drillIdentifier(key) {
			return ""
		}
		literal, ok := drillLiteral(pk[key])
		if !ok {
			return ""
		}
		predicates = append(predicates, key+" = "+literal)
	}
	return "SELECT * FROM " + relation + " WHERE " + strings.Join(predicates, " AND ")
}

func drillKey(sourcePK any) (map[string]any, bool) {
	switch pk := sourcePK.(type) {
	case map[string]any:
		return pk, true
	case string:
		var decoded map[string]any
		if err := json.Unmarshal([]byte(pk), &decoded); err != nil {
			return nil, false
		}
		return decoded, true
	case []byte:
		return drillKey(string(pk))
	default:
		return nil, false
	}
}

// drillIdentifier accepts the lowercase snake_case column names every
// source_pk uses, so a key can be spliced unquoted.
func drillIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if r == '_' || (r >= 'a' && r <= 'z') || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return false
	}
	return true
}

func drillLiteral(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'", true
	case float64:
		if v == math.Trunc(v) && math.Abs(v) < 1e15 {
			return fmt.Sprintf("%d", int64(v)), true
		}
		return fmt.Sprintf("%v", v), true
	case int, int32, int64:
		return fmt.Sprintf("%d", v), true
	case json.Number:
		return v.String(), true
	default:
		return "", false
	}
}

// addDrillSQL appends a drill_sql column to search rows that carry
// source_table and source_pk.
func addDrillSQL(columns []string, rows []map[string]any) []string {
	hasTable, hasPK := false, false
	for _, column := range columns {
		hasTable = hasTable || column == "source_table"
		hasPK = hasPK || column == "source_pk"
	}
	if !hasTable || !hasPK {
		return columns
	}
	for _, row := range rows {
		table, _ := row["source_table"].(string)
		if drill := DrillSQL(table, row["source_pk"]); drill != "" {
			row["drill_sql"] = drill
		}
	}
	return append(columns, "drill_sql")
}
