// Package warehouse is the app's view of the warehouse catalog.
//
// Every managed object is declared once in
// src/personal_data_warehouse/warehouse_catalog.json; catalog_gen.go is the
// generated Go mirror of that file and this file is the API over it. Warehouse
// SQL in the app is built by concatenating SQLRelation(...) results, so every
// reference is explicitly schema-qualified at the point it is written. There is
// no rewriter: a name this package does not know fails, it is not guessed at.
package warehouse

import (
	"fmt"
	"sort"
	"strings"
)

type CatalogSchema struct {
	Name         string
	Layer        string
	Domain       string
	Discoverable bool
	Comment      string
}

type CatalogObject struct {
	ID           string
	Kind         string
	Layer        string
	Domain       string
	Schema       string
	Name         string
	Discoverable bool
	QueryAccess  string
	Secret       bool
	// Comment is the relation's own catalog prose — what this table is for,
	// and the trap a caller would otherwise learn from a wrong answer. It is
	// published as the pg_class comment, and rendered by schema_overview
	// (first sentence) and describe_table (in full).
	Comment string
}

type StartHereGuidance struct {
	Schema   string
	Headline string
	Lines    []string
}

type Relation struct {
	Schema string
	Name   string
}

var (
	objectsByID      = map[string]CatalogObject{}
	relationsByID    = map[string]Relation{}
	queryableSchemas []string
)

func init() {
	for _, obj := range Objects {
		objectsByID[obj.ID] = obj
		relationsByID[obj.ID] = Relation{Schema: obj.Schema, Name: obj.Name}
	}
	for _, schema := range Schemas {
		if schema.Discoverable {
			queryableSchemas = append(queryableSchemas, schema.Name)
		}
	}
	sort.Strings(queryableSchemas)
}

// QueryableSchemas is what ordinary discovery lists and what the read-only
// query role may read wholesale: base_*, derived_*, marts_*, timeline. It sorts
// naturally in that order. ops/private/internal are deliberately absent.
func QueryableSchemas() []string {
	out := make([]string, len(queryableSchemas))
	copy(out, queryableSchemas)
	return out
}

// SQLRelation renders a schema-qualified reference for a stable logical id.
// Unknown ids panic: warehouse SQL that names something the catalog does not
// know is a bug in this binary, not a runtime condition to paper over, and the
// alternative (emitting a bare identifier) is exactly the silent-shadowing
// failure the reorg removed.
func SQLRelation(logical string) string {
	rel, ok := relationsByID[logical]
	if !ok {
		panic(fmt.Sprintf("unknown warehouse relation %q", logical))
	}
	return QuoteIdent(rel.Schema) + "." + QuoteIdent(rel.Name)
}

// SchemaOf returns the physical schema holding a stable logical id.
func SchemaOf(logical string) string {
	rel, ok := relationsByID[logical]
	if !ok {
		panic(fmt.Sprintf("unknown warehouse relation %q", logical))
	}
	return rel.Schema
}

// DisplayRelation renders the unquoted schema.name a human should see.
func DisplayRelation(logical string) string {
	rel, ok := relationsByID[logical]
	if !ok {
		return logical
	}
	return rel.Schema + "." + rel.Name
}

// CurrentSourceTable resolves a timeline row's stored source_table onto its
// current catalog id. Rows written before a catalog rename keep the historical
// token until the one-shot upgrade rewrites them.
func CurrentSourceTable(sourceTable string) string {
	if current, ok := RenamedTimelineSourceTables[sourceTable]; ok {
		return current
	}
	return sourceTable
}

// DrillDownRelation resolves a timeline source_table to the relation a detail
// view may read. Objects the read-only query role cannot reach are refused
// rather than producing a confusing permission error.
func DrillDownRelation(sourceTable string) (Relation, bool) {
	obj, ok := objectsByID[CurrentSourceTable(sourceTable)]
	if !ok || obj.QueryAccess == "denied" || !obj.IsRelation() {
		return Relation{}, false
	}
	return Relation{Schema: obj.Schema, Name: obj.Name}, true
}

// IsRelation reports whether the object lives in pg_class as something a
// SELECT can name.
func (o CatalogObject) IsRelation() bool {
	return o.Kind == "table" || o.Kind == "view"
}

func QuoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func SQLString(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}
