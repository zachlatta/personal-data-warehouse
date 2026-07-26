package warehouse

import (
	"fmt"
	"strings"
)

// ExpandRelations expands explicit `@logical_id` markers in SQL text into
// schema-qualified relation names, the Go twin of the Python
// `expand_relations`.
//
// It replaced a rewriter that qualified *bare* identifiers anywhere in a
// statement. That could not distinguish a relation reference from a column of
// the same name, and it silently let an unknown name through unqualified for
// the search_path to resolve. A marker is unambiguous, and an unknown one
// panics: SQL naming something the catalog does not know is a bug in this
// binary. Markers inside SQL string literals and comments are left alone.
func ExpandRelations(sql string) string {
	if !strings.ContainsRune(sql, '@') {
		return sql
	}
	var out strings.Builder
	out.Grow(len(sql) + 64)
	for i := 0; i < len(sql); {
		ch := sql[i]
		switch {
		case ch == '\'':
			start := i
			i++
			for i < len(sql) {
				if sql[i] == '\'' {
					i++
					if i < len(sql) && sql[i] == '\'' {
						i++
						continue
					}
					break
				}
				i++
			}
			out.WriteString(sql[start:i])
		case ch == '"':
			start := i
			i++
			for i < len(sql) {
				if sql[i] == '"' {
					i++
					if i < len(sql) && sql[i] == '"' {
						i++
						continue
					}
					break
				}
				i++
			}
			out.WriteString(sql[start:i])
		case ch == '-' && i+1 < len(sql) && sql[i+1] == '-':
			start := i
			i += 2
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			out.WriteString(sql[start:i])
		case ch == '/' && i+1 < len(sql) && sql[i+1] == '*':
			start := i
			i += 2
			for i+1 < len(sql) && !(sql[i] == '*' && sql[i+1] == '/') {
				i++
			}
			if i+2 <= len(sql) {
				i += 2
			} else {
				i = len(sql)
			}
			out.WriteString(sql[start:i])
		case ch == '@' && i+1 < len(sql) && isIdentStart(sql[i+1]):
			start := i + 1
			i++
			for i < len(sql) && isIdentPart(sql[i]) {
				i++
			}
			logical := sql[start:i]
			rel, ok := relationsByID[logical]
			if !ok {
				panic(fmt.Sprintf("unknown warehouse relation %q in SQL", logical))
			}
			out.WriteString(QuoteIdent(rel.Schema) + "." + QuoteIdent(rel.Name))
		default:
			out.WriteByte(ch)
			i++
		}
	}
	return out.String()
}

func isIdentStart(ch byte) bool {
	return ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
}

func isIdentPart(ch byte) bool {
	return isIdentStart(ch) || (ch >= '0' && ch <= '9')
}
