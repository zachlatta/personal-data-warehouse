package query

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

var allowedFirstKeywords = map[string]bool{
	"SELECT":  true,
	"WITH":    true,
	"SHOW":    true,
	"EXPLAIN": true,
}

var forbiddenKeywords = map[string]bool{
	"ALTER":    true,
	"ATTACH":   true,
	"CREATE":   true,
	"DELETE":   true,
	"DETACH":   true,
	"DROP":     true,
	"GRANT":    true,
	"INSERT":   true,
	"INTO":     true,
	"KILL":     true,
	"OPTIMIZE": true,
	"RENAME":   true,
	"REVOKE":   true,
	"SYSTEM":   true,
	"TRUNCATE": true,
	"UPDATE":   true,
}

func ValidateReadOnlySQL(sql string) error {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return errors.New("SQL must not be empty")
	}
	if err := rejectMultipleStatements(trimmed); err != nil {
		return err
	}

	words := sqlWords(trimmed)
	if len(words) == 0 {
		return errors.New("SQL must contain a statement")
	}
	if !allowedFirstKeywords[words[0]] {
		return fmt.Errorf("query tool is read-only; statement must start with SELECT, WITH, SHOW, or EXPLAIN")
	}
	for _, word := range words {
		if forbiddenKeywords[word] {
			return fmt.Errorf("query tool is read-only; %s statements are not allowed", word)
		}
	}
	return nil
}

func rejectMultipleStatements(sql string) error {
	code := sqlCode(sql)
	if i := strings.IndexByte(code, ';'); i >= 0 && strings.TrimSpace(strings.ReplaceAll(code[i+1:], ";", "")) != "" {
		return errors.New("multiple SQL statements are not allowed; send one statement per call and run the rest as separate queries")
	}
	return nil
}

// sqlCode returns sql with every comment removed and the contents of every
// string, quoted identifier and dollar-quoted body blanked, so what is left is
// the statement's own tokens. The read-only guard reads keywords and ';' from
// this, never from the raw text: tracking quote characters alone made an
// apostrophe inside a `--` comment open a phantom string (so a later ';'
// literal read as a second statement) and refused a comment that mentioned
// "update" as an UPDATE (2026-10-01). Postgres syntax: `--` to end of line,
// nested /* */ comments, ” and "" doubling, E” backslash escapes, and
// $tag$...$tag$ bodies.
func sqlCode(sql string) string {
	var b strings.Builder
	n := len(sql)
	for i := 0; i < n; {
		c := sql[i]
		switch {
		case c == '-' && i+1 < n && sql[i+1] == '-':
			for i < n && sql[i] != '\n' {
				i++
			}
			b.WriteByte(' ')
		case c == '/' && i+1 < n && sql[i+1] == '*':
			depth := 0
			for i < n {
				if i+1 < n && sql[i] == '/' && sql[i+1] == '*' {
					depth++
					i += 2
					continue
				}
				if i+1 < n && sql[i] == '*' && sql[i+1] == '/' {
					depth--
					i += 2
					if depth == 0 {
						break
					}
					continue
				}
				i++
			}
			b.WriteByte(' ')
		case c == '\'':
			escapes := i > 0 && (sql[i-1] == 'E' || sql[i-1] == 'e') && (i < 2 || !isIdentByte(sql[i-2]))
			i = skipQuoted(sql, i, '\'', escapes)
			b.WriteString("''")
		case c == '"' || c == '`':
			i = skipQuoted(sql, i, c, false)
			b.WriteString(`""`)
		case c == '$':
			if tag, ok := dollarTag(sql[i:]); ok && (i == 0 || !isIdentByte(sql[i-1])) {
				end := strings.Index(sql[i+len(tag):], tag)
				if end < 0 {
					i = n
				} else {
					i += len(tag) + end + len(tag)
				}
				b.WriteString("''")
				continue
			}
			b.WriteByte(c)
			i++
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// skipQuoted returns the index just past the quoted run that opens at start.
// A doubled quote stays inside it; with escapes, a backslash takes the next
// byte with it.
func skipQuoted(sql string, start int, quote byte, escapes bool) int {
	for i := start + 1; i < len(sql); i++ {
		switch {
		case escapes && sql[i] == '\\':
			i++
		case sql[i] == quote:
			if i+1 < len(sql) && sql[i+1] == quote {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(sql)
}

// dollarTag reports the $tag$ (or $$) that opens s, if one does. A positional
// parameter ($1) is not a tag: a tag must start with a letter or underscore.
func dollarTag(s string) (string, bool) {
	for j := 1; j < len(s); j++ {
		c := s[j]
		if c == '$' {
			return s[:j+1], true
		}
		if !(c == '_' || unicode.IsLetter(rune(c)) || (j > 1 && unicode.IsDigit(rune(c)))) {
			return "", false
		}
	}
	return "", false
}

func isIdentByte(c byte) bool {
	return c == '_' || unicode.IsLetter(rune(c)) || unicode.IsDigit(rune(c))
}

func sqlWords(sql string) []string {
	var words []string
	var b strings.Builder
	flush := func() {
		if b.Len() > 0 {
			words = append(words, strings.ToUpper(b.String()))
			b.Reset()
		}
	}
	for _, r := range sqlCode(sql) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return words
}
