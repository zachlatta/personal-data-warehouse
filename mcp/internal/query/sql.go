package query

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

var allowedFirstKeywords = map[string]bool{
	"SELECT":   true,
	"WITH":     true,
	"SHOW":     true,
	"DESCRIBE": true,
	"DESC":     true,
	"EXPLAIN":  true,
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
		return fmt.Errorf("query tool is read-only; statement must start with SELECT, WITH, SHOW, DESCRIBE, DESC, or EXPLAIN")
	}
	for _, word := range words {
		if forbiddenKeywords[word] {
			return fmt.Errorf("query tool is read-only; %s statements are not allowed", word)
		}
	}
	return nil
}

func rejectMultipleStatements(sql string) error {
	inSingle, inDouble, inBacktick := false, false, false
	for i, r := range sql {
		switch r {
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
		case ';':
			if !inSingle && !inDouble && !inBacktick && strings.TrimSpace(sql[i+1:]) != "" {
				return errors.New("multiple SQL statements are not allowed")
			}
		}
	}
	return nil
}

func sqlWords(sql string) []string {
	var words []string
	inSingle, inDouble, inBacktick := false, false, false
	var b strings.Builder
	flush := func() {
		if b.Len() > 0 {
			words = append(words, strings.ToUpper(b.String()))
			b.Reset()
		}
	}

	for _, r := range sql {
		switch r {
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
			flush()
			continue
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
			flush()
			continue
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
			flush()
			continue
		}
		if inSingle || inDouble || inBacktick {
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return words
}
