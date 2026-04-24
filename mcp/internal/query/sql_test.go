package query

import "testing"

func TestValidateReadOnlySQLAllowsReadOnlyStatements(t *testing.T) {
	for _, sql := range []string{
		"SELECT * FROM gmail_messages LIMIT 1",
		"WITH recent AS (SELECT 1) SELECT * FROM recent",
		"SHOW TABLES",
		"DESCRIBE TABLE slack_messages",
		"DESC gmail_messages",
		"EXPLAIN SELECT 1",
		"  SELECT 1;  ",
	} {
		t.Run(sql, func(t *testing.T) {
			if err := ValidateReadOnlySQL(sql); err != nil {
				t.Fatalf("ValidateReadOnlySQL returned error: %v", err)
			}
		})
	}
}

func TestValidateReadOnlySQLRejectsMutationsAndMultipleStatements(t *testing.T) {
	for _, sql := range []string{
		"INSERT INTO gmail_messages SELECT * FROM other",
		"DELETE FROM gmail_messages WHERE 1",
		"ALTER TABLE gmail_messages DELETE WHERE 1",
		"SELECT 1; SELECT 2",
		"",
		"   ",
	} {
		t.Run(sql, func(t *testing.T) {
			if err := ValidateReadOnlySQL(sql); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
