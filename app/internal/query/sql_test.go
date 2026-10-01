package query

import "testing"

func TestValidateReadOnlySQLAllowsReadOnlyStatements(t *testing.T) {
	for _, sql := range []string{
		"SELECT * FROM gmail_messages LIMIT 1",
		"WITH recent AS (SELECT 1) SELECT * FROM recent",
		"SHOW TABLES",
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
		"SELECT * INTO evil_table FROM gmail_messages",
		"select id into other from slack_messages",
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

// Comments and dollar quotes are part of SQL. The guard used to track only
// quote characters, so an apostrophe in a `--` comment ("Zach's") flipped it
// into a phantom string and a later ';' literal read as a second statement,
// and a comment that said "update the cursor" was refused as an UPDATE.
// Found 2026-10-01 running the agent-usage aggregate through pdw sql.
func TestValidateReadOnlySQLUnderstandsCommentsAndDollarQuotes(t *testing.T) {
	for _, sql := range []string{
		"SELECT 1 AS a -- Zach's note\n, ';' AS b",
		"SELECT 1 -- update the cursor, then drop the temp rows\n",
		"SELECT 1 /* it's fine; really */ AS a",
		"SELECT 1 /* outer /* nested; */ still comment */ AS a",
		"SELECT $$a;b drop$$ AS x",
		"SELECT $tag$it's; DELETE$tag$ AS x",
		"SELECT 1; -- trailing comment",
		"SELECT 1; /* trailing */",
		"SELECT E'it\\'s;' AS x",
	} {
		t.Run(sql, func(t *testing.T) {
			if err := ValidateReadOnlySQL(sql); err != nil {
				t.Fatalf("ValidateReadOnlySQL(%q) = %v", sql, err)
			}
		})
	}
	for _, sql := range []string{
		"SELECT 1 -- comment\n; DELETE FROM x",
		"SELECT 1 /* c */; SELECT 2",
		"/* SELECT */ DELETE FROM x",
		"-- SELECT\nUPDATE x SET y = 1",
		"SELECT $$x$$; DROP TABLE y",
	} {
		t.Run(sql, func(t *testing.T) {
			if err := ValidateReadOnlySQL(sql); err == nil {
				t.Fatalf("ValidateReadOnlySQL(%q) should refuse", sql)
			}
		})
	}
}
