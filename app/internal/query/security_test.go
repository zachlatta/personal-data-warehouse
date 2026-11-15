package query

import "testing"

func TestSQLRejectsSessionAndPrivilegeChanges(t *testing.T) {
	for _, sql := range []string{
		`SELECT set_config('role','postgres',true), query_to_xml('select 1',true,false,'')`,
		`SELECT pg_catalog."set_config"('role','postgres',true)`,
		`SELECT "pg_catalog"./* comment */"set_config"('role','postgres',true)`,
		`SELECT U&"set\005fconfig"('role','postgres',true)`,
		`SET ROLE postgres`, `RESET ROLE`, `SET SESSION AUTHORIZATION postgres`,
		`SELECT set_role('postgres')`,
	} {
		if ValidateReadOnlySQL(sql) == nil {
			t.Errorf("accepted %s", sql)
		}
	}
	if err := ValidateReadOnlySQL(`SELECT 'set_config', $$RESET ROLE$$ AS body`); err != nil {
		t.Fatal(err)
	}
}
