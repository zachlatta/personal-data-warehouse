package warehouse

import "testing"

func TestQualifySQLDoesNotRewriteCreateSchemaNames(t *testing.T) {
	stmt := `CREATE SCHEMA IF NOT EXISTS "upstream_mutations"`
	if got := QualifySQL(stmt); got != stmt {
		t.Fatalf("CREATE SCHEMA statement was rewritten: %s", got)
	}
}

func TestQualifySQLRewritesWhoopRelationReferences(t *testing.T) {
	got := QualifySQL(`SELECT sleep_id FROM whoop_sleeps ORDER BY start_at DESC`)
	want := `SELECT sleep_id FROM "whoop"."sleeps" ORDER BY start_at DESC`
	if got != want {
		t.Fatalf("qualified SQL mismatch\nwant: %s\n got: %s", want, got)
	}
}

func TestQualifySQLRewritesUpstreamMutationRelationReferences(t *testing.T) {
	got := QualifySQL(`SELECT id FROM upstream_mutations WHERE status = 'pending_review'`)
	want := `SELECT id FROM "upstream_mutations"."operations" WHERE status = 'pending_review'`
	if got != want {
		t.Fatalf("qualified SQL mismatch\nwant: %s\n got: %s", want, got)
	}
}
