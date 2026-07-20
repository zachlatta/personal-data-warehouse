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

func TestQualifySQLRewritesPlaidRelations(t *testing.T) {
	got := QualifySQL(`SELECT account_id FROM plaid_accounts WHERE is_removed = 0`)
	want := `SELECT account_id FROM "plaid"."accounts" WHERE is_removed = 0`
	if got != want {
		t.Fatalf("qualified SQL mismatch\nwant: %s\n got: %s", want, got)
	}
}

func TestTimelineDetailRelationsUseCanonicalSchemas(t *testing.T) {
	cases := map[string]string{
		"alice_voice_recordings":          `"alice_voice_recordings"."recordings"`,
		"alice_voice_recording_artifacts": `"alice_voice_recordings"."artifacts"`,
		"finance_transactions":            `"finance"."transactions"`,
		"finance_observations":            `"finance"."observations"`,
		"manual_finance_documents":        `"manual_finance"."documents"`,
		"manual_finance_extractions":      `"manual_finance"."extractions"`,
		"apple_message_chats":             `"apple_messages"."chats"`,
	}
	for logical, want := range cases {
		if got := SQLRelation(logical); got != want {
			t.Fatalf("SQLRelation(%q) = %s, want %s", logical, got, want)
		}
	}
}
