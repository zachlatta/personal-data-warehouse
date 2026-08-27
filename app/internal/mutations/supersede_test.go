package mutations

import (
	"strings"
	"testing"
)

func TestSupersedeDDLAddsTheColumnToExistingDatabases(t *testing.T) {
	var found bool
	for _, statement := range upstreamMutationSchemaStatements {
		if !strings.Contains(statement, "superseded_by_request_id") {
			continue
		}
		if !strings.Contains(statement, "@upstream_mutation_requests") {
			t.Fatalf("supersede DDL does not name its relation through the catalog: %s", statement)
		}
		if strings.Contains(statement, "ADD COLUMN IF NOT EXISTS superseded_by_request_id") {
			found = true
		}
	}
	if !found {
		t.Fatal("no idempotent ALTER adds superseded_by_request_id to existing databases")
	}
}

func TestRequestIsSupersedable(t *testing.T) {
	for _, status := range []string{"failed_terminal", "blocked_missing_credentials", "failed_retryable"} {
		if !requestIsSupersedable(status) {
			t.Fatalf("%q should be supersedable", status)
		}
	}
	for _, status := range []string{"pending_review", "approved", "executing", "succeeded", "observed", "rejected"} {
		if requestIsSupersedable(status) {
			t.Fatalf("%q should not be supersedable", status)
		}
	}
}
