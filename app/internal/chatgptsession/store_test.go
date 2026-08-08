package chatgptsession

import (
	"strings"
	"testing"
)

func TestCredentialTableCarriesPipelineHealthState(t *testing.T) {
	for _, column := range []string{
		"expired_at timestamptz",
		"expired_token_sha256 text",
		"status text",
		"error text",
	} {
		if !strings.Contains(createTableSQL, column) {
			t.Errorf("createTableSQL missing %q", column)
		}
	}
	for _, column := range []string{"expired_at", "expired_token_sha256", "status", "error"} {
		found := false
		for _, statement := range ensureColumnSQL {
			if strings.Contains(statement, " "+column+" ") {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("ensureColumnSQL missing migration for %q", column)
		}
	}
}
