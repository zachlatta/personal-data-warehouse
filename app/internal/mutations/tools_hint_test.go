package mutations

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestMutationHelpAnswersGuessedTypeFields(t *testing.T) {
	var help interface {
		Invoke(context.Context, json.RawMessage) (any, bool, error)
	}
	for _, candidate := range Tools(&Service{}) {
		if candidate.Name() == "propose_mutation_help" {
			help = candidate
		}
	}
	if help == nil {
		t.Fatal("propose_mutation_help is not registered")
	}
	for _, key := range []string{"operation", "kind", "action"} {
		_, _, err := help.Invoke(context.Background(), json.RawMessage(`{"`+key+`":"gmail.send_email"}`))
		if err == nil || !strings.Contains(err.Error(), `"type":"gmail.send_email"`) {
			t.Fatalf("%s: %v", key, err)
		}
	}
}
