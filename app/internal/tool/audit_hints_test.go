package tool

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestMutationArgumentHints(t *testing.T) {
	for _, key := range []string{"operation", "kind", "action"} {
		tool := &Typed[struct {
			Type string `json:"type"`
		}, string]{NameStr: "propose_mutation_help"}
		_, _, err := tool.Invoke(context.Background(), json.RawMessage(`{"`+key+`":"gmail.send_email"}`))
		if err == nil || !strings.Contains(err.Error(), `"type":"gmail.send_email"`) {
			t.Fatal(err)
		}
	}
	tool := &Typed[struct {
		File string `json:"storage_file_id"`
	}, string]{NameStr: "get_object"}
	_, _, err := tool.Invoke(context.Background(), json.RawMessage(`{"storage_file_id":"req_example"}`))
	if err == nil || !strings.Contains(err.Error(), "WHERE id = 'req_example'") {
		t.Fatal(err)
	}
}
