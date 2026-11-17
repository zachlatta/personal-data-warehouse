package tool_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

// A tool's ArgumentHint answers before decoding on both surfaces, so a
// guessed field gets the corrected call rather than "unknown field".
func TestArgumentHintRunsOnBothSurfaces(t *testing.T) {
	handled := false
	tt := &tool.Typed[echoInput, echoOutput]{
		NameStr: "hinted",
		Handle: func(context.Context, echoInput) (echoOutput, error) {
			handled = true
			return echoOutput{}, nil
		},
		ArgumentHint: func(raw json.RawMessage) error {
			if strings.Contains(string(raw), "guessed") {
				return &tool.InvalidInputError{Message: "use value"}
			}
			return nil
		},
	}
	_, _, err := tt.Invoke(context.Background(), json.RawMessage(`{"guessed":"x"}`))
	var invalid *tool.InvalidInputError
	if !errors.As(err, &invalid) || invalid.Message != "use value" {
		t.Fatalf("CLI path: %v", err)
	}
	result := runOverMCP(t, tt, tool.Hooks{}, "hinted", map[string]any{"guessed": "x"})
	if !result.IsError || len(result.Content) == 0 {
		t.Fatalf("MCP path: %+v", result)
	}
	if !strings.Contains(string(mustJSON(t, result.Content)), "use value") {
		t.Fatalf("MCP path lost the hint: %s", mustJSON(t, result.Content))
	}
	if handled {
		t.Fatal("a hinted call must not reach the handler")
	}
	if result := runOverMCP(t, tt, tool.Hooks{}, "hinted", map[string]any{"value": "x"}); result.IsError {
		t.Fatalf("an unhinted call must still run: %s", mustJSON(t, result.Content))
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
