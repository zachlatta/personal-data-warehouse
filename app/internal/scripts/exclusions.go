package scripts

import (
	"context"
	"encoding/json"
	"strings"
)

type Exclusions struct {
	Adapters []string `json:"adapters"`
	Refs     []string `json:"refs"`
}
type exclusionKey struct{}

func WithExclusions(ctx context.Context, e Exclusions) context.Context {
	return context.WithValue(ctx, exclusionKey{}, e)
}
func filterExcluded(ctx context.Context, method string, input json.RawMessage, data map[string]any) int {
	e, _ := ctx.Value(exclusionKey{}).(Exclusions)
	if len(e.Adapters)+len(e.Refs) == 0 {
		return 0
	}
	identifiers := append([]string{}, e.Refs...)
	for _, ref := range e.Refs {
		_, id, ok := strings.Cut(ref, ":")
		if ok {
			identifiers = append(identifiers, id)
			parts := strings.Split(id, "|")
			if len(parts) >= 2 && parts[1] != "" {
				identifiers = append(identifiers, parts[1])
			}
		}
	}
	var matches func(any) bool
	matches = func(v any) bool {
		switch x := v.(type) {
		case map[string]any:
			for _, c := range x {
				if matches(c) {
					return true
				}
			}
		case []any:
			for _, c := range x {
				if matches(c) {
					return true
				}
			}
		case string:
			for _, a := range e.Adapters {
				if x == a || strings.Contains(x, a+":") {
					return true
				}
			}
			for _, id := range identifiers {
				if strings.Contains(x, id) {
					return true
				}
			}
		}
		return false
	}
	// The input ref is checked as well as rows, matching the Python proxy's
	// context refusal even if a source backend returns rows without coordinates.
	var in any
	_ = json.Unmarshal(input, &in)
	rows, _ := data["rows"].([]any)
	kept := make([]any, 0, len(rows))
	for _, r := range rows {
		if !matches(r) && !(method == "context" && matches(in)) {
			kept = append(kept, r)
		}
	}
	dropped := len(rows) - len(kept)
	if dropped > 0 {
		data["rows"] = kept
		data["total_rows"] = len(kept)
	}
	return dropped
}
