package tool

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Handler runs a tool's business logic. A non-nil error is a transport-level
// or unexpected failure; domain errors that today set IsError=true on MCP
// (e.g. query_response error fields) should be returned as part of out and
// surfaced via Typed.IsError instead.
type Handler[I any, O any] func(ctx context.Context, input I) (O, error)

// IsErrorFn promotes a successful handler return to an error in the
// adapter's eyes (MCP CallToolResult.IsError=true, HTTP non-2xx).
type IsErrorFn[O any] func(O) bool

// Typed is the concrete implementation tools use. The generic parameters
// keep the MCP SDK able to reflect on I to build the JSON input schema.
type Typed[I any, O any] struct {
	NameStr        string
	TitleStr       string
	DescriptionStr string
	Handle         Handler[I, O]
	IsError        IsErrorFn[O]
}

func (t *Typed[I, O]) Name() string        { return t.NameStr }
func (t *Typed[I, O]) Title() string       { return t.TitleStr }
func (t *Typed[I, O]) Description() string { return t.DescriptionStr }

func (t *Typed[I, O]) InputSchema() (*jsonschema.Schema, error) {
	var zero I
	return jsonschema.ForType(reflect.TypeOf(zero), &jsonschema.ForOptions{})
}

func (t *Typed[I, O]) Invoke(ctx context.Context, raw json.RawMessage) (any, bool, error) {
	var input I
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, true, err
		}
	}
	out, err := t.Handle(ctx, input)
	if err != nil {
		return nil, true, err
	}
	isErr := t.IsError != nil && t.IsError(out)
	return out, isErr, nil
}

func (t *Typed[I, O]) RegisterMCP(server *mcp.Server, hooks Hooks) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        t.NameStr,
		Title:       t.TitleStr,
		Description: t.DescriptionStr,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input I) (*mcp.CallToolResult, any, error) {
		if hooks.OnCall != nil {
			hooks.OnCall(ctx, t.NameStr)
		}
		out, err := t.Handle(ctx, input)
		if err != nil {
			return mcpErrorResult(err), nil, nil
		}
		isErr := t.IsError != nil && t.IsError(out)
		if mc, ok := any(out).(MultiContentMarshaler); ok {
			return mc.MCPCallToolResult(isErr), nil, nil
		}
		return jsonToolResult(out, isErr), nil, nil
	})
}

func jsonToolResult(value any, isError bool) *mcp.CallToolResult {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: `{"error":"failed to encode tool response"}`}},
			IsError: true,
		}
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
		IsError: isError,
	}
}

func mcpErrorResult(err error) *mcp.CallToolResult {
	body, mErr := json.MarshalIndent(map[string]string{"error": err.Error()}, "", "  ")
	if mErr != nil {
		body = []byte(`{"error":"failed to encode tool response"}`)
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(body)}},
		IsError: true,
	}
}
