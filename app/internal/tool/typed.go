package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Handler runs a tool's business logic. A non-nil error is a transport-level
// or unexpected failure; domain errors that today set IsError=true on MCP
// (e.g. query_response error fields) should be returned as part of out and
// surfaced via Typed.IsError instead.
type Handler[I any, O any] func(ctx context.Context, input I) (O, error)

// IsErrorFn promotes a successful handler return to a domain-level error in
// the adapter's eyes (for example, MCP CallToolResult.IsError=true).
type IsErrorFn[O any] func(O) bool

// MCPArgumentNormalizer rewrites raw MCP arguments before schema validation.
// It is only needed for compatibility shims where real clients serialize a
// valid nested value into a string.
type MCPArgumentNormalizer func(json.RawMessage) (json.RawMessage, error)

// Typed is the concrete implementation tools use. The generic parameters
// keep the MCP SDK able to reflect on I to build the JSON input schema.
type Typed[I any, O any] struct {
	NameStr        string
	TitleStr       string
	DescriptionStr string
	// SurfacesField defaults to SurfaceAll so existing tools keep their
	// "exposed everywhere" behavior without setting the field.
	SurfacesField Surface
	Handle        Handler[I, O]
	IsError       IsErrorFn[O]
	// NormalizeMCPArguments, when set, runs before MCP input validation. The
	// advertised schema still comes from I; this only tolerates transport/client
	// encoding quirks before applying that schema.
	NormalizeMCPArguments MCPArgumentNormalizer
}

func (t *Typed[I, O]) Name() string        { return t.NameStr }
func (t *Typed[I, O]) Title() string       { return t.TitleStr }
func (t *Typed[I, O]) Description() string { return t.DescriptionStr }
func (t *Typed[I, O]) Surfaces() Surface   { return t.SurfacesField }

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
	if t.NormalizeMCPArguments != nil {
		t.registerNormalizingMCP(server, hooks)
		return
	}
	mcp.AddTool(server, &mcp.Tool{
		Name:        t.NameStr,
		Title:       t.TitleStr,
		Description: t.DescriptionStr,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input I) (*mcp.CallToolResult, any, error) {
		return t.mcpCallToolResult(ctx, input, hooks), nil, nil
	})
}

func (t *Typed[I, O]) registerNormalizingMCP(server *mcp.Server, hooks Hooks) {
	inputSchema, err := t.InputSchema()
	if err != nil {
		panic(fmt.Sprintf("AddTool: tool %q: input schema: %v", t.NameStr, err))
	}
	resolved, err := inputSchema.Resolve(&jsonschema.ResolveOptions{ValidateDefaults: true})
	if err != nil {
		panic(fmt.Sprintf("AddTool: tool %q: input schema: %v", t.NameStr, err))
	}
	server.AddTool(&mcp.Tool{
		Name:        t.NameStr,
		Title:       t.TitleStr,
		Description: t.DescriptionStr,
		InputSchema: inputSchema,
	}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		input := req.Params.Arguments
		input, err := t.NormalizeMCPArguments(input)
		if err != nil {
			var errRes mcp.CallToolResult
			errRes.SetError(err)
			return &errRes, nil
		}
		input, err = validateMCPArguments(input, resolved)
		if err != nil {
			var errRes mcp.CallToolResult
			errRes.SetError(fmt.Errorf("validating \"arguments\": %v", err))
			return &errRes, nil
		}

		var typedInput I
		if input != nil {
			if err := json.Unmarshal(input, &typedInput); err != nil {
				var errRes mcp.CallToolResult
				errRes.SetError(err)
				return &errRes, nil
			}
		}
		return t.mcpCallToolResult(ctx, typedInput, hooks), nil
	})
}

func validateMCPArguments(input json.RawMessage, resolved *jsonschema.Resolved) (json.RawMessage, error) {
	if resolved == nil {
		return input, nil
	}
	value := make(map[string]any)
	if len(input) > 0 {
		if err := json.Unmarshal(input, &value); err != nil {
			return nil, fmt.Errorf("unmarshaling arguments: %w", err)
		}
	}
	if err := resolved.ApplyDefaults(&value); err != nil {
		return nil, fmt.Errorf("applying schema defaults:\n%w", err)
	}
	if err := resolved.Validate(&value); err != nil {
		return nil, err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshalling with defaults: %v", err)
	}
	return data, nil
}

func (t *Typed[I, O]) mcpCallToolResult(ctx context.Context, input I, hooks Hooks) *mcp.CallToolResult {
	if hooks.OnCall != nil {
		hooks.OnCall(ctx, t.NameStr)
	}
	out, err := t.Handle(ctx, input)
	if err != nil {
		if hooks.OnResult != nil {
			hooks.OnResult(ctx, t.NameStr, nil, true, err)
		}
		return mcpErrorResult(err)
	}
	isErr := t.IsError != nil && t.IsError(out)
	if hooks.OnResult != nil {
		hooks.OnResult(ctx, t.NameStr, out, isErr, nil)
	}
	if mc, ok := any(out).(MultiContentMarshaler); ok {
		return mc.MCPCallToolResult(isErr)
	}
	return jsonToolResult(out, isErr)
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
